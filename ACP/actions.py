"""
Motor de Ações Seguras de OT (Defesa em Profundidade) da camada ACP.

Princípio fundamental de IA em sistemas críticos:
    "Segurança NUNCA mora no prompt — mora em código determinístico."

Toda ação proposta pela LLM atravessa obrigatoriamente 4 barreiras:
  1. Allowlist & Validação Estrita (Least Privilege): só aceita alvos/severidades
     pré-aprovados, sem shell livre nem escrita arbitrária.
  2. Intertravamento Determinístico (Hard Interlock + Fail-Closed): consulta o
     estado físico real via OPC UA / Docker antes de autorizar qualquer ação
     sobre o controlador (ex.: BLOQUEIA restart do OpenPLC se Burner=True ou
     Machine_State=2 WELDING, ou se OPC UA estiver inacessível com PLC rodando).
  3. Human-in-the-Loop (HITL) + Fronteira de Autonomia:
       - Ações de BAIXO risco (ex.: abrir Ordem de Serviço / Ticket): permitidas
         tanto no chat quanto no Watcher autônomo.
       - Ações de ALTO risco (ex.: reiniciar container DCN):
           * Modo `interactive` (operador no terminal): exibe plano + leitura
             dos intertravamentos e exige digitar `APPROVE`.
           * Modo `autonomous` (Watcher sem humano / stdin não-interativo):
             NUNCA executa sozinho — grava proposta em `ACP/pending_approvals/`
             com status `PENDING_HUMAN_APPROVAL` para aprovação posterior
             (via CLI ou futura interface de gestão ACP).
  4. Trilha de Auditoria Imutável (`ACP/audit_log.jsonl`): registra TODA tentativa
     (EXECUTED, DENIED_BY_HUMAN, BLOCKED_BY_ALLOWLIST, BLOCKED_BY_INTERLOCK,
     PENDING_HUMAN_APPROVAL, TIMEOUT_REACHED, FAILED) com timestamp, ator,
     snapshot físico e retorno.

Self-test determinístico das barreiras (sem Docker real e sem LLM):
    ./.venv/bin/python ACP/actions.py --test-safety
"""

import argparse
import json
import subprocess
import sys
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

# Permite importar tanto via `ACP.actions` quanto rodando `python ACP/actions.py`
try:
    from ACP.tools import DCN_ID, DOCKER_STACK_NAME, get_docker_status, get_live_machine_state
except ImportError:
    from tools import DCN_ID, DOCKER_STACK_NAME, get_docker_status, get_live_machine_state

# ---------------------------------------------------------------------------
# Caminhos de persistência (já modelados para alimentar uma futura UI de gestão ACP)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
WORK_ORDERS_DIR = PROJECT_ROOT / "ACP" / "work_orders"
PENDING_APPROVALS_DIR = PROJECT_ROOT / "ACP" / "pending_approvals"
AUDIT_LOG_PATH = PROJECT_ROOT / "ACP" / "audit_log.jsonl"

# ---------------------------------------------------------------------------
# Barreira 1: Allowlists estritas (Least Privilege) e timeouts operacionais
# ---------------------------------------------------------------------------
ALLOWED_RESTART_SERVICES = frozenset({"dcn-ws_nodered", "dcn-ws_openplc"})
VALID_SEVERITIES = ("LOW", "MEDIUM", "HIGH", "CRITICAL")
VALID_CONFIDENCES = ("LOW", "MEDIUM", "HIGH")
DOCKER_RESTART_TIMEOUT_SEC = 90.0


@dataclass
class ActionContext:
    """Contexto de execução propagado pelo Agent Loop / Watcher para as tools de ação."""
    mode: str = "interactive"  # "interactive" (operador no CLI) | "autonomous" (Watcher)
    actor: str = "acp_agent"   # Ex.: "acp_agent" | "acp_watcher"
    work_orders_dir: Path = WORK_ORDERS_DIR
    pending_approvals_dir: Path = PENDING_APPROVALS_DIR
    audit_log_path: Path = AUDIT_LOG_PATH


# Contexto padrão ativo durante uma chamada `execute_tool`
_CURRENT_CONTEXT = ActionContext()


def set_action_context(ctx: ActionContext) -> None:
    """Define o contexto de segurança/autonomia antes de executar um turno do agente."""
    global _CURRENT_CONTEXT
    _CURRENT_CONTEXT = ctx


def get_action_context() -> ActionContext:
    return _CURRENT_CONTEXT


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Barreira 4: Auditoria append-only (`ACP/audit_log.jsonl`)
# ---------------------------------------------------------------------------
def append_audit_log(
    ctx: ActionContext,
    tool: str,
    arguments: dict[str, Any],
    decision: str,
    interlock: dict[str, Any] | None,
    result: dict[str, Any],
) -> None:
    """Grava uma entrada JSONL imutável na trilha de auditoria de ações OT."""
    ctx.audit_log_path.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "timestamp_utc": _now_utc_iso(),
        "actor": ctx.actor,
        "mode": ctx.mode,
        "dcn_id": DCN_ID,
        "tool": tool,
        "arguments": arguments,
        "interlock": interlock,
        "decision": decision,
        "result": result,
    }
    with ctx.audit_log_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------
# Barreira 2: Intertravamentos Determinísticos em Python (Hard Interlocks)
# ---------------------------------------------------------------------------
def evaluate_restart_interlock(
    service_name: str,
    live_state_fn: Callable[[], dict[str, Any]] = get_live_machine_state,
    docker_status_fn: Callable[[int], dict[str, Any]] = get_docker_status,
) -> dict[str, Any]:
    """
    Avalia deterministicamente se é seguro reiniciar um serviço da stack DCN.

    Regras de segurança OT:
      - `dcn-ws_nodered` (HMI + Gateway OPC UA): fora da malha de controle de 20 ms
        do PLC -> permitido mesmo durante soldagem (ainda registra snapshot).
      - `dcn-ws_openplc` (Soft-PLC Runtime):
          1. Se OPC UA acessível e (`Burner == True` OU `Machine_State == 2 WELDING`):
             -> BLOQUEIO DURO (nunca reiniciar PLC com maçarico/soldagem ativa!).
          2. Se OPC UA inacessível:
             -> Checa Docker Swarm. Se OpenPLC já está fora do ar (`0/1`), permite
                recuperação. Se ainda está `1/1 Running`, aplica FAIL-CLOSED
                (bloqueia porque o PLC pode estar soldando sem telemetria visível!).
    """
    live = live_state_fn()
    opcua_ok = "error" not in live

    if service_name == "dcn-ws_nodered":
        return {
            "passed": True,
            "policy": "HMI_GATEWAY_OUT_OF_CONTROL_LOOP",
            "reason": "Node-RED (HMI/OPC UA gateway) is outside the 20 ms deterministic PLC control loop.",
            "live_snapshot": live if opcua_ok else {"opcua_reachable": False, "error": live.get("error")},
        }

    # Caso crítico: `dcn-ws_openplc`
    if opcua_ok:
        state_code = int(live.get("machine_state", -1))
        state_name = live.get("machine_state_name", "UNKNOWN")
        burner_on = bool(live.get("actuators", {}).get("Burner", False))

        if burner_on or state_code == 2:
            return {
                "passed": False,
                "policy": "HARD_INTERLOCK_ACTIVE_WELDING",
                "reason": (
                    f"SAFETY INTERLOCK BLOCK: restarting `{service_name}` is forbidden "
                    f"during active welding (Machine_State={state_code} {state_name}, "
                    f"Burner={burner_on}). Wait until the cycle completes (State 0/3/4)."
                ),
                "live_snapshot": {
                    "machine_state": state_code,
                    "machine_state_name": state_name,
                    "Burner": burner_on,
                },
            }

        return {
            "passed": True,
            "policy": "SAFE_STATE_VERIFIED_VIA_OPCUA",
            "reason": (
                f"Interlock passed: Machine_State={state_code} ({state_name}) "
                f"and Burner=False verified in real time via OPC UA."
            ),
            "live_snapshot": {
                "machine_state": state_code,
                "machine_state_name": state_name,
                "Burner": burner_on,
            },
        }

    # OPC UA inacessível -> checa se o container OpenPLC já está caído (0/1) ou rodando (1/1)
    docker_info = docker_status_fn(0)
    plc_svc = next(
        (s for s in docker_info.get("services", []) if s.get("name") == "dcn-ws_openplc"),
        None,
    )
    replicas = str(plc_svc.get("replicas", "")) if plc_svc else ""

    if plc_svc is not None and replicas.startswith("0/"):
        return {
            "passed": True,
            "policy": "DEAD_CONTAINER_RECOVERY",
            "reason": (
                f"OPC UA unreachable, but `{service_name}` is confirmed down "
                f"(replicas={replicas}). Recovery allowed pending human approval."
            ),
            "live_snapshot": {"opcua_reachable": False, "docker_replicas": replicas},
        }

    # FAIL-CLOSED (IEC 61508 / 61511): na dúvida sobre o estado do maçarico, BLOQUEIA!
    return {
        "passed": False,
        "policy": "FAIL_CLOSED_UNVERIFIABLE_STATE",
        "reason": (
            f"FAIL-CLOSED BLOCK: OPC UA unavailable ({live.get('details', live.get('error'))}) "
            f"while `{service_name}` reports replicas='{replicas or 'unknown'}'. "
            "Cannot safely guarantee Burner=False — verify the physical station "
            "or restore OPC UA / Node-RED telemetry first."
        ),
        "live_snapshot": {"opcua_reachable": False, "docker_replicas": replicas or "unknown"},
    }


# ---------------------------------------------------------------------------
# Barreira 3: Human-in-the-Loop (terminal interativo vs. fila de aprovação)
# ---------------------------------------------------------------------------
def _prompt_operator_approval(
    tool: str,
    arguments: dict[str, Any],
    interlock: dict[str, Any],
) -> tuple[bool, str]:
    """Solicita confirmação explícita do operador no terminal interativo."""
    print(f"\n{'!' * 72}")
    print("  [!] HUMAN-IN-THE-LOOP APPROVAL REQUIRED (OT SAFETY GATE)")
    print("!" * 72)
    print(f"  Proposed action : {tool}")
    print(f"  Target / Args   : {json.dumps(arguments, ensure_ascii=False)}")
    print(f"  Interlock       : PASS - {interlock.get('reason')}")
    print("-" * 72)
    try:
        answer = input("  Type 'APPROVE' to execute now (Enter/anything else cancels): ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\n  [Cancelled by operator]")
        return False, "Interrupted by operator."

    if answer == "APPROVE":
        return True, "Explicitly approved by operator in terminal."
    return False, f"Rejected by operator (input: '{answer or '<empty>'}')."


def _save_pending_approval(
    ctx: ActionContext,
    tool: str,
    arguments: dict[str, Any],
    interlock: dict[str, Any],
) -> tuple[str, Path]:
    """Persiste uma solicitação pendente de aprovação humana (para CLI ou UI de gestão ACP)."""
    ctx.pending_approvals_dir.mkdir(parents=True, exist_ok=True)
    approval_id = f"APR-{uuid.uuid4().hex[:8].upper()}"
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = ctx.pending_approvals_dir / f"{stamp}_{approval_id}.json"

    payload = {
        "approval_id": approval_id,
        "status": "PENDING_HUMAN_APPROVAL",
        "created_at_utc": _now_utc_iso(),
        "dcn_id": DCN_ID,
        "actor": ctx.actor,
        "tool": tool,
        "arguments": arguments,
        "interlock_at_proposal": interlock,
        "note": (
            "High-risk action proposed in autonomous mode. Execution requires explicit "
            "human review and real-time interlock revalidation at approval time."
        ),
    }
    file_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return approval_id, file_path


def _execute_docker_force_restart(service_name: str) -> subprocess.CompletedProcess:
    """Executa `docker service update --force <service>` sem shell=True."""
    return subprocess.run(
        ["docker", "service", "update", "--force", service_name],
        capture_output=True,
        text=True,
        timeout=DOCKER_RESTART_TIMEOUT_SEC,
        check=False,
    )


# ---------------------------------------------------------------------------
# Action Tool 1 (Baixo risco): Abrir Ticket / Ordem de Serviço de Manutenção
# ---------------------------------------------------------------------------
def create_maintenance_ticket(
    severity: str,
    component: str,
    title: str,
    diagnosis_summary: str,
    confidence: str,
    confidence_rationale: str,
    evidence: list[str],
    recommended_actions: list[str],
    ctx: ActionContext | None = None,
) -> dict[str, Any]:
    """
    Abre uma Ordem de Serviço (Ticket de Manutenção) estruturada em JSON na camada
    ACP (`ACP/work_orders/`), pronta para consumo por equipes de confiabilidade
    ou por uma futura interface web de gestão (APM/CMMS).

    Nível de risco: BAIXO (não atua sobre a planta física nem containers).
    """
    active_ctx = ctx or get_action_context()
    sev = str(severity).strip().upper()
    conf = str(confidence).strip().upper()

    raw_args = {
        "severity": sev,
        "component": component,
        "title": title,
        "confidence": conf,
    }

    if sev not in VALID_SEVERITIES:
        err = {"status": "REJECTED", "error": f"Invalid severity '{severity}'. Allowed: {VALID_SEVERITIES}"}
        append_audit_log(active_ctx, "create_maintenance_ticket", raw_args, "BLOCKED_BY_VALIDATION", None, err)
        return err

    if conf not in VALID_CONFIDENCES:
        err = {"status": "REJECTED", "error": f"Invalid confidence '{confidence}'. Allowed: {VALID_CONFIDENCES}"}
        append_audit_log(active_ctx, "create_maintenance_ticket", raw_args, "BLOCKED_BY_VALIDATION", None, err)
        return err

    active_ctx.work_orders_dir.mkdir(parents=True, exist_ok=True)
    ticket_id = f"WO-{uuid.uuid4().hex[:8].upper()}"
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = active_ctx.work_orders_dir / f"{stamp}_{ticket_id}.json"

    ticket = {
        "ticket_id": ticket_id,
        "status": "OPEN",
        "created_at_utc": _now_utc_iso(),
        "dcn_id": DCN_ID,
        "stack": DOCKER_STACK_NAME,
        "created_by": active_ctx.actor,
        "severity": sev,
        "component": str(component).strip(),
        "title": str(title).strip(),
        "diagnosis_summary": str(diagnosis_summary).strip(),
        "confidence": {
            "level": conf,
            "rationale": str(confidence_rationale).strip(),
        },
        "evidence": [str(item).strip() for item in evidence if str(item).strip()],
        "recommended_actions": [str(item).strip() for item in recommended_actions if str(item).strip()],
    }

    file_path.write_text(json.dumps(ticket, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    result = {
        "status": "CREATED",
        "ticket_id": ticket_id,
        "severity": sev,
        "component": ticket["component"],
        "confidence": conf,
        "file": str(file_path.relative_to(PROJECT_ROOT)) if file_path.is_relative_to(PROJECT_ROOT) else str(file_path),
    }
    decision = "EXECUTED_AUTONOMOUS" if active_ctx.mode == "autonomous" else "EXECUTED"
    append_audit_log(active_ctx, "create_maintenance_ticket", raw_args, decision, {"passed": True, "policy": "LOW_RISK_NON_ACTUATING"}, result)
    return result


# ---------------------------------------------------------------------------
# Action Tool 2 (Alto risco): Reiniciar serviço DCN no Docker Swarm
# ---------------------------------------------------------------------------
def restart_dcn_service(
    service_name: str,
    reason: str,
    ctx: ActionContext | None = None,
    live_state_fn: Callable[[], dict[str, Any]] = get_live_machine_state,
    docker_status_fn: Callable[[int], dict[str, Any]] = get_docker_status,
    confirm_fn: Callable[[str, dict[str, Any], dict[str, Any]], tuple[bool, str]] = _prompt_operator_approval,
    docker_exec_fn: Callable[[str], subprocess.CompletedProcess] = _execute_docker_force_restart,
) -> dict[str, Any]:
    """
    Solicita o reinício controlado (`docker service update --force`) de um serviço
    da stack DCN (`dcn-ws_nodered` ou `dcn-ws_openplc`), protegido pelas 4 barreiras:
    Allowlist -> Intertravamento Determinístico -> Human-in-the-Loop -> Auditoria.
    """
    active_ctx = ctx or get_action_context()
    target = str(service_name).strip()
    args = {"service_name": target, "reason": str(reason).strip()}

    # --- BARREIRA 1: Allowlist estrita ---
    if target not in ALLOWED_RESTART_SERVICES:
        result = {
            "status": "BLOCKED_BY_ALLOWLIST",
            "error": (
                f"Service '{target}' is not permitted. Only OT allowlisted services "
                f"may be restarted: {sorted(ALLOWED_RESTART_SERVICES)}"
            ),
        }
        append_audit_log(active_ctx, "restart_dcn_service", args, "BLOCKED_BY_ALLOWLIST", None, result)
        return result

    # --- BARREIRA 2: Intertravamento Determinístico (Hard Interlock) ---
    interlock = evaluate_restart_interlock(
        target,
        live_state_fn=live_state_fn,
        docker_status_fn=docker_status_fn,
    )
    if not interlock["passed"]:
        result = {
            "status": "BLOCKED_BY_INTERLOCK",
            "service_name": target,
            "policy": interlock["policy"],
            "reason": interlock["reason"],
            "live_snapshot": interlock["live_snapshot"],
        }
        append_audit_log(active_ctx, "restart_dcn_service", args, "BLOCKED_BY_INTERLOCK", interlock, result)
        return result

    # --- BARREIRA 3: Human-in-the-Loop & Fronteira de Autonomia ---
    # Fail-safe: se modo é `autonomous` OU não há terminal interativo (stdin sem TTY
    # quando usando o prompt padrão), NUNCA executa sozinho — enfileira para aprovação!
    is_non_interactive_tty = confirm_fn is _prompt_operator_approval and not sys.stdin.isatty()
    if active_ctx.mode != "interactive" or is_non_interactive_tty:
        approval_id, approval_path = _save_pending_approval(
            active_ctx, "restart_dcn_service", args, interlock
        )
        result = {
            "status": "PENDING_HUMAN_APPROVAL",
            "approval_id": approval_id,
            "service_name": target,
            "interlock": interlock["reason"],
            "file": str(approval_path.relative_to(PROJECT_ROOT)) if approval_path.is_relative_to(PROJECT_ROOT) else str(approval_path),
            "message": (
                "Autonomous execution blocked in non-interactive mode. Request queued "
                f"({approval_id}) pending explicit operator approval."
            ),
        }
        append_audit_log(active_ctx, "restart_dcn_service", args, "PENDING_HUMAN_APPROVAL", interlock, result)
        return result

    approved, human_note = confirm_fn("restart_dcn_service", args, interlock)
    if not approved:
        result = {
            "status": "DENIED_BY_HUMAN",
            "service_name": target,
            "reason": human_note,
        }
        append_audit_log(active_ctx, "restart_dcn_service", args, "DENIED_BY_HUMAN", interlock, result)
        return result

    # Revalidação do intertravamento imediatamente ANTES da execução (evita janela
    # TOCTOU caso a máquina tenha entrado em WELDING enquanto o humano pensava!)
    recheck = evaluate_restart_interlock(
        target,
        live_state_fn=live_state_fn,
        docker_status_fn=docker_status_fn,
    )
    if not recheck["passed"]:
        result = {
            "status": "BLOCKED_BY_INTERLOCK_RECHECK",
            "service_name": target,
            "policy": recheck["policy"],
            "reason": (
                "Physical state changed while awaiting human approval! "
                + recheck["reason"]
            ),
            "live_snapshot": recheck["live_snapshot"],
        }
        append_audit_log(active_ctx, "restart_dcn_service", args, "BLOCKED_BY_INTERLOCK_RECHECK", recheck, result)
        return result

    # --- EXECUÇÃO CONTROLADA (sem shell=True) ---
    try:
        proc = docker_exec_fn(target)
    except subprocess.TimeoutExpired as exc:
        result = {
            "status": "TIMEOUT_REACHED",
            "service_name": target,
            "timeout_sec": exc.timeout,
            "error": (
                f"Timeout of {exc.timeout}s reached while waiting for Docker Swarm "
                f"convergence on `{target}`. Rollout may still be in progress — verify via `get_docker_status`."
            ),
        }
        append_audit_log(active_ctx, "restart_dcn_service", args, "TIMEOUT_REACHED", recheck, result)
        return result
    except Exception as exc:
        result = {"status": "FAILED", "service_name": target, "error": str(exc)}
        append_audit_log(active_ctx, "restart_dcn_service", args, "FAILED", recheck, result)
        return result

    if proc.returncode != 0:
        result = {
            "status": "FAILED",
            "service_name": target,
            "returncode": proc.returncode,
            "stderr": proc.stderr.strip(),
        }
        append_audit_log(active_ctx, "restart_dcn_service", args, "FAILED", recheck, result)
        return result

    result = {
        "status": "EXECUTED",
        "service_name": target,
        "approved_note": human_note,
        "stdout": proc.stdout.strip()[-300:],
    }
    append_audit_log(active_ctx, "restart_dcn_service", args, "EXECUTED", recheck, result)
    return result


# ---------------------------------------------------------------------------
# Contratos JSON (Schemas OpenAI-compatible) das Tools de Ação
# ---------------------------------------------------------------------------
ACTION_TOOL_SCHEMAS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "create_maintenance_ticket",
            "description": (
                "Creates a structured maintenance work order (ticket) in `ACP/work_orders/` "
                "with severity, affected component, evidence-based confidence level "
                "(LOW/MEDIUM/HIGH), verified evidence list, and recommended actions. "
                "Low-risk non-actuating action. Use when requested by the operator or when "
                "a confirmed anomaly requires maintenance intervention."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "severity": {
                        "type": "string",
                        "enum": list(VALID_SEVERITIES),
                        "description": "Operational severity of the incident.",
                    },
                    "component": {
                        "type": "string",
                        "description": "Affected physical component or service (e.g. 'WorkBase', 'SuctionShield', 'Conveyor', 'OpenPLC', 'Node-RED').",
                    },
                    "title": {
                        "type": "string",
                        "description": "Short, objective work order title.",
                    },
                    "diagnosis_summary": {
                        "type": "string",
                        "description": "Technical summary of the probable root cause and process impact.",
                    },
                    "confidence": {
                        "type": "string",
                        "enum": list(VALID_CONFIDENCES),
                        "description": "Evidence-grounded confidence level (HIGH = isolated by actuator/telemetry; MEDIUM = strong hypothesis pending physical check; LOW = partial evidence).",
                    },
                    "confidence_rationale": {
                        "type": "string",
                        "description": "Short justification for the confidence level and what requires physical confirmation.",
                    },
                    "evidence": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of verified facts and measurements collected from read-only tools.",
                    },
                    "recommended_actions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Actionable steps for maintenance or operations personnel.",
                    },
                },
                "required": [
                    "severity",
                    "component",
                    "title",
                    "diagnosis_summary",
                    "confidence",
                    "confidence_rationale",
                    "evidence",
                    "recommended_actions",
                ],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "restart_dcn_service",
            "description": (
                "Requests a controlled restart (`docker service update --force`) of an "
                "allowlisted DCN Swarm service (`dcn-ws_nodered` or `dcn-ws_openplc`). "
                "HIGH-RISK ACTION protected by deterministic OT safety interlocks (blocks "
                "OpenPLC restart while welding/Burner is active or state is unverifiable) "
                "and mandatory Human-in-the-Loop approval. Use ONLY when container failure "
                "is confirmed or when explicitly requested by the operator."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "service_name": {
                        "type": "string",
                        "enum": sorted(ALLOWED_RESTART_SERVICES),
                        "description": "Exact Docker Swarm service name to restart.",
                    },
                    "reason": {
                        "type": "string",
                        "description": "Objective technical justification for the restart.",
                    },
                },
                "required": ["service_name", "reason"],
            },
        },
    },
]

ACTION_TOOL_REGISTRY = {
    "create_maintenance_ticket": create_maintenance_ticket,
    "restart_dcn_service": restart_dcn_service,
}


# ---------------------------------------------------------------------------
# Self-test determinístico das 4 barreiras de segurança (sem Docker e sem LLM!)
# ---------------------------------------------------------------------------
def _run_safety_self_test() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        interactive_ctx = ActionContext(
            mode="interactive",
            actor="test_agent",
            work_orders_dir=tmp_dir / "work_orders",
            pending_approvals_dir=tmp_dir / "pending_approvals",
            audit_log_path=tmp_dir / "audit_log.jsonl",
        )
        autonomous_ctx = ActionContext(
            mode="autonomous",
            actor="test_watcher",
            work_orders_dir=tmp_dir / "work_orders",
            pending_approvals_dir=tmp_dir / "pending_approvals",
            audit_log_path=tmp_dir / "audit_log.jsonl",
        )

        # Snapshots físicos simulados
        live_welding = lambda: {"machine_state": 2, "machine_state_name": "WELDING", "actuators": {"Burner": True}}
        live_safe = lambda: {"machine_state": 0, "machine_state_name": "WAITING_FOR_PIECE", "actuators": {"Burner": False}}
        live_offline = lambda: {"error": "Failed to read OPC UA server", "details": "Errno 111"}
        docker_running = lambda tail=0: {"services": [{"name": "dcn-ws_openplc", "replicas": "1/1"}]}
        docker_down = lambda tail=0: {"services": [{"name": "dcn-ws_openplc", "replicas": "0/1"}]}

        approve_yes = lambda tool, args, il: (True, "Approved in unit test")
        approve_no = lambda tool, args, il: (False, "Denied in unit test")
        fake_docker_ok = lambda svc: subprocess.CompletedProcess(["docker"], 0, f"service {svc} updated", "")

        def fake_docker_timeout(svc: str) -> subprocess.CompletedProcess:
            raise subprocess.TimeoutExpired(
                cmd=["docker", "service", "update", "--force", svc],
                timeout=DOCKER_RESTART_TIMEOUT_SEC,
            )

        # 1. Barreira Allowlist
        r1 = restart_dcn_service("evil_container; rm -rf /", "injection test", ctx=interactive_ctx)
        assert r1["status"] == "BLOCKED_BY_ALLOWLIST", r1
        print("  [1/8] Allowlist blocked non-allowlisted service                (PASS)")

        # 2. Hard Interlock durante soldagem (Burner=True / State=2)
        r2 = restart_dcn_service(
            "dcn-ws_openplc", "welding test", ctx=interactive_ctx,
            live_state_fn=live_welding, confirm_fn=approve_yes, docker_exec_fn=fake_docker_ok,
        )
        assert r2["status"] == "BLOCKED_BY_INTERLOCK", r2
        print("  [2/8] Hard Interlock blocked OpenPLC restart during WELDING    (PASS)")

        # 3. Fail-closed quando OPC UA offline mas OpenPLC ainda 1/1
        r3 = restart_dcn_service(
            "dcn-ws_openplc", "opcua offline test", ctx=interactive_ctx,
            live_state_fn=live_offline, docker_status_fn=docker_running,
            confirm_fn=approve_yes, docker_exec_fn=fake_docker_ok,
        )
        assert r3["status"] == "BLOCKED_BY_INTERLOCK" and r3["policy"] == "FAIL_CLOSED_UNVERIFIABLE_STATE", r3
        print("  [3/8] Fail-Closed interlock blocked OPC UA offline + 1/1       (PASS)")

        # 4. Human-in-the-Loop rejeitando ação segura
        r4 = restart_dcn_service(
            "dcn-ws_openplc", "maintenance test", ctx=interactive_ctx,
            live_state_fn=live_safe, confirm_fn=approve_no, docker_exec_fn=fake_docker_ok,
        )
        assert r4["status"] == "DENIED_BY_HUMAN", r4
        print("  [4/8] Human-in-the-Loop blocked when operator rejected         (PASS)")

        # 5. Proteção TOCTOU: seguro na proposta, entrou em WELDING antes da execução!
        states_seq = iter([live_safe(), live_welding()])
        r5 = restart_dcn_service(
            "dcn-ws_openplc", "toctou test", ctx=interactive_ctx,
            live_state_fn=lambda: next(states_seq),
            confirm_fn=approve_yes, docker_exec_fn=fake_docker_ok,
        )
        assert r5["status"] == "BLOCKED_BY_INTERLOCK_RECHECK", r5
        print("  [5/8] TOCTOU recheck blocked state transition to WELDING       (PASS)")

        # 6. Modo autônomo (Watcher) propondo ação de alto risco -> PENDING_HUMAN_APPROVAL
        r6 = restart_dcn_service(
            "dcn-ws_openplc", "container 0/1", ctx=autonomous_ctx,
            live_state_fn=live_offline, docker_status_fn=docker_down,
            docker_exec_fn=fake_docker_ok,
        )
        assert r6["status"] == "PENDING_HUMAN_APPROVAL", r6
        assert len(list(autonomous_ctx.pending_approvals_dir.glob("*.json"))) == 1
        print("  [6/8] Autonomous mode queued PENDING_HUMAN_APPROVAL            (PASS)")

        # 7. Timeout de convergência Docker Swarm -> TIMEOUT_REACHED (distinto de FAILED)
        r7 = restart_dcn_service(
            "dcn-ws_nodered", "swarm timeout test", ctx=interactive_ctx,
            live_state_fn=live_safe, confirm_fn=approve_yes, docker_exec_fn=fake_docker_timeout,
        )
        assert r7["status"] == "TIMEOUT_REACHED" and r7["timeout_sec"] == DOCKER_RESTART_TIMEOUT_SEC, r7
        print("  [7/8] Swarm convergence timeout classified as TIMEOUT_REACHED  (PASS)")

        # 8. Ticket de manutenção (baixo risco) + validação da trilha de auditoria
        r8 = create_maintenance_ticket(
            severity="HIGH",
            component="WorkBase",
            title="Pneumatic degradation on WorkBase stroke",
            diagnosis_summary="Base_Up and Base_Down +70% slower; Shield nominal.",
            confidence="HIGH",
            confidence_rationale="actuator_timings isolated Base_Up/Base_Down (+70%) vs Shield (0%).",
            evidence=["Base_Up: 8.54s vs 5.02s", "Shield_Down: 5.02s (0%)"],
            recommended_actions=["Inspect WorkBase pneumatic line and cylinder seals"],
            ctx=autonomous_ctx,
        )
        assert r8["status"] == "CREATED", r8
        assert len(list(autonomous_ctx.work_orders_dir.glob("*.json"))) == 1

        audit_lines = autonomous_ctx.audit_log_path.read_text(encoding="utf-8").strip().splitlines()
        assert len(audit_lines) == 8, f"Expected 8 audit entries, got {len(audit_lines)}"
        print("  [8/8] Work order JSON created + 8/8 audit entries logged       (PASS)")

    print("\nAll 8 OT Defense-in-Depth safety checks passed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="O-PAS ACP Safe OT Actions Engine")
    parser.add_argument(
        "--test-safety",
        action="store_true",
        help="Run deterministic unit self-test for all 4 OT safety layers (no Docker/LLM required)",
    )
    args = parser.parse_args()

    if args.test_safety:
        _run_safety_self_test()
    else:
        parser.print_help()
