"""
Ferramentas (Tools) da camada ACP (Advanced Computing Platform) para o AI Agent.

Em sistemas de agentes de IA, uma "tool" tem sempre DUAS partes:
  1. A função Python real — que consulta o sistema (OPC UA, SQLite, Docker, arquivos).
  2. O Schema JSON (TOOL_SCHEMAS) — o "cardápio" que enviamos à LLM explicando
     o nome da função, para que ela serve e quais parâmetros aceita.

A LLM NUNCA executa código diretamente no sistema OT: ela apenas *pede* para
chamar uma tool pelo nome + argumentos em JSON, e o nosso código Python
(`execute_tool`) valida, executa e devolve o resultado estruturado.

Teste direto sem LLM:
    ./.venv/bin/python ACP/tools.py
"""

import asyncio
import json
import re
import sqlite3
import subprocess
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from asyncua import Client

# ---------------------------------------------------------------------------
# Caminhos e constantes do sistema
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "OCF" / "data" / "historian.db"
PLC_XML_PATH = PROJECT_ROOT / "DCN" / "Controller" / "plc.xml"
INCIDENTS_DIR = PROJECT_ROOT / "ACP" / "incidents"

DOCKER_STACK_NAME = "dcn-ws"

OPCUA_ENDPOINT = "opc.tcp://localhost:4840/OPCUA/SimulationServer"
NAMESPACE_URI = "urn:opas:welding-station"
DCN_ID = "DCN-WS"

ACTUATOR_TAGS = [
    "Conveyor",
    "Base_Up",
    "Base_Rotate",
    "Base_Down",
    "Shield_Down",
    "Shield_Up",
    "Burner",
]

SENSOR_TAGS = [
    "Piece_Pos_Sensor",
    "Base_Up_Sensor",
    "Base_Down_Sensor",
    "Shield_Down_Sensor",
    "Shield_Up_Sensor",
]

STATE_NAMES = {
    0: "WAITING_FOR_PIECE",
    1: "PREPARING_TO_WELD",
    2: "WELDING",
    3: "FINISHING_PROCESS",
    4: "RELEASING_PIECE",
}


# ---------------------------------------------------------------------------
# Tool 1: Leitura ao vivo do servidor OPC UA
# ---------------------------------------------------------------------------
async def _fetch_opcua_snapshot() -> dict[str, Any]:
    """Conecta no servidor OPC UA, lê todas as 13 tags em lote e desconecta."""
    all_tags = ["Machine_State", *ACTUATOR_TAGS, *SENSOR_TAGS]

    # Timeout curto (3 s) para a tool não travar o agente se o servidor estiver fora
    async with Client(url=OPCUA_ENDPOINT, timeout=3) as client:
        ns_idx = await client.get_namespace_index(NAMESPACE_URI)
        nodes = [client.get_node(f"ns={ns_idx};s={DCN_ID}/{tag}") for tag in all_tags]
        values = await client.read_values(nodes)

    data = dict(zip(all_tags, values))
    state_code = int(data["Machine_State"])

    return {
        "dcn_id": DCN_ID,
        "machine_state": state_code,
        "machine_state_name": STATE_NAMES.get(state_code, f"UNKNOWN_{state_code}"),
        "actuators": {tag: bool(data[tag]) for tag in ACTUATOR_TAGS},
        "sensors": {tag: bool(data[tag]) for tag in SENSOR_TAGS},
    }


def get_live_machine_state() -> dict[str, Any]:
    """Lê o estado atual em tempo real da Welding Station via OPC UA."""
    try:
        return asyncio.run(_fetch_opcua_snapshot())
    except Exception as exc:
        # Em vez de deixar a exceção quebrar o agente, devolvemos um erro
        # estruturado — isso é uma evidência útil pro diagnóstico (ex.: OCF offline).
        return {
            "error": "Falha ao ler servidor OPC UA",
            "endpoint": OPCUA_ENDPOINT,
            "details": str(exc),
        }


# ---------------------------------------------------------------------------
# Tool 2: Estatísticas históricas por estado (Historian / SQLite)
# ---------------------------------------------------------------------------
def _open_historian_readonly() -> sqlite3.Connection:
    """Abre o SQLite em modo somente-leitura (segurança: agente não altera histórico)."""
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Banco Historian não encontrado em {DB_PATH}")
    # mode=ro garante a nível de SQLite que nenhuma query aqui consegue escrever
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row  # Permite acessar colunas por nome (row["avg_sec"])
    return conn


def _has_close_reason_column(conn: sqlite3.Connection) -> bool:
    """Verifica se o banco já possui a coluna `close_reason` (retrocompatibilidade)."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(state_cycles);").fetchall()}
    return "close_reason" in cols


def get_state_statistics() -> dict[str, Any]:
    """Retorna estatísticas (média, mín, máx, contagem) de duração de cada estado da FSM."""
    try:
        conn = _open_historian_readonly()
    except Exception as exc:
        return {"error": str(exc)}

    try:
        has_reason = _has_close_reason_column(conn)
        total_telemetry = conn.execute("SELECT COUNT(*) FROM telemetry;").fetchone()[0]
        total_cycles = conn.execute(
            "SELECT COUNT(DISTINCT cycle_id) FROM state_cycles;"
        ).fetchone()[0]

        reason_filter = (
            "AND (close_reason IS NULL OR close_reason = 'transition')"
            if has_reason
            else ""
        )
        excluded_partial = (
            conn.execute(
                """
                SELECT COUNT(*)
                  FROM state_cycles
                 WHERE close_reason IN ('shutdown', 'partial_startup');
                """
            ).fetchone()[0]
            if has_reason
            else 0
        )

        rows = conn.execute(
            f"""
            SELECT state,
                   state_name,
                   COUNT(*)                     AS samples,
                   ROUND(AVG(duration_sec), 2)  AS avg_sec,
                   ROUND(MIN(duration_sec), 2)  AS min_sec,
                   ROUND(MAX(duration_sec), 2)  AS max_sec
              FROM state_cycles
             WHERE duration_sec IS NOT NULL
               {reason_filter}
             GROUP BY state, state_name
             ORDER BY state;
            """
        ).fetchall()

        per_state = [dict(r) for r in rows]
        avg_full_cycle = round(sum(item["avg_sec"] for item in per_state), 2) if per_state else 0.0
        est_pieces_per_hour = round(3600.0 / avg_full_cycle, 1) if avg_full_cycle > 0 else 0.0

        return {
            "total_telemetry_rows": total_telemetry,
            "total_cycles_recorded": total_cycles,
            "excluded_partial_or_shutdown_states": excluded_partial,
            "estimated_avg_full_cycle_sec": avg_full_cycle,
            "estimated_throughput_pieces_per_hour": est_pieces_per_hour,
            "states": per_state,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Tool 3: Transições recentes e estado atualmente em andamento
# ---------------------------------------------------------------------------
def get_recent_cycles(limit: int = 10) -> dict[str, Any]:
    """Retorna as últimas transições de estado, incluindo tempo decorrido se houver estado aberto."""
    # Mini-guardrail: evita que a LLM peça 100.000 linhas e estoure o contexto
    safe_limit = max(1, min(int(limit), 50))

    try:
        conn = _open_historian_readonly()
    except Exception as exc:
        return {"error": str(exc)}

    try:
        has_reason = _has_close_reason_column(conn)
        reason_col = ", close_reason" if has_reason else ""

        rows = conn.execute(
            f"""
            SELECT id, cycle_id, state, state_name,
                   started_at, started_epoch, ended_at, duration_sec{reason_col}
              FROM state_cycles
             ORDER BY id DESC
             LIMIT ?;
            """,
            (safe_limit,),
        ).fetchall()

        now_epoch = time.time()
        transitions = []
        # Reverte para apresentar em ordem cronológica (mais antigo -> mais recente)
        for row in reversed(rows):
            item: dict[str, Any] = {
                "cycle_id": row["cycle_id"],
                "state": row["state"],
                "state_name": row["state_name"],
                "started_at": row["started_at"],
                "ended_at": row["ended_at"],
                "duration_sec": row["duration_sec"],
            }
            if has_reason and row["close_reason"] is not None:
                item["close_reason"] = row["close_reason"]

            # Se ended_at é None, a máquina AINDA está neste estado — calculamos
            # quanto tempo já se passou para detectar travamentos imediatamente!
            if row["ended_at"] is None:
                item["status"] = "IN_PROGRESS"
                item["elapsed_sec"] = round(now_epoch - row["started_epoch"], 2)
            else:
                item["status"] = "COMPLETED"
            transitions.append(item)

        return {
            "returned_count": len(transitions),
            "transitions": transitions,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Tool 4: Leitura limpa da lógica do PLC (DCN/Controller/plc.xml -> ST + variáveis)
# ---------------------------------------------------------------------------
def read_plc_logic() -> dict[str, Any]:
    """
    Extrai o código Structured Text (IEC 61131-3) e o mapa de variáveis do
    arquivo DCN/Controller/plc.xml.

    Boa prática de IA (Context Hygiene): em vez de jogar o XML bruto cheio de
    tags de metadados no prompt, extraímos só o que importa — economiza tokens
    e melhora o raciocínio do modelo.
    """
    if not PLC_XML_PATH.exists():
        return {"error": f"Arquivo PLC não encontrado em {PLC_XML_PATH}"}

    try:
        tree = ET.parse(PLC_XML_PATH)
        root = tree.getroot()
        ns = {
            "plc": "http://www.plcopen.org/xml/tc6_0201",
            "xhtml": "http://www.w3.org/1999/xhtml",
        }

        pou = root.find(".//plc:pou", ns)
        pou_name = pou.attrib.get("name", "unknown") if pou is not None else "unknown"

        variables = []
        for var in root.findall(".//plc:pou/plc:interface//plc:variable", ns):
            type_elem = var.find("plc:type", ns)
            var_type = "UNKNOWN"
            if type_elem is not None and len(type_elem) > 0:
                child = type_elem[0]
                tag = child.tag.split("}")[-1]
                var_type = child.attrib.get("name", tag) if tag == "derived" else tag

            variables.append(
                {
                    "name": var.attrib.get("name"),
                    "address": var.attrib.get("address", "internal"),
                    "type": var_type,
                }
            )

        st_elem = root.find(".//plc:pou/plc:body/plc:ST/xhtml:p", ns)
        st_code = (st_elem.text or "").strip() if st_elem is not None else ""

        task_elem = root.find(".//plc:task", ns)
        scan_interval = task_elem.attrib.get("interval", "unknown") if task_elem is not None else "unknown"

        return {
            "program_name": pou_name,
            "scan_interval": scan_interval,
            "variables": variables,
            "structured_text_code": st_code,
        }
    except Exception as exc:
        return {"error": f"Falha ao parsear {PLC_XML_PATH.name}: {exc}"}


# ---------------------------------------------------------------------------
# Tool 5: Saúde e logs dos serviços do DCN no Docker Swarm (somente leitura)
# ---------------------------------------------------------------------------
def _run_docker_cmd(args: list[str], timeout: float = 5.0) -> subprocess.CompletedProcess:
    """Executa um subcomando `docker` com lista fixa de argumentos (sem shell=True)."""
    return subprocess.run(
        ["docker", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def get_docker_status(tail_logs: int = 12) -> dict[str, Any]:
    """
    Inspeciona os serviços da stack `dcn-ws` no Docker Swarm (OpenPLC e Node-RED):
    réplicas ativas, imagem em execução, histórico recente de tarefas/restarts e
    as últimas linhas de log de cada serviço.
    """
    # Guardrail de contexto: limita linhas de log para não estourar tokens
    safe_tail = max(0, min(int(tail_logs), 30))

    try:
        ls_proc = _run_docker_cmd(
            [
                "service",
                "ls",
                "--filter",
                f"label=com.docker.stack.namespace={DOCKER_STACK_NAME}",
                "--format",
                "{{json .}}",
            ]
        )
    except FileNotFoundError:
        return {"error": "CLI 'docker' não encontrado na máquina."}
    except subprocess.TimeoutExpired:
        return {"error": "Timeout ao consultar Docker Swarm."}

    if ls_proc.returncode != 0:
        return {
            "error": "Falha ao consultar Docker Swarm",
            "stderr": ls_proc.stderr.strip(),
        }

    raw_lines = [line for line in ls_proc.stdout.splitlines() if line.strip()]
    if not raw_lines:
        return {
            "stack": DOCKER_STACK_NAME,
            "status": "NOT_DEPLOYED",
            "services": [],
        }

    services_info: list[dict[str, Any]] = []
    for line in raw_lines:
        svc = json.loads(line)
        svc_name = svc.get("Name", "")

        # Histórico das 5 últimas tarefas (expõe crashes, restarts e rolling updates!)
        ps_proc = _run_docker_cmd(
            ["service", "ps", svc_name, "--no-trunc", "--format", "{{json .}}"]
        )
        tasks = []
        if ps_proc.returncode == 0:
            for tline in ps_proc.stdout.splitlines()[:5]:
                if not tline.strip():
                    continue
                t = json.loads(tline)
                tasks.append(
                    {
                        "name": t.get("Name"),
                        "image": t.get("Image"),
                        "desired_state": t.get("DesiredState"),
                        "current_state": t.get("CurrentState"),
                        "error": t.get("Error") or None,
                    }
                )

        # Últimas linhas de log do serviço (Node-RED / OpenPLC)
        logs: list[str] = []
        if safe_tail > 0:
            log_proc = _run_docker_cmd(
                ["service", "logs", "--tail", str(safe_tail), "--no-trunc", svc_name]
            )
            combined = (log_proc.stdout + "\n" + log_proc.stderr).strip()
            if combined:
                logs = [ln[-220:] for ln in combined.splitlines()[-safe_tail:]]

        services_info.append(
            {
                "name": svc_name,
                "image": svc.get("Image"),
                "replicas": svc.get("Replicas"),
                "ports": svc.get("Ports"),
                "recent_tasks": tasks,
                "recent_logs": logs,
            }
        )

    return {
        "stack": DOCKER_STACK_NAME,
        "status": "DEPLOYED",
        "services": services_info,
    }


# ---------------------------------------------------------------------------
# Tool 6: Memória Episódica — Incidentes recentes salvos em ACP/incidents/
# ---------------------------------------------------------------------------
def _parse_incident_markdown(path: Path) -> dict[str, Any]:
    """Extrai cabeçalho e resumo de um relatório `.md` de incidente sem inflar contexto."""
    text = path.read_text(encoding="utf-8", errors="replace")

    kind_match = re.search(r"^#\s*Relatório de Incidente\s*[—-]\s*(.+)$", text, re.MULTILINE)
    ts_match = re.search(r"-\s*\*\*Data/Hora:\*\*\s*(.+)$", text, re.MULTILINE)
    trigger_match = re.search(r"-\s*\*\*Gatilho Determinístico:\*\*\s*(.+)$", text, re.MULTILINE)

    # Tenta capturar a seção "### Resumo" ou "**Conclusão:**"; senão usa o início do corpo
    summary_text = ""
    resumo_match = re.search(r"###\s*Resumo\s*\n+(.+?)(?:\n###|\Z)", text, re.DOTALL)
    if resumo_match:
        summary_text = resumo_match.group(1).strip()
    else:
        body = text.split("---", 1)[-1].strip()
        summary_text = body[:450] + ("..." if len(body) > 450 else "")

    return {
        "file": path.name,
        "kind": kind_match.group(1).strip() if kind_match else "unknown",
        "timestamp": ts_match.group(1).strip() if ts_match else "unknown",
        "trigger": trigger_match.group(1).strip() if trigger_match else "unknown",
        "summary": summary_text,
    }


def get_recent_incidents(limit: int = 5) -> dict[str, Any]:
    """
    Lê os relatórios de incidentes mais recentes em `ACP/incidents/` (memória
    episódica). Permite correlacionar alertas atuais com falhas passadas e
    identificar problemas recorrentes.
    """
    safe_limit = max(1, min(int(limit), 15))

    if not INCIDENTS_DIR.exists():
        return {"total_incidents_stored": 0, "incidents": []}

    files = sorted(INCIDENTS_DIR.glob("*.md"))
    selected = files[-safe_limit:]

    return {
        "total_incidents_stored": len(files),
        "returned_count": len(selected),
        "incidents": [_parse_incident_markdown(f) for f in selected],
    }


# ---------------------------------------------------------------------------
# Contratos JSON (Schemas OpenAI-compatible) + Dispatcher
# ---------------------------------------------------------------------------
# Esta lista é enviada no parâmetro `tools=` da chamada à API da Upstage/OpenAI.
# Repare como as descrições são claras: a qualidade da descrição guia diretamente
# QUANDO e COMO a LLM escolhe chamar cada ferramenta!
TOOL_SCHEMAS: list[dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "get_live_machine_state",
            "description": (
                "Lê em tempo real (via OPC UA) o estado atual da máquina de estados "
                "(Machine_State) e o valor booleano de todos os 7 atuadores e 5 sensores "
                "da Welding Station. Use para ver a fotografia de AGORA da máquina."
            ),
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_state_statistics",
            "description": (
                "Consulta o Historian (SQLite) e retorna estatísticas históricas "
                "(contagem, média, mínimo e máximo de duração em segundos) de cada estado "
                "da FSM da Welding Station, além do tempo médio de ciclo completo e "
                "vazão estimada (peças/hora). Use para linha de base, OEE e gargalos."
            ),
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_recent_cycles",
            "description": (
                "Consulta o Historian (SQLite) e retorna as transições de estado mais "
                "recentes em ordem cronológica, incluindo se há algum estado EM ANDAMENTO "
                "e há quantos segundos (elapsed_sec) ele está aberto. Use para detectar "
                "travamentos, ciclos lentos recentes ou degradação progressiva."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Quantidade de transições recentes a retornar (entre 1 e 50, padrão 10).",
                        "default": 10,
                    }
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_plc_logic",
            "description": (
                "Lê o código de controle IEC 61131-3 (Structured Text) e a tabela de "
                "variáveis/endereços do OpenPLC da Welding Station. Use para verificar "
                "quais sensores e condições destravam cada transição de estado da FSM."
            ),
            "parameters": {
                "type": "object",
                "properties": {},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_docker_status",
            "description": (
                "Inspeciona os serviços da stack DCN (`dcn-ws`: OpenPLC e Node-RED) no "
                "Docker Swarm, retornando réplicas ativas, imagem/versão, histórico de "
                "restarts/falhas de containers e as últimas linhas de log. Use quando "
                "suspeitar de queda de container, erro de comunicação Modbus/OPC UA ou "
                "impacto de rolling update."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "tail_logs": {
                        "type": "integer",
                        "description": "Quantidade de linhas finais de log por serviço (entre 0 e 30, padrão 12).",
                        "default": 12,
                    }
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_recent_incidents",
            "description": (
                "Consulta a memória episódica de incidentes já diagnosticados pelo "
                "Watcher/Agente em `ACP/incidents/`, retornando data/hora, gatilho e "
                "resumo de cada ocorrência passada. Use para verificar se um problema "
                "é recorrente ou consequência de um evento anterior recente."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Quantidade de incidentes mais recentes a retornar (entre 1 e 15, padrão 5).",
                        "default": 5,
                    }
                },
                "required": [],
            },
        },
    },
]

# Mapa nome -> função Python real (usado pelo Agent Loop quando a LLM pedir uma tool)
TOOL_REGISTRY = {
    "get_live_machine_state": get_live_machine_state,
    "get_state_statistics": get_state_statistics,
    "get_recent_cycles": get_recent_cycles,
    "read_plc_logic": read_plc_logic,
    "get_docker_status": get_docker_status,
    "get_recent_incidents": get_recent_incidents,
}


def execute_tool(name: str, arguments: dict[str, Any] | None = None) -> str:
    """
    Executa uma tool registrada pelo nome e sempre devolve uma string JSON.
    (A API OpenAI/Upstage exige que o retorno de uma mensagem `role: "tool"` seja string.)
    """
    func = TOOL_REGISTRY.get(name)
    if func is None:
        return json.dumps(
            {"error": f"Tool desconhecida: '{name}'", "available_tools": list(TOOL_REGISTRY.keys())},
            ensure_ascii=False,
        )

    try:
        kwargs = arguments or {}
        result = func(**kwargs)
        return json.dumps(result, ensure_ascii=False, indent=2)
    except Exception as exc:
        return json.dumps(
            {"error": f"Erro ao executar tool '{name}': {exc}"},
            ensure_ascii=False,
        )


# ---------------------------------------------------------------------------
# Self-test local (sem gastar nenhum token de LLM!)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    tests = [
        ("get_live_machine_state", {}),
        ("get_state_statistics", {}),
        ("get_recent_cycles", {"limit": 6}),
        ("read_plc_logic", {}),
        ("get_docker_status", {"tail_logs": 5}),
        ("get_recent_incidents", {"limit": 3}),
    ]

    for tool_name, tool_args in tests:
        print(f"\n{'=' * 70}")
        print(f"TOOL: {tool_name}({tool_args})")
        print("=" * 70)
        print(execute_tool(tool_name, tool_args))
