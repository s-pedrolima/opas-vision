"""
Watcher Determinístico da camada ACP (Advanced Computing Platform).

Princípio de IA em sistemas reais ("Determinístico vigia, Probabilístico investiga"):
  - Enquanto a Welding Station opera normal, este script faz apenas queries SQL
    baratas no `OCF/data/historian.db` a cada 2 segundos -> CUSTO ZERO de LLM!
  - Gestão de Alarmes Stateful com Histerese e Off-Delay (inspirada na norma ISA-18.2):
      * NORMAL -> ACTIVE (edge-triggered): aciona o AI Agent 1 única vez quando
        a métrica ultrapassa o limiar de disparo (`raise_threshold`).
      * SUPPRESSED enquanto persistir: evita "alarm flooding" / queima de tokens
        repetida para a mesma falha ainda aberta.
      * ESCALATED (opcional): re-aciona o agente apenas se a condição piorar
        significativamente (ex.: +30% acima do patamar que gerou o alerta).
      * ACTIVE -> NORMAL (histerese + off-delay): só fecha e rearma o alarme quando
        a métrica permanecer abaixo do `clear_threshold` pelo tempo `off_delay_sec`,
        eliminando *chattering* (oscilação de alarmes em torno do limiar).

Como rodar:
    # Modo normal (chama a LLM quando detectar anomalia):
    ./.venv/bin/python ACP/watcher.py

    # Modo dry-run (detecta anomalias e transições de alarme SEM chamar a LLM):
    ./.venv/bin/python ACP/watcher.py --dry-run

    # Self-test unitário da máquina de estados de alarmes (sem banco e sem LLM):
    ./.venv/bin/python ACP/watcher.py --test-alarms
"""

import argparse
import logging
import sqlite3
import statistics
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

# Permite rodar tanto `python ACP/watcher.py` quanto `python -m ACP.watcher`
try:
    from ACP.actions import ActionContext
    from ACP.agent import SYSTEM_PROMPT, build_client, run_agent_turn
except ImportError:
    from actions import ActionContext
    from agent import SYSTEM_PROMPT, build_client, run_agent_turn

# ---------------------------------------------------------------------------
# Configurações e limiares determinísticos (baseados na física + linha de base)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "OCF" / "data" / "historian.db"
INCIDENTS_DIR = PROJECT_ROOT / "ACP" / "incidents"

CHECK_INTERVAL_SEC = 2.0  # Frequência de verificação no SQLite

# Regra 1 — Timeout de estado aberto (TRAVAMENTO real, distinto de degradação):
#   - Estado 0: regime permanente ~4.5s, mas 1ª peça (cold start X=0->50cm) = 10s -> 16s
#   - Estados 1 e 3: nominal ~5.0s; degradação severa (factor=0.4) ~12.5s -> 18s separa
#     claramente "movimento degradado" (slow_degradation) de "mecanismo travado" (timeout)
#   - Estado 2: timer fixo T_Weld = 10.0s -> 15s
#   - Estado 4: ~0.5s nominal -> 5s
STATE_TIMEOUT_SEC = {
    0: 16.0,  # WAITING_FOR_PIECE  (ex.: esteira parada ou sensor de peça falhando)
    1: 18.0,  # PREPARING_TO_WELD  (ex.: Base_Up_Sensor ou Shield_Down_Sensor travado)
    2: 15.0,  # WELDING            (ex.: timer T_Weld ou OpenPLC travado)
    3: 18.0,  # FINISHING_PROCESS  (ex.: Base_Down_Sensor ou Shield_Up_Sensor travado)
    4: 5.0,   # RELEASING_PIECE    (ex.: Piece_Pos_Sensor colado em True)
}

# Off-delay (ISA-18.2) para `state_timeout`: exige 45s contínuos (~1–2 ciclos limpos)
# sem novo timeout antes de rearmar o alarme, impedindo chattering se um estado
# muito lento ultrapassar o limiar em todo ciclo e encerrar logo depois.
STATE_TIMEOUT_OFF_DELAY_SEC = 45.0

# Regra 2 — Degradação lenta com histerese (ex.: vazamento pneumático):
#   - Dispara (RAISE) quando mediana dos últimos N ciclos > 6.75s (+35% sobre 5.0s)
#   - Normaliza (CLEAR) apenas quando mediana <= 5.75s (+15% sobre 5.0s)
#   - Banda morta [5.75s .. 6.75s] evita "chattering" (oscilação de alarmes)
#   - Escala (ESCALATED) se piorar >= +30% em relação ao último patamar notificado
DEGRADATION_STATES = (1, 3)
DEGRADATION_WINDOW = 3
DEGRADATION_BASELINE_SEC = 5.0
DEGRADATION_RAISE_FACTOR = 1.35  # > 6.75s -> ACTIVE
DEGRADATION_CLEAR_FACTOR = 1.15  # <= 5.75s -> RESOLVED (histerese)
DEGRADATION_ESCALATION_FACTOR = 1.30  # +30% pior que o alerta anterior -> ESCALATED
ESCALATION_MIN_INTERVAL_SEC = 120.0   # Intervalo mínimo entre escalações

# Regra 3 — Telemetria congelada com histerese (Historian heartbeat = 10s):
STALE_TELEMETRY_RAISE_SEC = 22.0  # > 22s sem nova linha -> ACTIVE
STALE_TELEMETRY_CLEAR_SEC = 12.0  # <= 12s (heartbeat voltou) -> RESOLVED


# ---------------------------------------------------------------------------
# Estruturas da Gestão de Alarmes Stateful (ISA-18.2)
# ---------------------------------------------------------------------------
@dataclass
class AlarmEvaluation:
    """Resultado determinístico da avaliação de um canal de alarme em um ciclo de leitura."""
    alarm_id: str           # Identificador único do canal (ex.: "slow_degradation:state_1")
    kind: str               # Categoria ("state_timeout", "slow_degradation", "stale_telemetry")
    metric: float           # Valor numérico observado agora (segundos)
    raise_threshold: float  # Limiar para abrir alarme (NORMAL -> ACTIVE)
    clear_threshold: float  # Limiar para fechar alarme (ACTIVE -> NORMAL, histerese)
    summary: str            # Descrição curta para log / cabeçalho do incidente
    prompt: str             # Evento factual enviado ao AI Agent se disparar
    allow_escalation: bool = False  # Permite re-alerta apenas se piorar >= +30%
    off_delay_sec: float = 0.0      # Tempo contínuo abaixo do clear_threshold p/ resolver


@dataclass
class ActiveAlarm:
    """Estado em memória de um alarme atualmente ATIVO."""
    alarm_id: str
    kind: str
    opened_at: float
    last_notified_at: float
    last_notified_metric: float
    escalations: int = 0
    clear_since: float | None = None  # Início da janela de confirmação (off-delay)


@dataclass
class AlarmTransition:
    """Transição produzida pelo AlarmManager ao avaliar um canal."""
    action: str  # "NONE" | "RAISED" | "SUPPRESSED" | "ESCALATED" | "RESOLVED"
    evaluation: AlarmEvaluation
    duration_sec: float | None = None
    previous_metric: float | None = None
    escalation_count: int = 0


class AlarmManager:
    """
    Máquina de estados de alarmes (edge-triggered com histerese, off-delay e escalação).

    Evita tempestades de alertas (alarm flooding), chattering em limiares e queima
    repetida de tokens quando uma falha permanece ativa por minutos ou horas.
    """

    def __init__(
        self,
        escalation_factor: float = DEGRADATION_ESCALATION_FACTOR,
        escalation_min_interval: float = ESCALATION_MIN_INTERVAL_SEC,
    ) -> None:
        self.active: dict[str, ActiveAlarm] = {}
        self.escalation_factor = escalation_factor
        self.escalation_min_interval = escalation_min_interval

    def process(self, ev: AlarmEvaluation, now_epoch: float) -> AlarmTransition:
        current = self.active.get(ev.alarm_id)

        # Estado atual: NORMAL
        if current is None:
            if ev.metric > ev.raise_threshold:
                self.active[ev.alarm_id] = ActiveAlarm(
                    alarm_id=ev.alarm_id,
                    kind=ev.kind,
                    opened_at=now_epoch,
                    last_notified_at=now_epoch,
                    last_notified_metric=ev.metric,
                )
                return AlarmTransition(action="RAISED", evaluation=ev)
            return AlarmTransition(action="NONE", evaluation=ev)

        # Estado atual: ACTIVE -> verifica se normalizou (histerese + off-delay)
        if ev.metric <= ev.clear_threshold:
            if ev.off_delay_sec <= 0.0:
                del self.active[ev.alarm_id]
                duration = round(max(0.0, now_epoch - current.opened_at), 1)
                return AlarmTransition(
                    action="RESOLVED",
                    evaluation=ev,
                    duration_sec=duration,
                    previous_metric=current.last_notified_metric,
                )

            # Off-delay ativo: exige estabilidade contínua abaixo do clear_threshold
            if current.clear_since is None:
                current.clear_since = now_epoch
                return AlarmTransition(action="SUPPRESSED", evaluation=ev)

            if (now_epoch - current.clear_since) >= ev.off_delay_sec:
                del self.active[ev.alarm_id]
                duration = round(max(0.0, now_epoch - current.opened_at), 1)
                return AlarmTransition(
                    action="RESOLVED",
                    evaluation=ev,
                    duration_sec=duration,
                    previous_metric=current.last_notified_metric,
                )
            return AlarmTransition(action="SUPPRESSED", evaluation=ev)

        # Métrica voltou acima do clear_threshold -> cancela qualquer contagem de off-delay
        current.clear_since = None

        # Estado atual: ACTIVE -> verifica se houve piora relevante (escalação)
        if (
            ev.allow_escalation
            and current.last_notified_metric > 0
            and ev.metric >= current.last_notified_metric * self.escalation_factor
            and (now_epoch - current.last_notified_at) >= self.escalation_min_interval
        ):
            prev_metric = current.last_notified_metric
            current.last_notified_metric = ev.metric
            current.last_notified_at = now_epoch
            current.escalations += 1
            return AlarmTransition(
                action="ESCALATED",
                evaluation=ev,
                duration_sec=round(max(0.0, now_epoch - current.opened_at), 1),
                previous_metric=prev_metric,
                escalation_count=current.escalations,
            )

        # Continua ativo no mesmo patamar (ou dentro da banda morta) -> silenciado!
        return AlarmTransition(action="SUPPRESSED", evaluation=ev)


def _open_db_readonly() -> sqlite3.Connection | None:
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Avaliadores determinísticos (emitem estado atual de CADA canal monitorado)
# ---------------------------------------------------------------------------
def evaluate_state_timeouts(conn: sqlite3.Connection, now_epoch: float) -> list[AlarmEvaluation]:
    """
    Regra 1: Avalia todos os estados da FSM quanto a travamento (tempo aberto).
    Usa `clear_threshold = threshold * 0.8` + `off_delay_sec = 45s` para evitar
    chattering quando um estado muito degradado ultrapassa o limite por poucos
    segundos e conclui o ciclo repetidamente.
    """
    row = conn.execute(
        """
        SELECT id, cycle_id, state, state_name, started_at, started_epoch
          FROM state_cycles
         WHERE ended_at IS NULL
         ORDER BY id DESC
         LIMIT 1;
        """
    ).fetchone()

    open_state = int(row["state"]) if row is not None else None
    evaluations: list[AlarmEvaluation] = []

    for state, threshold in STATE_TIMEOUT_SEC.items():
        alarm_id = f"state_timeout:state_{state}"
        clear_th = round(threshold * 0.8, 2)
        if row is not None and state == open_state:
            elapsed = round(max(0.0, now_epoch - float(row["started_epoch"])), 2)
            state_name = row["state_name"]
            evaluations.append(
                AlarmEvaluation(
                    alarm_id=alarm_id,
                    kind="state_timeout",
                    metric=elapsed,
                    raise_threshold=threshold,
                    clear_threshold=clear_th,
                    summary=(
                        f"STATE TIMEOUT in Cycle #{row['cycle_id']} — "
                        f"State {state} ({state_name}) open for {elapsed:.1f}s "
                        f"(limit: {threshold:.0f}s)"
                    ),
                    prompt=(
                        f"EVENTO DO WATCHER (state_timeout): O Estado {state} ({state_name}) "
                        f"no Ciclo #{row['cycle_id']} atingiu {elapsed:.1f} segundos aberto "
                        f"(limiar configurado: {threshold:.0f}s, início em {row['started_at']}).\n"
                        f"Verifique o estado ao vivo no OPC UA, os ciclos recentes no Historian, "
                        f"a saúde do Docker Swarm, o histórico de incidentes e a lógica do PLC. "
                        f"Confirme se o travamento ainda persiste ou se já foi normalizado (evento "
                        f"transitório) e apresente o diagnóstico técnico e recomendações."
                    ),
                    allow_escalation=False,
                    off_delay_sec=STATE_TIMEOUT_OFF_DELAY_SEC,
                )
            )
        else:
            evaluations.append(
                AlarmEvaluation(
                    alarm_id=alarm_id,
                    kind="state_timeout",
                    metric=0.0,
                    raise_threshold=threshold,
                    clear_threshold=clear_th,
                    summary=f"State {state} operating within timeout limits.",
                    prompt="",
                    allow_escalation=False,
                    off_delay_sec=STATE_TIMEOUT_OFF_DELAY_SEC,
                )
            )

    return evaluations


def evaluate_slow_degradation(conn: sqlite3.Connection) -> list[AlarmEvaluation]:
    """
    Regra 2: Avalia degradação sustentada em TODOS os estados de movimentação
    usando a mediana dos últimos N ciclos concluídos (com histerese).
    """
    cols = {r[1] for r in conn.execute("PRAGMA table_info(state_cycles);").fetchall()}
    reason_filter = (
        "AND (close_reason IS NULL OR close_reason = 'transition')"
        if "close_reason" in cols
        else ""
    )

    raise_threshold = round(DEGRADATION_BASELINE_SEC * DEGRADATION_RAISE_FACTOR, 2)
    clear_threshold = round(DEGRADATION_BASELINE_SEC * DEGRADATION_CLEAR_FACTOR, 2)
    evaluations: list[AlarmEvaluation] = []

    for state in DEGRADATION_STATES:
        rows = conn.execute(
            f"""
            SELECT id, cycle_id, state_name, duration_sec
              FROM state_cycles
             WHERE state = ?
               AND duration_sec IS NOT NULL
               {reason_filter}
             ORDER BY id DESC
             LIMIT ?;
            """,
            (state, DEGRADATION_WINDOW),
        ).fetchall()

        if len(rows) < DEGRADATION_WINDOW:
            continue

        durations = [round(float(r["duration_sec"]), 2) for r in reversed(rows)]
        recent_median = round(statistics.median(durations), 2)
        state_name = rows[0]["state_name"]

        evaluations.append(
            AlarmEvaluation(
                alarm_id=f"slow_degradation:state_{state}",
                kind="slow_degradation",
                metric=recent_median,
                raise_threshold=raise_threshold,
                clear_threshold=clear_threshold,
                summary=(
                    f"SUSTAINED DEGRADATION in State {state} ({state_name}): "
                    f"last {DEGRADATION_WINDOW}-cycle median = {recent_median:.2f}s "
                    f"(values: {durations}, nominal ~{DEGRADATION_BASELINE_SEC:.1f}s)"
                ),
                prompt=(
                    f"EVENTO PREDITIVO DO WATCHER (slow_degradation): A mediana das "
                    f"durações dos últimos {DEGRADATION_WINDOW} ciclos concluídos do "
                    f"Estado {state} ({state_name}) está em {recent_median:.2f}s "
                    f"(durações: {durations} s; nominal ~{DEGRADATION_BASELINE_SEC:.1f}s).\n"
                    f"Consulte o Historian (incluindo estatísticas por estado e tempos "
                    f"individuais de atuadores), o estado ao vivo, incidentes recentes, "
                    f"saúde do Docker e o código PLC para diagnosticar a causa raiz e "
                    f"estimar o impacto no throughput."
                ),
                allow_escalation=True,
                off_delay_sec=0.0,
            )
        )

    return evaluations


def evaluate_stale_telemetry(conn: sqlite3.Connection, now_epoch: float) -> list[AlarmEvaluation]:
    """Regra 3: Avalia frescor da telemetria no Historian (com histerese)."""
    row = conn.execute(
        "SELECT id, ts, ts_epoch FROM telemetry ORDER BY id DESC LIMIT 1;"
    ).fetchone()

    if row is None:
        return []

    age_sec = round(max(0.0, now_epoch - float(row["ts_epoch"])), 1)
    return [
        AlarmEvaluation(
            alarm_id="stale_telemetry",
            kind="stale_telemetry",
            metric=age_sec,
            raise_threshold=STALE_TELEMETRY_RAISE_SEC,
            clear_threshold=STALE_TELEMETRY_CLEAR_SEC,
            summary=(
                f"STALE TELEMETRY: last Historian sample was {age_sec:.1f}s ago "
                f"(at {row['ts']}, limit: {STALE_TELEMETRY_RAISE_SEC:.0f}s)"
            ),
            prompt=(
                f"EVENTO DO WATCHER (stale_telemetry): O Historian não registra nenhuma "
                f"nova amostra nem heartbeat há {age_sec:.1f} segundos (última leitura em "
                f"{row['ts']}).\n"
                f"Verifique o estado ao vivo no OPC UA, a saúde/logs dos containers no "
                f"Docker Swarm, os últimos ciclos no Historian e incidentes recentes para "
                f"identificar em qual camada O-PAS ocorreu a interrupção."
            ),
            allow_escalation=False,
            off_delay_sec=0.0,
        )
    ]


# ---------------------------------------------------------------------------
# Persistência do relatório gerado pelo Agente
# ---------------------------------------------------------------------------
def save_incident_report(
    transition: AlarmTransition,
    model: str,
    diagnosis: str,
) -> Path:
    """Salva o diagnóstico gerado pela LLM em ACP/incidents/<timestamp>_<tipo>.md."""
    ev = transition.evaluation
    INCIDENTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = f"{ev.kind}_escalated" if transition.action == "ESCALATED" else ev.kind
    report_path = INCIDENTS_DIR / f"{stamp}_{suffix}.md"

    event_label = (
        f"ESCALATED #{transition.escalation_count} "
        f"({transition.previous_metric:.2f}s -> {ev.metric:.2f}s)"
        if transition.action == "ESCALATED" and transition.previous_metric is not None
        else "RAISED"
    )

    content = (
        f"# Incident Report — {ev.kind}\n\n"
        f"- **Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"- **Alarm Channel:** `{ev.alarm_id}` ({event_label})\n"
        f"- **LLM Model:** `{model}`\n"
        f"- **Deterministic Trigger:** {ev.summary}\n\n"
        f"---\n\n"
        f"{diagnosis}\n"
    )
    report_path.write_text(content, encoding="utf-8")
    return report_path


# ---------------------------------------------------------------------------
# Loop principal do Watcher
# ---------------------------------------------------------------------------
def run_watcher(dry_run: bool = False, verbose: bool = False) -> None:
    client = None
    model = "dry-run"
    if not dry_run:
        client, model = build_client()

    raise_deg = DEGRADATION_BASELINE_SEC * DEGRADATION_RAISE_FACTOR
    clear_deg = DEGRADATION_BASELINE_SEC * DEGRADATION_CLEAR_FACTOR

    logging.info("ACP Watcher started (ISA-18.2 Stateful) | mode=%s | db=%s", "DRY-RUN" if dry_run else model, DB_PATH)
    logging.info(
        "Thresholds: timeouts=%s (off-delay=%.0fs) | degradation RAISE>%.2fs / CLEAR<=%.2fs (escalation +%.0f%%) | telemetry RAISE>%.0fs / CLEAR<=%.0fs",
        STATE_TIMEOUT_SEC,
        STATE_TIMEOUT_OFF_DELAY_SEC,
        raise_deg,
        clear_deg,
        (DEGRADATION_ESCALATION_FACTOR - 1.0) * 100.0,
        STALE_TELEMETRY_RAISE_SEC,
        STALE_TELEMETRY_CLEAR_SEC,
    )

    alarm_mgr = AlarmManager()

    # Aguarda ver dados frescos ao menos uma vez antes de avaliar telemetria/degradação
    # (evita falso alarme na subida inicial antes do Historian começar a gravar)
    saw_fresh_telemetry = False

    while True:
        conn = _open_db_readonly()
        if conn is None:
            logging.info("Waiting for Historian database at %s...", DB_PATH)
            time.sleep(CHECK_INTERVAL_SEC)
            continue

        try:
            now_epoch = time.time()

            latest = conn.execute("SELECT MAX(ts_epoch) FROM telemetry;").fetchone()[0]
            if latest is not None and (now_epoch - float(latest)) <= STALE_TELEMETRY_RAISE_SEC:
                saw_fresh_telemetry = True

            evaluations: list[AlarmEvaluation] = []
            if saw_fresh_telemetry:
                evaluations.extend(evaluate_stale_telemetry(conn, now_epoch))

            evaluations.extend(evaluate_state_timeouts(conn, now_epoch))

            if saw_fresh_telemetry:
                evaluations.extend(evaluate_slow_degradation(conn))
        finally:
            conn.close()

        for ev in evaluations:
            transition = alarm_mgr.process(ev, now_epoch)

            if transition.action in ("NONE", "SUPPRESSED"):
                continue

            if transition.action == "RESOLVED":
                logging.info(
                    "[RESOLVED] Alarm cleared [%s]: metric=%.2fs (<= %.2fs) | active_duration=%.1fs",
                    ev.alarm_id,
                    ev.metric,
                    ev.clear_threshold,
                    transition.duration_sec or 0.0,
                )
                continue

            # RAISED ou ESCALATED -> aciona o AI Agent!
            prompt_to_send = ev.prompt
            if transition.action == "ESCALATED":
                logging.warning(
                    "[ESCALATED #%d] [%s] (%.2fs -> %.2fs): %s",
                    transition.escalation_count,
                    ev.alarm_id,
                    transition.previous_metric or 0.0,
                    ev.metric,
                    ev.summary,
                )
                prompt_to_send = (
                    f"[ESCALAÇÃO #{transition.escalation_count}: alarme `{ev.alarm_id}` "
                    f"já ativo há {transition.duration_sec:.0f}s agravou de "
                    f"{transition.previous_metric:.2f}s para {ev.metric:.2f}s]\n\n"
                    f"{ev.prompt}"
                )
            else:
                logging.warning("[ALARM RAISED] [%s]: %s", ev.alarm_id, ev.summary)

            if dry_run:
                logging.info("[DRY-RUN] Prompt that would be sent to agent:\n%s", prompt_to_send)
                continue

            logging.info("Triggering AI Agent (%s) for root-cause diagnosis...", model)
            # Cada incidente começa com uma conversa limpa (evita contaminar com incidentes antigos)
            messages: list[dict[str, Any]] = [{"role": "system", "content": SYSTEM_PROMPT}]
            diagnosis = run_agent_turn(
                client,
                model,
                messages,
                prompt_to_send,
                verbose=verbose,
                action_context=ActionContext(mode="autonomous", actor="acp_watcher"),
            )

            report_path = save_incident_report(transition, model, diagnosis)
            print(f"\n{'=' * 72}")
            print(f"AUTONOMOUS DIAGNOSIS ({ev.kind} — {transition.action})")
            print("=" * 72)
            print(diagnosis)
            print("=" * 72)
            logging.info("Incident report saved to %s", report_path)

        time.sleep(CHECK_INTERVAL_SEC)


# ---------------------------------------------------------------------------
# Self-test unitário da máquina de estados de alarmes (sem banco e sem LLM)
# ---------------------------------------------------------------------------
def _run_alarm_self_test() -> None:
    mgr = AlarmManager(escalation_factor=1.30, escalation_min_interval=60.0)

    print("1) Testing slow_degradation (hysteresis + anti-flooding + escalation):")

    def make_deg(metric: float) -> AlarmEvaluation:
        return AlarmEvaluation(
            alarm_id="slow_degradation:state_1",
            kind="slow_degradation",
            metric=metric,
            raise_threshold=6.75,
            clear_threshold=5.75,
            summary=f"median={metric:.2f}s",
            prompt="test",
            allow_escalation=True,
        )

    deg_sequence = [
        (0.0,   5.02, "NONE"),        # Operação saudável
        (10.0,  6.20, "NONE"),        # Na banda morta, mas vindo de NORMAL -> não dispara
        (20.0,  8.03, "RAISED"),      # Cruzou 6.75s -> DISPARA 1ª vez!
        (30.0,  8.54, "SUPPRESSED"),  # Piora pequena (+6%) -> SILENCIADO (zero spam!)
        (150.0, 8.54, "SUPPRESSED"),  # Mesmo após 130s no mesmo patamar -> SILENCIADO!
        (160.0, 6.20, "SUPPRESSED"),  # Caiu abaixo de 6.75s, mas ainda > 5.75s (histerese!)
        (170.0, 6.90, "SUPPRESSED"),  # Oscilou acima de 6.75s de novo -> SEM chatter!
        (180.0, 10.6, "ESCALATED"),   # Piorou +32% sobre 8.03s (>= 10.44s) -> ESCALA!
        (200.0, 10.7, "SUPPRESSED"),  # Estabilizou no novo patamar -> SILENCIADO!
        (220.0, 5.02, "RESOLVED"),    # Voltou <= 5.75s -> NORMALIZADO e rearmado!
        (230.0, 5.02, "NONE"),        # Saudável
        (240.0, 8.50, "RAISED"),      # Nova falha futura -> dispara novamente!
    ]

    for t, metric, expected in deg_sequence:
        tr = mgr.process(make_deg(metric), now_epoch=t)
        assert tr.action == expected, (
            f"Degradation test failed at t={t}s, metric={metric}: expected {expected}, got {tr.action}"
        )
        print(f"  t={t:5.1f}s | metric={metric:5.2f}s -> {tr.action:<10} (PASS)")

    print("\n2) Testing state_timeout (45s anti-chattering off-delay):")

    def make_to(elapsed: float) -> AlarmEvaluation:
        return AlarmEvaluation(
            alarm_id="state_timeout:state_1",
            kind="state_timeout",
            metric=elapsed,
            raise_threshold=18.0,
            clear_threshold=14.4,
            summary=f"elapsed={elapsed:.1f}s",
            prompt="test",
            allow_escalation=False,
            off_delay_sec=45.0,
        )

    to_sequence = [
        (0.0,   5.0,  "NONE"),        # Normal
        (20.0,  18.5, "RAISED"),      # Travou > 18s -> DISPARA 1ª vez!
        (22.0,  0.0,  "SUPPRESSED"),  # Fechou o ciclo -> inicia off-delay (t=22s)
        (55.0,  18.8, "SUPPRESSED"),  # 33s depois (< 45s!) bateu 18.8s de novo -> SEM chatter!
        (57.0,  0.0,  "SUPPRESSED"),  # Fechou de novo -> reinicia off-delay (t=57s)
        (90.0,  5.0,  "SUPPRESSED"),  # 33s limpo (< 45s) -> ainda aguardando confirmação
        (103.0, 0.0,  "RESOLVED"),    # 46s limpo (>= 45s) -> NORMALIZADO e rearmado!
        (125.0, 19.0, "RAISED"),      # Novo travamento futuro -> dispara normalmente!
    ]

    for t, elapsed, expected in to_sequence:
        tr = mgr.process(make_to(elapsed), now_epoch=t)
        assert tr.action == expected, (
            f"Timeout test failed at t={t}s, elapsed={elapsed}: expected {expected}, got {tr.action}"
        )
        print(f"  t={t:5.1f}s | elapsed={elapsed:5.1f}s -> {tr.action:<10} (PASS)")

    print("\nAll 20 AlarmManager scenarios (hysteresis + off-delay + escalation) passed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="O-PAS ACP Deterministic Watcher -> AI Agent Trigger"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Detect anomalies without calling the LLM API (useful for testing thresholds)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print full JSON returned by agent tools",
    )
    parser.add_argument(
        "--test-alarms",
        action="store_true",
        help="Run AlarmManager unit self-test (no database or LLM required)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if args.test_alarms:
        _run_alarm_self_test()
    else:
        try:
            run_watcher(dry_run=args.dry_run, verbose=args.verbose)
        except KeyboardInterrupt:
            print("\nACP Watcher stopped.")
