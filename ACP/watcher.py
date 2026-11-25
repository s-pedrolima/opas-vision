"""
Watcher Determinístico da camada ACP (Advanced Computing Platform).

Princípio de IA em sistemas reais ("Determinístico vigia, Probabilístico investiga"):
  - Enquanto a Welding Station opera normal, este script faz apenas queries SQL
    baratas no `OCF/data/historian.db` a cada 2 segundos -> CUSTO ZERO de LLM!
  - Assim que uma regra determinística dispara (travamento em um estado,
    degradação progressiva de tempo de ciclo ou perda de telemetria), o Watcher
    aciona automaticamente o AI Agent (`ACP/agent.py`) para investigar a causa
    raiz e salvar um relatório de incidente em `ACP/incidents/`.

Como rodar:
    # Modo normal (chama a LLM quando detectar anomalia):
    ./.venv/bin/python ACP/watcher.py

    # Modo dry-run (detecta anomalias e imprime o alerta SEM chamar a LLM):
    ./.venv/bin/python ACP/watcher.py --dry-run
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
    from ACP.agent import SYSTEM_PROMPT, build_client, run_agent_turn
except ImportError:
    from agent import SYSTEM_PROMPT, build_client, run_agent_turn

# ---------------------------------------------------------------------------
# Configurações e limiares determinísticos (baseados na física + linha de base)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "OCF" / "data" / "historian.db"
INCIDENTS_DIR = PROJECT_ROOT / "ACP" / "incidents"

CHECK_INTERVAL_SEC = 2.0  # Frequência de verificação no SQLite

# Tempo máximo esperado em cada estado antes de sinalizar possível TRAVAMENTO.
# Observações físicas:
#   - Estado 0: peças em regime permanente saem de X=25 cm (~4.5s), mas a 1ª peça
#     no cold start sai de X=0 cm (50 cm / 5 cm/s = 10s) -> limite 16s evita falso alarme!
#   - Estados 1 e 3: ~5.0s nominal -> limite 12s
#   - Estado 2: timer fixo T_Weld = 10.0s -> limite 15s
#   - Estado 4: ~0.5s nominal -> limite 5s
STATE_TIMEOUT_SEC = {
    0: 16.0,  # WAITING_FOR_PIECE  (ex.: esteira parada ou sensor de peça falhando)
    1: 12.0,  # PREPARING_TO_WELD  (ex.: Base_Up_Sensor ou Shield_Down_Sensor travado)
    2: 15.0,  # WELDING            (ex.: timer T_Weld ou OpenPLC travado)
    3: 12.0,  # FINISHING_PROCESS  (ex.: Base_Down_Sensor ou Shield_Up_Sensor travado)
    4: 5.0,   # RELEASING_PIECE    (ex.: Piece_Pos_Sensor colado em True)
}

# Regra de degradação lenta (ex.: vazamento pneumático desacelerando Base/Shield):
# Usamos a MEDIANA (estatística robusta a outliers!) dos últimos N ciclos concluídos
# dos estados de movimentação (1 ou 3). Assim, um único travamento pontual (ex.: 56s
# seguido de ciclos normais de 5s) NÃO dispara falso alarme de degradação progressiva!
DEGRADATION_STATES = (1, 3)
DEGRADATION_WINDOW = 3         # Quantidade de ciclos recentes comparados
DEGRADATION_BASELINE_SEC = 5.0 # Tempo nominal de subida/descida
DEGRADATION_FACTOR = 1.35      # +35% acima do nominal (> 6.75s na mediana)

# O Historian grava heartbeat a cada 10s -> mais de 22s sem nova linha = telemetria parada
STALE_TELEMETRY_SEC = 22.0

# Guardrail de FinOps (anti-spam): impede que o mesmo tipo de alerta chame a LLM
# repetidamente a cada 2 segundos enquanto a máquina continua parada!
ALERT_COOLDOWN_SEC = 90.0


@dataclass
class Anomaly:
    """Estrutura de um evento anômalo detectado pelas regras determinísticas."""
    kind: str         # Ex.: "state_timeout", "slow_degradation", "stale_telemetry"
    dedup_key: str    # Chave única para não alertar 2x sobre o MESMO evento/linha
    summary: str      # Descrição curta para o log
    prompt: str       # Evento factual enviado ao AI Agent (sem viés de confirmação)


def _open_db_readonly() -> sqlite3.Connection | None:
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# As 3 regras determinísticas de detecção
# ---------------------------------------------------------------------------
def check_state_timeout(conn: sqlite3.Connection, now_epoch: float) -> Anomaly | None:
    """Regra 1: Detecta se um estado aberto ultrapassou o tempo máximo esperado."""
    row = conn.execute(
        """
        SELECT id, cycle_id, state, state_name, started_at, started_epoch
          FROM state_cycles
         WHERE ended_at IS NULL
         ORDER BY id DESC
         LIMIT 1;
        """
    ).fetchone()

    if row is None:
        return None

    state = int(row["state"])
    elapsed = round(now_epoch - float(row["started_epoch"]), 2)
    threshold = STATE_TIMEOUT_SEC.get(state, 15.0)

    if elapsed <= threshold:
        return None

    return Anomaly(
        kind="state_timeout",
        dedup_key=f"state_timeout:row_{row['id']}",
        summary=(
            f"TIMEOUT detectado no Ciclo #{row['cycle_id']} — "
            f"Estado {state} ({row['state_name']}) aberto há {elapsed:.1f}s "
            f"(limite: {threshold:.0f}s)"
        ),
        # Framing neutro (boa prática para evitar viés de confirmação na LLM):
        # apresenta o fato observado e pede verificação antes de concluir falha ativa.
        prompt=(
            f"EVENTO DO WATCHER (state_timeout): O Estado {state} ({row['state_name']}) "
            f"no Ciclo #{row['cycle_id']} atingiu {elapsed:.1f} segundos aberto "
            f"(limiar configurado: {threshold:.0f}s, início em {row['started_at']}).\n"
            f"Verifique o estado ao vivo no OPC UA, os ciclos recentes no Historian, "
            f"a saúde do Docker Swarm, o histórico de incidentes e a lógica do PLC. "
            f"Confirme se o travamento ainda persiste ou se já foi normalizado (evento "
            f"transitório) e apresente o diagnóstico técnico e recomendações."
        ),
    )


def check_slow_degradation(conn: sqlite3.Connection) -> Anomaly | None:
    """Regra 2: Detecta degradação sustentada (mediana dos últimos N ciclos acima do limite)."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(state_cycles);").fetchall()}
    reason_filter = (
        "AND (close_reason IS NULL OR close_reason = 'transition')"
        if "close_reason" in cols
        else ""
    )

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
        # Mediana ignora outliers isolados (como um único ciclo travado de 56s)
        recent_median = round(statistics.median(durations), 2)
        threshold = round(DEGRADATION_BASELINE_SEC * DEGRADATION_FACTOR, 2)

        if recent_median > threshold:
            latest_id = rows[0]["id"]
            state_name = rows[0]["state_name"]
            return Anomaly(
                kind="slow_degradation",
                dedup_key=f"degradation:state_{state}:up_to_{latest_id}",
                summary=(
                    f"DEGRADAÇÃO SUSTENTADA no Estado {state} ({state_name}): "
                    f"mediana dos últimos {DEGRADATION_WINDOW} ciclos = {recent_median:.2f}s "
                    f"(valores: {durations}, nominal ~{DEGRADATION_BASELINE_SEC:.1f}s)"
                ),
                prompt=(
                    f"EVENTO PREDITIVO DO WATCHER (slow_degradation): A mediana das "
                    f"durações dos últimos {DEGRADATION_WINDOW} ciclos concluídos do "
                    f"Estado {state} ({state_name}) subiu para {recent_median:.2f}s "
                    f"(durações: {durations} s; nominal ~{DEGRADATION_BASELINE_SEC:.1f}s).\n"
                    f"Consulte o Historian, o estado ao vivo, incidentes recentes e o "
                    f"código PLC para diagnosticar possível perda de velocidade mecânica/"
                    f"pneumática e estimar o impacto no throughput."
                ),
            )

    return None


def check_stale_telemetry(conn: sqlite3.Connection, now_epoch: float) -> Anomaly | None:
    """Regra 3: Detecta se o Historian parou de receber dados/heartbeats."""
    row = conn.execute(
        "SELECT id, ts, ts_epoch FROM telemetry ORDER BY id DESC LIMIT 1;"
    ).fetchone()

    if row is None:
        return None

    age_sec = round(now_epoch - float(row["ts_epoch"]), 1)
    if age_sec <= STALE_TELEMETRY_SEC:
        return None

    return Anomaly(
        kind="stale_telemetry",
        dedup_key=f"stale_telemetry:last_row_{row['id']}",
        summary=(
            f"TELEMETRIA CONGELADA: última amostra no Historian foi há {age_sec:.1f}s "
            f"(em {row['ts']}, limite: {STALE_TELEMETRY_SEC:.0f}s)"
        ),
        prompt=(
            f"EVENTO DO WATCHER (stale_telemetry): O Historian não registra nenhuma "
            f"nova amostra nem heartbeat há {age_sec:.1f} segundos (última leitura em "
            f"{row['ts']}).\n"
            f"Verifique o estado ao vivo no OPC UA, a saúde/logs dos containers no "
            f"Docker Swarm, os últimos ciclos no Historian e incidentes recentes para "
            f"identificar em qual camada O-PAS ocorreu a interrupção."
        ),
    )


# ---------------------------------------------------------------------------
# Persistência do relatório gerado pelo Agente
# ---------------------------------------------------------------------------
def save_incident_report(anomaly: Anomaly, model: str, diagnosis: str) -> Path:
    """Salva o diagnóstico gerado pela LLM em ACP/incidents/<timestamp>_<tipo>.md."""
    INCIDENTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = INCIDENTS_DIR / f"{stamp}_{anomaly.kind}.md"

    content = (
        f"# Relatório de Incidente — {anomaly.kind}\n\n"
        f"- **Data/Hora:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"- **Modelo LLM:** `{model}`\n"
        f"- **Gatilho Determinístico:** {anomaly.summary}\n\n"
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

    logging.info("Watcher ACP iniciado | modo=%s | banco=%s", "DRY-RUN" if dry_run else model, DB_PATH)
    logging.info(
        "Limiares: timeouts=%s | degradação (mediana) > %.2fs | telemetria > %.0fs | cooldown=%.0fs",
        STATE_TIMEOUT_SEC,
        DEGRADATION_BASELINE_SEC * DEGRADATION_FACTOR,
        STALE_TELEMETRY_SEC,
        ALERT_COOLDOWN_SEC,
    )

    # Controle de anti-spam:
    # - `handled_keys`: evita alertar novamente sobre a EXATA mesma linha do banco
    # - `last_alert_by_kind`: garante um cooldown mínimo entre alertas do mesmo tipo
    handled_keys: set[str] = set()
    last_alert_by_kind: dict[str, float] = {}

    # Aguarda ver dados frescos ao menos uma vez antes de disparar "telemetria congelada"
    # (assim, se o Watcher subir 5 segundos antes do Historian no Simulation.sh, não gera falso alarme!)
    saw_fresh_telemetry = False

    while True:
        conn = _open_db_readonly()
        if conn is None:
            logging.info("Aguardando criação do banco %s...", DB_PATH)
            time.sleep(CHECK_INTERVAL_SEC)
            continue

        try:
            now_epoch = time.time()

            # Verifica se já há telemetria recente (< STALE_TELEMETRY_SEC)
            latest = conn.execute("SELECT MAX(ts_epoch) FROM telemetry;").fetchone()[0]
            if latest is not None and (now_epoch - float(latest)) <= STALE_TELEMETRY_SEC:
                saw_fresh_telemetry = True

            # Avalia as 3 regras em ordem de prioridade
            anomalies: list[Anomaly] = []

            if saw_fresh_telemetry:
                stale = check_stale_telemetry(conn, now_epoch)
                if stale:
                    anomalies.append(stale)

            timeout_hit = check_state_timeout(conn, now_epoch)
            if timeout_hit:
                anomalies.append(timeout_hit)

            # Só verifica degradação lenta se já vimos dados vivos nesta sessão
            if saw_fresh_telemetry:
                degrad_hit = check_slow_degradation(conn)
                if degrad_hit:
                    anomalies.append(degrad_hit)

        finally:
            conn.close()

        for anomaly in anomalies:
            # 1. Já investigamos exatamente esse evento? Ignora!
            if anomaly.dedup_key in handled_keys:
                continue

            # 2. Esse tipo de alerta ainda está em cooldown? Ignora!
            last_ts = last_alert_by_kind.get(anomaly.kind, 0.0)
            if (time.time() - last_ts) < ALERT_COOLDOWN_SEC:
                continue

            # Registra antes de chamar a LLM para garantir proteção mesmo em erro
            handled_keys.add(anomaly.dedup_key)
            last_alert_by_kind[anomaly.kind] = time.time()

            logging.warning("🚨 %s", anomaly.summary)

            if dry_run:
                logging.info("[DRY-RUN] Prompt que seria enviado ao agente:\n%s", anomaly.prompt)
                continue

            logging.info("🤖 Acionando AI Agent (%s) para diagnóstico...", model)
            # Cada incidente começa com uma conversa limpa (evita contaminar com incidentes antigos)
            messages: list[dict[str, Any]] = [{"role": "system", "content": SYSTEM_PROMPT}]
            diagnosis = run_agent_turn(
                client,
                model,
                messages,
                anomaly.prompt,
                verbose=verbose,
            )

            report_path = save_incident_report(anomaly, model, diagnosis)
            print(f"\n{'=' * 72}")
            print(f"DIAGNÓSTICO AUTOMÁTICO ({anomaly.kind})")
            print("=" * 72)
            print(diagnosis)
            print("=" * 72)
            logging.info("📝 Relatório salvo em %s", report_path)

        time.sleep(CHECK_INTERVAL_SEC)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="O-PAS ACP Deterministic Watcher -> AI Agent Trigger"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Detecta anomalias sem chamar a API da LLM (ótimo para testar limiares)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Exibe o JSON completo retornado pelas tools do agente",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    try:
        run_watcher(dry_run=args.dry_run, verbose=args.verbose)
    except KeyboardInterrupt:
        print("\nWatcher ACP encerrado.")
