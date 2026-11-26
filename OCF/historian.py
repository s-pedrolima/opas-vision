"""
Historian da camada OCF (O-PAS Connectivity Framework).

Cliente OPC UA independente que lê continuamente as 13 variáveis da Welding
Station no `opcua_server.py` e persiste o histórico em um banco SQLite local
(`OCF/data/historian.db`).

Papel na arquitetura:
    [opcua_server.py] --(OPC UA Client)--> [historian.py] --> SQLite (historian.db)
                                                                    ▲
                                                            Watcher / AI Agent

Uso:
    # Rodar a coleta contínua:
    ./.venv/bin/python OCF/historian.py

    # Ver um resumo rápido do que já foi gravado no banco:
    ./.venv/bin/python OCF/historian.py --summary
"""

import argparse
import asyncio
import logging
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from asyncua import Client

# ---------------------------------------------------------------------------
# Configurações
# ---------------------------------------------------------------------------
OPCUA_ENDPOINT = "opc.tcp://localhost:4840/OPCUA/SimulationServer"
NAMESPACE_URI = "urn:opas:welding-station"
DCN_ID = "DCN-WS"

# O banco SQLite fica na mesma pasta deste script: OCF/data/historian.db
DB_PATH = Path(__file__).resolve().parent / "data" / "historian.db"

POLL_INTERVAL_SEC = 0.5        # Mesma cadência de 500 ms do Node-RED
HEARTBEAT_INTERVAL_SEC = 10.0  # Grava 1 amostra a cada 10 s mesmo se nada mudar
RECONNECT_DELAY_SEC = 3.0

# Mesma ordem das 13 variáveis expostas pelo opcua_server.py
TAG_NAMES = [
    "Machine_State",
    "Conveyor",
    "Base_Up",
    "Base_Rotate",
    "Base_Down",
    "Shield_Down",
    "Shield_Up",
    "Burner",
    "Piece_Pos_Sensor",
    "Base_Up_Sensor",
    "Base_Down_Sensor",
    "Shield_Down_Sensor",
    "Shield_Up_Sensor",
]

# Mapeamento legível dos estados da FSM definidos em DCN/Controller/plc.xml
STATE_NAMES = {
    0: "WAITING_FOR_PIECE",
    1: "PREPARING_TO_WELD",
    2: "WELDING",
    3: "FINISHING_PROCESS",
    4: "RELEASING_PIECE",
}


def utc_now_iso() -> str:
    """Retorna timestamp atual em ISO-8601 UTC (ex.: 2026-11-23T21:50:01.123+00:00)."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


# ---------------------------------------------------------------------------
# Camada de Persistência (SQLite)
# ---------------------------------------------------------------------------
def init_db(db_path: Path) -> sqlite3.Connection:
    """Abre (ou cria) o banco SQLite, garante tabelas e aplica migrações leves."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)

    # WAL (Write-Ahead Logging) permite leituras simultâneas enquanto o Historian
    # escreve — essencial para o Watcher e o AI Agent consultarem o banco sem
    # tomar erro de "database is locked".
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")

    # Tabela 1: linha do tempo dos sinais (gravada on-change + heartbeat)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS telemetry (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            ts                  TEXT    NOT NULL,
            ts_epoch            REAL    NOT NULL,
            reason              TEXT    NOT NULL,
            machine_state       INTEGER NOT NULL,
            conveyor            INTEGER NOT NULL,
            base_up             INTEGER NOT NULL,
            base_rotate         INTEGER NOT NULL,
            base_down           INTEGER NOT NULL,
            shield_down         INTEGER NOT NULL,
            shield_up           INTEGER NOT NULL,
            burner              INTEGER NOT NULL,
            piece_pos_sensor    INTEGER NOT NULL,
            base_up_sensor      INTEGER NOT NULL,
            base_down_sensor    INTEGER NOT NULL,
            shield_down_sensor  INTEGER NOT NULL,
            shield_up_sensor    INTEGER NOT NULL
        );
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_telemetry_ts ON telemetry(ts_epoch);"
    )

    # Tabela 2: duração de cada estado da máquina (FSM).
    # `close_reason` diferencia o motivo de fechamento do registro:
    #   - 'transition'      : transição normal da FSM (usado em estatísticas!)
    #   - 'partial_startup' : 1º estado capturado ao conectar (já estava em andamento)
    #   - 'shutdown'        : Historian encerrado/reiniciado no meio do estado
    #   - NULL              : estado ainda EM ANDAMENTO (ended_at IS NULL)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS state_cycles (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            cycle_id        INTEGER NOT NULL,
            state           INTEGER NOT NULL,
            state_name      TEXT    NOT NULL,
            started_at      TEXT    NOT NULL,
            started_epoch   REAL    NOT NULL,
            ended_at        TEXT,
            ended_epoch     REAL,
            duration_sec    REAL,
            close_reason    TEXT
        );
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cycles_state ON state_cycles(state, started_epoch);"
    )

    # Migração automática para bancos criados antes da coluna `close_reason` existir
    columns = {
        row[1] for row in conn.execute("PRAGMA table_info(state_cycles);").fetchall()
    }
    if "close_reason" not in columns:
        conn.execute("ALTER TABLE state_cycles ADD COLUMN close_reason TEXT;")

    conn.commit()
    return conn


def get_next_cycle_id(conn: sqlite3.Connection) -> int:
    """Recupera o último cycle_id gravado para continuar a numeração após reinícios."""
    row = conn.execute("SELECT MAX(cycle_id) FROM state_cycles;").fetchone()
    return (row[0] or 0) + 1


def close_dangling_states(conn: sqlite3.Connection) -> None:
    """Fecha eventuais estados que ficaram em aberto de uma execução anterior."""
    now_iso = utc_now_iso()
    now_epoch = time.time()
    conn.execute(
        """
        UPDATE state_cycles
           SET ended_at = ?,
               ended_epoch = ?,
               duration_sec = ROUND(? - started_epoch, 3),
               close_reason = 'shutdown'
         WHERE ended_at IS NULL;
        """,
        (now_iso, now_epoch, now_epoch),
    )
    conn.commit()


def insert_telemetry(
    conn: sqlite3.Connection,
    ts_iso: str,
    ts_epoch: float,
    reason: str,
    values: list,
) -> None:
    """Insere um snapshot completo das 13 tags na tabela telemetry."""
    # Converte booleanos Python (True/False) para 1/0 (padrão do SQLite)
    row_values = [int(v) for v in values]
    conn.execute(
        """
        INSERT INTO telemetry (
            ts, ts_epoch, reason,
            machine_state, conveyor, base_up, base_rotate, base_down,
            shield_down, shield_up, burner,
            piece_pos_sensor, base_up_sensor, base_down_sensor,
            shield_down_sensor, shield_up_sensor
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (ts_iso, ts_epoch, reason, *row_values),
    )
    conn.commit()


def open_state_row(
    conn: sqlite3.Connection,
    cycle_id: int,
    state: int,
    ts_iso: str,
    ts_epoch: float,
) -> int:
    """Abre um novo registro em state_cycles quando a máquina entra em um estado."""
    state_name = STATE_NAMES.get(state, f"UNKNOWN_{state}")
    cursor = conn.execute(
        """
        INSERT INTO state_cycles (cycle_id, state, state_name, started_at, started_epoch)
        VALUES (?, ?, ?, ?, ?);
        """,
        (cycle_id, state, state_name, ts_iso, ts_epoch),
    )
    conn.commit()
    return cursor.lastrowid


def close_state_row(
    conn: sqlite3.Connection,
    row_id: int,
    started_epoch: float,
    ts_iso: str,
    ts_epoch: float,
    close_reason: str = "transition",
) -> float:
    """Fecha o registro do estado anterior registrando o motivo e retorna a duração."""
    duration = round(ts_epoch - started_epoch, 3)
    conn.execute(
        """
        UPDATE state_cycles
           SET ended_at = ?,
               ended_epoch = ?,
               duration_sec = ?,
               close_reason = ?
         WHERE id = ?;
        """,
        (ts_iso, ts_epoch, duration, close_reason, row_id),
    )
    conn.commit()
    return duration


# ---------------------------------------------------------------------------
# Loop Principal (Cliente OPC UA)
# ---------------------------------------------------------------------------
async def run_historian() -> None:
    conn = init_db(DB_PATH)
    close_dangling_states(conn)
    cycle_id = get_next_cycle_id(conn)

    logging.info("Historian connected to SQLite at %s", DB_PATH)

    last_values: list | None = None
    last_write_epoch: float = 0.0
    current_state: int | None = None
    open_row_id: int | None = None
    open_state_epoch: float = 0.0
    # O 1º estado capturado ao iniciar o script provavelmente já estava em andamento,
    # então marcamos com `partial_startup` ao fechar para não distorcer o min/avg!
    is_partial_startup: bool = True

    try:
        while True:
            try:
                logging.info("Connecting to OPC UA server at %s...", OPCUA_ENDPOINT)
                async with Client(url=OPCUA_ENDPOINT) as client:
                    # Descoberta dinâmica do namespace pelo URI (boa prática O-PAS:
                    # não precisa chumbar ns=2 no código do cliente!).
                    ns_idx = await client.get_namespace_index(NAMESPACE_URI)
                    nodes = [
                        client.get_node(f"ns={ns_idx};s={DCN_ID}/{tag}")
                        for tag in TAG_NAMES
                    ]
                    logging.info(
                        "Connected! Namespace '%s' -> ns=%d | Monitoring %d tags.",
                        NAMESPACE_URI,
                        ns_idx,
                        len(nodes),
                    )

                    while True:
                        # Lê todas as 13 tags em UMA única requisição OPC UA (snapshot coerente)
                        values = await client.read_values(nodes)
                        now_iso = utc_now_iso()
                        now_epoch = time.time()

                        # 1. Gravação on-change + heartbeat na tabela `telemetry`
                        changed = last_values is None or values != last_values
                        heartbeat_due = (now_epoch - last_write_epoch) >= HEARTBEAT_INTERVAL_SEC

                        if changed or heartbeat_due:
                            reason = "change" if changed else "heartbeat"
                            insert_telemetry(conn, now_iso, now_epoch, reason, values)
                            last_values = list(values)
                            last_write_epoch = now_epoch

                        # 2. Rastreamento de transições de estado na tabela `state_cycles`
                        state = int(values[0])  # values[0] == Machine_State
                        if state != current_state:
                            if open_row_id is not None:
                                reason = "partial_startup" if is_partial_startup else "transition"
                                is_partial_startup = False
                                duration = close_state_row(
                                    conn,
                                    open_row_id,
                                    open_state_epoch,
                                    now_iso,
                                    now_epoch,
                                    close_reason=reason,
                                )
                                logging.info(
                                    "Cycle #%d | State %d (%s) closed in %.2fs (%s)",
                                    cycle_id,
                                    current_state,
                                    STATE_NAMES.get(current_state, "?"),
                                    duration,
                                    reason,
                                )

                            # Quando volta para o estado 0 vindo do 4, inicia um novo ciclo de peça
                            if current_state == 4 and state == 0:
                                cycle_id += 1

                            open_row_id = open_state_row(
                                conn, cycle_id, state, now_iso, now_epoch
                            )
                            open_state_epoch = now_epoch
                            current_state = state

                            logging.info(
                                "Cycle #%d | Entered state %d (%s)",
                                cycle_id,
                                state,
                                STATE_NAMES.get(state, "?"),
                            )

                        await asyncio.sleep(POLL_INTERVAL_SEC)

            except (ConnectionError, OSError, asyncio.TimeoutError) as exc:
                logging.warning(
                    "OPC UA connection lost (%s). Reconnecting in %.0fs...",
                    exc,
                    RECONNECT_DELAY_SEC,
                )
                await asyncio.sleep(RECONNECT_DELAY_SEC)

    finally:
        # Fecha o estado em aberto ao interromper com Ctrl+C marcando como 'shutdown'
        if open_row_id is not None:
            close_state_row(
                conn,
                open_row_id,
                open_state_epoch,
                utc_now_iso(),
                time.time(),
                close_reason="shutdown",
            )
        conn.close()
        logging.info("SQLite connection closed cleanly.")


# ---------------------------------------------------------------------------
# Utilitário de inspeção rápida (--summary)
# ---------------------------------------------------------------------------
def print_summary() -> None:
    """Exibe um resumo dos dados já gravados em OCF/data/historian.db."""
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}. Start the historian first.")
        return

    conn = init_db(DB_PATH)
    total_telemetry = conn.execute("SELECT COUNT(*) FROM telemetry;").fetchone()[0]
    total_cycles = conn.execute("SELECT COUNT(DISTINCT cycle_id) FROM state_cycles;").fetchone()[0]
    excluded = conn.execute(
        """
        SELECT COUNT(*)
          FROM state_cycles
         WHERE close_reason IN ('shutdown', 'partial_startup');
        """
    ).fetchone()[0]

    print(f"\n=== Historian Summary ({DB_PATH.name}) ===")
    print(f"Telemetry rows          : {total_telemetry}")
    print(f"Recorded cycles         : {total_cycles}")
    print(f"Excluded partial states : {excluded} (shutdown / partial_startup)")

    print("\n--- Average duration per state (completed transitions) ---")
    rows = conn.execute(
        """
        SELECT state, state_name, COUNT(*), ROUND(AVG(duration_sec), 2),
               ROUND(MIN(duration_sec), 2), ROUND(MAX(duration_sec), 2)
          FROM state_cycles
         WHERE duration_sec IS NOT NULL
           AND (close_reason IS NULL OR close_reason = 'transition')
         GROUP BY state, state_name
         ORDER BY state;
        """
    ).fetchall()

    if rows:
        print(f"{'State':<24} {'Cnt':>4} {'Avg(s)':>9} {'Min(s)':>8} {'Max(s)':>8}")
        for st, name, cnt, avg_d, min_d, max_d in rows:
            print(f"{f'{st} - {name}':<24} {cnt:>4} {avg_d:>9.2f} {min_d:>8.2f} {max_d:>8.2f}")
    else:
        print("No completed states recorded yet.")

    print("\n--- Last 8 transitions ---")
    recent = conn.execute(
        """
        SELECT cycle_id, state, state_name, started_at, duration_sec, close_reason
          FROM state_cycles
         ORDER BY id DESC
         LIMIT 8;
        """
    ).fetchall()
    for cid, st, name, started, dur, reason in reversed(recent):
        if dur is None:
            dur_str = "IN_PROGRESS..."
        else:
            tag = f" [{reason}]" if reason and reason != "transition" else ""
            dur_str = f"{dur:.2f}s{tag}"
        print(f"  Cycle #{cid:<3} | {st} - {name:<18} | start {started[11:19]}Z | {dur_str}")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="O-PAS OCF Historian (OPC UA -> SQLite)")
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print a summary of recorded SQLite data and exit",
    )
    args = parser.parse_args()

    if args.summary:
        print_summary()
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
        )
        logging.getLogger("asyncua").setLevel(logging.WARNING)
        try:
            asyncio.run(run_historian())
        except KeyboardInterrupt:
            print("\nHistorian stopped.")
