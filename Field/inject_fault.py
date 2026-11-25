"""
Injetor de Falhas Industriais (CLI) para a Welding Station.

Escreve um perfil de falha em `Field/faults.json`, lido em tempo real pelo
`Field/IO_Simulation.py` sem precisar reiniciar a simulação nem os containers.

Cenários disponíveis:
  1. sensor-stuck   -> Trava um sensor de fim de curso / presença em False ou True
  2. pneumatic-leak -> Reduz a velocidade da Base (e/ou Escudo), simulando queda
                       de pressão pneumática ou desgaste mecânico (degradação lenta!)
  3. conveyor-jam   -> Trava mecanicamente a esteira mesmo com o motor ligado
  4. clear          -> Remove todas as falhas e volta à operação normal
  5. status         -> Mostra quais falhas estão ativas agora

Exemplos de uso:
    ./.venv/bin/python Field/inject_fault.py sensor-stuck shield_down false
    ./.venv/bin/python Field/inject_fault.py pneumatic-leak --factor 0.6
    ./.venv/bin/python Field/inject_fault.py conveyor-jam
    ./.venv/bin/python Field/inject_fault.py status
    ./.venv/bin/python Field/inject_fault.py clear
"""

import argparse
import json
from pathlib import Path

FAULTS_FILE = Path(__file__).resolve().parent / "faults.json"

VALID_SENSORS = [
    "piece_pos",
    "base_up",
    "base_down",
    "shield_down",
    "shield_up",
]


def load_profile() -> dict:
    if not FAULTS_FILE.exists():
        return {
            "sensor_overrides": {},
            "base_speed_factor": 1.0,
            "shield_speed_factor": 1.0,
            "conveyor_jam": False,
        }
    return json.loads(FAULTS_FILE.read_text(encoding="utf-8"))


def save_profile(profile: dict) -> None:
    FAULTS_FILE.write_text(
        json.dumps(profile, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Injetor de Falhas da Welding Station (escreve Field/faults.json)"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # 1. sensor-stuck <sensor> <true|false>
    p_sensor = subparsers.add_parser(
        "sensor-stuck",
        help="Trava um sensor em True ou False (ex.: sensor-stuck shield_down false)",
    )
    p_sensor.add_argument("sensor", choices=VALID_SENSORS, help="Sensor a travar")
    p_sensor.add_argument(
        "value",
        choices=["true", "false"],
        help="Valor lógico travado (true ou false)",
    )

    # 2. pneumatic-leak [--factor 0.6] [--target base|shield|both]
    p_leak = subparsers.add_parser(
        "pneumatic-leak",
        help="Simula perda de pressão pneumática desacelerando atuador(es)",
    )
    p_leak.add_argument(
        "--factor",
        type=float,
        default=0.6,
        help="Fator de velocidade entre 0.1 e 1.0 (padrão: 0.6 -> ~8.5s em vez de 5s)",
    )
    p_leak.add_argument(
        "--target",
        choices=["base", "shield", "both"],
        default="base",
        help="Qual atuador sofre degradação (padrão: base)",
    )

    # 3. conveyor-jam
    subparsers.add_parser(
        "conveyor-jam",
        help="Trava mecanicamente a esteira mesmo quando o PLC envia Conveyor_Run=True",
    )

    # 4. clear
    subparsers.add_parser("clear", help="Remove todas as falhas ativas")

    # 5. status
    subparsers.add_parser("status", help="Exibe o perfil de falhas ativo")

    args = parser.parse_args()

    if args.command == "status":
        if not FAULTS_FILE.exists():
            print("✅ Nenhuma falha ativa (operação normal).")
        else:
            print(f"⚠️  Perfil ativo em {FAULTS_FILE.name}:")
            print(FAULTS_FILE.read_text(encoding="utf-8"))
        return

    if args.command == "clear":
        if FAULTS_FILE.exists():
            FAULTS_FILE.unlink()
        print("✅ Todas as falhas removidas — operação normal restaurada.")
        return

    profile = load_profile()

    if args.command == "sensor-stuck":
        stuck_val = args.value == "true"
        profile["sensor_overrides"][args.sensor] = stuck_val
        save_profile(profile)
        print(f"🚨 Falha injetada: sensor '{args.sensor}' travado em {stuck_val}.")

    elif args.command == "pneumatic-leak":
        factor = max(0.1, min(args.factor, 1.0))
        if args.target in ("base", "both"):
            profile["base_speed_factor"] = factor
        if args.target in ("shield", "both"):
            profile["shield_speed_factor"] = factor
        save_profile(profile)
        print(
            f"🚨 Falha injetada: degradação pneumática ({args.target}) "
            f"com velocidade em {factor * 100:.0f}%."
        )

    elif args.command == "conveyor-jam":
        profile["conveyor_jam"] = True
        save_profile(profile)
        print("🚨 Falha injetada: esteira mecanicamente travada (conveyor_jam=True).")


if __name__ == "__main__":
    main()
