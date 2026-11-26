import json
from pathlib import Path
from time import sleep
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ConnectionException

TCP_IP = 'localhost'

# Arquivo compartilhado com `Field/inject_fault.py` para injeção de falhas ao vivo
FAULTS_FILE = Path(__file__).resolve().parent / "faults.json"

# Configurações da Simulação -------------------------------------------------
REAL_MOTOR_SPEED = 5  # Velocidade real [cm/s]
TIME_SKIP = 0.25      # Tempo de atualização por 1/2 ciclo [s]
MOTOR_SPEED = REAL_MOTOR_SPEED * (2 * TIME_SKIP)  # Velocidade simulada [cm/loop]
MACHINE_X_POS = 50    # Posição X da estação na esteira [cm]

# Limites mecânicos (batentes físicos) da Base e do Escudo
BASE_Y_MIN = 0.0
BASE_Y_MAX = 20.0
SHIELD_Y_MIN = 30.0
SHIELD_Y_MAX = 50.0

NOMINAL_BASE_SPEED = 2.0
NOMINAL_SHIELD_SPEED = 2.0

# Endereços de variáveis do PLC ----------------------------------------------
PLC_COILS_START = 0     # Coils 0-6: comandos PLC -> Python
SENSOR_COILS_START = 7  # Coils 7-11: sensores de posição Python -> PLC
RESET_COIL = 12         # Coil 12: reseta coils e registradores do PLC

# Mapeamento das Coils -------------------------------------------------------
#
# ** Coils de Comando (PLC -> Python) **
# coil0: Conveyor_Run
# coil1: Base_Up
# coil2: Base_Rotate
# coil3: Base_Down
# coil4: Shield_Down
# coil5: Shield_Up
# coil6: Burner
#
# ** Coils de Sensores (Python -> PLC) **
# coil7: Piece_Pos_Sensor
# coil8: Base_Up_Sensor
# coil9: Base_Down_Sensor
# coil10: Shield_Down_Sensor
# coil11: Shield_Up_Sensor


# Carregador Dinâmico de Injeção de Falhas -----------------------------------
class FaultProfile:
    """
    Lê `Field/faults.json` dinamicamente (só re-parseia quando o arquivo muda).
    Permite injetar e remover falhas industriais com a simulação em andamento!
    """

    def __init__(self, path: Path):
        self.path = path
        self._last_mtime: float | None = None
        self.sensor_overrides: dict[str, bool] = {}
        self.base_speed_factor: float = 1.0
        self.shield_speed_factor: float = 1.0
        self.conveyor_jam: bool = False

    def reload_if_changed(self) -> None:
        if not self.path.exists():
            if self._last_mtime is not None:
                self._reset()
                self._last_mtime = None
                print("[FAULT] All faults cleared — normal operation restored.")
            return

        mtime = self.path.stat().st_mtime
        if mtime == self._last_mtime:
            return

        self._last_mtime = mtime
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            print(f"[FAULT] Failed to read {self.path.name}: {exc}")
            return

        self.sensor_overrides = {
            k: bool(v) for k, v in data.get("sensor_overrides", {}).items()
        }
        self.base_speed_factor = max(0.1, min(float(data.get("base_speed_factor", 1.0)), 2.0))
        self.shield_speed_factor = max(0.1, min(float(data.get("shield_speed_factor", 1.0)), 2.0))
        self.conveyor_jam = bool(data.get("conveyor_jam", False))

        if self.has_active_faults():
            print(
                f"[FAULT] Active profile -> sensors={self.sensor_overrides} | "
                f"base_speed={self.base_speed_factor:.2f}x | "
                f"shield_speed={self.shield_speed_factor:.2f}x | "
                f"conveyor_jam={self.conveyor_jam}"
            )
        else:
            print("[FAULT] Empty profile — normal operation restored.")

    def _reset(self) -> None:
        self.sensor_overrides = {}
        self.base_speed_factor = 1.0
        self.shield_speed_factor = 1.0
        self.conveyor_jam = False

    def has_active_faults(self) -> bool:
        return (
            bool(self.sensor_overrides)
            or self.base_speed_factor != 1.0
            or self.shield_speed_factor != 1.0
            or self.conveyor_jam
        )

    def apply_sensor(self, key: str, real_value: bool) -> bool:
        """Se houver override de falha para este sensor, devolve o valor travado."""
        if key in self.sensor_overrides:
            return self.sensor_overrides[key]
        return real_value


# Conexão Modbus TCP com o OpenPLC -------------------------------------------
def connect_tcp_client(ip):
    client = ModbusTcpClient(ip, port=503, timeout=5)
    while not client.connect():
        print(f"TCP: Failed to connect to {ip}, retrying in 2s...")
        sleep(2)
    print(f"TCP: Connected to {ip}")
    return client


# Componentes Passivos -------------------------------------------------------
class Workpiece:
    def __init__(self, ID):
        self.name = "Piece"
        self.ID = ID
        self.x_pos = 0.0
        self.y_pos = 0.0
        self.ang_pos = 0.0
        self.welded = False

    def state(self):
        return (
            f"ID: {self.ID}, X Pos: {self.x_pos:.1f}, Y Pos: {self.y_pos:.1f}, "
            f"Angle: {self.ang_pos:.0f}, Welded: {self.welded}"
        )

    def x_move(self, speed):
        self.x_pos += speed

    def y_move(self, speed):
        self.y_pos += speed

    def rotate(self, speed):
        self.ang_pos += speed


# Atuadores ------------------------------------------------------------------
class ConveyorBelt:
    def __init__(self, length, motor_speed):
        self.speed = motor_speed
        self.length = length
        self.status = False
        self.workpieces = []

    def motor_on(self):
        if not self.status:
            print("Conveyor on")
        self.status = True

    def motor_off(self):
        if self.status:
            print("Conveyor off")
        self.status = False

    def add_piece(self, piece):
        self.workpieces.insert(0, piece)

    def update(self, jammed: bool = False):
        if self.status and not jammed:
            for piece in self.workpieces:
                piece.x_move(self.speed)
            # Remove peças que chegaram ao fim da esteira
            self.workpieces = [p for p in self.workpieces if p.x_pos < self.length]


class WorkBase:
    def __init__(self, x_pos, y_pos, motor_speed, rotation_speed):
        self.name = "Base"
        self.x_pos = float(x_pos)
        self.y_pos = float(y_pos)
        self.motor_speed = float(motor_speed)
        self.rotation_speed = float(rotation_speed)
        self.moving = False

    def lift(self, piece):
        # Respeita o batente mecânico superior (BASE_Y_MAX) mesmo sob velocidade reduzida
        new_y = min(BASE_Y_MAX, self.y_pos + self.motor_speed)
        delta = new_y - self.y_pos
        self.y_pos = new_y
        if piece is not None:
            piece.y_move(delta)
        if not self.moving:
            print("Base lifting...")

    def lower(self, piece):
        # Respeita o batente mecânico inferior (BASE_Y_MIN)
        new_y = max(BASE_Y_MIN, self.y_pos - self.motor_speed)
        delta = new_y - self.y_pos
        self.y_pos = new_y
        if piece is not None:
            piece.y_move(delta)
        if not self.moving:
            print("Base lowering...")

    def rotate(self, piece):
        if piece is not None:
            piece.rotate(self.rotation_speed)


class Burner:
    def __init__(self):
        self.active = False

    def ignite(self):
        if not self.active:
            self.active = True
            print("Burner ignited")

    def extinguish(self):
        if self.active:
            self.active = False
            print("Burner extinguished")


class SuctionShield:
    def __init__(self, x_pos, y_pos, motor_speed):
        self.name = "Shield"
        self.x_pos = float(x_pos)
        self.y_pos = float(y_pos)
        self.speed = float(motor_speed)
        self.active = False
        self.moving = False

    def lower(self):
        self.y_pos = max(SHIELD_Y_MIN, self.y_pos - self.speed)
        if not self.moving:
            print("Shield lowering...")

    def lift(self):
        self.y_pos = min(SHIELD_Y_MAX, self.y_pos + self.speed)
        if not self.moving:
            print("Shield lifting...")

    def turn_on(self):
        if not self.active:
            self.active = True
            print("Suction on")

    def turn_off(self):
        if self.active:
            self.active = False
            print("Suction off")


# Sensores -------------------------------------------------------------------
class Sensor:
    def __init__(self, x_pos, y_pos):
        self.x_pos = float(x_pos)
        self.y_pos = float(y_pos)
        self.status = False

    def check(self, elem):
        # Tolerância numérica pequena para lidar com ponto flutuante em velocidades degradadas
        self.status = (
            abs(elem.x_pos - self.x_pos) < 1e-6
            and abs(elem.y_pos - self.y_pos) < 1e-6
        )
        if self.status:
            is_piece = not hasattr(elem, 'moving')
            if is_piece or elem.moving:
                print(f"Sensor detected an element: {elem.name}")
        return self.status


# Loop Principal -------------------------------------------------------------
if __name__ == "__main__":

    conveyor_belt = ConveyorBelt(length=100, motor_speed=MOTOR_SPEED)
    burner = Burner()
    base = WorkBase(
        x_pos=MACHINE_X_POS,
        y_pos=BASE_Y_MIN,
        motor_speed=NOMINAL_BASE_SPEED,
        rotation_speed=18,  # 360 graus a cada 10s
    )
    shield = SuctionShield(
        x_pos=MACHINE_X_POS,
        y_pos=SHIELD_Y_MAX,
        motor_speed=NOMINAL_SHIELD_SPEED,  # 4 cm/s
    )
    piece_pos_sensor = Sensor(x_pos=MACHINE_X_POS, y_pos=BASE_Y_MIN)
    shield_endcourse_sensor = Sensor(x_pos=MACHINE_X_POS, y_pos=SHIELD_Y_MIN)
    shield_begincourse_sensor = Sensor(x_pos=MACHINE_X_POS, y_pos=SHIELD_Y_MAX)
    base_endcourse_sensor = Sensor(x_pos=MACHINE_X_POS, y_pos=BASE_Y_MAX)
    base_begincourse_sensor = Sensor(x_pos=MACHINE_X_POS, y_pos=BASE_Y_MIN)

    faults = FaultProfile(FAULTS_FILE)

    piece_id = 1
    workpiece = None
    time_counter = 0.0

    prev_coils = [False] * 7

    try:
        tcp_client = connect_tcp_client(TCP_IP)

        print("Waiting for OpenPLC Runtime (1s)...")
        sleep(1)

        # Reseta as coils de comando (%QX) e os registradores de estado (%MW) no OpenPLC
        try:
            tcp_client.write_coils(address=RESET_COIL, values=[True])
            sleep(0.2)
            tcp_client.write_coils(address=RESET_COIL, values=[False])
            print("PLC coils and registers reset")
        except ConnectionException:
            print("Error resetting PLC coils/registers")

        print("Starting simulation...")

        conveyor_belt.add_piece(Workpiece(piece_id))

        while True:
            # Verifica se o usuário injetou ou removeu alguma falha via inject_fault.py
            faults.reload_if_changed()
            base.motor_speed = NOMINAL_BASE_SPEED * faults.base_speed_factor
            shield.speed = NOMINAL_SHIELD_SPEED * faults.shield_speed_factor

            if not tcp_client.is_socket_open():
                print("TCP disconnected, reconnecting...")
                tcp_client.close()
                tcp_client = connect_tcp_client(TCP_IP)

            # --- Atualiza sensores (Python -> PLC) ---
            real_piece_sensor = False

            for piece in conveyor_belt.workpieces:
                if piece_pos_sensor.check(piece):
                    if workpiece is None or workpiece.ID != piece.ID:
                        workpiece = piece
                    else:
                        workpiece.welded = True
                        print(f"Piece ID {workpiece.ID} successfully welded!")
                    real_piece_sensor = True
                    break

            sensor_values = [
                faults.apply_sensor("piece_pos", real_piece_sensor),
                faults.apply_sensor("base_up", base_endcourse_sensor.check(base)),
                faults.apply_sensor("base_down", base_begincourse_sensor.check(base)),
                faults.apply_sensor("shield_down", shield_endcourse_sensor.check(shield)),
                faults.apply_sensor("shield_up", shield_begincourse_sensor.check(shield)),
            ]

            tcp_client.write_coils(address=SENSOR_COILS_START, values=sensor_values)

            sleep(TIME_SKIP)

            # --- Lê comandos do PLC (PLC -> Python) ---
            result = None
            try:
                result = tcp_client.read_coils(address=PLC_COILS_START, count=7)
                if result:
                    coils = result.bits
                    prev_coils = coils
                else:
                    coils = prev_coils
            except ConnectionException:
                print("Error reading PLC coils")
                coils = prev_coils

            # Comando da Esteira
            conveyor_belt.motor_on() if coils[0] else conveyor_belt.motor_off()

            # Preparação para soldagem
            if coils[1]:
                base.lift(workpiece)

            if coils[4]:
                shield.lower()

            # Soldagem
            burner.ignite() if coils[6] else burner.extinguish()
            if coils[2]:
                base.rotate(workpiece)

            # Finalização da soldagem
            if coils[3]:
                base.lower(workpiece)

            if coils[5]:
                shield.lift()

            base.moving = bool(coils[1] or coils[3])
            shield.moving = bool(coils[4] or coils[5])

            # Adiciona novas peças automaticamente na esteira
            if conveyor_belt.workpieces and conveyor_belt.workpieces[0].x_pos >= 25:
                piece_id += 1
                conveyor_belt.add_piece(Workpiece(piece_id))
                print(f"New piece added: ID {piece_id}")

            sleep(TIME_SKIP)

            # Atualiza o passo da simulação
            conveyor_belt.update(jammed=faults.conveyor_jam)
            time_counter += TIME_SKIP * 2

            if time_counter % 1 == 0:
                print(f"\n\n[Time: {time_counter}s]")
                for piece in conveyor_belt.workpieces:
                    print(piece.state())

    except KeyboardInterrupt:
        print("\nClosing simulation...")

    finally:
        tcp_client.close()
        print("TCP closed")
