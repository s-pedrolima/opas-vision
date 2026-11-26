"""
Servidor OPC UA da camada OCF (O-PAS Connectivity Framework).

Servidor leve, versionável e 100% em Python usando a biblioteca `asyncua`.

Papel na arquitetura:
    OpenPLC (Modbus TCP) -> Node-RED (Gateway) -> [Este Servidor] -> Historian / AI Agent
"""

import asyncio
import logging
from asyncua import Server, ua

# ---------------------------------------------------------------------------
# Configurações do Servidor
# ---------------------------------------------------------------------------
# 0.0.0.0 faz o servidor escutar em TODAS as interfaces de rede da máquina
# (localhost, rede Wi-Fi/Ethernet e a ponte bridge do Docker).
# **OBRIGATÓRIO**, pois o Node-RED roda dentro de um container Docker
# e acessa o host via `host.docker.internal:4840`.
ENDPOINT = "opc.tcp://0.0.0.0:4840/OPCUA/SimulationServer"

# URI que identifica o nosso "dicionário" (namespace) de variáveis do projeto.
# No OPC UA:
#   ns=0 -> nós padrão da OPC Foundation (Objects, Types, etc.)
#   ns=1 -> nós internos do próprio servidor
#   ns=2 -> primeiro namespace customizado que registramos
NAMESPACE_URI = "urn:opas:welding-station"

# Deve bater com a variável de ambiente DCN_ID no WeldingStation.yml
DCN_ID = "DCN-WS"

# Mapeamento das 13 variáveis publicadas pelo Node-RED (flows-sm-ws.json):
# (nome_da_tag, tipo_opcua, valor_inicial)
PROCESS_VARIABLES = [
    # Estado da máquina de estados (FSM: 0..4) — Holding Register %MW0
    ("Machine_State", ua.VariantType.Int16, 0),

    # Atuadores (coils 0..6: OpenPLC -> Simulação)
    ("Conveyor", ua.VariantType.Boolean, False),
    ("Base_Up", ua.VariantType.Boolean, False),
    ("Base_Rotate", ua.VariantType.Boolean, False),
    ("Base_Down", ua.VariantType.Boolean, False),
    ("Shield_Down", ua.VariantType.Boolean, False),
    ("Shield_Up", ua.VariantType.Boolean, False),
    ("Burner", ua.VariantType.Boolean, False),

    # Sensores de fim de curso / presença (coils 7..11: Simulação -> OpenPLC)
    ("Piece_Pos_Sensor", ua.VariantType.Boolean, False),
    ("Base_Up_Sensor", ua.VariantType.Boolean, False),
    ("Base_Down_Sensor", ua.VariantType.Boolean, False),
    ("Shield_Down_Sensor", ua.VariantType.Boolean, False),
    ("Shield_Up_Sensor", ua.VariantType.Boolean, False),
]


async def build_server() -> tuple[Server, dict[str, object]]:
    """Instancia o servidor OPC UA e monta a árvore de nós sob Objects/DCN-WS."""
    server = Server()
    await server.init()

    server.set_endpoint(ENDPOINT)
    server.set_server_name("O-PAS OCF OPC UA Server")

    # Modo de desenvolvimento local sem certificados X.509 (alinhado ao Node-RED).
    # Em produção OT (IEC 62443), usaríamos Basic256Sha256 + SignAndEncrypt.
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])

    # 1. Registra o namespace customizado.
    ns_idx = await server.register_namespace(NAMESPACE_URI)  # ns_idx = 2 (primeiro registro)

    # 2. Cria uma pasta dentro do nó raiz `Objects`.
    objects_node = server.nodes.objects
    dcn_folder = await objects_node.add_folder(
        f"ns={ns_idx};s={DCN_ID}",  # NodeId explícito em string
        DCN_ID,                     # BrowseName (nome legível na árvore)
    )

    # 3. Cria cada uma das 13 variáveis dentro da pasta `DCN-WS`.
    nodes: dict[str, object] = {}
    for var_name, variant_type, initial_value in PROCESS_VARIABLES:
        # NodeId no formato exato que o Node-RED envia no msg.topic:
        # ex.: "ns=2;s=DCN-WS/Machine_State", "ns=2;s=DCN-WS/Burner", etc.
        node_id = f"ns={ns_idx};s={DCN_ID}/{var_name}"

        var_node = await dcn_folder.add_variable(
            node_id,
            var_name,
            ua.Variant(initial_value, variant_type),
            varianttype=variant_type,
        )

        # Por padrão, variáveis OPC UA são somente-leitura para clientes externos.
        # set_writable() libera o Node-RED para publicar os valores vindos do OpenPLC.
        await var_node.set_writable()
        nodes[var_name] = var_node

    # Retorna o servidor e o dicionário com os nós das variáveis.
    return server, nodes


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    # Reduz ruído de logs internos do asyncua no terminal
    logging.getLogger("asyncua").setLevel(logging.WARNING)

    server, nodes = await build_server()

    logging.info("OPC UA server ready at %s", ENDPOINT)
    logging.info("Namespace registered: ns=2 (%s)", NAMESPACE_URI)
    logging.info("%d variables created under Objects/%s", len(nodes), DCN_ID)

    # O context manager `async with server:` inicia o servidor na porta 4840
    # e garante encerramento limpo ao pressionar Ctrl+C.
    async with server:
        last_state = None
        while True:
            # Pequeno log de acompanhamento: imprime sempre que Machine_State muda
            state = await nodes["Machine_State"].read_value()
            if state != last_state:
                burner = await nodes["Burner"].read_value()
                conveyor = await nodes["Conveyor"].read_value()
                logging.info(
                    "[%s] Machine_State=%s | Conveyor=%s | Burner=%s",
                    DCN_ID,
                    state,
                    conveyor,
                    burner,
                )
                last_state = state

            await asyncio.sleep(0.5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nOPC UA server stopped.")
