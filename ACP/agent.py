"""
Agent v0 da camada ACP (Advanced Computing Platform) — Python puro + OpenAI SDK.

Este arquivo implementa o "coração" de um Agente de IA sem caixas-pretas:
um loop `while` que conversa com a LLM, executa as tools reais que ela pedir
(OPC UA, Historian SQLite, lógica IEC 61131-3) e devolve os resultados até ela
ter evidências suficientes para responder.

Como rodar:
    # Modo chat interativo:
    ./.venv/bin/python ACP/agent.py

    # Pergunta única direta:
    ./.venv/bin/python ACP/agent.py "Qual o desempenho histórico da máquina e qual o maior gargalo?"

    # Modo detalhado (mostra o JSON completo retornado por cada tool):
    ./.venv/bin/python ACP/agent.py --verbose
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from openai import OpenAI

# Permite rodar tanto `python ACP/agent.py` quanto `python -m ACP.agent`
try:
    from ACP.tools import TOOL_SCHEMAS, execute_tool
except ImportError:
    from tools import TOOL_SCHEMAS, execute_tool

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# System Prompt: define papel, regras de investigação e limites do agente
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """
Você é um Engenheiro de Confiabilidade e Automação Industrial (OT) operando na
camada ACP (Advanced Computing Platform) de um sistema aberto baseado no padrão
O-PAS. Você monitora uma Estação de Soldagem (Welding Station, DCN-WS) controlada
por OpenPLC (IEC 61131-3) e integrada via OPC UA + Historian (SQLite).

Regras obrigatórias de conduta:
1. NUNCA chute valores de sensores, estados da máquina, tempos de ciclo ou trechos
   do código PLC. Sempre use as ferramentas disponíveis para obter fatos.
2. Chame APENAS as ferramentas necessárias para responder à pergunta:
   - Pergunta hipotética ou puramente sobre lógica de controle ("se o sensor X falhar..."):
     basta ler `read_plc_logic` e deixar claro que se trata de análise hipotética (não
     afirme que a falha está acontecendo agora, a menos que o usuário peça status ao vivo).
   - Pergunta de desempenho histórico / gargalos: consulte `get_state_statistics` (e
     `read_plc_logic` se precisar explicar o porquê de um tempo fixo como `T_Weld`).
   - Investigação de incidente, alarme ou status ao vivo: cruze `get_live_machine_state`,
     `get_recent_cycles`, `get_state_statistics` e `read_plc_logic`.
3. Rastreie o código Structured Text (IEC 61131-3) linha por linha dentro de um ciclo
   de varredura (scan cycle): lembre-se que atribuições posteriores (ex.:
   `IF Base_Up_Sensor THEN Base_Up := FALSE;`) sobrescrevem atribuições anteriores
   (`Base_Up := TRUE;`) no mesmo estado. Só afirme que um atuador permanece ligado
   se o seu sensor de fim de curso correspondente NÃO estiver ativo.
4. Interpretação de dados do Historian:
   - Registros com `close_reason = "shutdown"` ou `"partial_startup"` significam que
     o serviço Historian foi iniciado ou encerrado no meio do estado — NÃO são falhas
     físicas da máquina nem instabilidade do processo.
   - Se uma ferramenta retornar erro (ex.: servidor OPC UA offline), trate esse erro
     como evidência real no diagnóstico, sem inventar leituras.
5. Responda sempre em português claro, objetivo e técnico, estruturando diagnósticos
   com: Situação / Evidências / Causa Raiz (ou Gargalo) / Recomendação.
6. Você tem acesso SOMENTE LEITURA ao sistema OT. Nunca afirme que executou ações
   físicas na máquina ou no Docker.
""".strip()

# Guardrail de segurança: impede que a LLM entre em loop infinito chamando tools
MAX_AGENT_STEPS = 8


# ---------------------------------------------------------------------------
# Configuração do cliente OpenAI-compatible (Upstage / Groq / OpenRouter / etc.)
# ---------------------------------------------------------------------------
def build_client() -> tuple[OpenAI, str]:
    """Lê o arquivo .env na raiz do projeto e retorna (client, model_name)."""
    load_dotenv(PROJECT_ROOT / ".env")

    api_key = os.getenv("LLM_API_KEY", "").strip()
    base_url = os.getenv("LLM_BASE_URL", "https://api.upstage.ai/v1").strip()
    model = os.getenv("LLM_MODEL", "solar-mini-4").strip()

    if not api_key:
        print(
            "[ERRO] Configure LLM_API_KEY no arquivo .env antes de rodar o agente.\n"
            "       Veja o modelo em .env.example.",
            file=sys.stderr,
        )
        sys.exit(1)

    client = OpenAI(api_key=api_key, base_url=base_url)
    return client, model


# ---------------------------------------------------------------------------
# O Agent Loop (o núcleo de qualquer agente de IA!)
# ---------------------------------------------------------------------------
def _print_token_usage(prompt_tokens: int, completion_tokens: int) -> None:
    if prompt_tokens or completion_tokens:
        print(
            f"\n[tokens] prompt={prompt_tokens} | "
            f"completion={completion_tokens} | "
            f"total={prompt_tokens + completion_tokens}"
        )


def run_agent_turn(
    client: OpenAI,
    model: str,
    messages: list[dict[str, Any]],
    user_input: str,
    verbose: bool = False,
) -> str:
    """
    Executa um turno completo de conversa (pergunta do usuário -> 0..N chamadas
    de tools -> resposta final fundamentada da LLM).
    """
    messages.append({"role": "user", "content": user_input})

    total_prompt_tokens = 0
    total_completion_tokens = 0

    for step in range(1, MAX_AGENT_STEPS + 1):
        # 1. Envia todo o histórico + o "cardápio" de tools para a LLM decidir o próximo passo
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            tools=TOOL_SCHEMAS,
            tool_choice="auto",  # A própria LLM decide se chama tool(s) ou já responde
            temperature=0.2,     # Temperatura baixa = mais precisão técnica, menos "criatividade"
        )

        # Contabiliza consumo de tokens (ótimo para aprender custo de agentes!)
        if response.usage:
            total_prompt_tokens += response.usage.prompt_tokens or 0
            total_completion_tokens += response.usage.completion_tokens or 0

        assistant_msg = response.choices[0].message

        # 2. Caso A: a LLM NÃO pediu nenhuma tool -> ela terminou de investigar!
        if not assistant_msg.tool_calls:
            final_text = assistant_msg.content or "(Sem resposta textual da LLM)"
            messages.append({"role": "assistant", "content": final_text})
            _print_token_usage(total_prompt_tokens, total_completion_tokens)
            return final_text

        # 3. Caso B: a LLM pediu 1 ou mais tools (suporta parallel tool calls!).
        #    REGRA DO PROTOCOLO: primeiro gravamos a mensagem do assistente contendo
        #    os pedidos `tool_calls`, e logo depois gravamos uma mensagem `role: "tool"`
        #    para CADA `tool_call.id`, senão a API rejeita o histórico.
        messages.append(
            {
                "role": "assistant",
                "content": assistant_msg.content or "",
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        },
                    }
                    for tc in assistant_msg.tool_calls
                ],
            }
        )

        # 4. Executa cada tool localmente no nosso Python e devolve o JSON pro histórico
        for tc in assistant_msg.tool_calls:
            tool_name = tc.function.name
            raw_args = tc.function.arguments or "{}"

            print(f"  [passo {step}] -> chamando tool: {tool_name}({raw_args})")

            try:
                parsed_args = json.loads(raw_args)
            except json.JSONDecodeError as exc:
                tool_result = json.dumps(
                    {"error": f"Argumentos JSON inválidos enviados pela LLM: {exc}"},
                    ensure_ascii=False,
                )
            else:
                tool_result = execute_tool(tool_name, parsed_args)

            if verbose:
                print(f"  [resultado {tool_name}]:\n{tool_result}\n")
            else:
                # Resumo curto de 1 linha para acompanhar o raciocínio sem poluir o terminal
                preview = tool_result.replace("\n", " ")
                if len(preview) > 120:
                    preview = preview[:117] + "..."
                print(f"             <- {preview}")

            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": tool_result,
                }
            )

        # Ao chegar aqui, o `for` volta ao topo e manda os resultados das tools para a LLM!

    # Síntese graciosa: se consumiu todos os passos chamando tools, faz 1 chamada final
    # SEM tools para obrigar o modelo a sintetizar tudo o que já coletou (em vez de
    # jogar fora o trabalho dos 8 passos!).
    print(f"  [limite {MAX_AGENT_STEPS} passos] -> solicitando síntese final sem novas tools...")
    synthesis_messages = [
        *messages,
        {
            "role": "user",
            "content": (
                "Limite de consultas de ferramentas atingido. Com base exclusivamente "
                "nas evidências já coletadas acima, apresente agora sua síntese e "
                "diagnóstico técnico final estruturado."
            ),
        },
    ]
    try:
        synth_response = client.chat.completions.create(
            model=model,
            messages=synthesis_messages,
            temperature=0.2,
        )
        if synth_response.usage:
            total_prompt_tokens += synth_response.usage.prompt_tokens or 0
            total_completion_tokens += synth_response.usage.completion_tokens or 0

        final_text = (synth_response.choices[0].message.content or "").strip()
        if final_text:
            messages.append({"role": "assistant", "content": final_text})
            _print_token_usage(total_prompt_tokens, total_completion_tokens)
            return final_text
    except Exception as exc:
        print(f"  [erro na síntese final]: {exc}", file=sys.stderr)

    _print_token_usage(total_prompt_tokens, total_completion_tokens)
    return (
        f"[AVISO] O agente atingiu o limite máximo de {MAX_AGENT_STEPS} passos "
        "sem concluir uma resposta final."
    )


# ---------------------------------------------------------------------------
# Interface de linha de comando (chat interativo ou pergunta única)
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="O-PAS ACP AI Agent (Welding Station)")
    parser.add_argument(
        "prompt",
        nargs="?",
        help="Pergunta única opcional (se omitida, abre chat interativo)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Mostra o JSON completo retornado por cada tool",
    )
    args = parser.parse_args()

    client, model = build_client()

    # O histórico de mensagens começa SEMPRE com o System Prompt.
    # É mantendo essa lista viva entre turnos que o agente tem "memória de curto prazo"!
    messages: list[dict[str, Any]] = [{"role": "system", "content": SYSTEM_PROMPT}]

    # Modo pergunta única (ex.: python ACP/agent.py "Como está a máquina?")
    if args.prompt:
        print(f"Modelo: {model}\n")
        answer = run_agent_turn(client, model, messages, args.prompt, verbose=args.verbose)
        print(f"\n=== Resposta do Agente ===\n{answer}\n")
        return

    # Modo chat interativo
    print(f"=== O-PAS ACP Agent ({model}) ===")
    print("Comandos: '/quit' para encerrar | '/reset' para limpar memória da conversa\n")

    while True:
        try:
            user_input = input("Você > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nEncerrando agente.")
            break

        if not user_input:
            continue
        if user_input.lower() == "/quit":
            break
        if user_input.lower() == "/reset":
            messages = [{"role": "system", "content": SYSTEM_PROMPT}]
            print("[memória da conversa limpa]\n")
            continue

        print()
        answer = run_agent_turn(client, model, messages, user_input, verbose=args.verbose)
        print(f"\nAgente >\n{answer}\n")


if __name__ == "__main__":
    main()
