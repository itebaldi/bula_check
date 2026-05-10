"""
chatbot/app.py
--------------
Interface de chatbot local com Gradio
Mantém estado de conversa entre turnos

Uso:
    python -m bulacheck.chatbot.app
    python -m bulacheck.chatbot.app --provider anthropic --model claude-3-5-haiku-20241022
    python -m bulacheck.chatbot.app --provider ollama --model llama3.2
"""

import argparse
import os
from pathlib import Path

import gradio as gr
from langchain_core.messages import HumanMessage

from bula_check.agents.pipeline import build_graph
from bula_check.agents.pipeline import make_initial_state
from bula_check.agents.protocol import BulaCheckConfig
from bula_check.agents.protocol import LLMProvider


def _make_session(config: BulaCheckConfig) -> dict:
    return {
        "graph": build_graph(config),
        "state": make_initial_state(config),
        "cfg": config,
    }


def respond(
    user_message: str,
    chat_history: list[tuple[str, str]],
    session: dict,
) -> tuple[str, list[tuple[str, str]], dict]:
    """Processa uma mensagem do usuário e retorna a resposta do BulaCheck."""
    if not user_message.strip():
        return "", chat_history, session

    graph = session["graph"]
    state = session["state"]

    state["messages"].append(HumanMessage(content=user_message))

    try:
        new_state = graph.invoke(state)
    except Exception as e:
        bot_response = f"Erro interno: {e}"
        chat_history.append((user_message, bot_response))
        return "", chat_history, session

    ai_messages = [
        m for m in new_state["messages"] if not isinstance(m, HumanMessage)
    ]
    if ai_messages:
        bot_response = ai_messages[-1].content
    else:
        bot_response = "Não consegui processar sua solicitação."

    session["state"] = new_state
    chat_history.append((user_message, bot_response))

    return "", chat_history, session


def reset_session(session: dict) -> tuple[list, dict]:
    """Reinicia a conversa."""
    cfg = session["cfg"]
    new_session = _make_session(cfg)
    return [], new_session


################################################################### UI Gradio

WELCOME_MESSAGE = """
# 💊 BulaCheck

Verificador de alegações e perguntas sobre medicamentos com base em bulas oficiais.

**Como usar:**
- Faça uma pergunta: *"Tylenol pode causar náusea?"*
- Verifique uma alegação: *"Dipirona não tem contraindicações"*
- Pergunte sobre dosagem: *"Qual a dose máxima de paracetamol?"*

As respostas são baseadas exclusivamente nas bulas cadastradas na base de dados.
"""


def build_ui(cfg: BulaCheckConfig) -> gr.Blocks:
    with gr.Blocks(
        title="BulaCheck",
        theme=gr.themes.Soft(
            primary_hue="blue",
            secondary_hue="slate",
        ),
        css="""
        .gradio-container { max-width: 860px !important; margin: auto; }
        .verdict-confirmed { color: #16a34a; font-weight: bold; }
        .verdict-refuted { color: #dc2626; font-weight: bold; }
        .verdict-inconclusive { color: #d97706; font-weight: bold; }
        """,
    ) as demo:
        gr.Markdown(WELCOME_MESSAGE)

        # Estado da sessão (por usuário)
        session_state = gr.State(value=_make_session(cfg))

        chatbot = gr.Chatbot(
            label="BulaCheck",
            height=500,
            bubble_full_width=False,  # type: ignore
            avatar_images=(None, "💊"),
            render_markdown=True,
        )

        with gr.Row():
            msg_input = gr.Textbox(
                placeholder="Ex: Tylenol pode ser tomado em jejum?",
                label="Sua pergunta ou alegação",
                scale=5,
                autofocus=True,
            )
            send_btn = gr.Button("Enviar", variant="primary", scale=1)

        with gr.Row():
            clear_btn = gr.Button("🗑️ Nova conversa", variant="secondary")
            gr.Markdown(
                "⚕️ *As informações são baseadas em bulas e não substituem orientação médica.*",
                elem_classes=["text-sm"],
            )

        # Configurações visíveis (somente leitura)
        with gr.Accordion("⚙️ Configuração atual", open=False):
            gr.Markdown(
                f"""
- **Provedor LLM:** `{cfg.llm_provider.value}`
- **Modelo:** `{cfg.llm_model}`
- **Banco BulaGratis:** `{cfg.bulagratis_db_path}`
- **Banco ANVISA:** `{cfg.anvisa_db_path}`
- **Top-K chunks:** `{cfg.top_k_chunks}`
- **Peso lexical / semântico:** `{cfg.lexical_weight} / {cfg.semantic_weight}`
                """
            )

        # Eventos
        send_btn.click(
            respond,
            inputs=[msg_input, chatbot, session_state],
            outputs=[msg_input, chatbot, session_state],
        )
        msg_input.submit(
            respond,
            inputs=[msg_input, chatbot, session_state],
            outputs=[msg_input, chatbot, session_state],
        )
        clear_btn.click(
            reset_session,
            inputs=[session_state],
            outputs=[chatbot, session_state],
        )

    return demo


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="BulaCheck — chatbot local")
    parser.add_argument(
        "--provider",
        choices=["openai", "anthropic", "ollama"],
        default="openai",
        help="Provedor LLM (default: openai)",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Modelo LLM (default: gpt-4o-mini / claude-3-5-haiku / llama3.2)",
    )
    parser.add_argument(
        "--db", default="bulas.db", help="Caminho para o banco BulaGratis"
    )
    parser.add_argument(
        "--anvisa-db", default="anvisa.db", help="Caminho para o banco ANVISA"
    )
    parser.add_argument(
        "--port", type=int, default=7860, help="Porta do servidor Gradio"
    )
    parser.add_argument(
        "--share", action="store_true", help="Gera link público Gradio"
    )
    args = parser.parse_args()

    # Modelo padrão por provedor
    default_models = {
        "openai": "gpt-4o-mini",
        "anthropic": "claude-3-5-haiku-20241022",
        "ollama": "llama3.2",
    }
    model = args.model or default_models[args.provider]

    cfg = BulaCheckConfig(
        llm_provider=LLMProvider(args.provider),
        llm_model=model,
        bulagratis_db_path=Path(args.db),
        anvisa_db_path=Path(args.anvisa_db),
        decs_api_key=os.getenv("DECS_API_KEY"),
        obm_token=os.getenv("OBM_TOKEN"),
    )

    print(f"\n🔬 BulaCheck iniciando com {args.provider}/{model}")
    print(f"   Banco BulaGratis : {cfg.bulagratis_db_path}")
    print(f"   Banco ANVISA     : {cfg.anvisa_db_path}")
    print(f"   Acesse em        : http://localhost:{args.port}\n")

    ui = build_ui(cfg)
    ui.launch(
        server_port=args.port,
        share=args.share,
        show_error=True,
    )


if __name__ == "__main__":
    main()
