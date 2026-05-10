"""
chatbot/app.py
--------------
Interface de chatbot local com Gradio.
Mantém estado de conversa entre turnos.

Uso:
    python -m bula_check.agents.app
    python -m bula_check.agents.app --provider anthropic --model claude-3-5-haiku-20241022
    python -m bula_check.agents.app --provider ollama --model llama3.2
"""

import argparse
import os
import traceback

import gradio as gr
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage

from bula_check.agents.pipeline import build_graph
from bula_check.agents.pipeline import make_initial_state
from bula_check.agents.protocol import DEFAULT_CONFIG
from bula_check.agents.protocol import BulaCheckConfig
from bula_check.agents.protocol import BulaCheckState
from bula_check.agents.protocol import LLMProvider

load_dotenv()
WELCOME_MESSAGE = """
# 💊 BulaCheck

Verificador de alegações e perguntas sobre medicamentos com base em bulas oficiais.

**Como usar:**
- Faça uma pergunta: *"Tylenol pode causar náusea?"*
- Verifique uma alegação: *"Dipirona não tem contraindicações"*
- Pergunte sobre dosagem: *"Qual a dose máxima de paracetamol?"*

As respostas são baseadas exclusivamente nas bulas cadastradas na base de dados.
"""


def _append_chat_message(
    chat_history: list[dict[str, str]],
    role: str,
    content: str,
) -> None:
    chat_history.append(
        {
            "role": role,
            "content": content,
        }
    )


def build_ui(cfg: BulaCheckConfig) -> gr.Blocks:
    """
    Build the Gradio UI.

    Important:
    The compiled LangGraph is kept outside gr.State. gr.State stores only the
    conversation state, because storing complex objects there can break Gradio /
    LangGraph serialization.
    """
    graph = build_graph(cfg)

    def respond(
        user_message: str,
        chat_history: list[dict[str, str]],
        state: BulaCheckState,
    ) -> tuple[str, list[dict[str, str]], BulaCheckState]:
        """Processa uma mensagem do usuário e retorna a resposta do BulaCheck."""
        if not user_message.strip():
            return "", chat_history, state

        state["messages"].append(HumanMessage(content=user_message))

        try:
            new_state = graph.invoke(state)  # type: ignore

        except Exception as error:
            traceback.print_exc()

            bot_response = f"Erro interno: {error}"

            _append_chat_message(chat_history, "user", user_message)
            _append_chat_message(chat_history, "assistant", bot_response)

            return "", chat_history, state

        ai_messages = [
            message
            for message in new_state["messages"]
            if not isinstance(message, HumanMessage)
        ]

        if ai_messages:
            bot_response = str(ai_messages[-1].content)
        else:
            bot_response = "Não consegui processar sua solicitação."

        _append_chat_message(chat_history, "user", user_message)
        _append_chat_message(chat_history, "assistant", bot_response)

        return "", chat_history, new_state

    def reset_session(
        state: BulaCheckState,
    ) -> tuple[list[dict[str, str]], BulaCheckState]:
        """Reinicia a conversa."""
        return [], make_initial_state(cfg)

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

        session_state = gr.State(value=make_initial_state(cfg))

        chatbot = gr.Chatbot(
            label="BulaCheck",
            height=500,
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

        with gr.Accordion("⚙️ Configuração atual", open=False):
            gr.Markdown(
                f"""
- **Provedor LLM:** `{cfg["llm_provider"].value}`
- **Modelo:** `{cfg["llm_model"]}`
- **Banco BulaGratis:** `{cfg["bulagratis_db_path"]}`
- **Banco ANVISA:** `{cfg["anvisa_db_path"]}`
- **Top-K chunks:** `{cfg["top_k_chunks"]}`
- **Peso lexical / semântico:** `{cfg["lexical_weight"]} / {cfg["semantic_weight"]}`
                """
            )

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


def main() -> None:
    parser = argparse.ArgumentParser(description="BulaCheck — chatbot local")

    parser.add_argument(
        "--provider",
        choices=["openai", "anthropic", "ollama"],
        default="openai",
        help="Provedor LLM.",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Modelo LLM.",
    )
    parser.add_argument(
        "--db",
        default="bulas.db",
        help="Caminho para o banco BulaGratis.",
    )
    parser.add_argument(
        "--anvisa-db",
        default="anvisa.db",
        help="Caminho para o banco ANVISA.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=7860,
        help="Porta do servidor Gradio.",
    )
    parser.add_argument(
        "--share",
        action="store_true",
        help="Gera link público Gradio.",
    )

    args = parser.parse_args()

    default_models = {
        "openai": "gpt-4o-mini",
        "anthropic": "claude-3-5-haiku-20241022",
        "ollama": "llama3.2",
    }

    model = args.model or default_models[args.provider]

    cfg: BulaCheckConfig = {
        **DEFAULT_CONFIG,
        "llm_provider": LLMProvider(args.provider),
        "llm_model": model,
    }

    print(f"\n🔬 BulaCheck iniciando com {args.provider}/{model}")
    print(f"   Banco BulaGratis : {cfg['bulagratis_db_path']}")
    print(f"   Banco ANVISA     : {cfg['anvisa_db_path']}")
    print(f"   Acesse em        : http://localhost:{args.port}\n")

    ui = build_ui(cfg)

    ui.launch(
        server_port=args.port,
        share=args.share,
        show_error=True,
    )


if __name__ == "__main__":
    main()
