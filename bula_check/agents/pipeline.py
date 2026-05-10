from langgraph.graph import END
from langgraph.graph import START
from langgraph.graph import StateGraph

from bula_check.agents.nodes import node_expand_decs
from bula_check.agents.nodes import node_fetch_chunks
from bula_check.agents.nodes import node_find_medicine
from bula_check.agents.nodes import node_parse_query
from bula_check.agents.nodes import node_suggest_similar
from bula_check.agents.nodes import node_verify_claim
from bula_check.agents.protocol import BulaCheckConfig
from bula_check.agents.protocol import BulaCheckState


def build_graph(cfg: BulaCheckConfig) -> StateGraph:
    """
    Constrói e compila o grafo LangGraph com a configuração fornecida.

    Parameters
    ----------
    cfg : BulaCheckConfig

    Returns
    -------
    CompiledGraph
        Grafo compilado pronto para invocar com `graph.invoke(state)`.

    Examples
    --------
    ::

        from bula_check.agents.protocol import DEFAULT_CONFIG

        cfg: BulaCheckConfig = {**DEFAULT_CONFIG, "llm_model": "gpt-4o-mini"}
        graph = build_graph(cfg)
        result = graph.invoke(initial_state)
    """

    builder = StateGraph(BulaCheckState)

    builder.add_node("parse_query", node_parse_query)
    builder.add_node("expand_decs", node_expand_decs)
    builder.add_node("find_medicine", node_find_medicine)
    builder.add_node("fetch_chunks", node_fetch_chunks)
    builder.add_node("verify_claim", node_verify_claim)
    builder.add_node("suggest_similar", node_suggest_similar)

    builder.add_edge(START, "parse_query")
    builder.add_edge("parse_query", "expand_decs")
    builder.add_edge("expand_decs", "find_medicine")
    builder.add_edge("fetch_chunks", "verify_claim")
    builder.add_edge("verify_claim", END)

    # condicionais
    builder.add_conditional_edges(
        "find_medicine",
        _route_after_find_medicine,
        {
            "fetch_chunks": "fetch_chunks",
            "suggest_similar": "suggest_similar",
        },
    )
    builder.add_conditional_edges(
        "suggest_similar",
        _route_after_suggest,
        {
            "fetch_chunks": "fetch_chunks",
            END: END,
        },
    )

    return builder.compile()  # type: ignore


def _route_after_find_medicine(state: BulaCheckState) -> str:
    """Decide o próximo passo após a busca do medicamento."""
    # aguardando confirmação de sugestão anterior
    if state.get("awaiting_user_confirmation"):
        return "suggest_similar"

    # medicamento já selecionado (confirmação chegou)
    if state.get("selected_medicine"):
        return "fetch_chunks"

    # nenhum candidato, vai para sugestão
    return "suggest_similar"


def _route_after_suggest(state: BulaCheckState) -> str:
    """Após sugestão: se confirmado, busca chunks; senão encerra."""
    if state.get("selected_medicine"):
        return "fetch_chunks"
    return END


def make_initial_state(cfg: BulaCheckConfig) -> BulaCheckState:
    """Cria o estado inicial do grafo."""
    return BulaCheckState(
        messages=[],
        parsed_query=None,
        medicine_candidates=[],
        selected_medicine=None,
        similar_medicines=[],
        decs_keywords=[],
        retrieved_chunks=[],
        verification_result=None,
        search_attempted_bulagratis=False,
        search_attempted_anvisa=False,
        awaiting_user_confirmation=False,
        suggested_medicine_name=None,
        config=cfg,
    )
