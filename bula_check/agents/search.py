import json
import math
import re
import sqlite3
from difflib import SequenceMatcher
from typing import Any

from nemo.preprocessing.text import normalize_text_whitespace
from nemo.preprocessing.text import remove_text_accents
from nemo.preprocessing.text import uppercase_text
from nemo.vector_retrieval.indexing import Document
from nemo.vector_retrieval.indexing import gen_inverted_index
from nemo.vector_retrieval.query import Query
from nemo.vector_retrieval.search import search
from nemo.vector_retrieval.tf_idf import VectorModel
from nemo.vector_retrieval.tf_idf import gen_vector_space_model
from toolz.functoolz import pipe

from bula_check.agents.protocol import BulaCheckConfig
from bula_check.agents.protocol import ChunksDict
from bula_check.agents.protocol import MedicineCandidate
from bula_check.agents.protocol import MedicinesDict
from bula_check.agents.protocol import RetrievedChunk


def find_medicine_candidates(
    bulagratis_conn: sqlite3.Connection,
    anvisa_conn: sqlite3.Connection | None,
    name: str,
    active_ingredient: str | None,
    cfg: BulaCheckConfig,
) -> list[MedicineCandidate]:
    """
    Estratégia de busca de medicamentos:
    1. Busca no BulaGratis pelo nome
    2. Se vazio e anvisa_conn disponível: busca ANVISA → extrai princípio ativo → re-busca BulaGratis
    3. Filtra resultados que não atingem o threshold de match (no_good_match)
    4. Rankeamento por score lexical
    """
    name_norm = normalize_text(name)
    name_tokens = _split_tex(name_norm)
    ai_norm = normalize_text(active_ingredient) if active_ingredient else None
    ai_tokens = _split_tex(ai_norm) if ai_norm else []

    # BulaGratis
    rows = search_medicines_lexical(
        bulagratis_conn, name, active_ingredient, limit=cfg["top_k_medicines"] * 3
    )
    rows = _filter_low_match(rows, n_tokens=len(name_tokens) + len(ai_tokens))

    # fallback ANVISA, re-busca com princípio ativo. Itera por TODOS os AIs
    # únicos retornados pelo ANVISA para o nome buscado e acumula matches.
    # Cada brand pode aparecer no ANVISA em variantes (ex: TYLENOL puro
    # AI=PARACETAMOL, TYLENOL SINUS AI=PARACETAMOL+PSEUDOEFEDRINA), e
    # medicamentos relevantes de cada variante são candidatos válidos. O
    # _filter_low_match já protege contra ruído em AIs multi-token (exige
    # majoritário) e single-token AIs passam tudo que matchou o ingrediente.
    if not rows and anvisa_conn is not None:
        anvisa_rows = search_medicines_lexical(
            anvisa_conn, name, limit=cfg["top_k_medicines"] * 3
        )
        collected: dict[str, dict[str, Any]] = {}
        ais_used: list[str] = []

        for anvisa_row in anvisa_rows:
            ai_from_anvisa = anvisa_row.get("active_ingredient")
            if not ai_from_anvisa:
                continue
            ai_normalized = normalize_text(ai_from_anvisa)
            if ai_normalized in ais_used:
                continue  # AI já processado nessa iteração

            ai_anvisa_tokens = _split_tex(ai_normalized)
            candidate_rows = search_medicines_lexical(
                bulagratis_conn,
                ai_from_anvisa,
                limit=cfg["top_k_medicines"] * 3,
            )
            candidate_rows = _filter_equivalent_fallback(
                candidate_rows, n_ai_tokens=len(ai_anvisa_tokens)
            )

            if candidate_rows:
                ais_used.append(ai_normalized)
                for r in candidate_rows:
                    if r["id"] not in collected:
                        # tagueia o candidato com o AI (do ANVISA) que o
                        # recuperou; vira alvo de score quando o usuário não
                        # informou princípio ativo. NÃO concatenamos os AIs num
                        # único alvo: isso faria o Jaccard premiar combos (mais
                        # tokens) acima do mono-fármaco pedido.
                        r["_anvisa_ai"] = ai_normalized
                        collected[r["id"]] = r

        if collected:
            rows = list(collected.values())

    if not rows:
        return []

    candidates: list[MedicineCandidate] = []
    for row in rows:
        # alvo do score: AI informado pelo usuário (sinal de intenção puro vs
        # combo) tem precedência; senão o AI do ANVISA que recuperou a row.
        ai_target = ai_norm if ai_norm else row.get("_anvisa_ai")
        score = _score_medicine_match(row, name_norm, ai_target)
        # match_count/_anvisa_ai são metadados da query, não pertencem ao MedicinesDict
        row_clean = {
            k: v for k, v in row.items() if k not in ("match_count", "_anvisa_ai")
        }
        medicine = MedicinesDict(**row_clean)
        candidates.append(
            MedicineCandidate(
                medicine=medicine,
                score=score,
            )
        )

    # score desc; empate -> menos ingredientes primeiro (mono-fármaco canônico
    # vence combo quando não há AI do usuário para desempatar).
    candidates.sort(
        key=lambda candidate: (
            candidate["score"],
            -len(_match_tokens(candidate["medicine"].get("processed_name") or "")),
        ),
        reverse=True,
    )
    return candidates[: cfg["top_k_medicines"]]


def _filter_low_match(
    rows: list[dict[str, Any]], n_tokens: int
) -> list[dict[str, Any]]:
    """
    Descarta rows com match_count abaixo de (n_tokens // 2 + 1) — i.e.
    estritamente mais que a metade dos tokens.

    Threshold por n_tokens:
      - 1 token: sem filtro (todo match conta)
      - 2 tokens: exige 2 (ambos)
      - 3 tokens: exige 2 (majoritário)
      - 4 tokens: exige 3 (majoritário)
      - n tokens: exige n//2 + 1

    Quando há múltiplos tokens (ex: 'paracetamol, fenilefrina, carbinoxamina'),
    medicamentos que só matcham 1 token tendem a ser ruído — só o token comum
    (paracetamol) ressoando. Exigir majoritário evita esse falso positivo sem
    ser estritamente AND (que falharia em variações de grafia).
    """
    if n_tokens <= 1:
        return rows
    min_required = n_tokens // 2 + 1
    return [r for r in rows if r.get("match_count", 0) >= min_required]


def _filter_equivalent_fallback(
    rows: list[dict[str, Any]], n_ai_tokens: int
) -> list[dict[str, Any]]:
    """
    Filtro estrito para uso no fallback ANVISA.

    Além do threshold majoritário sobre match_count, exige *cardinalidade
    igual*: o medicamento em bula_gratis deve ter exatamente n_ai_tokens
    ingredientes (contados via _split_tex sobre o `name` original — que
    preserva separadores '+', ',', ';' que o processed_name perde).

    Garante equivalência funcional, não só substring:
      - AI=PARACETAMOL (1 ingrediente) → só medicamentos mono-paracetamol
        passam ('PARACETAMOL'), nunca 'paracetamol + cafeína' nem
        'cloridrato de tramadol + paracetamol'.
      - AI=PARACETAMOL+PSEUDOEFEDRINA (2 ingredientes) → só medicamentos
        di-ingrediente passam, e o threshold majoritário garante que sejam
        OS ingredientes certos (não 'paracetamol + cafeína' que tem mc=1).
    """
    if not rows:
        return rows

    threshold = n_ai_tokens // 2 + 1 if n_ai_tokens > 1 else 1
    out = []
    for r in rows:
        if r.get("match_count", 0) < threshold:
            continue
        n_ingredients = len(_split_tex(normalize_text(r["name"])))
        if n_ingredients != n_ai_tokens:
            continue
        out.append(r)
    return out


def find_similar_medicines(
    conn: sqlite3.Connection,
    name: str,
    limit: int = 3,
) -> list[MedicineCandidate]:
    """
    Busca medicamentos com nomes parecidos usando similaridade textual.

    Usado como sugestão quando o medicamento não é encontrado exatamente.
    """
    name_norm = normalize_text(name)

    if not name_norm:
        return []

    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT id, name, processed_name, active_ingredient,
               processed_active_ingredient, source, url, registration_number,
               therapeutic_classes, company_name, processed_company_name, cnpj
        FROM medicines
        WHERE processed_name IS NOT NULL
        """
    )

    rows = [dict(row) for row in cursor.fetchall()]

    candidates: list[MedicineCandidate] = []

    for row in rows:
        row_name = row.get("processed_name") or ""

        similarity = SequenceMatcher(
            None,
            name_norm,
            row_name,
        ).ratio()

        token_bonus = 0.0
        for token in row_name.split():
            token_similarity = SequenceMatcher(
                None,
                name_norm,
                token,
            ).ratio()
            token_bonus = max(token_bonus, token_similarity)

        score = max(similarity, token_bonus)

        if score < 0.55:
            continue

        candidates.append(
            MedicineCandidate(
                medicine=MedicinesDict(**row),
                score=score,
            )
        )

    candidates.sort(key=lambda candidate: candidate["score"], reverse=True)

    return candidates[:limit]


def _split_tex(text: str):
    return [t for t in re.split(r"\s*[,+;/]\s*", text) if t]


def search_medicines_lexical(
    conn: sqlite3.Connection,
    name: str,
    active_ingredient: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Busca lexical na tabela medicines por nome e/ou princípio ativo.

    Estratégia: OR no WHERE (preserva recall) + ranking por match_count
    (medicamentos que matcham mais tokens vêm primeiro). Sem isso, queries
    como "Paracetamol, Cloridrato de Fenilefrina, Maleato de Carbinoxamina"
    são dominadas pelo token mais comum (paracetamol) e o medicamento exato
    fica abaixo do LIMIT.
    """
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    name_norm = normalize_text(name)
    name_tokens = _split_tex(name_norm)

    where_parts: list[str] = []
    where_params: list[Any] = []
    score_parts: list[str] = []
    score_params: list[Any] = []

    for token in name_tokens:
        pattern = f"%{token}%"
        where_parts.append("processed_name LIKE ?")
        where_params.append(pattern)
        score_parts.append("(CASE WHEN processed_name LIKE ? THEN 1 ELSE 0 END)")
        score_params.append(pattern)

    if active_ingredient:
        ai_norm = normalize_text(active_ingredient)
        ai_tokens = _split_tex(ai_norm)

        for token in ai_tokens:
            pattern = f"%{token}%"
            where_parts.append(
                "(processed_active_ingredient LIKE ? OR processed_name LIKE ?)"
            )
            where_params.extend([pattern, pattern])
            score_parts.append(
                "(CASE WHEN processed_active_ingredient LIKE ? "
                "      OR processed_name LIKE ? THEN 1 ELSE 0 END)"
            )
            score_params.extend([pattern, pattern])

    if not where_parts:
        return []

    score_expr = " + ".join(score_parts)
    where_clause = " OR ".join(where_parts)

    query = f"""
        SELECT id, name, processed_name, active_ingredient,
               processed_active_ingredient, source, url, registration_number,
               therapeutic_classes, company_name,
               ({score_expr}) AS match_count
        FROM medicines
        WHERE {where_clause}
        ORDER BY match_count DESC
        LIMIT ?
    """

    params = score_params + where_params + [limit]
    cursor.execute(query, params)

    return [dict(row) for row in cursor.fetchall()]


def _match_tokens(text: str | None) -> set[str]:
    """
    Tokens normalizados para comparação de similaridade: sem pontuação,
    maiúsculos, com no mínimo 2 caracteres.
    """
    cleaned = re.sub(r"[^\w\s]", " ", text or "")
    return {t for t in cleaned.upper().split() if len(t) >= 2}


def _token_f1(query_tokens: set[str], cand_tokens: set[str]) -> float:
    """
    F1 entre dois conjuntos de tokens. A precisão penaliza tokens a mais no
    candidato (ingredientes de um combo ou nome de fabricante) e o recall
    penaliza tokens faltando — então igualdade exata pontua 1.0 e o combo perde
    para o mono-fármaco quando o alvo é um único princípio ativo.
    """
    if not query_tokens or not cand_tokens:
        return 0.0
    intersection = query_tokens & cand_tokens
    if not intersection:
        return 0.0
    precision = len(intersection) / len(cand_tokens)
    recall = len(intersection) / len(query_tokens)
    return 2 * precision * recall / (precision + recall)


def _score_medicine_match(
    row: dict,
    name_norm: str,
    ai_target: str | None,
) -> float:
    """
    Score de similaridade do medicamento em [0, 1].

    O único sinal disponível é o nome processado: a coluna
    ``processed_active_ingredient`` do BulaGratis é vazia, então o princípio
    ativo vive dentro do próprio nome. Pontua por F1 de tokens contra (a) a
    marca buscada e (b) o princípio ativo alvo, ficando com o melhor dos dois —
    o candidato só precisa casar bem com um dos identificadores.
    """
    cand_tokens = _match_tokens(row.get("processed_name"))
    if not cand_tokens:
        return 0.0

    brand_score = _token_f1(_match_tokens(name_norm), cand_tokens)
    ai_score = _token_f1(_match_tokens(ai_target), cand_tokens) if ai_target else 0.0

    return max(brand_score, ai_score)


def normalize_text(text: str | None) -> str:
    return pipe(
        text or "",
        uppercase_text,
        remove_text_accents,
        # remove_text_punctuation,
        normalize_text_whitespace,
    )


def hybrid_chunk_search(
    conn: sqlite3.Connection,
    medicine_id: str,
    keywords: list[str],
    sections: list[str] | None,
    query_embedding: list[float] | None,
    cfg: BulaCheckConfig,
) -> list[RetrievedChunk]:
    chunks = _fetch_chunks_for_medicine(
        conn=conn,
        medicine_id=medicine_id,
        sections=sections,
    )

    if not chunks:
        return []

    chunks_by_id = {chunk["id"]: chunk for chunk in chunks}

    retrieval_query_text = " ".join(keywords)

    lexical_scores = _score_chunks_tfidf(
        chunks=chunks,
        query_text=retrieval_query_text,
    )  # {chunk_id: tfidf_score}

    semantic_scores: dict[str, float] = {}  # {chunk_id: semantic_score}

    if query_embedding:
        for chunk in chunks:
            semantic_scores[chunk["id"]] = _cosine(
                query_embedding,
                chunk["embedding"],
            )

    final_scores: list[tuple[str, float]] = []

    lexical_weight = cfg.get("lexical_weight")
    semantic_weight = cfg.get("semantic_weight")

    for chunk_id in chunks_by_id:
        lexical_score = lexical_scores.get(chunk_id, 0.0)
        semantic_score = semantic_scores.get(chunk_id, 0.0)

        combined_score = 0.0

        if lexical_weight is not None:
            combined_score += lexical_weight * lexical_score

        if semantic_weight is not None:
            combined_score += semantic_weight * semantic_score

        final_scores.append((chunk_id, combined_score))

    final_scores.sort(key=lambda item: item[1], reverse=True)

    top_retrieved = [
        RetrievedChunk(
            chunk=chunks_by_id[chunk_id],
            score=score,
        )
        for chunk_id, score in final_scores[: cfg["top_k_chunks"]]
    ]

    return_mode = cfg.get("return_chunks", "only_desired")
    if return_mode == "with_prev_and_next":
        return _expand_with_neighbours(top_retrieved, chunks)

    return top_retrieved


def _expand_with_neighbours(
    retrieved: list[RetrievedChunk],
    all_chunks: list[ChunksDict],
) -> list[RetrievedChunk]:
    """
    Para cada chunk recuperado, inclui o chunk anterior e o próximo dentro
    da mesma seção (mesmo medicine_id + section).

    A ordenação final respeita a posição original na seção
    (paragraph_idx, chunk_idx) para que o LLM receba o texto em ordem
    natural de leitura.

    Chunks vizinhos são adicionados com score=0.0 para indicar que foram
    incluídos por contexto, não por relevância direta.
    """
    if not retrieved:
        return retrieved

    # Índice posicional por (medicine_id, section, paragraph_idx, chunk_idx)
    # para localizar prev/next rapidamente.
    # Usamos a lista ordenada de todos os chunks agrupada por seção.
    from collections import defaultdict

    # Agrupa por (medicine_id, section)
    section_key = lambda c: (c["medicine_id"], str(c["section"]))  # noqa: E731

    section_lists: dict[tuple, list[ChunksDict]] = defaultdict(list)
    for chunk in all_chunks:
        section_lists[section_key(chunk)].append(chunk)

    # Garante ordenação dentro de cada seção
    for key in section_lists:
        section_lists[key].sort(key=lambda c: (c["paragraph_idx"], c["chunk_idx"]))

    # Mapeia chunk_id → posição dentro da lista da sua seção
    position_in_section: dict[str, tuple[tuple, int]] = {}
    for key, lst in section_lists.items():
        for pos, chunk in enumerate(lst):
            position_in_section[chunk["id"]] = (key, pos)

    # IDs já presentes no resultado (evita duplicatas)
    seen_ids: set[str] = {r["chunk"]["id"] for r in retrieved}

    # Coleta os vizinhos
    neighbour_chunks: list[ChunksDict] = []
    for retrieved_chunk in retrieved:
        cid = retrieved_chunk["chunk"]["id"]
        if cid not in position_in_section:
            continue
        sec_key, pos = position_in_section[cid]
        lst = section_lists[sec_key]

        for neighbour_pos in (pos - 1, pos + 1):
            if 0 <= neighbour_pos < len(lst):
                neighbour = lst[neighbour_pos]
                if neighbour["id"] not in seen_ids:
                    seen_ids.add(neighbour["id"])
                    neighbour_chunks.append(neighbour)

    # Junta retrieved + vizinhos e ordena por (section, paragraph_idx, chunk_idx)
    all_retrieved: list[RetrievedChunk] = list(retrieved) + [
        RetrievedChunk(chunk=nc, score=0.0) for nc in neighbour_chunks
    ]

    all_retrieved.sort(
        key=lambda r: (
            str(r["chunk"]["section"]),
            r["chunk"]["paragraph_idx"],
            r["chunk"]["chunk_idx"],
        )
    )

    return all_retrieved


def _fetch_chunks_for_medicine(
    conn: sqlite3.Connection,
    medicine_id: str,
    sections: list[str] | None = None,
) -> list[ChunksDict]:
    """Fetch chunks from a medicine, optionally filtering by section."""
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    where_parts = ["medicine_id = ?"]
    params: list[Any] = [medicine_id]

    if sections:
        placeholders = ",".join("?" for _ in sections)
        where_parts.append(f"section IN ({placeholders})")
        params.extend(sections)

    query = f"""
        SELECT id, medicine_id, medicine_name, section,
               paragraph_idx, chunk_idx, text, embedding
        FROM chunks
        WHERE {" AND ".join(where_parts)}
    """

    cursor.execute(query, params)
    rows = [dict(row) for row in cursor.fetchall()]

    chunks: list[ChunksDict] = []

    for row in rows:
        row["embedding"] = _parse_embedding(row["embedding"])
        chunks.append(ChunksDict(**row))

    return chunks


def _score_chunks_tfidf(
    chunks: list[ChunksDict],
    query_text: str,
) -> dict[str, float]:
    """
    Compute TF-IDF scores between a query and candidate chunks.

    Parameters
    ----------
    chunks : list[Chunks]
        Candidate chunks from one medicine.
    query_text : str
        Retrieval query text.

    Returns
    -------
    dict[str, float]
        Mapping from chunk id to TF-IDF score.
    """
    if not chunks:
        return {}

    documents = [
        Document(
            document_id=index,
            text=chunk["text"],
        )
        for index, chunk in enumerate(chunks)
    ]

    index_to_chunk_id = {index: chunk["id"] for index, chunk in enumerate(chunks)}

    inverted_index = gen_inverted_index(documents)

    vector_model = gen_vector_space_model(
        inverted_index=inverted_index,
        tf_method=VectorModel.normalized_tf_method,
        idf_method=VectorModel.standard_idf_method,
    )

    query_id = "query"

    search_results = search(
        queries=[
            Query(
                query_id=query_id,
                text=query_text,
            )
        ],
        vector_model=vector_model,
    )

    ranked_documents = search_results.root.get(query_id, [])

    scores: dict[str, float] = {}

    for ranked_document in ranked_documents:
        chunk_id = index_to_chunk_id[ranked_document.document_id]
        scores[chunk_id] = ranked_document.score

    return scores


def _parse_embedding(value: Any) -> list[float]:
    if isinstance(value, str):
        return json.loads(value)

    if isinstance(value, (bytes, bytearray)):
        return json.loads(value.decode())

    return value


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)
