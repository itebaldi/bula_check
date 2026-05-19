import json
import math
import sqlite3
from difflib import SequenceMatcher
from typing import Any

from nemo.preprocessing.text import normalize_text_whitespace
from nemo.preprocessing.text import remove_text_accents
from nemo.preprocessing.text import remove_text_punctuation
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
    3. Rankeamento por score lexical
    """
    name_norm = normalize_text(name)
    ai_norm = normalize_text(active_ingredient) if active_ingredient else None

    # BulaGratis
    rows = search_medicines_lexical(
        bulagratis_conn, name, active_ingredient, limit=cfg["top_k_medicines"] * 3
    )

    # fallback ANVISA, re-busca com princípio ativo
    if not rows and anvisa_conn is not None:
        anvisa_rows = search_medicines_lexical(anvisa_conn, name, limit=5)
        for anvisa_row in anvisa_rows:
            ai_from_anvisa = anvisa_row.get("active_ingredient")
            if ai_from_anvisa:
                rows = search_medicines_lexical(
                    bulagratis_conn,
                    ai_from_anvisa,
                    limit=cfg["top_k_medicines"] * 3,
                )
                if rows:
                    ai_norm = normalize_text(ai_from_anvisa)
                    break

    if not rows:
        return []

    candidates: list[MedicineCandidate] = []
    for row in rows:
        score = _score_medicine_match(row, name_norm, ai_norm)
        medicine = MedicinesDict(**row)
        candidates.append(
            MedicineCandidate(
                medicine=medicine,
                score=score,
            )
        )

    candidates.sort(key=lambda candidate: candidate["score"], reverse=True)
    return candidates[: cfg["top_k_medicines"]]


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


def search_medicines_lexical(
    conn: sqlite3.Connection,
    name: str,
    active_ingredient: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Busca lexical na tabela medicines por nome e/ou princípio ativo."""
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    name_norm = normalize_text(name)
    name_tokens = name_norm.split()

    where_parts: list[str] = []
    params: list[Any] = []

    for token in name_tokens:
        where_parts.append("processed_name LIKE ?")
        params.append(f"%{token}%")

    if active_ingredient:
        ai_norm = normalize_text(active_ingredient)
        ai_tokens = ai_norm.split()

        for token in ai_tokens:
            where_parts.append(
                "(processed_active_ingredient LIKE ? OR processed_name LIKE ?)"
            )
            params.extend([f"%{token}%", f"%{token}%"])

    if not where_parts:
        return []

    query = f"""
        SELECT id, name, processed_name, active_ingredient,
               processed_active_ingredient, source, url, registration_number,
               therapeutic_classes, company_name
        FROM medicines
        WHERE {" OR ".join(where_parts)}
        LIMIT ?
    """

    params.append(limit)
    cursor.execute(query, params)

    return [dict(row) for row in cursor.fetchall()]


def _score_medicine_match(
    row: dict,
    name_norm: str,
    ai_norm: str | None,
) -> float:
    """
    Heurística de score para ranqueamento lexical de medicamentos.
    Retorna valor em [0, 1].
    """
    name_tokens = set(name_norm.split())
    row_name = row.get("processed_name") or ""
    row_tokens = set(row_name.split())

    score = 0.0

    if name_tokens and row_tokens:
        intersection = name_tokens & row_tokens
        union = name_tokens | row_tokens

        jaccard = len(intersection) / len(union) if union else 0.0

        exact_name_bonus = 0.3 if row_name == name_norm else 0.0
        contains_name_bonus = 0.2 if name_norm in row_name else 0.0

        name_parts = name_norm.split()
        first_token = name_parts[0] if name_parts else ""
        prefix_bonus = (
            0.1 if first_token and row_name.startswith(first_token) else 0.0
        )

        score += jaccard + exact_name_bonus + contains_name_bonus + prefix_bonus

    if ai_norm:
        row_ai = row.get("processed_active_ingredient") or ""
        row_ai_search_text = f"{row_ai} {row_name}".strip()

        ai_tokens = set(ai_norm.split())
        row_ai_tokens = set(row_ai_search_text.split())

        if ai_tokens and row_ai_tokens:
            ai_intersection = ai_tokens & row_ai_tokens
            ai_union = ai_tokens | row_ai_tokens
            ai_jaccard = len(ai_intersection) / len(ai_union) if ai_union else 0.0

            score += 0.35 * ai_jaccard

        if ai_norm in row_ai_search_text:
            score += 0.25

    return min(1.0, score)


def normalize_text(text: str) -> str:
    return pipe(
        text,
        uppercase_text,
        remove_text_accents,
        remove_text_punctuation,
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
