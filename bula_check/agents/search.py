import json
import math
import sqlite3
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
    name_norm = _normalize(name)
    ai_norm = _normalize(active_ingredient) if active_ingredient else None

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
                    ai_norm = _normalize(ai_from_anvisa)
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
    Busca medicamentos com nome parecido usando tokens parciais.
    Usado como sugestão quando o medicamento não é encontrado.
    """
    name_norm = _normalize(name)
    tokens = sorted(name_norm.split(), key=len, reverse=True)  # pega maior token

    if not tokens:
        return []

    anchor = tokens[0] if tokens else name_norm

    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, name, processed_name, active_ingredient,
               processed_active_ingredient, source, url, registration_number,
               therapeutic_classes, company_name, processed_company_name, cnpj
        FROM medicines
        WHERE processed_name LIKE ?
        LIMIT ?
        """,
        [f"%{anchor}%", limit * 3],
    )
    rows = [dict(row) for row in cursor.fetchall()]

    candidates: list[MedicineCandidate] = []

    for row in rows:
        score = _score_medicine_match(row, name_norm, None)

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

    name_norm = _normalize(name)
    tokens = name_norm.split()

    where_parts: list[str] = []
    params: list[Any] = []

    for token in tokens:
        where_parts.append("processed_name LIKE ?")
        params.append(f"%{token}%")

    if active_ingredient:
        ai_norm = _normalize(active_ingredient)
        ai_tokens = ai_norm.split()
        for token in ai_tokens:
            where_parts.append("processed_active_ingredient LIKE ?")
            params.append(f"%{token}%")

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


def _score_medicine_match(row: dict, name_norm: str, ai_norm: str | None) -> float:
    """
    Heurística de score para rankeamento lexical de medicamentos.
    Retorna valor em [0, 1].
    """
    name_tokens = set(name_norm.split())
    row_name = row.get("processed_name", "")
    row_tokens = set(row_name.split())

    if not name_tokens or not row_tokens:
        return 0.0

    intersection = name_tokens & row_tokens
    union = name_tokens | row_tokens

    # TODO explicar isso
    jaccard = len(intersection) / len(union) if union else 0.0

    exact_name_bonus = 0.3 if row_name == name_norm else 0.0

    contains_name_bonus = 0.2 if name_norm in row_name else 0.0

    prefix_bonus = 0.1 if row_name.startswith(name_norm.split()[0]) else 0.0

    score = jaccard + exact_name_bonus + contains_name_bonus + prefix_bonus

    if ai_norm:
        row_ai = row.get("processed_active_ingredient", "")
        ai_tokens = set(ai_norm.split())
        row_ai_tokens = set(row_ai.split())

        if ai_tokens and row_ai_tokens:
            ai_intersection = ai_tokens & row_ai_tokens
            ai_union = ai_tokens | row_ai_tokens
            ai_jaccard = len(ai_intersection) / len(ai_union) if ai_union else 0.0

            score += 0.25 * ai_jaccard

            if ai_norm in row_ai:
                score += 0.15

    return score


def _normalize(text: str) -> str:
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

    for chunk_id in chunks_by_id:
        lexical_score = lexical_scores.get(chunk_id, 0.0)
        semantic_score = semantic_scores.get(chunk_id, 0.0)

        combined_score = (
            cfg["lexical_weight"] * lexical_score
            + cfg["semantic_weight"] * semantic_score
        )

        final_scores.append((chunk_id, combined_score))

    final_scores.sort(key=lambda item: item[1], reverse=True)

    return [
        RetrievedChunk(
            chunk=chunks_by_id[chunk_id],
            score=score,
        )
        for chunk_id, score in final_scores[: cfg["top_k_chunks"]]
    ]


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
