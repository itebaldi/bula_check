import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

from nemo.preprocessing.text import lowercase_text
from nemo.preprocessing.text import normalize_text_whitespace
from nemo.preprocessing.text import remove_text_accents
from nemo.preprocessing.text import remove_text_punctuation
from toolz.functoolz import pipe


def _normalize_text(value: str) -> str:
    return pipe(
        lowercase_text(value),
        remove_text_accents,
        remove_text_punctuation,
        normalize_text_whitespace,
    )


def search_in_db_from_file(
    db_path: str | Path,
    keyword: str,
    table_name: str,
    columns: list[str] | None = None,
    limit: int | None = 20,
) -> list[dict[str, Any]]:

    with closing(sqlite3.connect(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        search = search_in_db(
            db_connection=conn,
            keyword=keyword,
            table_name=table_name,
            columns=columns,
            limit=limit,
        )

    return search


def search_by_filters_from_file(
    db_path: str | Path,
    table_name: str,
    filters: dict[str, Any],
    limit: int | None = 20,
) -> list[dict[str, Any]]:

    with closing(sqlite3.connect(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        search = search_by_filters(
            db_connection=conn,
            table_name=table_name,
            filters=filters,
            limit=limit,
        )

    return search


def search_in_db(
    db_connection: sqlite3.Connection,
    keyword: str,
    table_name: str,
    columns: list[str] | None = None,
    limit: int | None = 20,
) -> list[dict[str, Any]]:
    db_connection.row_factory = sqlite3.Row
    cursor = db_connection.cursor()
    keyword_tokens = _normalize_text(keyword).split()

    if columns is None:
        cursor.execute(f"PRAGMA table_info({table_name})")
        table_info = cursor.fetchall()
        columns = [
            row["name"]
            for row in table_info
            if row["type"].upper() in ("TEXT", "VARCHAR", "CHAR", "JSON")
        ]

    where_parts = []
    params = []

    for token in keyword_tokens:
        token_clause = " OR ".join([f"LOWER({col}) LIKE ?" for col in columns])
        where_parts.append(f"({token_clause})")
        params.extend([f"%{token.lower()}%" for _ in columns])

    query = f"""
        SELECT *
        FROM {table_name}
        WHERE {" AND ".join(where_parts)}
    """

    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    cursor.execute(query, params)
    return [dict(row) for row in cursor.fetchall()]


def search_by_filters(
    db_connection: sqlite3.Connection,
    table_name: str,
    filters: dict[str, Any],
    limit: int | None = 20,
    match_mode: str = "contains",
) -> list[dict[str, Any]]:
    db_connection.row_factory = sqlite3.Row
    cursor = db_connection.cursor()

    where_parts = []
    params = []

    for column, value in filters.items():
        if isinstance(value, str):
            tokens = _normalize_text(value).split()

            if match_mode == "contains":
                for token in tokens:
                    where_parts.append(f"LOWER({column}) LIKE ?")
                    params.append(f"%{token}%")

            elif match_mode == "exact":
                where_parts.append(f"LOWER({column}) = ?")
                params.append(value.lower())

            else:
                raise ValueError(f"match_mode inválido: {match_mode}")

        elif isinstance(value, list):
            placeholders = ",".join("?" for _ in value)
            where_parts.append(f"{column} IN ({placeholders})")
            params.extend(value)

        else:
            where_parts.append(f"{column} = ?")
            params.append(value)

    query = f"""
        SELECT *
        FROM {table_name}
        WHERE {" AND ".join(where_parts)}
    """

    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)

    cursor.execute(query, params)
    return [dict(row) for row in cursor.fetchall()]
