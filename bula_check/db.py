import sqlite3
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


def search_in_db(
    db_path: str | Path,
    keyword: str,
    table_name: str,
    columns: list[str] | None = None,
    limit: int | None = 20,
) -> list[dict[str, Any]]:
    keyword_tokens = _normalize_text(keyword).split()

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        if columns is None:
            cursor.execute(f"PRAGMA table_info({table_name})")
            table_info = cursor.fetchall()
            columns = [
                row["name"]
                for row in table_info
                if row["type"].upper() in ("TEXT", "VARCHAR", "CHAR")
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
    db_path: str | Path,
    table_name: str,
    filters: dict[str, Any],
    limit: int | None = 20,
) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        where_parts = []
        params = []

        for column, value in filters.items():
            if isinstance(value, list):
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
