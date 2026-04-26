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


def _looks_like_json_cell(value: str) -> bool:
    stripped = str(value).lstrip()
    return stripped.startswith("{") or stripped.startswith("[")


def _cell_matches_token(
    value: Any,
    token: str,
    column_sql_type: str,
) -> bool:
    """Match one token in one cell.

    - Plain text: same idea as ``LOWER(column) LIKE '%' || token || '%'`` in
      SQLite (token comes from :func:`_normalize_text` on the keyword).
    - JSON or JSON-shaped TEXT: compare against :func:`_normalize_text` of
      the cell so accents, ``+``, etc. line up with the keyword tokens.
    """
    text = str(value or "")
    col_type = column_sql_type.upper()
    if col_type == "JSON" or _looks_like_json_cell(text):
        return token in _normalize_text(text)
    return token.lower() in text.lower()


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

    cursor.execute(f"PRAGMA table_info({table_name})")
    table_info = cursor.fetchall()
    column_types = {row["name"]: row["type"] for row in table_info}

    if columns is None:
        columns = [
            row["name"]
            for row in table_info
            if row["type"].upper() in ("TEXT", "VARCHAR", "CHAR", "JSON")
        ]

    if not columns:
        raise ValueError(f"No searchable columns found for table '{table_name}'.")

    keyword_tokens = _normalize_text(keyword).split()

    if not keyword_tokens:
        raise ValueError("Keyword must contain at least one searchable token.")

    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()

    results: list[dict[str, Any]] = []

    for row in rows:
        row_dict = dict(row)
        if all(
            any(
                _cell_matches_token(
                    row_dict.get(column),
                    token,
                    column_types.get(column, ""),
                )
                for column in columns
            )
            for token in keyword_tokens
        ):
            results.append(row_dict)
            if limit is not None and len(results) >= limit:
                break

    return results


_TEXT_FILTER_TYPES = frozenset({"TEXT", "VARCHAR", "CHAR", "JSON"})


def search_by_filters(
    db_connection: sqlite3.Connection,
    table_name: str,
    filters: dict[str, Any],
    limit: int | None = 20,
) -> list[dict[str, Any]]:

    db_connection.row_factory = sqlite3.Row
    cursor = db_connection.cursor()

    cursor.execute(f"PRAGMA table_info({table_name})")
    table_info = cursor.fetchall()
    column_types = {row["name"]: row["type"] for row in table_info}

    sql_where_parts: list[str] = []
    sql_params: list[Any] = []
    text_filters: dict[str, str] = {}

    for column, value in filters.items():
        col_type = column_types.get(column, "").upper()
        if isinstance(value, str) and col_type in _TEXT_FILTER_TYPES:
            text_filters[column] = value
            continue
        if isinstance(value, list):
            placeholders = ",".join("?" for _ in value)
            sql_where_parts.append(f"{column} IN ({placeholders})")
            sql_params.extend(value)
        else:
            sql_where_parts.append(f"{column} = ?")
            sql_params.append(value)

    query = f"SELECT * FROM {table_name}"
    if sql_where_parts:
        query += f" WHERE {' AND '.join(sql_where_parts)}"
    apply_limit_in_sql = limit is not None and not text_filters
    if apply_limit_in_sql:
        query += " LIMIT ?"
        sql_params.append(limit)

    cursor.execute(query, sql_params)
    rows = [dict(row) for row in cursor.fetchall()]

    if not text_filters:
        return rows

    results: list[dict[str, Any]] = []
    for row in rows:
        skip_row = False
        for column, raw in text_filters.items():
            tokens = _normalize_text(raw).split()
            if not tokens:
                raise ValueError(
                    f"String filter on {column!r} must contain at least one token.",
                )
            ctype = column_types.get(column, "")
            if not all(
                _cell_matches_token(row.get(column), token, ctype)
                for token in tokens
            ):
                skip_row = True
                break
        if skip_row:
            continue
        results.append(row)
        if limit is not None and len(results) >= limit:
            break

    return results
