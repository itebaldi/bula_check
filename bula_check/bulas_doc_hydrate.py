"""
Fill ``bula_doc_index`` rows created during Anvisa crawl with PDF text.

Uses plain HTTP (``requests``) and ``pypdf`` only — no Playwright or browser
automation. If the Anvisa endpoint returns 403 for your network, run hydration
from an environment where direct downloads work, or use a separate tunnel.
"""

from __future__ import annotations

import io
import re
import sqlite3
from datetime import datetime
from datetime import timezone
from pathlib import Path

import requests
from pypdf import PdfReader

from bula_check.bulas import _get_reference_brand
from bula_check.bulas import _normalize_text
from bula_check.constants import SECTION_PATTERNS
from bula_check.preprocessing.text import normalize_text_whitespace

DEFAULT_BULAS_DOC_DB = Path("inputs/bulas/bulas_doc.db")

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _normalize_anvisa_parecer_url(pdf_url: str) -> str:
    """Older crawls stored ``...?Authorization=``; the portal expects ``Guest``."""
    if pdf_url.endswith("?Authorization="):
        return f"{pdf_url}Guest"
    return pdf_url


def _download_pdf_bytes(pdf_url: str, *, timeout: float) -> bytes:
    pdf_url = _normalize_anvisa_parecer_url(pdf_url)
    response = requests.get(
        pdf_url,
        headers={"User-Agent": _BROWSER_UA},
        timeout=timeout,
    )
    response.raise_for_status()
    data = response.content
    content_type = response.headers.get("content-type", "").lower()
    if "pdf" not in content_type and not data.startswith(b"%PDF"):
        raise ValueError(f"URL did not return a PDF: {pdf_url}")
    return data


def _extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    texts: list[str] = []
    for page in reader.pages:
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        if text:
            texts.append(text)

    raw_text = normalize_text_whitespace("\n\n".join(texts))
    if not raw_text:
        raise ValueError("Could not extract text from PDF.")
    return raw_text


def _split_text_into_heading_blocks(raw_text: str) -> list[dict[str, str]]:
    blocks: list[dict[str, str]] = []
    lines = [normalize_text_whitespace(line) for line in raw_text.splitlines()]
    lines = [line for line in lines if line]

    heading_pattern = re.compile(
        r"^(?:\d+\.\s*)?(?:para que este medicamento e indicado|quando nao devo usar este medicamento|o que devo saber antes de usar este medicamento|quais os males que este medicamento pode me causar|indica(?:c|ç)(?:o|õ)es|contraindica(?:c|ç)(?:a|ã)o|advert(?:e|ê)ncias(?: e precau(?:c|ç)(?:o|õ)es)?|rea(?:c|ç)(?:o|õ)es adversas)\b",
        flags=re.IGNORECASE,
    )

    current_heading: str | None = None
    current_parts: list[str] = []

    for line in lines:
        if heading_pattern.search(line):
            if current_heading is not None:
                blocks.append(
                    {
                        "heading": current_heading,
                        "content": normalize_text_whitespace(
                            " ".join(current_parts)
                        ),
                    }
                )
            current_heading = line
            current_parts = []
        elif current_heading is not None:
            current_parts.append(line)

    if current_heading is not None:
        blocks.append(
            {
                "heading": current_heading,
                "content": normalize_text_whitespace(" ".join(current_parts)),
            }
        )

    if not blocks:
        blocks.append({"heading": "document", "content": raw_text})

    return blocks


def _section_dict_from_raw_text(raw_text: str) -> dict[str, str | None]:
    blocks = _split_text_into_heading_blocks(raw_text)
    sections: dict[str, str | None] = {
        "indications": None,
        "contraindications": None,
        "warnings_and_precautions": None,
        "adverse_reactions": None,
    }

    for block in blocks:
        heading_norm = _normalize_text(block["heading"])
        for section_name, patterns in SECTION_PATTERNS.items():
            if sections[section_name] is not None:
                continue
            if any(pattern in heading_norm for pattern in patterns):
                sections[section_name] = block["content"] or None

    return sections


def hydrate_pending_bulas_doc_rows(
    db_path: str | Path | None = None,
    *,
    timeout: float = 60.0,
    limit: int | None = None,
) -> int:
    """
    For each pending row in ``bula_doc_index``, GET the PDF over HTTP, extract
    text, and UPDATE the row (``documented_at``, sections, etc.).

    Parameters
    ----------
    db_path
        Path to ``bulas_doc.db`` (default: ``inputs/bulas/bulas_doc.db``).
    timeout
        Per-request timeout in seconds.
    limit
        Maximum number of rows to hydrate; ``None`` means all pending rows.

    Returns
    -------
    int
        Number of rows successfully updated.
    """
    path = Path(db_path) if db_path is not None else DEFAULT_BULAS_DOC_DB
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    done = 0
    try:
        rows = conn.execute(
            """
            SELECT id, patient_pdf_url, professional_pdf_url, source_url,
                   drug_name, company_name, metadata_json
            FROM bula_doc_index
            WHERE documented_at IS NULL
              AND (patient_pdf_url IS NOT NULL OR professional_pdf_url IS NOT NULL)
            ORDER BY id
            """
        ).fetchall()

        for row in rows:
            if limit is not None and done >= limit:
                break
            pdf_url = row["patient_pdf_url"] or row["professional_pdf_url"]
            if not pdf_url:
                continue

            pdf_bytes = _download_pdf_bytes(pdf_url, timeout=timeout)
            raw_text = _extract_text_from_pdf_bytes(pdf_bytes)
            sections = _section_dict_from_raw_text(raw_text)
            reference_brand = _get_reference_brand(raw_text)

            documented = datetime.now(timezone.utc).isoformat()
            created_at = documented
            conn.execute(
                """
                UPDATE bula_doc_index SET
                    documented_at = ?,
                    reference_brand = ?,
                    patient_url = ?,
                    professional_url = ?,
                    raw_text = ?,
                    created_at = ?,
                    indications = ?,
                    contraindications = ?,
                    warnings_and_precautions = ?,
                    adverse_reactions = ?
                WHERE id = ?
                """,
                (
                    documented,
                    reference_brand,
                    row["patient_pdf_url"],
                    row["professional_pdf_url"],
                    raw_text,
                    created_at,
                    sections["indications"],
                    sections["contraindications"],
                    sections["warnings_and_precautions"],
                    sections["adverse_reactions"],
                    row["id"],
                ),
            )
            conn.commit()
            done += 1
    finally:
        conn.close()
    return done
