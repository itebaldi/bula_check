"""
Fill ``bula_doc_index`` rows created during Anvisa crawl with PDF text.

Uses plain HTTP (``requests``) and ``pypdf`` only — no Playwright or browser
automation. If the Anvisa endpoint returns 403 for your network, run hydration
from an environment where direct downloads work, or use a separate tunnel.
"""

from __future__ import annotations

import io
import json
import re
import sqlite3
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from pypdf import PdfReader

from bula_check.bulas_anvisa import Bula
from bula_check.bulas_anvisa import _extract_sections_from_raw_text
from bula_check.bulas_anvisa import _get_reference_brand
from bula_check.preprocessing.text import normalize_text_whitespace

DEFAULT_BULAS_DOC_DB = Path("inputs/bulas/bulas_doc.db")

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _download_pdf_bytes(pdf_url: str, *, timeout: float) -> bytes:
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


def _guess_drug_name_from_text_or_url(raw_text: str, pdf_url: str) -> str:
    patterns = [
        r"\b(?:nome comercial|medicamento|nome do medicamento)\s*:?\s*([A-ZÀ-Ú0-9][^\n]{2,120})",
        r"\b([A-ZÀ-Ú][A-ZÀ-Ú0-9\-\s]{2,80})\s+(?:é um medicamento|contém|apresenta)",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw_text, flags=re.IGNORECASE)
        if match:
            value = normalize_text_whitespace(match.group(1))
            if 2 <= len(value) <= 120:
                return value.title()
    path = (urlparse(pdf_url).path or "").rstrip("/")
    frag = path.split("/")[-1] if path else ""
    if frag and frag not in ("parecer", "bula"):
        slug = normalize_text_whitespace(frag.replace("-", " "))
        if slug:
            return slug
    return "medication"


def _build_bula_from_pdf_http(
    pdf_url: str,
    *,
    source_url: str,
    drug_name: str | None,
    company_name: str | None,
    patient_url: str | None,
    professional_url: str | None,
    metadata: dict[str, Any],
    timeout: float,
) -> Bula:
    pdf_bytes = _download_pdf_bytes(pdf_url, timeout=timeout)
    raw_text = _extract_text_from_pdf_bytes(pdf_bytes)
    sections = _extract_sections_from_raw_text(raw_text)
    reference_brand = _get_reference_brand(raw_text)
    resolved_name = drug_name or _guess_drug_name_from_text_or_url(raw_text, pdf_url)
    return Bula(
        drug_name=resolved_name,
        reference_brand=reference_brand,
        company_name=company_name,
        source_url=source_url or pdf_url,
        patient_url=patient_url or pdf_url,
        professional_url=professional_url,
        raw_text=raw_text,
        sections=sections,
        metadata=metadata,
    )


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
            meta_raw = row["metadata_json"]
            metadata: dict[str, Any] = {}
            if meta_raw:
                try:
                    loaded = json.loads(meta_raw)
                    if isinstance(loaded, dict):
                        raw_row = loaded.get("raw_row")
                        if isinstance(raw_row, dict):
                            metadata = dict(raw_row)
                        if loaded.get("category") is not None:
                            metadata["category"] = loaded["category"]
                except json.JSONDecodeError:
                    metadata = {}

            bula = _build_bula_from_pdf_http(
                pdf_url,
                source_url=row["source_url"],
                drug_name=row["drug_name"],
                company_name=row["company_name"],
                patient_url=row["patient_pdf_url"],
                professional_url=row["professional_pdf_url"],
                metadata=metadata,
                timeout=timeout,
            )
            documented = datetime.now(timezone.utc).isoformat()
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
                    bula.reference_brand,
                    bula.patient_url,
                    bula.professional_url,
                    bula.raw_text,
                    bula.created_at.isoformat(),
                    bula.sections.indications,
                    bula.sections.contraindications,
                    bula.sections.warnings_and_precautions,
                    bula.sections.adverse_reactions,
                    row["id"],
                ),
            )
            conn.commit()
            done += 1
    finally:
        conn.close()
    return done
