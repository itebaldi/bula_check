from __future__ import annotations

import base64
import io
import json
import re
import sqlite3
import time
from dataclasses import asdict
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel
from toolz.functoolz import pipe

from bula_check.bulas import _get_reference_brand
from bula_check.preprocessing.text import lowercase_text
from bula_check.preprocessing.text import normalize_text_whitespace
from bula_check.preprocessing.text import remove_text_accents
from bula_check.preprocessing.text import remove_text_punctuation
from bula_check.preprocessing.text import replace_spaces_with_text_underscores

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover
    PdfReader = None  # type: ignore

try:
    from playwright.sync_api import sync_playwright
except Exception:  # pragma: no cover
    sync_playwright = None  # type: ignore


class Logs(BaseModel):
    processed: int
    saved: int
    failure: int
    failures: list[dict[str, str]]

    def write_to_json(
        self,
        output_path: str | Path,
        indent: int = 4,
    ) -> Path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.model_dump_json(indent=indent), encoding="utf-8")
        return path


@dataclass
class AnvisaRecord:
    """
    Índice mínimo de um medicamento na consulta Anvisa (JSON persistido).

    ``patient_url`` / ``professional_url`` usam JWT de curta duração no path;
    use ``source_url`` e os identificadores de registro para obter links novos.
    """

    drug_name: str
    company_name: str | None
    source_url: str
    patient_url: str | None
    professional_url: str | None
    created_at: str
    registration_number: str | None
    cnpj: str | None
    product_id: int | None
    reference_brand: str | None
    process_number: str | None


def _anvisa_created_at_iso() -> str:
    return datetime.now().isoformat()


def _write_anvisa_record_json(path: Path, record: AnvisaRecord) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(asdict(record), indent=4, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _str_or_none(v: Any) -> str | None:
    if v is None:
        return None
    return str(v).strip() or None


def _int_or_none(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# Colunas do crawl = chaves do JSON (``asdict(AnvisaRecord)``). ``id`` é PK.
ANVISA_RECORD_DB_COLUMNS: tuple[str, ...] = (
    "drug_name",
    "company_name",
    "source_url",
    "patient_url",
    "professional_url",
    "created_at",
    "registration_number",
    "cnpj",
    "product_id",
    "reference_brand",
    "process_number",
)

# Com ``limit`` e ``chunk_by_year=True``, vários anos seguidos sem resultados do
# Bulário implicam uma sessão Playwright por ano (muito lento). Abortar após N
# chunks vazios evita percorrer >100 anos.
_SAVE_ALL_MAX_EMPTY_YEAR_CHUNKS = 10

_CREATE_BULA_DOC_INDEX_SQL = f"""
CREATE TABLE IF NOT EXISTS bula_doc_index (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    {", ".join(f"{c} TEXT" if c != "product_id" else "product_id INTEGER" for c in ANVISA_RECORD_DB_COLUMNS)},
    UNIQUE (source_url)
)
"""

# documented_at TEXT,
# raw_text TEXT,
# indications TEXT,
# contraindications TEXT,
# warnings_and_precautions TEXT,
# adverse_reactions TEXT,


def _bula_doc_table_columns(conn: sqlite3.Connection) -> list[str]:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='bula_doc_index'"
    )
    if cur.fetchone() is None:
        return []
    return [
        r[1] for r in conn.execute("PRAGMA table_info(bula_doc_index)").fetchall()
    ]


def _legacy_bula_doc_schema(cols: list[str]) -> bool:
    return bool(cols) and "pdf_fetch_key" in cols


def _stale_bula_doc_schema(cols: list[str]) -> bool:
    """Tabela existe mas não tem o layout atual (ex.: sem ``registration_number``)."""
    return bool(cols) and "registration_number" not in cols


def _migrate_bula_doc_index_from_legacy(conn: sqlite3.Connection) -> None:
    conn.execute("ALTER TABLE bula_doc_index RENAME TO bula_doc_index__legacy")
    conn.execute(_CREATE_BULA_DOC_INDEX_SQL)
    prev_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        legacy_rows = conn.execute("SELECT * FROM bula_doc_index__legacy").fetchall()
        for row in legacy_rows:
            rd = {k: row[k] for k in row.keys()}
            meta: dict[str, Any] = {}
            raw = rd.get("metadata_json")
            if raw:
                try:
                    loaded = json.loads(raw)
                    if isinstance(loaded, dict):
                        meta = loaded
                except json.JSONDecodeError:
                    pass
            rr = meta.get("raw_row") if isinstance(meta.get("raw_row"), dict) else {}
            patient = rd.get("patient_pdf_url") or rd.get("patient_url")
            prof = rd.get("professional_pdf_url") or rd.get("professional_url")
            created = (
                rd.get("created_at")
                or rd.get("crawled_at")
                or _anvisa_created_at_iso()
            )
            rec = AnvisaRecord(
                drug_name=(rd.get("drug_name") or "").strip() or "medication",
                company_name=rd.get("company_name"),
                source_url=(rd.get("source_url") or "").strip(),
                patient_url=patient,
                professional_url=prof,
                created_at=str(created),
                registration_number=_str_or_none(rr.get("numeroRegistro")),
                cnpj=_str_or_none(rr.get("cnpj")),
                product_id=_int_or_none(rr.get("idProduto")),
                reference_brand=rd.get("reference_brand"),
                process_number=_str_or_none(rr.get("numProcesso")),
            )
            if not rec.source_url:
                continue
            _upsert_bula_doc_record(conn, rec)
            # if rd.get("documented_at") is not None or rd.get("raw_text"):
            #     conn.execute(
            #         """
            #         UPDATE bula_doc_index SET
            #             documented_at = ?,
            #             raw_text = ?,
            #             indications = ?,
            #             contraindications = ?,
            #             warnings_and_precautions = ?,
            #             adverse_reactions = ?
            #         WHERE source_url = ?
            #         """,
            #         (
            #             rd.get("documented_at"),
            #             rd.get("raw_text"),
            #             rd.get("indications"),
            #             rd.get("contraindications"),
            #             rd.get("warnings_and_precautions"),
            #             rd.get("adverse_reactions"),
            #             rec.source_url,
            #         ),
            #     )
        conn.execute("DROP TABLE bula_doc_index__legacy")
    finally:
        conn.row_factory = prev_factory


def _upsert_bula_doc_record(conn: sqlite3.Connection, record: AnvisaRecord) -> bool:
    """
    Atualiza por ``source_url``; se não existir linha, insere. Não usa
    ``ON CONFLICT`` para não depender de haver ``UNIQUE`` na BD antiga
    (sem isso o SQLite falha e nada é gravado).
    """
    row = asdict(record)
    # assignments = ", ".join(
    #     f"{c} = ?" for c in ANVISA_RECORD_DB_COLUMNS if c != "source_url"
    # )
    # upd_vals = tuple(row[c] for c in ANVISA_RECORD_DB_COLUMNS if c != "source_url")
    # cur = conn.execute(
    #     f"""
    #     UPDATE bula_doc_index SET
    #         {assignments},
    #         documented_at = NULL
    #     WHERE source_url = ?
    #     """,
    #     upd_vals + (row["source_url"],),
    # )
    # if cur.rowcount > 0:
    #     return True
    cols_sql = ", ".join(ANVISA_RECORD_DB_COLUMNS)
    placeholders = ", ".join("?" for _ in ANVISA_RECORD_DB_COLUMNS)
    values = tuple(row[c] for c in ANVISA_RECORD_DB_COLUMNS)
    conn.execute(
        f"INSERT INTO bula_doc_index ({cols_sql}) VALUES ({placeholders})",
        values,
    )
    return True


class AnvisaBularioClient:
    """
    Bulk-capable client for the official Anvisa Bulário Eletrônico.

    Parecer PDF URLs (``.../bula/parecer/{jwt}/``) expire within minutes. There is
    no documented public API only to mint a new JWT: the SPA reads
    ``idBulaPacienteProtegido`` / ``idBulaProfissionalProtegido`` from Bulário
    JSON (e.g. ``/api/consulta/bulario``). Calling those endpoints with plain
    ``requests`` sometimes works but often fails (403 / Cloudflare / cookies). The
    supported flow here is **Playwright** (``save_all`` with ``save_sqlite=True``)
    to store fresh URLs in ``bulas_doc.db``, then ``bulas_doc_hydrate`` over HTTP.

    Important
    ---------
    This client is built to run locally and relies on browser automation
    to interact with the public Anvisa consultation portal. The portal UI is
    the official access path documented by Anvisa for searching by medication,
    company, publication period, and downloading patient/professional PDFs.

    Because the search interface is a dynamic web application and the internal
    network calls are not documented as a stable public API, this client uses
    Playwright to drive the UI and then downloads the official PDF URLs exposed
    by the results.

    Before first use, install browser automation dependencies locally:

        poetry add playwright pypdf
        playwright install chromium

    Official references:
    - https://www.gov.br/anvisa/pt-br/sistemas/bulario-eletronico
    - https://consultas.anvisa.gov.br/
    """

    BASE_URL = "https://consultas.anvisa.gov.br"
    # Trailing slash matters: `/#/bulario` is rewritten to `/#/` and the view
    # never loads; the portal link uses `/#/bulario/` (AngularJS ui-router).
    BULARIO_URL = f"{BASE_URL}/#/bulario/"
    BULAS_DOC_DB_DEFAULT = Path("inputs/bulas/bulas_doc.db")

    _BROWSER_USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    def __init__(
        self,
        timeout: int = 30,
        sleep_between_requests: float = 0.0,
        headless: bool = True,
    ) -> None:
        self.timeout = timeout
        self.sleep_between_requests = sleep_between_requests
        self.headless = headless

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (compatible; AnvisaBularioClient/1.0; "
                    "+https://consultas.anvisa.gov.br/)"
                )
            }
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def search(
        self,
        medication: str,
        limit: int = 10,
        save_json: bool = False,
    ) -> list[AnvisaRecord]:
        records = self.search_records(medication=medication, limit=limit)
        results: list[AnvisaRecord] = []

        for record in records:
            filled = self.get_by_record(record, save_json=save_json)
            if filled is not None:
                results.append(filled)

            if self.sleep_between_requests > 0:
                time.sleep(self.sleep_between_requests)

        return results

    def search_records(
        self,
        medication: str,
        limit: int = 10,
    ) -> list[AnvisaRecord]:
        return self._collect_records_via_browser(
            medication=medication,
            publication_start=None,
            publication_end=None,
            limit=limit,
        )

    def get_by_record(
        self,
        record: AnvisaRecord,
        save_json: bool = False,
        prefer_patient_bula: bool = True,
    ) -> AnvisaRecord | None:
        pdf_url = (
            record.patient_url
            if prefer_patient_bula and record.patient_url
            else record.professional_url or record.patient_url
        )
        if not pdf_url:
            return None

        return self.get_by_pdf_url(
            pdf_url,
            source_url=record.source_url,
            drug_name=record.drug_name,
            company_name=record.company_name,
            patient_url=record.patient_url,
            professional_url=record.professional_url,
            registration_number=record.registration_number,
            cnpj=record.cnpj,
            product_id=record.product_id,
            process_number=record.process_number,
            save_json=save_json,
        )

    def get_by_url(
        self,
        med_url: str,
        save_json: bool = False,
    ) -> AnvisaRecord | None:
        """
        Build a record from an Anvisa medicamento detail URL.

        Expected examples
        -----------------
        https://consultas.anvisa.gov.br/#/medicamentos/25351023179202157/
        https://consultas.anvisa.gov.br/api/consulta/medicamentos/arquivo/bula/parecer/.../?Authorization=Guest
        """
        if "/arquivo/bula/" in med_url:
            return self.get_by_pdf_url(
                pdf_url=med_url,
                source_url=med_url,
                save_json=save_json,
            )

        record = self._collect_record_from_detail_page(med_url)
        if record is None:
            return None
        return self.get_by_record(record, save_json=save_json)

    def get_by_pdf_url(
        self,
        pdf_url: str,
        *,
        source_url: str | None = None,
        drug_name: str | None = None,
        company_name: str | None = None,
        patient_url: str | None = None,
        professional_url: str | None = None,
        registration_number: str | None = None,
        cnpj: str | None = None,
        product_id: int | None = None,
        process_number: str | None = None,
        save_json: bool = False,
    ) -> AnvisaRecord:
        pdf_bytes = self._download_pdf(pdf_url)
        raw_text = self._extract_text_from_pdf_bytes(pdf_bytes)
        reference_brand = _get_reference_brand(raw_text)
        resolved_name = drug_name or self._guess_drug_name_from_text_or_url(
            raw_text, pdf_url
        )
        out = AnvisaRecord(
            drug_name=resolved_name,
            company_name=company_name,
            source_url=source_url or pdf_url,
            patient_url=patient_url or pdf_url,
            professional_url=professional_url,
            created_at=_anvisa_created_at_iso(),
            registration_number=registration_number,
            cnpj=cnpj,
            product_id=product_id,
            reference_brand=reference_brand,
            process_number=process_number,
        )

        if save_json:
            output_dir = Path("inputs/bulas/json")
            safe_name = self._gen_safe_filename(out.drug_name or "medication")
            safe_company = self._gen_safe_filename(
                out.company_name or "unknown_company"
            )
            filename = f"{safe_name}__{safe_company}.json"
            _write_anvisa_record_json(output_dir / filename, out)

        return out

    def save_all(
        self,
        limit: int | None = None,
        continue_on_error: bool = True,
        save_logs: bool = True,
        save_sqlite: bool = False,
        publication_start: str = "1900-01-01",
        publication_end: str | None = None,
        chunk_by_year: bool = True,
        save_json: bool = False,
    ) -> Logs:
        """
        Download many bulas from the official Anvisa portal.

        Strategy
        --------
        The public UI allows filtering by publication period. To make bulk
        extraction more reliable, this method can sweep the catalog year by year,
        paginating through the results and downloading each patient PDF.

        When ``limit`` is set, each chunk only collects up to that many remaining
        rows from the API (not the entire year), and years are processed from
        newest to oldest so small limits finish quickly instead of walking from
        1900 with a full browser session per year. If many consecutive year chunks
        return no rows, the loop stops after
        10 consecutive empty year chunks
        (each chunk still pays the full Playwright startup cost).

        The SQLite file is only written when ``save_sqlite=True``; otherwise
        ``bulas_doc.db`` is left unchanged.

        When ``save_sqlite`` is true, rows are stored in ``bulas_doc.db`` with PDF
        URLs and crawl time only (no PDF download). Use
        :func:`bula_check.bulas_doc_hydrate.hydrate_pending_bulas_doc_rows` later
        to fetch PDFs over HTTP and fill text and sections. Re-run this crawl
        when parecer JWTs expire, so ``patient_url`` / ``professional_url``
        are refreshed in place (matched by ``source_url``).

        When neither ``save_json`` nor ``save_sqlite`` is true, ``limit`` still
        applies: each distinct record processed counts toward ``limit`` so the
        year sweep stops (otherwise ``saved`` would never increase and every
        year chunk would run).
        """
        dir = Path("inputs/bulas")
        output_dir = dir / "json"
        output_dir.mkdir(parents=True, exist_ok=True)

        conn: sqlite3.Connection | None = None
        if save_sqlite:
            conn = self._init_bulas_doc_db(self.BULAS_DOC_DB_DEFAULT)
        failures: list[dict[str, str]] = []
        saved_count = 0
        processed_count = 0

        start = _coerce_date(publication_start)
        end = _coerce_date(publication_end) if publication_end else date.today()

        chunks = _year_chunks(start, end) if chunk_by_year else [(start, end)]
        if limit is not None and chunk_by_year:
            chunks = list(reversed(chunks))

        seen_keys: set[str] = set()
        empty_year_chunks = 0

        for chunk_start, chunk_end in chunks:
            if limit is not None and saved_count >= limit:
                break
            remaining: int | None
            if limit is None:
                remaining = None
            else:
                remaining = limit - saved_count
                if remaining <= 0:
                    break

            try:
                records = self._collect_records_via_browser(
                    medication=None,
                    publication_start=chunk_start.isoformat(),
                    publication_end=chunk_end.isoformat(),
                    limit=remaining,
                )
            except Exception as exc:
                failures.append(
                    {
                        "url": self.BULARIO_URL,
                        "error": f"Failed to collect records for chunk {chunk_start}..{chunk_end}: {exc}",
                    }
                )
                if not continue_on_error:
                    break
                continue

            if not records:
                if limit is not None and chunk_by_year:
                    empty_year_chunks += 1
                    if empty_year_chunks >= _SAVE_ALL_MAX_EMPTY_YEAR_CHUNKS:
                        break
                continue

            empty_year_chunks = 0

            for record in records:
                dedupe_key = (
                    record.patient_url
                    or record.professional_url
                    or record.source_url
                )
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)
                processed_count += 1

                try:
                    persisted = False
                    if save_sqlite and conn is not None:
                        persisted = self._save_bula_doc_crawl_row(conn, record)
                        conn.commit()

                    if save_json:
                        if not (record.patient_url or record.professional_url):
                            raise ValueError(
                                "Record does not expose patient/professional PDF URL."
                            )
                        safe_name = self._gen_safe_filename(
                            record.drug_name or "medication"
                        )
                        safe_company = self._gen_safe_filename(
                            record.company_name or "unknown_company"
                        )
                        filename = f"{safe_name}__{safe_company}.json"
                        _write_anvisa_record_json(output_dir / filename, record)
                        persisted = True

                    if persisted or (not save_sqlite and not save_json):
                        saved_count += 1

                except Exception as exc:
                    failures.append(
                        {
                            "url": dedupe_key,
                            "error": str(exc),
                        }
                    )
                    if not continue_on_error:
                        break

                if limit is not None and saved_count >= limit:
                    break

                if self.sleep_between_requests > 0:
                    time.sleep(self.sleep_between_requests)

            if limit is not None and saved_count >= limit:
                break

        logs = Logs(
            processed=processed_count,
            saved=saved_count,
            failure=len(failures),
            failures=failures,
        )

        if save_logs:
            logs.write_to_json(dir / "logs.json")

        if conn is not None:
            conn.close()

        return logs

    # ------------------------------------------------------------------
    # Browser automation
    # ------------------------------------------------------------------
    def _collect_records_via_browser(
        self,
        medication: str | None,
        publication_start: str | None,
        publication_end: str | None,
        limit: int | None,
    ) -> list[AnvisaRecord]:
        self._ensure_playwright_available()

        with sync_playwright() as pw:  # type: ignore
            browser = pw.chromium.launch(headless=self.headless)
            try:
                page = browser.new_page(user_agent=self._BROWSER_USER_AGENT)
                api_rows: list[dict[str, Any]] = []

                def _on_bulario_list_response(response: Any) -> None:
                    if "/api/consulta/bulario" not in response.url:
                        return
                    if response.status != 200:
                        return
                    try:
                        payload = response.json()
                    except Exception:
                        return
                    content = payload.get("content")
                    if isinstance(content, list):
                        api_rows.extend(content)

                page.on("response", _on_bulario_list_response)
                page.goto(
                    self.BULARIO_URL,
                    wait_until="domcontentloaded",
                    timeout=self.timeout * 1000,
                )
                try:
                    page.wait_for_load_state(
                        "networkidle", timeout=min(20_000, self.timeout * 1000)
                    )
                except Exception:
                    pass
                page.wait_for_timeout(2500)
                self._wait_for_bulario_form(page)

                self._fill_search_form(
                    page=page,
                    medication=medication,
                    publication_start=publication_start,
                    publication_end=publication_end,
                )
                self._trigger_search(page)
                self._wait_for_results(page)

                records = self._records_from_bulario_api_items(api_rows)
                if not records:
                    records = self._scrape_result_pages(page, limit=limit)
                else:
                    records = self._paginate_bulario_api_results(
                        page, api_rows, records, limit
                    )
                return records
            finally:
                browser.close()

    def _collect_record_from_detail_page(self, med_url: str) -> AnvisaRecord | None:
        self._ensure_playwright_available()

        with sync_playwright() as pw:  # type: ignore
            browser = pw.chromium.launch(headless=self.headless)
            try:
                page = browser.new_page(user_agent=self._BROWSER_USER_AGENT)
                page.goto(
                    med_url,
                    wait_until="domcontentloaded",
                    timeout=self.timeout * 1000,
                )
                page.wait_for_timeout(2500)

                pdf_links = self._extract_pdf_links_from_page(page)
                html = page.content()
            finally:
                browser.close()

        text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
        text = normalize_text_whitespace(text)
        drug_name = (
            self._guess_drug_name_from_page_text(text)
            or self._get_last_path_segment(med_url)
            or "medication"
        )

        if not pdf_links["patient"] and not pdf_links["professional"]:
            return None

        return AnvisaRecord(
            drug_name=drug_name,
            company_name=None,
            source_url=med_url,
            patient_url=pdf_links["patient"],
            professional_url=pdf_links["professional"],
            created_at=_anvisa_created_at_iso(),
            registration_number=None,
            cnpj=None,
            product_id=None,
            reference_brand=None,
            process_number=None,
        )

    def _fill_search_form(
        self,
        page: Any,
        medication: str | None,
        publication_start: str | None,
        publication_end: str | None,
    ) -> None:
        has_publication_filter = bool(publication_start or publication_end)

        if has_publication_filter:
            self._fill_publication_period(page, publication_start, publication_end)

        if medication:
            input_candidates = [
                'input[ng-model="filter"]',
                'input[placeholder*="Medicamento"]',
                'input[aria-label*="Medicamento"]',
                'input[name*="medic"]',
                'input[id*="medic"]',
                'input[formcontrolname*="medic"]',
                'input[formcontrolname*="nome"]',
            ]
            if not self._fill_first_visible(page, input_candidates, medication):
                raise ValueError(
                    f"Could not find medication input for selectors: {input_candidates}"
                )
            # Do not use the uib-typeahead ArrowDown+Enter path: it advances to another
            # wizard step and replaces the DOM, so "Consultar" is detached and clicks
            # time out. Submitting with the typed name works (nomeProduto=...).
            page.wait_for_timeout(500)

    def _wait_for_bulario_form(self, page: Any) -> None:
        """
        Bulário is an AngularJS app: the route must be ``#/bulario/`` and the
        portal may not render inputs under Playwright's default user agent.
        """
        page.locator(
            'input[placeholder="Data inicial"], input[placeholder*="Medicamento"]'
        ).first.wait_for(
            state="visible",
            timeout=self.timeout * 1000,
        )

    def _fill_publication_period(
        self,
        page: Any,
        publication_start: str | None,
        publication_end: str | None,
    ) -> None:
        start_fmt = (
            _format_date_for_ui(publication_start) if publication_start else ""
        )
        end_fmt = _format_date_for_ui(publication_end) if publication_end else ""

        if publication_start and publication_end:
            if self._try_fill_date_range_pair(page, start_fmt, end_fmt):
                return

        missing: list[str] = []

        start_candidates = [
            'input[placeholder="Data inicial"]',
            'input[placeholder*="Data inicial"]',
            'input[placeholder*="Início"]',
            'input[placeholder*="Inicio"]',
            'input[aria-label*="Início"]',
            'input[aria-label*="Inicio"]',
            'input[name*="inicio"]',
            'input[name*="início"]',
            'input[id*="inicio"]',
            'input[id*="início"]',
            'input[name*="publicacao"]',
            'input[name*="publicação"]',
            'input[formcontrolname*="inicio"]',
            'input[formcontrolname*="Inicio"]',
            'input[formcontrolname*="dataInicio"]',
            'input[formcontrolname*="data_inicio"]',
            'input[formcontrolname*="dtInicio"]',
            'input[formcontrolname*="periodoInicio"]',
        ]
        end_candidates = [
            'input[placeholder="Data final"]',
            'input[placeholder*="Data final"]',
            'input[placeholder*="Fim"]',
            'input[aria-label*="Fim"]',
            'input[name*="fim"]',
            'input[id*="fim"]',
            'input[formcontrolname*="fim"]',
            'input[formcontrolname*="Fim"]',
            'input[formcontrolname*="dataFim"]',
            'input[formcontrolname*="data_fim"]',
            'input[formcontrolname*="dtFim"]',
            'input[formcontrolname*="periodoFim"]',
        ]

        if publication_start:
            if not self._fill_first_visible(page, start_candidates, start_fmt):
                if not self._fill_by_accessible_hints(
                    page,
                    start_fmt,
                    (
                        re.compile(r"in[ií]cio", re.I),
                        re.compile(r"data\s+inicial", re.I),
                    ),
                ):
                    missing.append("publication start (início / período)")
        if publication_end:
            if not self._fill_first_visible(page, end_candidates, end_fmt):
                if not self._fill_by_accessible_hints(
                    page,
                    end_fmt,
                    (
                        re.compile(r"\bfim\b", re.I),
                        re.compile(r"até", re.I),
                        re.compile(r"data\s+final", re.I),
                    ),
                ):
                    missing.append("publication end (fim / até)")

        if missing:
            raise ValueError(
                "Could not fill Anvisa publication date field(s): "
                + ", ".join(missing)
            )

    def _try_fill_date_range_pair(
        self, page: Any, start_fmt: str, end_fmt: str
    ) -> bool:
        di = page.locator('input[placeholder="Data inicial"]')
        df = page.locator('input[placeholder="Data final"]')
        try:
            if di.count() >= 1 and df.count() >= 1:
                a, b = di.first, df.first
                if a.is_visible() and b.is_visible():
                    a.click(timeout=2000)
                    a.fill(start_fmt, timeout=5000)
                    b.click(timeout=2000)
                    b.fill(end_fmt, timeout=5000)
                    return True
        except Exception:
            pass

        pair_queries = [
            "input.mat-datepicker-input",
            "input[matDatepicker]",
            "input[matdatepicker]",
            "input.mat-mdc-input-element.mat-datepicker-input",
        ]
        for q in pair_queries:
            loc = page.locator(q)
            try:
                n = loc.count()
            except Exception:
                continue
            if n < 2:
                continue
            first, second = loc.nth(0), loc.nth(1)
            try:
                if first.is_visible() and second.is_visible():
                    first.click(timeout=2000)
                    first.fill(start_fmt, timeout=5000)
                    second.click(timeout=2000)
                    second.fill(end_fmt, timeout=5000)
                    return True
            except Exception:
                continue

        for frag in ("DD/MM", "dd/mm", "AAAA", "aaaa"):
            loc = page.locator(f'input[placeholder*="{frag}"]')
            try:
                n = loc.count()
            except Exception:
                continue
            if n < 2:
                continue
            first, second = loc.nth(0), loc.nth(1)
            try:
                if first.is_visible() and second.is_visible():
                    first.click(timeout=2000)
                    first.fill(start_fmt, timeout=5000)
                    second.click(timeout=2000)
                    second.fill(end_fmt, timeout=5000)
                    return True
            except Exception:
                continue

        return False

    def _fill_by_accessible_hints(
        self,
        page: Any,
        value: str,
        patterns: tuple[re.Pattern[str], ...],
    ) -> bool:
        for pat in patterns:
            try:
                loc = page.get_by_label(pat)
                if loc.count() == 0:
                    continue
                target = loc.first
                if target.is_visible():
                    target.click(timeout=2000)
                    target.fill(value, timeout=5000)
                    return True
            except Exception:
                pass
            try:
                loc = page.get_by_placeholder(pat)
                if loc.count() == 0:
                    continue
                target = loc.first
                if target.is_visible():
                    target.click(timeout=2000)
                    target.fill(value, timeout=5000)
                    return True
            except Exception:
                pass
        return False

    def _trigger_search(self, page: Any) -> None:
        button_selectors = [
            'input[type="submit"][value="Consultar"]',
            'input.btn-primary[type="submit"]',
            'button:has-text("Consultar")',
            'button:has-text("Pesquisar")',
            'button:has-text("Buscar")',
            'button[type="submit"]',
            'input[type="submit"]',
        ]
        click_timeout = self.timeout * 1000
        for selector in button_selectors:
            locator = page.locator(selector)
            if locator.count() == 0:
                continue
            target = locator.first
            try:
                target.click(timeout=click_timeout)
            except Exception:
                target.evaluate(
                    """el => {
                        el.dispatchEvent(new MouseEvent('click', { bubbles: true }));
                    }"""
                )
            page.wait_for_timeout(2000)
            return
        page.keyboard.press("Enter")
        page.wait_for_timeout(2000)

    def _wait_for_results(self, page: Any) -> None:
        candidates = [
            "table",
            '[role="table"]',
            'text="Bula do Paciente"',
            'text="Bula do Profissional"',
            'text="PDF"',
        ]
        for selector in candidates:
            try:
                page.locator(selector).first.wait_for(timeout=self.timeout * 1000)
                return
            except Exception:
                pass
        raise TimeoutError("Could not detect results table in Anvisa Bulário UI.")

    def _bula_parecer_pdf_url(self, protected_token: str) -> str:
        return (
            f"{self.BASE_URL}/api/consulta/medicamentos/arquivo/bula/parecer/"
            f"{protected_token.strip()}/?Authorization=Guest"
        )

    def _records_from_bulario_api_items(
        self,
        raw_rows: list[dict[str, Any]],
    ) -> list[AnvisaRecord]:
        """
        Build records from the paginated JSON returned by ``/api/consulta/bulario``.

        The results table uses ``ng-click`` for PDFs, not ``href``; tokens in the
        API map to ``.../arquivo/bula/parecer/{token}/?Authorization=Guest``.
        """
        seen: set[tuple[Any, ...]] = set()
        out: list[AnvisaRecord] = []

        for item in raw_rows:
            key = (
                item.get("numeroRegistro"),
                item.get("expediente"),
                item.get("numProcesso"),
            )
            if key in seen:
                continue
            seen.add(key)

            pat = item.get("idBulaPacienteProtegido")
            prof = item.get("idBulaProfissionalProtegido")
            patient_url = (
                self._bula_parecer_pdf_url(pat)
                if isinstance(pat, str) and pat
                else None
            )
            professional_url = (
                self._bula_parecer_pdf_url(prof)
                if isinstance(prof, str) and prof
                else None
            )
            if patient_url is None and professional_url is None:
                continue

            id_prod = item.get("idProduto")
            nproc = item.get("numProcesso")
            nome = item.get("nomeProduto") or "medication"
            razao = item.get("razaoSocial")
            cnpj = item.get("cnpj")
            company: str | None
            if razao and cnpj:
                company = f"{razao} - {cnpj}"
            elif razao:
                company = str(razao)
            else:
                company = None

            med_href = f"#/medicamentos/{id_prod}?numeroProcesso={nproc}"
            source_url = urljoin(f"{self.BASE_URL}/", med_href)

            out.append(
                AnvisaRecord(
                    drug_name=str(nome),
                    company_name=company,
                    source_url=source_url,
                    patient_url=patient_url,
                    professional_url=professional_url,
                    created_at=_anvisa_created_at_iso(),
                    registration_number=_str_or_none(item.get("numeroRegistro")),
                    cnpj=_str_or_none(item.get("cnpj")),
                    product_id=_int_or_none(item.get("idProduto")),
                    reference_brand=None,
                    process_number=_str_or_none(item.get("numProcesso")),
                )
            )
        return out

    def _paginate_bulario_api_results(
        self,
        page: Any,
        api_rows: list[dict[str, Any]],
        current: list[AnvisaRecord],
        limit: int | None,
    ) -> list[AnvisaRecord]:
        records = list(current)
        if limit is not None and len(records) >= limit:
            return records[:limit]
        while limit is None or len(records) < limit:
            prev_len = len(api_rows)
            next_loc = self._find_next_page_locator(page)
            if next_loc is None:
                break
            try:
                if not next_loc.is_enabled():
                    break
            except Exception:
                pass
            next_loc.click()
            page.wait_for_timeout(2500)
            records = self._records_from_bulario_api_items(api_rows)
            if len(api_rows) == prev_len:
                break
        if limit is not None:
            return records[:limit]
        return records

    def _scrape_result_pages(
        self, page: Any, limit: int | None
    ) -> list[AnvisaRecord]:
        records: list[AnvisaRecord] = []
        seen_keys: set[str] = set()

        while True:
            html = page.content()
            partial = self._extract_records_from_html(html)

            for record in partial:
                key = (
                    record.patient_url
                    or record.professional_url
                    or record.source_url
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                records.append(record)
                if limit is not None and len(records) >= limit:
                    return records

            next_page_locator = self._find_next_page_locator(page)
            if next_page_locator is None:
                break

            next_page_locator.click()
            page.wait_for_timeout(2000)

        return records

    def _find_next_page_locator(self, page: Any) -> Any | None:
        candidates = [
            'button[aria-label*="Próxima"]',
            'button[aria-label*="Next"]',
            'a[aria-label*="Próxima"]',
            'a[aria-label*="Next"]',
            'button:has-text("Próxima")',
            'button:has-text("Next")',
            'a:has-text("Próxima")',
            'a:has-text("Next")',
            'li[title*="Next"] a',
        ]
        for selector in candidates:
            locator = page.locator(selector)
            if locator.count() > 0:
                try:
                    first = locator.first
                    if first.is_enabled():
                        return first
                except Exception:
                    return locator.first
        return None

    # ------------------------------------------------------------------
    # HTML / PDF parsing
    # ------------------------------------------------------------------
    def _extract_records_from_html(self, html: str) -> list[AnvisaRecord]:
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.find_all("tr")
        records: list[AnvisaRecord] = []

        for row in rows:
            text = normalize_text_whitespace(row.get_text(" ", strip=True))
            if not text:
                continue
            links = row.find_all("a", href=True)
            has_med_link = any(
                "/medicamentos/" in (ln.get("href") or "") for ln in links
            )
            if not has_med_link and not any(
                keyword in text.lower()
                for keyword in [
                    "bula",
                    "registro",
                    "apresentação",
                    "paciente",
                    "profissional",
                ]
            ):
                continue

            patient_url: str | None = None
            professional_url: str | None = None
            source_url: str | None = None

            for link in links:
                href = urljoin(self.BASE_URL, link["href"])  # type: ignore
                label = normalize_text_whitespace(
                    link.get_text(" ", strip=True)
                ).lower()
                href_lower = href.lower()

                if "/#/medicamentos/" in href or "/medicamentos/" in href:
                    source_url = href

                if "/arquivo/bula/" in href_lower or href_lower.endswith(".pdf"):
                    nearby = normalize_text_whitespace(
                        link.parent.get_text(" ", strip=True)  # type: ignore
                    ).lower()
                    context = f"{label} {nearby}"
                    if "paciente" in context and patient_url is None:
                        patient_url = href
                    elif "profissional" in context and professional_url is None:
                        professional_url = href
                    elif patient_url is None:
                        patient_url = href
                    else:
                        professional_url = professional_url or href

            if not patient_url and not professional_url:
                continue

            columns = [
                normalize_text_whitespace(td.get_text(" ", strip=True))
                for td in row.find_all(["td", "th"])
            ]
            drug_name = (
                self._pick_drug_name_from_columns(columns)
                or self._guess_drug_name_from_page_text(text)
                or "medication"
            )
            company_name = self._pick_company_name_from_columns(columns)

            records.append(
                AnvisaRecord(
                    drug_name=drug_name,
                    company_name=company_name,
                    source_url=source_url
                    or (patient_url or professional_url or self.BULARIO_URL),
                    patient_url=patient_url,
                    professional_url=professional_url,
                    created_at=_anvisa_created_at_iso(),
                    registration_number=None,
                    cnpj=None,
                    product_id=None,
                    reference_brand=None,
                    process_number=None,
                )
            )

        return records

    def _extract_pdf_links_from_page(self, page: Any) -> dict[str, str | None]:
        html = page.content()
        soup = BeautifulSoup(html, "html.parser")

        result: dict[str, str | None] = {
            "patient": None,
            "professional": None,
        }

        for link in soup.find_all("a", href=True):
            href = urljoin(self.BASE_URL, link["href"])  # type: ignore
            context = normalize_text_whitespace(
                link.get_text(" ", strip=True)
            ).lower()
            parent_text = (
                normalize_text_whitespace(
                    link.parent.get_text(" ", strip=True)
                ).lower()
                if link.parent
                else ""
            )
            combined = f"{context} {parent_text}"
            if "/arquivo/bula/" not in href and not href.lower().endswith(".pdf"):
                continue
            if "paciente" in combined and result["patient"] is None:
                result["patient"] = href
            elif "profissional" in combined and result["professional"] is None:
                result["professional"] = href
            elif result["patient"] is None:
                result["patient"] = href
            elif result["professional"] is None:
                result["professional"] = href

        return result

    def _download_pdf(self, pdf_url: str) -> bytes:
        if pdf_url.endswith("?Authorization="):
            pdf_url = f"{pdf_url}Guest"
        headers = {"User-Agent": self._BROWSER_USER_AGENT}
        try:
            response = self.session.get(
                pdf_url, timeout=self.timeout, headers=headers
            )
            response.raise_for_status()
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 403:
                return self._download_pdf_via_playwright(pdf_url)
            raise

        data = response.content
        content_type = response.headers.get("content-type", "").lower()
        if "pdf" not in content_type and not data.startswith(b"%PDF"):
            raise ValueError(f"URL did not return a PDF: {pdf_url}")
        return data

    def _download_pdf_via_playwright(self, pdf_url: str) -> bytes:
        """
        Cloudflare often blocks bare ``requests`` to ``/api/consulta/.../bula/...``.
        Open the Bulário shell first, then capture the PDF response from an in-page
        ``fetch`` (same cookies as a normal visit).
        """
        self._ensure_playwright_available()
        with sync_playwright() as p:  # type: ignore
            browser = p.chromium.launch(headless=self.headless)
            try:
                context = browser.new_context(user_agent=self._BROWSER_USER_AGENT)
                page = context.new_page()
                page.goto(
                    self.BULARIO_URL,
                    wait_until="domcontentloaded",
                    timeout=self.timeout * 1000,
                )
                page.wait_for_timeout(2000)
                b64 = page.evaluate(
                    """async (url) => {
                        const r = await fetch(url, { credentials: 'include' });
                        if (!r.ok) {
                            return { ok: false, status: r.status };
                        }
                        const buf = await r.arrayBuffer();
                        const bytes = new Uint8Array(buf);
                        let binary = '';
                        const chunk = 0x8000;
                        for (let i = 0; i < bytes.length; i += chunk) {
                            binary += String.fromCharCode.apply(
                                null, bytes.subarray(i, i + chunk)
                            );
                        }
                        return { ok: true, b64: btoa(binary) };
                    }""",
                    pdf_url,
                )
            finally:
                browser.close()
        if not isinstance(b64, dict):
            raise RuntimeError(f"Unexpected PDF fetch result for {pdf_url}")
        if not b64.get("ok"):
            raise RuntimeError(
                f"PDF fetch failed HTTP {b64.get('status')}: {pdf_url}"
            )
        data = base64.b64decode(b64["b64"])
        if not data.startswith(b"%PDF"):
            raise ValueError(f"URL did not return a PDF: {pdf_url}")
        return data

    def _extract_text_from_pdf_bytes(self, pdf_bytes: bytes) -> str:
        if PdfReader is None:
            raise ImportError(
                "pypdf is required to extract text from Anvisa PDFs. Install with: poetry add pypdf"
            )

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
            raise ValueError("Could not extract text from Anvisa PDF.")
        return raw_text

    # ------------------------------------------------------------------
    # SQLite (bulas_doc.db — crawl index + optional hydrate)
    # ------------------------------------------------------------------
    def _init_bulas_doc_db(self, sqlite_path: str | Path) -> sqlite3.Connection:
        db_path = Path(sqlite_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(db_path)
        cols = _bula_doc_table_columns(conn)
        if not cols:
            conn.execute(_CREATE_BULA_DOC_INDEX_SQL)
        elif _legacy_bula_doc_schema(cols):
            _migrate_bula_doc_index_from_legacy(conn)
        elif _stale_bula_doc_schema(cols):
            conn.execute(
                "ALTER TABLE bula_doc_index RENAME TO bula_doc_index__replaced"
            )
            conn.execute(_CREATE_BULA_DOC_INDEX_SQL)
        conn.commit()
        return conn

    def _save_bula_doc_crawl_row(
        self,
        conn: sqlite3.Connection,
        record: AnvisaRecord,
    ) -> bool:
        """
        Grava no SQLite as mesmas chaves que o JSON (``AnvisaRecord``) + ``id``
        autogerado. ``ON CONFLICT(source_url)`` atualiza o crawl e repõe
        ``documented_at`` a NULL para voltar a hidratar.
        """
        if not (record.patient_url or record.professional_url):
            raise ValueError(
                "Record does not expose patient or professional PDF URL."
            )
        return _upsert_bula_doc_record(conn, record)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _ensure_playwright_available(self) -> None:
        if sync_playwright is None:
            raise ImportError(
                "Playwright is required for Anvisa portal automation. "
                "Install with: poetry add playwright && playwright install chromium"
            )

    def _fill_first_visible(
        self, page: Any, selectors: list[str], value: str
    ) -> bool:
        for selector in selectors:
            locator = page.locator(selector)
            try:
                n = locator.count()
            except Exception:
                continue
            for i in range(n):
                target = locator.nth(i)
                try:
                    if not target.is_visible():
                        continue
                    target.click(timeout=2000)
                    target.fill(value, timeout=5000)
                    return True
                except Exception:
                    continue
        return False

    def _guess_drug_name_from_text_or_url(self, raw_text: str, url: str) -> str:
        name = self._guess_drug_name_from_page_text(raw_text)
        if name:
            return name
        slug = self._get_last_path_segment(url)
        if slug:
            return normalize_text_whitespace(slug.replace("-", " "))
        return "medication"

    def _guess_drug_name_from_page_text(self, text: str) -> str | None:
        patterns = [
            r"\b(?:nome comercial|medicamento|nome do medicamento)\s*:?\s*([A-ZÀ-Ú0-9][^\n]{2,120})",
            r"\b([A-ZÀ-Ú][A-ZÀ-Ú0-9\-\s]{2,80})\s+(?:é um medicamento|contém|apresenta)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = normalize_text_whitespace(match.group(1))
                if 2 <= len(value) <= 120:
                    return value.title()
        return None

    def _pick_drug_name_from_columns(self, columns: list[str]) -> str | None:
        blacklist = {
            "bula do paciente",
            "bula do profissional",
            "pdf",
            "registro",
            "apresentação",
        }
        for col in columns:
            if not col:
                continue
            if col.strip().lower() in blacklist:
                continue
            if any(
                token in col.lower() for token in ["pdf", "paciente", "profissional"]
            ):
                continue
            if re.search(r"\d{5,}", col):
                continue
            return col
        return None

    def _pick_company_name_from_columns(self, columns: list[str]) -> str | None:
        for col in columns:
            lower = col.lower()
            if any(
                token in lower
                for token in [
                    "ltda",
                    "s/a",
                    "sa",
                    "eireli",
                    "farmac",
                    "laborat",
                    "indústria",
                    "industria",
                ]
            ):
                return col
        return None

    def _get_last_path_segment(self, url: str) -> str:
        parsed = urlparse(url)
        parts = [part for part in parsed.path.split("/") if part]
        if parts:
            return parts[-1]
        frag = parsed.fragment or ""
        frag_parts = [part for part in frag.split("/") if part]
        return frag_parts[-1] if frag_parts else ""

    def _gen_safe_filename(self, name: str) -> str:
        return pipe(
            lowercase_text(name),
            remove_text_accents,
            replace_spaces_with_text_underscores,
            remove_text_punctuation,
            normalize_text_whitespace,
        )


def _format_date_for_ui(value: str) -> str:
    dt = _coerce_date(value)
    return dt.strftime("%d/%m/%Y")


def _coerce_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return datetime.strptime(value, "%Y-%m-%d").date()


def _year_chunks(start: date, end: date) -> list[tuple[date, date]]:
    chunks: list[tuple[date, date]] = []
    current_year = start.year
    while current_year <= end.year:
        chunk_start = date(current_year, 1, 1)
        chunk_end = date(current_year, 12, 31)
        if current_year == start.year:
            chunk_start = start
        if current_year == end.year:
            chunk_end = end
        chunks.append((chunk_start, chunk_end))
        current_year += 1
    return chunks
