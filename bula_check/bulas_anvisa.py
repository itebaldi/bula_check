from __future__ import annotations

import base64
import io
import re
import sqlite3
import string
import time
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import Optional
from urllib.parse import parse_qs
from urllib.parse import urljoin
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel
from toolz.functoolz import pipe

from bula_check.constants import SECTION_PATTERNS
from bula_check.preprocessing.text import lowercase_text
from bula_check.preprocessing.text import normalize_text_whitespace
from bula_check.preprocessing.text import remove_text_accents
from bula_check.preprocessing.text import remove_text_punctuation
from bula_check.preprocessing.text import remove_text_stopwords
from bula_check.preprocessing.text import replace_spaces_with_text_underscores
from inputs.stopwords import get_portuguese_stopwords

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover
    PdfReader = None  # type: ignore

try:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
except Exception:  # pragma: no cover
    sync_playwright = None  # type: ignore
    PlaywrightTimeoutError = Exception  # type: ignore


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


class Sections(BaseModel):
    indications: Optional[str] = None
    contraindications: Optional[str] = None
    warnings_and_precautions: Optional[str] = None
    adverse_reactions: Optional[str] = None


class Bula(BaseModel):
    drug_name: str
    reference_brand: Optional[str] = None
    company_name: Optional[str] = None
    source_url: str
    patient_url: str
    professional_url: Optional[str] = None
    raw_text: str
    created_at: datetime = datetime.now()
    sections: Sections
    metadata: dict[str, Any] = {}

    def write_to_json(
        self,
        output_path: str | Path,
        indent: int = 4,
    ) -> Path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.model_dump_json(indent=indent), encoding="utf-8")
        return path

    @classmethod
    def read_from_json(cls, input_path: str | Path) -> "Bula":
        path = Path(input_path)
        json_data = path.read_text(encoding="utf-8")
        return cls.model_validate_json(json_data)


@dataclass
class AnvisaSearchRecord:
    drug_name: str
    company_name: str | None
    category: str | None
    source_url: str
    patient_pdf_url: str | None
    professional_pdf_url: str | None
    raw_row: dict[str, Any]


class AnvisaBularioClient:
    """
    Bulk-capable client for the official Anvisa Bulário Eletrônico.

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
    MEDICAMENTOS_URL = f"{BASE_URL}/#/medicamentos/"

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
    ) -> list[Bula]:
        records = self.search_records(medication=medication, limit=limit)
        results: list[Bula] = []

        for record in records:
            bula = self.get_by_record(record, save_json=save_json)
            if bula is not None:
                results.append(bula)

            if self.sleep_between_requests > 0:
                time.sleep(self.sleep_between_requests)

        return results

    def search_records(
        self,
        medication: str,
        limit: int = 10,
    ) -> list[AnvisaSearchRecord]:
        return self._collect_records_via_browser(
            medication=medication,
            publication_start=None,
            publication_end=None,
            limit=limit,
        )

    def get_by_record(
        self,
        record: AnvisaSearchRecord,
        save_json: bool = False,
        prefer_patient_bula: bool = True,
    ) -> Optional[Bula]:
        pdf_url = (
            record.patient_pdf_url
            if prefer_patient_bula and record.patient_pdf_url
            else record.professional_pdf_url or record.patient_pdf_url
        )
        if not pdf_url:
            return None

        bula = self.get_by_pdf_url(
            pdf_url=pdf_url,
            source_url=record.source_url,
            drug_name=record.drug_name,
            company_name=record.company_name,
            patient_url=record.patient_pdf_url,
            professional_url=record.professional_pdf_url,
            metadata={
                "category": record.category,
                **record.raw_row,
            },
            save_json=save_json,
        )
        return bula

    def get_by_url(
        self,
        med_url: str,
        save_json: bool = False,
    ) -> Optional[Bula]:
        """
        Build a Bula from an Anvisa medicamento detail URL.

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
        source_url: str | None = None,
        drug_name: str | None = None,
        company_name: str | None = None,
        patient_url: str | None = None,
        professional_url: str | None = None,
        metadata: dict[str, Any] | None = None,
        save_json: bool = False,
    ) -> Bula:
        pdf_bytes = self._download_pdf(pdf_url)
        raw_text = self._extract_text_from_pdf_bytes(pdf_bytes)
        sections = _extract_sections_from_raw_text(raw_text)
        reference_brand = _get_reference_brand(raw_text)

        bula = Bula(
            drug_name=drug_name
            or self._guess_drug_name_from_text_or_url(raw_text, pdf_url),
            reference_brand=reference_brand,
            company_name=company_name,
            source_url=source_url or pdf_url,
            patient_url=patient_url or pdf_url,
            professional_url=professional_url,
            raw_text=raw_text,
            sections=sections,
            metadata=metadata or {},
        )

        if save_json:
            output_dir = Path("inputs/bulas/json")
            safe_name = self._gen_safe_filename(bula.drug_name or "medication")
            safe_company = self._gen_safe_filename(
                bula.company_name or "unknown_company"
            )
            filename = f"{safe_name}__{safe_company}.json"
            bula.write_to_json(output_dir / filename)

        return bula

    def save_all(
        self,
        limit: int | None = None,
        continue_on_error: bool = True,
        save_logs: bool = True,
        save_sqlite: bool = False,
        publication_start: str = "1900-01-01",
        publication_end: str | None = None,
        chunk_by_year: bool = True,
        save_json: bool = True,
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
        1900 with a full browser session per year.
        """
        dir = Path("inputs/bulas")
        output_dir = dir / "json"
        output_dir.mkdir(parents=True, exist_ok=True)

        conn: sqlite3.Connection | None = None
        if save_sqlite:
            conn = self._init_sqlite_db(dir / "bulas.db")

        failures: list[dict[str, str]] = []
        saved_count = 0
        processed_count = 0

        start = _coerce_date(publication_start)
        end = _coerce_date(publication_end) if publication_end else date.today()

        chunks = _year_chunks(start, end) if chunk_by_year else [(start, end)]
        if limit is not None and chunk_by_year:
            chunks = list(reversed(chunks))

        seen_keys: set[str] = set()

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

            for record in records:
                dedupe_key = (
                    record.patient_pdf_url
                    or record.professional_pdf_url
                    or record.source_url
                )
                if dedupe_key in seen_keys:
                    continue
                seen_keys.add(dedupe_key)
                processed_count += 1

                try:
                    bula = self.get_by_record(record, save_json=False)
                    if bula is None:
                        raise ValueError(
                            "Record does not expose patient/professional PDF URL."
                        )

                    if save_json:
                        safe_name = self._gen_safe_filename(
                            bula.drug_name or "medication"
                        )
                        safe_company = self._gen_safe_filename(
                            bula.company_name or "unknown_company"
                        )
                        filename = f"{safe_name}__{safe_company}.json"
                        bula.write_to_json(output_dir / filename)

                    if save_sqlite and conn is not None:
                        self._save_bula_to_sqlite(bula, conn)
                        conn.commit()

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
    ) -> list[AnvisaSearchRecord]:
        self._ensure_playwright_available()

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=self.headless)
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
                page.wait_for_load_state("networkidle", timeout=min(20_000, self.timeout * 1000))
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
            browser.close()
            return records

    def _collect_record_from_detail_page(
        self, med_url: str
    ) -> AnvisaSearchRecord | None:
        self._ensure_playwright_available()

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=self.headless)
            page = browser.new_page(user_agent=self._BROWSER_USER_AGENT)
            page.goto(
                med_url, wait_until="domcontentloaded", timeout=self.timeout * 1000
            )
            page.wait_for_timeout(2500)

            pdf_links = self._extract_pdf_links_from_page(page)
            html = page.content()
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

        return AnvisaSearchRecord(
            drug_name=drug_name,
            company_name=None,
            category=None,
            source_url=med_url,
            patient_pdf_url=pdf_links["patient"],
            professional_pdf_url=pdf_links["professional"],
            raw_row={},
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

    def _try_fill_date_range_pair(self, page: Any, start_fmt: str, end_fmt: str) -> bool:
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
            f"{protected_token.strip()}/?Authorization="
        )

    def _records_from_bulario_api_items(
        self,
        raw_rows: list[dict[str, Any]],
    ) -> list[AnvisaSearchRecord]:
        """
        Build records from the paginated JSON returned by ``/api/consulta/bulario``.

        The results table uses ``ng-click`` for PDFs, not ``href``; tokens in the
        API map to ``.../arquivo/bula/parecer/{token}/?Authorization=``.
        """
        seen: set[tuple[Any, ...]] = set()
        out: list[AnvisaSearchRecord] = []

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
                AnvisaSearchRecord(
                    drug_name=str(nome),
                    company_name=company,
                    category=None,
                    source_url=source_url,
                    patient_pdf_url=patient_url,
                    professional_pdf_url=professional_url,
                    raw_row=dict(item),
                )
            )
        return out

    def _paginate_bulario_api_results(
        self,
        page: Any,
        api_rows: list[dict[str, Any]],
        current: list[AnvisaSearchRecord],
        limit: int | None,
    ) -> list[AnvisaSearchRecord]:
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
    ) -> list[AnvisaSearchRecord]:
        records: list[AnvisaSearchRecord] = []
        seen_keys: set[str] = set()

        while True:
            html = page.content()
            partial = self._extract_records_from_html(html)

            for record in partial:
                key = (
                    record.patient_pdf_url
                    or record.professional_pdf_url
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
    def _extract_records_from_html(self, html: str) -> list[AnvisaSearchRecord]:
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.find_all("tr")
        records: list[AnvisaSearchRecord] = []

        for row in rows:
            text = normalize_text_whitespace(row.get_text(" ", strip=True))
            if not text:
                continue
            links = row.find_all("a", href=True)
            has_med_link = any(
                "/medicamentos/" in (ln.get("href") or "")
                for ln in links
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

            patient_pdf_url: str | None = None
            professional_pdf_url: str | None = None
            source_url: str | None = None

            for link in links:
                href = urljoin(self.BASE_URL, link["href"])
                label = normalize_text_whitespace(
                    link.get_text(" ", strip=True)
                ).lower()
                href_lower = href.lower()

                if "/#/medicamentos/" in href or "/medicamentos/" in href:
                    source_url = href

                if "/arquivo/bula/" in href_lower or href_lower.endswith(".pdf"):
                    nearby = normalize_text_whitespace(
                        link.parent.get_text(" ", strip=True)
                    ).lower()
                    context = f"{label} {nearby}"
                    if "paciente" in context and patient_pdf_url is None:
                        patient_pdf_url = href
                    elif "profissional" in context and professional_pdf_url is None:
                        professional_pdf_url = href
                    elif patient_pdf_url is None:
                        patient_pdf_url = href
                    else:
                        professional_pdf_url = professional_pdf_url or href

            if not patient_pdf_url and not professional_pdf_url:
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
            category = self._pick_category_from_columns(columns)

            records.append(
                AnvisaSearchRecord(
                    drug_name=drug_name,
                    company_name=company_name,
                    category=category,
                    source_url=source_url
                    or (patient_pdf_url or professional_pdf_url or self.BULARIO_URL),
                    patient_pdf_url=patient_pdf_url,
                    professional_pdf_url=professional_pdf_url,
                    raw_row={
                        "columns": columns,
                        "row_text": text,
                    },
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
            href = urljoin(self.BASE_URL, link["href"])
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
        with sync_playwright() as p:
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
    # SQLite
    # ------------------------------------------------------------------
    def _init_sqlite_db(self, sqlite_path: str | Path) -> sqlite3.Connection:
        db_path = Path(sqlite_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(db_path)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bulas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                drug_name TEXT NOT NULL,
                reference_brand TEXT,
                company_name TEXT,
                source_url TEXT NOT NULL UNIQUE,
                patient_url TEXT NOT NULL,
                professional_url TEXT,
                raw_text TEXT NOT NULL,
                created_at TEXT NOT NULL,
                indications TEXT,
                contraindications TEXT,
                warnings_and_precautions TEXT,
                adverse_reactions TEXT,
                metadata_json TEXT
            )
            """
        )
        conn.commit()
        return conn

    def _save_bula_to_sqlite(
        self,
        bula: Bula,
        conn: sqlite3.Connection,
    ) -> None:
        conn.execute(
            """
            INSERT INTO bulas (
                drug_name,
                reference_brand,
                company_name,
                source_url,
                patient_url,
                professional_url,
                raw_text,
                created_at,
                indications,
                contraindications,
                warnings_and_precautions,
                adverse_reactions,
                metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, json(?))
            ON CONFLICT(source_url) DO UPDATE SET
                drug_name = excluded.drug_name,
                reference_brand = excluded.reference_brand,
                company_name = excluded.company_name,
                patient_url = excluded.patient_url,
                professional_url = excluded.professional_url,
                raw_text = excluded.raw_text,
                created_at = excluded.created_at,
                indications = excluded.indications,
                contraindications = excluded.contraindications,
                warnings_and_precautions = excluded.warnings_and_precautions,
                adverse_reactions = excluded.adverse_reactions,
                metadata_json = excluded.metadata_json
            """,
            (
                bula.drug_name,
                bula.reference_brand,
                bula.company_name,
                bula.source_url,
                bula.patient_url,
                bula.professional_url,
                bula.raw_text,
                bula.created_at.isoformat(),
                bula.sections.indications,
                bula.sections.contraindications,
                bula.sections.warnings_and_precautions,
                bula.sections.adverse_reactions,
                __import__("json").dumps(bula.metadata, ensure_ascii=False),
            ),
        )

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

    def _pick_category_from_columns(self, columns: list[str]) -> str | None:
        for col in columns:
            lower = col.lower()
            if any(
                token in lower
                for token in [
                    "genérico",
                    "generico",
                    "similar",
                    "novo",
                    "biológico",
                    "específico",
                    "fitoterápico",
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


# Backward-compatible alias to reduce breakage in existing imports.
BulaGratisClient = AnvisaBularioClient


def _normalize_text(text: str) -> str:
    return pipe(
        lowercase_text(text),
        remove_text_accents,
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


def _extract_sections_from_raw_text(raw_text: str) -> Sections:
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

    return Sections(**sections)


def _get_reference_brand(raw_text: str) -> str | None:
    processed_text = pipe(
        lowercase_text(raw_text),
        remove_text_accents,
        remove_text_punctuation,
        remove_text_stopwords(stop_words=get_portuguese_stopwords()),
    )

    tail_words = r"(?:publicada|registrada|fabricada|bulario|eletronico|anvisa)"

    patterns = [
        rf"\bmedicamento referencia\s+([a-z0-9]+(?:\s+[a-z0-9]+){{0,3}}?)(?=\s+{tail_words}\b|$)",
        rf"\breferencia\s+([a-z0-9]+(?:\s+[a-z0-9]+){{0,3}}?)(?=\s+{tail_words}\b|$)",
    ]

    for pattern in patterns:
        match = re.search(pattern, processed_text, flags=re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            value = re.sub(
                rf"\b{tail_words}\b.*$",
                "",
                value,
                flags=re.IGNORECASE,
            ).strip()
            return value

    return None
