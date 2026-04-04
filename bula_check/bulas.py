from __future__ import annotations

import re
import string
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from bs4 import Tag
from pydantic import BaseModel
from toolz.functoolz import pipe

from bula_check.constants import SECTION_PATTERNS
from bula_check.preprocessing.text import lowercase_text
from bula_check.preprocessing.text import normalize_text_whitespace
from bula_check.preprocessing.text import remove_text_accents
from bula_check.preprocessing.text import remove_text_punctuation
from bula_check.preprocessing.text import replace_spaces_with_text_underscores


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
    """
    Represents the extracted sections of a bula.

    Attributes
    ----------
    indications : str | None
        Text content of the "Indicações" section.
    contraindications : str | None
        Text content of the "Contraindicações" section.
    warnings_and_precautions : str | None
        Text content of the "Advertências e Precauções" section.
    adverse_reactions : str | None
        Text content of the "Reações Adversas" section.
    """

    indications: Optional[str] = None
    contraindications: Optional[str] = None
    warnings_and_precautions: Optional[str] = None
    adverse_reactions: Optional[str] = None


class Bula(BaseModel):
    """
    Result entry returned from Bula Grátis.

    Attributes
    ----------
    drug_name : str
        Drug name extracted from the bula page.
    company_name : str | None
        Manufacturer or laboratory name, if available.
    source_url : str
        Original URL of the medication page.
    patient_url : str
        URL of the patient bula page.
    raw_text : str
        The full, concatenated text content of the bula.
    created_at : datetime
        Timestamp of when the instance was created.
    sections : Sections
        Extracted content from specific sections of the bula.
    """

    drug_name: str
    company_name: Optional[str]
    source_url: str
    patient_url: str
    raw_text: str
    created_at: datetime = datetime.now()
    sections: Sections

    def write_to_json(
        self,
        output_path: str | Path,
        indent: int = 4,
    ) -> Path:
        """
        Write the Bula instance to a JSON file.

        Parameters
        ----------
        output_path : str | Path
            Path to the output JSON file.
        indent : int, default=4
            JSON indentation level.

        Returns
        -------
        Path
            The path where the file was saved.
        """
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.model_dump_json(indent=indent), encoding="utf-8")
        return path

    @classmethod
    def read_from_json(cls, input_path: str | Path) -> Bula:
        """
        Load a Bula instance from a JSON file.

        Parameters
        ----------
        input_path : str | Path
            Path to the input JSON file.

        Returns
        -------
        Bula
            The loaded Bula instance.
        """
        path = Path(input_path)
        json_data = path.read_text(encoding="utf-8")
        return cls.model_validate_json(json_data)


class BulaGratisClient:
    """
    Client for retrieving medication bulas from Bula Grátis.

    Parameters
    ----------
    timeout : int, default=30
        Request timeout in seconds.
    sleep_between_requests : float, default=0.0
        Delay between requests, in seconds.

        This delay is not required for correctness. It exists only to make
        repeated requests less aggressive when scraping many pages in sequence,
        helping reduce the chance of temporary blocking or rate limiting.
        For single-medication searches or local tests, it can usually be set
        to ``0.0``.

    Notes
    -----
    This client does not use browser automation. It relies on regular HTTP
    requests and HTML parsing with BeautifulSoup.

    The main index page is::

        https://bula.gratis/todas
    """

    BASE_URL: str = "https://bula.gratis"
    INDEX_URL: str = f"{BASE_URL}/todas"

    def __init__(
        self,
        timeout: int = 30,
        sleep_between_requests: float = 0.0,
    ) -> None:
        self.timeout = timeout
        self.sleep_between_requests = sleep_between_requests

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (compatible; BulaGratisClient/1.0; "
                    "+https://example.com/contact)"
                )
            }
        )

        self._index_cache: dict[str, list[dict[str, str]]] | None = None
        """
        In-memory cache for the parsed `/todas` index.
        Once loaded, the index can be reused across multiple searches in the same
        client instance, avoiding repeated downloads and parsing of the full A-Z page.
        """

    def search(
        self,
        medication: str,
        limit: int = 10,
        save_json: bool = False,
    ) -> list[Bula]:
        """
        Search bulas for a medication name.

        Parameters
        ----------
        medication : str
            Medication name or partial medication name.
        limit : int, default=10
            Maximum number of results to return.
        save_json : bool, default=False
            Whether to save each matched bula as a JSON file.

        Returns
        -------
        list[Bula]
            Matching bula results.
        """
        medication_norm = _normalize_text(medication)
        links_by_letter = self._get_index_links()

        first_letter = medication_norm[:1].upper()
        candidate_items: list[dict[str, str]]

        if first_letter in string.ascii_uppercase:
            candidate_items = links_by_letter.get(first_letter, [])
        else:
            candidate_items = [
                item for items in links_by_letter.values() for item in items
            ]

        matches: list[Bula] = []

        for item in candidate_items:
            med_url = item["url"]
            label = item["label"]

            last_path_norm = _normalize_text(self._get_last_path_segment(med_url))
            label_norm = _normalize_text(label)

            if (
                medication_norm not in last_path_norm
                and medication_norm not in label_norm
            ):
                continue

            result = self._build_bula_instance(
                med_url=med_url,
                save_json=save_json,
            )
            if result is not None:
                matches.append(result)

            if len(matches) >= limit:
                break

            if self.sleep_between_requests > 0:
                time.sleep(self.sleep_between_requests)

        return matches

    def get_by_url(
        self,
        med_url: str,
        save_json: bool = False,
    ) -> Optional[Bula]:
        """
        Retrieve bula metadata directly from a medication URL.

        Parameters
        ----------
        med_url : str
            Medication page URL.
        save_json : bool, default=False
            Whether to save the bula as a JSON file.

        Returns
        -------
        Bula | None
            Parsed bula result, or ``None`` if extraction fails.
        """
        return self._build_bula_instance(med_url=med_url, save_json=save_json)

    def _build_bula_instance(
        self,
        med_url: str,
        save_json: bool,
    ) -> Optional[Bula]:
        """
        Build a bula result from a medication page URL.

        Parameters
        ----------
        med_url : str
            Medication page URL.
        save_json : bool
            Whether to save the bula as a JSON file.

        Returns
        -------
        Bula | None
            Parsed result, or ``None`` if parsing fails.
        """
        patient_url = _build_patient_url(med_url)
        patient_html = self._get_parsed_html(patient_url)

        bula_instance = gen_bula_instance(med_url, patient_html)

        drug_name = bula_instance.drug_name
        company_name = bula_instance.company_name

        if save_json:
            # TODO check on windows
            output_dir = Path("inputs/bulas/json")

            safe_name = self._gen_safe_filename(drug_name or "medication")
            safe_company = self._gen_safe_filename(company_name or "unknown_company")
            filename = f"{safe_name}__{safe_company}.json"
            local_json_path = output_dir / filename
            bula_instance.write_to_json(local_json_path)

        return bula_instance

    def _get_index_links(self) -> dict[str, list[dict[str, str]]]:
        """
        Retrieve and cache medication links from the A-Z index.

        Returns
        -------
        dict[str, list[dict[str, str]]]
            Dictionary mapping initial letter to result entries.
        """
        if self._index_cache is not None:
            return self._index_cache

        index_html = self._get_parsed_html(self.INDEX_URL)
        self._index_cache = self._group_links_by_letter(index_html)
        return self._index_cache

    def _get_parsed_html(self, url: str) -> BeautifulSoup:
        """
        Perform a GET request and parse the HTML response.

        Parameters
        ----------
        url : str
            Target URL.

        Returns
        -------
        BeautifulSoup
            Parsed HTML document.
        """
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def _group_links_by_letter(
        self,
        index_html: BeautifulSoup,
    ) -> dict[str, list[dict[str, str]]]:
        """
        Extract medication links from the A-Z index page and group them by letter.

        Parameters
        ----------
        index_html : BeautifulSoup
            Parsed index page.

        Returns
        -------
        dict[str, list[dict[str, str]]]
            Dictionary mapping letters to result entries with URL and label.
        """
        links_by_letter: dict[str, list[dict[str, str]]] = {
            letter: [] for letter in string.ascii_uppercase
        }

        for anchor in index_html.find_all("a", href=True):
            text = normalize_text_whitespace(anchor.get_text(" ", strip=True))
            href = anchor["href"]
            full_url = urljoin(self.BASE_URL, href)  # type: ignore[arg-type]

            if not text:
                continue

            if text in string.ascii_uppercase:
                continue

            if text.lower() in {"página inicial", "bulas de a a z", "recentes"}:
                continue

            parsed = urlparse(full_url)
            path_parts = [part for part in parsed.path.split("/") if part]

            if len(path_parts) >= 3 and path_parts[1] in {"0", "1"}:
                first_char = text[0].upper()
                if first_char in links_by_letter:
                    links_by_letter[first_char].append(
                        {
                            "url": full_url,
                            "label": text,
                        }
                    )

        for letter, items in links_by_letter.items():
            deduped: list[dict[str, str]] = []
            seen_urls: set[str] = set()

            for item in items:
                if item["url"] in seen_urls:
                    continue
                seen_urls.add(item["url"])
                deduped.append(item)

            links_by_letter[letter] = deduped

        return links_by_letter

    def _get_last_path_segment(self, url: str) -> str:
        """
        Extract the last path segment from a medication URL.

        Parameters
        ----------
        url : str
            Medication URL.

        Returns
        -------
        str
            URL slug.
        """
        parsed = urlparse(url)
        parts = [part for part in parsed.path.split("/") if part]
        return parts[-1] if parts else ""

    def _gen_safe_filename(self, name: str) -> str:
        """
        Sanitize a string for use as a filename.

        Parameters
        ----------
        name : str
            Input string.

        Returns
        -------
        str
            Safe filename.
        """
        return pipe(
            lowercase_text(name),
            remove_text_accents,
            replace_spaces_with_text_underscores,
            remove_text_punctuation,
            normalize_text_whitespace,
        )

    def _iter_all_bula_urls(self) -> list[str]:
        """
        Return all medication URLs found in the A-Z index.

        Returns
        -------
        list[str]
            Deduplicated list of medication URLs.
        """
        links_by_letter = self._get_index_links()

        all_urls = [
            item["url"] for items in links_by_letter.values() for item in items
        ]

        return list(dict.fromkeys(all_urls))

    def save_all_to_json(
        self,
        limit: int | None = None,
        continue_on_error: bool = True,
        save_logs: bool = True,
    ) -> Logs:
        """
        Fetch all bulas from the index and save one JSON file per bula.

        Parameters
        ----------
        limit : int | None, optional
            Maximum number of bulas to process. If ``None``, process all.
        continue_on_error : bool, default=True
            Whether to continue processing after an error.
        save_logs : bool, default=True
            Whether to save a JSON log file with processing summary.

        Returns
        -------
        Logs
            Summary with saved files, processed URLs, and failures.
        """
        dir = Path("inputs/bulas")

        output_dir = dir / "json"
        output_dir.mkdir(parents=True, exist_ok=True)

        urls = self._iter_all_bula_urls()
        if limit is not None:
            urls = urls[:limit]

        saved_paths: list[Path] = []
        failures: list[dict[str, str]] = []

        for med_url in urls:
            try:
                bula = self.get_by_url(med_url, save_json=False)
                if bula is None:
                    failures.append(
                        {
                            "url": med_url,
                            "error": "Could not build Bula instance.",
                        }
                    )
                    continue

                safe_name = self._gen_safe_filename(bula.drug_name or "medication")
                safe_company = self._gen_safe_filename(
                    bula.company_name or "unknown_company"
                )
                filename = f"{safe_name}__{safe_company}.json"
                output_path = output_dir / filename

                bula.write_to_json(output_path)
                saved_paths.append(output_path)

            except Exception as exc:
                failures.append(
                    {
                        "url": med_url,
                        "error": str(exc),
                    }
                )
                if not continue_on_error:
                    raise

            if self.sleep_between_requests > 0:
                time.sleep(self.sleep_between_requests)

        logs = Logs(
            processed=len(urls),
            saved=len(saved_paths),
            failure=len(failures),
            failures=failures,
        )

        if save_logs:
            logs_path = dir / "logs.json"
            logs.write_to_json(logs_path)

        return logs


def _normalize_text(text: str) -> str:
    """
    Normalize text for case-insensitive matching.

    Parameters
    ----------
    text : str
        Input text.

    Returns
    -------
    str
        Normalized text.
    """
    return pipe(
        lowercase_text(text),
        remove_text_accents,
        remove_text_punctuation,
        normalize_text_whitespace,
    )


def _build_patient_url(med_url: str) -> str:
    """
    Ensure the bula URL points to the patient version.

    Parameters
    ----------
    med_url : str
        A URL for a medication on bula.gratis.

    Returns
    -------
    str
        The URL for the patient-specific bula.
    """
    return (
        med_url
        if med_url.endswith("/paciente")
        else med_url.rstrip("/") + "/paciente"
    )


def gen_bula_instance(med_url: str, patient_html: BeautifulSoup) -> Bula:
    """
    Generate a Bula instance from its URL and parsed patient HTML.

    Parameters
    ----------
    med_url : str
        The source URL for the medication.
    patient_html : BeautifulSoup
        The parsed HTML content of the patient bula page.

    Returns
    -------
    Bula
        A populated Bula instance with extracted information.
    """

    patient_url = _build_patient_url(med_url)

    root = _find_bula_article(patient_html)
    blocks = _collect_heading_blocks(root)

    return Bula(
        drug_name=_get_drug_name(patient_html) or "medication",
        company_name=_get_company_name(patient_html),
        source_url=med_url,
        patient_url=patient_url,
        raw_text=_build_raw_text(blocks),
        sections=_extract_sections(blocks),
    )


def _get_drug_name(soup: BeautifulSoup) -> str | None:
    """
    Extract the drug name from the parsed HTML of a bula page.

    It first attempts to find the name in the ``#nome_medicamento`` element.
    If that fails, it falls back to parsing the page's ``<title>`` tag.

    Parameters
    ----------
    soup : BeautifulSoup
        The parsed HTML of the bula page.

    Returns
    -------
    str | None
        The extracted drug name, or ``None`` if it cannot be found.
    """
    tag = soup.select_one("#nome_medicamento")
    if tag:
        return normalize_text_whitespace(tag.get_text())
    title = soup.title.get_text() if soup.title else ""
    match = re.match(r"^(.*?)\s*\(Bula", title, flags=re.IGNORECASE)
    return normalize_text_whitespace(match.group(1)) if match else None


def _get_company_name(soup: BeautifulSoup) -> str | None:
    """
    Extract the company name from the parsed HTML of a bula page.

    It looks for the ``#empresa_medicamento`` element and cleans the
    extracted text by removing the trailing CNPJ identifier.

    Parameters
    ----------
    soup : BeautifulSoup
        The parsed HTML of the bula page.

    Returns
    -------
    str | None
        The extracted company name, or ``None`` if it cannot be found.
    """
    tag = soup.select_one("#empresa_medicamento")
    if tag:
        text = normalize_text_whitespace(tag.get_text(" ", strip=True))
        # remove CNPJ no fim
        text = re.sub(r"\s*-\s*\d{8,14}\b.*$", "", text).strip()
        return text
    return None


def _find_bula_article(soup: BeautifulSoup) -> Tag:
    """
    Find the main <article> tag containing the bula content.

    Parameters
    ----------
    soup : BeautifulSoup
        The parsed HTML of the bula page.

    Returns
    -------
    Tag
        The BeautifulSoup ``Tag`` for the <article> element.

    Raises
    ------
    ValueError
        If the <article> tag cannot be found in the document.
    """
    article = soup.find("article")
    if article is None:
        raise ValueError("Could not find bula article content in HTML.")
    return article


def _collect_heading_blocks(root: Tag) -> list[dict[str, str]]:
    """
    Group bula content into blocks based on <h3> headings.

    Parameters
    ----------
    root : Tag
        The root HTML element (typically the <article>) to search within.

    Returns
    -------
    list[dict[str, str]]
        A list of dictionaries, each containing a "heading" and its "content".
    """
    blocks: list[dict[str, str]] = []
    current_heading: str | None = None
    current_parts: list[str] = []

    for node in root.find_all(["h3", "p"], recursive=True):
        text = normalize_text_whitespace(node.get_text(" ", strip=True))
        if not text:
            continue

        if node.name == "h3":
            if current_heading is not None:
                blocks.append(
                    {
                        "heading": current_heading,
                        "content": normalize_text_whitespace(
                            " ".join(current_parts)
                        ),
                    }
                )
            current_heading = text
            current_parts = []
        else:
            if current_heading is not None:
                current_parts.append(text)

    if current_heading is not None:
        blocks.append(
            {
                "heading": current_heading,
                "content": normalize_text_whitespace(" ".join(current_parts)),
            }
        )

    return blocks


def _build_raw_text(blocks: list[dict[str, str]]) -> str:
    """
    Concatenate all heading and content blocks into a single raw text string.

    Parameters
    ----------
    blocks : list[dict[str, str]]
        A list of heading/content blocks from ``_collect_heading_blocks``.

    Returns
    -------
    str
        The full, formatted raw text of the bula.
    """
    parts: list[str] = []
    for block in blocks:
        parts.append(block["heading"])
        if block["content"]:
            parts.append(block["content"])
    return normalize_text_whitespace("\n\n".join(parts))


def _extract_sections(blocks: list[dict[str, str]]) -> Sections:
    """
    Extract specific, predefined sections from the bula content blocks.

    Parameters
    ----------
    blocks : list[dict[str, str]]
        A list of heading/content blocks from ``_collect_heading_blocks``.

    Returns
    -------
    Sections
        A ``Sections`` instance populated with the extracted content.
    """
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
