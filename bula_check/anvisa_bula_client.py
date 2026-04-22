"""
anvisa_bula_client.py
----------------------
Busca bulas da Anvisa pelo nome do produto e baixa os PDFs.
Sem Playwright — usa apenas requests direto nas APIs públicas.

Estratégia de busca:
  1. Tenta o Bulário (/api/consulta/bulario) — tem os tokens de PDF direto
  2. Se não achar, cai no cadastro de produtos (/api/consulta/medicamento/produtos/)
     e tenta montar a URL de PDF a partir do idProduto

Uso:
    from anvisa_bula_client import AnvisaBulaClient

    client = AnvisaBulaClient()

    # por nome
    records = client.search("RINOSORO")

    # por número de registro (vindo da OBM)
    records = client.search_by_registration("1.0236.0161.001-9")

    for r in records:
        print(r.drug_name, r.patient_url)
        path = client.download_pdf(r, folder="pdfs/")
        print("salvo em", path)
"""

from __future__ import annotations

import re
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
BASE_URL = "https://consultas.anvisa.gov.br"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Authorization": "Guest",
    "Referer": f"{BASE_URL}/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}


# ---------------------------------------------------------------------------
# Modelo de saída
# ---------------------------------------------------------------------------
@dataclass
class AnvisaRecord:
    drug_name: str
    company_name: str | None
    source_url: str
    patient_url: str | None
    professional_url: str | None
    registration_number: str | None
    product_id: int | None
    process_number: str | None
    cnpj: str | None
    active_ingredient: list[str] | None = None
    therapeutic_classes: list[str] | None = None
    file_name: str | None = None


# ---------------------------------------------------------------------------
# Cliente
# ---------------------------------------------------------------------------
class AnvisaBulaClient:
    # Bulário: medicamentos com bula cadastrada + tokens de PDF
    BULARIO_API = f"{BASE_URL}/api/consulta/bulario"
    # Produtos: cadastro geral (inclui medicamentos sem bula no Bulário)
    PRODUTOS_API = f"{BASE_URL}/api/consulta/medicamento/produtos/"
    DETAIL_API = f"{BASE_URL}/api/consulta/medicamento/produtos/codigo"
    PDF_API = f"{BASE_URL}/api/consulta/medicamentos/arquivo/bula/parecer"

    def __init__(self, timeout: int = 30, sleep: float = 0.3) -> None:
        self.timeout = timeout
        self.sleep = sleep
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # ------------------------------------------------------------------
    # Busca principal
    # ------------------------------------------------------------------
    def search(self, name: str, max_results: int = 20) -> list[AnvisaRecord]:
        """
        Busca pelo nome do produto.

        Tenta primeiro o Bulário (tem PDFs diretos). Se não achar nada lá,
        cai no cadastro de produtos como fallback.
        """
        # 1) Bulário
        bulario_rows = self._fetch_bulario(
            filter_name="nomeProduto", value=name, max_results=max_results
        )
        records = [
            self._bulario_row_to_record(r) for r in bulario_rows if _has_pdf_token(r)
        ]

        if records:
            return records

        # 2) Fallback: cadastro de produtos
        print(
            f"  [info] '{name}' não encontrado no Bulário — tentando cadastro de produtos..."
        )
        produto_rows = self._fetch_produtos(name=name, max_results=max_results)
        return [self._produto_row_to_record(r) for r in produto_rows]

    def search_by_registration(
        self, registration_number: str, max_results: int = 20
    ) -> list[AnvisaRecord]:
        """Busca pelo número de registro (ex: '1.0236.0161.001-9' ou '10236016100')."""
        digits = "".join(ch for ch in registration_number if ch.isdigit())

        # 1) Bulário por número de registro
        bulario_rows = self._fetch_bulario(
            filter_name="numeroRegistro", value=digits, max_results=max_results
        )
        records = [
            self._bulario_row_to_record(r) for r in bulario_rows if _has_pdf_token(r)
        ]

        if records:
            return records

        # 2) Fallback: cadastro de produtos por número de registro
        print(
            f"  [info] registro '{digits}' não encontrado no Bulário — tentando cadastro de produtos..."
        )
        produto_rows = self._fetch_produtos(
            registration=digits, max_results=max_results
        )
        return [self._produto_row_to_record(r) for r in produto_rows]

    # ------------------------------------------------------------------
    # Download de PDF
    # ------------------------------------------------------------------
    def download_pdf(
        self,
        record: AnvisaRecord,
        folder: str | Path = "pdfs",
        prefer_patient: bool = True,
    ) -> Path | None:
        """Baixa o PDF e salva em `folder`. Retorna o Path ou None."""
        url = (
            record.patient_url
            if prefer_patient and record.patient_url
            else record.professional_url or record.patient_url
        )
        if not url:
            print(f"  [aviso] {record.drug_name}: sem URL de PDF disponível")
            return None

        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()

        if not resp.content.startswith(b"%PDF"):
            raise ValueError(f"Resposta não é um PDF válido: {url}")

        out_dir = Path(folder)
        out_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{_safe_filename(record.drug_name)}.pdf"
        path = out_dir / filename
        path.write_bytes(resp.content)
        record.file_name = filename
        return path

    # ------------------------------------------------------------------
    # Fetch — Bulário
    # ------------------------------------------------------------------
    def _fetch_bulario(
        self, filter_name: str, value: str, max_results: int
    ) -> list[dict]:
        rows: list[dict] = []
        page = 1
        while len(rows) < max_results:
            qs = urlencode(
                {"count": min(20, max_results), "order": "asc", "page": page}
            )
            qs += f"&filter[{filter_name}]={value}"
            resp = self.session.get(f"{self.BULARIO_API}?{qs}", timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
            content = data.get("content") or []
            total_pages = int(data.get("totalPages") or 1)
            if not content:
                break
            rows.extend(content)
            if page >= total_pages:
                break
            page += 1
            time.sleep(self.sleep)
        return rows[:max_results]

    # ------------------------------------------------------------------
    # Fetch — Cadastro de Produtos (fallback)
    # ------------------------------------------------------------------
    def _fetch_produtos(
        self,
        name: str | None = None,
        registration: str | None = None,
        max_results: int = 20,
    ) -> list[dict]:
        qs = urlencode(
            {"column": "", "count": max_results, "order": "asc", "page": 1}
        )
        if name:
            qs += f"&filter[nomeProduto]={name}"
        elif registration:
            qs += f"&filter[numeroRegistro]={registration}"
        resp = self.session.get(f"{self.PRODUTOS_API}?{qs}", timeout=self.timeout)
        resp.raise_for_status()
        return resp.json().get("content") or []

    # ------------------------------------------------------------------
    # Fetch — Detalhe do produto
    # ------------------------------------------------------------------
    def _fetch_detail(self, product_id: int | None) -> dict | None:
        if product_id is None:
            return None
        try:
            resp = self.session.get(
                f"{self.DETAIL_API}/{product_id}", timeout=self.timeout
            )
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Conversão — row do Bulário → AnvisaRecord  (tem tokens de PDF)
    # ------------------------------------------------------------------
    def _bulario_row_to_record(self, row: dict) -> AnvisaRecord:
        product_id = _int_or_none(row.get("idProduto"))
        detail = self._fetch_detail(product_id)

        patient_url = self._pdf_url(row.get("idBulaPacienteProtegido"))
        professional_url = self._pdf_url(row.get("idBulaProfissionalProtegido"))

        empresa = (detail or {}).get("empresa") or {}
        razao = row.get("razaoSocial") or empresa.get("razaoSocial")
        cnpj = row.get("cnpj") or empresa.get("cnpj")
        company = f"{razao} — {cnpj}" if razao and cnpj else razao or cnpj
        process_number = _str_or_none(row.get("numProcesso"))

        return AnvisaRecord(
            drug_name=str(row.get("nomeProduto") or "medication"),
            company_name=company,
            source_url=f"{BASE_URL}/#/medicamentos/{product_id}?numeroProcesso={process_number}",
            patient_url=patient_url,
            professional_url=professional_url,
            registration_number=_str_or_none(row.get("numeroRegistro")),
            product_id=product_id,
            process_number=process_number,
            cnpj=_str_or_none(cnpj),
            active_ingredient=_active_ingredient(detail),
            therapeutic_classes=_therapeutic_classes(detail),
        )

    # ------------------------------------------------------------------
    # Conversão — row do cadastro de Produtos → AnvisaRecord
    # Produtos sem bula no Bulário não têm token de PDF — patient_url fica None
    # ------------------------------------------------------------------
    def _produto_row_to_record(self, row: dict) -> AnvisaRecord:
        produto = row.get("produto") or {}
        empresa = row.get("empresa") or {}
        processo = row.get("processo") or {}
        product_id = _int_or_none(produto.get("codigo"))
        detail = self._fetch_detail(product_id)

        razao = empresa.get("razaoSocial")
        cnpj = empresa.get("cnpj")
        company = f"{razao} — {cnpj}" if razao and cnpj else razao or cnpj

        # Tenta achar PDF mesmo assim via detalhe do produto
        patient_url, professional_url = self._pdf_urls_from_detail(detail)

        return AnvisaRecord(
            drug_name=str(produto.get("nome") or "medication"),
            company_name=company,
            source_url=f"{BASE_URL}/#/medicamentos/{product_id}",
            patient_url=patient_url,
            professional_url=professional_url,
            registration_number=_str_or_none(produto.get("numeroRegistro")),
            product_id=product_id,
            process_number=_str_or_none(processo.get("numeroProcessoFormatado")),
            cnpj=_str_or_none(cnpj),
            active_ingredient=_active_ingredient(detail),
            therapeutic_classes=_therapeutic_classes(detail),
        )

    def _pdf_url(self, token: str | None) -> str | None:
        if not isinstance(token, str) or not token:
            return None
        return f"{self.PDF_API}/{token}/?Authorization=Guest"

    def _pdf_urls_from_detail(
        self, detail: dict | None
    ) -> tuple[str | None, str | None]:
        """Tenta extrair URLs de PDF a partir do detalhe do produto."""
        if not isinstance(detail, dict):
            return None, None
        pat = self._pdf_url(detail.get("idBulaPacienteProtegido"))
        prof = self._pdf_url(detail.get("idBulaProfissionalProtegido"))
        return pat, prof


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _has_pdf_token(row: dict) -> bool:
    return bool(
        row.get("idBulaPacienteProtegido") or row.get("idBulaProfissionalProtegido")
    )


def _int_or_none(v: Any) -> int | None:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _str_or_none(v: Any) -> str | None:
    s = str(v).strip() if v is not None else None
    return s or None


def _active_ingredient(detail: dict | None) -> list[str] | None:
    if not isinstance(detail, dict):
        return None
    apresentacoes = detail.get("apresentacoes")
    if isinstance(apresentacoes, list):
        seen, result = set(), []
        for a in apresentacoes:
            for part in (a.get("principioAtivo") or "").split("+"):
                part = part.strip()
                if part and part not in seen:
                    seen.add(part)
                    result.append(part)
        return result or None
    pa = detail.get("principioAtivo")
    if isinstance(pa, str) and pa.strip():
        return [pa.strip()]
    return None


def _therapeutic_classes(detail: dict | None) -> list[str] | None:
    if not isinstance(detail, dict):
        return None
    classes = detail.get("classeTerapeutica") or detail.get("classesTA")
    if isinstance(classes, list):
        result = [str(c).strip() for c in classes if str(c).strip()]
        return result or None
    if isinstance(classes, str) and classes.strip():
        return [classes.strip()]
    return None


def _safe_filename(name: str) -> str:
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = re.sub(r"[^\w\s-]", "", name).strip().lower()
    return re.sub(r"\s+", "_", name)


# ---------------------------------------------------------------------------
# Uso via linha de comando:  python anvisa_bula_client.py RINOSORO
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    query = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "RINOSORO"
    print(f"Buscando: {query!r}\n")

    client = AnvisaBulaClient()
    records = client.search(query, max_results=5)

    if not records:
        print("Nenhum resultado encontrado.")
        sys.exit(0)

    for r in records:
        print(f"  {r.drug_name}")
        print(f"    Empresa:      {r.company_name}")
        print(f"    Registro:     {r.registration_number}")
        print(f"    PDF paciente: {r.patient_url or '(sem bula cadastrada)'}")
        print()

    first = records[0]
    if first.patient_url or first.professional_url:
        path = client.download_pdf(first, folder="pdfs/")
        print(f"PDF salvo em: {path}")
    else:
        print("Este medicamento não possui PDF de bula disponível.")
