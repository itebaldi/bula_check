"""
anvisa_crawler.py
-----------------
Coleta medicamentos da API pública da ANVISA e os mapeia para o modelo
`Medicines` definido em `protocol.py`.

Estratégia de coleta:
  - Varredura por prefixos (letras a-z + dígitos 0-9), paginando a API de
    produtos: /api/consulta/medicamento/produtos/
  - Para cada produto, tenta obter URLs de PDF via Bulário
    (/api/consulta/bulario) para preencher patient_url.
  - Medicamentos sem `numeroRegistro` são pulados.

Funções públicas:
  get_by_name(name, save_jsons, save_sqlite)  — busca um medicamento pelo nome
  crawl(prefixes, save_jsons, save_sqlite)    — varre todos os prefixos

Uso rápido:
    python anvisa_crawler.py
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests

from bula_check.protocol import DEFAULT_DB_PATH
from bula_check.protocol import Medicines
from bula_check.protocol import init_db
from bula_check.protocol import normalize_processed_field
from bula_check.protocol import save_medicine

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
BASE_URL = "https://consultas.anvisa.gov.br"
PRODUTOS_API = f"{BASE_URL}/api/consulta/medicamento/produtos/"
BULARIO_API = f"{BASE_URL}/api/consulta/bulario"
DETAIL_API = f"{BASE_URL}/api/consulta/medicamento/produtos/codigo"
PDF_API = f"{BASE_URL}/api/consulta/medicamentos/arquivo/bula/parecer"

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

PAGE_SIZE = 100
SLEEP_BETWEEN_PAGES = 1.0
SLEEP_BETWEEN_PREFIXES = 2.0
MAX_RETRIES = 6
RETRY_BACKOFFS = [5, 15, 30, 60, 120]
THROTTLE_THRESHOLD = 3
THROTTLE_PAUSE = 120

DEFAULT_PREFIXES = [chr(c) for c in range(ord("a"), ord("z") + 1)] + [
    str(d) for d in range(10)
]

LOG_FILE = Path("anvisa_crawler.log")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceções internas
# ---------------------------------------------------------------------------
class _Throttled(Exception):
    """Sinaliza throttling detectado — pausa longa necessária."""


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def _make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def _get_page(session: requests.Session, prefix: str, page: int) -> dict:
    """Busca uma página da API de produtos com backoff escalonado."""
    consecutive_errors = 0

    for attempt in range(MAX_RETRIES):
        try:
            qs = urlencode(
                {"column": "", "count": PAGE_SIZE, "order": "asc", "page": page}
            )
            qs += f"&filter[nomeProduto]={prefix}"
            url = f"{PRODUTOS_API}?{qs}"

            resp = session.get(url, timeout=45)

            if resp.status_code in (500, 503):
                consecutive_errors += 1
                wait = RETRY_BACKOFFS[min(attempt, len(RETRY_BACKOFFS) - 1)]
                log.warning(
                    "HTTP %d  tentativa %d/%d  prefix=%r page=%d  → aguardando %ds",
                    resp.status_code,
                    attempt + 1,
                    MAX_RETRIES,
                    prefix,
                    page,
                    wait,
                )
                if consecutive_errors >= THROTTLE_THRESHOLD:
                    raise _Throttled()
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except _Throttled:
            raise
        except requests.Timeout:
            consecutive_errors += 1
            wait = RETRY_BACKOFFS[min(attempt, len(RETRY_BACKOFFS) - 1)]
            log.warning(
                "Timeout  tentativa %d/%d  prefix=%r page=%d  → aguardando %ds",
                attempt + 1,
                MAX_RETRIES,
                prefix,
                page,
                wait,
            )
            time.sleep(wait)
        except requests.HTTPError as exc:
            wait = RETRY_BACKOFFS[min(attempt, len(RETRY_BACKOFFS) - 1)]
            log.warning(
                "HTTP %s  tentativa %d/%d  prefix=%r page=%d  → aguardando %ds",
                exc.response.status_code,
                attempt + 1,
                MAX_RETRIES,
                prefix,
                page,
                wait,
            )
            time.sleep(wait)
        except Exception as exc:
            wait = RETRY_BACKOFFS[min(attempt, len(RETRY_BACKOFFS) - 1)]
            log.warning(
                "Erro tentativa %d/%d  prefix=%r page=%d: %s  → aguardando %ds",
                attempt + 1,
                MAX_RETRIES,
                prefix,
                page,
                exc,
                wait,
            )
            time.sleep(wait)

    raise RuntimeError(
        f"Página não obtida após {MAX_RETRIES} tentativas (prefix={prefix!r} page={page})"
    )


def _fetch_detail(session: requests.Session, product_id: int | None) -> dict | None:
    if product_id is None:
        return None
    try:
        resp = session.get(f"{DETAIL_API}/{product_id}", timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def _fetch_bulario_by_registration(
    session: requests.Session, registration: str, max_results: int = 5
) -> list[dict]:
    try:
        qs = urlencode({"count": max_results, "order": "asc", "page": 1})
        qs += f"&filter[numeroRegistro]={registration}"
        resp = session.get(f"{BULARIO_API}?{qs}", timeout=30)
        resp.raise_for_status()
        return resp.json().get("content") or []
    except Exception:
        return []


def _pdf_url(token: str | None) -> str | None:
    if not isinstance(token, str) or not token:
        return None
    return f"{PDF_API}/{token}/?Authorization=Guest"


# ---------------------------------------------------------------------------
# Conversão de raw API item → Medicines
# ---------------------------------------------------------------------------
def _active_ingredient(detail: dict | None) -> list[str] | None:
    if not isinstance(detail, dict):
        return None
    apresentacoes = detail.get("apresentacoes")
    if isinstance(apresentacoes, list):
        seen: set[str] = set()
        result: list[str] = []
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


def _int_or_none(v: Any) -> int | None:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _str_or_none(v: Any) -> str | None:
    s = str(v).strip() if v is not None else None
    return s or None


def _item_to_medicine(
    item: dict,
    session: requests.Session,
) -> Medicines | None:
    """
    Converte um item cru da API de produtos para Medicines.
    Retorna None se o medicamento não tiver numeroRegistro.
    """
    p = item.get("produto") or {}
    e = item.get("empresa") or {}
    pr = item.get("processo") or {}

    registration_str = _str_or_none(p.get("numeroRegistro"))
    if not registration_str:
        return None  # pula medicamentos sem número de registro

    registration_number = _int_or_none(re.sub(r"\D", "", registration_str) or None)

    product_id = _int_or_none(p.get("codigo"))
    detail = _fetch_detail(session, product_id)

    # Tenta obter URL da bula via bulário
    patient_url: str | None = None
    if registration_str:
        digits = re.sub(r"\D", "", registration_str)
        bulario_rows = _fetch_bulario_by_registration(session, digits)
        for row in bulario_rows:
            token = row.get("idBulaPacienteProtegido")
            if token:
                patient_url = _pdf_url(token)
                break

    razao = e.get("razaoSocial") or ""
    cnpj_raw = e.get("cnpj")
    cnpj = _str_or_none(cnpj_raw)

    company_name = razao.strip() if razao else "Desconhecida"
    source_url = (
        f"{BASE_URL}/#/medicamentos/{product_id}"
        if product_id
        else f"{BASE_URL}/#/medicamentos"
    )
    url = patient_url or source_url

    active_ingredient = _active_ingredient(detail) or (
        [p["principioAtivo"]] if p.get("principioAtivo") else None
    )
    therapeutic_classes = _therapeutic_classes(detail)

    # extras = json.dumps(
    #     {
    #         "produto_codigo": product_id,
    #         "registration_number_formatted": p.get("numeroRegistroFormatado"),
    #         "tipo_autorizacao": p.get("tipoAutorizacao"),
    #         "situacao": p.get("situacaoApresentacao"),
    #         "categoria_regulatoria": (p.get("categoriaRegulatoria") or {}).get(
    #             "descricao"
    #         ),
    #         "data_registro": p.get("dataRegistro"),
    #         "data_vencimento": p.get("dataVencimentoRegistro"),
    #         "medicamento_referencia": p.get("medicamentoReferencia"),
    #         "processo": pr.get("numeroProcessoFormatado"),
    #         "professional_url": None,  # pode ser preenchido manualmente se necessário
    #     },
    #     ensure_ascii=False,
    # )

    name = str(p.get("nome") or "Medicamento")

    processed_ai = (
        [normalize_processed_field(ai) for ai in active_ingredient]
        if active_ingredient
        else None
    )

    return Medicines(
        id=str(uuid.uuid4()),
        name=name,
        processed_name=normalize_processed_field(name),
        active_ingredient=active_ingredient,
        processed_active_ingredient=processed_ai,
        source="anvisa",
        url=url,
        registration_number=registration_number,
        therapeutic_classes=therapeutic_classes,
        company_name=company_name,
        processed_company_name=normalize_processed_field(company_name),
        cnpj=cnpj,
    )


# ---------------------------------------------------------------------------
# Função pública: busca por nome
# ---------------------------------------------------------------------------
def get_by_name(
    name: str,
    save_jsons: bool = False,
    save_sqlite: bool = False,
    db_path: Path = DEFAULT_DB_PATH,
    json_output_dir: Path = Path("outputs/anvisa/json"),
) -> list[Medicines]:
    """
    Busca medicamentos da ANVISA pelo nome e retorna lista de Medicines.

    Parameters
    ----------
    name : str
        Nome (ou prefixo) do medicamento.
    save_jsons : bool
        Se True, salva um JSON único com a lista de resultados para debug.
    save_sqlite : bool
        Se True, upsert no banco SQLite (tabela medicines).
    db_path : Path
        Caminho do banco SQLite.
    json_output_dir : Path
        Diretório onde o JSON de debug será salvo.

    Returns
    -------
    list[Medicines]
    """
    session = _make_session()
    medicines: list[Medicines] = []
    seen_ids: set = set()

    page = 1
    total_pages: int | None = None

    while total_pages is None or page <= total_pages:
        try:
            qs = urlencode(
                {"column": "", "count": PAGE_SIZE, "order": "asc", "page": page}
            )
            qs += f"&filter[nomeProduto]={name}"
            resp = session.get(f"{PRODUTOS_API}?{qs}", timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log.error("Erro ao buscar nome=%r page=%d: %s", name, page, exc)
            break

        if total_pages is None:
            total_pages = int(data.get("totalPages") or 1)

        for item in data.get("content") or []:
            p = item.get("produto") or {}
            uid = p.get("codigo") or p.get("numeroRegistro")
            if uid in seen_ids:
                continue
            if uid:
                seen_ids.add(uid)

            med = _item_to_medicine(item, session)
            if med is not None:
                medicines.append(med)

        if page >= total_pages:
            break
        page += 1
        time.sleep(SLEEP_BETWEEN_PAGES)

    if save_jsons and medicines:
        json_output_dir.mkdir(parents=True, exist_ok=True)
        safe = re.sub(r"\W+", "_", name).strip("_").lower()
        out = json_output_dir / f"anvisa_{safe}.json"
        out.write_text(
            json.dumps(
                [m.model_dump() for m in medicines], ensure_ascii=False, indent=2
            ),
            encoding="utf-8",
        )
        log.info("JSON salvo: %s (%d medicamentos)", out, len(medicines))

    if save_sqlite and medicines:
        conn = init_db(db_path)
        try:
            for med in medicines:
                save_medicine(conn, med)
            conn.commit()
            log.info("SQLite: %d medicamentos salvos em %s", len(medicines), db_path)
        finally:
            conn.close()

    return medicines


# ---------------------------------------------------------------------------
# Função pública: crawler completo
# ---------------------------------------------------------------------------
def crawl(
    prefixes: list[str] | None = None,
    save_jsons: bool = False,
    save_sqlite: bool = False,
    db_path: Path = DEFAULT_DB_PATH,
    json_output_dir: Path = Path("outputs/anvisa/json"),
) -> list[Medicines]:
    """
    Varre todos os prefixos da ANVISA e retorna lista de Medicines.

    Parameters
    ----------
    prefixes : list[str] | None
        Lista de prefixos. Padrão: letras a-z + dígitos 0-9.
        Exemplos: list("ABC") ou ["0","1","2"]
    save_jsons : bool
        Se True, salva um JSON único de debug com todos os resultados.
    save_sqlite : bool
        Se True, faz upsert de cada medicamento no banco SQLite.
    db_path : Path
        Caminho do banco SQLite.
    json_output_dir : Path
        Diretório para o JSON de debug.

    Returns
    -------
    list[Medicines]
    """
    if prefixes is None:
        prefixes = DEFAULT_PREFIXES

    session = _make_session()
    all_medicines: list[Medicines] = []
    seen_ids: set = set()

    conn: sqlite3.Connection | None = None
    if save_sqlite:
        conn = init_db(db_path)

    try:
        for prefix in prefixes:
            log.info("=== Prefix: %r ===", prefix)
            page = 1
            total_pages: int | None = None
            skipped_pages: list[int] = []

            while total_pages is None or page <= total_pages:
                log.info(
                    "prefix=%-3r  page=%3d/%s  total=%d",
                    prefix,
                    page,
                    total_pages or "?",
                    len(all_medicines),
                )

                try:
                    data = _get_page(session, prefix, page)

                except _Throttled:
                    log.warning(
                        "Throttling — pausando %ds antes de retomar prefix=%r page=%d",
                        THROTTLE_PAUSE,
                        prefix,
                        page,
                    )
                    time.sleep(THROTTLE_PAUSE)
                    continue

                except RuntimeError as exc:
                    log.error(
                        "Pulando página: prefix=%r page=%d — %s", prefix, page, exc
                    )
                    skipped_pages.append(page)
                    page += 1
                    time.sleep(SLEEP_BETWEEN_PAGES)
                    continue

                if total_pages is None:
                    total_pages = int(data.get("totalPages") or 1)
                    log.info("  total_pages para prefix=%r: %d", prefix, total_pages)

                items = data.get("content") or []
                if not items:
                    log.info(
                        "  Sem itens na page=%d — encerrando prefix=%r", page, prefix
                    )
                    break

                for item in items:
                    p = item.get("produto") or {}
                    uid = p.get("codigo") or p.get("numeroRegistro")
                    if uid in seen_ids:
                        continue
                    if uid:
                        seen_ids.add(uid)

                    med = _item_to_medicine(item, session)
                    if med is None:
                        continue  # sem numeroRegistro — pula

                    all_medicines.append(med)

                    if conn is not None:
                        save_medicine(conn, med)
                        conn.commit()

                log.info("  → page %d/%d processada", page, total_pages)

                if page >= total_pages:
                    break
                page += 1
                time.sleep(SLEEP_BETWEEN_PAGES)

            if skipped_pages:
                log.warning(
                    "prefix=%r concluído com %d página(s) pulada(s): %s",
                    prefix,
                    len(skipped_pages),
                    skipped_pages,
                )

            time.sleep(SLEEP_BETWEEN_PREFIXES)

    finally:
        if conn is not None:
            n = conn.execute("SELECT COUNT(*) FROM medicines").fetchone()[0]
            log.info("SQLite: %d medicamentos em %s", n, db_path)
            conn.close()

    if save_jsons:
        json_output_dir.mkdir(parents=True, exist_ok=True)
        out = json_output_dir / "anvisa_all.json"
        out.write_text(
            json.dumps(
                [m.model_dump() for m in all_medicines],
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        log.info("JSON salvo: %s (%d medicamentos)", out, len(all_medicines))

    log.info("Crawler ANVISA concluído — %d medicamentos", len(all_medicines))
    return all_medicines


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # python -m bula_check.anvisa_crawler
    log.info(
        "Iniciando crawler ANVISA — %d prefixos, PAGE_SIZE=%d",
        len(DEFAULT_PREFIXES),
        PAGE_SIZE,
    )
    path = Path("bulas_anvisa.db")
    records = crawl(save_sqlite=True, db_path=path, prefixes=["A"])
    log.info("Pronto! %d medicamentos coletados. DB: %s", len(records), path)
