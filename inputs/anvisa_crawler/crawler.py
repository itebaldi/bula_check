"""
anvisa_medicamentos_crawler.py  (v4 — retry robusto + retoma página após falha)
---------------------------------------------------------------------------------
Coleta todos os medicamentos da API pública da Anvisa.

Uso:
    python anvisa_medicamentos_crawler.py

Dependências:
    pip install requests

Saída:
    medicamentos.csv    — um medicamento por linha (abre direto no Excel)
    medicamentos.json   — lista completa em JSON
    medicamentos.db     — SQLite com tabelas normalizadas
    crawler.log         — log de progresso
"""

from __future__ import annotations

import csv
import json
import logging
import sqlite3
import time
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
BASE_URL = "https://consultas.anvisa.gov.br/api/consulta/medicamento/produtos/"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Authorization": "Guest",
    "Referer": "https://consultas.anvisa.gov.br/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

PAGE_SIZE = 100
SLEEP_BETWEEN_PAGES = 1.0  # segundos entre páginas normais
SLEEP_BETWEEN_PREFIXES = 2.0  # segundos entre cada letra
MAX_RETRIES = 6  # tentativas por página
# Backoff escalonado por tentativa (segundos): 5, 15, 30, 60, 120
RETRY_BACKOFFS = [5, 15, 30, 60, 120]
# Se receber N erros 503/timeout seguidos, pausa longa antes de continuar
THROTTLE_THRESHOLD = 3
THROTTLE_PAUSE = 120  # 2 minutos de pausa ao detectar throttling

OUTPUT_CSV = Path("medicamentos.csv")
OUTPUT_JSON = Path("medicamentos.json")
OUTPUT_DB = Path("medicamentos.db")
LOG_FILE = Path("crawler.log")

PREFIXES = [chr(c) for c in range(ord("a"), ord("z") + 1)] + [
    str(d) for d in range(10)
]

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
# Campos / flatten
# ---------------------------------------------------------------------------
FIELDNAMES = [
    "produto_codigo",
    "produto_nome",
    "produto_numeroRegistro",
    "produto_numeroRegistroFormatado",
    "produto_tipoAutorizacao",
    "produto_situacaoApresentacao",
    "produto_principioAtivo",
    "produto_categoriaRegulatoria",
    "produto_mesAnoVencimentoFormatado",
    "produto_dataRegistro",
    "produto_dataVencimentoRegistro",
    "produto_dataCancelamento",
    "produto_dataAtualizacao",
    "produto_medicamentoReferencia",
    "produto_complemento",
    "empresa_razaoSocial",
    "empresa_cnpj",
    "empresa_cnpjFormatado",
    "empresa_numeroAutorizacao",
    "processo_numeroProcessoFormatado",
    "processo_situacao",
]


def _flatten(item: dict) -> dict:
    p = item.get("produto") or {}
    e = item.get("empresa") or {}
    pr = item.get("processo") or {}
    cat_reg = p.get("categoriaRegulatoria") or {}
    return {
        "produto_codigo": p.get("codigo"),
        "produto_nome": p.get("nome"),
        "produto_numeroRegistro": p.get("numeroRegistro"),
        "produto_numeroRegistroFormatado": p.get("numeroRegistroFormatado"),
        "produto_tipoAutorizacao": p.get("tipoAutorizacao"),
        "produto_situacaoApresentacao": p.get("situacaoApresentacao"),
        "produto_principioAtivo": p.get("principioAtivo"),
        "produto_categoriaRegulatoria": cat_reg.get("descricao"),
        "produto_mesAnoVencimentoFormatado": p.get("mesAnoVencimentoFormatado"),
        "produto_dataRegistro": p.get("dataRegistro"),
        "produto_dataVencimentoRegistro": p.get("dataVencimentoRegistro"),
        "produto_dataCancelamento": p.get("dataCancelamento"),
        "produto_dataAtualizacao": p.get("dataAtualizacao"),
        "produto_medicamentoReferencia": p.get("medicamentoReferencia"),
        "produto_complemento": p.get("complemento"),
        "empresa_razaoSocial": e.get("razaoSocial"),
        "empresa_cnpj": e.get("cnpj"),
        "empresa_cnpjFormatado": e.get("cnpjFormatado"),
        "empresa_numeroAutorizacao": e.get("numeroAutorizacao"),
        "processo_numeroProcessoFormatado": pr.get("numeroProcessoFormatado"),
        "processo_situacao": pr.get("situacao"),
    }


# ---------------------------------------------------------------------------
# HTTP helper  (retorna None em caso de falha recuperável, levanta só se
#               esgotou todas as tentativas)
# ---------------------------------------------------------------------------
class _Throttled(Exception):
    """Servidor retornou 503 / timeout — sinaliza para pausar."""


def _get_page(session: requests.Session, prefix: str, page: int) -> dict:
    """
    Tenta buscar uma página com backoff escalonado.
    - 503 ou timeout → aguarda RETRY_BACKOFFS[attempt] segundos e tenta de novo
    - Após MAX_RETRIES falhas → levanta RuntimeError (página pulada, letra continua)
    """
    consecutive_errors = 0

    for attempt in range(MAX_RETRIES):
        try:
            # requests codifica [ ] como %5B%5D, mas a API da Anvisa exige colchetes
            # literais — por isso montamos a URL manualmente.
            from urllib.parse import urlencode

            qs = urlencode(
                {"column": "", "count": PAGE_SIZE, "order": "asc", "page": page}
            )
            qs += f"&filter[nomeProduto]={prefix}"
            url = f"{BASE_URL}?{qs}"

            resp = session.get(url, headers=HEADERS, timeout=45)

            if resp.status_code in (500, 503):
                consecutive_errors += 1
                wait = RETRY_BACKOFFS[min(attempt, len(RETRY_BACKOFFS) - 1)]
                # Na primeira ocorrência, loga a URL e o corpo para diagnóstico
                if attempt == 0:
                    try:
                        body = resp.text[:400]
                    except Exception:
                        body = "<não foi possível ler>"
                    log.warning(
                        "HTTP %d  tentativa 1/%d  prefix=%r page=%d  → aguardando %ds\n"
                        "  URL: %s\n  Resposta: %s",
                        resp.status_code,
                        MAX_RETRIES,
                        prefix,
                        page,
                        wait,
                        url,
                        body,
                    )
                else:
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


# ---------------------------------------------------------------------------
# Crawler principal
# ---------------------------------------------------------------------------
def crawl() -> list[dict]:
    seen_ids: set = set()
    all_records: list[dict] = []
    session = requests.Session()

    for prefix in PREFIXES:
        page = 1
        total_pages = None  # descoberto na 1ª resposta bem-sucedida
        skipped_pages: list[int] = []

        while total_pages is None or page <= total_pages:
            log.info(
                "prefix=%-3r  page=%3d/%s  total_coletado=%d",
                prefix,
                page,
                total_pages or "?",
                len(all_records),
            )

            try:
                data = _get_page(session, prefix, page)

            except _Throttled:
                log.warning(
                    "Throttling detectado — pausando %ds antes de retomar prefix=%r page=%d",
                    THROTTLE_PAUSE,
                    prefix,
                    page,
                )
                time.sleep(THROTTLE_PAUSE)
                # Tenta a mesma página de novo (não avança)
                continue

            except RuntimeError as exc:
                # Esgotou retries nesta página — pula ela mas continua a letra
                log.error(
                    "Pulando página (será registrada): prefix=%r page=%d — %s",
                    prefix,
                    page,
                    exc,
                )
                skipped_pages.append(page)
                page += 1
                time.sleep(SLEEP_BETWEEN_PAGES)
                continue

            items = data.get("content") or []
            if total_pages is None:
                total_pages = int(data.get("totalPages") or 1)
                log.info("  total_pages para prefix=%r: %d", prefix, total_pages)

            if not items:
                log.info(
                    "  Sem itens na page=%d — encerrando prefix=%r", page, prefix
                )
                break

            new_count = 0
            for item in items:
                p = item.get("produto") or {}
                uid = p.get("codigo") or p.get("numeroRegistro")
                if uid and uid in seen_ids:
                    continue
                if uid:
                    seen_ids.add(uid)
                all_records.append(_flatten(item))
                new_count += 1

            log.info("  → %d novos  (page %d/%d)", new_count, page, total_pages)

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

    return all_records


# ---------------------------------------------------------------------------
# Persistência — CSV
# ---------------------------------------------------------------------------
def save_csv(records: list[dict], path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
    log.info("CSV salvo: %s  (%d registros)", path, len(records))


# ---------------------------------------------------------------------------
# Persistência — JSON
# ---------------------------------------------------------------------------
def save_json(records: list[dict], path: Path) -> None:
    path.write_text(
        json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info("JSON salvo: %s  (%d registros)", path, len(records))


# ---------------------------------------------------------------------------
# Persistência — SQLite
# ---------------------------------------------------------------------------
_DDL = """
CREATE TABLE IF NOT EXISTS empresa (
    cnpj               TEXT PRIMARY KEY,
    razaoSocial        TEXT,
    cnpjFormatado      TEXT,
    numeroAutorizacao  TEXT
);

CREATE TABLE IF NOT EXISTS produto (
    codigo                    INTEGER PRIMARY KEY,
    nome                      TEXT,
    numeroRegistro            TEXT,
    numeroRegistroFormatado   TEXT,
    tipoAutorizacao           TEXT,
    situacaoApresentacao      TEXT,
    principioAtivo            TEXT,
    categoriaRegulatoria      TEXT,
    mesAnoVencimentoFormatado TEXT,
    dataRegistro              TEXT,
    dataVencimentoRegistro    TEXT,
    dataCancelamento          TEXT,
    dataAtualizacao           TEXT,
    medicamentoReferencia     TEXT,
    complemento               TEXT,
    empresa_cnpj              TEXT REFERENCES empresa(cnpj)
);

CREATE TABLE IF NOT EXISTS processo (
    numeroFormatado  TEXT PRIMARY KEY,
    situacao         INTEGER,
    produto_codigo   INTEGER REFERENCES produto(codigo)
);

CREATE INDEX IF NOT EXISTS idx_produto_nome           ON produto(nome);
CREATE INDEX IF NOT EXISTS idx_produto_numeroRegistro ON produto(numeroRegistro);
CREATE INDEX IF NOT EXISTS idx_produto_empresa        ON produto(empresa_cnpj);

CREATE VIEW IF NOT EXISTS medicamentos_view AS
SELECT
    p.codigo                    AS produto_codigo,
    p.nome                      AS produto_nome,
    p.numeroRegistro            AS produto_numeroRegistro,
    p.numeroRegistroFormatado   AS produto_numeroRegistroFormatado,
    p.tipoAutorizacao           AS produto_tipoAutorizacao,
    p.situacaoApresentacao      AS produto_situacaoApresentacao,
    p.principioAtivo            AS produto_principioAtivo,
    p.categoriaRegulatoria      AS produto_categoriaRegulatoria,
    p.mesAnoVencimentoFormatado AS produto_mesAnoVencimentoFormatado,
    p.dataRegistro              AS produto_dataRegistro,
    p.dataVencimentoRegistro    AS produto_dataVencimentoRegistro,
    p.dataCancelamento          AS produto_dataCancelamento,
    p.dataAtualizacao           AS produto_dataAtualizacao,
    p.medicamentoReferencia     AS produto_medicamentoReferencia,
    p.complemento               AS produto_complemento,
    e.razaoSocial               AS empresa_razaoSocial,
    e.cnpj                      AS empresa_cnpj,
    e.cnpjFormatado             AS empresa_cnpjFormatado,
    e.numeroAutorizacao         AS empresa_numeroAutorizacao,
    pr.numeroFormatado          AS processo_numeroProcessoFormatado,
    pr.situacao                 AS processo_situacao
FROM produto p
LEFT JOIN empresa  e  ON e.cnpj            = p.empresa_cnpj
LEFT JOIN processo pr ON pr.produto_codigo = p.codigo;
"""

_UPSERT_EMPRESA = """
INSERT INTO empresa (cnpj, razaoSocial, cnpjFormatado, numeroAutorizacao)
VALUES (:cnpj, :razaoSocial, :cnpjFormatado, :numeroAutorizacao)
ON CONFLICT(cnpj) DO UPDATE SET
    razaoSocial       = excluded.razaoSocial,
    cnpjFormatado     = excluded.cnpjFormatado,
    numeroAutorizacao = excluded.numeroAutorizacao;
"""

_UPSERT_PRODUTO = """
INSERT INTO produto (
    codigo, nome, numeroRegistro, numeroRegistroFormatado,
    tipoAutorizacao, situacaoApresentacao, principioAtivo,
    categoriaRegulatoria, mesAnoVencimentoFormatado,
    dataRegistro, dataVencimentoRegistro, dataCancelamento,
    dataAtualizacao, medicamentoReferencia, complemento, empresa_cnpj
) VALUES (
    :codigo, :nome, :numeroRegistro, :numeroRegistroFormatado,
    :tipoAutorizacao, :situacaoApresentacao, :principioAtivo,
    :categoriaRegulatoria, :mesAnoVencimentoFormatado,
    :dataRegistro, :dataVencimentoRegistro, :dataCancelamento,
    :dataAtualizacao, :medicamentoReferencia, :complemento, :empresa_cnpj
)
ON CONFLICT(codigo) DO UPDATE SET
    nome                      = excluded.nome,
    numeroRegistro            = excluded.numeroRegistro,
    numeroRegistroFormatado   = excluded.numeroRegistroFormatado,
    tipoAutorizacao           = excluded.tipoAutorizacao,
    situacaoApresentacao      = excluded.situacaoApresentacao,
    principioAtivo            = excluded.principioAtivo,
    categoriaRegulatoria      = excluded.categoriaRegulatoria,
    mesAnoVencimentoFormatado = excluded.mesAnoVencimentoFormatado,
    dataRegistro              = excluded.dataRegistro,
    dataVencimentoRegistro    = excluded.dataVencimentoRegistro,
    dataCancelamento          = excluded.dataCancelamento,
    dataAtualizacao           = excluded.dataAtualizacao,
    medicamentoReferencia     = excluded.medicamentoReferencia,
    complemento               = excluded.complemento,
    empresa_cnpj              = excluded.empresa_cnpj;
"""

_UPSERT_PROCESSO = """
INSERT INTO processo (numeroFormatado, situacao, produto_codigo)
VALUES (:numeroFormatado, :situacao, :produto_codigo)
ON CONFLICT(numeroFormatado) DO UPDATE SET
    situacao       = excluded.situacao,
    produto_codigo = excluded.produto_codigo;
"""


def save_sqlite(records: list[dict], path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(_DDL)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        for r in records:
            if r.get("empresa_cnpj"):
                conn.execute(
                    _UPSERT_EMPRESA,
                    {
                        "cnpj": r["empresa_cnpj"],
                        "razaoSocial": r["empresa_razaoSocial"],
                        "cnpjFormatado": r["empresa_cnpjFormatado"],
                        "numeroAutorizacao": r["empresa_numeroAutorizacao"],
                    },
                )
            if r.get("produto_codigo"):
                conn.execute(
                    _UPSERT_PRODUTO,
                    {
                        "codigo": r["produto_codigo"],
                        "nome": r["produto_nome"],
                        "numeroRegistro": r["produto_numeroRegistro"],
                        "numeroRegistroFormatado": r[
                            "produto_numeroRegistroFormatado"
                        ],
                        "tipoAutorizacao": r["produto_tipoAutorizacao"],
                        "situacaoApresentacao": r["produto_situacaoApresentacao"],
                        "principioAtivo": r["produto_principioAtivo"],
                        "categoriaRegulatoria": r["produto_categoriaRegulatoria"],
                        "mesAnoVencimentoFormatado": r[
                            "produto_mesAnoVencimentoFormatado"
                        ],
                        "dataRegistro": r["produto_dataRegistro"],
                        "dataVencimentoRegistro": r[
                            "produto_dataVencimentoRegistro"
                        ],
                        "dataCancelamento": r["produto_dataCancelamento"],
                        "dataAtualizacao": r["produto_dataAtualizacao"],
                        "medicamentoReferencia": r["produto_medicamentoReferencia"],
                        "complemento": r["produto_complemento"],
                        "empresa_cnpj": r["empresa_cnpj"],
                    },
                )
            if r.get("processo_numeroProcessoFormatado"):
                conn.execute(
                    _UPSERT_PROCESSO,
                    {
                        "numeroFormatado": r["processo_numeroProcessoFormatado"],
                        "situacao": r["processo_situacao"],
                        "produto_codigo": r["produto_codigo"],
                    },
                )

        conn.commit()
        n_p = conn.execute("SELECT COUNT(*) FROM produto").fetchone()[0]
        n_e = conn.execute("SELECT COUNT(*) FROM empresa").fetchone()[0]
        n_pr = conn.execute("SELECT COUNT(*) FROM processo").fetchone()[0]
        log.info(
            "SQLite salvo: %s  (%d produtos | %d empresas | %d processos)",
            path,
            n_p,
            n_e,
            n_pr,
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    log.info(
        "Iniciando crawler Anvisa — %d prefixos, PAGE_SIZE=%d",
        len(PREFIXES),
        PAGE_SIZE,
    )
    records = crawl()
    log.info("Coleta concluída. Total único: %d medicamentos", len(records))
    save_csv(records, OUTPUT_CSV)
    save_json(records, OUTPUT_JSON)
    save_sqlite(records, OUTPUT_DB)
    log.info(
        "Pronto!  %s  |  %s  |  %s  |  %s",
        OUTPUT_CSV,
        OUTPUT_JSON,
        OUTPUT_DB,
        LOG_FILE,
    )
