"""
PDF da bula do **paciente** na Anvisa (token ``parecer`` de curta duração).

O portal expõe ``idBulaPacienteProtegido`` de forma estável nas respostas do
**Bulário Eletrônico** (``/api/consulta/bulario``), não na ficha
``#/medicamentos/{id}``. Este módulo:

1. Abre a ficha (``source_url``) só para obter o **nome** do medicamento
   (``nomeProduto`` no HTML ou título), a menos que ``medication_hint`` seja
   informado;
2. Busca no Bulário com esse nome e intercepta o JSON do Bulário;
3. Escolhe a linha com ``idProduto`` + ``numProcesso`` iguais aos informados;
4. Baixa o PDF com ``fetch`` in-page (cookies), como ``AnvisaBularioClient``.
"""

from __future__ import annotations

import base64
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

try:
    from playwright.sync_api import sync_playwright
except ImportError:  # pragma: no cover
    sync_playwright = None  # type: ignore

from bula_check.bulas_anvisa import AnvisaBularioClient


class AnvisaPdfError(RuntimeError):
    pass


def _ensure_playwright() -> None:
    if sync_playwright is None:
        raise AnvisaPdfError(
            "Playwright não está instalado. Use: poetry add playwright && "
            "playwright install chromium"
        )


def _norm_process(s: str) -> str:
    return re.sub(r"[\s\-./]", "", str(s).strip())


def _normalize_guest_query(pdf_url: str) -> str:
    pdf_url = pdf_url.strip()
    if pdf_url.endswith("?Authorization="):
        return f"{pdf_url}Guest"
    if (
        "/api/consulta/medicamentos/arquivo/bula/parecer/" in pdf_url
        and "Authorization" not in pdf_url
    ):
        return pdf_url.rstrip("/") + "/?Authorization=Guest"
    return pdf_url


def _assert_source_url_matches_ids(
    source_url: str,
    product_id: int,
    numero_processo: str,
) -> None:
    if str(product_id) not in source_url:
        raise AnvisaPdfError(
            f"source_url não parece conter idProduto={product_id}: {source_url!r}"
        )
    nproc = str(numero_processo).strip()
    if nproc and nproc not in source_url.replace(" ", ""):
        raise AnvisaPdfError(
            f"source_url não parece conter numeroProcesso={numero_processo!r}"
        )


def _extract_medication_name_from_medicamento_page(page: Any) -> str | None:
    """Tenta obter nome comercial a partir do HTML/DOM da ficha."""
    html = page.content()
    for pat in (
        r'"nomeProduto"\s*:\s*"([^"\\]+)"',
        r'"nomeComercial"\s*:\s*"([^"\\]+)"',
    ):
        m = re.search(pat, html, re.I)
        if m:
            name = m.group(1).strip()
            if len(name) > 2:
                return name

    for sel in ("h1", "h2", ".nome-produto", "[class*='titulo']"):
        try:
            t = page.locator(sel).first.inner_text(timeout=2500).strip()
            t = t.split("\n")[0].strip()
            if 3 < len(t) < 280 and "anvisa" not in t.lower():
                return t
        except Exception:
            continue
    return None


def _append_bulario_content_from_response(
    response: Any, api_rows: list[dict[str, Any]]
) -> None:
    if "/api/consulta/bulario" not in response.url:
        return
    if response.status != 200:
        return
    try:
        payload = response.json()
    except Exception:
        try:
            raw = response.text()
            if not raw.strip().startswith("{"):
                return
            payload = json.loads(raw)
        except Exception:
            return
    content = payload.get("content")
    if isinstance(content, list):
        for row in content:
            if isinstance(row, dict):
                api_rows.append(row)


def _patient_token_from_bulario_rows(
    rows: list[dict[str, Any]],
    product_id: int,
    numero_processo: str,
) -> str | None:
    want_p = _norm_process(numero_processo)
    for item in rows:
        ip = item.get("idProduto")
        if ip is None:
            continue
        try:
            if int(ip) != int(product_id):
                continue
        except (TypeError, ValueError):
            continue
        np = item.get("numProcesso")
        if _norm_process(str(np or "")) != want_p:
            continue
        tok = item.get("idBulaPacienteProtegido")
        if isinstance(tok, str) and tok.strip():
            return tok.strip()
    return None


def _download_pdf_via_fetch(page: Any, pdf_url: str) -> bytes:
    pdf_url = _normalize_guest_query(pdf_url)
    result = page.evaluate(
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
            return {
                ok: true,
                b64: btoa(binary),
                contentType: r.headers.get('content-type') || '',
            };
        }""",
        pdf_url,
    )

    if not isinstance(result, dict):
        raise AnvisaPdfError("Resposta inesperada ao baixar PDF.")

    if not result.get("ok"):
        raise AnvisaPdfError(
            f"Falha HTTP {result.get('status')} ao baixar PDF da Anvisa."
        )

    data = base64.b64decode(result["b64"])
    content_type = str(result.get("contentType") or "").lower()
    if "pdf" not in content_type and not data.startswith(b"%PDF"):
        raise AnvisaPdfError("A URL não devolveu um PDF (token expirado ou bloqueio).")

    return data


def download_patient_pdf_from_anvisa(
    *,
    source_url: str,
    product_id: int,
    numero_processo: str,
    output_path: str | Path | None = None,
    timeout: float = 60.0,
    headless: bool = True,
    medication_hint: str | None = None,
    max_bulario_pages: int = 25,
) -> bytes:
    """
    Obtém bytes do PDF da bula do paciente com JWT novo.

    Parameters
    ----------
    source_url
        Ficha ``#/medicamentos/{id}?numeroProcesso=...`` (validação + nome).
    product_id, numero_processo
        Devem coincidir com uma linha do JSON do Bulário após a busca.
    medication_hint
        Nome (ou trecho) digitado no Bulário. Se omitido, tenta ler da ficha.
    max_bulario_pages
        Máximo de páginas de resultados a percorrer ao procurar o par id/processo.
    """
    _ensure_playwright()
    _assert_source_url_matches_ids(source_url, product_id, numero_processo)

    timeout_ms = max(10_000, int(timeout * 1000))
    client = AnvisaBularioClient(
        timeout=max(30, int(timeout)),
        headless=headless,
    )

    api_rows: list[dict[str, Any]] = []

    def _on_response(response: Any) -> None:
        _append_bulario_content_from_response(response, api_rows)

    pdf_bytes: bytes | None = None

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        try:
            context = browser.new_context(user_agent=client._BROWSER_USER_AGENT)
            page = context.new_page()
            page.on("response", _on_response)

            med = (medication_hint or "").strip()
            if not med:
                page.goto(
                    source_url,
                    wait_until="domcontentloaded",
                    timeout=timeout_ms,
                )
                page.wait_for_timeout(2000)
                med = _extract_medication_name_from_medicamento_page(page) or ""
            if not med:
                raise AnvisaPdfError(
                    "Não foi possível obter o nome do medicamento na ficha. "
                    "Passe ``medication_hint='NOME'`` (como no Bulário) ou confira a URL."
                )

            api_rows.clear()
            page.goto(
                client.BULARIO_URL,
                wait_until="domcontentloaded",
                timeout=timeout_ms,
            )
            try:
                page.wait_for_load_state(
                    "networkidle",
                    timeout=min(20_000, timeout_ms),
                )
            except Exception:
                pass
            page.wait_for_timeout(2000)

            client._wait_for_bulario_form(page)
            client._fill_search_form(
                page,
                medication=med,
                publication_start=None,
                publication_end=None,
            )
            client._trigger_search(page)
            client._wait_for_results(page)

            token: str | None = None
            for _ in range(max(1, max_bulario_pages)):
                token = _patient_token_from_bulario_rows(
                    api_rows, product_id, numero_processo
                )
                if token:
                    break
                nxt = client._find_next_page_locator(page)
                if nxt is None:
                    break
                try:
                    if not nxt.is_enabled():
                        break
                except Exception:
                    pass
                before = len(api_rows)
                nxt.click()
                page.wait_for_timeout(2500)
                if len(api_rows) == before:
                    break

            if not token:
                raise AnvisaPdfError(
                    "Nenhuma linha do Bulário correspondeu a "
                    f"idProduto={product_id} e numProcesso={numero_processo!r} "
                    f"(busca por {med!r}). Ajuste ``medication_hint`` ou ``max_bulario_pages``."
                )

            pdf_url = client._bula_parecer_pdf_url(token)
            pdf_bytes = _download_pdf_via_fetch(page, pdf_url)
        finally:
            browser.close()

    assert pdf_bytes is not None
    if output_path is not None:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(pdf_bytes)

    return pdf_bytes


def build_medicamento_source_url(product_id: int, numero_processo: str) -> str:
    """Monta a URL da ficha ``#/medicamentos/{id}?numeroProcesso=...``."""
    nproc = re.sub(r"\s+", "", str(numero_processo).strip())
    frag = f"/medicamentos/{int(product_id)}?numeroProcesso={nproc}"
    return urljoin(f"{AnvisaBularioClient.BASE_URL}/", "#" + frag)
