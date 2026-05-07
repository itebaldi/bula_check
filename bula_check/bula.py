"""
bula.py
-------
Parser de bulas em PDF (fonte ANVISA).

Recebe uma lista de `langchain_core.documents.Document` (páginas do PDF já
extraídas) e produz:

  - dict[str, list[str]]   — seções brutas (título → parágrafos)
  - dict[str, str | None]  — seções mapeadas para os nomes canônicos
  - list[Chunks]           — fragmentos prontos para embedding

Uso típico:
    from langchain_community.document_loaders import PyPDFLoader
    docs = PyPDFLoader("bula_paciente.pdf").load()

    sections = gen_sections_from_pdf(docs)
    chunks   = gen_chunks_from_pdf(medicine_id, medicine_name, docs)
"""

from __future__ import annotations

import re
import uuid

from langchain_core.documents import Document

from bula_check.protocol import SECTION_PATTERNS
from bula_check.protocol import Chunks
from bula_check.protocol import Section
from bula_check.protocol import normalize_for_matching


# ---------------------------------------------------------------------------
# Helpers de normalização (locais — sem dependência de nemo aqui)
# ---------------------------------------------------------------------------
def _normalize_text(text: str) -> str:
    """Uppercase + remove acentos + remove pontuação + colapsa espaços."""
    import unicodedata

    text = text.upper()
    nfd = unicodedata.normalize("NFD", text)
    text = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ---------------------------------------------------------------------------
# Splitting
# ---------------------------------------------------------------------------
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")
_UPPERCASE_HEADER_RE = re.compile(r"(?m)^[A-ZÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ0-9®/\-\s]+$")


def _split_paragraphs(text: str) -> list[str]:
    """Divide um bloco de texto em parágrafos."""
    text = re.sub(r"(?<=[.!?])\s*\n\s*(?=[A-ZÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ])", "\n\n", text)
    paragraphs = re.split(r"\n\s*\n+", text)
    return [p.strip() for p in paragraphs if p.strip()]


def _split_sentences(paragraph: str) -> list[str]:
    """Divide um parágrafo em sentenças."""
    sentences = _SENTENCE_SPLIT.split(paragraph)
    return [s.strip() for s in sentences if s.strip()]


# ---------------------------------------------------------------------------
# Detecção de cabeçalhos
# ---------------------------------------------------------------------------
def _is_probably_noise_header(line: str) -> bool:
    norm = _normalize_text(line)
    if not norm:
        return True
    if re.fullmatch(r"[\d\s]+", norm):
        return True
    if len(norm) < 4:
        return True
    blocked = {
        "VPS",
        "VPVPS",
        "RESPONSAVEL TECNICO",
        "RAZAO SOCIAL",
        "INDUSTRIA BRASILEIRA",
        "MARCA REGISTRADA",
        "VENDA SOB PRESCRICAO",
    }
    return norm in blocked


def _is_uppercase_header(line: str) -> bool:
    line = line.strip()
    if not line:
        return False
    if "?" in line:
        return False
    if not _UPPERCASE_HEADER_RE.fullmatch(line):
        return False
    if _is_probably_noise_header(line):
        return False
    norm = _normalize_text(line)
    if any(char.isdigit() for char in norm):
        return False
    return True


# ---------------------------------------------------------------------------
# Extração de cabeçalhos numerados (1. ... ? até 9. ... ?)
# ---------------------------------------------------------------------------
def _extract_numbered_headers(text: str) -> list[tuple[int, int, int, str]]:
    """Retorna lista de (numero, start, end, title) para cada cabeçalho numerado."""
    lines = text.splitlines(keepends=True)
    matches: list[tuple[int, int, int, str]] = []

    offset = 0
    i = 0

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        number_match = re.match(r"^(\d+)\.\s*(.*)$", stripped)
        if not number_match:
            offset += len(line)
            i += 1
            continue

        section_number = int(number_match.group(1))
        start = offset
        header_parts: list[str] = []

        rest_of_line = number_match.group(2).strip()
        if rest_of_line:
            header_parts.append(rest_of_line)

        end = offset + len(line)
        j = i + 1

        while "?" not in " ".join(header_parts) and j < len(lines):
            next_line = lines[j].strip()
            if next_line:
                if re.match(r"^\d+\.\s*", next_line):
                    break
                header_parts.append(next_line)
            end += len(lines[j])
            j += 1

        full_header = " ".join(header_parts).strip()

        if "?" in full_header:
            title = full_header.split("?", 1)[0].strip()
            matches.append((section_number, start, end, title))
            offset = end
            i = j
        else:
            offset += len(line)
            i += 1

    return matches


def _keep_first_sequential_block(
    headers: list[tuple[int, int, int, str]],
) -> list[tuple[int, int, str]]:
    """Mantém apenas o primeiro bloco contínuo 1, 2, 3, ..., 9."""
    if not headers:
        return []

    headers = sorted(headers, key=lambda x: x[1])
    kept: list[tuple[int, int, str]] = []
    expected = 1
    started = False

    for number, start, end, title in headers:
        if not started:
            if number != 1:
                continue
            started = True
        if number == expected:
            kept.append((start, end, title))
            expected += 1
        elif started:
            break

    return kept


# ---------------------------------------------------------------------------
# Extração de seções brutas de um texto
# ---------------------------------------------------------------------------
def _gen_sections_from_text(text: str) -> dict[str, list[str]]:
    """
    Retorna dict {titulo_normalizado: [paragrafos]} para um bloco de texto.
    """
    numbered_headers = _extract_numbered_headers(text)
    numbered_matches = _keep_first_sequential_block(numbered_headers)

    if not numbered_matches:
        return {}

    first_numbered_start = numbered_matches[0][0]
    last_numbered_end = numbered_matches[-1][1]

    matches: list[tuple[int, int, str]] = list(numbered_matches)

    non_numbered_whitelist = {
        "APRESENTACOES",
        "USO ORAL",
        "COMPOSICAO",
        "DIZERES LEGAIS",
    }

    # cabeçalhos não-numerados antes da seção 1
    pre_numbered_text = text[:first_numbered_start]
    for line_match in re.finditer(r"(?m)^.*$", pre_numbered_text):
        line = line_match.group(0).strip()
        norm = _normalize_text(line)
        if norm in non_numbered_whitelist and _is_uppercase_header(line):
            matches.append((line_match.start(), line_match.end(), line))

    # DIZERES LEGAIS após a seção 9
    post_numbered_text = text[last_numbered_end:]
    for line_match in re.finditer(r"(?m)^.*$", post_numbered_text):
        line = line_match.group(0).strip()
        norm = _normalize_text(line)
        if norm == "DIZERES LEGAIS" and _is_uppercase_header(line):
            start = last_numbered_end + line_match.start()
            end = last_numbered_end + line_match.end()
            matches.append((start, end, line))
            break

    matches.sort(key=lambda item: item[0])

    sections: dict[str, list[str]] = {}
    for i, (_, end, title) in enumerate(matches):
        next_start = matches[i + 1][0] if i + 1 < len(matches) else len(text)
        content = text[end:next_start].strip()
        normalized_title = _normalize_text(title)
        if not normalized_title:
            continue
        sections[normalized_title] = _split_paragraphs(content)

    return sections


# ---------------------------------------------------------------------------
# API pública: seções canônicas
# ---------------------------------------------------------------------------
def gen_sections_from_pdf(documents: list[Document]) -> dict[str, str | None]:
    """
    Processa uma lista de Documents (páginas de PDF) e retorna as seções
    canônicas mapeadas para os nomes de `Section`.

    Returns
    -------
    dict[str, str | None]
        Chaves = Section.value (ex: "indications"), valores = texto da seção
        ou None se não encontrada.
    """
    result: dict[str, str | None] = {s.value: None for s in Section}

    # padrões já normalizados vindos de protocol.py
    normalized_patterns = SECTION_PATTERNS

    for document in documents:
        raw_sections = _gen_sections_from_text(document.page_content)
        if not raw_sections:
            continue

        # normaliza as chaves extraídas para matching
        normalized_extracted = {
            normalize_for_matching(title): paragraphs
            for title, paragraphs in raw_sections.items()
        }

        for section_name, patterns in normalized_patterns.items():
            if result[section_name] is not None:
                continue
            for extracted_title, paragraphs in normalized_extracted.items():
                if any(pattern in extracted_title for pattern in patterns):
                    joined = " ".join(paragraphs).strip()
                    result[section_name] = joined or None
                    break

    return result


# ---------------------------------------------------------------------------
# API pública: dicionário bruto
# ---------------------------------------------------------------------------
def gen_dictionary_from_pdf(documents: list[Document]) -> dict[str, list[str]]:
    """
    Concatena todas as páginas e retorna as seções brutas.

    Returns
    -------
    dict[str, list[str]]
        Chaves = título normalizado, valores = lista de parágrafos.
    """
    full_text = "\n".join(
        doc.page_content
        for doc in documents
        if doc.page_content and doc.page_content.strip()
    )
    return _gen_sections_from_text(full_text)


# ---------------------------------------------------------------------------
# API pública: chunks
# ---------------------------------------------------------------------------
def gen_chunks_from_pdf(
    medicine_id: str,
    medicine_name: str,
    documents: list[Document],
) -> list[Chunks]:
    """
    Processa os Documents de uma bula e retorna os Chunks prontos para
    embedding (embedding preenchido com zeros — use _embed_chunks do
    bula_gratis_crawler para preencher via OpenAI, ou equivalente).

    Estratégia:
      página por página → extrai seções canônicas → divide em parágrafos
      → divide em sentenças → gera um Chunk por sentença.

    Parameters
    ----------
    medicine_id : str
        UUID do medicamento (deve existir na tabela medicines).
    medicine_name : str
        Nome do medicamento (para o campo medicine_name do chunk).
    documents : list[Document]
        Páginas do PDF carregadas pelo loader do LangChain.

    Returns
    -------
    list[Chunks]
    """
    from bula_check.protocol import (
        OPENAI_EMBEDDING_DIM,
    )  # importado aqui para evitar circular

    dummy_embedding: list[float] = [0.0] * OPENAI_EMBEDDING_DIM

    # coleta seções de todas as páginas (mantém contexto multi-página)
    sections = gen_sections_from_pdf(documents)

    chunks: list[Chunks] = []

    for section_name, text in sections.items():
        if not text:
            continue

        try:
            section_enum = Section(section_name)
        except ValueError:
            continue

        for para_idx, paragraph in enumerate(_split_paragraphs(text)):
            for chunk_idx, sentence in enumerate(_split_sentences(paragraph)):
                chunks.append(
                    Chunks(
                        id=str(uuid.uuid4()),
                        medicine_id=medicine_id,
                        medicine_name=medicine_name,
                        section=section_enum,
                        paragraph_idx=para_idx,
                        chunk_idx=chunk_idx,
                        text=sentence,
                        embedding=dummy_embedding,
                    )
                )

    return chunks
