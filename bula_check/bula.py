from collections import defaultdict
import re
from typing import ClassVar
from typing import Optional

from langchain_core.documents import Document
from nemo.preprocessing.text import normalize_text_whitespace
from nemo.preprocessing.text import remove_text_accents
from nemo.preprocessing.text import remove_text_punctuation
from nemo.preprocessing.text import uppercase_text
from nemo.protocol import _BaseModel
from toolz.functoolz import pipe


class Sections(_BaseModel):
    indications: Optional[str] = None
    how_it_works: Optional[str] = None
    contraindications: Optional[str] = None
    warnings_and_precautions: Optional[str] = None
    storage: Optional[str] = None
    dosage_and_administration: Optional[str] = None
    missed_dose: Optional[str] = None
    adverse_reactions: Optional[str] = None
    overdose: Optional[str] = None

    _RAW_SECTION_PATTERNS: ClassVar[dict[str, list[str]]] = {
        # 1. PARA QUE ESTE MEDICAMENTO É INDICADO?
        "indications": [
            "para que este medicamento e indicado",
            "PARA QUE ESTE MEDICAMENTO E INDICADO",
        ],
        # 2. COMO ESTE MEDICAMENTO FUNCIONA?
        "how_it_works": [
            "como este medicamento funciona",
            "COMO ESTE MEDICAMENTO FUNCIONA",
        ],
        # 3. QUANDO NÃO DEVO USAR ESTE MEDICAMENTO?
        "contraindications": [
            "quando nao devo usar este medicamento",
            "quem nao deve usar este medicamento",
            "QUANDO NAO DEVO USAR ESTE MEDICAMENTO",
        ],
        # 4. O QUE DEVO SABER ANTES DE USAR ESTE MEDICAMENTO?
        "warnings_and_precautions": [
            "o que devo saber antes de usar este medicamento",
            "precaucoes",
            "O QUE DEVO SABER ANTES DE USAR ESTE MEDICAMENTO",
        ],
        # 5. ONDE, COMO E POR QUANTO TEMPO POSSO GUARDAR ESTE MEDICAMENTO?
        "storage": [
            "onde como e por quanto tempo posso guardar este medicamento",
            "ONDE, COMO E POR QUANTO TEMPO POSSO GUARDAR ESTE MEDICAMENTO",
        ],
        # 6. COMO DEVO USAR ESTE MEDICAMENTO?
        "dosage_and_administration": [
            "como devo usar este medicamento",
            "COMO DEVO USAR ESTE MEDICAMENTO",
        ],
        # 7. O QUE DEVO FAZER QUANDO EU ME ESQUECER DE USAR ESTE MEDICAMENTO?
        "missed_dose": [
            "o que devo fazer quando eu me esquecer de usar este medicamento",
            "O QUE DEVO FAZER QUANDO EU ME ESQUECER DE USAR ESTE MEDICAMENTO",
        ],
        # 8. QUAIS OS MALES QUE ESTE MEDICAMENTO PODE ME CAUSAR?
        "adverse_reactions": [
            "quais os males que este medicamento pode me causar",
            "QUAIS OS MALES QUE ESTE MEDICAMENTO PODE ME CAUSAR",
        ],
        # 9. O QUE FAZER SE ALGUÉM USAR UMA QUANTIDADE MAIOR DO QUE A INDICADA DESTE MEDICAMENTO?
        "overdose": [
            "o que fazer se alguem usar uma quantidade maior do que a indicada deste medicamento",
            "O QUE FAZER SE ALGUEM USAR UMA QUANTIDADE MAIOR DO QUE A INDICADA DESTE MEDICAMENTO",
        ],
    }

    @classmethod
    def empty(cls) -> "Sections":
        return cls(**{name: None for name in cls.model_fields})

    @classmethod
    def section_names(cls) -> list[str]:
        return list(cls.model_fields)

    @classmethod
    def section_patterns(cls) -> dict[str, list[str]]:
        missing = set(cls.model_fields) - set(cls._RAW_SECTION_PATTERNS)
        extra = set(cls._RAW_SECTION_PATTERNS) - set(cls.model_fields)

        if missing or extra:
            raise ValueError(
                f"Sections mismatch. Missing: {missing or None}, Extra: {extra or None}"
            )

        return {name: cls._RAW_SECTION_PATTERNS[name] for name in cls.model_fields}


def gen_sections_from_pdf(documents: list[Document]) -> Sections:
    sections = Sections.empty()

    normalized_patterns = {
        section_name: [_normalize_text(pattern) for pattern in patterns]
        for section_name, patterns in Sections.section_patterns().items()
    }

    gen_dictionary_from_pdf(documents)

    for document in documents:
        extracted_sections = _gen_sections_from_text(document.page_content)
        if not extracted_sections:
            continue

        normalized_extracted_sections = {
            _normalize_text(title): content
            for title, content in extracted_sections.items()
        }

        for section_name, patterns in normalized_patterns.items():
            if sections[section_name] is not None:
                continue

            for (
                extracted_title,
                extracted_content,
            ) in normalized_extracted_sections.items():
                if any(pattern in extracted_title for pattern in patterns):
                    sections[section_name] = (
                        _normalize_text(extracted_content) or None
                    )
                    break

    return sections


def _normalize_text(text: str) -> str:
    return pipe(
        uppercase_text(text),
        remove_text_accents,
        remove_text_punctuation,
        normalize_text_whitespace,
    )


UPPERCASE_HEADER_RE = re.compile(r"(?m)^[A-ZÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ0-9®/\-\s]+$")


def _split_paragraphs(text: str) -> list[str]:
    # procura por final de frase . ! ou ?
    # depois uma quebra de linha
    # depois uma letra maiúscula
    # transforma isso em separação de parágrafo com \n\n
    text = re.sub(r"(?<=[.!?])\s*\n\s*(?=[A-ZÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ])", "\n\n", text)
    paragraphs = re.split(r"\n\s*\n+", text)
    return [p.strip() for p in paragraphs if p.strip()]


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

    if not UPPERCASE_HEADER_RE.fullmatch(line):
        return False

    if _is_probably_noise_header(line):
        return False

    norm = _normalize_text(line)
    if any(char.isdigit() for char in norm):
        return False

    return True


def _extract_numbered_headers(text: str) -> list[tuple[int, int, int, str]]:
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
    """
    Keeps the first continuous numbered block: 1, 2, 3, ..., 9.
    Stops when numbering breaks.
    """
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


def _gen_sections_from_text(text: str) -> dict[str, list[str]]:
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

    pre_numbered_text = text[:first_numbered_start]

    for line_match in re.finditer(r"(?m)^.*$", pre_numbered_text):
        line = line_match.group(0).strip()
        norm = _normalize_text(line)

        if norm in non_numbered_whitelist and _is_uppercase_header(line):
            matches.append((line_match.start(), line_match.end(), line))

    # opcional: pegar DIZERES LEGAIS logo após a seção 9
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

    searchable_end = len(text)
    for i, (_, _, title) in enumerate(matches):
        if _normalize_text(title) == "DIZERES LEGAIS":
            searchable_end = len(text)
            break

    sections: dict[str, list[str]] = {}

    for i, (_, end, title) in enumerate(matches):
        next_start = matches[i + 1][0] if i + 1 < len(matches) else searchable_end
        content = text[end:next_start].strip()

        normalized_title = _normalize_text(title)
        if not normalized_title:
            continue

        sections[normalized_title] = _split_paragraphs(content)

    return sections


def gen_dictionary_from_pdf(documents: list[Document]) -> dict[str, list[str]]:
    full_text = "\n".join(
        document.page_content
        for document in documents
        if document.page_content and document.page_content.strip()
    )

    return _gen_sections_from_text(full_text)
