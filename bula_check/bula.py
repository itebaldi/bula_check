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


def _gen_sections_from_text(text: str) -> dict[str, str]:
    pattern = re.compile(r"(?m)^\s*(\d+)\.\s+(.+?)\?\s*$")

    matches = list(pattern.finditer(text))

    sections = {}

    for i, match in enumerate(matches):
        title = match.group(2).strip()
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        content = text[start:end].strip()
        sections[_normalize_text(title)] = content

    return sections
