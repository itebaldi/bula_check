from typing import Literal

LANGUAGES = Literal[
    "arabic",
    "danish",
    "dutch",
    "english",
    "finnish",
    "french",
    "german",
    "hungarian",
    "italian",
    "norwegian",
    "porter",
    "portuguese",
    "romanian",
    "russian",
    "spanish",
    "swedish",
]
SECTION_PATTERNS: dict[str, list[str]] = {
    "indications": [
        "para que este medicamento e indicado",
        "indications",
    ],
    "contraindications": [
        "quando nao devo usar este medicamento",
        "contraindications",
        "quem nao deve usar este medicamento",
    ],
    "warnings_and_precautions": [
        "o que devo saber antes de usar este medicamento",
        "warnings_and_precautions",
        "precaucoes",
    ],
    "adverse_reactions": [
        "quais os males que este medicamento pode me causar",
        "reacoes adversas",
        "efeitos adversos",
    ],
}
