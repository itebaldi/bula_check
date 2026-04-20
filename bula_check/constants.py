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
    # 1. PARA QUE ESTE MEDICAMENTO É INDICADO?
    "indications": [
        "para que este medicamento e indicado",
        "indications",
        "PARA QUE ESTE MEDICAMENTO É INDICADO",
    ],
    # 3. QUANDO NÃO DEVO USAR ESTE MEDICAMENTO?
    "contraindications": [
        "quando nao devo usar este medicamento",
        "contraindications",
        "quem nao deve usar este medicamento",
        "QUANDO NÃO DEVO USAR ESTE MEDICAMENTO",
    ],
    # 4. O QUE DEVO SABER ANTES DE USAR ESTE MEDICAMENTO?
    "warnings_and_precautions": [
        "o que devo saber antes de usar este medicamento",
        "warnings_and_precautions",
        "precaucoes",
        "O QUE DEVO SABER ANTES DE USAR ESTE MEDICAMENTO",
    ],
    # 8. QUAIS OS MALES QUE ESTE MEDICAMENTO PODE ME CAUSAR?
    "adverse_reactions": [
        "quais os males que este medicamento pode me causar",
        "reacoes adversas",
        "efeitos adversos",
        "QUAIS OS MALES QUE ESTE MEDICAMENTO PODE ME CAUSAR",
    ],
    # 2. COMO ESTE MEDICAMENTO FUNCIONA?
    "s": [
        "COMO ESTE MEDICAMENTO FUNCIONA",
    ],
}

# 5. ONDE, COMO E POR QUANTO TEMPO POSSO GUARDAR ESTE MEDICAMENTO?
# 6. COMO DEVO USAR ESTE MEDICAMENTO?
# 7. O QUE DEVO FAZER QUANDO EU ME ESQUECER DE USAR ESTE MEDICAMENTO?
# 9. O QUE FAZER SE ALGUÉM USAR UMA QUANTIDADE MAIOR DO QUE A INDICADA DESTE MEDICAMENTO?
