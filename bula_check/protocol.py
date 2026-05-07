from enum import Enum
from typing import Literal

from pydantic import BaseModel


class Section(Enum):
    indications = "indications"
    how_it_works = "how_it_works"
    contraindications = "contraindications"
    warnings_and_precautions = "warnings_and_precautions"
    storage = "storage"
    dosage_and_administration = "dosage_and_administration"
    missed_dose = "missed_dose"
    adverse_reactions = "adverse_reactions"
    overdose = "overdose"


class Medicines(BaseModel):
    """Represents a medicine record.

    Attributes
    ----------
    id : str
        Unique identifier for the medicine. uuid
    name : str
        The original name of the medicine.
    processed_name : str
        The normalized name of the medicine.
    active_ingredient : list[str] | None
        List of active ingredients.
    processed_active_ingredient : list[str] | None
        List of normalized active ingredients.
    source : Literal["anvisa", "bula_gratis"]
        The source of the medicine information.
    url : str
        The source URL of the medicine information.
    registration_number : int
        The registration number of the medicine.
    therapeutic_classes : list[str] | None
        List of therapeutic classes.
    company_name : str
        The name of the company.
    processed_company_name : str
        The normalized name of the company.
    cnpj : str | None
        The company's CNPJ.
    extras : str
        Additional information about the medicine.
    """

    id: str
    name: str
    processed_name: str
    active_ingredient: list[str] | None = None
    processed_active_ingredient: list[str] | None = None
    source: Literal["anvisa", "bula_gratis"]
    url: str  # source_url
    registration_number: int
    therapeutic_classes: list[str] | None = None
    company_name: str
    processed_company_name: str
    cnpj: str | None
    extras: str


class Chunks(BaseModel):
    """Represents a text chunk from a medicine's bula.

    Attributes
    ----------
    id : str
        Unique identifier for the chunk.
    medicine_id : str
        unique Identifier of the medicine this chunk belongs to.
    medicine_name : str
        name of the medicine this chunk belongs to.
    section : Section
        The section of the bula where the chunk is from.
    paragraph_idx : int
        The index of the paragraph within the section.
    chunk_idx : int
        The index of the chunk within the paragraph.
    text : str
        The text content of the chunk.
    embedding : list[float]
        The embedding vector for the chunk's text.
    """

    id: str
    medicine_id: str
    medicine_name: str
    section: Section
    paragraph_idx: int
    chunk_idx: int
    text: str
    embedding: list[float]


RAW_SECTION_PATTERNS: dict[str, list[str]] = {
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
