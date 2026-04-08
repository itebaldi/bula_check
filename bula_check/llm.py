import json
from enum import Enum
from typing import Any

import requests
from pydantic import BaseModel


class Verdict(Enum):
    Supported = "supported"
    Refuted = "refuted"
    Insufficient = "insufficient"


class ClaimType(Enum):
    AdverseEffect = "adverse_effect"
    Contraindication = "contraindication"
    Interaction = "interaction"
    Indication = "indication"
    Dosage = "dosage"
    Other = "other"


class PrecheckResult(BaseModel):
    drug_name: str
    active_ingredient: str
    claim_type: ClaimType
    claimed_effect: str
    target: str
    verdict: Verdict
    confidence: float | int
    needs_evidence: bool
    justification: str
    summary: str


class ValidateResult(BaseModel):
    verdict: Verdict
    justification: str
    evidence_used: list[str]


def precheck_llm(claim: str) -> PrecheckResult:
    return PrecheckResult(**_connect_to_llm(gen_precheck_prompt(claim)))


def validate_llm(claim: str, drug_name: str, text: str) -> ValidateResult:
    return ValidateResult(
        **_connect_to_llm(gen_validation_prompt(claim, drug_name, text))
    )


def _connect_to_llm(payload: Any, timeout: int = 120):
    # ollama run llama3.1:8b
    # ollama stop llama3.1:8b
    url = "http://localhost:11434/api/generate"

    response = requests.post(url, json=payload, timeout=timeout)
    response.raise_for_status()

    response_str = response.json().get("response", "{}")

    return json.loads(response_str)


def gen_precheck_prompt(claim: str) -> dict[str, Any]:

    return {
        "model": "llama3.1:8b",
        "prompt": f"""
Você é um avaliador preliminar de alegações médicas em português sobre medicamentos.

Analise a alegação e responda APENAS com JSON válido.

Campos obrigatórios:
- drug_name: nome do medicamento mencionado
- active_ingredient: princípio ativo, se souber com segurança; caso contrário, string vazia
- claim_type: um entre adverse_effect, contraindication, interaction, indication, dosage ou other
- claimed_effect: o efeito alegado
- target: alvo clínico, anatômico ou fisiológico da alegação; se não houver, string vazia
- verdict: um entre supported, refuted, insufficient
- confidence: número entre 0 e 1
- needs_evidence: true ou false
- justification: apresentar uma justificativa
- summary: frase curta em português, natural e clara, voltada ao usuário final. Deve 
resumir o medicamento identificado e o veredito preliminar. Não deve inventar informações nem repetir o JSON literalmente.
Se tiver confiança ou não precisar de evidência, notificar o usuário.

Regras:
- Se houver incerteza, ambiguidade ou risco de erro, use "insufficient".
- Não invente fontes.
- Não trate conhecimento incerto como fato.
- Se não souber o princípio ativo com segurança, deixe "active_ingredient" como "".
- Para efeitos adversos, interações, contraindicações e posologia, seja conservador.
- Para alegações sobre medicamentos, prefira "needs_evidence": true, exceto se a afirmação for extremamente óbvia.
- Não escreva nada fora do JSON.

Alegação:
{claim}

Formato:
{{
  "drug_name": "",
  "active_ingredient": "",
  "claim_type": "",
  "claimed_effect": "",
  "target": "",
  "verdict": "",
  "confidence": 0.0,
  "needs_evidence": true,
  "summary": "",
  "justification": ""
}}
""",
        "stream": False,
    }


def gen_validation_prompt(claim: str, drug_name: str, text: str) -> dict[str, Any]:

    return {
        "model": "llama3.1:8b",
        "prompt": f"""
Você é um verificador de alegações sobre medicamentos.

Você receberá:
- uma alegação
- o nome do medicamento
- trechos da bula

Sua tarefa é avaliar se os trechos:
- sustentam a alegação
- refutam a alegação
- ou não são suficientes para decidir

Analise a alegação e responda APENAS com JSON válido.

Campos obrigatórios:
- verdict: um entre supported, refuted, insufficient
- justification: justificativa, baseada apenas nos trechos fornecidos
- evidence_used: lista com os trechos ou resumos dos trechos usados na decisão. Lista
de strings APENAS

Regras:
- Baseie-se nos trechos fornecidos.
- Se os trechos não forem suficientes, use "insufficient".
- Não extrapole além da evidência.
- Se houver incerteza, ambiguidade ou risco de erro, use "insufficient".
- Não invente fontes.
- Não trate conhecimento incerto como fato.
- Não escreva nada fora do JSON.

Alegação:
{claim}

Medicamento:
{drug_name}

Trechos da bula:
{text}

Formato:
{{
  "verdict": "",
  "justification": "",
  "evidence_used": []
}}
""",
        "stream": False,
    }
