# Validação por LLM-as-judge

- Painel: `openai:gpt-4o, openai:gpt-4.1, openai:gpt-4.1-mini`
- Perguntas: 50
- Status geral: {'aprovado': 45, 'reprovado': 5}
- Flags p/ revisão humana: 5  |  erros: 0

## Concordância inter-juízes
- judge_verdict: acordo par-a-par 0.9333 | Fleiss κ 0.886
- medicine_ok: acordo par-a-par 1.0 | Fleiss κ 1.0
- evidence_ok: acordo par-a-par 1.0 | Fleiss κ 1.0

## Por dimensão (voto majoritário)
- verdict_ok: {'sim': 45, 'não': 5}
- medicine_ok: {'sim': 50}
- evidence_ok: {'sim': 50}

## Itens sinalizados (fila do farmacêutico)
- **q381** (negacao, gold=refuted) verdict_ok=não — Já tive uma reação alérgica grave a essa vacina em uma dose anterior. Ainda assim posso tomar a próxima dose de Comirnaty?
  - openai:gpt-4o: Comirnaty não deve ser administrada em pessoas que tiveram reação alérgica aos componentes. | openai:gpt-4.1: O trecho afirma que Comirnaty não deve ser administrada a quem já teve reação alérgica à vacina. | openai:gpt-4.1-mini: Comirnaty® não deve ser administrada em pessoas que tenham apresentado reação alérgica (hipersensibilidade) aos componentes dessa vacina. [vereditos: confirmed, confirmed, refuted]
- **q587** (fidelidade, gold=inconclusive) verdict_ok=não — Queda de cabelo está entre as reações adversas listadas do bissulfato de clopidogrel?
  - openai:gpt-4o: A seção citada lista reações adversas, mas não menciona queda de cabelo. | openai:gpt-4.1: Nenhum dos trechos listados de reações adversas menciona queda de cabelo (alopecia) como reação ao bissulfato de clopidogrel. | openai:gpt-4.1-mini: Na seção 'Reações Adversas | f53c9238-fd47-4480-904f-435660fa4d2e' não há menção à queda de cabelo entre as reações adversas listadas para o bissulfato de clopidogrel.
- **q062** (fidelidade, gold=inconclusive) verdict_ok=não — É verdade que a mupirocina ajuda o cabelo a crescer em quem tem calvície?
  - openai:gpt-4o: O trecho afirma que a mupirocina é um antibiótico para infecções de pele, sem menção a crescimento capilar. | openai:gpt-4.1: O texto afirma que a mupirocina é um antibiótico tópico para infecções de pele, sem menção a crescimento capilar ou calvície. | openai:gpt-4.1-mini: O texto afirma que a mupirocina é um antibiótico tópico para infecções de pele, sem mencionar qualquer efeito sobre crescimento capilar.
- **q049** (fidelidade, gold=inconclusive) verdict_ok=não — o mektovi serve pra baixar o colesterol?
  - openai:gpt-4o: O Mektovi é um medicamento anticancerígeno, não indicado para baixar o colesterol. | openai:gpt-4.1: O texto afirma que Mektovi é indicado para melanoma, não para baixar colesterol. | openai:gpt-4.1-mini: O Mektovi® é um medicamento anticancerígeno usado para tratar melanoma, não para baixar o colesterol.
- **q056** (fidelidade, gold=inconclusive) verdict_ok=não — o tynna não serve também pra baixar o colesterol?
  - openai:gpt-4o: Tynna é um anti-histamínico, não indicado para baixar o colesterol. | openai:gpt-4.1: O texto afirma que Tynna é um anti-histamínico, sem menção a efeito sobre colesterol. | openai:gpt-4.1-mini: O texto afirma que Tynna contém bilastina, um anti-histamínico, sem mencionar qualquer efeito para baixar o colesterol.
