# Validação por LLM-as-judge

- Painel: `openai:gpt-4o, google:gemini-1.5-pro, groq:llama-3.3-70b-versatile`
- Perguntas: 3
- Status geral: {'reprovado': 1, 'aprovado': 2}
- Flags p/ revisão humana: 2  |  erros: 3

## Concordância inter-juízes
- judge_verdict: acordo par-a-par 0.0 | Fleiss κ 0.0
- medicine_ok: acordo par-a-par 0.0 | Fleiss κ 0.0
- evidence_ok: acordo par-a-par 0.0 | Fleiss κ 0.0

## Por dimensão (voto majoritário)
- verdict_ok: {'não': 1, 'sim': 2}
- medicine_ok: {'sim': 2, 'incerto': 1}
- evidence_ok: {'sim': 3}

## Itens sinalizados (fila do farmacêutico)
- **q138** (fidelidade, gold=inconclusive) verdict_ok=não — o isoforine serve pra tratar enxaqueca?
  - openai:gpt-4o: Isoforine é indicado para anestesia geral, não para enxaqueca. | groq:llama-3.3-70b-versatile: Trecho de Indicações não menciona enxaqueca como indicação. [vereditos: refuted, inconclusive, refuted]
- **q447** (giria, gold=confirmed) medicine_ok=incerto — Passei da dose sem perceber e agora estou com o estômago embrulhado e uma canseira fora do comum, isso combina com o que pode acontecer quando se exagera nesse remédio?
  - openai:gpt-4o: O trecho menciona sintomas de superdose como letargia, náusea e dor no estômago. | groq:llama-3.3-70b-versatile: Sintomas como letargia, sonolência, náusea, vômito e dor no estômago estão descritos na seção de Superdosagem. [vereditos: confirmed, inconclusive, confirmed]
