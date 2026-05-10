# 💊 BulaCheck

Verificador de veracidade de alegações e perguntas sobre medicamentos, com base em bulas oficiais.

## Visão geral da arquitetura

```
Usuário
  │
  ▼
[Chatbot Gradio]  ←──────────────────────────────────────────────────────────┐
  │                                                                           │
  ▼                                                                           │
[LangGraph Pipeline]                                                          │
  │                                                                           │
  ├─ Node 1: parse_query                                                      │
  │    └─ LLM extrai: nome do medicamento, seções relevantes, keywords        │
  │                                                                           │
  ├─ Node 2: expand_decs                                                      │
  │    └─ DeCS expande keywords com sinônimos médicos                         │
  │       ("náusea" → "náusea, vômito, enjoo, êmese")                         │
  │                                                                           │
  ├─ Node 3: find_medicine                                                    │
  │    ├─ Busca lexical no banco BulaGratis (nome / processed_name)           │
  │    ├─ [fallback] Busca ANVISA → extrai princípio ativo → re-busca         │
  │    └─ [fallback] Sugere medicamentos similares → aguarda confirmação       │
  │                                                                           │
  ├─ Node 4: fetch_chunks                                                     │
  │    ├─ Busca lexical: LIKE nos campos de texto                             │
  │    ├─ Busca semântica: cosine similarity nos embeddings                   │
  │    └─ Score híbrido: lexical × 0.4 + semântico × 0.6                     │
  │                                                                           │
  ├─ Node 5: verify_claim                                                     │
  │    └─ LLM analisa chunks + query → Veredicto + citações da bula           │
  │                                                                           │
  └─ Resposta → Chatbot ───────────────────────────────────────────────────────
```

## Estrutura do projeto

```
bulacheck/
├── config.py              # BulaCheckConfig, BulaCheckState, modelos Pydantic
├── llm_factory.py         # Fábrica LLM: OpenAI / Anthropic / Ollama
├── pyproject.toml         # Dependências e metadados do pacote
│
├── tools/
│   └── query_tools.py     # 4 LangChain tools:
│                          #   parse_medicine_query  — LLM extrai structured data
│                          #   expand_keywords_decs  — expansão via DeCS
│                          #   get_query_embedding   — OpenAI text-embedding-3-small
│                          #   lookup_obm_presentation — nome comercial → ANVISA
│
├── retrieval/
│   └── search.py          # Busca híbrida (lexical + semântica) nos bancos SQLite
│
├── graph/
│   ├── nodes.py           # Funções de nó do LangGraph (1 função = 1 nó)
│   └── pipeline.py        # build_graph() — monta e compila o StateGraph
│
└── chatbot/
    └── app.py             # Interface Gradio + CLI (--provider, --model, --port)
```

```
Topologia:
                     ┌──────────────────────────────────────┐
                     │                                      │
  START → parse_query → expand_decs → find_medicine ──────→ fetch_chunks → verify_claim → END
                                           │
                                    (não encontrado)
                                           │
                                    suggest_similar → END (aguarda confirmação)
                                           │
                                    (confirmado)
                                           │
                                    fetch_chunks → verify_claim → END
```

## Instalação

```bash
# Clone / copie os arquivos para sua pasta de projeto
cd seu-projeto/

# Instale o pacote (editable)
pip install -e ".[openai]"          # para OpenAI
pip install -e ".[anthropic]"       # para Anthropic
pip install -e ".[ollama]"          # para Ollama local
pip install -e ".[all]"             # todos os providers

# Configure variáveis de ambiente
cp .env.example .env
# edite .env com suas chaves
```

## Variáveis de ambiente

```bash
# .env
OPENAI_API_KEY=sk-...          # necessário para OpenAI e para embeddings
ANTHROPIC_API_KEY=sk-ant-...   # necessário para Anthropic
DECS_API_KEY=...               # opcional: expansão de keywords via DeCS
```

## Como rodar

```bash
# OpenAI (padrão)
python -m bulacheck.chatbot.app

# Anthropic
python -m bulacheck.chatbot.app --provider anthropic --model claude-3-5-haiku-20241022

# Ollama (local, sem API key)
python -m bulacheck.chatbot.app --provider ollama --model llama3.2

# Com bancos customizados
python -m bulacheck.chatbot.app --db /caminho/bulas.db --anvisa-db /caminho/anvisa.db

# Com link público (ngrok interno do Gradio)
python -m bulacheck.chatbot.app --share
```

Acesse em: **http://localhost:7860**

## Uso programático

```python
from bulacheck.config import BulaCheckConfig, LLMProvider
from bulacheck.graph.pipeline import build_graph, make_initial_state
from langchain_core.messages import HumanMessage

cfg = BulaCheckConfig(
    llm_provider=LLMProvider.openai,
    llm_model="gpt-4o-mini",
    bulagratis_db_path="bulas.db",
)

graph = build_graph(cfg)
state = make_initial_state(cfg)
state["messages"].append(HumanMessage(content="Tylenol pode causar náusea?"))

result = graph.invoke(state)
print(result["verification_result"]["response_text"])
```

## Exemplos de perguntas

| Pergunta | Seções consultadas |
|---|---|
| "Tylenol pode causar náusea?" | adverse_reactions |
| "Dipirona não tem contraindicações" | contraindications |
| "Qual a dose máxima de ibuprofeno?" | dosage_and_administration, overdose |
| "Posso tomar amoxicilina com estômago cheio?" | dosage_and_administration, warnings_and_precautions |
| "Como devo guardar insulina?" | storage |

## Notas sobre os bancos de dados

### Banco BulaGratis (`bulas.db`)
- Tabela `medicines`: um medicamento por linha
- Tabela `chunks`: fragmentos de texto das bulas com embeddings (OpenAI text-embedding-3-small, dim=1536)
- Seções disponíveis: indications, how_it_works, contraindications, warnings_and_precautions, storage, dosage_and_administration, missed_dose, adverse_reactions, overdose

### Banco ANVISA (`anvisa.db`)
- Tabela `medicines`: registro ANVISA com princípio ativo
- Usado como fallback para resolver nome comercial → princípio ativo

## Extensão para LangGraph avançado

O pipeline foi projetado para migração fácil para LangGraph com:
- **Memória persistente**: substituir `make_initial_state` por `MemorySaver`
- **Streaming**: `graph.stream()` em vez de `graph.invoke()`
- **Human-in-the-loop**: breakpoints em `suggest_similar` para confirmação explícita
- **Paralelismo**: expandir `expand_decs` e `fetch_chunks` com `Send()` para múltiplos medicamentos simultâneos
