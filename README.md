# 💊 BulaCheck

BulaCheck é um projeto de verificação de alegações textuais sobre medicamentos com base em bulas oficiais e modelos de linguagem.

A proposta é receber uma afirmação curta, como:

> "Tylenol faz mal para o coração"

e produzir uma resposta fundamentada, indicando se a alegação é verdadeira, falsa ou parcialmente sustentada pelas evidências encontradas nas bulas.

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

## Estrutura do projeto

```
bula_check/
├── agents/
│   ├── app.py          # Interface Gradio + CLI do chatbot
│   ├── llm.py          # Fábrica LLM: OpenAI / Anthropic / Ollama
│   ├── nodes.py        # Funções de nó do LangGraph
│   ├── pipeline.py     # build_graph() e make_initial_state()
│   ├── protocol.py     # Config, State e tipos do fluxo agente/RAG
│   ├── search.py       # Busca lexical, fuzzy, híbrida e semântica
│   └── tools.py        # Tools LangChain: parsing, DeCS e embeddings
│
├── anvisa_crawler.py   # Crawler/consulta da base ANVISA
├── bula_gratis_crawler.py # Crawler/consulta da base BulaGrátis
├── bula_pdf.py         # Leitura e processamento de PDFs de bulas
├── constants.py        # Constantes gerais do projeto
├── db.py               # Funções de banco de dados
├── decs.py             # Cliente/integração com DeCS
├── omb.py              # Integração/consulta OBM
├── protocol.py         # Modelos centrais do domínio: Medicines, Chunks, Section etc.
└── inputs/             # Arquivos de entrada, bancos ou dados auxiliares
```


## Ambiente (Conda + Poetry)

Crie e ative um ambiente Conda na pasta do projeto e instale as dependências com o Poetry:

```sh
conda create --prefix ./venv python=3.11 -y
conda activate ./venv
poetry env use "$(which python)"
poetry install
```

O `poetry env use` associa o Poetry ao Python 3.11 do Conda (evita outro virtualenv separado).

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
python -m bula_check.agents.app

# Anthropic
python -m bula_check.agents.app --provider anthropic --model claude-3-5-haiku-20241022

# Ollama (local, sem API key)
python -m bula_check.agents.app --provider ollama --model llama3.2

# Com bancos customizados
python -m bula_check.agents.app --db /caminho/bulas.db --anvisa-db /caminho/anvisa.db

# Com link público (ngrok interno do Gradio)
python -m bula_check.agents.app --share
```

Acesse em: **http://localhost:7860**


## Exemplos de perguntas

| Pergunta | Seções consultadas |
|---|---|
| "Tylenol pode causar náusea?" | adverse_reactions |
| "Dipirona não tem contraindicações" | contraindications |
| "Qual a dose máxima de ibuprofeno?" | dosage_and_administration, overdose |
| "Posso tomar amoxicilina com estômago cheio?" | dosage_and_administration, warnings_and_precautions |
| "Como devo guardar insulina?" | storage |

## Notas sobre os bancos de dados

### Banco BulaGratis (`bulas_gratis.db`)
Fazer download do [Drive](https://drive.google.com/open?id=1i81WnDNGXvmzTRGm89ozhL8Xvz7qeUQm&usp=drive_fs)
- Tabela `medicines`: um medicamento por linha
- Tabela `chunks`: fragmentos de texto das bulas com embeddings (OpenAI text-embedding-3-small, dim=1536)
- Seções disponíveis: indications, how_it_works, contraindications, warnings_and_precautions, storage, dosage_and_administration, missed_dose, adverse_reactions, overdose

### Banco ANVISA (`bulas_anvisa.db`)
- Tabela `medicines`: registro ANVISA com princípio ativo
- Usado como fallback para resolver nome comercial → princípio ativo
