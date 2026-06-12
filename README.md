# 💊 BulaCheck

BulaCheck é um projeto de verificação de alegações textuais sobre medicamentos com base em bulas oficiais e modelos de linguagem.

A proposta é receber uma afirmação curta, como:

> "Tylenol faz mal para o coração"

e produzir uma resposta fundamentada, indicando se a alegação é verdadeira, falsa ou parcialmente sustentada pelas evidências encontradas nas bulas.

## Exemplos do que faz

| Pergunta | Seções consultadas |
|---|---|
| "Tylenol pode causar náusea?" | adverse_reactions |
| "Dipirona não tem contraindicações" | contraindications |
| "Qual a dose máxima de ibuprofeno?" | dosage_and_administration, overdose |
| "Posso tomar amoxicilina com estômago cheio?" | dosage_and_administration, warnings_and_precautions |
| "Como devo guardar insulina?" | storage |

## Setup

### 1. Clone

```sh
git clone <repo-url>
cd bula_check
```

### 2. Ambiente (Conda + Poetry)

Pré-requisitos: [Miniconda](https://docs.conda.io/en/latest/miniconda.html) e [Poetry](https://python-poetry.org/docs/#installation) instalados.

```sh
conda create --prefix ./venv python=3.11 -y
conda activate ./venv
poetry env use "$(which python)"
poetry install
```

O `poetry env use` associa o Poetry ao Python 3.11 do Conda (evita outro virtualenv separado).

### 3. Variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto.

**Apenas `OPENAI_API_KEY` é estritamente obrigatória** (usada pra embeddings em runtime e para o LLM padrão).
As demais dependem de quais features e providers você for usar.

| Variável | Quando precisa | Onde é lida |
|---|---|---|
| `OPENAI_API_KEY` | Sempre (LLM `gpt-4o-mini` padrão + embeddings `text-embedding-3-small`) | [bula_gratis_crawler.py:421](bula_check/bula_gratis_crawler.py#L421), [sliding_window_crawler.py:231](bula_check/sliding_window_crawler.py#L231), [agents/tools.py:226](bula_check/agents/tools.py#L226), `ChatOpenAI` (SDK) |
| `ANTHROPIC_API_KEY` | Se usar `--provider anthropic` | `ChatAnthropic` (SDK) |
| `GROQ_API_KEY` | Se usar `--provider groq` | `ChatGroq` (SDK) |
| `GOOGLE_API_KEY` | Se usar `--provider google` | `ChatGoogleGenerativeAI` (SDK) |
| `DECS_API_KEY` | Opcional — expansão de keywords via DeCS | [decs.py:41](bula_check/decs.py#L41), [agents/tools.py:150](bula_check/agents/tools.py#L150) |
| `OBM_TOKEN` | Opcional — wrapper da API OMB | [omb.py:25](bula_check/omb.py#L25) |

Ollama não precisa de chave (roda local em `http://localhost:11434`).

Exemplo mínimo de `.env`:

```env
OPENAI_API_KEY=sk-...
DECS_API_KEY=... # opcional, mas recomendado para melhor expansão de keywords
```

### 4. Bancos de dados

Os bancos devem ficar na **raiz do projeto** (caminhos default em [agents/protocol.py:69-70](bula_check/agents/protocol.py#L69)).
Total combinado dos obrigatórios: **~1.22 GB**.

| Arquivo | Tamanho | Propósito | Obrigatório? |
|---|---|---|---|
| `bulas_gratis.db` | **1.2 GB** | Chunks de bulas do bula.gratis com embeddings `text-embedding-3-small` (1536-dim). Núcleo do RAG. | **Sim** |
| `bulas_anvisa.db` | **14 MB** | Metadata de medicamentos ANVISA (nome, registro, princípio ativo). Usado como fallback em `find_medicine` pra resolver brand → genérico. | **Sim** |
| `bulas_gratis_sliding.db` | **1.1 GB** | Variante alternativa do BulaGratis com chunking por janela deslizante. | Opcional |

#### Download

- **`bulas_gratis.db`** — [Google Drive](https://drive.google.com/file/d/1i81WnDNGXvmzTRGm89ozhL8Xvz7qeUQm/view?usp=sharing)
- **`bulas_gratis_sliding.db`** — [Google Drive](https://drive.google.com/file/d/1UKYkkpWv4hn5c5JInPVl1mpGiRHkOvQh/view?usp=sharing)

#### Alternativa: gerar via crawl

Se preferir não baixar (~1.2 GB), pode gerar os bancos rodando os crawlers do próprio projeto:

```bash
# bulas_anvisa.db (~5-10min, API pública, sem custo)
python -m bula_check.anvisa_crawler

# bulas_gratis.db (várias horas, com custo de embeddings OpenAI)
python -m bula_check.bula_gratis_crawler
```

⚠️ O crawl de `bula_gratis` com `embed=True` gera ~36k chunks via OpenAI `text-embedding-3-small` —
da ordem de alguns dólares e várias horas. Distribuir o `.db` pronto é bem mais prático.

### 5. Verificar que funcionou

```sh
python -m bula_check.agents.app
```

Acesse **http://localhost:7860** e pergunte:

> "Tylenol pode causar náusea?"

Esperado: veredicto **CONFIRMADA** com citação de um chunk da seção `adverse_reactions` de uma bula de paracetamol.

## Como rodar

```bash
# OpenAI (padrão)
python -m bula_check.agents.app

# OpenAI, apontando para os bancos
python -m bula_check.agents.app --db bulas_gratis.db --anvisa-db bulas_anvisa.db

# Anthropic
python -m bula_check.agents.app --provider anthropic --model claude-3-5-sonnet-20241022 \
    --db bulas_gratis.db --anvisa-db bulas_anvisa.db

# Ollama (local, sem API key)
python -m bula_check.agents.app --provider ollama --model llama3.2 \
    --db bulas_gratis.db --anvisa-db bulas_anvisa.db

# Link público (Gradio share)
python -m bula_check.agents.app --share --db bulas_gratis.db --anvisa-db bulas_anvisa.db
```

Flags suportadas: `--provider {openai,anthropic,ollama}`, `--model`, `--db`, `--anvisa-db`, `--port` (default 7860), `--share`.

## Testes e avaliação

### Rodando a avaliação

O dataset gold-standard fica em `inputs/evaluation/dataset.json` (e `dataset_sliding.json` pro DB sliding). Os resultados de cada run são gravados em `outputs/evaluation/results/{nome}.json`.

```sh
# Roda o modelo mais rápido ~2min
pytest "tests/evaluate/test_evaluate.py::test_evaluate_results[0.3-True-False-True-False-only_desired-openai-gpt-4o-mini]" -xvs

# Roda o modelo com melhor resultado ~38min
pytest "tests/evaluate/test_evaluate.py::test_evaluate_results[1.3-True-False-True-False-only_desired-ollama-qwen3:8b]" -xvs

# Roda o(s) modelo(s) atualmente descomentado(s) em tests/evaluate/test_evaluate.py
pytest -k test_evaluate_results -xvs
```

`tests/evaluate/test_evaluate.py` é parametrizado — comente/descomente as linhas pra escolher quais combinações rodar:

- `with_rag` — True (pipeline completo) ou False (baseline closed-book, sem retrieval)
- `lexical_weight` / `semantic_weight` — habilita/desabilita cada componente do score híbrido
- `sliding_db` — True usa `bulas_gratis_sliding.db` e `dataset_sliding.json`
- `return_chunks` — `only_desired` ou `with_prev_and_next` (vizinhos no contexto)
- `llm_provider` / `llm_model` — OpenAI, Anthropic, Ollama, Google, Groq

### Métricas reportadas

Cada arquivo de resultado contém um JSON com:

- **Acurácia ponta-a-ponta**
  - `medicine_accuracy` — medicamento previsto bate com o esperado (match normalizado, incluindo princípio ativo como fallback)
  - `section_accuracy` — alguma seção esperada foi recuperada
  - `verdict_accuracy` — verdict do LLM bate com o esperado
- **Métricas IR semânticas** (cosine entre retrieved e gabarito + section gate)
  - `semantic_recall`, `semantic_precision`, `semantic_f1`
  - `semantic_r_precision` — precision@|gabarito| (mode-agnostic em relação a top_k)
  - `semantic_ap` — Average Precision (vira MAP quando agregado entre queries)
  - `semantic_mrr`, `semantic_hit_at_1`
- **`config`** — snapshot dos parâmetros usados na run

Detalhes em [bula_check/evaluate.py](bula_check/evaluate.py).

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

A flag `with_rag=False` no `BulaCheckConfig` curto-circuita `expand_decs → find_medicine → fetch_chunks` e vai direto de `parse_query` para `verify_claim` em modo closed-book — usado pra ablation study (medir contribuição do retrieval vs conhecimento paramétrico do LLM).

## Estrutura do projeto

```
.
├── bula_check/                  # Pacote Python (código)
│   ├── agents/
│   │   ├── app.py               # Interface Gradio + CLI do chatbot
│   │   ├── llm.py               # Fábrica LLM: OpenAI / Anthropic / Ollama / Groq / Google
│   │   ├── nodes.py             # Funções de nó do LangGraph
│   │   ├── pipeline.py          # build_graph() e make_initial_state()
│   │   ├── protocol.py          # Config, State e tipos do fluxo agente/RAG
│   │   ├── search.py            # Busca lexical, fuzzy, híbrida e semântica
│   │   └── tools.py             # Tools LangChain: parsing, DeCS e embeddings
│   │
│   ├── anvisa_crawler.py        # Crawler/consulta da base ANVISA
│   ├── bula_gratis_crawler.py   # Crawler/consulta da base BulaGratis
│   ├── bula_pdf.py              # Leitura e processamento de PDFs de bulas
│   ├── constants.py             # Constantes gerais do projeto
│   ├── db.py                    # Funções de banco de dados
│   ├── decs.py                  # Cliente/integração com DeCS
│   ├── evaluate.py              # Métricas do experimento
│   ├── omb.py                   # Integração/consulta OBM
│   ├── protocol.py              # Modelos centrais: Medicines, Chunks, Section etc.
│   ├── semantic_eval.py         # Métricas IR semânticas (cosine + section gate)
│   └── sliding_window_crawler.py # Crawler com chunking por janela deslizante
│
├── bulas_gratis.db              # 1.2 GB — banco principal RAG (obrigatório, baixar do Drive)
├── bulas_anvisa.db              # 14 MB — metadata ANVISA
├── bulas_gratis_sliding.db      # 1.1 GB — variante sliding window (opcional)
├── .env                         # Variáveis de ambiente (OPENAI_API_KEY etc.)
│
├── inputs/
│   └── evaluation/              # Dataset gold-standard e queries de avaliação
│       ├── dataset.json
│       └── dataset_sliding.json
│
├── outputs/
│   ├── evaluation/results/      # Resultados das runs de avaliação (pytest -k test_evaluate_results)
│   ├── bula_gratis/             # JSONs exportados pelo crawler/eval (chunks + sections)
│   └── anvisa/                  # JSONs exportados pelo crawler ANVISA
│
├── tests/                       # pytest tests/ pra rodar todos
├── pyproject.toml               # Dependências e configuração Poetry
└── README.md
```

> ℹ️ Os arquivos `.db` ficam na **raiz do projeto** (não em `inputs/`). `inputs/` guarda só o dataset de avaliação; resultados de eval e dumps de crawler vão pra `outputs/`.
