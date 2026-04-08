# BulaCheck

BulaCheck é um projeto de verificação de alegações textuais sobre medicamentos com base em bulas oficiais e modelos de linguagem.

A proposta é receber uma afirmação curta, como:

> "Tylenol faz mal para o coração"

e produzir uma resposta fundamentada, indicando se a alegação é verdadeira, falsa ou parcialmente sustentada pelas evidências encontradas nas bulas.

## Ambiente (Conda + Poetry)

Crie e ative um ambiente Conda na pasta do projeto e instale as dependências com o Poetry:

```sh
conda create --prefix ./venv python=3.11 -y
conda activate ./venv
poetry env use "$(which python)"
poetry install
```

O `poetry env use` associa o Poetry ao Python 3.11 do Conda (evita outro virtualenv separado).

## Ollama (modelo local)

Confira se o [Ollama](https://ollama.com/) está instalado. A documentação oficial está no [quickstart](https://docs.ollama.com/quickstart). Instalação rápida:

- **Linux:**

  ```sh
  curl -fsSL https://ollama.com/install.sh | sh
  ```

- **Windows** (PowerShell):

  ```powershell
  irm https://ollama.com/install.ps1 | iex
  ```

Inicie o modelo (exemplo com Llama 3.1 8B):

```sh
ollama run llama3.1:8b
```

Deixe o Ollama em execução enquanto usa o BulaCheck (a API local costuma ficar em `http://localhost:11434`).

## Executar o BulaCheck

Na raiz do repositório, com o ambiente ativado:

```sh
python -m bula_check
```

O programa pede uma alegação e conversa com o modelo.

**Estado atual:** o fluxo de busca automática nas bulas ainda não está concluído. Para depuração, quando a alegação menciona **paracetamol** e o pré-check pede evidências, o código usa **informações fixas** (bula lida de `inputs/bulas/json/paracetamol__prati_donaduzzi__cia_ltda.json`) em vez de buscar no site da Anvisa.

## Objetivo

Desenvolver um sistema capaz de:

- identificar o medicamento citado e a alegação principal
- recuperar trechos relevantes em bulas oficiais
- analisar evidências favoráveis, contrárias ou parciais
- gerar uma resposta final clara e justificável

## Dados

O projeto utiliza dois conjuntos principais de dados:

- **Bulas de medicamentos**
  - coletadas de fontes públicas
  - armazenadas inicialmente em PDF
  - convertidas para texto processável

- **Alegações textuais**
  - construídas manualmente
  - organizadas em formato estruturado
  - associadas, quando possível, a rótulos de veracidade esperados
