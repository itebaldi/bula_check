# BulaCheck

BulaCheck é um projeto de verificação de alegações textuais sobre medicamentos com base em bulas oficiais e modelos de linguagem.

A proposta é receber uma afirmação curta, como:

> "Tylenol faz mal para o coração"

e produzir uma resposta fundamentada, indicando se a alegação é verdadeira, falsa ou parcialmente sustentada pelas evidências encontradas nas bulas.

## Objetivo

Desenvolver um sistema capaz de:

- identificar o medicamento citado e a alegação principal
- recuperar trechos relevantes em bulas oficiais
- analisar evidências favoráveis, contrárias ou parciais
- gerar uma resposta final clara e justificável

## Ideia geral da solução

O sistema segue uma arquitetura em etapas:

1. **Pré-processamento da alegação**
   - normalização do texto
   - identificação do medicamento
   - extração da afirmação principal

2. **Representação e consulta**
   - transformação da alegação para busca por palavras-chave ou busca vetorial

3. **Recuperação de evidências**
   - busca de trechos relevantes em bulas oficiais

4. **Análise multiagente**
   - agente de evidências favoráveis
   - agente de evidências contrárias
   - agente de evidências parciais ou ambíguas

5. **Síntese final**
   - consolidação dos resultados
   - geração da resposta final com justificativa

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

## Tecnologias previstas

- Python
- Requests
- BeautifulSoup
- LangChain
- CrewAI

## Baseline

Como comparação inicial, será implementado um baseline supervisionado com:

- TF-IDF
- Regressão Logística ou SVM

Esse baseline utilizará apenas o texto da alegação, sem consulta às bulas.

## Avaliação

O projeto será avaliado em duas frentes:

### Classificação da veracidade
- acurácia
- precisão
- recall
- F1-score

### Recuperação de evidências
- Recall@k
- Precision@k