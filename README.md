# BRICS Currency Data Pipeline

Pipeline de dados desenvolvido para coletar, processar e analisar taxas de cambio das moedas dos paises do BRICS.

O projeto implementa um fluxo completo de engenharia de dados:

API -> Ingestao -> Transformacao -> Armazenamento -> Analise

Os dados sao coletados automaticamente de APIs publicas de cambio e organizados em um dataset estruturado para analise e visualizacao.

Este projeto demonstra conceitos praticos de:
- Data Ingestion
- Data Transformation
- Data Modeling
- Data Pipelines
- Automacao de coleta de dados

## Arquitetura do Pipeline

O pipeline segue as seguintes etapas:

1. Coleta de dados
- Consome dados de taxas de cambio via API.

2. Processamento
- Limpeza e padronizacao dos dados.
- Conversao de tipos e tratamento de valores inconsistentes.

3. Armazenamento
- Dados estruturados sao armazenados para analises futuras.

4. Analise
- Os dados podem ser utilizados para analises economicas ou dashboards.

Fluxo resumido:

API -> Python -> Processamento -> Banco de Dados -> Analise

## Tecnologias Utilizadas

- Python
- Pandas
- Requests
- psycopg2
- python-dotenv
- PostgreSQL
- Git / GitHub
- GitHub Actions

## Como executar o projeto

1. Clone o repositorio

```bash
git clone https://github.com/eduardo-wenzel/brics-currency-data-pipeline.git
```

2. Entre na pasta do projeto

```bash
cd brics-currency-data-pipeline
```

3. Instale as dependencias

```bash
pip install -r requirements.txt
```

4. Configure as variaveis de ambiente em `.env` (baseie-se em `.env.example`).

5. Execute o pipeline

```bash
python src/pipeline.py
```

## Variaveis de Ambiente

- `API_URL`
- `CURRENCIES`
- `PG_HOST`
- `PG_DATABASE`
- `PG_USER`
- `PG_PASSWORD`
- `PG_PORT`

## Persistencia e Historico

O projeto grava dados em:

- `analytics.fact_exchange_rate`: snapshot atual (UPSERT)
- `analytics.fact_exchange_rate_history`: historico temporal (append-only)
- `analytics.pipeline_run_log`: status de execucao, erro e quantidade carregada

Exemplo de analise historica:

```sql
SELECT target_currency AS currency, AVG(rate)
FROM analytics.fact_exchange_rate_history
GROUP BY target_currency;
```

## Logs do Pipeline

O pipeline registra eventos operacionais no arquivo `logs/pipeline.log`, incluindo:

- Rodou?
- Deu erro?
- Quantos dados foram carregados?

Eventos importantes:

- `API request successful`
- `N currencies processed`
- `Data loaded into database`

## Agendamento Automatico

O projeto suporta automacao por:

- Windows Task Scheduler (`scripts/register_task.ps1`)
- GitHub Actions (`.github/workflows/pipeline.yml`)

## Objetivo do Projeto

Este projeto foi desenvolvido como parte do meu aprendizado em Engenharia de Dados, com foco em:

- construcao de pipelines de dados
- ingestao via APIs
- processamento e estruturacao de dados
- persistencia historica para analise temporal
- automacao e observabilidade operacional
