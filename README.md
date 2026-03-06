# BRICS Currency Data Pipeline

Pipeline de Engenharia de Dados para coletar, processar e armazenar taxas de cambio das moedas do BRICS em formato analitico e historico.

## Qual problema o projeto resolve?

Projetos de cambio iniciantes costumam gerar apenas uma foto do momento. Este projeto gera serie temporal e observabilidade operacional:
- coleta dados de API publica
- processa e padroniza os dados
- grava snapshot atual e historico
- registra se o pipeline rodou, falhou e quantos registros carregou

## Arquitetura

Fluxo principal:

`API -> Extraction -> Transformation -> Storage -> Analysis`

Diagrama rapido:

```text
ExchangeRate API -> pipeline/extract.py -> pipeline/transform.py -> pipeline/load.py -> PostgreSQL
```

## Tecnologias utilizadas

- Python
- Pandas
- Requests
- PostgreSQL
- psycopg2
- python-dotenv
- Git / GitHub
- GitHub Actions

## Estrutura do projeto

```text
brics-currency-data-pipeline/
  data/
    raw/
    processed/
  pipeline/
    extract.py
    transform.py
    load.py
    run.py
  notebooks/
    analysis.ipynb
  scripts/
    run_pipeline.ps1
    register_task.ps1
  sql/
    001_create_exchange_tables.sql
  requirements.txt
  README.md
```

## Etapas do pipeline

1. Extract
- busca cotacoes em API externa
- salva payload bruto em `data/raw`

2. Transform
- normaliza JSON em estrutura tabular
- filtra moedas configuradas
- prepara dataset para carga

3. Load
- grava snapshot em `analytics.fact_exchange_rate`
- grava historico append-only em `analytics.fact_exchange_rate_history`
- grava serie temporal simplificada em `analytics.exchange_rates`
- registra execucao em `analytics.pipeline_run_log`

## Variaveis de ambiente

- `API_URL`
- `CURRENCIES`
- `PG_HOST`
- `PG_DATABASE`
- `PG_USER`
- `PG_PASSWORD`
- `PG_PORT`

Opcional (alertas CI):
- `SLACK_WEBHOOK_URL`
- `EMAIL_SMTP_SERVER`
- `EMAIL_SMTP_PORT`
- `EMAIL_USERNAME`
- `EMAIL_PASSWORD`
- `EMAIL_TO`
- `EMAIL_FROM`

## Exemplo de resultado

| date | currency | value |
|---|---|---:|
| 2026-03-06 | BRL | 5.02 |
| 2026-03-06 | CNY | 7.19 |
| 2026-03-06 | INR | 83.11 |

## Como executar

```bash
git clone https://github.com/eduardo-wenzel/brics-currency-data-pipeline.git
cd brics-currency-data-pipeline
pip install -r requirements.txt
python pipeline/run.py
```

Execucao por etapa:

```bash
python pipeline/extract.py
python pipeline/transform.py
```

## Reprodutibilidade

Dependencias fixadas em `requirements.txt`.
Camadas de dados separadas em `data/raw` e `data/processed`.
Script SQL versionado em `sql/001_create_exchange_tables.sql`.

## Historico de cambio (serie temporal)

Tabela simplificada para analise temporal:
- `analytics.exchange_rates` (`id`, `currency`, `rate`, `timestamp`)

Consulta exemplo:

```sql
SELECT currency, AVG(rate)
FROM analytics.exchange_rates
GROUP BY currency;
```

## Observabilidade

Arquivo de log: `logs/pipeline.log`

Eventos chave:
- `API request successful`
- `N currencies processed`
- `Data loaded into database`

## Automacao

- Windows Task Scheduler: `scripts/register_task.ps1`
- GitHub Actions: `.github/workflows/pipeline.yml`
