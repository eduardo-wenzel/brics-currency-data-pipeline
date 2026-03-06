# BRICS Currency Data Pipeline

Projeto de Engenharia de Dados para coletar, processar e armazenar taxas de cambio de moedas dos paises do BRICS.

Este pipeline resolve um problema simples e real: transformar dados brutos de API em dados confiaveis, estruturados e historicos para analise.

## O que este projeto resolve

- Coleta automatizada de cotacoes via API publica.
- Padronizacao e transformacao dos dados para formato analitico.
- Persistencia em banco relacional com snapshot e historico temporal.
- Observabilidade da execucao (rodou, falhou, quantos registros carregou).

## Tecnologias utilizadas

- Python
- Pandas
- Requests
- psycopg2
- python-dotenv
- PostgreSQL
- Git / GitHub
- GitHub Actions

## Arquitetura

Fluxo do pipeline:

`API -> Extraction -> Transformation -> Storage`

Etapas:

1. Extrai cotacoes da API externa.
2. Transforma o JSON em formato tabular.
3. Estrutura os dados por moeda e data de referencia.
4. Armazena para analise (snapshot + historico).

## Estrutura do projeto

```text
brics-currency-data-pipeline/
  data/
    raw/
    processed/
  logs/
  scripts/
    run_pipeline.ps1
    register_task.ps1
  sql/
    001_create_exchange_tables.sql
  src/
    ingest.py
    transform.py
    load.py
    pipeline.py
  .github/workflows/
    pipeline.yml
  README.md
  requirements.txt
```

## Modelo de dados

- `analytics.fact_exchange_rate` (snapshot atual via UPSERT)
- `analytics.fact_exchange_rate_history` (historico append-only)
- `analytics.pipeline_run_log` (status, erro, quantidade carregada)

## Variaveis de ambiente

- `API_URL`
- `CURRENCIES`
- `PG_HOST`
- `PG_DATABASE`
- `PG_USER`
- `PG_PASSWORD`
- `PG_PORT`

## Exemplo de resultado

| date | currency | value |
|---|---|---:|
| 2026-03-06 | BRL | 5.02 |
| 2026-03-06 | CNY | 7.19 |
| 2026-03-06 | INR | 83.11 |

## Como rodar

1. Clone o repositorio

```bash
git clone https://github.com/eduardo-wenzel/brics-currency-data-pipeline.git
cd brics-currency-data-pipeline
```

2. Instale dependencias

```bash
pip install -r requirements.txt
```

3. Configure `.env` com base em `.env.example`

4. Execute o pipeline

```bash
python src/pipeline.py
```

## Automacao

- Local: Windows Task Scheduler via `scripts/register_task.ps1`
- CI/CD: GitHub Actions em `.github/workflows/pipeline.yml`

## Observabilidade

O pipeline registra eventos em `logs/pipeline.log`, incluindo:

- `API request successful`
- `N currencies processed`
- `Data loaded into database`

## Melhorias futuras

- Dashboard para analise temporal das moedas
- Camada de testes automatizados
- Containerizacao com Docker
- Orquestracao com Airflow ou Prefect
