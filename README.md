# 🪙 BRICS Currency Data Pipeline

![Python](https://img.shields.io/badge/python-3.11+-blue.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-316192.svg?logo=postgresql)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?logo=docker&logoColor=white)
![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)
![CI Status](https://github.com/eduardo-wenzel/brics-currency-data-pipeline/actions/workflows/pipeline.yml/badge.svg)

Um pipeline de dados em estilo de producao que coleta taxas de cambio das moedas do BRICS,
processa os dados com Python e Pandas, e armazena os resultados no PostgreSQL.
O projeto inclui testes automatizados, workflows de CI e conteinerizacao com Docker.

## Qual problema este projeto resolve?

Projetos de cambio iniciantes costumam gerar apenas um snapshot momentaneo. Este projeto foca em serie temporal e observabilidade operacional:

- Coleta dados de API publica de cambio.
- Processa e padroniza o payload (JSON -> DataFrame).
- Grava snapshot atual idempotente (UPSERT) e historico append-only.
- Registra execucao, sucesso/falha e volume de dados carregados.

## 🧱 Arquitetura

Fluxo principal:

`API -> Extract -> Transform -> Load -> PostgreSQL`

```mermaid
graph TD
    A[Exchange Rate API] -->|JSON| B[Extract: data/raw]
    B -->|Pandas| C[Transform: DataFrame]
    C -->|Parquet| D[Storage: data/processed]
    C -->|psycopg2 UPSERT| E[(PostgreSQL)]

    E --> F[fact_exchange_rate]
    E --> G[fact_exchange_rate_history]
    E --> H[exchange_rates]
    E --> I[pipeline_run_log]
```

## 📀 Modelo de dados (PostgreSQL)

Schema: `analytics`

- `fact_exchange_rate`: estado atual por `base_currency`, `target_currency`, `reference_date` (UPSERT).
- `fact_exchange_rate_history`: historico append-only por execucao do pipeline.
- `exchange_rates`: serie temporal simplificada para analise rapida.
- `pipeline_run_log`: auditoria operacional (inicio, fim, status, erro, registros carregados).

## 🚀 Como executar com Docker (recomendado)

Prerequisitos:

- Docker Desktop
- Docker Compose

1. Clone o repositorio:

```bash
git clone https://github.com/eduardo-wenzel/brics-currency-data-pipeline.git
cd brics-currency-data-pipeline
```

2. Configure ambiente:

```bash
cp .env.example .env
```

No Windows PowerShell, se necessario:

```powershell
Copy-Item .env.example .env
```

3. Suba o banco:

```powershell
./scripts/docker.ps1 up
```

4. Execute o pipeline no container:

```powershell
./scripts/docker.ps1 run
```

5. Veja logs e status:

```powershell
./scripts/docker.ps1 logs
./scripts/docker.ps1 ps
```

6. Suba o PgAdmin (opcional):

```powershell
./scripts/docker.ps1 pgadmin
```

Acesso PgAdmin: `http://localhost:5050`

7. Pare o ambiente:

```powershell
./scripts/docker.ps1 down
```

## 💻 Como executar localmente (desenvolvimento)

1. Crie/ative um ambiente virtual.
2. Instale dependencias:

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

3. Execute o pipeline:

```bash
python pipeline/run.py
```

Execucao por etapa:

```bash
python pipeline/extract.py
python pipeline/transform.py
```

## 🧪 Qualidade de codigo e testes

```bash
pytest
ruff check .
black --check .
pre-commit run --all-files
```

## Variaveis de ambiente

Obrigatorias para o pipeline:

- `API_URL`
- `CURRENCIES`
- `PG_HOST`
- `PG_DATABASE`
- `PG_USER`
- `PG_PASSWORD`
- `PG_PORT`

Opcionais para PgAdmin (Docker):

- `PGADMIN_DEFAULT_EMAIL`
- `PGADMIN_DEFAULT_PASSWORD`

Opcionais para alertas CI:

- `SLACK_WEBHOOK_URL`
- `EMAIL_SMTP_SERVER`
- `EMAIL_SMTP_PORT`
- `EMAIL_USERNAME`
- `EMAIL_PASSWORD`
- `EMAIL_TO`
- `EMAIL_FROM`

## ⚙️ Automacao e CI/CD

- Local: `scripts/register_task.ps1` (Windows Task Scheduler).
- GitHub Actions: `.github/workflows/pipeline.yml` para execucao automatizada e checks de qualidade.

## 🗺️ Roadmap

- [ ] Migrar camadas de dados locais para S3 (Bronze/Silver).
- [ ] Adotar orquestrador dedicado (Airflow ou Dagster).
- [ ] Adicionar testes de qualidade de dados (dbt/Great Expectations).
- [ ] Expor dashboards em ferramenta de BI (Metabase/Superset).
- [ ] Provisionar infraestrutura com Terraform.


