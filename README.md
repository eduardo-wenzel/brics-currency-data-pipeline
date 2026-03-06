# BRICS Currency Data Pipeline

Pipeline de dados que extrai, transforma e carrega cotacoes de moedas em PostgreSQL.

## Execucao manual

1. Crie e ative um ambiente Python.
2. Instale dependencias:

```bash
pip install -r requirements.txt
```

3. Configure variaveis de ambiente copiando `.env.example` para `.env`.
4. Execute:

```bash
python src/pipeline.py
```

## Persistencia de dados

O pipeline grava em duas tabelas de fatos:
- `analytics.fact_exchange_rate`: snapshot atual por `(base_currency, target_currency, reference_date)` via UPSERT.
- `analytics.fact_exchange_rate_history`: historico append-only, uma nova linha por moeda a cada execucao.

Log de execucao:
- `analytics.pipeline_run_log`: status (`RUNNING`, `SUCCESS`, `FAILED`), quantidade carregada e mensagem de erro.

Script SQL de criacao:
- `sql/001_create_exchange_tables.sql`

Consultas uteis:

```sql
-- Serie temporal por moeda
SELECT reference_date, loaded_at, rate
FROM analytics.fact_exchange_rate_history
WHERE base_currency = 'USD' AND target_currency = 'BRL'
ORDER BY loaded_at DESC;

-- Ultimas execucoes do pipeline
SELECT run_id, started_at, finished_at, status, records_loaded, error_message
FROM analytics.pipeline_run_log
ORDER BY run_id DESC
LIMIT 20;
```

## Logging operacional

Eventos registrados em `logs/pipeline.log`:
- `API request successful`
- `N currencies processed`
- `Data loaded into database`

## Automacao local (Windows Task Scheduler)

Registrar tarefa diaria (exemplo: 06:00):

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\register_task.ps1 -DailyAt "06:00"
```

Rodar sob demanda:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_pipeline.ps1
```

## Automacao no GitHub Actions

Workflow em `.github/workflows/pipeline.yml` com:
- `workflow_dispatch` (manual)
- `schedule` diario as 09:00 UTC

Secrets necessarios no repositorio:
- `API_URL`
- `CURRENCIES`

## Alertas (opcional)

Slack (webhook):
- `SLACK_WEBHOOK_URL`

E-mail (SMTP):
- `EMAIL_SMTP_SERVER`
- `EMAIL_SMTP_PORT`
- `EMAIL_USERNAME`
- `EMAIL_PASSWORD`
- `EMAIL_TO`
- `EMAIL_FROM`
