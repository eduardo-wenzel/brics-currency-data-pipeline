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

## Estrutura resumida

Fluxo: API -> Raw JSON (`data/raw`) -> Transform -> PostgreSQL (UPSERT).

A carga usa `INSERT ... ON CONFLICT` para manter idempotencia por `(base_currency, target_currency, reference_date)`.

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
