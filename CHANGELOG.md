# Changelog

Todas as mudancas relevantes deste projeto serao registradas aqui.

O formato segue a ideia de "Keep a Changelog" e usa versionamento baseado nos marcos atuais do repositorio.

## [Unreleased]

### Added
- Camada Gold no data lake com metricas analiticas derivadas da Silver.
- `CHANGELOG.md` para registrar a evolucao do projeto.
- Scripts cross-platform `scripts/docker.sh`, `scripts/run_pipeline.sh` e `Makefile`.
- Retry com backoff exponencial e endpoint secundario opcional na extracao da API.
- Docstrings nas funcoes principais do pipeline para melhorar manutencao e onboarding.

## [0.4.0] - 2026-04-14

### Added
- Camada Gold persistida em `data/gold` ou `S3_GOLD_PREFIX`.
- Etapa `gold_task` na DAG do Airflow.
- Metricas analiticas como taxa anterior, variacao percentual, tendencia e ranking por snapshot.

### Changed
- Fluxo principal ampliado para `Bronze -> Silver -> Gold`.
- Documentacao e configuracao atualizadas para refletir a nova camada.

## [0.3.0] - 2026-03-12

### Added
- Suporte a data lake em S3 para Bronze e Silver.
- Modo de execucao `S3-only` com Docker.
- Melhorias visuais e arquiteturais no README.

## [0.2.0] - 2026-03-11

### Added
- DAG do Apache Airflow para orquestracao fim a fim.
- Migracoes SQL versionadas.
- `pipeline_run_log` e historico append-only de cargas.
- Workflows de CI com pytest, ruff e black.
- Alertas opcionais por Slack e email.

### Changed
- Reestruturacao do projeto para um layout mais profissional de data pipeline.

## [0.1.0] - 2026-03-06

### Added
- Pipeline inicial de extracao, transformacao e carga de cotacoes BRICS.
- Persistencia relacional no PostgreSQL.
- Estrutura de testes e automacao basica.
