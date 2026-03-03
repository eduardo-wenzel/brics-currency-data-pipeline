# 🚀 BRICS Currency Data Pipeline
------------------------------------------------

## 📌 Overview

Este projeto implementa um pipeline de dados modular para ingestão, transformação e persistência de taxas de câmbio obtidas via API externa.

A arquitetura foi projetada com foco em:

+ Separação de responsabilidades (Ingest, Transform, Load)

+ Persistência auditável (Raw e Processed)

+ Idempotência via UPSERT no PostgreSQL

+ Validação estrutural do payload da API

+ Configuração via variáveis de ambiente

Fluxo completo:

API → Raw JSON → Transform → Parquet → PostgreSQL

## 🏗 Arquitetura

    ┌────────────┐
    │   FX API   │
    └──────┬─────┘
           │
    (Ingest Layer)
           │
     Raw JSON Storage
           |
    (Transform Layer)
           │
    Parquet Storage
           │
      (Load Layer)
           │
    PostgreSQL - UPSERT

  

🔹 Ingest

+ Consome API de câmbio

+ Valida resposta

+ Persiste payload bruto em data/raw

+ Loga quantidade de moedas recebidas

🔹 Transform

+ Lê o arquivo raw mais recente

+ Valida estrutura esperada

+ Usa base_code e time_last_update_utc

+ Gera DataFrame estruturado

+ Persiste em Parquet

🔹 Load

Conecta ao PostgreSQL

Executa INSERT ... ON CONFLICT

Garante idempotência e integridade via índice único

-------------------------------------------------------

## 🧠 Modelagem

Tabela analytics.fact_exchange_rate

+ base_currency

+ target_currency

+ rate

+ reference_date

+ created_at

Constraint:

+ Índice único em (base_currency, target_currency, reference_date)

+ Isso evita duplicidade e permite atualização segura de taxas.

--------------------

## 🛠 Tecnologias Utilizadas

+ Python

+ Pandas

+ PostgreSQL

+ psycopg2

+ Requests

+ python-dotenv

+ Logging nativo

------------------------------

## ▶ Como Executar

Criar banco PostgreSQL

Criar schema e tabela:

```CREATE SCHEMA analytics;

CREATE TABLE analytics.fact_exchange_rate (
    id SERIAL PRIMARY KEY,
    base_currency VARCHAR(10) NOT NULL,
    target_currency VARCHAR(10) NOT NULL,
    rate NUMERIC(18,8) NOT NULL,
    reference_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX ux_exchange_unique
ON analytics.fact_exchange_rate (base_currency, target_currency, reference_date);
````
Criar .env baseado em .env.example

Instalar dependências:

```pip install -r requirements.txt```

Executar:

```python src/pipeline.py```

------------------------------------------

## 📊 Evoluções Futuras

+ Containerização com Docker

+ Orquestração com Airflow ou Prefect

+ Testes unitários para camada de transformação

+ Implementação de camadas Bronze/Silver/Gold

+ Deploy em ambiente cloud
