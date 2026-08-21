# crypto-kafka-pipeline

Real-time data pipeline that ingests live crypto trade data, processes it through a modern data stack, detects trading-volume anomalies, and exports rolling metrics to Google Sheets for visualization.

## Overview

This project simulates a production-style streaming pipeline for cryptocurrency trade data. Raw trades are ingested in near real-time and progressively transformed into clean, aggregated metrics — with automated data-quality testing and anomaly detection built into the pipeline itself.

**Stack:** Kafka -> DuckDB -> dbt -> Dagster -> Google Sheets -> Looker Studio

## Architecture

```
Binance WebSocket (wss://stream.binance.com:9443/ws/btcusdt@trade)
        |
        v
  Kafka producer (producer.py) --> topic "crypto-trades"
        |
        v
  Kafka consumer (consumer.py)
        |
        v
  raw_crypto_trades (DuckDB)
        |
        v
   dbt: stg_crypto_trades (staging/cleaning)
        |
        v
   dbt: mart_crypto_metrics (per-minute aggregates)
        |
        v
   Anomaly detection (volume spikes vs. historical baseline)
        |
        v
   Google Sheets export (LiveData tab)
        |
        v
   Looker Studio dashboard
```

The Kafka producer and consumer run as standalone long-running processes, continuously streaming and persisting trade data. The rest of the pipeline (dbt transforms, tests, anomaly detection, Sheets export) is orchestrated by **Dagster** and scheduled to run automatically every 10 minutes.

### Kafka producer

`producer.py` connects to the **Binance WebSocket API** (`btcusdt@trade` stream) and publishes each trade event to the Kafka topic **`crypto-trades`**, keyed by symbol. Each message contains: `timestamp`, `symbol`, `price`, `quantity`, `trade_id`.

### Kafka consumer

`consumer.py` subscribes to the **`crypto-trades`** topic (consumer group `duckdb-consumer-v2`) and writes every incoming trade into the **`raw_crypto_trades`** table in DuckDB (`data/crypto.duckdb`), initializing the table/schema on first run.

## Pipeline steps (Dagster assets)

| Asset | Description |
|---|---|
| `crypto_dbt_models` | Runs `dbt run`, materializing the staging and marts models. Reports row counts for raw, staging, and mart tables. |
| `crypto_dbt_tests` | Runs `dbt test` to validate data quality (not-null checks, positive price/quantity checks, etc.). |
| `crypto_anomaly_check` | Compares the latest 10-minute trading volume against the historical baseline (average of the prior 10 windows). Flags a window as `ANOMALY` if volume is 3x or more above baseline, and logs results to a `crypto_anomalies` table. |
| `crypto_metrics_to_sheets` | Uploads `mart_crypto_metrics` to a Google Sheet via a service account, so the data can feed a live dashboard. |

All four assets run in sequence, on a schedule (`crypto_pipeline_10min`, every 10 minutes).

## Data model

- **`raw_crypto_trades`** — raw trade events as ingested from the Kafka stream.
- **`stg_crypto_trades`** (dbt staging model) — cleaned/typed version of raw trades.
- **`mart_crypto_metrics`** (dbt mart model) — trades aggregated into 1-minute buckets per symbol, with:
  - `minute_bucket`, `symbol`
  - `trade_count`, `avg_price`, `max_price`, `min_price`
  - `volume`, `traded_value`
- **`crypto_anomalies`** — one row per anomaly check run, recording the window, recent vs. baseline volume, the ratio, and a PASS/ANOMALY status.

## dbt tests

Data-quality tests run automatically as part of the pipeline (`dbt test`):
- Not-null checks on core fields (`price`, `quantity`, `symbol`, `timestamp`, `trade_id`)
- Custom singular tests: `price_positive`, `quantity_positive`

## Tech stack

- **Kafka** (`kafka-python`) — real-time ingestion of crypto trade data, producer/consumer pattern
- **Binance WebSocket API** — live source of BTCUSDT trade data
- **DuckDB** — lightweight embedded analytical database
- **dbt** — SQL transformation, testing, and documentation
- **Dagster** — orchestration, scheduling, and asset lineage
- **gspread / Google Sheets API** — export layer for visualization
- **Looker Studio** — dashboarding on top of the exported data

## Project structure

```
crypto-kafka-pipeline/
├── producer.py             # Binance WebSocket -> Kafka topic "crypto-trades"
├── consumer.py             # Kafka topic "crypto-trades" -> raw_crypto_trades (DuckDB)
├── dagster_crypto/
│   ├── assets.py          # Dagster asset definitions (dbt run/test, anomaly check, sheets export)
│   └── definitions.py     # Dagster Definitions + schedule
├── dbt_crypto/
│   ├── models/
│   │   ├── staging/       # stg_crypto_trades
│   │   └── marts/         # mart_crypto_metrics
│   └── tests/             # custom dbt tests
├── data/
│   └── crypto.duckdb       # local DuckDB database (gitignored)
├── service_account.json   # Google service account credentials (gitignored, not committed)
└── README.md
```

## Setup

### 1. Install dependencies

```bash
pip install dagster dagster-webserver duckdb dbt-duckdb gspread google-auth
```

### 2. Google Sheets access

This pipeline writes to Google Sheets using a service account:

1. Create a Google Cloud project and enable the **Google Sheets API** and **Google Drive API**.
2. Create a service account, generate a JSON key, and save it as `service_account.json` in the project root.
3. Share your target Google Sheet with the service account's email (found in the `client_email` field of the JSON file), with **Editor** access.
4. Update `SPREADSHEET_ID` and `WORKSHEET_NAME` in `dagster_crypto/assets.py` to match your sheet.

> `service_account.json` is excluded from version control via `.gitignore` — never commit real credentials.

### 3. Start Kafka

Make sure a local Kafka broker is running and reachable at `localhost:9092` (e.g. via `kafka-server-start` or a Docker Kafka setup), and that the `crypto-trades` topic exists (or auto-creation is enabled).

### 4. Start the producer and consumer

In two separate terminals:

```bash
python producer.py
```

```bash
python consumer.py
```

The producer streams live BTCUSDT trades from Binance into Kafka; the consumer reads from Kafka and persists them into `data/crypto.duckdb`. Leave both running in the background — this is what feeds `raw_crypto_trades` continuously.

### 5. Run the Dagster pipeline

```bash
dagster dev -m dagster_crypto.definitions
```

Open the Dagster UI at `http://127.0.0.1:3000`, go to **Assets**, and materialize the pipeline (or let the 10-minute schedule run automatically). This step transforms whatever `raw_crypto_trades` has accumulated so far, tests it, checks for anomalies, and exports to Google Sheets.

## Why this project

This project was built as a portfolio piece for Data Engineer roles, focused on demonstrating:
- End-to-end orchestration of a streaming-style pipeline (ingestion -> transformation -> testing -> alerting -> export)
- Practical use of dbt for both transformation and data-quality enforcement
- Lightweight anomaly detection built directly into the pipeline
- Integration with an external reporting layer (Google Sheets / Looker Studio) rather than stopping at the warehouse

## Possible next steps

- Add alerting (e.g. Slack/email) when `crypto_anomaly_check` flags a real anomaly
- Support multiple trading symbols beyond BTCUSDT
- Add a `dbt` incremental model for `mart_crypto_metrics` instead of full-refresh table materialization
- Deploy Dagster with a persistent `DAGSTER_HOME` and a proper daemon instead of `dagster dev`