# Data Engineering Portfolio

A data engineering portfolio demonstrating end-to-end batch and streaming data pipelines using modern open-source technologies.

The projects cover real API ingestion, event streaming, local analytical databases, data transformation, data quality, dimensional modeling, orchestration, anomaly detection and reporting.

## Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Programming | Python | Data ingestion, processing and pipeline orchestration |
| Batch ingestion | dlt | Extract and load data into DuckDB |
| Streaming | Apache Kafka | Real-time event streaming |
| Storage | DuckDB | Local analytical database |
| Transformation | dbt | SQL transformations, models and data quality tests |
| Orchestration | Dagster | Pipeline scheduling and monitoring |
| APIs | Open-Meteo / ENTSO-E / Binance | Real-world data ingestion |
| Visualization | Google Sheets / Looker Studio | Reporting and analytics |
| Infrastructure | Docker | Local Kafka infrastructure |
| Version Control | Git / GitHub | Source control and reproducibility |

## Pipelines

### 1. 🌦️ Weather Data Pipeline — Real API

An end-to-end batch pipeline that retrieves real hourly weather data from the Open-Meteo API for five Romanian cities: Bucharest, Cluj-Napoca, Iași, Timișoara and Constanța.

The pipeline collects temperature, humidity, wind speed, precipitation and cloud coverage data for the previous 30 days.

**Architecture:**

```text
Open-Meteo API
      ↓
Python
      ↓
dlt ingestion
      ↓
DuckDB
      ↓
dbt staging
      ↓
dbt daily mart
      ↓
Data quality tests
```

The project demonstrates REST API ingestion, data normalization, local analytical storage, SQL transformations and automated data validation.

**Stack:** Python · dlt · DuckDB · dbt · SQL

---

### 2. ⚡ ENTSO-E Energy Price Pipeline — Real API

A batch data engineering pipeline that extracts day-ahead electricity prices from the ENTSO-E Transparency Platform.

The pipeline retrieves XML data from the API, parses the response, loads the data into DuckDB using dlt, transforms it with dbt and generates a daily electricity price report.

**Architecture:**

```text
ENTSO-E Transparency Platform
            ↓
Python API extraction
            ↓
XML parsing
            ↓
dlt ingestion
            ↓
DuckDB
            ↓
dbt staging
            ↓
dbt daily mart
            ↓
Data quality tests
            ↓
Daily price report
```

The dbt layer includes staging and daily analytical models together with tests for null values, duplicate timestamps and price validity.

**Stack:** Python · ENTSO-E API · dlt · DuckDB · dbt · SQL

---

### 3. 🛒 Kaggle Superstore — Star Schema

A dimensional modeling project that transforms the Kaggle Superstore dataset into an analytical Star Schema using dbt and DuckDB.

The project demonstrates staging, dimensional modeling, surrogate keys, fact and dimension tables, data quality testing and business validation.

**Architecture:**

```text
Kaggle Superstore
       ↓
Staging models
       ↓
Dimensions + Fact
       ↓
Star Schema
       ↓
dbt tests
       ↓
Business validation
       ↓
dbt documentation
```

The final model contains:

- `fact_orders`
- `dim_date`
- `dim_customer`
- `dim_location`
- `dim_product`

The fact table contains **9,994 rows** and the project includes **21 dbt data quality tests** covering uniqueness and nullability.

Business validation checks include total sales, profit, quantity, customers, products and foreign-key integrity.

**Stack:** Python · dbt · DuckDB · SQL

---

### 4. ₿ Crypto Kafka Pipeline — Real-Time Streaming

A real-time streaming pipeline that ingests cryptocurrency trade events from the Binance WebSocket API and processes them through Apache Kafka, DuckDB and dbt.

The pipeline continuously captures BTCUSDT trades, stores raw events in DuckDB, transforms them into analytical metrics and detects abnormal trading-volume spikes.

**Architecture:**

```text
Binance WebSocket
      ↓
Kafka Producer
      ↓
Kafka topic: crypto-trades
      ↓
Kafka Consumer
      ↓
DuckDB
      ↓
dbt staging
      ↓
dbt crypto metrics
      ↓
Anomaly detection
      ↓
Google Sheets
      ↓
Looker Studio
```

The analytical layer produces minute-level metrics including trade count, average/min/max price, trading volume and traded value.

The pipeline also compares recent trading volume against a historical baseline and flags significant volume spikes as anomalies.

Dagster orchestrates the dbt transformations, data quality tests, anomaly detection and Google Sheets export.

**Stack:** Python · Apache Kafka · Binance WebSocket · DuckDB · dbt · Dagster · Google Sheets · Looker Studio

---

## Project Structure

```text
data-engineering-portfolio/

├── weather/
│   ├── weather_pipeline/
│   ├── dbt_weather/
│   └── README.md
│
├── entsoe_energy/
│   ├── src/
│   ├── dbt_energy/
│   ├── data/
│   ├── run_pipeline.py
│   └── README.md
│
├── kaggle_star_schema/
│   ├── models/
│   ├── tests/
│   ├── run_pipeline.py
│   ├── dbt_project.yml
│   └── README.md
│
├── crypto-kafka-pipeline/
│   ├── producer.py
│   ├── consumer.py
│   ├── dbt_crypto/
│   ├── dagster_crypto/
│   ├── data/
│   └── README.md
│
└── README.md
```

## Key Capabilities Demonstrated

Across the projects, the portfolio demonstrates:

- Batch data ingestion
- Real-time event streaming
- REST API integration
- WebSocket data ingestion
- XML parsing
- Kafka producer/consumer architecture
- DuckDB analytical storage
- dlt ingestion pipelines
- dbt staging and mart models
- Dimensional modeling and Star Schema design
- Surrogate keys and foreign-key relationships
- Automated data quality testing
- Business-level data validation
- Anomaly detection
- Pipeline orchestration with Dagster
- Google Sheets data export
- Looker Studio reporting
- Reproducible local data pipelines

## How to Run

Clone the repository:

```bash
git clone https://github.com/AlexandraGhiba/my-portfolio.git
cd my-portfolio
```

The portfolio contains four independent projects.

The first three projects are standalone local pipelines and can be executed directly with Python. They do not require Kafka, Docker or Dagster.

The Crypto Kafka pipeline is different: it uses Apache Kafka and Dagster, so it requires a separate virtual environment and Docker-based Kafka infrastructure.

### 2. Batch Projects

The following projects can be run directly with Python:

- Weather Data Pipeline
- ENTSO-E Energy Price Pipeline
- Kaggle Star Schema

Each project contains its own pipeline entry point and manages its workflow internally.

### 3. Crypto Kafka Pipeline

The Crypto Kafka project uses a separate Python environment because it has additional dependencies and infrastructure requirements.

#### Create the virtual environment

```powershell
cd crypto-kafka-pipeline
python -m venv .venv
```

Activate it on Windows:

```powershell
.venv\Scripts\Activate.ps1
```

Install the dependencies:

```powershell
pip install -r requirements.txt
```

#### Start Docker / Kafka

Docker Desktop must be installed and running.

Start the Kafka infrastructure from the project directory:

```powershell
docker compose up -d
```

Verify that the containers are running:

```powershell
docker ps
```

Kafka must be available on:

```text
localhost:9092
```

#### Start the Kafka producer

Open a new terminal, activate the Crypto environment and run:

```powershell
cd crypto-kafka-pipeline
.venv\Scripts\Activate.ps1
python producer.py
```

The producer connects to the Binance WebSocket and publishes live BTCUSDT trade events to the Kafka topic:

```text
crypto-trades
```

Leave this terminal running.

#### Start the Kafka consumer

Open another terminal:

```powershell
cd crypto-kafka-pipeline
.venv\Scripts\Activate.ps1
python consumer.py
```

The consumer reads events from Kafka and stores them in:

```text
data/crypto.duckdb
```

Leave the consumer running as well.

At this point the streaming ingestion layer is:

```

#### Start Dagster

Open a third terminal:

```powershell
cd crypto-kafka-pipeline
.venv\Scripts\Activate.ps1
dagster dev -m dagster_crypto.definitions
```

Dagster starts the local orchestration UI.

Open:

```text
http://127.0.0.1:3000
```

From the Dagster UI, the pipeline assets can be materialized manually or allowed to run according to the configured schedule.

The Dagster pipeline runs:


### Crypto Pipeline — Complete Architecture

Once all components are running:

```text
                 Binance WebSocket
                        │
                        ▼
                 Kafka Producer
                        │
                        ▼
                Kafka: crypto-trades
                        │
                        ▼
                 Kafka Consumer
                        │
                        ▼
                     DuckDB
                        │
                        ▼
                    dbt models
                        │
                        ▼
                   dbt tests
                        │
                        ▼
                Anomaly detection
                        │
                        ▼
                  Google Sheets
                        │
                        ▼
                  Looker Studio
```

### Important

The projects are intentionally independent.

You do **not** need to start Kafka, Docker or Dagster to run the Weather, ENTSO-E or Kaggle Star Schema pipelines.

Only the Crypto Kafka project requires the additional streaming infrastructure.

For the three batch projects, the Python pipeline acts as the main orchestration layer and executes the required ingestion, transformation and validation steps automatically.



## Reproducibility

The projects are designed to run locally using Python and open-source data engineering tools.

Each pipeline follows the general pattern:

```text
Extract
  ↓
Ingest
  ↓
Store
  ↓
Transform
  ↓
Test
  ↓
Validate
  ↓
Report
```

The streaming project extends this architecture with continuous ingestion, orchestration and anomaly detection.

## Author

**Ghiba Alexandra**

Data Engineering portfolio focused on practical experience with Python, SQL, dbt, DuckDB, dlt, Kafka and Dagster.

The projects are designed to demonstrate hands-on experience building reliable data pipelines from ingestion through transformation, validation and analytics.