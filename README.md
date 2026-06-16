# Crypto Market Pipeline

Professional ETL/ELT data pipeline built with Apache Airflow, PostgreSQL, Docker, and Python.

This project extracts cryptocurrency market data from the CoinGecko API, stores raw payloads, validates data quality, and processes the data through a Medallion Architecture: Landing, Staging, Bronze, Silver, and Gold.

---

## Architecture Overview

```text
CoinGecko API
    ↓
landing.crypto_market_raw
    ↓
quality_check
    ↓
staging.crypto_market_valid
rejected.crypto_market_rejected
    ↓
bronze.crypto_market
    ↓
silver.crypto_market
    ↓
gold.crypto_market_summary
gold.crypto_top_performers
gold.crypto_market_dominance
```

---

## Pipeline Flow

The Airflow DAG is named:

```text
crypto_market_pipeline
```

It runs every 6 hours and contains the following tasks:

```text
start_pipeline_run
    ↓
extract_and_load_landing
    ↓
quality_check
    ↓
bronze_upsert
    ↓
silver_upsert
    ↓
gold_upsert
```

---

## Main Features

* Apache Airflow orchestration
* Docker-based local development environment
* PostgreSQL storage layer
* Medallion Architecture
* CoinGecko API extraction
* Incremental processing with watermarks
* Data quality validation
* Rejected records handling
* Pipeline audit table
* DAG success and failure callbacks
* Unit tests with pytest
* GitHub Actions CI workflow

---

## Data Quality

The pipeline validates raw records before allowing them into the Bronze layer.

The validation logic checks:

* Missing `id`
* Missing `symbol`
* Missing `name`
* Invalid `current_price`
* Invalid `market_cap`
* Invalid `market_cap_rank`

Valid records are inserted into:

```text
staging.crypto_market_valid
```

Invalid records are inserted into:

```text
rejected.crypto_market_rejected
```

The quality metrics are stored in:

```text
audit.data_quality_metrics
```

---

## Medallion Layers

### Landing

Stores the raw JSON payload exactly as received from the API.

```text
landing.crypto_market_raw
```

### Staging

Stores records that passed data quality validation.

```text
staging.crypto_market_valid
```

### Bronze

Stores cleaned cryptocurrency market records in structured columns.

```text
bronze.crypto_market
```

### Silver

Stores processed daily snapshots.

```text
silver.crypto_market
```

### Gold

Stores analytical tables ready for reporting.

```text
gold.crypto_market_summary
gold.crypto_top_performers
gold.crypto_market_dominance
```

---

## Watermark Strategy

The pipeline uses watermarks to process only new records.

Watermarks are stored in:

```text
audit.watermarks
```

Each major processing step has its own watermark:

```text
crypto_market_pipeline
bronze_upsert
silver_upsert
gold_upsert
```

This allows the pipeline to run incrementally and avoid reprocessing already processed records.

---

## Audit Strategy

Pipeline execution is tracked in:

```text
audit.pipeline_runs
```

The audit layer stores:

* DAG ID
* Start time
* End time
* Duration
* Records extracted
* Valid records
* Invalid records
* Status
* Error message

The DAG uses callbacks to update the audit table automatically on success or failure.

---

## Airflow Variables

The pipeline uses Airflow Variables for configurable values:

```text
quality_min_score
coingecko_per_page
coingecko_page
```

Default values are included in the DAG as defensive protection.

---

## Running the Project Locally

Start the Airflow environment:

```bash
docker compose up -d
```

Open Airflow UI:

```text
http://localhost:8080
```

Default credentials:

```text
Username: airflow
Password: airflow
```

---

## Running Unit Tests

Enter the Airflow webserver container:

```bash
docker compose exec airflow-webserver bash
```

Install pytest:

```bash
pip install pytest
```

Run the tests:

```bash
python -m pytest -v tests/test_validate_record.py
```

Expected result:

```text
8 passed
```

---

## GitHub Actions

This project includes a GitHub Actions workflow that runs unit tests automatically when a Pull Request is opened against `main`.

Workflow file:

```text
.github/workflows/tests.yml
```

The workflow:

1. Checks out the repository
2. Starts the Airflow Docker environment
3. Installs pytest inside the Airflow container
4. Runs the unit tests

---

## Development Workflow

Recommended development flow:

```text
Create feature branch
    ↓
Make changes
    ↓
Run tests locally
    ↓
Commit changes
    ↓
Push branch
    ↓
Open Pull Request
    ↓
Wait for GitHub Actions checks
    ↓
Review changes
    ↓
Merge into main
    ↓
Delete feature branch
```

Example:

```bash
git checkout main
git pull origin main
git checkout -b feature/new-feature
```

---

## Project Structure

```text
.
├── dags/
│   └── crypto_market_pipeline.py
├── tests/
│   ├── __init__.py
│   └── test_validate_record.py
├── .github/
│   └── workflows/
│       └── tests.yml
├── docker-compose.yml
├── README.md
└── .gitignore
```

---

## Current Status

Completed:

* Airflow DAG
* CoinGecko extraction
* Landing load
* Data quality validation
* Bronze upsert
* Silver upsert
* Gold upsert
* Watermarks
* Pipeline audit
* Unit tests
* GitHub Actions CI

Planned improvements:

* Slack alerts
* Smarter retry strategy
* Silver partitioning by snapshot date
* TaskGroups
* SLA monitoring
* BranchPythonOperator
* Airflow Datasets
* Versioned Airflow Variables
* More unit and integration tests
