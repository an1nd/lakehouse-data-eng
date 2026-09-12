# Walmart Retail Lakehouse: End-to-End Medallion Architecture

An automated, enterprise-grade data lakehouse pipeline processing retail transactions across Bronze, Silver, and Gold tiers using Databricks, dbt, and Apache Airflow on Docker.

---

## Architecture Overview

```
[Postgres] 
       │ (Databricks CDC / JDBC Ingestion)
       ▼
[Bronze Layer]  ──► Delta Lake Raw Tables (`walmart.bronze.*`)
       │
       │ (dbt Incremental Models + Schema Tests)
       ▼
[Silver Technical] ─► Cleaned & Validated Delta Tables (`walmart.silver.silver_*`)
       │
       │ (dbt Dynamic Jinja Joins)
       ▼
[Silver Business]  ─► One Big Table (`walmart.silver.obt_orders`)
       │
       ├──► [Gold Snapshots] ─► Slowly Changing Dimensions (SCD Type 2) (`walmart.gold.customer_snapshot`)
       │
       └──► [Gold Fact Layer] ─► Aggregated Business Metrics (`walmart.gold.fact_daily_metrics`)
```

**Orchestration Engine:** Apache Airflow (Docker / LocalExecutor) triggers and polls Databricks SDK jobs, handles Delta Optimistic Concurrency Control (OCC) retries, and executes downstream dbt builds in strict sequence.

---

## Tech Stack

* **Storage & Compute:** Databricks Lakehouse Platform, Delta Lake, Unity Catalog
* **Data Transformation:** dbt-core, dbt-databricks (Jinja templating, generic tests, snapshots)
* **Orchestration:** Apache Airflow 2.9 (Custom Docker build with dbt & Databricks SDK)
* **Infrastructure:** Docker Desktop, WSL 2, Git

---

## Pipeline Layers

### 1. Bronze (Raw Ingestion)
* Ingests upstream operational tables (`customers`, `orders`, `products`, `order_items`, `stores`) into Databricks Delta tables.
* Tracks updates via cursor column `updated_timestamp` for incremental syncs.

### 2. Silver Technical (Data Cleansing)
* Materialized as incremental Delta models using `is_incremental()` logic.
* Automated dbt tests enforce data integrity constraints (`unique`, `not_null`, custom assertions).

### 3. Silver Business (OBT)
* Implements a metadata-driven `obt_orders` table via Jinja loops to dynamically compile joins across dimensional attributes without hardcoding repetitive SQL.

### 4. Gold (Analytics & Historical Tracking)
* **SCD Type 2 Dimension:** `customer_snapshot` tracks historical attribute changes over time using `check` strategy.
* **Aggregated Fact Table:** `fact_daily_metrics` produces daily rollups of revenue, order volume, and average order value (AOV) partitioned for BI consumption.

---

## Airflow Orchestration DAG

The DAG (`walmart_lakehouse_orchestration`) runs on a daily schedule (`0 11 * * *`):
1. `trigger_databricks_bronze_ingest`: Databricks SDK Python task triggering the ingestion notebook and polling `life_cycle_state` until successful termination.
2. `clean_dbt_target`: Clears residual compilation targets to prevent cached artifacts from skewing builds.
3. `dbt_run_silver_technical`: Builds incremental silver tables.
4. `dbt_test_silver_technical`: Enforces generic schema validations.
5. `dbt_run_silver_business`: Compiles and updates the OBT layer.
6. `dbt_snapshot_gold`: Executes SCD Type 2 dimension snapshots.
7. `dbt_run_gold_fact`: Materializes reporting facts.

---

## Local Setup & Deployment

1. **Clone repository:**
   ```bash
   git clone [https://github.com/](https://github.com/)<YOUR_USERNAME>/<YOUR_REPO_NAME>.git
   cd <YOUR_REPO_NAME>
   ```

2. **Configure profiles and credentials:**
   * Place `profiles.yml` inside `walmart_project/` with your Databricks host, HTTP path, and personal access token.
   * Add `AIRFLOW_UID=50000` to `airflow/.env`.

3. **Spin up Airflow:**
   ```bash
   cd airflow
   docker compose up -d --build
   ```

4. **Access the Airflow UI:**
   * URL: `http://localhost:8080`
   * Unpause `walmart_lakehouse_orchestration` and trigger the DAG.
