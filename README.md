
# ✈️ Databricks + dbt Star Schema Pipeline

> End-to-end data engineering pipeline built on **Databricks Medallion Architecture** with dbt transformations, Delta Lake, and GitHub Actions CI/CD — processing 1,300+ flight booking records across 4 source entities.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                      │
│   dim_airports.csv  │  dim_flights.csv  │  dim_passengers.csv  │  fact_bookings.csv │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER  (Delta Lake)                           │
│                                                                          │
│   Databricks Autoloader  →  cloudFiles format  →  Delta Tables          │
│   Dynamic For Each Workflow  →  scales to 100+ sources                  │
│                                                                          │
│   /Volumes/workspace/bronzes/bronzevolume/{source}/data                 │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     SILVER LAYER  (DLT / Lakeflow)                      │
│                                                                          │
│   Lakeflow Declarative Pipelines  →  apply_changes()  →  SCD Type 1    │
│                                                                          │
│   silver_flights      ← CDC upsert on flight_id                        │
│   silver_passengers   ← CDC upsert on passenger_id                     │
│   silver_airports     ← CDC upsert on airport_id                       │
│   silver_bookings     ← append + quality rules (not null checks)       │
│   silver_business     ← joined snapshot view                           │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     GOLD LAYER  (Star Schema)                            │
│                                                                          │
│   PySpark MERGE  +  Surrogate Keys  +  Watermark Incremental            │
│                                                                          │
│   dim_flights      (Dimflightkey)     110 records                      │
│   dim_passengers   (Dimpassengerkey)  220 records                      │
│   dim_airports     (Dimairportkey)     55 records                      │
│   fact_bookings    → joins all 3 dims  1,300+ records                  │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     DBT LAYER  (Business Models)                         │
│                                                                          │
│   business_view       → Revenue by country                             │
│   passenger_revenue   → Bookings and spend per passenger               │
│   flight_revenue      → Revenue by airline route                       │
│   airport_revenue     → Revenue by airport and country                 │
│                                                                          │
│   ref() + source() lineage  │  Jinja templating  │  9 data tests       │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     CI/CD  (GitHub Actions)                              │
│                                                                          │
│   git push  →  GitHub Actions  →  dbt build  →  Green ✅ / Red ❌      │
│   Runs: install → connect → dbt run + dbt test → report                │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Category | Technology |
|----------|-----------|
| Platform | Databricks (Free Edition) |
| Storage | Delta Lake, Unity Catalog |
| Ingestion | Autoloader (cloudFiles), Dynamic For Each Workflows |
| Streaming | Lakeflow Declarative Pipelines (DLT) |
| Processing | PySpark, Spark SQL |
| Transformation | dbt (dbt-databricks 1.10.3) |
| CI/CD | GitHub Actions |
| Cloud | Azure (Databricks managed) |
| Catalog | Unity Catalog — 3-level namespace |

---

## 📁 Project Structure

```
databricks-dbt-star-schema-pipeline/
│
├── databricks/
│   ├── bronze/
│   │   ├── 01_src_parameter.ipynb      # builds src_array, sets task values
│   │   └── 02_bronze_autoloader.ipynb  # Autoloader worker notebook
│   ├── silver/
│   │   └── 03_silver.ipynb             # DLT pipeline (CDC + SCD Type 1)
│   ├── dlt/
│   │   └── 07_dlt_pipeline.py          # Lakeflow DLT pipeline code
│   ├── gold/
│   │   ├── 04_gold_dims.ipynb          # Dimension loader (surrogate keys + MERGE)
│   │   └── 05_gold_fact.ipynb          # Fact loader (dynamic joins + MERGE)
│   └── setup/
│       └── 06_setup.ipynb              # Unity Catalog setup
│
├── dbt/
│   ├── dbt_project.yml
│   └── models/
│       └── example/
│           ├── business_view.sql        # Revenue by country
│           ├── passenger_revenue.sql    # Spend per passenger
│           ├── flight_revenue.sql       # Revenue by airline route
│           ├── airport_revenue.sql      # Revenue by airport
│           ├── sources.yml              # Source table definitions
│           └── schema.yml              # Column tests
│
├── .github/
│   └── workflows/
│       └── dbt_ci.yml                  # GitHub Actions CI pipeline
│
└── README.md
```

---

## 🔄 Pipeline Layers — Deep Dive

### Bronze — Autoloader Ingestion
- **Pattern**: `cloudFiles` format with schema evolution (`rescue` mode)
- **Orchestration**: Dynamic For Each workflow — single parameter notebook builds `src_array`, For Each task scales to any number of sources without code changes
- **Output**: Append-only Delta tables in managed Unity Catalog volumes

### Silver — CDC with Lakeflow DLT
- **Pattern**: `dlt.apply_changes()` with `stored_as_scd_type=1`
- **Keys**: Natural business keys (flight_id, passenger_id, airport_id)
- **Quality**: `@dlt.expect_all()` rules enforcing not-null constraints on booking_id, passenger_id
- **Join**: `silver_business` — snapshot join across all 4 silver tables

### Gold — Star Schema with Surrogate Keys
- **Pattern**: High watermark incremental load + `DeltaTable.forName().merge()`
- **Surrogate keys**: `monotonically_increasing_id()` + `max(surrogate_key)` offset
- **Reusability**: Single parameterized notebook loads any dimension — no hardcoded schema logic
- **Fact table**: Dynamic dimension config — joins all 3 dims via surrogate keys

### dbt — Business Transformations
- **Materialization**: `table` — persisted in `workspace.golds`
- **Lineage**: `{{ source() }}` references tracked automatically in DAG
- **Tests**: 9 auto-generated tests from `schema.yml` (not_null, unique)
- **Docs**: Full column documentation + lineage graph via `dbt docs serve`

---

## 🔁 dbt Lineage Graph

```
fact_bookings ──┬──→ business_view
                ├──→ passenger_revenue
                ├──→ flight_revenue
                └──→ airport_revenue

dim_passengers ──→ passenger_revenue
dim_flights    ──→ flight_revenue
dim_airports   ──┬──→ airport_revenue
                 └──→ business_view
```

---

## ⚙️ CI/CD Pipeline

Every `git push` to `main` triggers GitHub Actions automatically:

```
git push origin main
       ↓
GitHub Actions spins up Ubuntu runner
       ↓
pip install dbt-databricks
       ↓
profiles.yml built from GitHub Secrets
       ↓
dbt build  (dbt run + dbt test)
       ↓
Green ✅ → deployed    Red ❌ → fix required
```

**Secrets used** (never hardcoded):
- `DATABRICKS_HOST`
- `DATABRICKS_HTTP_PATH`
- `DATABRICKS_TOKEN`

---

## 📊 Data Volume

| Layer | Table | Records |
|-------|-------|---------|
| Silver | silver_bookings | 1,300+ |
| Silver | silver_flights | 110 |
| Silver | silver_passengers | 220 |
| Silver | silver_airports | 55 |
| Gold | dim_flights | 110 |
| Gold | dim_passengers | 220 |
| Gold | dim_airports | 55 |
| Gold | fact_bookings | 1,300+ |

---

## 🚀 How to Run

### Prerequisites
- Databricks workspace with Unity Catalog
- dbt-databricks installed (`pip install dbt-databricks`)
- GitHub secrets configured

### Run Bronze Layer
```
Databricks → Jobs → Bronze Pipeline
→ Run Now
```

### Run Silver Layer (DLT)
```
Databricks → Jobs & Pipelines → DLT_SILVER_LAYER
→ Start
```

### Run Gold Layer
```
Databricks → Notebooks → gold/04_gold_dims.ipynb
Databricks → Notebooks → gold/05_gold_fact.ipynb
```

### Run dbt Models
```bash
cd dbt/
dbt run       # run all models
dbt test      # run all data quality tests
dbt build     # run + test together
```

### View dbt Docs
```bash
dbt docs generate
dbt docs serve
# open http://localhost:8080
```

---

## 👩‍💻 Author

**Himabindu** — Data Engineering  
MS Computer Science, Cleveland State University  
PG Program in Data Engineering, IIIT Bangalore  

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue)](https://linkedin.com/in/himabindu)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-black)](https://github.com/ugeebindu15)
