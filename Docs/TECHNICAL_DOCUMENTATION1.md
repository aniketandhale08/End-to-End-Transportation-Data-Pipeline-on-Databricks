# Technical Documentation — GoodCabs Transportation Data Pipeline

This document is a deep-dive companion to the README, meant for **interview preparation** and for anyone who wants to understand exactly how the pipeline works internally.

---

## 1. Project Explanation

GoodCabs is a cab service operating across 10 Indian cities. The original data platform used procedural Spark jobs with manual orchestration — brittle, slow to change, and unable to give regional teams city-specific data on time.

This project rebuilds the platform using **Databricks LakeFlow Spark Declarative Pipelines (SDP)** — you declare *what* each table should contain, and Databricks figures out *how* and *in what order* to compute it, including incremental refresh and orchestration.

Two data sources are involved:
- **`city`** — a static dimension table (10 rows, city_id → city_name)
- **`trips`** — the fact data: one row per cab trip (trip_id, date, city_id, passenger_type, distance, fare, ratings). Delivered as 148 "full load" files plus incremental files dropped later to simulate daily arrivals.

---

## 2. Data Flow

```
S3 (raw CSVs)
   │
   ▼
Bronze  (raw ingestion + lineage columns)
   │
   ▼
Silver  (cleaned, validated, deduplicated via CDC)
   │
   ▼
Gold    (business-ready fact + per-city views)
```

Two distinct ingestion patterns are used side-by-side:
- `city` → **batch, full-refresh** (small, rarely-changing dimension)
- `trips` → **streaming, incremental** via Auto Loader (large, continuously-arriving fact data)

---

## 3. Pipeline Explanation (LakeFlow Declarative Pipelines / SDP)

Each table is defined as a Python function decorated with a pipeline decorator (`@dp.materialized_view`, `@dp.table`, `@dp.view`). Databricks:
1. Parses every table definition in the pipeline
2. Builds a dependency graph by looking at which tables each function *reads* (`spark.read.table(...)`)
3. Executes tables in the correct order automatically — no manual `orchestrate()` calls, no Airflow DAG needed
4. Refreshes each table incrementally where possible (streaming tables) or fully (materialized views)

This is what "declarative" means here: the code says *what* `transportation.silver.city` should look like, not *when or in what sequence* to run it relative to other tables.

---

## 4. Bronze Layer

### `bronze.city` (`src/bronze/city.py`)
- `@dp.materialized_view` — a **batch** table, fully recomputed on each pipeline run
- Reads all CSVs from `s3://goodcabs/data-store/city` with `inferSchema`, `PERMISSIVE` mode, and a `_corrupt_record` column to capture malformed rows
- Adds `file_name` (from `_metadata.file_path`) and `ingest_datetime` for traceability

### `bronze.trips` (`src/bronze/trips.py`)
- `@dp.table` — a **streaming table**, since it reads with `spark.readStream`
- Uses **Auto Loader** (`cloudFiles` format) to incrementally discover and ingest new CSV files from `s3://goodcabs/data-store/trips`
- Key Auto Loader options:
  - `cloudFiles.inferColumnTypes = true` — infers schema instead of forcing manual typing
  - `cloudFiles.schemaEvolutionMode = "rescue"` — new/unexpected columns are captured in a rescue column instead of failing the pipeline
  - `cloudFiles.maxFilesPerTrigger = 100` — caps how many new files are processed per micro-batch, useful for controlling load when the 148 full-load files first arrive
- Renames `distance_travelled(km)` → `distance_travelled_km` (parentheses in column names cause query errors downstream)
- Adds the same `file_name` / `ingest_datetime` lineage columns

Both tables set Delta table properties: `delta.enableChangeDataFeed`, `optimizeWrite`, and `autoCompact` — enabling downstream CDC and keeping small-file counts under control as incremental files land.

---

## 5. Silver Layer

### `silver.city` (`src/silver/city.py`)
- `@dp.materialized_view`, reads `bronze.city`
- Selects and renames columns to business-friendly names, adds `silver_processed_timestamp`

### `silver.calendar` (`src/silver/calendar.py`)
- A **generated**, not sourced, dimension table — built entirely from `sequence()` over a date range
- `start_date` / `end_date` are pulled from **pipeline configuration** (`spark.conf.get(...)`), so the date range is configurable per environment without changing code
- Derives 15+ standard date attributes: year, month, quarter, day-of-week, week-of-year, weekday/weekend flags
- Flags 3 hardcoded Indian national holidays for 2025 (Republic Day, Independence Day, Gandhi Jayanti) via `is_holiday` / `holiday_name`

### `silver.trips` (`src/silver/trips.py`)
This is the most complex table — a **streaming CDC pipeline**, built in three parts:

1. **Staging view** (`trips_silver_staging`, `@dp.view`) — reads `bronze.trips` as a stream, selects/renames columns (`id`, `business_date`, `city_id`, `passenger_category`, `distance_kms`, `sales_amt`, `passenger_rating`, `driver_rating`), and declares **data quality expectations**:
   ```python
   @dp.expect("valid_date", "year(business_date) >= 2020")
   @dp.expect("valid_driver_rating", "driver_rating BETWEEN 1 AND 10")
   @dp.expect("valid_passenger_rating", "passenger_rating BETWEEN 1 AND 10")
   ```
   These are declared with plain `@dp.expect` (not `expect_or_drop` / `expect_or_fail`), so **violating rows are still written through** — the pipeline only tracks and reports the violation counts. This is a deliberate "monitor first" choice; production hardening would upgrade these to `expect_or_drop`.

2. **Streaming target table** — `dp.create_streaming_table(name="transportation.silver.trips", ...)` declares the destination table that will hold the deduplicated, upserted trip records.

3. **CDC flow** — `dp.create_auto_cdc_flow(...)` wires the staging view to the target table:
   - `keys=["id"]` — trip_id is the natural key
   - `sequence_by=silver_processed_timestamp` — determines which version of a record "wins" if duplicates arrive
   - `stored_as_scd_type=1` — **overwrite** semantics: the latest record replaces the previous one, no history retained (as opposed to SCD Type 2, which would preserve every version)

---

## 6. SQL Transformations (Gold Layer)

### `gold.fact_trips` (`src/gold/trips_gold.sql`)
A `CREATE OR REPLACE VIEW` that joins:
- `silver.trips` (facts)
- `silver.city` (city dimension, for `city_name`)
- `silver.calendar` (date dimension, for month/week/quarter/holiday attributes)

This produces a single, denormalized, analytics-ready fact view — a classic **star schema** consumption pattern.

### `gold.fact_trips_<city>` (10 files)
Each is a one-line filter on top of `fact_trips`:
```sql
CREATE OR REPLACE VIEW transportation.gold.fact_trips_<city>
AS (SELECT * FROM transportation.gold.fact_trips WHERE city_id = '<code>');
```
This is the direct technical answer to GoodCabs' original business problem: instead of one generic dashboard, every region gets its own dedicated, always-current view.

| City | city_id |
|---|---|
| Jaipur | RJ01 |
| Lucknow | UP01 |
| Surat | GJ01 |
| Kochi | KL01 |
| Indore | MP01 |
| Chandigarh | CH01 |
| Vadodara | GJ02 |
| Visakhapatnam | AP01 |
| Coimbatore | TN01 |
| Mysore | KA01 |

---

## 7. Incremental Processing

- The `trips` table processes new files incrementally because it's declared as a **streaming table** fed by **Auto Loader**, which tracks which files it has already ingested (via its own checkpointing) so re-runs never reprocess old files
- The pipeline was tested end-to-end by uploading **148 "Full Load" files** first, then manually dropping **5 additional "incremental" files** into the same S3 path afterward, and confirming the new rows flowed through Bronze → Silver → Gold without reprocessing the original 148
- In **continuous** pipeline mode, Databricks watches the source location and triggers processing automatically as soon as a new file lands — this is what enables near-real-time updates without a fixed cron schedule

## 8. Auto Loader

Auto Loader (`cloudFiles` source) is Databricks' managed, scalable file-ingestion mechanism. In this project it provides:
- Automatic discovery of new files in the S3 path (no manual file-listing logic)
- Schema inference and evolution handling (`rescue` mode) so unexpected new columns don't break the pipeline
- Built-in exactly-once processing guarantees combined with Delta Lake

## 9. CDC (Change Data Capture)

CDC here refers to the **`silver.trips` upsert flow**: rather than blindly appending every incoming record (which could create duplicates if the same trip file were reprocessed, or if late-arriving corrections came through), `create_auto_cdc_flow` merges incoming records into the target table by key (`id`), keeping only the latest version per key (SCD Type 1).

## 10. Data Validation

Handled entirely through **declarative expectations** (`@dp.expect`) rather than manual `filter()`/`assert` statements. The pipeline UI surfaces pass/fail counts per expectation per run — giving built-in data quality observability without extra tooling.

## 11. Pipeline Execution

- **Setup step (once):** `setup/project_setup.ipynb` creates the `transportation` catalog and its `bronze`/`silver`/`gold` schemas via `dbutils.widgets` + `spark.sql(CREATE CATALOG/SCHEMA IF NOT EXISTS ...)`
- **Pipeline modes:**
  - *Triggered* — runs once when manually started or on a schedule; used for the initial full load
  - *Continuous* — pipeline stays running and reacts to new files automatically; used to validate incremental loads
- Pipeline configuration (`start_date`, `end_date`) is passed in via the pipeline settings, not hardcoded, so the calendar range is environment-configurable

## 12. Performance Considerations

- `delta.autoOptimize.optimizeWrite` and `autoCompact` are set on every table to avoid small-file problems — important given data arrives as many small CSV batches
- `maxFilesPerTrigger=100` bounds how much a single micro-batch processes, preventing one huge burst of files from overwhelming a cluster
- `delta.enableChangeDataFeed` is enabled on Bronze/Silver tables so downstream CDC and future auditing can read row-level change history efficiently instead of diffing full tables

## 13. Design Decisions

| Decision | Reasoning |
|---|---|
| `city` as materialized view, `trips` as streaming table | City is small and rarely changes (safe to fully recompute); trips is large and continuously growing (needs incremental processing) |
| SCD Type 1 for trips | Business only needs the latest state of a trip record — no requirement to track historical changes |
| Calendar generated in-pipeline, not sourced | Avoids depending on an external date file; single source of truth driven by pipeline config |
| One Gold view per city instead of one shared dashboard | Solves the actual business complaint (generic, unusable dashboards) with minimal SQL |
| Expectations without `_or_drop` | Chosen as an observability-first approach for this version of the pipeline — a known area for hardening (see Future Improvements) |
