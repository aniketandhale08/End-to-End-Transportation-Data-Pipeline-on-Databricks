# 📚 Technical Documentation — GoodCabs Data Engineering Pipeline

> **Purpose:** Deep-dive technical reference for the GoodCabs Databricks project. Covers architecture decisions, layer-by-layer implementation, pipeline mechanics, and design rationale. Primarily for interview preparation and peer review.

---

## 1. Project Overview

**GoodCabs** is a simulated cab service operating across 10 Indian cities. The platform ingests daily trip export CSV files from AWS S3 into a Databricks Lakehouse using the **LakeFlow Spark Declarative Pipelines (SDP)** framework and a **Medallion Architecture** (Bronze → Silver → Gold).

### Catalog & Namespace

| Layer | Schema | Tables/Views |
|---|---|---|
| Bronze | `transportation.bronze` | `trips` (streaming), `city` (materialized view) |
| Silver | `transportation.silver` | `trips` (streaming+CDC), `city` (materialized view), `calendar` (materialized view) |
| Gold | `transportation.gold` | `fact_trips` (view) + 10 city-specific views |

---

## 2. Data Sources

### 2.1 Trip Data

- **Location:** `s3://goodcabs/data-store/trips/`
- **Format:** CSV with header
- **Schema:**

```
trip_id                  STRING
date                     STRING (cast to DATE in Silver)
city_id                  STRING
passenger_type           STRING (new / repeated)
distance_travelled(km)   DOUBLE → renamed in Bronze
fare_amount              LONG
passenger_rating         INTEGER (1–10)
driver_rating            INTEGER (1–10)
```

- **Volume:** 148 daily files (full load) + 5 incremental files = ~19.5 MB total
- **File naming pattern:** `trip_export_2025-MM-DD.csv`
- **Cities covered:** Jaipur, Lucknow, Surat, Kochi, Indore, Chandigarh, Vadodara, Visakhapatnam, Coimbatore, Mysore

### 2.2 City Data

- **Location:** `s3://goodcabs/data-store/city/`
- **Format:** CSV with header
- **Schema:**

```
city_id    STRING (e.g., RJ01, UP01, GJ01)
city_name  STRING (e.g., Jaipur, Lucknow, Surat)
```

- **Volume:** 10 rows, 165 bytes — static reference table

---

## 3. Data Flow

```
Phase 1: Ingestion
  S3/trips/ ──[Auto Loader cloudFiles]──► bronze.trips  (streaming delta table)
  S3/city/  ──[Batch CSV read]──────────► bronze.city   (materialized view)

Phase 2: Cleaning & Validation
  bronze.trips ──► trips_silver_staging (view) ──[CDC]──► silver.trips
  bronze.city  ──────────────────────────────────────────► silver.city
  [Config params] ────────────────────────────────────────► silver.calendar

Phase 3: Analytics
  silver.trips + silver.city + silver.calendar ──[JOIN]──► gold.fact_trips
  gold.fact_trips ──[WHERE city_id = 'XX01']──► gold.fact_trips_{city} × 10
```

---

## 4. Pipeline Architecture — LakeFlow SDP

### What is SDP?

LakeFlow Spark Declarative Pipelines (SDP) is a Databricks framework where you **declare what data you want** (using `@dp.materialized_view`, `@dp.table`, `@dp.view`) and SDP handles:
- Execution order (DAG resolution)
- Incremental processing
- Schema management
- Error handling

### Pipeline Name: `transportation_pipeline`

**Configured parameters:**

| Key | Value |
|---|---|
| `start_date` | `2025-01-01` |
| `end_date` | `2025-12-31` |

These are read at runtime in `calendar.py` via `spark.conf.get("start_date")`.

### Execution Mode
- `bronze.city` → **Full recompute** (static data, always refreshed)
- `silver.city` → **Full recompute** (depends on bronze.city)
- `silver.calendar` → **Full recompute** (deterministic, generated data)
- `bronze.trips` → **Incremental streaming** (only new S3 files processed)
- `silver.trips` → **Incremental CDC upsert** (only changed/new trip records)

---

## 5. Bronze Layer — Detailed Explanation

### 5.1 `bronze/trips.py` — Streaming Ingestion

```python
@dp.table(
    name="transportation.bronze.trips",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    }
)
def orders_bronze():
    df = spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger", 100)
        .load(SOURCE_PATH)

    df = df.withColumnRenamed("distance_travelled(km)", "distance_travelled_km")
    df = df.withColumn("file_name", col("_metadata.file_path"))
           .withColumn("ingest_datetime", current_timestamp())
    return df
```

**Key decisions explained:**

| Option | Value | Reason |
|---|---|---|
| `cloudFiles.format` | `csv` | Source files are CSV |
| `cloudFiles.inferColumnTypes` | `true` | Auto-detect numeric types |
| `cloudFiles.schemaEvolutionMode` | `rescue` | New columns go to `_rescued_data` — no pipeline failure |
| `cloudFiles.maxFilesPerTrigger` | `100` | Process up to 100 files per micro-batch |
| `withColumnRenamed` | special char fix | `distance_travelled(km)` breaks schema; renamed to `distance_travelled_km` |
| `_metadata.file_path` | audit trail | Records source file for each row |

### 5.2 `bronze/city.py` — Batch Ingestion

```python
@dp.materialized_view(name="transportation.bronze.city")
def city_bronze():
    df = spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(SOURCE_PATH)
    df = df.withColumn("file_name", col("_metadata.file_path"))
           .withColumn("ingest_datetime", current_timestamp())
    return df
```

Uses **PERMISSIVE mode** — corrupt records are captured in `_corrupt_record` column rather than failing the pipeline.

---

## 6. Silver Layer — Detailed Explanation

### 6.1 `silver/trips.py` — CDC with SCD Type 1

**Step 1: Staging view with data quality checks**

```python
@dp.view(name="trips_silver_staging")
@dp.expect("valid_date",             "year(business_date) >= 2020")
@dp.expect("valid_driver_rating",    "driver_rating BETWEEN 1 AND 10")
@dp.expect("valid_passenger_rating", "passenger_rating BETWEEN 1 AND 10")
def trips_silver():
    df_bronze = spark.readStream.table("transportation.bronze.trips")
    df_silver = df_bronze.select(
        col("trip_id").alias("id"),
        col("date").cast("date").alias("business_date"),
        col("city_id"),
        col("passenger_type").alias("passenger_category"),
        col("distance_travelled_km").alias("distance_kms"),
        col("fare_amount").alias("sales_amt"),
        col("passenger_rating"),
        col("driver_rating"),
        col("ingest_datetime").alias("bronze_ingest_timestamp"),
    )
    df_silver = df_silver.withColumn("silver_processed_timestamp", current_timestamp())
    return df_silver
```

**Step 2: Streaming table target + CDC flow**

```python
dp.create_streaming_table("transportation.silver.trips")

dp.create_auto_cdc_flow(
    target="transportation.silver.trips",
    source="trips_silver_staging",
    keys=["id"],                        # trip_id as primary key
    sequence_by=col("silver_processed_timestamp"),
    stored_as_scd_type=1,               # SCD Type 1: overwrites on update
)
```

**Why SCD Type 1?**
- Trip data is immutable once recorded (no historical changes expected)
- Simpler implementation — no need for `is_current`, `valid_from`, `valid_to` columns
- Re-processing the same file won't create duplicates

### 6.2 `silver/city.py` — Dimension Cleaning

Simple projection + timestamp addition. Takes `city_id`, `city_name` from bronze and adds `silver_processed_timestamp`.

### 6.3 `silver/calendar.py` — Calendar Dimension

**How it works:**

```python
start_date = spark.conf.get("start_date")  # From pipeline config: 2025-01-01
end_date   = spark.conf.get("end_date")    # From pipeline config: 2025-12-31

df = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{start_date}'),
        to_date('{end_date}'),
        interval 1 day
    )) as date
""")
```

This generates a row for every day in 2025 (365 rows), then derives 17 attributes:

| Attribute | Type | Example |
|---|---|---|
| `date` | DATE | 2025-08-01 |
| `date_key` | INT | 20250801 |
| `year` | INT | 2025 |
| `month` | INT | 8 |
| `quarter` | INT | 3 |
| `day_of_month` | INT | 1 |
| `day_of_week` | STRING | "Friday" |
| `day_of_week_abbr` | STRING | "Fri" |
| `day_of_week_num` | INT | 6 |
| `month_name` | STRING | "August" |
| `month_year` | STRING | "August 2025" |
| `quarter_year` | STRING | "Q3 2025" |
| `week_of_year` | INT | 31 |
| `day_of_year` | INT | 213 |
| `is_weekday` | BOOLEAN | true |
| `is_weekend` | BOOLEAN | false |
| `is_holiday` | BOOLEAN | false |
| `holiday_name` | STRING | null / "Republic Day" |

**Indian holidays mapped:**
- January 26 → Republic Day
- August 15 → Independence Day
- October 2 → Gandhi Jayanti

---

## 7. Gold Layer — Detailed Explanation

### 7.1 `trips_gold.sql` — Master Fact View

```sql
CREATE OR REPLACE VIEW transportation.gold.fact_trips AS (
    SELECT
        t.id,
        t.business_date,
        t.city_id,
        c.city_name,
        t.passenger_category,
        t.distance_kms,
        t.sales_amt,
        t.passenger_rating,
        t.driver_rating,
        ca.month,
        ca.day_of_month,
        ca.day_of_week,
        ca.month_name,
        ca.month_year,
        ca.quarter,
        ca.quarter_year,
        ca.week_of_year,
        ca.is_weekday,
        ca.is_weekend,
        ca.is_holiday as national_holiday
    FROM transportation.silver.trips t
    JOIN transportation.silver.city c     ON t.city_id = c.city_id
    JOIN transportation.silver.calendar ca ON t.business_date = ca.date
);
```

This is a **view** (not a table) — it reads from Silver in real-time. Always reflects the latest Silver state.

### 7.2 City-Specific Views

Each city gets a dedicated Gold view:

```sql
-- Example: Chandigarh (CH01)
CREATE OR REPLACE VIEW transportation.gold.fact_trips_chandigarh AS (
    SELECT * FROM transportation.gold.fact_trips WHERE city_id = 'CH01'
);
```

**City ID Mapping:**

| City | city_id | Gold View |
|---|---|---|
| Jaipur | RJ01 | `fact_trips_jaipur` |
| Lucknow | UP01 | `fact_trips_lucknow` |
| Surat | GJ01 | `fact_trips_surat` |
| Kochi | KL01 | `fact_trips_kochi` |
| Indore | MP01 | `fact_trips_indore` |
| Chandigarh | CH01 | `fact_trips_chandigarh` |
| Vadodara | GJ02 | `fact_trips_vadodara` |
| Visakhapatnam | AP01 | `fact_trips_visakhapatnam` |
| Coimbatore | TN01 | `fact_trips_coimbatore` |
| Mysore | KA01 | `fact_trips_mysore` |

---

## 8. Incremental Processing — Auto Loader Deep Dive

### How Auto Loader Works

1. **First run:** Scans all existing files in S3 path, processes them, writes checkpoint
2. **Subsequent runs:** Reads checkpoint, identifies only new files since last checkpoint, processes those
3. **Guarantee:** Exactly-once processing — no re-processing of already-ingested files

### Simulation Approach

Since a real production DB was not available:
- **148 CSV files** uploaded to `s3://goodcabs/data-store/trips/` as the initial full load
- **5 additional CSV files** manually uploaded post-pipeline-run to test incremental triggering
- Pipeline was re-run → Auto Loader detected and processed only the 5 new files

### Key Auto Loader Options Used

| Option | Value | Effect |
|---|---|---|
| `cloudFiles.format` | `csv` | Read CSV files |
| `cloudFiles.inferColumnTypes` | `true` | Infer types from data |
| `cloudFiles.schemaEvolutionMode` | `rescue` | New columns → `_rescued_data` |
| `cloudFiles.maxFilesPerTrigger` | `100` | Process ≤100 files per batch |

---

## 9. Change Data Capture (CDC)

### Why CDC for Trips?

Trip data could be re-exported with corrections. Without CDC:
- Re-running the pipeline would create duplicate trip records
- No way to handle updates to existing trips

### How `create_auto_cdc_flow` Works

```
Source: trips_silver_staging (streaming view)
Target: silver.trips (streaming Delta table)
Key: ["id"] (trip_id)
Sequence: silver_processed_timestamp
SCD Type: 1
```

**Behavior:**
- **New `id`:** INSERT into silver.trips
- **Existing `id`, newer timestamp:** UPDATE silver.trips (overwrites old values)
- **Existing `id`, older timestamp:** IGNORE (stale record, not applied)

This is implemented as a **MERGE** operation on the Delta table internally.

---

## 10. Data Validation — `@dp.expect()` Decorators

Three quality rules are applied on `trips_silver_staging`:

```python
@dp.expect("valid_date",             "year(business_date) >= 2020")
@dp.expect("valid_driver_rating",    "driver_rating BETWEEN 1 AND 10")
@dp.expect("valid_passenger_rating", "passenger_rating BETWEEN 1 AND 10")
```

**Default behavior (`@dp.expect`):** Rows violating the rule are still written to the target, but the violation is recorded in pipeline metrics. This is a **soft constraint** — data is not dropped.

**Alternative options available:**
- `@dp.expect_or_drop()` — drop violating rows
- `@dp.expect_or_fail()` — fail the entire pipeline

**Why soft constraints were chosen:**
- Preserves all data (no silent data loss)
- Violations are visible in pipeline monitoring
- Allows investigating bad data without re-ingesting

---

## 11. Pipeline Execution Flow

### DAG Execution Order (resolved automatically by SDP)

```
1. bronze.city                (Materialized View — batch)
2. bronze.trips               (Streaming Table — Auto Loader)
3. silver.city                (Materialized View — from bronze.city)
4. silver.calendar            (Materialized View — from config params)
5. trips_silver_staging       (View — from bronze.trips stream)
6. silver.trips               (Streaming Table — CDC from staging)
7. gold.fact_trips            (SQL View — joins silver.trips + city + calendar)
8. gold.fact_trips_{city} × 10 (SQL Views — filtered from fact_trips)
```

### Run Statistics (from pipeline screenshots)
- Total run time: ~17–18 minutes for full load
- `bronze.trips`: 10K output records
- `silver.trips`: 10K upserted records
- `silver.calendar`: 365 records (full year 2025)
- `bronze.city` / `silver.city`: 10 records each

---

## 12. Performance Considerations

### Delta Lake Optimizations Applied

| Property | Effect |
|---|---|
| `delta.autoOptimize.optimizeWrite` | Automatically optimizes file sizes during writes |
| `delta.autoOptimize.autoCompact` | Automatically compacts small files after writes |
| `delta.enableChangeDataFeed` | Enables downstream CDC consumers |

### Auto Loader Performance

- `cloudFiles.maxFilesPerTrigger: 100` limits micro-batch size to prevent OOM
- `cloudFiles.schemaEvolutionMode: rescue` avoids full recompute on schema changes
- Checkpoint location persisted in DBFS — enables resume after failure

### Serverless Compute

- Pipeline runs on **Serverless Starter Warehouse (2X-Small)**
- No cluster configuration needed — Databricks manages compute automatically
- Cost-efficient for development/learning environments

---

## 13. Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Processing framework | LakeFlow SDP | Declarative, auto-orchestrated, no boilerplate code |
| Storage format | Delta Lake | ACID, CDC support, time travel, schema evolution |
| Ingestion method | Auto Loader | Scalable incremental ingestion from S3 |
| CDC strategy | SCD Type 1 | Trips are immutable; historical tracking not required |
| Calendar generation | Programmatic (Spark SQL sequence) | No external calendar dependency |
| Gold layer type | SQL Views (not tables) | Always fresh; no additional compute/storage for Gold |
| City views strategy | One view per city | Regional teams get pre-filtered, dedicated datasets |
| Data quality | `@dp.expect` (soft) | Preserve all data; flag issues without dropping records |
| Compute | Serverless | Zero cluster management; free-tier compatible |

---

## 14. Unity Catalog Setup

The `project_setup.ipynb` notebook creates the catalog and schemas:

```python
# Widget for catalog name (default: "transportation")
catalog_name = dbutils.widgets.get("catalog_name")

# Create catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")

# Create schemas (layers)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold")
```

This enables the three-tier namespace: `transportation.bronze.trips`, `transportation.silver.trips`, `transportation.gold.fact_trips`.

---

## 15. Databricks Genie (AI Analytics)

After the pipeline runs:
- Open `transportation.gold.fact_trips` in Unity Catalog
- Click **"Sample Data"** → **"Ask + Run"**
- Genie generates SQL from natural language questions

**Sample questions Genie can answer:**
- "What is the average driver rating by city?"
- "How do passenger ratings vary by weekday vs weekend?"
- "What is the total distance traveled each month?"
- "Which city has the highest average fare per trip?"
- "What are the top 3 cities by total revenue?"

---

*End of Technical Documentation*
