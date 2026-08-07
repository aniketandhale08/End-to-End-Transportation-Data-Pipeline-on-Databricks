# 🚖 GoodCabs — Interview Preparation Guide

### Q1. Walk me through this project in brief.

I built an end-to-end data pipeline for GoodCabs — a cab service operating across 10 Indian cities. The business problem was that regional managers weren't getting timely, city-specific data. They had one shared generic dashboard and had to manually rework exports every time they needed city-level insights.

I built a **Medallion Architecture (Bronze → Silver → Gold)** on **Databricks**. Raw trip CSV files land on **AWS S3**. **Auto Loader** picks them up automatically, the data is cleaned and validated in Silver, and 10 city-specific Gold views are served — one per region — so each team gets exactly the data they need, always up to date, no manual work.

---

### Q2. What was the exact business problem you were solving?

Three problems:
- Regional managers weren't getting trip data **on time**
- There was **one generic dashboard** — each city team had to manually filter their own data out of it
- The old pipeline was tightly coupled procedural code — hard to extend, hard to maintain

**My solution:** A declarative, event-driven pipeline that auto-ingests new trip data the moment it lands in S3, cleans it once in Silver, and serves 10 pre-filtered, always-current Gold views — one per city — with zero manual work.

---

### Q3. What is Medallion Architecture?

Medallion Architecture is a layered data organization pattern with three zones:

- **Bronze** — raw data as-is from the source. No major changes, just ingestion + metadata columns. Think of it as your backup copy of everything.
- **Silver** — cleaned, validated, and structured data. Business logic is applied here — renaming, type casting, deduplication, quality checks.
- **Gold** — analytics-ready data shaped for the business — joined, pre-filtered, and named for the end user to query directly.

The idea: each layer adds more value, and you can always re-derive downstream layers from raw Bronze if something breaks.

---

### Q4. Walk me through your overall architecture.

- **Source:** Trip CSV files are generated from the operational DB and exported to AWS S3
- **Bronze:** Auto Loader reads only new CSV files from S3 into a streaming Delta table. City data is batch-read as a materialized view.
- **Silver:** Trips are cleaned, validated with quality expectations, and CDC-upserted into a Silver streaming table using SCD Type 1. City and Calendar are materialized views.
- **Gold:** A SQL `fact_trips` view joins Silver trips + city + calendar. Ten city-specific views filter fact_trips by city_id.
- **Consumption:** Regional teams query city-specific Gold views. Databricks Genie provides natural language querying.

---

### Q5. Why Medallion Architecture — why not just load data directly into one table?

- **Bronze** preserves raw data — a safety net. If downstream logic ever breaks, you can re-derive everything from scratch without going back to the source.
- **Silver** is the single place where cleaning happens — no consumer team re-does it themselves.
- **Gold** is business-ready, decoupling "how data was cleaned" from "how it's queried."
- Separating layers also makes **debugging easy** — if Gold data looks wrong, check Silver; if Silver is wrong, check Bronze.


### Q6. What is LakeFlow Spark Declarative Pipelines (SDP)?

SDP is Databricks' pipeline framework where you **declare what data you want**, not step-by-step how to compute it. You write Python functions decorated with `@dp.materialized_view`, `@dp.table`, or `@dp.view`, and SDP automatically:
- Figures out the execution order by reading table dependencies
- Handles incremental processing
- Manages schema evolution
- Tracks data quality metrics

Instead of writing "run step A, then step B, then step C," you just say "this table reads from that table" — and Databricks builds the DAG and runs everything in the right order automatically.

**Example:**

Suppose a company receives sales data every day from a cloud storage location. We want to build a pipeline with three layers: Bronze → Silver → Gold. Bronze stores the raw sales data, Silver cleans the data by removing invalid records, and Gold calculates daily sales by region.

In a traditional Spark setup, I would write the Spark transformation code and then separately configure jobs, schedules, task dependencies, retries, and execution order. For example, I would configure Silver to run after Bronze and Gold to run after Silver.

With SDP, I can declare these datasets using Python or SQL. For example:
```python
@dp.materialized_view
def bronze():
    return spark.read.table("raw_sales")

@dp.materialized_view
def silver():
    return spark.read.table("bronze").filter("amount > 0")

@dp.materialized_view
def gold():
    return spark.read.table("silver") \
        .groupBy("region") \
        .sum("amount")
```
Here, I am mainly telling Databricks what each dataset should contain. SDP understands that Silver depends on Bronze and Gold depends on Silver, so it builds the dependency graph and manages the pipeline execution.

---

### Q7. Why Declarative Pipelines instead of plain PySpark jobs?

With plain PySpark, you manually manage execution order, checkpoints, watermarks, error handling, and retries — that's a lot of boilerplate code. With SDP:
- **DAG is auto-resolved** — Databricks figures out the correct order from the table dependencies you declare
- **Built-in incremental processing** for streaming tables — no hand-written checkpoint logic
- **Data quality tracking** via `@dp.expect` is built in — no custom logging
- **Pipeline lineage and observability** come for free in the Databricks UI


*DAG (Directed Acyclic Graph)*: A DAG (Directed Acyclic Graph) is a diagram or structure that shows tasks and their dependencies in a pipeline. It defines which task runs first and which task runs next, with a clear direction and no circular dependencies. It helps the system understand the correct execution order of tasks.

---

**What is Data Lake?**

A Data Lake is a central storage system used to store large amounts of raw data in its original format. It can store structured, semi-structured, and unstructured data, such as CSV, JSON, Parquet files, images, and logs. Data can come from databases, applications, APIs, and other sources. The data is usually stored in cloud storage like Amazon S3, Azure Data Lake Storage, or Google Cloud Storage. It is mainly used for data processing, analytics, and machine learning.

### Q8. What is Delta Lake?

Delta Lake is an open-source data management/storage layer that sits on top of a data lake and Parquet files. It makes the data lake more reliable by adding database-like features such as ACID transactions, schema enforcement, schema evolution, and MERGE/UPSERT operations. It uses a transaction log (_delta_log) to track changes and versions of the data. It also provides Time Travel to query historical versions and Change Data Feed (CDF) to track which rows were changed. In short, Delta Lake turns a basic data lake into a reliable and transactional data storage system.

---

### Q9. What is Parquet?

**A:** Parquet is a columnar file format used to store large datasets efficiently — it compresses data well and is fast to read for analytics. But it's **just a file format** — no ACID, no MERGE, no schema enforcement. You can read it and write it, but you can't update a row in place or roll back a bad write.

---

### Q10. Why Delta Lake over plain Parquet?

Delta Lake gives you everything Parquet doesn't:

| Feature | Delta Lake | Plain Parquet |
|---|---|---|
| ACID transactions | ✅ | ❌ |
| MERGE / UPSERT | ✅ | ❌ |
| Schema enforcement | ✅ | ❌ |
| Time travel | ✅ | ❌ |
| Change Data Feed | ✅ | ❌ |
| Streaming + batch unified | ✅ | ❌ |

In this project specifically, Delta enables:
1. CDC upserts on `silver.trips` (MERGE under the hood)
2. Exactly-once streaming guarantees
3. Auto-compaction for small file management
4. CDF so Silver can read only what changed in Bronze

---

### Q11. What is Auto Loader?

Auto Loader is a Databricks feature, used through the cloudFiles format, to automatically and incrementally ingest new files from cloud storage such as S3, ADLS, or GCS. It processes only the files it has not seen before and maintains a checkpoint to track processed files. If the pipeline fails and restarts, it can resume from where it stopped instead of processing everything again. It also supports schema inference and schema evolution and provides exactly-once file ingestion without requiring custom file-tracking logic.

---

### Q12. Why Auto Loader instead of a scheduled batch job that reads all files?

- A batch job would re-read **all** files every time — wasteful and slow as data grows
- Auto Loader tracks which files it already processed (via checkpoint) and reads **only the new ones**
- It scales automatically — no code changes if you go from 10 files to 10,000
- `cloudFiles.maxFilesPerTrigger: 100` controls how many files are processed per micro-batch, preventing memory overload

---

### Q13. How does Auto Loader know which files are new?

**A:** It maintains a checkpoint directory (stored in DBFS or cloud storage). Every time it runs:
1. Lists all files currently in the S3 path
2. Compares against its checkpoint log (which stores file paths + ETags — a content hash)
3. Processes only the files **not yet in the checkpoint**
4. Updates the checkpoint after successful processing

For very large buckets, it can also switch to **file notification mode** using AWS SQS/SNS — S3 sends a notification when a new file arrives, so no directory listing needed.

---

### Q14. What is Unity Catalog?

Unity Catalog is a centralized governance and security solution in Databricks used to manage and control access to data and AI assets. It provides a central metastore/catalog where you can organize catalogs, schemas, tables, views, and volumes. It allows you to define permissions and access controls so users can access only the data they are authorized to use. It also provides data lineage, auditing, and data discovery, making it easier to understand where data comes from, how it is used, and who accessed it.

 It provides a three-level namespace: `catalog.schema.table`. In this project, everything lives under the `transportation` catalog:
- `transportation.bronze.trips`
- `transportation.silver.trips`
- `transportation.gold.fact_trips`

It also handles **access control** — you can grant a regional team access to only their city's Gold view, not the entire catalog.

---

### Q15. Define CDC and CDF.

**What is CDC (Change Data Capture)?**

CDC is a process used to identify and capture changes made to data in a source system. It tracks operations such as INSERT, UPDATE, and DELETE, so downstream systems can process only the changed data instead of processing the entire dataset again. For example, if 10 rows are updated in a table containing 1 million rows, CDC can capture those 10 changed rows.

I implemented it using `dp.create_auto_cdc_flow()`:
```python
dp.create_auto_cdc_flow(
    target="transportation.silver.trips",
    source="trips_silver_staging",   # the staging view
    keys=["id"],                     # trip_id is the primary key
    sequence_by=F.col("silver_processed_timestamp"),
    stored_as_scd_type=1,
)
```
- **New `id`** → INSERT into silver.trips
- **Same `id`, newer timestamp** → UPDATE (overwrite)
- **Same `id`, older timestamp** → IGNORE (stale record)

Internally this runs as a MERGE operation on the Delta table.

**What is CDF (Change Data Feed)?**

CDF (Change Data Feed) is a Delta Lake feature that records the row-level changes made to a Delta table. It can show whether a row was inserted, updated, or deleted, along with the old and new values for updates. This allows downstream pipelines to easily consume only the changed records.

CDF is a Delta Lake feature that tracks every INSERT, UPDATE, and DELETE that happens on a Delta table. When enabled (`delta.enableChangeDataFeed: true`), downstream consumers can read **only the rows that changed** since their last read — instead of reading the full table every time.

In this project, it's enabled on all Bronze tables so the Silver layer can process only new/changed records — true incremental processing. If I added a reporting table downstream, it could also read only the delta from Silver efficiently.


## Part 4: Bronze Layer

### Q16. What happens in the Bronze Layer?

**A:** Bronze is the **raw ingestion layer** — data comes in almost exactly as-is from S3 with minimal transformation. Two tables:

- **`bronze.trips`** (Streaming Table) — Auto Loader reads new CSV files from S3. Only changes made here: rename `distance_travelled(km)` → `distance_travelled_km` (Spark can't handle parentheses in column names downstream), and add `file_name` + `ingest_datetime` as audit/lineage metadata columns.

- **`bronze.city`** (Materialized View) — batch-read from a single `city.csv` file (10 rows, static reference data). Fully recomputed on every pipeline run.

Both tables have CDF, auto-optimize, and auto-compact enabled.

---

### Q17. Why Auto Loader for trips but a plain batch read for city?

Trips is large and constantly growing — new files land every day. Auto Loader handles that incrementally without reprocessing old files. City is a tiny 10-row reference table that rarely changes — a full batch reload is simple, fast, and cheap. No need for incremental logic on something that small.

---

### Q18. What is `cloudFiles.schemaEvolutionMode: rescue`? Why did you use it?

`rescue` is an **Auto Loader schema evolution mode** that prevents the pipeline from failing when incoming data contains **unexpected columns or data types**. Instead of creating new columns, it stores the unexpected data in a special column called **`_rescued_data`**. This makes the pipeline more **fault-tolerant** while preserving unexpected data for later analysis.

It controls what happens when a new file contains a column that **didn't exist before in the schema**:

- **`rescue`** — keeps the unexpected data in **`_rescued_data`** without changing the existing schema or failing the pipeline.
- **`addNewColumns`** — **automatically adds the new column** to the schema.
- **`failOnNewColumns`** — **fails/stops the pipeline** when a new column is detected.

**Why use `rescue`?** It is safer when you want to **keep the pipeline running while preserving unexpected data** for later investigation.


## Part 5: Silver Layer


### Q19. What happens in the Silver Layer?

Silver is where data gets **cleaned, validated, and prepared** for analytics. Three tables:

- **`silver.trips`** — cleaned via a staging view, data quality checks applied, then CDC-upserted (SCD Type 1). Column names renamed for business clarity.
- **`silver.city`** — simple projection of city dimension with a timestamp added.
- **`silver.calendar`** — 365-row date dimension generated programmatically for full year 2025 (17 date attributes + 3 Indian national holidays).

---

### Q21. What is SCD? What is the difference between SCD Type 1 and Type 2?

### What is SCD?

**SCD (Slowly Changing Dimension)** is a data warehouse technique used to **manage changes in dimension data over time**. **SCD Type 1** overwrites the old value with the new value, so **no history is maintained**. **SCD Type 2** maintains history by keeping the old record and **creating a new record** when a value changes. In Type 2, we usually add **`start_date`**, **`end_date`**, and **`is_current`** columns to track when each version of the record was active. When a change occurs, the old record is marked as **inactive** by setting its `end_date` and `is_current = false`, and a new record is inserted with the new value, a new `start_date`, `end_date = NULL`, and `is_current = true`.

> **Type 1 = Overwrite → No history**  
> **Type 2 = Expire old record + Insert new record → History maintained**
---

### Q22. Why did you choose SCD Type 1 for trips?

The business need is "give me the **current state** of each trip" — not "show me how a trip record changed over time." Trip data is immutable once recorded; there's no business requirement to track its history. SCD Type 1 keeps the model simple, the table small, and deduplication straightforward. If GoodCabs ever needed an audit trail of trip corrections, I'd switch to `stored_as_scd_type=2`.


**What is a Materialized View?**

A Materialized View is a database object that stores the result of a SQL query physically, similar to a table. Unlike a normal view, it does not calculate the query result from scratch every time you query it. It can be refreshed when the underlying data changes. It is mainly used to improve query performance for complex queries and aggregations. In simple words, Materialized View = stored query result that can be queried like a table.

---

### Q23. How do you handle data quality in this pipeline?

Using `@dp.expect` decorators on the Silver staging view:
```python
@dp.expect("valid_date",             "year(business_date) >= 2020")
@dp.expect("valid_driver_rating",    "driver_rating BETWEEN 1 AND 10")
@dp.expect("valid_passenger_rating", "passenger_rating BETWEEN 1 AND 10")
```
These are **soft expectations** — rows that violate a rule are still written through, but the violation count is recorded and visible in the pipeline monitoring UI. This means no silent data loss — all data is preserved.

**Alternatives available:**
- `@dp.expect_or_drop()` — drops the violating rows entirely
- `@dp.expect_or_fail()` — fails the entire pipeline if any row violates

I chose soft expectations as an "observe first" approach — see what's happening in the data before taking destructive action.

---

### Q24. What is the Calendar Dimension and how did you generate it?

It's a date reference table with 365 rows — one per day in 2025. I generated it **programmatically** inside the pipeline using Spark SQL's `sequence()` function:
```python
start_date = spark.conf.get("start_date")  # pulled from pipeline config: 2025-01-01
end_date   = spark.conf.get("end_date")    # pulled from pipeline config: 2025-12-31

df = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{start_date}'),
        to_date('{end_date}'),
        interval 1 day
    )) as date
""")
```
From that date column, I derived **17 attributes**: year, month, quarter, day_of_week, day_of_week_abbr, month_name, month_year, quarter_year, week_of_year, day_of_year, is_weekday, is_weekend, is_holiday, holiday_name.

**Indian holidays hardcoded:** Republic Day (Jan 26), Independence Day (Aug 15), Gandhi Jayanti (Oct 2).

**Why generated in code?** No external file dependency. The date range is configurable via pipeline parameters — same code works for any year without touching the code.


## Part 6: Gold Layer

### Q25. What happens in the Gold Layer?

**A:** Gold is the **analytics-ready layer** — data shaped exactly for business consumption. Two types:

1. **`fact_trips`** — a SQL view that joins `silver.trips` + `silver.city` + `silver.calendar` on `city_id` and `business_date`. This is the master fact table with all columns a business analyst needs.

2. **10 city-specific views** — each one is `SELECT * FROM fact_trips WHERE city_id = 'XX01'`. One per city, so each regional team queries only their own data without needing to know city_id codes.

---

### Q26. Why SQL Views for Gold instead of physical tables?

**A:** Views always reflect the **latest Silver state in real-time** — no additional pipeline run needed to refresh Gold. When Silver updates, Gold is automatically up to date the next time it's queried. This also means zero additional compute or storage cost for maintaining Gold data.

---

### Q27. Why create 10 separate city views instead of one view that everyone queries with a filter?

**A:**
- Regional teams get **pre-filtered, dedicated datasets** — they don't need to know city_id codes or add WHERE clauses themselves
- Each team's dataset is isolated and easier to grant permissions to in Unity Catalog
- It directly solves the original business complaint: each regional manager sees only their city's data
- Simpler queries for the end user


## Part 7: Data

### Q28. What data did you work with?

**A:** Two datasets from AWS S3:

**1. Trips data** — `s3://goodcabs/data-store/trips/`
- 148 daily CSV files (one per day, Jan–May 2025) + 5 incremental test files
- Columns: `trip_id`, `date`, `city_id`, `passenger_type`, `distance_travelled(km)`, `fare_amount`, `passenger_rating`, `driver_rating`
- Total: ~19.5 MB
- Cities: Jaipur, Lucknow, Surat, Kochi, Indore, Chandigarh, Vadodara, Visakhapatnam, Coimbatore, Mysore

**2. City data** — `s3://goodcabs/data-store/city/`
- Single `city.csv` file, 10 rows
- Columns: `city_id` (e.g., RJ01, UP01, GJ01), `city_name`

---

### Q29. What was the total data volume and record count?

**A:** 148 + 5 = 153 CSV files, ~19.5 MB total. After full pipeline run: **366143 trip records** in Bronze and Silver, **365 calendar records**, **10 city records**. Full pipeline run time: ~17–18 minutes.


## Part 8: Code Overview


### Q30. Show me what the Bronze trips ingestion code looks like.

**A:** Here's the actual code from `bronze/trips.py`:
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
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger", 100)
        .load(SOURCE_PATH)
    )
    # Fix column name with special character — parentheses break downstream references
    df = df.withColumnRenamed("distance_travelled(km)", "distance_travelled_km")

    # Add audit/lineage metadata
    df = df.withColumn("file_name", F.col("_metadata.file_path")) \
           .withColumn("ingest_datetime", F.current_timestamp())
    return df
```
**Key points to remember:**
- `spark.readStream` + `cloudFiles` = Auto Loader (streaming, incremental)
- `@dp.table` = streaming Delta table
- `withColumnRenamed` fixes the parentheses problem in the column name
- `_metadata.file_path` = built-in Spark metadata to capture the source file name per row

---

### Q31. What does the Bronze city code look like?

**A:** Here's `bronze/city.py` — it's a batch read, not streaming:
```python
@dp.materialized_view(name="transportation.bronze.city")
def city_bronze():
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .load(SOURCE_PATH)
    )
    df = df.withColumn("file_name", col("_metadata.file_path")) \
           .withColumn("ingest_datetime", current_timestamp())
    return df
```
**Key points:**
- `spark.read` (not `readStream`) = batch read, fully recomputed each run
- `@dp.materialized_view` = batch table
- `PERMISSIVE` mode + `_corrupt_record` = corrupt rows are captured in a separate column, not dropped silently

---

### Q32. What key transformations happen in Silver trips?

**A:** From `silver/trips.py` — two steps:

**Step 1 — Staging view** (cleaning + quality checks):
```python
@dp.view(name="trips_silver_staging")
@dp.expect("valid_date",             "year(business_date) >= 2020")
@dp.expect("valid_driver_rating",    "driver_rating BETWEEN 1 AND 10")
@dp.expect("valid_passenger_rating", "passenger_rating BETWEEN 1 AND 10")
def trips_silver():
    df_bronze = spark.readStream.table("transportation.bronze.trips")
    df_silver = df_bronze.select(
        F.col("trip_id").alias("id"),
        F.col("date").cast("date").alias("business_date"),   # string → DATE type
        F.col("city_id"),
        F.col("passenger_type").alias("passenger_category"),
        F.col("distance_travelled_km").alias("distance_kms"),
        F.col("fare_amount").alias("sales_amt"),
        F.col("passenger_rating"),
        F.col("driver_rating"),
        F.col("ingest_datetime").alias("bronze_ingest_timestamp"),
    )
    df_silver = df_silver.withColumn("silver_processed_timestamp", F.current_timestamp())
    return df_silver
```

**Step 2 — CDC upsert into target table:**
```python
dp.create_streaming_table("transportation.silver.trips", ...)

dp.create_auto_cdc_flow(
    target="transportation.silver.trips",
    source="trips_silver_staging",
    keys=["id"],
    sequence_by=F.col("silver_processed_timestamp"),
    stored_as_scd_type=1,
)
```
**Key points:** column renames for business clarity, string-to-date type cast, 3 quality expectations, then CDC upsert using `id` as the primary key.

---

### Q33. What does the Gold `fact_trips` view look like?

**A:** From `gold/trips_gold.sql` — a three-table JOIN:
```sql
CREATE OR REPLACE VIEW transportation.gold.fact_trips AS (
    SELECT
        t.id, t.business_date, t.city_id, c.city_name,
        t.passenger_category, t.distance_kms, t.sales_amt,
        t.passenger_rating, t.driver_rating,
        ca.month, ca.day_of_month, ca.day_of_week,
        ca.month_name, ca.month_year,
        ca.quarter, ca.quarter_year, ca.week_of_year,
        ca.is_weekday, ca.is_weekend,
        ca.is_holiday AS national_holiday
    FROM transportation.silver.trips t
    JOIN transportation.silver.city c      ON t.city_id = c.city_id
    JOIN transportation.silver.calendar ca ON t.business_date = ca.date
);
```
**Key points:** `t` = trips (facts), `c` = city dimension, `ca` = calendar dimension. Classic **star schema** pattern — one fact table joined with two dimension tables.

---

### Q34. What do the city-specific Gold views look like?

**A:** One-liner per city:
```sql
-- Example: Chandigarh (city_id = CH01)
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


## Part 9: Performance & Design Decisions

### Q35. What performance optimizations did you apply?

- **`delta.autoOptimize.optimizeWrite`** — automatically optimizes file sizes during writes. Prevents the "too many small files" problem that slows down queries.
- **`delta.autoOptimize.autoCompact`** — automatically merges small files after writes. Important because data arrives as many small CSV batches.
- **`delta.enableChangeDataFeed`** — enables incremental downstream processing — consumers read only what changed.
- **`cloudFiles.maxFilesPerTrigger: 100`** — limits each micro-batch to 100 files max, preventing memory overload when the 148 full-load files first arrive.
- **Serverless Compute** — Databricks manages cluster scaling automatically. No manual cluster config needed.

---

### Q36. What were your key design decisions and why?

| Decision | Choice | Why |
|---|---|---|
| Pipeline framework | LakeFlow SDP | Declarative, auto-orchestrated, no boilerplate |
| Storage format | Delta Lake | ACID, CDC support, time travel, schema evolution |
| Trips ingestion | Auto Loader (streaming) | Incremental, exactly-once, scalable |
| City ingestion | Batch read (materialized view) | Small static table — full reload is cheap |
| CDC strategy | SCD Type 1 | Trips are immutable; history not needed |
| Calendar | Programmatic (Spark SQL sequence) | No external file dependency, configurable date range |
| Gold layer type | SQL Views (not tables) | Always fresh, zero storage/compute overhead |
| City isolation | One view per city | Pre-filtered data for each regional team |
| Data quality | `@dp.expect` (soft) | Preserve all data; flag issues without dropping |
| Compute | Serverless | Free-tier compatible, zero cluster management |

---

### Q37. What is the Pipeline DAG execution order?

**A:** SDP resolves this automatically from the table dependencies. The order is:

```
1. bronze.city               → Materialized View (batch, no upstream dependencies)
2. bronze.trips              → Streaming Table (Auto Loader, no upstream dependencies)
3. silver.city               → Materialized View (depends on bronze.city)
4. silver.calendar           → Materialized View (depends on pipeline config params only)
5. trips_silver_staging      → View (reads from bronze.trips stream)
6. silver.trips              → Streaming Table (CDC from trips_silver_staging)
7. gold.fact_trips           → SQL View (joins silver.trips + city + calendar)
8. gold.fact_trips_{city} ×10 → SQL Views (each filtered from fact_trips)
```


## Part 10: Testing & Challenges

### Q38. How did you test that incremental loads work correctly?

**A:** I uploaded all 148 full-load trip files to S3 and ran the pipeline. I recorded the row counts — 350K records in `silver.trips`. Then I manually added **5 new CSV files** to the same S3 path and re-ran the pipeline. I verified that Auto Loader picked up **only those 5 new files** (not all 153), and the row count in `silver.trips` increased by exactly the new trip records. That confirmed exactly-once incremental ingestion was working correctly.

---

### Q39. What was the trickiest technical issue you hit?

**A:** The column name `distance_travelled(km)` — the parentheses break Spark's column reference syntax in all downstream code. Spark can ingest the file fine, but the moment you try to reference that column by name in Silver or Gold, it throws an error. The fix was simple: rename it to `distance_travelled_km` in Bronze right after ingestion, before it causes any downstream issues. **Lesson:** Sanitize column names at the entry point.


## Part 11: Curveball Questions

### Q40. What happens if two trip files in S3 have the same `trip_id` but different fare amounts?

**A:** The CDC flow uses `sequence_by=silver_processed_timestamp`. The **most recently processed** record wins — it overwrites the older one in `silver.trips`. This is consistent with SCD Type 1 behavior: the latest version is always the truth.

---

### Q41. What would you do if a new city (say, Pune) is added to GoodCabs?

1. Add the new city row to `city.csv` and re-upload to S3 → `bronze.city` and `silver.city` refresh automatically on the next pipeline run
2. Create a new Gold SQL view: `fact_trips_pune.sql` (just a one-line filter)
3. Re-run the pipeline → new view is available in Unity Catalog
4. Grant permissions to the Pune regional team in Unity Catalog
5. **No changes** needed to Bronze or Silver pipeline code at all

---

### Q42. How would you scale this to 100 cities instead of 10?

The Bronze and Silver layers require **zero changes** — they are city-agnostic. For Gold:
- Move away from one SQL file per city (100 files would be unmanageable)
- Instead, generate views programmatically from the city dimension table using a loop
- Or replace city-specific views with a single **parameterized view** or a row-level access policy in Unity Catalog

---

### Q43. How would you monitor this pipeline in production?

- **Databricks pipeline event logs** — track run times, row counts, and failure reasons
- **`@dp.expect` metrics** — monitor expectation failure rates over time in the pipeline UI
- **Databricks SQL alerts** — set threshold-based alerts (e.g., alert if expectation failure rate > 5%)
- **Pipeline failure alerts** — Databricks can send email or webhook (Slack) notifications on pipeline failure
- In a more mature setup: integrate with a monitoring tool like Datadog or PagerDuty

---

### Q44. What happens if the pipeline fails halfway through? Does data get corrupted?

**A:** No — because of Delta Lake's **ACID transactions**. A write either completes fully or it doesn't happen at all. If the pipeline fails mid-write, the partial write is rolled back automatically. When the pipeline restarts, Auto Loader uses its checkpoint to know where it left off and only reprocesses what wasn't successfully committed. No duplicate data, no partial data.


## Part 12: What Would You Improve?

### Q45. What would you improve if you had more time?

- **Upgrade data quality expectations:** Move critical rules from `@dp.expect` (soft) to `@dp.expect_or_drop` so bad records are caught before reaching Gold
- **Programmatic city views:** Replace 10 hand-written SQL files with a loop that generates them from the city dimension — less duplication, easier to scale
- **Real BI dashboard:** Build a proper dashboard (Databricks SQL or Power BI) on top of the Gold layer for actual business use
- **Holidays reference table:** Move Indian holidays from hardcoded values in `calendar.py` to a proper reference table that's easy to update each year
- **Automated tests:** Add tests for pipeline quality expectations and Silver transformations
- **Real-time monitoring:** Set up automated alerting for pipeline failures and expectation violation spikes


## Part 13: Genie — AI Feature

### Q46. What is Databricks Genie and how did you use it?

**A:** Genie is Databricks' AI-powered natural language query interface. After the pipeline ran, I opened `transportation.gold.fact_trips` in Unity Catalog → clicked **"Sample Data"** → **"Ask + Run"**. You type a question in plain English and Genie generates and runs the SQL automatically.

**Example questions it can handle:**
- "What is the average driver rating by city?"
- "How do passenger ratings vary on weekdays vs weekends?"
- "Which city has the highest average fare per trip?"
- "What are the top 3 cities by total revenue?"
- "What is the total distance traveled each month?"

It's useful for business users who don't know SQL — they can query `fact_trips` directly without writing any code.