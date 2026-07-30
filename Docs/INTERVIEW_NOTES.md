# Interview Notes — GoodCabs Transportation Data Pipeline

Quick-revision Q&A based on this specific project. Answers are written the way you'd say them out loud in an interview.

---

### 1. Walk me through this project.
"I built a data pipeline for a cab service company, GoodCabs, that operates across 10 cities. The core problem was that regional managers weren't getting timely, city-specific data — they were stuck with one generic dashboard and had to manually rework exports. I built a Medallion architecture (Bronze/Silver/Gold) using Databricks LakeFlow Declarative Pipelines, ingesting trip and city data from S3 with Auto Loader, cleaning and deduplicating it with CDC, and landing it in 10 city-specific Gold views so each region can query its own always-current data."

### 2. Why did you use Databricks / LakeFlow Declarative Pipelines instead of writing plain PySpark jobs?
- Declarative pipelines auto-resolve table dependencies — no manual orchestration code
- Built-in incremental processing for streaming tables — you don't hand-write checkpoint/watermark logic
- Built-in data quality tracking via `@dp.expect`
- Automatic lineage graph and pipeline observability out of the box

### Q: Walk me through your overall architecture.

**A:**
- **Source:** Trip CSV files are generated from the operational DB and exported to AWS S3
- **Bronze:** Auto Loader reads only new CSV files from S3 into a streaming Delta table. City data is batch-read as a materialized view.
- **Silver:** Trips are cleaned, validated with quality expectations, and CDC-upserted into a Silver streaming table using SCD Type 1. City and Calendar are materialized views.
- **Gold:** A SQL `fact_trips` view joins Silver trips + city + calendar. Ten city-specific views filter fact_trips by city_id.
- **Consumption:** Regional teams query city-specific Gold views. Databricks Genie provides natural language querying.


### Q: Why Delta Lake? Why not Parquet directly?

**A:**
| Feature | Delta Lake | Plain Parquet |
|---|---|---|
| ACID transactions | ✅ | ❌ |
| Schema enforcement | ✅ | ❌ |
| Schema evolution | ✅ | ❌ |
| MERGE / UPSERT | ✅ | ❌ |
| Change Data Feed | ✅ | ❌ |
| Time travel | ✅ | ❌ |
| Streaming + batch | ✅ Unified | ❌ Separate |

In this project, Delta Lake enables:
1. CDC upserts on `silver.trips`
2. Exactly-once streaming guarantees
3. Auto-optimization (small file compaction)
4. CDF for downstream consumers

### 4. Why Medallion Architecture (Bronze/Silver/Gold)?
- **Bronze** preserves raw data exactly as received (with lineage metadata) — a safety net if downstream logic ever needs to be recomputed from scratch
- **Silver** is where cleaning, renaming, and validation happen once, in one place, instead of every consumer re-doing it
- **Gold** is business-ready and shaped for direct consumption (per-city views), decoupling "how data was cleaned" from "how it's queried"


### Q: Why Auto Loader instead of a scheduled batch job?

**A:**
- Auto Loader continuously monitors the S3 path for new files
- It maintains a **checkpoint** so it knows exactly which files have been processed
- Only new/unprocessed files are read — **no re-processing of old data**
- This is exactly-once processing — even if the pipeline fails and restarts
- Scales automatically — handles thousands of files without code changes
- `cloudFiles.maxFilesPerTrigger: 100` controls the rate of ingestion per micro-batch



### Q: How does Auto Loader know which files are new?

**A:**
- Auto Loader uses a **checkpoint directory** stored in DBFS or cloud storage
- It maintains a log of all processed file paths and their ETags (content hash)
- On each trigger, it lists S3 files, compares against the checkpoint, and processes only the difference
- For large buckets, it can use **file notification mode** (SQS/SNS on AWS) instead of directory listing



### 5. Why Auto Loader for trips but a plain batch read for city?
Trips is large and constantly growing — new files land regularly, so I need incremental, scalable ingestion that doesn't reprocess old files. Auto Loader tracks which files it's already seen and only processes new arrivals. City is a small, ~10-row dimension table that rarely changes, so a full batch reload is simpler and cheap enough not to need incremental logic.

### 6. Why Streaming for the trips pipeline specifically?
Because trip data arrives continuously (in this project, simulated with 148 full-load files followed by 5 incremental files). A streaming table means each pipeline run only processes what's new, which is both faster and matches how the data actually behaves in production — daily trip exports landing in S3.

### 7. Why SCD Type 1 for trips instead of Type 2?
The business need here is "give me the current state of each trip," not "show me how a trip record changed over time." SCD Type 1 (overwrite on key match) keeps the model simple and the table small. If GoodCabs later needed an audit trail of corrections to trip records, I'd switch the CDC flow to `stored_as_scd_type=2`.

### 8. What is CDC and how did you implement it?
CDC (Change Data Capture) means only processing what changed rather than reprocessing everything. I implemented it declaratively using `dp.create_auto_cdc_flow`, which upserts records from a staging view into the target `silver.trips` table by key (`id`), using `silver_processed_timestamp` to decide which version of a record wins if duplicates show up.


### Q: What is Change Data Feed (CDF) and why did you enable it?

**A:**
- CDF records every INSERT, UPDATE, DELETE operation on a Delta table
- Enabled via `delta.enableChangeDataFeed: true` on all tables
- Allows downstream consumers to read **only the rows that changed** since their last read
- In this project, it enables the Silver layer to process only the delta from Bronze — true incremental processing
- If I added a downstream consumer (e.g., a reporting table), it could efficiently read only new/changed rows


### Q: Why SCD Type 1 and not Type 2?

- **SCD Type 1:** Overwrites existing record with latest values — no history kept
- **SCD Type 2:** Adds new row for each change, keeps full history with `valid_from` / `valid_to`


### 9. How do you handle data quality in this pipeline?
Using `@dp.expect` decorators on the Silver staging view — three rules: valid business date (`>= 2020`), valid driver rating (1–10), and valid passenger rating (1–10). Right now these are "soft" expectations — violations are tracked and visible in the pipeline UI, but the rows still flow through. A natural next step is upgrading key rules to `expect_or_drop` so bad records don't reach Gold.

### 10. How did you test incremental loads?
I uploaded all 148 full-load trip files first and ran a full pipeline update. Then I manually added 5 more CSV files to the same S3 path afterward and either re-triggered the pipeline or ran it in **continuous** mode, confirming only the new files were picked up and the row counts in `silver.trips` and the Gold views increased by exactly the new records.

### 11. What was the trickiest technical issue you hit?
The raw column `distance_travelled(km)` — Spark doesn't play well with parentheses in column names when referenced downstream, so I explicitly renamed it to `distance_travelled_km` in the Bronze layer before it caused problems in Silver/Gold.

### 12. What would you improve if you had more time?
- Upgrade key data quality expectations to `expect_or_drop`/`expect_or_fail`
- Replace the 10 hand-written per-city SQL views with a single parameterized/templated view (or a `FOR EACH` loop generating them) to reduce duplication
- Build a real BI dashboard on top of the Gold layer
- Maintain holidays in a proper reference table instead of hardcoding 3 dates in code
- Add automated tests for the pipeline's expectations and transformations

### Q: What is `cloudFiles.schemaEvolutionMode: rescue`?

**A:**
- If a new CSV file arrives with an extra column that didn't exist in the schema, `rescue` mode captures that new column's data in a special `_rescued_data` JSON column
- The pipeline does NOT fail
- The pipeline does NOT silently drop the new data
- This is safer than `addNewColumns` (which could break downstream) or `failOnNewColumns` (which stops the pipeline)


### Q: What is the difference between `@dp.table` (streaming) and `@dp.materialized_view`?

**A:**

| Feature | `@dp.table` (Streaming) | `@dp.materialized_view` |
|---|---|---|
| Processing mode | Incremental / Streaming | Full recompute |
| State | Persists between runs | Re-derived each run |
| Supports CDC | ✅ Yes | ❌ No |
| Use case | Continuously growing datasets | Slowly changing dimensions |
| In this project | `bronze.trips`, `silver.trips` | `bronze.city`, `silver.city`, `silver.calendar` |


### 14. Why generate the calendar dimension in code instead of loading it from a file?
It removes a dependency on an external source and keeps date-range logic configurable — `start_date`/`end_date` come from pipeline configuration, so the same code works whether you need 2025 dates or a different range, without editing anything.


### Q: What would you do if a new city is added to GoodCabs?

**A:**
1. Add the new city row to `city.csv` and re-upload to S3 → `bronze.city` and `silver.city` refresh automatically
2. Create a new Gold SQL view: `fact_trips_{newcity}.sql`
3. Re-run the Gold layer → new view is available in Unity Catalog
4. Grant permissions to the regional team in Unity Catalog
5. No changes needed to Bronze or Silver pipeline code


---

## Possible Follow-Up / Curveball Questions

- *"What happens if two trip files in S3 have the same trip_id but different fare amounts?"* → The CDC flow's `sequence_by` (processing timestamp) decides which one wins — the most recently processed record overwrites the older one, consistent with SCD Type 1.
- *"How would you scale this to 100 cities instead of 10?"* → Move away from one SQL file per city; either parameterize a single view definition or, if physical tables are needed, generate them programmatically from the city dimension instead of hand-writing SQL.
- *"How would you monitor this pipeline in production?"* → Databricks pipeline event logs plus expectation metrics; I'd add alerting on expectation failure rate and pipeline run failures (e.g., via Databricks SQL alerts).
