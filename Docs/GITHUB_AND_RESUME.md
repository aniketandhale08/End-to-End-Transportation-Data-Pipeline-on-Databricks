## 8. Resume Content

### Resume Bullet Points
- Designed and built an end-to-end Medallion (Bronze/Silver/Gold) data pipeline on Databricks using LakeFlow Declarative Pipelines, PySpark, and Delta Lake to process multi-city cab trip data ingested from AWS S3.
- Built an end-to-end data engineering pipeline on Databricks using LakeFlow Spark Declarative Pipelines and Medallion Architecture (Bronze → Silver → Gold) to process 150+ daily trip CSV files from AWS S3 across 10 Indian cities, enabling automated, city-specific analytics for regional business teams. 
- Built a production-style, end-to-end data pipeline on Databricks using LakeFlow Spark Declarative Pipelines and Medallion Architecture (Bronze → Silver → Gold), processing 150+ daily CSV files from AWS S3 into Delta Lake tables with incremental Auto Loader ingestion and CDC-based SCD Type 1 deduplication.
- Implemented incremental data ingestion with Databricks Auto Loader and CDC-based upserts (SCD Type 1), enabling near-real-time data availability as new files landed in S3.
- Delivered 10 city-specific Gold-layer analytics views from a single fact table, replacing a generic dashboard model and enabling region-specific self-service reporting.

### LinkedIn Project Description
Built an end-to-end data engineering pipeline on Databricks (LakeFlow Declarative Pipelines) for a multi-city cab service use case. Data flows from AWS S3 through a Bronze → Silver → Gold Medallion architecture using PySpark and Delta Lake, with Auto Loader for incremental ingestion and CDC upserts (SCD Type 1) for deduplication. The Gold layer exposes 10 city-specific views, solving a real business problem: giving regional teams timely, region-specific data instead of one generic dashboard. Highlights: declarative data quality checks, dynamically generated calendar dimension, and continuous-mode pipeline execution for near-real-time updates.