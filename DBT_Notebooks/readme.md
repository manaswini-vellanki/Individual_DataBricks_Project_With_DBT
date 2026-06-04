# End-to-End Data Engineering Project using PySpark, dbt and Databricks

## About the Project

This project demonstrates an end-to-end data engineering pipeline built using PySpark, dbt Cloud, and Databricks. The pipeline follows the Medallion Architecture approach, transforming raw data into analytics-ready datasets through Bronze, Silver, and Gold layers.

The project focuses on data ingestion, transformation, data quality improvements, dimensional modeling, and incremental processing using modern data engineering tools and practices.

---

## Tech Stack

- Python
- PySpark
- Databricks
- Delta Lake
- dbt Cloud
- Git & GitHub

---

## Architecture

### Bronze Layer
- Ingest raw CSV files using PySpark Structured Streaming.
- Store data in Delta tables.
- Support incremental data ingestion.

### Silver Layer
- Clean and transform data.
- Handle duplicates and null values.
- Apply business rules and merge operations.

### Gold Layer
- Build analytical models using dbt.
- Create fact and dimension tables.
- Implement Star Schema for reporting and analytics.

---

## Key Features

- End-to-End Data Pipeline
- Medallion Architecture Implementation
- Incremental Data Processing
- Structured Streaming with PySpark
- Delta Lake Integration
- Data Validation and Transformation
- Fact and Dimension Modeling
- dbt-based Data Transformations

---

## Data Flow

```text
Raw CSV Files
      |
      V
 Bronze Layer
      |
      V
 Silver Layer
      |
      V
 Gold Layer
      |
      V
 Analytics & Reporting
```

---

## Data Engineering Concepts Implemented

- Medallion Architecture
- Structured Streaming
- Delta Lake
- Incremental Loading
- Data Cleansing
- Data Validation
- Upsert Operations
- Slowly Changing Dimensions (SCD)
- Star Schema Design
- Fact and Dimension Modeling

---

## What I Learned

- Building scalable data pipelines using PySpark.
- Working with Databricks and Delta Lake.
- Implementing incremental data processing.
- Creating dimensional models using dbt.
- Understanding modern data engineering architecture patterns.
- Developing analytics-ready datasets from raw data sources.

---

