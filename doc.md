DEV → UAT Checklist

✔ All notebooks run successfully in DEV  
✔ FULL pipeline runs successfully  
✔ INCR pipeline runs successfully  
✔ DQ checks pass  
✔ No temporary test code (like raise Exception)  
✔ Activity audit logging working  
✔ Failure handling working  
✔ Parameters working correctly  
✔ No hardcoded values  

DEV → UAT Checklist

✔ Pipeline tested in UAT  
✔ Business validation completed  
✔ No failed runs in UAT  
✔ Schedule configured correctly  
✔ FULL and INCR triggers verified  
✔ Error handling verified  
✔ No debug/test code  
✔ Final version approved  
--------------------------------------------------------------------------------------------------------------------------------------
 docs/
   - 01_Project_Overview.md
   - 02_Architecture.md
   - 03_Pipeline_Design.md
   - 04_Notebook_Design.md
   - 05_Data_Model.md
   - 06_CICD_Strategy.md
   - 07_Testing_Strategy.md
------------------------------------------------------------------------------------------------------------------------   
----01_Project_Overview.md
# Project Overview — Finance Data Engineering Pipeline

## Objective
The objective of this project is to design and implement an end-to-end data engineering pipeline using Microsoft Fabric to process financial transaction data.

The pipeline follows a Medallion Architecture (Bronze, Silver, Gold) and supports both full and incremental data processing.

## Key Features
- End-to-end pipeline orchestration using Fabric pipelines
- Parameter-driven execution (FULL / INCR)
- Medallion architecture (Bronze, Silver, Gold)
- Data quality validation layer
- Activity-level audit logging
- Failure handling and retry strategy
- Scheduled execution for FULL and INCR loads
- CI/CD-ready design with DEV, UAT, and PROD environments

## Technology Stack
- Microsoft Fabric (Lakehouse, Pipelines, Notebooks)
- PySpark
- Delta Lake
- SQL Endpoint
- Git Integration

## Pipeline Flow
DDL → Bronze → Silver → Gold → Optimize → Data Quality

## Processing Modes
- FULL: Complete data load
- INCR: Incremental processing using watermark logic

## Business Value
- Enables scalable financial transaction processing
- Supports fraud detection and KPI reporting
- Improves data quality and monitoring
- Provides production-ready orchestration design
--------------------------------------------------------------------------------------------------------------------------------
02_Architecture.md

# Architecture — Finance Data Engineering Pipeline

## Overview
The project is built using a Medallion Architecture in Microsoft Fabric, consisting of Bronze, Silver, and Gold layers. Each layer is responsible for a specific stage of data processing, ensuring scalability, maintainability, and data quality.

## Architecture Layers

### 1. Bronze Layer (Raw Ingestion)
- Source: PaySim dataset (CSV)
- Purpose: Store raw, unprocessed data
- Type: Append-only
- Key Features:
  - Parameterized ingestion
  - Metadata columns (pipeline_run_id, ingestion timestamp)
  - No transformations
  - Acts as source of truth

---

### 2. Silver Layer (Transformation & Cleansing)
- Purpose: Clean and standardize data
- Key Features:
  - Incremental processing using watermark
  - Data standardization
  - Derived columns (timestamps, flags)
  - Data Quality split:
    - Valid records
    - Rejected records
  - Writes to curated tables

---

### 3. Gold Layer (Business Model)
- Purpose: Business-ready data model
- Key Features:
  - Dimension tables (account, date, transaction type)
  - SCD Type 2 implementation for account profile
  - Fact table (transactions)
  - Aggregations:
    - Fraud summary
    - KPI summary
    - Data quality summary

---

### 4. Optimize Layer
- Purpose: Improve query performance
- Key Features:
  - Delta optimization
  - File compaction
  - Data layout improvement

---

### 5. Data Quality Layer
- Purpose: Validate final data before consumption
- Key Checks:
  - NULL key validation
  - Duplicate detection
  - Fraud KPI validation
  - Final DQ guard (pipeline fail on critical issues)

---

## Orchestration Flow

DDL → Bronze → Silver → Gold → Optimize → Data Quality

---

## Processing Modes

### FULL Load
- Loads entire dataset
- Used for initial load or recovery

### INCR Load
- Loads only new/changed data
- Uses watermark logic
- Improves performance

---

## Control Flow Logic
IF p_load_type = FULL → Full pipeline execution
ELSE IF p_load_type = INCR → Incremental pipeline execution
ELSE → Fail pipeline


---

## Monitoring & Logging

- Activity-level logging:
  - meta.pipeline_activity_audit
- Run-level logging:
  - meta.pipeline_run_audit

---

## Failure Handling

- Centralized failure handling using a single Fail activity
- All notebook failures route to fail_pipeline_execution
- Ensures consistent pipeline termination
-------------------------------------------------------------------------------------------------------------------------------------
03_Pipeline_Design.md

# Pipeline Design — Finance Data Engineering Pipeline

## Overview
The pipeline is designed using Microsoft Fabric Data Pipelines to orchestrate end-to-end data processing across Bronze, Silver, Gold, Optimize, and Data Quality layers.

The pipeline supports parameter-driven execution and handles both FULL and INCR load scenarios.

---

## Pipeline Name
- pl_finance_e2e_batch (FULL)
- pl_finance_e2e_batch_incr (INCR)

---

## Pipeline Parameters

| Parameter | Type | Description |
|----------|------|------------|
| p_load_type | String | Controls execution mode (FULL / INCR) |

---

## Pipeline Activities

### 1. If Condition — Load Type Controller
This is the main control flow component.

IF p_load_type = FULL → Execute FULL path
ELSE IF p_load_type = INCR → Execute INCR path
ELSE → Fail pipeline

---
## FULL Pipeline Flow
nb_00_ddl_setup
↓
nb_01_bronze_paysim_ingest_full
↓
nb_02_silver_paysim_transform
↓
nb_03_gold_model_build
↓
nb_04_gold_optimize_tables
↓
nb_05_data_quality_checks


### Key Features
- Sequential execution
- End-to-end full data load
- Used for initial load or recovery
---
## INCR Pipeline Flow
nb_01_bronze_paysim_ingest_full
↓
nb_02_silver_paysim_transform
↓
nb_03_gold_model_build
↓
nb_04_gold_optimize_tables
↓
nb_05_data_quality_checks

### Key Features
- Incremental processing using watermark
- Reuses same notebooks
- Controlled by parameter p_load_type = INCR

---

## Failure Handling

- Centralized failure handling using Fail activity:
  - fail_pipeline_execution
- All notebook failures route to Fail activity
- Ensures pipeline stops immediately on error

---

## Retry Strategy

- Retry enabled for notebook activities
- Handles transient failures
- Configurable retry count and interval

---

## Scheduling Strategy

### FULL Pipeline
- Runs once daily
- Example: 06:00 AM

### INCR Pipeline
- Runs frequently
- Example: every 1 hour

---

## Monitoring

- Activity-level logging:
  - meta.pipeline_activity_audit
- Run-level logging:
  - meta.pipeline_run_audit

---

## Design Principles

- Parameter-driven execution
- Reusability of notebooks
- Centralized failure handling
- Modular pipeline design
- Separation of FULL and INCR workloads


---------------------------------------------------------------------------------------------------------------------------------------
04_Notebook_Design.md
 
# Notebook Design — Finance Data Engineering Pipeline

## Overview

The pipeline is implemented using multiple PySpark notebooks in Microsoft Fabric. Each notebook is responsible for a specific stage of the Medallion Architecture and follows modular, reusable, and parameter-driven design principles.

---

## Notebook Inventory

| Notebook                        | Layer    | Purpose                                     |
| ------------------------------- | -------- | ------------------------------------------- |
| nb_00_ddl_setup                 | Setup    | Create schemas, tables, metadata structures |
| nb_01_bronze_paysim_ingest_full | Bronze   | Ingest raw data                             |
| nb_02_silver_paysim_transform   | Silver   | Clean and transform data                    |
| nb_03_gold_model_build          | Gold     | Build business model                        |
| nb_04_gold_optimize_tables      | Optimize | Improve performance                         |
| nb_05_data_quality_checks       | DQ       | Validate data quality                       |

---

## Common Design Pattern (Used in All Notebooks)

Each notebook follows a standard structure:

1. Read pipeline parameters
2. Capture start time
3. Initialize activity monitoring
4. Perform core logic
5. Capture end time
6. Insert activity audit record

---

## nb_00_ddl_setup

### Purpose

* Create schemas:

  * bronze
  * silver
  * gold
  * meta
* Create control tables:

  * ingestion_control
  * pipeline_activity_audit
  * pipeline_run_audit

### Key Features

* One-time setup
* Ensures environment readiness

---

## nb_01_bronze_paysim_ingest_full

### Purpose

* Ingest raw PaySim dataset into Bronze layer

### Key Features

* Parameterized execution (FULL / INCR)
* Reads source file from Lakehouse
* Appends data to bronze table
* Adds metadata columns:

  * pipeline_run_id
  * ingestion timestamp

---

## nb_02_silver_paysim_transform

### Purpose

* Clean and standardize Bronze data

### Key Features

* Incremental filtering using watermark
* Data standardization
* Derived columns:

  * event timestamp
  * flags
* Data Quality split:

  * valid records
  * rejected records
* Writes to Silver tables
* Updates watermark table

---

## nb_03_gold_model_build

### Purpose

* Build business-ready data model

### Key Features

* Dimension tables:

  * dim_account
  * dim_date
  * dim_transaction_type
* SCD Type 2 implementation for account profile
* Fact table:

  * fact_transactions
* Aggregations:

  * fraud_summary_daily
  * KPI summary
  * DQ summary

---

## nb_04_gold_optimize_tables

### Purpose

* Optimize Delta tables for performance

### Key Features

* File compaction
* Data optimization
* Improves query performance

---

## nb_05_data_quality_checks

### Purpose

* Validate final data before consumption

### Key Checks

* NULL key validation
* Duplicate transaction validation
* Fraud KPI validation
* Final DQ guard

### Behavior

* Pipeline fails if critical DQ issues are detected

---

## Parameter Handling

All notebooks use:

p_load_type = FULL / INCR

* FULL → complete data load
* INCR → incremental processing

---

## Logging & Monitoring

Each notebook writes to:

meta.pipeline_activity_audit

Captured fields:

* run_id
* activity_name
* layer
* start_time
* end_time
* status
* duration_seconds
* error_message

---

## Design Principles

* Modular notebook design
* Reusability across FULL and INCR
* Parameter-driven execution
* Centralized logging
* Enterprise-grade structure
-----------------------------------------------------------------------------------------------------------------------------------
05_Data_Model.md

# Data Model — Finance Data Engineering Pipeline

## Overview

The Gold layer follows a Star Schema design to support analytical reporting, fraud detection, and business intelligence use cases.

The model consists of fact and dimension tables designed for performance, scalability, and ease of use.

---

## Star Schema Design

* Central fact table: fact_transactions
* Surrounding dimensions:

  * dim_account
  * dim_date
  * dim_transaction_type

This design enables efficient querying and aggregation.

---

## Fact Table

### fact_transactions

#### Description

Stores all financial transaction records at the lowest level of granularity.

#### Grain

* One row per transaction

#### Key Columns

* txn_id (unique transaction identifier)
* date_key (FK to dim_date)
* origin_account_key (FK to dim_account)
* destination_account_key (FK to dim_account)
* transaction_type_key (FK to dim_transaction_type)

#### Measures

* amount
* balance_before
* balance_after

---

## Dimension Tables

### dim_account

#### Description

Stores account-level information.

#### Key Features

* Surrogate key: account_key
* Business key: account_id
* Implements Slowly Changing Dimension Type 2 (SCD2)

#### SCD2 Columns

* effective_start_date
* effective_end_date
* is_current

---

### dim_date

#### Description

Stores calendar and time-related attributes.

#### Key Features

* date_key (YYYYMMDD format)
* full_date
* year
* month
* day
* quarter

---

### dim_transaction_type

#### Description

Stores transaction categories.

#### Examples

* DEPOSIT
* WITHDRAWAL
* TRANSFER
* PAYMENT

---

## Aggregation Tables

### fraud_summary_daily

#### Purpose

* Daily fraud-related metrics

#### Example Metrics

* total_transactions
* fraud_transactions
* fraud_amount

---

### kpi_summary_daily

#### Purpose

* Executive-level reporting

#### Example Metrics

* total_volume
* total_amount
* average_transaction_value

---

### dq_summary_daily

#### Purpose

* Data quality monitoring

#### Example Metrics

* rejected_records_count
* null_key_count
* duplicate_count

---

## Relationships

* fact_transactions → dim_account (origin_account_key)
* fact_transactions → dim_account (destination_account_key)
* fact_transactions → dim_date (date_key)
* fact_transactions → dim_transaction_type (transaction_type_key)

---

## Design Principles

* Star schema for performance
* Surrogate keys for joins
* SCD Type 2 for historical tracking
* Separation of fact and dimensions
* Aggregations for reporting efficiency

---

## Benefits

* Fast query performance
* Easy integration with Power BI
* Supports complex analytics
* Enables fraud detection and KPI reporting
 
--------------------------------------------------------------------------------------------------------------------------------------
---06_CICD_Strategy.md

# CI/CD Strategy — Finance Data Engineering Pipeline

## Overview

This project follows a structured CI/CD (Continuous Integration and Continuous Deployment) approach using Microsoft Fabric workspaces and Git integration.

The strategy ensures controlled development, testing, and deployment across multiple environments.

---

## Environment Setup

| Environment | Workspace              | Purpose                         |
| ----------- | ---------------------- | ------------------------------- |
| DEV         | ws_fab_finance_de_dev  | Development and initial testing |
| UAT         | ws_fab_finance_de_uat  | User acceptance testing         |
| PROD        | ws_fab_finance_de_prod | Production deployment           |

---

## Git Branching Strategy

| Branch | Purpose                |
| ------ | ---------------------- |
| dev    | Active development     |
| uat    | Pre-production testing |
| main   | Production-ready code  |

---

## Environment Mapping

| Workspace | Git Branch |
| --------- | ---------- |
| DEV       | dev        |
| UAT       | uat        |
| PROD      | main       |

---

## Promotion Flow

```
feature → dev → uat → main
```

---

## Development Process

### Feature Development

* Create feature branch from dev
* Implement changes (notebooks, pipelines, logic)
* Test locally in DEV workspace

### Integration

* Merge feature branch into dev
* Validate pipeline execution in DEV

### UAT Promotion

* Merge dev into uat
* Deploy changes to UAT workspace
* Perform user/business validation

### Production Deployment

* Merge uat into main
* Deploy to PROD workspace
* Enable scheduled execution

---

## Deployment Checklist

### DEV → UAT

* All notebooks execute successfully
* FULL pipeline runs successfully
* INCR pipeline runs successfully
* Data Quality checks pass
* No debug or test code present
* Logging and monitoring verified

### UAT → PROD

* UAT validation completed
* No failed runs in UAT
* Scheduling configured correctly
* Failure handling verified
* Final approval received

---

## Configuration Strategy

* Parameter-driven execution using:

  * p_load_type (FULL / INCR)
* Separate pipelines for:

  * FULL execution
  * INCR execution

---

## Best Practices

* Do not develop directly in UAT or PROD
* Do not commit directly to main branch
* Use feature branches for isolated changes
* Validate all changes in DEV before promotion
* Ensure pipeline stability before production release

---

## Benefits

* Controlled deployment process
* Reduced risk of production failure
* Clear separation of environments
* Improved collaboration and version control
---------------------------------------------------------------------------------------------------------------------
07_Testing_Strategy.md
# Testing Strategy — Finance Data Engineering Pipeline

## Overview

This project includes a structured testing strategy to validate pipeline execution, data correctness, and failure handling.

Testing is performed at both pipeline level and data level to ensure reliability and production readiness.

---

## Testing Types

### 1. Pipeline Execution Testing

Validates that the pipeline executes correctly under different scenarios.

---

## Test Scenarios

### Scenario 1 — FULL Load

**Input:**
p_load_type = FULL

**Expected Behavior:**

* FULL branch executes
* All notebooks run sequentially:

  * Bronze → Silver → Gold → Optimize → DQ
* Pipeline completes successfully

---

### Scenario 2 — INCR Load

**Input:**
p_load_type = INCR

**Expected Behavior:**

* INCR branch executes
* Incremental logic is applied
* Only new/updated data is processed
* Pipeline completes successfully

---

### Scenario 3 — INVALID Load Type

**Input:**
p_load_type = XYZ

**Expected Behavior:**

* Neither the FULL nor the INCR branch executes
* Fail activity triggers
* Pipeline status = Failed
* Error message = INVALID_LOAD_TYPE

---

### Scenario 4 — Failure Simulation

**Test Method:**
Introduce a temporary error in a notebook:

```python
raise Exception("Test Failure")
```

**Expected Behavior:**

* Pipeline stops immediately
* fail_pipeline_execution activity triggers
* Pipeline status = Failed

---

## Data Validation Testing

### Bronze Layer

* Validate row count matches source
* Ensure metadata columns are populated

### Silver Layer

* Validate valid vs rejected records
* Ensure data standardization is correct
* Verify watermark logic

### Gold Layer

* Validate dimension and fact tables
* Verify SCD Type 2 behavior
* Check aggregation tables

---

## Data Quality Testing

Performed in nb_05_data_quality_checks

### Checks Included:

* NULL key validation
* Duplicate transaction detection
* Fraud KPI validation

### Expected Behavior:

* Pipeline fails if critical DQ issues are found

---

## Audit Logging Validation

Validate records in:

* meta.pipeline_activity_audit
* meta.pipeline_run_audit

### Expected:

* One record per notebook per pipeline run
* Correct status (SUCCESS / FAILED)
* Accurate duration and timestamps

---

## Testing Best Practices

* Test one scenario at a time
* Do not mix FULL and INCR runs
* Remove all test code after validation
* Capture test results for documentation
* Validate both success and failure paths

---

## Outcome

This testing strategy ensures:

* Correct pipeline execution
* Reliable data processing
* Proper failure handling
* Production readiness
