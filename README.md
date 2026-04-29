# 🏦 Finance Data Engineering Pipeline — Microsoft Fabric

![Microsoft Fabric](https://img.shields.io/badge/Microsoft-Fabric-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.x-orange)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-green)
![Python](https://img.shields.io/badge/Python-3.10-yellow)
![CI/CD](https://img.shields.io/badge/CICD-DEV--UAT--PROD-purple)

> Enterprise-grade Microsoft Fabric Lakehouse pipeline implementing 
> Medallion Architecture (Bronze → Silver → Gold) for financial 
> transaction processing and fraud detection.

---

## 📌 Table of Contents
- [Project Overview](#-project-overview)
- [Architecture](#-architecture)
- [Dataset](#-dataset)
- [Key Features](#-key-features)
- [Pipeline Design](#-pipeline-design)
- [Data Model](#-data-model)
- [Technology Stack](#-technology-stack)
- [CI/CD Strategy](#-cicd-strategy)
- [Monitoring & Logging](#-monitoring--logging)
- [Testing Strategy](#-testing-strategy)
- [Results & KPIs](#-results--kpis)
- [Project Structure](#-project-structure)
- [How to Run](#-how-to-run)
- [Author](#-author)

---

## 🚀 Project Overview
This project implements an end-to-end **enterprise-grade data engineering 
pipeline** using **Microsoft Fabric** to process financial transaction data 
from the **PaySim dataset**.

The pipeline follows **Medallion Architecture** (Bronze, Silver, Gold) and 
supports both **Full** and **Incremental** data processing with 
production-ready CI/CD deployment across DEV, UAT, and PROD environments.

---

## 🏗️ Architecture

> *(Add your architecture diagram image here)*
> ![Architecture Diagram](docs/images/architecture.png)

### Medallion Layers

| Layer | Purpose | Key Features |
|---|---|---|
| **Bronze** | Raw ingestion | Append-only, metadata columns, source of truth |
| **Silver** | Transform & cleanse | Watermark logic, DQ split, standardization |
| **Gold** | Business model | Star schema, SCD Type 2, aggregations |
| **Optimize** | Performance | Delta optimization, file compaction |
| **Data Quality** | Validation | NULL checks, duplicates, KPI validation |

### Pipeline Flow
```
DDL → Bronze → Silver → Gold → Optimize → Data Quality
```

---

## 📦 Dataset

- **Source:** [PaySim Synthetic Financial Dataset](https://www.kaggle.com/datasets/ealaxi/paysim1)
- **Domain:** Financial Transactions / Fraud Detection
- **Size:** 6.3M+ transactions
- **Key Fields:** transaction type, amount, origin/destination account, 
  fraud flag

---

## ⚙️ Key Features

- ✅ Parameter-driven execution (FULL / INCR)
- ✅ Incremental processing using watermark logic
- ✅ Centralized failure handling & retry strategy
- ✅ Activity-level audit logging
- ✅ Data Quality validation layer
- ✅ SCD Type 2 implementation for account dimension
- ✅ Scheduled pipeline execution
- ✅ CI/CD-ready design (DEV / UAT / PROD)

---

## 🧠 Pipeline Design

### Processing Modes

| Mode | Pipeline | Schedule | Use Case |
|---|---|---|---|
| FULL | pl_finance_e2e_batch | Daily 6:00 AM | Initial load / Recovery |
| INCR | pl_finance_e2e_batch_incr | Every 1 hour | Delta processing |

### Control Flow Logic
```
IF p_load_type = FULL  → Execute Full Pipeline
IF p_load_type = INCR  → Execute Incremental Pipeline
ELSE                   → Fail Pipeline (INVALID_LOAD_TYPE)
```

### Notebooks

| Notebook | Layer | Purpose |
|---|---|---|
| nb_00_ddl_setup | Setup | Create schemas & control tables |
| nb_01_bronze_paysim_ingest_full | Bronze | Raw data ingestion |
| nb_02_silver_paysim_transform | Silver | Cleanse & transform |
| nb_03_gold_model_build | Gold | Build star schema |
| nb_04_gold_optimize_tables | Optimize | Delta optimization |
| nb_05_data_quality_checks | DQ | Final validation |

---

## 📊 Data Model

### Star Schema
```
                    dim_date
                       |
dim_transaction_type — fact_transactions — dim_account (SCD2)
```

| Table | Type | Description |
|---|---|---|
| fact_transactions | Fact | All transactions, lowest grain |
| dim_account | Dimension (SCD2) | Account profile with history |
| dim_date | Dimension | Calendar attributes |
| dim_transaction_type | Dimension | Transaction categories |
| fraud_summary_daily | Aggregate | Daily fraud metrics |
| kpi_summary_daily | Aggregate | Executive KPI reporting |
| dq_summary_daily | Aggregate | DQ monitoring metrics |

---

## 🛠️ Technology Stack

| Tool | Purpose |
|---|---|
| Microsoft Fabric | Platform — Lakehouse, Pipelines, Notebooks |
| PySpark | Distributed data processing |
| Delta Lake | ACID-compliant storage format |
| SQL Endpoint | Analytical querying |
| Git Integration | Version control & CI/CD |

---

## 🔄 CI/CD Strategy

### Environment Mapping

| Environment | Workspace | Git Branch |
|---|---|---|
| Development | ws_fab_finance_de_dev | dev |
| UAT | ws_fab_finance_de_uat | uat |
| Production | ws_fab_finance_de_prod | main |

### Promotion Flow
```
feature → dev → uat → main
```

### Deployment Checklist
- ✔ All notebooks run successfully
- ✔ FULL and INCR pipelines validated
- ✔ DQ checks pass
- ✔ No hardcoded values
- ✔ Audit logging verified
- ✔ Failure handling tested

---

## 📈 Monitoring & Logging

All pipeline activity is captured in:

| Table | Purpose |
|---|---|
| meta.pipeline_activity_audit | Per-notebook execution tracking |
| meta.pipeline_run_audit | End-to-end run monitoring |

Captured fields: `run_id`, `activity_name`, `layer`, `start_time`, 
`end_time`, `status`, `duration_seconds`, `error_message`

---

## 🧪 Testing Strategy

| Scenario | Input | Expected Result |
|---|---|---|
| FULL Load | p_load_type = FULL | Complete pipeline success |
| INCR Load | p_load_type = INCR | Incremental run success |
| Invalid Type | p_load_type = XYZ | Pipeline fails with INVALID_LOAD_TYPE |
| Failure Simulation | raise Exception | Centralized fail activity triggers |

---

## 🎯 Results & KPIs

> *(Add your actual results here — example below)*

| Metric | Value |
|---|---|
| Total Transactions Processed | 6.3M+ |
| Fraud Transactions Detected | ~8,200 |
| Fraud Rate | ~0.13% |
| Pipeline Execution Time (FULL) | ~12 mins |
| Pipeline Execution Time (INCR) | ~2 mins |
| Data Quality Pass Rate | 99.8% |

---

## 📂 Project Structure
```text
fabric-finance-lakehouse/
│
├── docs/
│   ├── 01_Project_Overview.md
│   ├── 02_Architecture.md
│   ├── 03_Pipeline_Design.md
│   ├── 04_Notebook_Design.md
│   ├── 05_Data_Model.md
│   ├── 06_CICD_Strategy.md
│   └── 07_Testing_Strategy.md
│
├── notebooks/
│   ├── nb_00_ddl_setup.ipynb
│   ├── nb_01_bronze_paysim_ingest_full.ipynb
│   ├── nb_02_silver_paysim_transform.ipynb
│   ├── nb_03_gold_model_build.ipynb
│   ├── nb_04_gold_optimize_tables.ipynb
│   └── nb_05_data_quality_checks.ipynb
│
└── README.md
```

---

## ▶️ How to Run

1. Clone this repository
2. Set up Microsoft Fabric workspace
3. Upload notebooks to Fabric
4. Configure Lakehouse connection
5. Run `nb_00_ddl_setup` first
6. Execute pipeline:
   - FULL: Set `p_load_type = FULL.`
   - INCR: Set `p_load_type = INCR.`

---

## 👤 Author

**Dhiraj Misra**
📧 dhirajk266@gmail.com@gmail.com
🔗 [LinkedIn](https://www.linkedin.com/in/dirumisra/)
🐙 [GitHub](https://github.com/dirumisra)
