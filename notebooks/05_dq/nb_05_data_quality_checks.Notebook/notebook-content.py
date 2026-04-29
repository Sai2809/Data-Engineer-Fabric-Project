# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "03b3ab34-e351-4757-ac65-28db43180f57",
# META       "default_lakehouse_name": "lh_finance_core",
# META       "default_lakehouse_workspace_id": "aa5bab7a-005d-4922-95fa-0edc2e6626e2",
# META       "known_lakehouses": [
# META         {
# META           "id": "03b3ab34-e351-4757-ac65-28db43180f57"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### **Data Quality Validation – Gold Layer**
# 
# #### **Objective**
# Validate the quality and integrity of Gold layer output tables after the full Bronze → Silver → Gold pipeline has completed.
# 
# #### **Why this notebook is needed**
# In an enterprise data platform, loading tables is not enough.  
# We must also verify that the final data is reliable before it is consumed by reporting tools, dashboards, and downstream users.
# 
# This notebook performs post-load validation checks on Gold tables.
# 
# #### **Validations performed**
# This notebook will check:
# 
# 1. Gold tables are available and contain data
# 2. Critical fact table keys do not contain NULL values
# 3. Duplicate transaction IDs are identified
# 4. Fraud KPI tables contain expected output
# 5. Final validation status is determined
# 6. Pipeline audit record is written for monitoring
# 
# **Pipeline position**
# This notebook runs after:
# 
# - Bronze ingestion
# - Silver transformation
# - Gold model build
# - Gold optimization
# 
# **Notebook name**
# `nb_05_data_quality_checks`


# MARKDOWN ********************

# #### **Import required libraries**

# CELL ********************

# ============================================================
# STEP 1: Import required libraries
# ============================================================

# Import Spark SQL functions for validation logic
from pyspark.sql import functions as F

# Import Row to build audit records later
from pyspark.sql import Row

# Import datetime to capture notebook start and end timestamps
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Read pipeline parameters**

# CELL ********************

# ============================================================
# STEP 2: Read pipeline parameters
# ============================================================

# This helper function safely reads parameters passed from the
# Fabric pipeline. If the notebook is run manually and the
# parameter is not available, it returns None.
def _get_param(name):
    try:
        return mssparkutils.notebook.params.get(name)
    except Exception:
        return None

# Read parameters passed from the pipeline
p_env = _get_param("p_env")
p_load_type = _get_param("p_load_type")
p_pipeline_run_id = _get_param("p_pipeline_run_id")

# Manual notebook fallback:
# If the notebook is run directly instead of through pipeline,
# assign default values so validation can still run.
if p_pipeline_run_id is None or str(p_pipeline_run_id).strip() == "":
    print("WARNING: Manual notebook run detected — using fallback values")
    p_env = "dev" if p_env is None else p_env
    p_load_type = "FULL" if p_load_type is None else p_load_type
    p_pipeline_run_id = "MANUAL_RUN"

# Print parameter values for debugging and traceability
print("INFO: Parameters loaded successfully")
print("p_env =", p_env)
print("p_load_type =", p_load_type)
print("p_pipeline_run_id =", p_pipeline_run_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Capture notebook start time**

# CELL ********************

# ============================================================
# STEP 3: Capture notebook start time
# ============================================================

# Capture the notebook start timestamp.
# This value will later be written into the pipeline audit table.
notebook_start_time = datetime.now()

print("INFO: Notebook start time captured:", notebook_start_time)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **STEP 4 — Load Gold Layer Tables for Data Quality Validation**
# 
# #### **Objective**
# Load the required Gold layer tables into Spark DataFrames so that data quality validation checks can be performed.
# 
# #### **Why this step is required**
# The Gold layer contains the **final curated datasets** that are used for reporting, analytics, and Power BI semantic models.  
# Before validating data quality rules, the notebook must first load these tables into memory.
# 
# This step ensures that:
# 
# - The required Gold tables exist
# - The tables are accessible by the notebook
# - The tables contain data
# 
# #### **Tables loaded in this step**
# 
# | Table | Purpose |
# |------|------|
# | `gold.fact_transactions` | Main transaction fact table used for analytics |
# | `gold.dim_account` | Account dimension used for transaction participants |
# | `gold.dim_date` | Date dimension used for time-based analysis |
# | `gold.fraud_summary_daily` | Aggregated fraud KPI table used for business validation |
# 
# #### **Validation performed in this step**
# After loading the tables, a **row count check** is performed to confirm that:
# 
# - Tables are readable
# - Data exists in the Gold layer
# - The pipeline produced expected outputs
# 
# These counts also help with **quick debugging if any table is empty or missing**.
# 
# #### **Output**
# The notebook prints row counts for each table to confirm successful loading.


# CELL ********************

# ============================================================
# STEP 4: Load Gold layer tables for validation
# ============================================================

# This step loads the required Gold layer tables into Spark DataFrames
# so that data quality validation checks can be performed.
# The Gold layer contains curated, business-ready datasets.

print("INFO: Starting data quality validation checks...")

# ------------------------------------------------------------
# Load Gold tables into Spark DataFrames
# ------------------------------------------------------------

# Fact table containing all transaction records
df_fact = spark.table("gold.fact_transactions")

# Dimension table containing account information
df_account = spark.table("gold.dim_account")

# Date dimension table used for time-based analytics
df_date = spark.table("gold.dim_date")

# Fraud KPI summary table used for business-level validation
df_fraud = spark.table("gold.fraud_summary_daily")

# ------------------------------------------------------------
# Confirm that tables were loaded successfully
# ------------------------------------------------------------

print("INFO: Gold tables loaded successfully")

# Count rows as a quick sanity check
print("fact_transactions rows:", df_fact.count())
print("dim_account rows:", df_account.count())
print("dim_date rows:", df_date.count())
print("fraud_summary_daily rows:", df_fraud.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **STEP 5 — Validate NULL Values in Fact Table Keys**
# 
# #### **Objective**
# Ensure that critical foreign key columns in the `fact_transactions` table do not contain NULL values.
# 
# #### **Why this validation is important**
# In a dimensional data warehouse, fact tables reference dimension tables using foreign keys.
# 
# If a key column contains NULL values, it can break relationships between the fact table and dimension tables, leading to:
# 
# - incorrect joins
# - missing records in Power BI reports
# - inaccurate aggregations
# 
# #### **Columns validated**
# 
# | Column | Description |
# |------|------|
# | `date_key` | Links transactions to `dim_date` |
# | `origin_account_key` | Account initiating the transaction |
# | `destination_account_key` | Account receiving the transaction |
# | `transaction_type_key` | Links transaction to its type dimension |
# 
# **Expected result**
# All NULL counts should be **0**.
# 
# If NULL values exist, it indicates a data quality issue in the Gold layer.

# CELL ********************

# ============================================================
# STEP 5: Check for NULL values in critical fact table keys
# ============================================================

# ------------------------------------------------------------
# Data Quality Check: NULL values in key columns
# ------------------------------------------------------------
# In a dimensional model, fact tables store foreign keys
# that reference dimension tables. These keys should never
# contain NULL values because they break relationships
# between facts and dimensions.

null_checks = df_fact.select(

    # Count NULL values in date_key column
    # This key links fact_transactions to the dim_date table
    F.sum(F.col("date_key").isNull().cast("int")).alias("null_date_key"),

    # Count NULL values in origin_account_key
    # Represents the account initiating the transaction
    F.sum(F.col("origin_account_key").isNull().cast("int")).alias("null_origin_account"),

    # Count NULL values in destination_account_key
    # Represents the account receiving the transaction
    F.sum(F.col("destination_account_key").isNull().cast("int")).alias("null_destination_account"),

    # Count NULL values in transaction_type_key
    # Links the transaction to its transaction type dimension
    F.sum(F.col("transaction_type_key").isNull().cast("int")).alias("null_transaction_type")
)

# ------------------------------------------------------------
# Display validation results
# ------------------------------------------------------------

print("INFO: Checking for NULL values in fact table keys")

# Show counts of NULL values for each key column
# In a healthy data warehouse these should all be 0
null_checks.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **STEP 6 — Detect Duplicate Transactions**
# 
# #### **Objective**
# Identify duplicate transactions in the `fact_transactions` table using the transaction identifier.
# 
# #### **Why this validation is important**
# In transactional systems, each transaction should have a **unique identifier (`txn_id`)**.
# 
# Duplicate transaction IDs can indicate:
# 
# - duplicate ingestion during pipeline runs
# - upstream source system issues
# - incorrect merge or deduplication logic
# 
# If duplicates exist, they may cause:
# 
# - inflated transaction counts
# - incorrect financial totals
# - misleading fraud analytics
# 
# **Validation rule**
# Group transactions by `txn_id` and count occurrences.
# 
# If any transaction appears more than once, it is considered a duplicate.
# 
# **Expected result**
# Ideally:


# CELL ********************

# ============================================================
# STEP 6: Detect duplicate transactions
# ============================================================

# ------------------------------------------------------------
# Data Quality Check: Duplicate transaction IDs
# ------------------------------------------------------------
# This validation checks whether the same transaction ID
# appears multiple times in the fact table.

# Group records by transaction ID and count occurrences
duplicate_txn = (
    df_fact
    .groupBy("txn_id")
    .count()
    .filter("count > 1")   # Keep only duplicates
)

# Count number of duplicate transactions
duplicate_count = duplicate_txn.count()

print("INFO: Duplicate transaction count:", duplicate_count)

# ------------------------------------------------------------
# Display duplicates if they exist
# ------------------------------------------------------------

if duplicate_count > 0:
    print("WARNING: Duplicate transactions detected")
    duplicate_txn.show()
else:
    print("SUCCESS: No duplicate transactions found")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **STEP 7 — Validate Fraud KPI Aggregate Table**
# 
# #### **Objective**
# Validate that the Gold aggregate table `fraud_summary_daily` was created successfully and contains data.
# 
# #### **Why this validation is important**
# The `gold.fraud_summary_daily` table is a business-facing KPI table used for:
# 
# - fraud monitoring dashboards
# - daily fraud trend analysis
# - risk management reporting
# - operational analytics
# 
# If this table is empty or missing data, then Gold-layer reporting is incomplete.
# 
# #### **Validation performed**
# This step checks:
# 
# 1. the table contains at least one row
# 2. the aggregated output is readable
# 3. the table structure and sample values look correct
# 
# **Expected result**
# The fraud summary table should contain daily records and should not be empty.

# CELL ********************

# ============================================================
# STEP 7: Validate fraud KPI aggregates
# ============================================================

# ------------------------------------------------------------
# Data Validation: Fraud KPI summary table
# ------------------------------------------------------------
# The fraud_summary_daily table contains daily-level fraud
# metrics used by dashboards and business reports.

# Count total rows in the fraud summary table
fraud_rows = df_fraud.count()

print("INFO: Fraud summary table row count:", fraud_rows)

# If no rows exist, log a warning
if fraud_rows == 0:
    print("WARNING: fraud_summary_daily table is empty")
else:
    print("SUCCESS: fraud_summary_daily table contains data")

# Display first 5 rows for visual validation
df_fraud.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **STEP 8 — Final Data Quality Status Check**
# 
# #### **Objective**
# Evaluate the final outcome of all data quality checks and decide whether the notebook should pass or fail.
# 
# #### **Why this step is important**
# Individual validation steps help identify issues, but the pipeline needs one final decision point.
# 
# This step consolidates the results of previous validations and determines whether:
# 
# - the notebook should complete successfully
# - the pipeline should fail due to critical data quality issues
# 
# #### **Validation rules applied**
# This step uses the results from earlier checks:
# 
# 1. NULL values in critical fact table foreign keys
# 2. Duplicate transaction IDs
# 3. Fraud KPI table availability
# 
# #### **Failure strategy**
# - Critical failures cause the notebook to raise an exception
# - Duplicate transaction IDs are logged as warnings only for this PaySim project
# - If critical checks pass, the notebook returns success
# 
# **Expected behavior**
# - If any mandatory rule fails → notebook fails
# - If mandatory rules pass → notebook succeeds


# CELL ********************

# ============================================================
# STEP 8: Final Data Quality (DQ) status check and pipeline fail logic
# ============================================================

# Informational log to indicate the start of the final DQ validation stage
print("INFO: Starting final DQ status evaluation...")

# List to collect all DQ rule failures.
# If any rule fails, the pipeline will stop at the end.
dq_failures = []

# ------------------------------------------------------------
# Rule 1: Check results from NULL key validation
# ------------------------------------------------------------
# The NULL checks were calculated earlier in STEP 5 and stored
# in a dataframe called `null_checks`.
# We collect the results to the driver because it contains only
# aggregated counts for each column.

null_result = null_checks.collect()[0]

# If the number of NULL values in date_key is greater than 0,
# add it to the failure list.
if null_result["null_date_key"] > 0:
    dq_failures.append(f"null_date_key = {null_result['null_date_key']}")

# Validate that origin_account does not contain NULL values
if null_result["null_origin_account"] > 0:
    dq_failures.append(f"null_origin_account = {null_result['null_origin_account']}")

# Validate that destination_account does not contain NULL values
if null_result["null_destination_account"] > 0:
    dq_failures.append(f"null_destination_account = {null_result['null_destination_account']}")

# Validate that transaction_type does not contain NULL values
if null_result["null_transaction_type"] > 0:
    dq_failures.append(f"null_transaction_type = {null_result['null_transaction_type']}")

# ------------------------------------------------------------
# Rule 2: Check duplicate transaction result from STEP 6
# ------------------------------------------------------------
# `duplicate_count` was calculated earlier in STEP 6.
# In this PaySim project duplicates are treated as warnings only.
# They are logged but DO NOT stop the pipeline execution.

if duplicate_count > 0:
    print(f"WARNING: duplicate_txn_count = {duplicate_count}")

# ------------------------------------------------------------
# Rule 3: Check fraud KPI validation result from STEP 7
# ------------------------------------------------------------
# `fraud_rows` represents the number of rows generated in the
# fraud_summary_daily table.
# If the table is empty, something went wrong in earlier steps,
# so we mark this as a pipeline failure.

if fraud_rows == 0:
    dq_failures.append("fraud_summary_daily is empty")

# ------------------------------------------------------------
# Final DQ outcome
# ------------------------------------------------------------
# If any rule added an entry to dq_failures,
# the pipeline should fail and report all issues.

if len(dq_failures) > 0:

    print("ERROR: Data quality validation failed")

    # Print each failure reason for debugging
    for issue in dq_failures:
        print(" -", issue)

    # Raise an exception to stop the pipeline execution
    raise Exception("DATA QUALITY FAILURE: " + "; ".join(dq_failures))

# If no failures were detected, the pipeline succeeds
else:
    print("SUCCESS: All mandatory data quality checks passed")

    # Log informational metrics for monitoring
    print(f"INFO: Duplicate transaction warning count = {duplicate_count}")
    print(f"INFO: Fraud summary rows = {fraud_rows}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **STEP 9 — Insert Pipeline Audit Record**
# 
# #### **Objective**
# Write one audit record into `meta.pipeline_run_audit` after the data quality notebook completes successfully.
# 
# #### **Why this step is needed**
# Enterprise pipelines should maintain a run history for monitoring and troubleshooting.
# 
# This audit record captures:
# 
# - pipeline run id
# - pipeline name
# - environment
# - run start time
# - run end time
# - status
# - trigger type
# 
# #### **Expected behavior**
# This step should run only after all mandatory DQ checks have passed.
# If the DQ guard fails, this audit step will not execute.

# CELL ********************

# ============================================================
# STEP 9: Insert pipeline audit record
# ============================================================

# This step records pipeline execution metadata in the
# `meta.pipeline_run_audit` table.
# The audit record is written only after all mandatory
# data quality checks have passed successfully.

# Convert the pipeline run id parameter to string
# (in case it was passed as another type)
run_id = str(p_pipeline_run_id)

# Name of the pipeline being executed
pipeline_name = "pl_finance_e2e_batch"

# Environment in which the pipeline is running
# (e.g., dev / test / prod)
env = str(p_env)

# Pipeline start time captured earlier at the beginning of the notebook
run_start_time = notebook_start_time

# Capture pipeline completion time
run_end_time = datetime.now()

# Since this step runs only after successful execution,
# the pipeline status is marked as SUCCESS
status = "SUCCESS"

# Identify how the pipeline was triggered
# If run_id = MANUAL_RUN → triggered manually
# Otherwise it was triggered by an orchestration pipeline
trigger_type = "MANUAL" if run_id == "MANUAL_RUN" else "PIPELINE"

# Timestamp when the audit record is created
created_at = datetime.now()

# ------------------------------------------------------------
# Create one audit record
# ------------------------------------------------------------
# Spark Row object representing one pipeline run record.
# This contains all metadata needed for monitoring and auditing.

audit_row = Row(
    run_id=run_id,
    pipeline_name=pipeline_name,
    env=env,
    run_start_time=run_start_time,
    run_end_time=run_end_time,
    status=status,
    trigger_type=trigger_type,
    created_at=created_at
)

# ------------------------------------------------------------
# Convert the Row object into a Spark DataFrame
# ------------------------------------------------------------
# Spark writes data using DataFrames, so we convert the
# single Row into a DataFrame containing one record.

audit_df = spark.createDataFrame([audit_row])

# ------------------------------------------------------------
# Write audit record into the audit table
# ------------------------------------------------------------
# mode("append") ensures we add a new row for every pipeline run
# instead of overwriting previous records.

audit_df.write.mode("append").saveAsTable("meta.pipeline_run_audit")

# Log success message for pipeline monitoring
print("SUCCESS: Pipeline audit record inserted")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **STEP 10 — Validate Audit Table**
# 
# #### **Objective**
# Verify that the pipeline audit record was written successfully.
# 
# #### **Why this step is needed**
# This final check confirms that audit logging is working and that the current pipeline run has been captured in the monitoring table.

# CELL ********************

# ============================================================
# STEP 10: Validate audit table
# ============================================================

spark.table("meta.pipeline_run_audit").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **STEP 11 — Insert Notebook Activity Audit Record**
# 
# #### **Objective**
# Write one notebook-level execution record into `meta.pipeline_activity_audit`.
# 
# #### **Why this step is needed**
# The pipeline run audit table captures only one row per pipeline.
# 
# The activity audit table captures one row per notebook, which helps with:
# 
# - notebook-level monitoring
# - execution time analysis
# - failure troubleshooting
# - identifying slow pipeline steps
# 
# #### **Scope**
# This record represents the execution of the current notebook:
# 
# `nb_05_data_quality_checks`

# CELL ********************

# ============================================================
# STEP 11: Write notebook activity audit record
# ============================================================

# Import required data types for explicit schema definition
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType
)

# ------------------------------------------------------------
# Define notebook activity details
# ------------------------------------------------------------

# Name of the current notebook activity
activity_name = "nb_05_data_quality_checks"

# Layer name for monitoring
layer = "dq"

# Start and end time of notebook execution
activity_start_time = notebook_start_time
activity_end_time = datetime.now()

# Notebook execution status (default is SUCCESS)
activity_status = "SUCCESS"

# Keep error message blank because notebook succeeded (will capture in case of failure)
activity_error_message = ""

# Calculate total runtime in seconds
duration_seconds = float((activity_end_time - activity_start_time).total_seconds())

# ------------------------------------------------------------
# Define schema explicitly to avoid PySpark inference issues
# ------------------------------------------------------------
activity_audit_schema = StructType([
    StructField("run_id", StringType(), True),
    StructField("pipeline_name", StringType(), True),
    StructField("activity_name", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("error_message", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

# ------------------------------------------------------------
# Try to execute the notebook logic and handle errors
# ------------------------------------------------------------
try:
    # Create activity audit data
    activity_audit_data = [(
        str(p_pipeline_run_id),
        "pl_finance_e2e_batch",
        activity_name,
        layer,
        activity_start_time,
        activity_end_time,
        activity_status,
        duration_seconds,
        activity_error_message,
        datetime.now()
    )]

    # Convert activity audit data into Spark DataFrame
    activity_audit_df = spark.createDataFrame(
        activity_audit_data,
        schema=activity_audit_schema
    )

    # Append audit record into activity audit table
    activity_audit_df.write.mode("append").saveAsTable("meta.pipeline_activity_audit")

    # Print success message (or use logging for more production-grade output)
    print("SUCCESS: Notebook activity audit record inserted")

except Exception as e:
    # If something goes wrong, mark the activity as FAILED and capture the error
    activity_status = "FAILED"
    activity_error_message = str(e)
    
    # Log the failure in the audit table (still insert the record)
    activity_audit_data = [(
        str(p_pipeline_run_id),
        "pl_finance_e2e_batch",
        activity_name,
        layer,
        activity_start_time,
        activity_end_time,
        activity_status,
        duration_seconds,
        activity_error_message,
        datetime.now()
    )]

    # Convert activity audit data into Spark DataFrame and insert
    activity_audit_df = spark.createDataFrame(
        activity_audit_data,
        schema=activity_audit_schema
    )

    activity_audit_df.write.mode("append").saveAsTable("meta.pipeline_activity_audit")

    # Log the error
    print(f"ERROR: Notebook activity failed with error: {activity_error_message}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP 12: Validate notebook activity audit table
# ============================================================

spark.table("meta.pipeline_activity_audit").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
