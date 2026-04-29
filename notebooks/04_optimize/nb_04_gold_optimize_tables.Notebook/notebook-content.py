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

# #### **Read pipeline parameters**

# CELL ********************

# ============================================================
# STEP 0: Retrieve Pipeline Parameters
# Purpose: Read the pipeline run ID passed from the orchestration
# pipeline. If the notebook is executed manually (outside the
# pipeline), assign a default identifier for tracking.
# ============================================================

try:
    # Fetch pipeline run ID from notebook parameters
    p_pipeline_run_id = mssparkutils.notebook.params.get("p_pipeline_run_id")
except:
    # Default value when notebook is executed manually
    p_pipeline_run_id = "MANUAL_RUN"


# ============================================================
# STEP 1: Initialize Activity Monitoring
# Purpose: Capture execution metadata such as start time,
# pipeline name, activity name, and processing layer.
# This information is used for pipeline monitoring,
# logging, and auditing.
# ============================================================

from datetime import datetime

# Record the activity start time
activity_start_time = datetime.now()

# Define pipeline and activity metadata
pipeline_name = "pl_finance_e2e_batch"
activity_name = "nb_04_gold_optimize_tables"
layer = "optimize"

# Print execution details for tracking and troubleshooting
print("Activity monitoring initialized")
print("Run ID:", p_pipeline_run_id)
print("Activity:", activity_name)
print("Start Time:", activity_start_time)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Gold Layer – Optimize Delta Tables**
# 
# #### **Objective**
# Perform post-load maintenance on Gold tables after the full Bronze → Silver → Gold pipeline has completed successfully.
# 
# #### **Why this notebook is needed**
# In enterprise Delta pipelines, repeated writes can create:
# 
# - too many small files
# - slower query performance
# - inefficient scans for Power BI and downstream reporting
# 
# This notebook is used to improve performance and maintain table health after data load completion.
# 
# #### **What this notebook does**
# This notebook will:
# 
# 1. identify important Gold tables
# 2. optimize Delta storage layout
# 3. improve read/query performance
# 4. prepare the model for downstream analytics
# 
# #### **Target layer**
# Gold
# 
# #### **Execution position in pipeline**
# This notebook runs after:
# 
# - Bronze ingestion
# - Silver transformation
# - Gold model build
# 
# **Notebook name**
# `nb_04_gold_optimize_tables`


# CELL ********************

# This list contains the important tables from the "gold" layer
# that will be optimized in later steps of the notebook.
# The Gold layer typically contains curated, business-ready data
# used for analytics, dashboards, and reporting.

gold_tables = [
    "gold.dim_transaction_type",      # Dimension table containing different transaction types
    "gold.dim_date",                  # Date dimension table used for time-based analysis
    "gold.dim_account",               # Dimension table storing account-level details
    "gold.dim_account_profile_scd2",  # Slowly Changing Dimension Type 2 table for account profile history
    "gold.fact_transactions",         # Fact table storing all transaction records
    "gold.fraud_summary_daily",       # Daily aggregated fraud detection summary
    "gold.exec_kpi_daily",            # Daily executive KPIs for business monitoring
    "gold.ops_data_quality_daily"     # Daily operational data quality metrics
]

# Print confirmation message so the user knows which tables
# are selected for optimization in this notebook run.
print("INFO: Gold tables selected for optimization:")

# Loop through the list of tables and print each table name
# This helps verify the tables before running optimization steps.
for table_name in gold_tables:
    print("-", table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Optimize Delta tables**


# CELL ********************

# This step runs the Delta Lake OPTIMIZE command on each table
# listed in the "gold_tables" list.
# OPTIMIZE compacts small files into larger files which improves
# query performance and reduces file system overhead.

print("INFO: Starting Delta table optimization process...")

# Loop through each table defined in Step 1
for table_name in gold_tables:
    
    # Print which table is currently being optimized
    # This helps track progress during execution
    print(f"INFO: Optimizing table -> {table_name}")
    
    try:
        # Execute the Delta Lake OPTIMIZE command using Spark SQL
        # This compacts small files into fewer larger files
        spark.sql(f"OPTIMIZE {table_name}")
        
        # Print success message once optimization completes
        print(f"SUCCESS: Optimization completed for {table_name}")
        
    except Exception as e:
        # If optimization fails for a table, the exception is caught
        # This prevents the entire notebook from stopping and allows
        # the process to continue with the remaining tables
        
        print(f"ERROR: Optimization failed for {table_name}")
        
        # Print the error message for debugging and troubleshooting
        print(str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Activity Audit Record**

# CELL ********************

# ============================================================
# STEP: Write Notebook Activity Audit Record
# Purpose: Log the execution details of the current notebook
# activity into the pipeline activity audit table. This helps
# track execution status, runtime duration, and operational
# monitoring for pipeline activities.
# ============================================================

from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

# Capture the activity end time
activity_end_time = datetime.now()

# Calculate total execution duration in seconds
duration_seconds = float((activity_end_time - activity_start_time).total_seconds())

# Define schema for the pipeline activity audit record
audit_schema = StructType([
    StructField("run_id", StringType(), True),          # Unique pipeline run identifier
    StructField("pipeline_name", StringType(), True),   # Name of the pipeline
    StructField("activity_name", StringType(), True),   # Notebook or pipeline activity name
    StructField("layer", StringType(), True),           # Processing layer (e.g., bronze/silver/gold/optimize)
    StructField("start_time", TimestampType(), True),   # Activity start timestamp
    StructField("end_time", TimestampType(), True),     # Activity end timestamp
    StructField("status", StringType(), True),          # Execution status (SUCCESS/FAILED)
    StructField("duration_seconds", DoubleType(), True),# Total runtime of the activity
    StructField("error_message", StringType(), True),   # Error message if execution fails
    StructField("created_at", TimestampType(), True)    # Audit record creation timestamp
])

# Prepare audit record data
audit_data = [
    (
        str(p_pipeline_run_id),   # Pipeline run ID
        str(pipeline_name),       # Pipeline name
        str(activity_name),       # Activity/notebook name
        str(layer),               # Processing layer
        activity_start_time,      # Start time
        activity_end_time,        # End time
        "SUCCESS",                # Execution status
        duration_seconds,         # Execution duration in seconds
        None,                     # No error since execution succeeded
        datetime.now()            # Record creation timestamp
    )
]

# Create Spark DataFrame for audit logging
audit_df = spark.createDataFrame(audit_data, schema=audit_schema)

# Append audit record to the pipeline activity audit table
audit_df.write.mode("append").saveAsTable("meta.pipeline_activity_audit")

# Confirmation message for logging
print("SUCCESS: Optimize notebook activity audit record inserted")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Validation**

# CELL ********************

spark.sql("""
SELECT *
FROM meta.pipeline_activity_audit
WHERE activity_name = 'nb_04_gold_optimize_tables'
ORDER BY start_time DESC
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
