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

# #### **Imports Library** 

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import uuid

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Read Pipeline Parameters**

# CELL ********************

# ===============================
# Read pipeline parameters (Enterprise)
# ===============================

# Function to fetch parameter from dbutils.widgets and return it, or None if not found
def _get_param(name):
    try:
        return dbutils.widgets.get(name)  # Try to get the parameter value
    except Exception:
        return None  # Return None if there is an error in fetching the parameter

# Fetching parameters for environment, load type, source file, and pipeline run ID
p_env             = _get_param("p_env")              # Environment parameter (e.g., DEV, PROD)
p_load_type       = _get_param("p_load_type")        # Load type (e.g., FULL, INCREMENTAL)
p_source_file     = _get_param("p_source_file")      # Source file to be processed
p_pipeline_run_id = _get_param("p_pipeline_run_id")  # Pipeline run ID

# Check if the pipeline run is manual (i.e., no pipeline ID or manually triggered)
is_manual = (p_pipeline_run_id is None) or (str(p_pipeline_run_id).strip() == "") or (str(p_pipeline_run_id).upper() == "MANUAL_RUN")

if is_manual:
    # If manual run is detected, set default values for development environment
    print("⚠️ Manual notebook run detected — using dev defaults")
    p_env = "DEV"                              # Default environment as DEV
    p_load_type = "FULL"                       # Default load type as FULL
    p_source_file = "paysim_transactions_full_20260214.csv"  # Default source file
    p_pipeline_run_id = "MANUAL_RUN"           # Set pipeline run ID to 'MANUAL_RUN' for tracking
else:
    # If not a manual run, perform strict validation to ensure all required parameters are provided
    required = {
        "p_env": p_env,                         # Environment parameter
        "p_load_type": p_load_type,             # Load type parameter
        "p_source_file": p_source_file,         # Source file parameter
        "p_pipeline_run_id": p_pipeline_run_id  # Pipeline run ID parameter
    }
    
    # Check for any missing parameters
    missing = [k for k, v in required.items() if v is None or str(v).strip() == ""]
    
    if missing:
        # Raise an exception if any required parameters are missing
        raise Exception(f"Missing required parameters: {missing}")

    # ✅ Normalize AFTER manual/default logic
    p_env = str(p_env).upper()
    p_load_type = str(p_load_type).upper()

    # ✅ Allowed value validation (put here)
    if p_env not in ["DEV","UAT","PROD"]:
        raise Exception(f"Invalid p_env: {p_env}")
    
    if p_load_type not in ["FULL","INCR"]:
        raise Exception(f"Invalid p_load_type: {p_load_type}")
        
# If all parameters are valid, print the successfully loaded parameters
print("✅ Parameters Loaded:")
print("p_env =", p_env)               # Print the environment value
print("p_load_type =", p_load_type)   # Print the load type value
print("p_source_file =", p_source_file)  # Print the source file name
print("p_pipeline_run_id =", p_pipeline_run_id)  # Print the pipeline run ID

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Initialize activity monitoring**

# CELL ********************

# ============================================================
# STEP 1: Initialize activity monitoring
# ============================================================

from datetime import datetime

activity_start_time = datetime.now()

pipeline_name = "pl_finance_e2e_batch"
activity_name = "nb_01_bronze_paysim_ingest_full"
layer = "bronze"

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

#  #### **Read Watermark**

# CELL ********************

# ===============================
# Read Watermark (Enterprise-safe)
# ===============================

# Define the pipeline and entity (table) names
pipeline_name = "pl_finance_e2e_batch"
entity_name   = "paysim_transactions"

# Query the watermark table to get the last successful load timestamp
# This helps us know from which point we should process new data
watermark_row = spark.sql(f"""
    SELECT last_success_ts
    FROM meta.etl_watermark
    WHERE pipeline_name = '{pipeline_name}'
      AND entity_name   = '{entity_name}'
""").limit(1).collect()  # Limit to 1 record and collect result into a list

# Check if no record was returned from the watermark table
# If empty, it means this pipeline/entity was never registered or loaded before
if len(watermark_row) == 0:
    raise Exception(
        f"❌ No watermark record found for pipeline_name='{pipeline_name}' "
        f"and entity_name='{entity_name}'"
    )

# Extract the last_success_ts value from the first row of the result
last_success_ts = watermark_row[0]["last_success_ts"]

# Print the timestamp so we can verify what value is being used
print("✅ Last Success Timestamp:", last_success_ts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Load CSV File and Perform Initial Data Validation (Row Count, Schema, Preview)**

# MARKDOWN ********************

# ###### **Bronze Layer – Raw Source Ingestion (Landing to Delta)**
# 
# ###### **Purpose**
# Load raw PaySim transaction data from the landing zone into the Bronze Delta table.
# 
# ###### **Why Bronze?**
# Bronze stores raw, untransformed data with ingestion metadata to enable:
# - Data lineage
# - Reprocessing
# - Auditability
# - Traceability
# 
# ###### **Notes**
# - No business transformations are applied here.
# - No incremental filtering is applied in Bronze.
# - Incremental logic is handled in Silver layer.

# CELL ********************

# ===============================
# CELL-04: Load Source File (Bronze Layer)
# ===============================

# ---------------------------------
# Validate that pipeline parameter is loaded (Cell-02)
# ---------------------------------
if "p_source_file" not in globals() or p_source_file is None or str(p_source_file).strip() == "":
    raise Exception("❌ Parameters not loaded. Run Cell-02 (Read Pipeline Parameters) first.")

# ---------------------------------
# Build dynamic file path
# ---------------------------------
file_path = f"Files/landing/paysim/{p_source_file}"
print("📂 Loading file:", file_path)

# ---------------------------------
# Read CSV file into Spark DataFrame
# ---------------------------------
try:
    df_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")   # keep for now
        .csv(file_path)
    )
except Exception as e:
    raise Exception(f"❌ Failed to read source file at '{file_path}'. Error: {str(e)[:300]}")

# ---------------------------------
# Basic validation
# ---------------------------------
row_count = df_raw.count()
if row_count == 0:
    raise Exception(f"❌ Source file '{p_source_file}' contains 0 rows.")

print("✅ Rows Loaded:", row_count)

# ---------------------------------
# Schema + sample preview
# ---------------------------------
df_raw.printSchema()
df_raw.show(5, truncate=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Bronze Layer – Persist Raw Transactions with Ingestion Metadata**
# 
# ##### **Objective**
# Persist raw PaySim transaction data into the Bronze layer using Delta Lake while enriching it with ingestion metadata for traceability, auditability, and lineage tracking.
# 
# ---
# 
# ##### **Why Bronze?**
# 
# The Bronze layer is the raw landing zone of the Medallion Architecture.
# 
# It is designed to:
# 
# - Store data exactly as received from the source
# - Preserve historical raw records
# - Enable reprocessing and replay scenarios
# - Provide audit and compliance traceability
# 
# No business transformations or cleansing logic is applied at this stage.
# 
# ---
# 
# ##### **Why Are We Adding Metadata Columns?**
# 
# In enterprise data platforms, raw data must always be traceable.
# If a pipeline fails, reruns, or audits are required, we must know:
# 
# - When the data was ingested
# - Which pipeline run processed the data
# - Which source file generated the data
# - Whether the load was FULL or INCREMENTAL
# - Which environment processed the data
# 
# This ensures strong data lineage and operational transparency.
# 
# ---
# 
# ##### **Metadata Columns Added**
# 
# - `_ingest_ts` → Timestamp when the record was ingested
# - `_batch_id` → Unique identifier for this ingestion execution
# - `_source_file` → Name of the source file from the landing zone
# - `_ingest_mode` → FULL or INCR (based on pipeline parameter)
# - `_env` → Environment identifier (DEV / UAT / PROD)
# 
# ---
# 
# ##### **Why Use UUID for Batch ID?**
# 
# A UUID guarantees that each ingestion run is uniquely identifiable — even if:
# 
# - The same file is reprocessed
# - A rerun occurs after failure
# - Multiple loads happen in the same day
# 
# This supports:
# 
# - Replayability
# - Operational debugging
# - Audit traceability
# - Batch-level tracking
# 
# ---
# 
# ##### **Why Delta Lake?**
# 
# Delta Lake provides:
# 
# - ACID transaction guarantees
# - Schema enforcement
# - Time travel capability
# - Reliable append-based ingestion
# - Scalability for millions of records
# 
# ---
# 
# ##### **Bronze Write Strategy (Locked Rule Book)**
# 
# - Bronze follows an append-only strategy
# - Raw data is preserved as-is
# - No incremental filtering is applied in Bronze
# - Incremental logic is implemented in the Silver layer
# - Watermark updates occur only after successful Silver processing


# CELL ********************

#Transform and Load Data into Bronze Delta Table with Metadata

# Generate a unique batch ID for this ingestion
batch_id = str(uuid.uuid4())

# Add ingestion metadata columns to the DataFrame
df_bronze = (
    df_raw
    .withColumn("_ingest_ts", F.current_timestamp())  # Timestamp when the data is ingested
    .withColumn("_batch_id", F.lit(batch_id))         # UUID Batch ID (enterprise-safe)
    .withColumn("_source_file", F.lit(p_source_file)) # Dynamic source file
    .withColumn("_ingest_mode", F.lit(p_load_type))   # FULL / INCR from parameter
    .withColumn("_env", F.lit(p_env))                 # DEV / UAT / PROD
)


print("✅ Metadata columns added.")
print("✅ Bronze metadata added. Batch ID:", batch_id)
df_bronze.select("_batch_id","_ingest_mode","_env").show(3, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **Bronze Table – Append Raw Transactions (PaySim)**
# 
# ###### **Objective**
# Persist enriched raw transaction data into the Bronze Delta table using an append-only strategy to maintain full historical traceability.
# 
# ---
# 
# ###### **Why Append-Only in Bronze?**
# 
# In enterprise data platforms, the Bronze layer represents the raw landing zone.
# 
# It must:
# 
# - Preserve every ingestion event
# - Maintain full historical data
# - Support audit and compliance requirements
# - Allow replay and reprocessing if downstream layers fail
# - Ensure no loss of raw source data
# 
# Overwriting Bronze would break audit lineage and eliminate the ability to reprocess historical data.
# 
# Therefore, Bronze follows a strict append-only pattern.
# 
# ---
# 
# ###### **Why Not Apply FULL vs INCREMENTAL Logic Here?**
# 
# Bronze is responsible only for raw ingestion.
# 
# - No business filtering is applied
# - No deduplication is performed
# - No incremental filtering is implemented
# 
# Incremental logic belongs to the Silver layer, where data cleansing and transformation occur.
# 
# ---
# 
# ###### **What Happens in This Step?**
# 
# - The enriched dataset (`df_bronze`) is written to the Bronze Delta table
# - Data is appended to preserve historical records
# - ACID guarantees are provided by Delta Lake
# 
# ---
# 
# ###### **Enterprise Design Principle**
# 
# Bronze = Raw + Immutable + Replayable  
# Silver = Clean + Standardized + Incremental  
# Gold = Business Ready + Analytical  
# 
# This separation ensures scalability, auditability, and operational resilience.


# CELL ********************

# ===============================
# Bronze Table – Append Raw Transactions
# ===============================

bronze_table = "bronze.paysim_transactions_raw"

(df_bronze.write
    .format("delta")
    .mode("append")   # Bronze is append-only (enterprise standard)
    .saveAsTable(bronze_table))

print("✅ Bronze appended:", bronze_table)
# Raw Validation
spark.table("bronze.paysim_transactions_raw").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **Logging – Bronze Step Execution (Pipeline Run Log)**
# 
# ###### **Objective**
# Capture an audit record for the Bronze ingestion step so we can monitor pipeline executions, debug failures, and provide operational traceability.
# 
# ---
# 
# ###### **Why do we log this step?**
# 
# In enterprise ETL pipelines, every run must be traceable. Logging enables:
# 
# - Visibility of pipeline health (SUCCESS/FAILED)
# - Row counts for validation (rows_read vs rows_written)
# - Run-level traceability (linking data batches to pipeline runs)
# - Faster troubleshooting when failures occur
# - Inputs for monitoring dashboards (Power BI / Fabric monitoring)
# 
# ---
# 
# ###### **What gets logged?**
# 
# This step writes **one row per Bronze execution** into `log.pipeline_run_log` with:
# 
# - `run_id` → Fabric pipeline run id (`p_pipeline_run_id`)
# - `pipeline_name` → step identifier (`pl_ingest_paysim`)
# - `batch_id` → UUID for this ingestion execution
# - `env` → DEV/UAT/PROD
# - `start_ts`, `end_ts`, `created_at` → operational timestamps
# - `status` → SUCCESS (or FAILED in error branch later)
# - `rows_read`, `rows_written` → basic reconciliation metrics
# - `error_message` → populated only when failures occur
# 
# ---
# 
# ###### **Enterprise Note**
# - We always use `p_pipeline_run_id` so logs match Fabric pipeline executions.
# - We reuse the existing log table schema to avoid schema mismatch issues.
# - This logging step does not modify Bronze/Silver/Gold data—only operational metadata.


# CELL ********************

# ===============================
# Log Pipeline Execution Details (Enterprise + Stable)
# ===============================

# Use Fabric pipeline run id (DO NOT generate a new UUID here)
run_id = str(p_pipeline_run_id)

# Use dataframe counts (cheaper, and consistent)
rows_read = int(df_raw.count())
rows_written = int(df_bronze.count())

# Prepare a log record as a tuple (must match log table schema order)
log_entry = [(
    run_id,                   # run_id = pipeline run id
    "pl_ingest_paysim",       # pipeline/step name
    str(batch_id),            # UUID batch id from ingestion
    str(p_env),               # env from parameter
    None,                     # start_ts (populated below)
    None,                     # end_ts (populated below)
    "SUCCESS",                # status
    rows_read,                # rows_read
    rows_written,             # rows_written
    None,                     # error_message
    None                      # created_at
)]

# Get schema from existing pipeline log table (most stable approach)
log_schema = spark.table("log.pipeline_run_log").schema

df_log = spark.createDataFrame(log_entry, schema=log_schema) \
    .withColumn("start_ts", F.current_timestamp()) \
    .withColumn("end_ts", F.current_timestamp()) \
    .withColumn("created_at", F.current_timestamp())

df_log.write.format("delta").mode("append").saveAsTable("log.pipeline_run_log")

print("✅ Pipeline run logged successfully. run_id =", run_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Write notebook activity audit record**

# CELL ********************

# ============================================================
# STEP X: Write notebook activity audit record
# This step logs execution details of the current notebook
# activity into the audit table for monitoring and debugging.
# ============================================================

# Import required Spark SQL data types for defining schema
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType
)

# Import datetime module to capture timestamps
from datetime import datetime

# Capture notebook/activity end time
# This marks when the current notebook activity finished execution
activity_end_time = datetime.now()

# Mark current activity execution status
# If the notebook reaches here without exception, mark as SUCCESS
activity_status = "SUCCESS"

# Keep error message blank because execution completed successfully
# This field will contain error details in case of failure
activity_error_message = ""

# Calculate activity execution duration in seconds
# Subtract start time from end time and convert to seconds
duration_seconds = float((activity_end_time - activity_start_time).total_seconds())

# Explicitly define schema for the activity audit DataFrame
# This ensures correct column types when writing to the audit table
activity_audit_schema = StructType([
    StructField("run_id", StringType(), True),          # Unique pipeline run identifier
    StructField("pipeline_name", StringType(), True),   # Name of the pipeline
    StructField("activity_name", StringType(), True),   # Name of the notebook/activity
    StructField("layer", StringType(), True),           # Data layer (Bronze/Silver/Gold)
    StructField("start_time", TimestampType(), True),   # Activity start timestamp
    StructField("end_time", TimestampType(), True),     # Activity end timestamp
    StructField("status", StringType(), True),          # Execution status (SUCCESS/FAILED)
    StructField("duration_seconds", DoubleType(), True),# Total execution time in seconds
    StructField("error_message", StringType(), True),   # Error message if failure occurs
    StructField("created_at", TimestampType(), True)    # Audit record creation timestamp
])

# Prepare a single activity audit record as a tuple
# Values correspond to the schema defined above
activity_audit_data = [(
    str(p_pipeline_run_id),     # Pipeline run ID
    pipeline_name,              # Pipeline name
    activity_name,              # Activity/notebook name
    layer,                      # Data layer being processed
    activity_start_time,        # Start time of activity
    activity_end_time,          # End time of activity
    activity_status,            # Execution status
    duration_seconds,           # Total duration
    activity_error_message,     # Error message (blank if success)
    datetime.now()              # Record creation timestamp
)]

# Convert the activity audit record into a Spark DataFrame
activity_audit_df = spark.createDataFrame(
    activity_audit_data,
    schema=activity_audit_schema
)

# Append the audit record into the pipeline activity audit table
# Using append mode ensures previous records are preserved
activity_audit_df.write.mode("append").saveAsTable("meta.pipeline_activity_audit")

# Print confirmation message after successful insert
print("SUCCESS: Bronze notebook activity audit record inserted")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Validation**

# CELL ********************

spark.table("meta.pipeline_activity_audit") \
.filter("activity_name = 'nb_01_bronze_paysim_ingest_full'") \
.show(truncate=False)

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
