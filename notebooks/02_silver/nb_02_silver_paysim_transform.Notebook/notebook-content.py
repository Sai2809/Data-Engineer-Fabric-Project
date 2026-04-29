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

# #### **import library**

# CELL ********************

# Import PySpark SQL functions (like col, sum, max, when, etc.)
# We use "F" as a short alias to make code cleaner (e.g., F.col("column_name"))
from pyspark.sql import functions as F

# Import Window functions
# Used for operations like row_number(), rank(), lead(), lag() over partitions
from pyspark.sql.window import Window

# Import datetime module
# Used to work with dates and timestamps (e.g., current time, formatting dates)
from datetime import datetime

# Import uuid module
# Used to generate unique IDs (for example, unique transaction IDs or batch IDs)
import uuid

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Read pipeline parameters**

# CELL ********************

# ===============================
# CELL-02: Read pipeline parameters (Enterprise)
# ===============================

# Function to safely read notebook parameters (Databricks widgets)
# If the parameter does not exist, return None instead of failing
def _get_param(name):
    try:
        return dbutils.widgets.get(name)  # Get widget value
    except Exception:
        return None  # Return None if widget is missing

# Read all expected parameters from the notebook widgets
p_env             = _get_param("p_env")               # Environment (DEV/UAT/PROD)
p_load_type       = _get_param("p_load_type")         # Load type (FULL/INCR)
p_source_file     = _get_param("p_source_file")       # Input source file name
p_pipeline_run_id = _get_param("p_pipeline_run_id")   # Unique pipeline execution ID

# Detect if this notebook is being run manually (not from pipeline)
# Conditions for manual run:
# - pipeline_run_id is None
# - pipeline_run_id is empty
# - pipeline_run_id equals "MANUAL_RUN"
is_manual = (
    (p_pipeline_run_id is None) or 
    (str(p_pipeline_run_id).strip() == "") or 
    (str(p_pipeline_run_id).upper() == "MANUAL_RUN")
)

# If manual execution, assign default DEV values
if is_manual:
    print("⚠️ Manual notebook run detected — using dev defaults")
    
    p_env = "DEV"  # Default environment
    p_load_type = "FULL"  # Default load type
    p_source_file = "paysim_transactions_full_20260214.csv"  # Default file
    p_pipeline_run_id = "MANUAL_RUN"  # Default run ID

# If pipeline execution, validate required parameters
else:
    # Create dictionary of required parameters
    required = {
        "p_env": p_env,
        "p_load_type": p_load_type,
        "p_source_file": p_source_file,
        "p_pipeline_run_id": p_pipeline_run_id
    }

    # Check for missing or empty values
    missing = [k for k, v in required.items() if v is None or str(v).strip() == ""]

    # If any required parameter is missing, raise error
    if missing:
        raise Exception(f"Missing required parameters: {missing}")

# Normalize values AFTER validation/default assignment
# Convert to uppercase for consistency
p_env = str(p_env).upper()
p_load_type = str(p_load_type).upper()

# Validate allowed environment values
if p_env not in ["DEV", "UAT", "PROD"]:
    raise Exception(f"Invalid p_env: {p_env}")

# Validate allowed load type values
if p_load_type not in ["FULL", "INCR"]:
    raise Exception(f"Invalid p_load_type: {p_load_type}")

# Print final parameter values for verification/logging
print("✅ Parameters Loaded:")
print("p_env =", p_env)
print("p_load_type =", p_load_type)
print("p_source_file =", p_source_file)
print("p_pipeline_run_id =", p_pipeline_run_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Initialize activity monitoring**

# CELL ********************

# ============================================================
# Initialize activity monitoring
# ============================================================

from datetime import datetime

activity_start_time = datetime.now()

pipeline_name = "pl_finance_e2e_batch"
activity_name = "nb_02_silver_paysim_transform"
layer = "silver"

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

# #### **Read Watermark (Silver Incremental Control)**

# CELL ********************

# ===============================
# CELL-03: Read Watermark (Silver Incremental Control)
# ===============================

# Define the pipeline and entity (table) names
# These are used to identify the correct watermark record
pipeline_name = "pl_finance_e2e_batch"
entity_name   = "paysim_transactions"

# Read the last successful load timestamp from the watermark table
# This timestamp tells us up to which point data was already processed
watermark_row = spark.sql(f"""
    SELECT last_success_ts
    FROM meta.etl_watermark
    WHERE pipeline_name = '{pipeline_name}'
      AND entity_name   = '{entity_name}'
""").limit(1).collect()   # Get only 1 row and convert result into a Python list

# If no record is found, stop execution
# This prevents accidental full reloads or incorrect incremental logic
if len(watermark_row) == 0:
    raise Exception(
        f"❌ No watermark record found for pipeline_name='{pipeline_name}' "
        f"and entity_name='{entity_name}'"
    )

# Extract the last_success_ts value from the returned row
last_success_ts = watermark_row[0]["last_success_ts"]

# Print the watermark value for logging and debugging
print("✅ Watermark loaded. last_success_ts =", last_success_ts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Read Bronze Table**

# CELL ********************

# ===============================
# CELL-04: Read Bronze Table
# ===============================

# Define the fully qualified Bronze table name
# Format: workspace.database.schema.table
# This table contains raw PaySim transaction data
bronze_table = "ws_fab_finance_de_dev.lh_finance_core.bronze.paysim_transactions_raw"

# Load the Bronze table into a Spark DataFrame
# Spark reads the table metadata from the metastore
df_bronze = spark.table(bronze_table)

# Count total number of records
# Helps verify that data exists and check volume before transformations
print("Bronze Row Count:", df_bronze.count())

# Print schema (column names and data types)
# Important to validate structure before applying business logic
df_bronze.printSchema()

# Display first 5 records
# truncate=False ensures full column values are visible
df_bronze.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Incremental Filter (Silver)**

# CELL ********************

# ===============================
# CELL-06: Incremental Filter (Silver)
# ===============================

from pyspark.sql import functions as F

# -------------------------------------------------------
# Safety Check: Ensure Bronze DataFrame exists
# -------------------------------------------------------
# This prevents execution if the Bronze read cell was not run
try:
    df_bronze
except NameError:
    raise Exception("❌ df_bronze not found. Read Bronze table first (Silver Cell-01/Cell-04).")

# -------------------------------------------------------
# Step 1: Read Watermark (Last Successful Ingestion Timestamp)
# -------------------------------------------------------
# This tells us up to which timestamp data was already processed
watermark_df = spark.sql("""
SELECT last_success_ts
FROM meta.etl_watermark
WHERE pipeline_name = 'pl_finance_e2e_batch'
  AND entity_name   = 'paysim_transactions'
""")

# Validate watermark existence
if watermark_df.count() == 0:
    raise Exception("❌ No watermark record found for pl_finance_e2e_batch / paysim_transactions")

# Extract watermark value
last_success_ts = watermark_df.collect()[0]["last_success_ts"]

print("✅ Watermark last_success_ts =", last_success_ts)

# -------------------------------------------------------
# Step 2: Apply Load Logic (FULL vs INCR)
# -------------------------------------------------------

if p_load_type == "INCR":
    
    # INCREMENTAL MODE:
    # Filter only rows where ingestion timestamp is newer than watermark
    df_bronze_incr = df_bronze.filter(
        F.col("_ingest_ts") > F.lit(last_success_ts)
    )
    
    print("✅ INCR mode: filtering rows where _ingest_ts >", last_success_ts)

else:
    
    # FULL MODE:
    # No filtering — process entire Bronze dataset
    df_bronze_incr = df_bronze
    
    print("✅ FULL mode: no incremental filtering applied")

# -------------------------------------------------------
# Step 3: Validation Checks
# -------------------------------------------------------

# Compare total vs incremental rows
print("Bronze total rows      =", df_bronze.count())
print("Bronze incremental rows=", df_bronze_incr.count())

# Preview incremental dataset (metadata columns only)
df_bronze_incr.select(
    "_ingest_ts",
    "_batch_id",
    "_source_file"
).show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### ****Standardize Column Names****
# 
# In Silver, we rename raw source columns into consistent, business-friendly names.
# This makes downstream modeling (Gold star schema) and reporting much easier and less error-prone.
# 
# We keep Bronze untouched. Silver becomes the canonical clean dataset.


# CELL ********************

# ===============================
# CELL-07: Standardize Columns (Silver)
# ===============================

from pyspark.sql import functions as F

# Select and standardize required columns from incrementally filtered Bronze data
df_s1 = (
    df_bronze_incr.select(

        # --------------------------------------------------
        # Transaction Details
        # --------------------------------------------------

        # Convert step to integer for numeric processing
        F.col("step").cast("int").alias("step"),

        # Rename "type" to business-friendly column name
        F.col("type").alias("transaction_type"),

        # Cast amount to double for financial calculations
        F.col("amount").cast("double").alias("amount"),

        # --------------------------------------------------
        # Origin Customer Details
        # --------------------------------------------------

        F.col("nameOrig").alias("origin_customer_id"),
        F.col("oldbalanceOrg").cast("double").alias("origin_balance_before"),
        F.col("newbalanceOrig").cast("double").alias("origin_balance_after"),

        # --------------------------------------------------
        # Destination Customer Details
        # --------------------------------------------------

        F.col("nameDest").alias("destination_customer_id"),
        F.col("oldbalanceDest").cast("double").alias("destination_balance_before"),
        F.col("newbalanceDest").cast("double").alias("destination_balance_after"),

        # --------------------------------------------------
        # Fraud Indicators
        # --------------------------------------------------

        # Convert fraud flags to integer (0 or 1)
        F.col("isFraud").cast("int").alias("is_fraud"),
        F.col("isFlaggedFraud").cast("int").alias("is_flagged_fraud"),

        # --------------------------------------------------
        # Ingestion Metadata (Critical for Audit & Watermark)
        # --------------------------------------------------

        # These fields help track batch processing and data lineage
        F.col("_ingest_ts"),
        F.col("_batch_id"),
        F.col("_source_file"),
        F.col("_ingest_mode"),
        F.col("_env")
    )
)

# Log total rows after standardization
print("✅ Silver standardization done. Rows =", df_s1.count())

# Display schema to confirm data types and column names
df_s1.printSchema()

# Preview first 5 records for validation
df_s1.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Create Derived Business Columns**
# 
# This step enriches the Silver dataset with derived fields
# required for analytics and dimensional modeling.
# 
# Derived Columns:
# 
# - event_ts → Converted from `step` into a real timestamp
# - txn_id → Synthetic unique transaction identifier
# - transaction_direction → Debit/Credit style classification
# 
# These fields prepare the dataset for Gold layer modeling.


# CELL ********************

# ===============================
# CELL-08: Create Derived Business Columns (Enterprise)
# ===============================

from pyspark.sql import functions as F

# 1) event_ts: derive a timestamp from 'step' (PaySim step behaves like hours)
base_ts = F.to_timestamp(F.lit("2020-01-01 00:00:00"))
df_s2 = (
    df_s1
    .withColumn("event_ts", base_ts + (F.col("step").cast("int") * F.expr("INTERVAL 1 HOURS")))
)

# 2) txn_id: deterministic (stable across reruns) using a hash of business keys
#    NOTE: include columns that define uniqueness for your dataset
df_s2 = df_s2.withColumn(
    "txn_id",
    F.sha2(
        F.concat_ws(
            "||",
            F.col("step").cast("string"),
            F.col("transaction_type").cast("string"),
            F.col("amount").cast("string"),
            F.col("origin_customer_id").cast("string"),
            F.col("destination_customer_id").cast("string"),
            F.col("_source_file").cast("string")
        ),
        256
    )
)

# 3) transaction_direction: business rule
df_s2 = df_s2.withColumn(
    "transaction_direction",
    F.when(F.col("transaction_type").isin("CASH_OUT", "TRANSFER"), F.lit("DEBIT"))
     .otherwise(F.lit("CREDIT"))
)

print("✅ Derived columns added: event_ts, txn_id (stable hash), transaction_direction")
df_s2.select("step","transaction_type","amount","event_ts","txn_id","transaction_direction").show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **DQ checks + split**
# 
# #### *Data Quality Validation & Reject Handling*
# 
# This step enforces basic data quality rules in the Silver layer.
# 
# We split records into:
# - Valid records → written to `silver.paysim_transactions_clean`
# - Invalid records → written to `dq.paysim_rejects` with rejection reasons
# 
# This ensures reporting and Gold modeling never consume bad data,
# while still preserving rejected records for investigation.


# CELL ********************

# ===============================
# CELL-08: Data Quality Checks – Validate and Split (Enterprise / Fabric-safe)
# ===============================

from pyspark.sql import functions as F

# --------------------------------------------------
# Define Allowed Transaction Types (Business Rule)
# --------------------------------------------------
# Includes DEBIT as per your requirement
allowed_types = ["CASH_IN", "CASH_OUT", "TRANSFER", "PAYMENT", "DEBIT"]

# --------------------------------------------------
# Apply Data Quality Validation Rules
# --------------------------------------------------

df_dq = (
    df_s2

    # Create a single string column (dq_reason)
    # If multiple rules fail, they will be concatenated using " | "
    .withColumn(
        "dq_reason",
        F.concat_ws(
            " | ",

            # Rule 1: Amount must not be NULL and must be > 0
            F.when(
                (F.col("amount").isNull()) | (F.col("amount") <= 0),
                F.lit("INVALID_AMOUNT")
            ),

            # Rule 2: Transaction type must be in allowed list
            F.when(
                ~F.col("transaction_type").isin(*allowed_types),
                F.lit("INVALID_TXN_TYPE")
            ),

            # Rule 3: Customer IDs must not be NULL
            F.when(
                F.col("origin_customer_id").isNull() |
                F.col("destination_customer_id").isNull(),
                F.lit("MISSING_CUSTOMER_ID")
            ),

            # Rule 4: Balances must not be negative
            F.when(
                (F.col("origin_balance_before") < 0) |
                (F.col("origin_balance_after") < 0) |
                (F.col("destination_balance_before") < 0) |
                (F.col("destination_balance_after") < 0),
                F.lit("NEGATIVE_BALANCE")
            )
        )
    )

    # Assign final Data Quality status
    # If dq_reason is empty → VALID
    # If dq_reason contains any value → REJECT
    .withColumn(
        "dq_status",
        F.when(
            F.length(F.col("dq_reason")) == 0,
            F.lit("VALID")
        ).otherwise(F.lit("REJECT"))
    )
)

# --------------------------------------------------
# Split Data into VALID and REJECT
# --------------------------------------------------

# VALID records → drop dq_reason (clean data for Silver table)
df_valid = (
    df_dq
    .filter(F.col("dq_status") == "VALID")
    .drop("dq_reason")
)

# REJECT records → keep dq_reason for audit/debug
df_reject = df_dq.filter(F.col("dq_status") == "REJECT")

# --------------------------------------------------
# Log Summary Counts
# --------------------------------------------------

print("✅ Valid rows :", df_valid.count())
print("⚠️ Reject rows:", df_reject.count())

# Optional preview of rejected records
df_reject.select(
    "txn_id",
    "transaction_type",
    "amount",
    "dq_reason"
).show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Write VALID data to Silver**

# CELL ********************

# =========================================
# CELL-08A: Write VALID transactions to Silver
# =========================================

# Define target Silver table (Delta)
silver_table = "silver.paysim_transactions_clear"

# Determine write mode based on load type
# FULL  -> overwrite existing table
# INCR  -> append new records
silver_mode = "overwrite" if p_load_type == "FULL" else "append"

print(f"➡️ Writing VALID rows to {silver_table} | mode={silver_mode}")

(df_valid.write
    .format("delta")
    .mode(silver_mode)
    .option("overwriteSchema", "true")   # applied only when overwrite happens
    .saveAsTable(silver_table)
)

valid_written = int(df_valid.count())
print(f"✅ Silver write successful. Rows written = {valid_written}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Write REJECT data to Quarantine (DQ)**

# CELL ********************

# ==========================================
# CELL: Write REJECT transactions to Quarantine (DQ)
# ==========================================

from pyspark.sql import functions as F

dq_table = "dq.paysim_rejects"
dq_mode = "overwrite" if str(p_load_type).upper() == "FULL" else "append"

print(f"➡️ Writing REJECT rows to {dq_table} | mode={dq_mode}")

df_reject_out = (
    df_reject
    .withColumn("_reject_ts", F.current_timestamp())
    .withColumn("_reject_batch_id", F.col("_batch_id"))   # take from data
    .withColumn("_reject_run_id", F.lit(str(p_pipeline_run_id)))
    .withColumn("_reject_env", F.lit(str(p_env)))
)

reject_written = int(df_reject_out.count())

(df_reject_out.write
    .format("delta")
    .mode(dq_mode)
    .option("overwriteSchema", "true")
    .saveAsTable(dq_table)
)

print(f"✅ Quarantine write successful. Rows written = {reject_written}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Update watermark (ONLY after Valid and Reject succeeded)**

# CELL ********************

# ==========================================
# CELL: Update watermark ONLY after successful Silver + DQ writes
# ==========================================

from pyspark.sql import functions as F

pipeline_name = "pl_finance_e2e_batch"
entity_name   = "paysim_transactions"

# Get latest ingest_ts and its matching batch_id from VALID data
latest_row = (
    df_valid
    .select("_ingest_ts", "_batch_id")
    .orderBy(F.col("_ingest_ts").desc())
    .limit(1)
    .collect()
)

# Enterprise hard-fail if no VALID rows
if len(latest_row) == 0:
    raise Exception("❌ Cannot update watermark: df_valid has 0 rows")

new_watermark_ts = latest_row[0]["_ingest_ts"]
new_batch_id     = latest_row[0]["_batch_id"]

if new_watermark_ts is None:
    raise Exception("❌ Cannot update watermark: new_watermark_ts is NULL")

if new_batch_id is None:
    raise Exception("❌ Cannot update watermark: new_batch_id is NULL")

print("➡️ Updating watermark to:", new_watermark_ts, "batch:", new_batch_id)

spark.sql(f"""
UPDATE meta.etl_watermark
SET last_success_ts = timestamp('{new_watermark_ts}'),
    last_success_batch_id = '{new_batch_id}',
    updated_at = current_timestamp()
WHERE pipeline_name = '{pipeline_name}'
  AND entity_name   = '{entity_name}'
""")

print("✅ Watermark updated successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Validate watermark update**

# CELL ********************

spark.sql("""
SELECT *
FROM meta.etl_watermark
WHERE pipeline_name = 'pl_finance_e2e_batch'
AND entity_name = 'paysim_transactions'
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### **Write notebook activity audit record**

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# STEP: Write notebook activity audit record
# ============================================================

from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

activity_end_time = datetime.now()

duration_seconds = float((activity_end_time - activity_start_time).total_seconds())

audit_schema = StructType([
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

audit_data = [
    (
        str(p_pipeline_run_id),
        str(pipeline_name),
        str(activity_name),
        str(layer),
        activity_start_time,
        activity_end_time,
        "SUCCESS",
        duration_seconds,
        None,
        datetime.now()
    )
]

audit_df = spark.createDataFrame(audit_data, schema=audit_schema)

audit_df.write.mode("append").saveAsTable("meta.pipeline_activity_audit")

print("SUCCESS: Silver notebook activity audit record inserted")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Validation**

# CELL ********************

spark.table("meta.pipeline_activity_audit") \
.filter("activity_name = 'nb_02_silver_paysim_transform'") \
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
