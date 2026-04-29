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

# #### **Import Library**

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Create a Database**

# CELL ********************

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")
spark.sql("CREATE DATABASE IF NOT EXISTS meta")
spark.sql("CREATE DATABASE IF NOT EXISTS log")
spark.sql("CREATE DATABASE IF NOT EXISTS dq")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Verify The Database**

# CELL ********************

spark.sql("SHOW DATABASES").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Create meta ingestion_control**

# CELL ********************


ingestion_schema = StructType([
    StructField("pipeline_name", StringType(), False),
    StructField("entity_name", StringType(), False),
    StructField("load_type", StringType(), False),           # FULL / INCR
    StructField("last_watermark_ts", TimestampType(), True),
    StructField("last_source_file", StringType(), True),
    StructField("last_batch_id", StringType(), True),
    StructField("status", StringType(), False),              # ACTIVE/INACTIVE
    StructField("updated_at", TimestampType(), False),
    StructField("updated_by", StringType(), False)
])

empty_df = spark.createDataFrame([], ingestion_schema)

empty_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("meta.ingestion_control")

print("Created: meta.ingestion_control")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Verify meta ingestion tables**

# CELL ********************

spark.sql("SHOW TABLES IN meta").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Create log pipeline_run_log**

# CELL ********************

log_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("batch_id", StringType(), False),
    StructField("env", StringType(), False),                 # DEV/UAT/PROD
    StructField("start_ts", TimestampType(), False),
    StructField("end_ts", TimestampType(), True),
    StructField("status", StringType(), False),              # STARTED/SUCCESS/FAILED
    StructField("rows_read", LongType(), True),
    StructField("rows_written", LongType(), True),
    StructField("error_message", StringType(), True),
    StructField("created_at", TimestampType(), False)
])

empty_log_df = spark.createDataFrame([], log_schema)

empty_log_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("log.pipeline_run_log")

print("Created: log.pipeline_run_log")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Verify log pipeline tables**

# CELL ********************

spark.sql("SHOW TABLES IN log").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Control Record (PaySim)**

# CELL ********************

from pyspark.sql import functions as F

target_schema = spark.table("meta.ingestion_control").schema
print(target_schema)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

seed = [(
    "pl_ingest_paysim",        # pipeline_name
    "paysim_transactions",     # entity_name
    "FULL",                    # load_type
    None,                      # last_watermark_ts
    None,                      # last_source_file
    None,                      # last_batch_id
    "ACTIVE",                  # status
    None,                      # updated_at
    None                       # updated_by
)]

df_seed = spark.createDataFrame(seed, schema=target_schema) \
    .withColumn("updated_at", F.current_timestamp()) \
    .withColumn("updated_by", F.lit("bootstrap"))

df_seed.write.format("delta").mode("append").saveAsTable("meta.ingestion_control")

print("Seed record inserted into meta.ingestion_control")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Verify log pipeline tables**

# CELL ********************

spark.table("meta.ingestion_control").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Create Watermark / Control Table (Delta)**

# CELL ********************

from pyspark.sql import functions as F

# 1) Create table (empty) if not exists
spark.sql("""
CREATE TABLE IF NOT EXISTS meta.etl_watermark (
  pipeline_name STRING,
  entity_name STRING,
  last_success_ts TIMESTAMP,
  last_success_batch_id STRING,
  updated_at TIMESTAMP
)
USING DELTA
""")

# 2) Insert baseline row only if it doesn't exist
spark.sql("""
INSERT INTO meta.etl_watermark
SELECT
  'pl_finance_e2e_batch' AS pipeline_name,
  'paysim_transactions' AS entity_name,
  TIMESTAMP('1900-01-01 00:00:00') AS last_success_ts,
  'INIT' AS last_success_batch_id,
  current_timestamp() AS updated_at
WHERE NOT EXISTS (
  SELECT 1 FROM meta.etl_watermark
  WHERE pipeline_name='pl_finance_e2e_batch'
    AND entity_name='paysim_transactions'
)
""")

# 3) Verify
display(spark.sql("SELECT * FROM meta.etl_watermark"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Meta Layer – Create `pipeline_activity_audit`**
# 
# #### **Objective**
# Create an audit table to store one record per notebook activity execution.
# 
# #### **Why this table is needed**
# The pipeline run audit table tracks the overall pipeline execution.
# 
# This activity audit table tracks each notebook separately, which helps with:
# 
# - notebook-level monitoring
# - runtime analysis
# - troubleshooting failed activities
# - identifying slow pipeline steps
# 
# **Target table**
# `meta.pipeline_activity_audit`

# CELL ********************

# ============================================================
# STEP: Create meta.pipeline_activity_audit table
# ============================================================

# Create the metadata schema if it does not already exist.
# This schema is typically used to store operational metadata
# such as pipeline audits, activity logs, and monitoring tables.
spark.sql("""
CREATE SCHEMA IF NOT EXISTS meta
""")

# ------------------------------------------------------------
# Create the activity-level audit table
# ------------------------------------------------------------
# This table captures execution details for each pipeline activity
# (for example: Bronze load, Silver transformation, Gold aggregation).
#
# Unlike pipeline_run_audit (which tracks the whole pipeline),
# this table tracks individual steps within the pipeline.

spark.sql("""
CREATE TABLE IF NOT EXISTS meta.pipeline_activity_audit (
    
    -- Unique identifier of the pipeline run
    run_id STRING,
    
    -- Name of the pipeline executing the activity
    pipeline_name STRING,
    
    -- Name of the specific activity / notebook step
    activity_name STRING,
    
    -- Data layer where this activity operates
    -- Example: bronze / silver / gold
    layer STRING,
    
    -- Timestamp when the activity started
    start_time TIMESTAMP,
    
    -- Timestamp when the activity finished
    end_time TIMESTAMP,
    
    -- Execution result of the activity
    -- Possible values: SUCCESS / FAILED
    status STRING,
    
    -- Total execution time of the activity in seconds
    duration_seconds DOUBLE,
    
    -- Error message captured if the activity failed
    error_message STRING,
    
    -- Timestamp when the audit record was created
    created_at TIMESTAMP
)

-- Store the table using Delta Lake format
-- This allows ACID transactions, time travel, and efficient updates
USING DELTA
""")

# Log message confirming table creation
print("✅ Created table: meta.pipeline_activity_audit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Validate pipeline_activity_audit table creation**

# CELL ********************

# ============================================================
# STEP: Validate pipeline_activity_audit table creation
# ============================================================

spark.table("meta.pipeline_activity_audit").printSchema()
spark.table("meta.pipeline_activity_audit").show(5, truncate=False)

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
