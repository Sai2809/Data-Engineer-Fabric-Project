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

# CELL ********************

# Import required libraries
from delta.tables import DeltaTable

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

# #### **Retrieve Pipeline Parameter**

# CELL ********************

# ============================================================
# STEP 0: Retrieve Pipeline Parameters
# Purpose: Read the pipeline run ID passed from the orchestration pipeline.
# If the notebook is executed manually (outside the pipeline),
# assign a default value to identify the run.
# ============================================================

try:
    # Fetch pipeline run ID from notebook parameters
    p_pipeline_run_id = mssparkutils.notebook.params.get("p_pipeline_run_id")
except:
    # Default value used when notebook is executed manually
    p_pipeline_run_id = "MANUAL_RUN"


# ============================================================
# STEP 1: Initialize Activity Monitoring
# Purpose: Capture metadata required for monitoring the notebook execution,
# including start time, pipeline name, activity name, and processing layer.
# This information can later be used for logging, auditing, and debugging.
# ============================================================

from datetime import datetime

# Record activity start time for execution tracking
activity_start_time = datetime.now()

# Define pipeline and activity metadata
pipeline_name = "pl_finance_e2e_batch"
activity_name = "nb_03_gold_model_build"
layer = "gold"

# Print execution details for logging and troubleshooting
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

# #### **Activity Audit Record**

# CELL ********************

# ============================================================
# STEP: Write Notebook Activity Audit Record
# Purpose: Capture execution metadata for the current notebook
# activity and store it in the pipeline activity audit table.
# This helps track execution status, duration, and errors for
# monitoring, troubleshooting, and operational reporting.
# ============================================================

from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

# Record the activity end time
activity_end_time = datetime.now()

# Calculate total execution duration in seconds
duration_seconds = float((activity_end_time - activity_start_time).total_seconds())

# Define schema for the audit log table
audit_schema = StructType([
    StructField("run_id", StringType(), True),          # Unique pipeline run identifier
    StructField("pipeline_name", StringType(), True),   # Name of the pipeline triggering the notebook
    StructField("activity_name", StringType(), True),   # Notebook or activity name
    StructField("layer", StringType(), True),           # Data processing layer (e.g., bronze, silver, gold)
    StructField("start_time", TimestampType(), True),   # Activity start timestamp
    StructField("end_time", TimestampType(), True),     # Activity completion timestamp
    StructField("status", StringType(), True),          # Execution status (SUCCESS / FAILED)
    StructField("duration_seconds", DoubleType(), True),# Total runtime of the activity
    StructField("error_message", StringType(), True),   # Error details if execution fails
    StructField("created_at", TimestampType(), True)    # Record creation timestamp
])

# Prepare audit record data
audit_data = [
    (
        str(p_pipeline_run_id),   # Pipeline run ID
        str(pipeline_name),       # Pipeline name
        str(activity_name),       # Activity / notebook name
        str(layer),               # Data layer
        activity_start_time,      # Start time
        activity_end_time,        # End time
        "SUCCESS",                # Execution status
        duration_seconds,         # Duration of execution
        None,                     # No error message since execution succeeded
        datetime.now()            # Audit record creation timestamp
    )
]

# Create Spark DataFrame for audit logging
audit_df = spark.createDataFrame(audit_data, schema=audit_schema)

# Append the audit record into the metadata audit table
audit_df.write.mode("append").saveAsTable("meta.pipeline_activity_audit")

# Log confirmation message
print("SUCCESS: Gold notebook activity audit record inserted")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Validate**

# CELL ********************

spark.sql("""
SELECT *
FROM meta.pipeline_activity_audit
WHERE activity_name = 'nb_03_gold_model_build'
ORDER BY start_time DESC
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Gold Layer – Build `dim_transaction_type`**
# 
# #### **Objective**
# Create a conformed transaction type dimension table from the Silver layer.
# 
# #### **Why this dimension is needed**
# In the Silver table, `transaction_type` exists as a plain string column.  
# For enterprise reporting and dimensional modeling, it is better to convert this into a separate dimension table.
# 
# **This helps us:**
# 
# - standardize transaction categories
# - add reusable business attributes
# - improve star schema design
# - simplify joins in Power BI and Gold fact tables
# 
# **Business attributes added**
# We will derive the following:
# 
# - `transaction_direction`  
#   Indicates whether the transaction is treated as a **DEBIT** or **CREDIT**
# 
# - `risk_group`  
#   Helps classify transaction types into **HIGH** or **NORMAL** risk buckets
# 
# **Target table**
# `gold.dim_transaction_type`
# 
# **Source table**
# `silver.paysim_transactions_clear`

# CELL ********************

# ------------------------------------------------------------
# STEP 1: Read distinct transaction types from Silver layer
# ------------------------------------------------------------
# We only need unique transaction_type values to build the dimension table.
df_types = (
    spark.table("silver.paysim_transactions_clear")  # Load Silver transactions table
    .select("transaction_type")                      # Keep only the transaction_type column
    .dropDuplicates()                                # Remove duplicate transaction types
)

# Debug check: show distinct transaction types before enrichment
print("✅ Distinct transaction types found:", df_types.count())
df_types.show(truncate=False)

# ------------------------------------------------------------
# STEP 2: Add business attributes
# ------------------------------------------------------------
# Enrich each transaction type with business-relevant columns:
# 1. transaction_direction: DEBIT or CREDIT
# 2. risk_group: HIGH or NORMAL
# 3. transaction_type_key: unique surrogate key for dimension
df_dim_txn_type = (
    df_types

    # Assign transaction direction based on business rules
    # CASH_OUT and TRANSFER are DEBIT, all others are CREDIT
    .withColumn(
        "transaction_direction",
        F.when(
            F.col("transaction_type").isin("CASH_OUT", "TRANSFER"),
            F.lit("DEBIT")
        ).otherwise(F.lit("CREDIT"))
    )

    # Classify risk group
    # TRANSFER and CASH_OUT are HIGH risk, all others NORMAL
    .withColumn(
        "risk_group",
        F.when(
            F.col("transaction_type").isin("TRANSFER", "CASH_OUT"),
            F.lit("HIGH")
        ).otherwise(F.lit("NORMAL"))
    )

    # Generate a deterministic surrogate key
    # xxhash64 ensures the same transaction_type always gets the same numeric key
    .withColumn(
        "transaction_type_key",
        F.xxhash64(F.col("transaction_type")).cast("long")
    )

    # Select and reorder final columns for dimension table
    .select(
        "transaction_type_key",
        "transaction_type",
        "transaction_direction",
        "risk_group"
    )
)

# Debug check: preview the enriched dimension dataframe
print("✅ Preview of dim_transaction_type:")
df_dim_txn_type.show(truncate=False)

# ------------------------------------------------------------
# STEP 3: Write dimension table to Gold layer
# ------------------------------------------------------------
# Gold dimensions are small lookup tables, so we overwrite the table entirely.
# Using Delta format ensures ACID compliance, versioning, and schema enforcement.
(
    df_dim_txn_type.write
    .format("delta")                 # Delta format for reliability and performance
    .mode("overwrite")               # Overwrite existing table
    .option("overwriteSchema", "true") # Apply any schema changes
    .saveAsTable("gold.dim_transaction_type") # Save as managed table in Gold layer
)

print("✅ Created table: gold.dim_transaction_type")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Validate dim_transaction_type**

# CELL ********************

# Load the newly created dimension table from Gold layer
df_check = spark.table("gold.dim_transaction_type")

# Check the total number of rows to ensure data was written correctly
print("✅ Row count in gold.dim_transaction_type:", df_check.count())

# Preview the contents of the table to manually verify correctness
# truncate=False ensures all column values are fully displayed
df_check.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Gold Layer – Build `dim_date`**
# 
# #### **Objective**
# Create a reusable date dimension table for time-based analysis.
# 
# #### **Why this dimension is needed**
# The Silver table contains `event_ts`, which is a timestamp column.  
# To support analytics and reporting, we build a proper date dimension so that fact tables can join to a conformed calendar.
# 
# This helps us:
# 
# - perform time-based reporting in Power BI
# - group transactions by year, quarter, month, week, and day
# - maintain a consistent date model across all fact tables
# - improve star schema design
# 
# **Target table**
# `gold.dim_date`
# 
# **Source table**
# `silver.paysim_transactions_clear`
# 
# **Design approach**
# 1. calculate the minimum and maximum transaction dates from Silver
# 2. generate a full calendar range between those dates
# 3. enrich the range with standard date attributes
# 4. write the final dimension to Gold

# CELL ********************

# ------------------------------------------------------------
# STEP 1: Get minimum and maximum transaction dates
# ------------------------------------------------------------
# Use the 'event_ts' timestamp from the Silver layer as the base
# This allows us to determine the full calendar range for the date dimension.
date_range = (
    spark.table("silver.paysim_transactions_clear")  # Load Silver transactions table
    .select(
        F.min(F.to_date("event_ts")).alias("min_date"),  # Earliest date in the data
        F.max(F.to_date("event_ts")).alias("max_date")   # Latest date in the data
    )
    .collect()[0]  # Collect the result to a local Row object
)

# Extract min and max dates from the Row
min_date = date_range["min_date"]
max_date = date_range["max_date"]

print("✅ Min Date:", min_date)
print("✅ Max Date:", max_date)

# ------------------------------------------------------------
# STEP 2: Generate a full date sequence
# ------------------------------------------------------------
# Create a DataFrame containing all dates from min_date to max_date
# sequence(start, end, interval) generates an array of dates
# explode(array) converts the array into individual rows
df_dates = spark.sql(f"""
SELECT explode(
    sequence(
        to_date('{min_date}'),  -- Start of date range
        to_date('{max_date}'),  -- End of date range
        interval 1 day           -- Step size: 1 day
    )
) AS date
""")

print("✅ Date sequence generated")
df_dates.show(5, truncate=False)

# ------------------------------------------------------------
# STEP 3: Add standard calendar attributes
# ------------------------------------------------------------
# Enrich each date with attributes commonly used in analytics:
# - date_key: integer key in YYYYMMDD format
# - year, month, quarter, day, week_of_year, day_of_week
# - month_name: full month name for readability
df_dim_date = (
    df_dates
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("day_of_week", F.date_format("date", "EEEE"))
    # Reorder and rename columns for final dimension output
    .select(
        "date_key",
        F.col("date").alias("full_date"),
        "year",
        "quarter",
        "month",
        "month_name",
        "week_of_year",
        "day",
        "day_of_week"
    )
)

print("✅ Preview of dim_date:")
df_dim_date.show(5, truncate=False)

# ------------------------------------------------------------
# STEP 4: Write dim_date to Gold layer
# ------------------------------------------------------------
# Save the date dimension as a Delta table for downstream analytics
(
    df_dim_date.write
    .format("delta")                 # Delta format for ACID compliance & performance
    .mode("overwrite")               # Overwrite any existing table
    .option("overwriteSchema", "true") # Apply any schema changes
    .saveAsTable("gold.dim_date")    # Save as managed table in Gold layer
)

print("✅ Created table: gold.dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Validate dim_date**

# CELL ********************

# Load the dim_date table from the Gold layer
df_check = spark.table("gold.dim_date")

# Check the total number of rows to ensure the full date range was created
print("✅ Row count in gold.dim_date:", df_check.count())

# Verify that the minimum and maximum dates match the expected range
df_check.select(
    F.min("full_date").alias("min_date"),  # Earliest date in the table
    F.max("full_date").alias("max_date")   # Latest date in the table
).show(truncate=False)

# Preview the first 5 rows to visually inspect the date attributes
df_check.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Gold Layer – Build `dim_account`**
# 
# #### **Objective**
# Create a conformed account dimension from both origin and destination account identifiers.
# 
# #### **Why this dimension is needed**
# The Silver transaction table stores account identifiers as plain strings in two separate columns:
# 
# - `origin_customer_id`
# - `destination_customer_id`
# 
# For a proper star schema, fact tables should not directly join to raw string identifiers.  
# Instead, we create a reusable account dimension with a surrogate key.
# 
# This helps us:
# 
# - standardize account identifiers into one dimension
# - simplify joins in the fact table
# - improve star schema design
# - prepare the model for future account-level enrichment
# 
# **Target table**
# `gold.dim_account`
# 
# **Source table**
# `silver.paysim_transactions_clear`
# 
# **Design approach**
# 1. collect all distinct origin accounts
# 2. collect all distinct destination accounts
# 3. combine them into one account list
# 4. generate a deterministic surrogate key
# 5. write the final dimension to Gold


# CELL ********************


# ------------------------------------------------------------
# STEP 1: Read origin accounts from Silver layer
# ------------------------------------------------------------
# We extract the IDs of customers who are sending money
df_origin_accounts = (
    spark.table("silver.paysim_transactions_clear")  # Load Silver transactions table
    .select(F.col("origin_customer_id").alias("account_id"))  # Rename column to 'account_id'
)

# ------------------------------------------------------------
# STEP 2: Read destination accounts from Silver layer
# ------------------------------------------------------------
# We extract the IDs of customers who are receiving money
df_destination_accounts = (
    spark.table("silver.paysim_transactions_clear")
    .select(F.col("destination_customer_id").alias("account_id"))  # Rename to match origin accounts
)

# ------------------------------------------------------------
# STEP 3: Combine both account lists and keep only unique values
# ------------------------------------------------------------
# Merge origin and destination accounts into a single list and remove duplicates
df_accounts = (
    df_origin_accounts
    .union(df_destination_accounts)  # Combine origin and destination accounts
    .dropDuplicates()                # Keep only unique account IDs
)

# Quick check: see how many distinct accounts we have
print("✅ Distinct accounts identified:", df_accounts.count())
df_accounts.show(5, truncate=False)

# ------------------------------------------------------------
# STEP 4: Create surrogate key for accounts
# ------------------------------------------------------------
# Generate a deterministic numeric key using xxhash64
# Ensures the same account_id always gets the same key for joins
df_dim_account = (
    df_accounts
    .withColumn("account_key", F.xxhash64(F.col("account_id")).cast("long"))
    .select("account_key", "account_id")  # Select final columns for dimension table
)

# Preview the first few rows of the account dimension
print("✅ Preview of dim_account:")
df_dim_account.show(5, truncate=False)

# ------------------------------------------------------------
# STEP 5: Write dim_account to Gold layer
# ------------------------------------------------------------
# Save as Delta table in Gold layer for analytics and joins
(
    df_dim_account.write
    .format("delta")                 # Use Delta format for ACID compliance and performance
    .mode("overwrite")               # Overwrite existing table if present
    .option("overwriteSchema", "true") # Apply schema changes if necessary
    .saveAsTable("gold.dim_account") # Save as managed table
)

print("✅ Created table: gold.dim_account")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Validate dim_account**

# CELL ********************

# Load the newly created account dimension table from the Gold layer
df_check = spark.table("gold.dim_account")

# Quick sanity check: total number of distinct accounts
print("✅ Row count in gold.dim_account:", df_check.count())

# Preview the first 5 rows to ensure the surrogate keys and account IDs look correct
df_check.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Gold Layer – Build Account Profile Snapshot**
# 
# #### **Objective**
# Create a current account profile snapshot from the Silver transaction table.
# 
# #### **Why this step is needed**
# The account dimension (`gold.dim_account`) only stores the account identifier and surrogate key.
# 
# For business analytics, we also need profile-level attributes such as:
# 
# - transaction activity level
# - total transaction amount
# - fraud involvement
# - risk classification
# 
# This snapshot will later be used as the source for building a Slowly Changing Dimension Type 2 (SCD2).
# 
# **Target dataframe**
# `df_snapshot`
# 
# **Source table**
# `silver.paysim_transactions_clear`
# 
# **Design approach**
# 
# 1. create outgoing transaction metrics for each account
# 2. create incoming transaction metrics for each account
# 3. combine both sides into one profile
# 4. derive business segments
# 5. join with `gold.dim_account` to attach surrogate keys

# CELL ********************

# ------------------------------------------------------------
# STEP 1: Load Silver transactions
# ------------------------------------------------------------
# Read the cleaned transaction dataset from the Silver layer.
# This table contains all transactions used to derive account metrics.
df_silver = spark.table("silver.paysim_transactions_clear")

# ------------------------------------------------------------
# STEP 2: Calculate OUTGOING metrics by account
# ------------------------------------------------------------
# These metrics are based on origin_customer_id
# (i.e., accounts that SEND money)

df_origin = (
    df_silver
    .groupBy(F.col("origin_customer_id").alias("account_id"))  # Treat origin customer as account_id
    .agg(
        F.count("*").alias("out_txn_count"),       # Number of outgoing transactions
        F.sum("amount").alias("out_total_amount"), # Total amount sent
        F.max("is_fraud").alias("out_fraud_flag")  # If any outgoing txn was fraud → flag becomes 1
    )
)

print("✅ Outgoing account metrics ready")
df_origin.show(5, truncate=False)

# ------------------------------------------------------------
# STEP 3: Calculate INCOMING metrics by account
# ------------------------------------------------------------
# These metrics are based on destination_customer_id
# (i.e., accounts that RECEIVE money)

df_dest = (
    df_silver
    .groupBy(F.col("destination_customer_id").alias("account_id"))  # Treat destination customer as account_id
    .agg(
        F.count("*").alias("in_txn_count"),        # Number of incoming transactions
        F.sum("amount").alias("in_total_amount"),  # Total amount received
        F.max("is_fraud").alias("in_fraud_flag")   # If any incoming txn was fraud → flag becomes 1
    )
)

print("✅ Incoming account metrics ready")
df_dest.show(5, truncate=False)

# ------------------------------------------------------------
# STEP 4: Combine outgoing + incoming metrics
# ------------------------------------------------------------
# Some accounts may appear only as senders or only as receivers.
# unionByName merges both datasets while keeping column alignment.

df_profile = (
    df_origin
    .unionByName(df_dest, allowMissingColumns=True)

    # Re-aggregate after union so each account has one record
    .groupBy("account_id")
    .agg(
        F.sum(F.coalesce(F.col("out_txn_count"), F.lit(0))).alias("out_txn_count"),
        F.sum(F.coalesce(F.col("in_txn_count"), F.lit(0))).alias("in_txn_count"),
        F.sum(F.coalesce(F.col("out_total_amount"), F.lit(0.0))).alias("out_total_amount"),
        F.sum(F.coalesce(F.col("in_total_amount"), F.lit(0.0))).alias("in_total_amount"),
        F.max(F.coalesce(F.col("out_fraud_flag"), F.lit(0))).alias("out_fraud_flag"),
        F.max(F.coalesce(F.col("in_fraud_flag"), F.lit(0))).alias("in_fraud_flag")
    )

    # Calculate total transactions across incoming + outgoing
    .withColumn("total_txn_count", F.col("out_txn_count") + F.col("in_txn_count"))

    # Calculate total money movement for the account
    .withColumn("total_amount", F.col("out_total_amount") + F.col("in_total_amount"))

    # Flag if the account was involved in any fraud transaction
    .withColumn(
        "fraud_involved_flag",
        F.when(
            (F.col("out_fraud_flag") == 1) | (F.col("in_fraud_flag") == 1),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
)

print("✅ Combined account profile created")
df_profile.show(5, truncate=False)

# ------------------------------------------------------------
# STEP 5: Add business segmentation attributes
# ------------------------------------------------------------
# These fields classify accounts based on activity and risk.

df_snapshot = (
    df_profile

    # Activity level segmentation based on number of transactions
    .withColumn(
        "activity_segment",
        F.when(F.col("total_txn_count") >= 50, F.lit("HIGH"))     # Very active accounts
         .when(F.col("total_txn_count") >= 10, F.lit("MEDIUM"))   # Moderately active accounts
         .otherwise(F.lit("LOW"))                                 # Low activity accounts
    )

    # Risk classification based on fraud involvement
    .withColumn(
        "risk_tier",
        F.when(F.col("fraud_involved_flag") == 1, F.lit("HIGH"))
         .otherwise(F.lit("LOW"))
    )
)

print("✅ Business segments added")
df_snapshot.show(5, truncate=False)

# ------------------------------------------------------------
# STEP 6: Join with dim_account to attach surrogate key
# ------------------------------------------------------------
# The Gold fact tables should use surrogate keys instead of raw IDs.
# We join with the account dimension to retrieve account_key.

df_snapshot = (
    df_snapshot
    .join(
        spark.table("gold.dim_account"),
        on="account_id",
        how="inner"
    )
    .select(
        "account_key",          # Surrogate key from dimension
        "account_id",           # Natural key
        "total_txn_count",      # Total number of transactions
        "total_amount",         # Total money movement
        "activity_segment",     # Business activity classification
        "risk_tier",            # Risk classification
        "fraud_involved_flag"   # Fraud indicator
    )
)

print("✅ Final account profile snapshot ready")
df_snapshot.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Validate account profile snapshot**

# CELL ********************


print("✅ Snapshot row count:", df_snapshot.count())
df_snapshot.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Gold Layer – Create Initial SCD2 Table**
# 
# #### **Objective**
# Create the initial Slowly Changing Dimension Type 2 table for account profiles.
# 
# #### **Why this table is needed**
# The account profile snapshot contains the **current** state of each account.
# 
# However, in enterprise data platforms, account attributes can change over time, such as:
# 
# - activity segment
# - risk tier
# - fraud involvement
# - total transaction behavior
# 
# To preserve history, we create an SCD Type 2 table.
# 
# #### **SCD Type 2 fields**
# This table includes:
# 
# - `effective_start_date`  
#   The timestamp when the record became active
# 
# - `effective_end_date`  
#   The timestamp when the record was closed (NULL for current records)
# 
# - `is_current`  
#   Flag indicating whether the record is the latest active version
# 
# **Target table**
# `gold.dim_account_profile_scd2`
# 
# **Source dataframe**
# `df_snapshot`

# CELL ********************

# ------------------------------------------------------------
# STEP 1: Add SCD2 tracking columns
# ------------------------------------------------------------
# We prepare the first version of the Slowly Changing Dimension Type 2 table.
# SCD2 keeps historical versions of records when attributes change.

df_scd2_init = (
    df_snapshot

    # Timestamp when this version of the record becomes active
    .withColumn("effective_start_date", F.current_timestamp())

    # End timestamp of the record.
    # For the current active record, this remains NULL.
    .withColumn("effective_end_date", F.lit(None).cast("timestamp"))

    # Flag indicating whether this record is the current active version
    .withColumn("is_current", F.lit(True))
)

print("✅ SCD2 initial dataframe prepared")
df_scd2_init.show(5, truncate=False)

# ------------------------------------------------------------
# STEP 2: Write initial SCD2 table to Gold
# ------------------------------------------------------------
# We store the initial snapshot as a Delta table.
# Future pipeline runs will update this table using SCD2 logic
# (closing old records and inserting new versions when changes occur).

(
    df_scd2_init.write
    .format("delta")                 # Delta Lake format for ACID transactions
    .mode("overwrite")               # Initial load overwrites any existing table
    .option("overwriteSchema", "true") # Ensure schema updates are applied
    .saveAsTable("gold.dim_account_profile_scd2")  # Create Gold dimension table
)

print("✅ Created table: gold.dim_account_profile_scd2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Gold Layer – SCD Type 2 MERGE Logic**
# 
# #### **Objective**
# Maintain historical account profile records using Slowly Changing Dimension Type 2 logic.
# 
# #### **Why this step is needed**
# The initial SCD2 table created in the previous step only represents the baseline profile.
# 
# In future runs, account attributes may change, for example:
# 
# - total transaction count
# - total transaction amount
# - activity segment
# - risk tier
# - fraud involvement flag
# 
# When such changes occur, we should not overwrite the old row.
# 
# Instead, SCD Type 2 requires that we:
# 
# 1. close the old record  
# 2. insert a new active version  
# 3. retain full history for audit and analysis  
# 
# **Change detection logic**
# A record is considered changed when any of the tracked business attributes differ between:
# 
# - current snapshot (`df_snapshot`)
# - active record in `gold.dim_account_profile_scd2`
# 
# **Target table**
# `gold.dim_account_profile_scd2`

# CELL ********************

# ------------------------------------------------------------
# STEP 1: Define target table
# ------------------------------------------------------------
# This is the SCD2 dimension table that stores historical versions
# of account profiles.
target_table = "gold.dim_account_profile_scd2"

# ------------------------------------------------------------
# STEP 2: Prepare source snapshot with merge timestamp
# ------------------------------------------------------------
# df_snapshot contains the latest account profile metrics.
# We add a timestamp column that represents when this merge run happens.
# This timestamp will be used to close old records and start new ones.
df_src = (
    df_snapshot
    .withColumn("merge_run_ts", F.current_timestamp())
)

print("✅ Source snapshot prepared for MERGE")
df_src.show(5, truncate=False)

# ------------------------------------------------------------
# STEP 3: Load target Delta table
# ------------------------------------------------------------
# DeltaTable API allows us to perform MERGE operations
# (similar to SQL MERGE / UPSERT).
dt = DeltaTable.forName(spark, target_table)

# ------------------------------------------------------------
# STEP 4: Define change detection condition
# ------------------------------------------------------------
# We compare current values in the SCD2 table (t) with the
# new snapshot values (s).
#
# If any business attribute has changed, we consider the record
# to be updated and must close the previous version.
change_condition = """
t.is_current = true AND (
    t.total_txn_count <> s.total_txn_count OR
    round(t.total_amount, 2) <> round(s.total_amount, 2) OR
    t.activity_segment <> s.activity_segment OR
    t.risk_tier <> s.risk_tier OR
    t.fraud_involved_flag <> s.fraud_involved_flag
)
"""

# ------------------------------------------------------------
# STEP 5: Perform SCD2 MERGE operation
# ------------------------------------------------------------
# Logic:
# 1. If a matching account exists AND attributes changed:
#       → expire the current row
# 2. If account does not exist:
#       → insert a new record
# 3. If account exists but nothing changed:
#       → do nothing

(
    dt.alias("t")   # target table alias
    .merge(
        df_src.alias("s"),   # source snapshot alias
        "t.account_key = s.account_key AND t.is_current = true"
    )

    # --------------------------------------------------------
    # CASE 1: Account exists and attributes changed
    # --------------------------------------------------------
    # Expire the current record by setting end date
    # and marking it as not current.
    .whenMatchedUpdate(
        condition=change_condition,
        set={
            "effective_end_date": "s.merge_run_ts",
            "is_current": "false"
        }
    )

    # --------------------------------------------------------
    # CASE 2: Account does not exist in the SCD2 table
    # --------------------------------------------------------
    # Insert a brand new record.
    .whenNotMatchedInsert(
        values={
            "account_key": "s.account_key",
            "account_id": "s.account_id",
            "total_txn_count": "s.total_txn_count",
            "total_amount": "s.total_amount",
            "activity_segment": "s.activity_segment",
            "risk_tier": "s.risk_tier",
            "fraud_involved_flag": "s.fraud_involved_flag",
            "effective_start_date": "s.merge_run_ts",
            "effective_end_date": "cast(null as timestamp)",
            "is_current": "true"
        }
    )

    # Execute the MERGE operation
    .execute()
)

print("✅ SCD2 MERGE completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Insert New Current Versions**
# 
# The previous MERGE step expires changed records, but it does not automatically insert the new active version for those expired accounts.
# 
# So in this step, we:
# 
# 1. identify accounts whose current records were expired  
# 2. fetch the latest snapshot for those accounts  
# 3. append new active records into the SCD2 table

# CELL ********************

# ------------------------------------------------------------
# STEP 1: Identify accounts whose current records were expired
# ------------------------------------------------------------
# After the SCD2 MERGE (CELL-17), any record that experienced
# a change will have:
#   is_current = false
#   effective_end_date != NULL
#
# These represent accounts whose previous version was closed.
# We extract those account_keys so we can create their
# new "current" versions.
df_changed = (
    spark.table(target_table)
    .filter("is_current = false AND effective_end_date IS NOT NULL")
    .select("account_key")
    .distinct()
)

print("✅ Changed accounts identified:", df_changed.count())
df_changed.show(5, truncate=False)


# ------------------------------------------------------------
# STEP 2: Build new current versions from latest snapshot
# ------------------------------------------------------------
# We join the changed accounts with the latest snapshot
# (df_snapshot) to create new SCD2 records that represent
# the updated version of each account.
#
# SCD2 columns are assigned as follows:
#   effective_start_date → current timestamp
#   effective_end_date   → NULL (since this is the active version)
#   is_current           → TRUE
df_new_versions = (
    df_snapshot
    .join(df_changed, on="account_key", how="inner")
    .withColumn("effective_start_date", F.current_timestamp())
    .withColumn("effective_end_date", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
)

print("✅ New current versions prepared:", df_new_versions.count())
df_new_versions.show(5, truncate=False)


# ------------------------------------------------------------
# STEP 3: Insert new versions into the SCD2 table
# ------------------------------------------------------------
# These rows represent the new active version for accounts
# that experienced attribute changes. We append them to the
# Delta SCD2 table so history is preserved.
(
    df_new_versions.write
    .format("delta")
    .mode("append")
    .saveAsTable(target_table)
)

print("✅ Inserted new current versions into SCD2 table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Validate SCD2 table**

# CELL ********************

# ------------------------------------------------------------
# STEP 1: Load the SCD2 dimension table
# ------------------------------------------------------------
# This table stores historical versions of account profiles
# following Slowly Changing Dimension Type 2 (SCD2) logic.
# Each account can have multiple rows over time, but only
# one row should have is_current = true.
df_check = spark.table("gold.dim_account_profile_scd2")


# ------------------------------------------------------------
# STEP 2: Check total number of records
# ------------------------------------------------------------
# This shows the total number of rows in the SCD2 table,
# including both active and historical versions.
print("✅ Total rows in SCD2 table:", df_check.count())


# ------------------------------------------------------------
# STEP 3: Validate current vs historical records
# ------------------------------------------------------------
# is_current = true  → active/latest record
# is_current = false → expired historical record
#
# This helps verify that SCD2 logic is working correctly
# after the MERGE and insert operations.
(
    df_check
    .groupBy("is_current")
    .count()
    .show()
)


# ------------------------------------------------------------
# STEP 4: Display sample records
# ------------------------------------------------------------
# Shows a few rows to visually verify:
# - effective_start_date
# - effective_end_date
# - is_current flag
# - historical versions of accounts
df_check.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Gold Layer – Build `fact_transactions`**
# 
# #### **Objective**
# Create the central fact table for transaction analytics.
# 
# #### **Why this fact table is needed**
# The Silver layer contains transaction-level data, but it is not yet modeled into a proper star schema.
# 
# A fact table is needed to:
# 
# - centralize business measures such as amount and fraud flag
# - join transaction data to conformed dimensions
# - support reporting, dashboards, and aggregations
# - improve semantic model design in Power BI
# 
# #### **Dimensions joined**
# This fact table joins to:
# 
# - `gold.dim_date`
# - `gold.dim_transaction_type`
# - `gold.dim_account` (origin account)
# - `gold.dim_account` (destination account)
# 
# #### **Measures included**
# The fact table stores:
# 
# - transaction amount
# - fraud flag
# - transaction timestamp
# - batch id for lineage
# 
# **Target table**
# `gold.fact_transactions`

# CELL ********************

# ------------------------------------------------------------
# STEP 1: Load source transaction data from Silver layer
# ------------------------------------------------------------
# This dataset contains cleaned transactional records
# produced in the Silver layer. It is the primary source
# for building the fact table in the Gold layer.
df_silver = spark.table("silver.paysim_transactions_clear")

print("✅ Silver source loaded")
print("Row count:", df_silver.count())


# ------------------------------------------------------------
# STEP 2: Join with dimension tables
# ------------------------------------------------------------
# The fact table references dimension tables using surrogate
# keys instead of raw business columns.
#
# Dimension joins performed:
#   dim_date                → date_key
#   dim_transaction_type    → transaction_type_key
#   dim_account (origin)    → origin_account_key
#   dim_account (destination) → destination_account_key
#
# This converts raw transactional data into a
# star schema fact table structure.
df_fact = (
    df_silver.alias("s")

    # Join with Date Dimension
    .join(
        spark.table("gold.dim_date").alias("d"),
        F.to_date(F.col("s.event_ts")) == F.col("d.full_date"),
        "left"
    )

    # Join with Transaction Type Dimension
    .join(
        spark.table("gold.dim_transaction_type").alias("t"),
        F.col("s.transaction_type") == F.col("t.transaction_type"),
        "left"
    )

    # Join with Account Dimension (Origin Account)
    .join(
        spark.table("gold.dim_account").alias("a1"),
        F.col("s.origin_customer_id") == F.col("a1.account_id"),
        "left"
    )

    # Join with Account Dimension (Destination Account)
    .join(
        spark.table("gold.dim_account").alias("a2"),
        F.col("s.destination_customer_id") == F.col("a2.account_id"),
        "left"
    )

    # --------------------------------------------------------
    # STEP 3: Select final fact table columns
    # --------------------------------------------------------
    # These columns define the star schema structure:
    #   - Surrogate keys to dimensions
    #   - Transaction metrics
    #   - Fraud indicators
    #   - Metadata for lineage and audit
    .select(
        F.col("s.txn_id").alias("txn_id"),
        F.col("d.date_key").alias("date_key"),
        F.col("t.transaction_type_key").alias("transaction_type_key"),
        F.col("a1.account_key").alias("origin_account_key"),
        F.col("a2.account_key").alias("destination_account_key"),
        F.col("s.event_ts").alias("event_ts"),
        F.col("s.amount").alias("amount"),
        F.col("s.is_fraud").alias("is_fraud"),
        F.col("s.is_flagged_fraud").alias("is_flagged_fraud"),
        F.col("s.transaction_direction").alias("transaction_direction"),
        F.col("s._batch_id").alias("_batch_id"),
        F.col("s._ingest_ts").alias("_ingest_ts"),
        F.col("s._source_file").alias("_source_file"),
        F.col("s._env").alias("_env")
    )
)

print("✅ Fact dataframe built")
df_fact.show(5, truncate=False)


# ------------------------------------------------------------
# STEP 4: Write fact table to Gold layer
# ------------------------------------------------------------
# The final fact table is stored in Delta format in the
# Gold layer. This table will be used for analytics,
# dashboards, and downstream reporting systems.
(
    df_fact.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.fact_transactions")
)

print("✅ Created table: gold.fact_transactions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Validate fact_transactions**

# CELL ********************

# ------------------------------------------------------------
# STEP 1: Load the fact_transactions table from Gold layer
# ------------------------------------------------------------
# This is the fact table created in the previous cell.
# It contains transactional data enriched with surrogate keys
# from the dimensions (dim_date, dim_account, dim_transaction_type).
df_check = spark.table("gold.fact_transactions")


# ------------------------------------------------------------
# STEP 2: Check total number of rows
# ------------------------------------------------------------
# This gives a quick count to verify that all source transactions
# were successfully loaded into the fact table.
print("✅ Row count in gold.fact_transactions:", df_check.count())


# ------------------------------------------------------------
# STEP 3: Preview key fact table columns
# ------------------------------------------------------------
# Show a subset of important columns to ensure:
# - Dimension keys are correctly joined
# - Amounts and fraud flags are intact
# - Basic integrity of the fact table
df_check.select(
    "txn_id",                 # unique transaction identifier
    "date_key",               # foreign key to dim_date
    "transaction_type_key",   # foreign key to dim_transaction_type
    "origin_account_key",     # foreign key to origin account
    "destination_account_key",# foreign key to destination account
    "amount",                 # transaction amount
    "is_fraud"                # fraud indicator
).show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Gold Layer – Build `fraud_summary_daily`**
# 
# #### **Objective**
# Create a daily fraud summary table for risk and fraud analytics.
# 
# #### **Why this aggregate is needed**
# The transaction fact table stores row-level transaction data, but reporting teams often need daily fraud metrics instead of raw transaction records.
# 
# This aggregate helps us:
# 
# - monitor fraud trends over time
# - calculate daily fraud rates
# - summarize fraudulent transaction amounts
# - support fraud dashboards in Power BI
# 
# #### **Measures included**
# The table contains:
# 
# - total transactions
# - fraud transactions
# - fraud amount
# - fraud rate
# 
# **Target table**
# `gold.fraud_summary_daily`
# 
# **Source table**
# `gold.fact_transactions`

# CELL ********************

# ------------------------------------------------------------
# STEP 1: Load the fact_transactions table from Gold layer
# ------------------------------------------------------------
# This fact table contains transactional data enriched with
# dimension surrogate keys. We'll use it to calculate
# daily fraud metrics.
df_fact = spark.table("gold.fact_transactions")

print("✅ Fact table loaded for fraud summary")
print("Row count:", df_fact.count())


# ------------------------------------------------------------
# STEP 2: Aggregate daily fraud metrics
# ------------------------------------------------------------
# We calculate the following for each date:
# - total_transactions      → total number of transactions
# - fraud_transactions      → count of transactions marked as fraud
# - fraud_amount            → total amount involved in fraudulent transactions
# - fraud_rate              → fraction of transactions that are fraudulent
df_fraud_daily = (
    df_fact
    .groupBy("date_key")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum("is_fraud").alias("fraud_transactions"),
        F.sum(
            F.when(F.col("is_fraud") == 1, F.col("amount")).otherwise(0)
        ).alias("fraud_amount")
    )
    .withColumn(
        "fraud_rate",
        F.col("fraud_transactions") / F.col("total_transactions")
    )
)

print("✅ Fraud summary dataframe created")
df_fraud_daily.show(10, truncate=False)


# ------------------------------------------------------------
# STEP 3: Write fraud_summary_daily to Gold layer
# ------------------------------------------------------------
# The resulting table is a daily summary of fraud metrics.
# Stored in Delta format for analytics, reporting, and dashboards.
(
    df_fraud_daily.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.fraud_summary_daily")
)

print("✅ Created table: gold.fraud_summary_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Validate fraud_summary_daily**

# CELL ********************

# ------------------------------------------------------------
# STEP 1: Load fraud_summary_daily from Gold layer
# ------------------------------------------------------------
# This table contains daily fraud metrics:
# - total_transactions: total transactions per day
# - fraud_transactions: count of fraudulent transactions
# - fraud_amount: total amount involved in fraud
# - fraud_rate: fraction of transactions that were fraudulent
df_check = spark.table("gold.fraud_summary_daily")

# ------------------------------------------------------------
# STEP 2: Check total number of rows
# ------------------------------------------------------------
# Provides a quick check of how many daily records exist.
print("✅ Row count in gold.fraud_summary_daily:", df_check.count())

# ------------------------------------------------------------
# STEP 3: Preview first 10 rows ordered by date_key
# ------------------------------------------------------------
# Ordering by date_key allows chronological inspection of trends.
df_check.orderBy("date_key").show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Gold Layer – Build `exec_kpi_daily`**
# 
# #### **Objective**
# Create a daily executive KPI summary table for transaction monitoring.
# 
# #### **Why this aggregate is needed**
# While the fact table contains transaction-level data, executives typically require summarized daily KPIs instead of row-level detail.
# 
# This aggregate helps provide:
# 
# - total daily transaction volume
# - total transaction value
# - average transaction value
# - number of active accounts
# - gross daily amount movement
# 
# These KPIs are useful for dashboards, trend analysis, and executive decision-making.
# 
# #### **Measures included**
# The table contains:
# 
# - total transactions
# - total amount
# - average transaction amount
# - active accounts
# - gross amount
# 
# **Target table**
# `gold.exec_kpi_daily`
# 
# **Source table**
# `gold.fact_transactions`

# CELL ********************

# ============================================================
# GOLD CELL-28: Build exec_kpi_daily
# ============================================================

# ------------------------------------------------------------
# STEP 1: Load the fact_transactions table
# ------------------------------------------------------------
# This fact table contains transactional records with
# enriched dimension surrogate keys. We will use it to
# calculate executive-level daily KPIs.
df_fact = spark.table("gold.fact_transactions")

print("✅ Fact table loaded for executive KPI summary")
print("Row count:", df_fact.count())


# ------------------------------------------------------------
# STEP 2: Aggregate daily executive KPIs
# ------------------------------------------------------------
# KPIs calculated per date:
# - total_transactions       → total number of transactions
# - total_amount             → sum of transaction amounts
# - avg_transaction_amount   → average transaction value
# - active_accounts          → count of distinct origin accounts (daily active users)
# - gross_amount             → sum of all positive transaction amounts
df_exec_kpi = (
    df_fact
    .groupBy("date_key")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_transaction_amount"),
        F.countDistinct("origin_account_key").alias("active_accounts"),
        F.sum(
            F.when(F.col("amount") > 0, F.col("amount")).otherwise(0)
        ).alias("gross_amount")
    )
)

print("✅ Executive KPI dataframe created")
df_exec_kpi.show(10, truncate=False)


# ------------------------------------------------------------
# STEP 3: Write exec_kpi_daily to Gold layer
# ------------------------------------------------------------
# The resulting table contains daily executive KPIs,
# useful for dashboards and high-level reporting.
(
    df_exec_kpi.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.exec_kpi_daily")
)

print("✅ Created table: gold.exec_kpi_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Validate exec_kpi_daily**

# CELL ********************

# ------------------------------------------------------------
# STEP 1: Load exec_kpi_daily from Gold layer
# ------------------------------------------------------------
# This table contains daily executive KPIs such as total
# transactions, total amount, average transaction amount,
# active accounts, and gross amount.
df_check = spark.table("gold.exec_kpi_daily")

# ------------------------------------------------------------
# STEP 2: Check total row count
# ------------------------------------------------------------
# This gives a quick check of how many daily KPI records exist.
print("✅ Row count in gold.exec_kpi_daily:", df_check.count())

# ------------------------------------------------------------
# STEP 3: Preview the first 10 rows ordered by date
# ------------------------------------------------------------
# Ordering by date_key ensures chronological view.
# This allows visual validation of trends over time.
df_check.orderBy("date_key").show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Gold Layer – Build `ops_data_quality_daily`**
# 
# #### **Objective**
# Create a daily operational data quality summary table.
# 
# #### **Why this aggregate is needed**
# The Silver pipeline splits transactions into:
# 
# - valid rows → written to Silver
# - rejected rows → written to DQ quarantine
# 
# To monitor pipeline health and data quality trends, we need a daily summary table.
# 
# This aggregate helps us:
# 
# - track valid rows per day
# - track rejected rows per day
# - calculate total processed rows
# - calculate reject rate
# 
# This is useful for operational dashboards and monitoring.
# 
# #### **Measures included**
# The table contains:
# 
# - event_date
# - valid_rows
# - reject_rows
# - total_rows
# - reject_rate
# 
# **Target table**
# `gold.ops_data_quality_daily`
# 
# **Source tables**
# - `silver.paysim_transactions_clear`
# - `dq.paysim_rejects`

# CELL ********************

# ------------------------------------------------------------
# STEP 1: Load valid Silver transactions
# ------------------------------------------------------------
# The Silver layer contains all cleaned and accepted transactions.
df_silver_valid = spark.table("silver.paysim_transactions_clear")

print("✅ Silver valid table loaded")
print("Row count:", df_silver_valid.count())


# ------------------------------------------------------------
# STEP 2: Load rejected transactions from DQ quarantine
# ------------------------------------------------------------
# These are rows that failed data quality checks during ingestion.
df_reject = spark.table("dq.paysim_rejects")

print("✅ DQ reject table loaded")
print("Row count:", df_reject.count())


# ------------------------------------------------------------
# STEP 3: Aggregate valid transactions by event_date
# ------------------------------------------------------------
# Count the number of valid rows per day.
df_valid_daily = (
    df_silver_valid
    .groupBy(F.to_date("event_ts").alias("event_date"))
    .agg(
        F.count("*").alias("valid_rows")
    )
)


# ------------------------------------------------------------
# STEP 4: Aggregate rejected transactions by event_date
# ------------------------------------------------------------
# Count the number of rejected rows per day.
df_reject_daily = (
    df_reject
    .groupBy(F.to_date("event_ts").alias("event_date"))
    .agg(
        F.count("*").alias("reject_rows")
    )
)


# ------------------------------------------------------------
# STEP 5: Join valid + reject counts and calculate metrics
# ------------------------------------------------------------
# Combine valid and reject counts to compute operational DQ metrics:
# - total_rows  → total rows processed per day
# - reject_rate → fraction of rows that failed DQ
df_ops = (
    df_valid_daily
    .join(df_reject_daily, on="event_date", how="left")
    .fillna({"reject_rows": 0})
    .withColumn("total_rows", F.col("valid_rows") + F.col("reject_rows"))
    .withColumn("reject_rate", F.col("reject_rows") / F.col("total_rows"))
)

print("✅ Operational DQ summary dataframe created")
df_ops.show(10, truncate=False)


# ------------------------------------------------------------
# STEP 6: Write ops_data_quality_daily to Gold layer
# ------------------------------------------------------------
# Stores daily DQ metrics for operational monitoring and reporting.
(
    df_ops.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.ops_data_quality_daily")
)

print("✅ Created table: gold.ops_data_quality_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### **Validate ops_data_quality_daily**

# CELL ********************

# ------------------------------------------------------------
# STEP 1: Load ops_data_quality_daily from Gold layer
# ------------------------------------------------------------
# This table contains daily operational data quality metrics:
# - valid_rows: number of accepted transactions
# - reject_rows: number of rejected transactions
# - total_rows: total rows processed
# - reject_rate: fraction of rejected rows
df_check = spark.table("gold.ops_data_quality_daily")

# ------------------------------------------------------------
# STEP 2: Check total number of rows
# ------------------------------------------------------------
# Provides a quick check of how many daily records exist.
print("✅ Row count in gold.ops_data_quality_daily:", df_check.count())

# ------------------------------------------------------------
# STEP 3: Preview first 10 rows ordered by event_date
# ------------------------------------------------------------
# Ordering by event_date allows visual inspection of DQ trends
# over time to quickly identify days with high rejection rates.
df_check.orderBy("event_date").show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

activity_name = "nb_03_gold_model_build"
layer = "gold"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("meta.pipeline_activity_audit") \
    .filter("activity_name = 'nb_03_gold_model_build'") \
    .show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **What We Completed End-to-End**
# 
# ### **Bronze**
# - Parameterized ingestion
# - Watermark read
# - Raw append-only bronze
# - Metadata columns
# - Logging
# 
# ### **Silver**
# - Incremental filtering
# - Standardization
# - Derived columns
# - DQ split
# - Valid/reject writes
# - Watermark update
# 
# ### **Gold**
# - Dimensions
# - SCD2 profile dimension
# - Fact table
# - Daily fraud summary
# - Daily executive KPI summary
# - Daily operations DQ summary

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
