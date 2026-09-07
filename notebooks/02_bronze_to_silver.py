# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze to Silver
# MAGIC
# MAGIC Reads new rows from `bronze.stock_prices_raw`, dedupes on (ticker,
# MAGIC trade_date), applies data quality rules (§10), computes daily_return,
# MAGIC and MERGEs valid rows into `silver.stock_prices_clean`. Invalid rows
# MAGIC are routed to `silver.stock_prices_rejected` instead of being dropped.
# MAGIC
# MAGIC Business key: (ticker, trade_date) -- see docs/architecture.md, §5, §8.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

# Which Bronze batch(es) to process this run. For V1, we reprocess ALL of
# Bronze each time Silver runs -- simplest correct approach given our data
# volume (§6: no partitioning needed at this scale). A future optimization
# would filter to only new batch_ids since the last Silver run.
BRONZE_TABLE = "bronze.stock_prices_raw"
SILVER_CLEAN_TABLE = "silver.stock_prices_clean"
SILVER_REJECTED_TABLE = "silver.stock_prices_rejected"

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Bronze and cast types
# MAGIC
# MAGIC Bronze stores trade_date as a loosely-typed string (§4) -- Silver is
# MAGIC where we enforce real types for the first time.

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)

typed_df = (
    bronze_df
    .withColumn("ticker", F.upper(F.trim(F.col("ticker"))))
    .withColumn("trade_date", F.to_date(F.col("trade_date")))
    .withColumn("open", F.col("open").cast("double"))
    .withColumn("high", F.col("high").cast("double"))
    .withColumn("low", F.col("low").cast("double"))
    .withColumn("close", F.col("close").cast("double"))
    .withColumn("adj_close", F.col("adj_close").cast("double"))
    .withColumn(
        "volume",
        F.when(F.col("volume") < 0, None).otherwise(F.col("volume").cast("long")),
    )
)

print(f"Bronze rows read: {typed_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Deduplicate on (ticker, trade_date)
# MAGIC
# MAGIC If the same ticker/date appears more than once (e.g. reprocessing
# MAGIC overlapping batches), keep only the row with the most recent
# MAGIC ingestion_timestamp. This must happen BEFORE the MERGE, since Delta
# MAGIC MERGE requires the source side to have no duplicate match keys.

# COMMAND ----------

dedup_window = Window.partitionBy("ticker", "trade_date").orderBy(
    F.col("ingestion_timestamp").desc()
)

deduped_df = (
    typed_df
    .withColumn("row_rank", F.row_number().over(dedup_window))
    .filter(F.col("row_rank") == 1)
    .drop("row_rank")
)

dropped_dupes = typed_df.count() - deduped_df.count()
print(f"Rows after dedup: {deduped_df.count()}  (removed {dropped_dupes} duplicates)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Apply data quality rules (§10)
# MAGIC
# MAGIC Each rule below either invalidates a row (routed to rejected) or
# MAGIC nulls a single field while keeping the row valid, exactly as
# MAGIC specified in the architecture doc's DQ rules table.

# COMMAND ----------

dq_checked_df = (
    deduped_df
    .withColumn(
        "dq_failure_reason",
        F.when(F.col("trade_date").isNull(), "invalid_trade_date")
        .when(F.col("ticker").isNull(), "missing_ticker")
        .when(F.col("close").isNull(), "missing_close_price")
        .when(F.col("high") < F.col("low"), "high_less_than_low")
        .when(F.col("high") < F.col("open"), "high_less_than_open")
        .when(F.col("high") < F.col("close"), "high_less_than_close")
        .when(F.col("low") > F.col("open"), "low_greater_than_open")
        .when(F.col("low") > F.col("close"), "low_greater_than_close")
        .otherwise(None),
    )
    .withColumn("is_valid", F.col("dq_failure_reason").isNull())
)

valid_count = dq_checked_df.filter("is_valid = true").count()
rejected_count = dq_checked_df.filter("is_valid = false").count()
total_count = valid_count + rejected_count
rejected_pct = (rejected_count / total_count * 100) if total_count > 0 else 0

print(f"Valid rows: {valid_count}")
print(f"Rejected rows: {rejected_count} ({rejected_pct:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Batch-level DQ gate
# MAGIC
# MAGIC If more than 5% of rows in this run failed DQ, that signals a
# MAGIC systemic source problem (not row-level noise) -- fail loudly here
# MAGIC rather than silently continuing. This is the check Airflow's
# MAGIC silver_dq_gate task relies on (§10, §11).

# COMMAND ----------

DQ_FAILURE_THRESHOLD_PCT = 5.0

if rejected_pct > DQ_FAILURE_THRESHOLD_PCT:
    raise ValueError(
        f"DQ gate failed: {rejected_pct:.2f}% of rows rejected, "
        f"exceeds {DQ_FAILURE_THRESHOLD_PCT}% threshold. "
        f"Investigate before proceeding -- this may indicate a source data problem."
    )
else:
    print(f"DQ gate passed: {rejected_pct:.2f}% <= {DQ_FAILURE_THRESHOLD_PCT}% threshold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Compute daily_return (valid rows only)
# MAGIC
# MAGIC Window function per ticker, ordered by trade_date. Only computed for
# MAGIC valid rows -- a rejected row's return isn't meaningful.

# COMMAND ----------

return_window = Window.partitionBy("ticker").orderBy("trade_date")

valid_df = dq_checked_df.filter("is_valid = true")

silver_clean_df = (
    valid_df
    .withColumn("prev_close", F.lag("close").over(return_window))
    .withColumn(
        "daily_return",
        F.when(
            (F.col("prev_close").isNotNull()) & (F.col("prev_close") != 0),
            (F.col("close") - F.col("prev_close")) / F.col("prev_close"),
        ).otherwise(None),
    )
    .withColumn("silver_updated_at", F.current_timestamp())
    .withColumnRenamed("batch_id", "source_batch_id")
    .select(
        "ticker", "trade_date", "open", "high", "low", "close", "adj_close",
        "volume", "daily_return", "is_valid", "dq_failure_reason",
        "source_batch_id", "silver_updated_at",
    )
)

silver_rejected_df = (
    dq_checked_df
    .filter("is_valid = false")
    .withColumn("silver_updated_at", F.current_timestamp())
    .withColumnRenamed("batch_id", "source_batch_id")
    .select(
        "ticker", "trade_date", "open", "high", "low", "close", "adj_close",
        "volume", "is_valid", "dq_failure_reason", "source_batch_id",
        "silver_updated_at",
    )
)

display(silver_clean_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: MERGE into silver.stock_prices_clean
# MAGIC
# MAGIC Keyed on (ticker, trade_date) -- this is the idempotency mechanism
# MAGIC from §8. Matched rows get updated (handles corrections/backfills);
# MAGIC unmatched rows get inserted (handles new days).

# COMMAND ----------

# Create the table on first run if it doesn't exist yet
if not spark.catalog.tableExists(SILVER_CLEAN_TABLE):
    (
        silver_clean_df.limit(0).write
        .format("delta")
        .saveAsTable(SILVER_CLEAN_TABLE)
    )

silver_clean_df.createOrReplaceTempView("silver_clean_updates")

spark.sql(f"""
    MERGE INTO {SILVER_CLEAN_TABLE} AS target
    USING silver_clean_updates AS source
    ON target.ticker = source.ticker AND target.trade_date = source.trade_date
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"Silver clean table row count: {spark.table(SILVER_CLEAN_TABLE).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Append rejected rows
# MAGIC
# MAGIC Rejected is a quarantine log, not a MERGE target -- append-only, so
# MAGIC we keep a full history of what was rejected and why, across runs.

# COMMAND ----------

if silver_rejected_df.count() > 0:
    (
        silver_rejected_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(SILVER_REJECTED_TABLE)
    )
    print(f"Appended {silver_rejected_df.count()} rows to {SILVER_REJECTED_TABLE}")
else:
    print("No rejected rows this run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks

# COMMAND ----------

print("=== Silver clean: per-ticker row counts ===")
display(
    spark.table(SILVER_CLEAN_TABLE)
    .groupBy("ticker")
    .count()
    .orderBy("ticker")
)

print("=== Silver clean: sample daily_return values ===")
display(
    spark.table(SILVER_CLEAN_TABLE)
    .filter("daily_return IS NOT NULL")
    .orderBy(F.desc("trade_date"))
    .limit(10)
)

if spark.catalog.tableExists(SILVER_REJECTED_TABLE):
    print("=== Rejected rows by reason ===")
    display(
        spark.table(SILVER_REJECTED_TABLE)
        .groupBy("dq_failure_reason")
        .count()
        .orderBy(F.desc("count"))
    )
