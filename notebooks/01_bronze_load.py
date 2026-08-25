# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 - Bronze Load
# MAGIC
# MAGIC Reads raw OHLCV CSVs manually uploaded to a Volumes/DBFS landing path and
# MAGIC appends them into `bronze.stock_prices_raw` (Delta, append-only).
# MAGIC
# MAGIC This notebook is the ONLY place `control.ingestion_watermark` gets
# MAGIC written to -- and only after this Bronze write succeeds. See
# MAGIC docs/architecture.md, §7 and §8.
# MAGIC
# MAGIC V1: landing path is populated manually via the Databricks UI upload flow
# MAGIC (fetch_yfinance.py writes locally; this is a documented manual handoff
# MAGIC step, not yet automated).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, TimestampType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config -- set this to wherever you uploaded the batch in Step 1

# COMMAND ----------

LANDING_PATH = "/Volumes/workspace/default/landing/"  # adjust to your actual upload path
BATCH_ID = "20260825T014650Z"  # adjust to the batch folder you uploaded

batch_path = f"{LANDING_PATH}{BATCH_ID}/"

print(f"Reading batch from: {batch_path}")
files = dbutils.fs.ls(batch_path)
for f in files:
    print(f"  {f.name}  ({f.size} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read raw CSVs
# MAGIC
# MAGIC Bronze schema enforcement is intentionally light -- we accept whatever
# MAGIC yfinance/our script produced, tag it with lineage metadata, and land it.
# MAGIC Real validation happens in Silver.

# COMMAND ----------

raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(batch_path + "*.csv")
)

print(f"Rows read from landing: {raw_df.count()}")
raw_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conform to bronze.stock_prices_raw schema
# MAGIC
# MAGIC Column names coming out of yfinance/pandas may not exactly match our
# MAGIC target schema (case, naming) -- normalize here, but do NOT drop rows
# MAGIC or coerce values beyond renaming/casting. That's Silver's job.

# COMMAND ----------

bronze_df = (
    raw_df
    .withColumnRenamed("Date", "trade_date")
    .withColumnRenamed("Open", "open")
    .withColumnRenamed("High", "high")
    .withColumnRenamed("Low", "low")
    .withColumnRenamed("Close", "close")
    .withColumnRenamed("Adj Close", "adj_close")
    .withColumnRenamed("Volume", "volume")
    .withColumn("trade_date", F.col("trade_date").cast(StringType()))
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("ingestion_date", F.to_date(F.col("ingestion_timestamp")))
    .withColumn("source_file", F.col("_metadata.file_path"))
    .withColumn("batch_id", F.lit(BATCH_ID))
    .select(
        "ticker", "trade_date", "open", "high", "low", "close",
        "adj_close", "volume", "ingestion_timestamp", "ingestion_date",
        "source_file", "batch_id",
    )
)

display(bronze_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze -- append-only, partitioned by ingestion_date

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

(
    bronze_df.write
    .format("delta")
    .mode("append")
    .partitionBy("ingestion_date")
    .option("mergeSchema", "true")  # Bronze allows schema drift, per §13
    .saveAsTable("bronze.stock_prices_raw")
)

written_count = bronze_df.count()
print(f"Wrote {written_count} rows to bronze.stock_prices_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update the watermark -- ONLY after the Bronze write above succeeded
# MAGIC
# MAGIC For each ticker in this batch, set last_successful_trade_date to the
# MAGIC MAX trade_date we just landed, and last_run_timestamp to now.
# MAGIC MERGE keeps this idempotent -- rerunning this notebook for the same
# MAGIC batch just re-asserts the same watermark values.

# COMMAND ----------

watermark_updates = (
    bronze_df
    .filter(F.col("ticker").isNotNull())
    .groupBy("ticker")
    .agg(F.max("trade_date").alias("last_successful_trade_date"))
    .withColumn("last_run_timestamp", F.current_timestamp())
)

watermark_updates.createOrReplaceTempView("watermark_updates")

spark.sql("""
    MERGE INTO control.ingestion_watermark AS target
    USING watermark_updates AS source
    ON target.ticker = source.ticker
    WHEN MATCHED THEN UPDATE SET
        target.last_successful_trade_date = source.last_successful_trade_date,
        target.last_run_timestamp = source.last_run_timestamp
    WHEN NOT MATCHED THEN INSERT (
        ticker, last_successful_trade_date, last_run_timestamp
    ) VALUES (
        source.ticker, source.last_successful_trade_date, source.last_run_timestamp
    )
""")

print("Watermark updated for tickers in this batch:")
display(spark.sql("SELECT * FROM control.ingestion_watermark ORDER BY ticker"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks

# COMMAND ----------

print(f"Bronze table row count: {spark.table('bronze.stock_prices_raw').count()}")
display(
    spark.table("bronze.stock_prices_raw")
    .groupBy("ticker")
    .count()
    .orderBy("ticker")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DELETE FROM bronze.stock_prices_raw WHERE ticker IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ticker, COUNT(*) FROM bronze.stock_prices_raw 
# MAGIC WHERE ticker IS NULL
# MAGIC GROUP BY 1;