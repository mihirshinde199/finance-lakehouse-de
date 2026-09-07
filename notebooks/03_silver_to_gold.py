# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver to Gold
# MAGIC
# MAGIC Computes business-level aggregates from `silver.stock_prices_clean`:
# MAGIC   - gold.daily_price_summary        (SMA-20/50, volatility, volume avg)
# MAGIC   - gold.ticker_performance_summary (YTD return, max drawdown)
# MAGIC
# MAGIC Idempotency strategy: WINDOWED OVERWRITE, not MERGE (§8). Rolling
# MAGIC metrics depend on trailing rows, so we recompute a trailing window of
# MAGIC daily_price_summary on every run and replace that window wholesale,
# MAGIC rather than trying to incrementally patch rolling aggregates. This
# MAGIC avoids drift between incremental Gold logic and the true rolling math.
# MAGIC
# MAGIC ticker_performance_summary is a once-per-day snapshot keyed on
# MAGIC (ticker, as_of_date), so it uses MERGE instead -- reruns on the same
# MAGIC day should just update that day's snapshot, not duplicate it.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

SILVER_TABLE = "silver.stock_prices_clean"
GOLD_DAILY_TABLE = "gold.daily_price_summary"
GOLD_PERFORMANCE_TABLE = "gold.ticker_performance_summary"

# How many trailing trading days of Gold output we overwrite each run.
# Must be well beyond the longest rolling window (50) so SMA-50 etc. are
# never truncated at the edge of what we recompute.
GOLD_LOOKBACK_TRADING_DAYS = 90

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Silver, compute rolling metrics over FULL history
# MAGIC
# MAGIC We deliberately compute window functions over the entire Silver
# MAGIC history per ticker, not just the lookback window -- otherwise SMA-50
# MAGIC and volatility_20d would be wrong (truncated/incomplete) for the
# MAGIC first ~50 rows of whatever slice we took. We only WRITE the trailing
# MAGIC window at the end (Step 3), but every row's rolling value is computed
# MAGIC with full trailing context.

# COMMAND ----------

silver_df = spark.table(SILVER_TABLE).filter("is_valid = true")

roll_window_20 = (
    Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(-19, 0)
)
roll_window_50 = (
    Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(-49, 0)
)

metrics_df = (
    silver_df
    .withColumn("sma_20", F.avg("close").over(roll_window_20))
    .withColumn("sma_50", F.avg("close").over(roll_window_50))
    .withColumn("volatility_20d", F.stddev("daily_return").over(roll_window_20))
    .withColumn("volume_avg_20d", F.avg("volume").over(roll_window_20))
)

print(f"Rows with full rolling context computed: {metrics_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Determine the lookback cutoff per ticker
# MAGIC
# MAGIC "Last N trading days" per ticker, not a fixed calendar cutoff --
# MAGIC tickers may have slightly different trading calendars/history
# MAGIC lengths, so rank rows per ticker and keep the top N.

# COMMAND ----------

recency_window = Window.partitionBy("ticker").orderBy(F.desc("trade_date"))

gold_daily_df = (
    metrics_df
    .withColumn("recency_rank", F.row_number().over(recency_window))
    .filter(F.col("recency_rank") <= GOLD_LOOKBACK_TRADING_DAYS)
    .drop("recency_rank")
    .withColumn("gold_updated_at", F.current_timestamp())
    .select(
        "ticker", "trade_date", "close", "daily_return", "sma_20", "sma_50",
        "volatility_20d", "volume", "volume_avg_20d", "gold_updated_at",
    )
)

print(f"Rows to write (trailing {GOLD_LOOKBACK_TRADING_DAYS} trading days/ticker): "
      f"{gold_daily_df.count()}")
display(gold_daily_df.orderBy("ticker", F.desc("trade_date")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Windowed overwrite into gold.daily_price_summary
# MAGIC
# MAGIC Delete any existing rows that fall within this run's (ticker,
# MAGIC trade_date) window, then append the freshly computed rows. This is
# MAGIC equivalent to "overwrite this window" and is safe to rerun --
# MAGIC rerunning deletes and re-inserts the same window, producing the same
# MAGIC result (idempotent), without needing MERGE's row-by-row matching.

# COMMAND ----------

if not spark.catalog.tableExists(GOLD_DAILY_TABLE):
    (
        gold_daily_df.limit(0).write
        .format("delta")
        .saveAsTable(GOLD_DAILY_TABLE)
    )

gold_daily_df.createOrReplaceTempView("gold_daily_updates")

# Delete existing rows for any (ticker, trade_date) pair present in this
# run's window -- scoped precisely, not a blind full-table wipe.
spark.sql(f"""
    DELETE FROM {GOLD_DAILY_TABLE}
    WHERE (ticker, trade_date) IN (
        SELECT ticker, trade_date FROM gold_daily_updates
    )
""")

gold_daily_df.write.format("delta").mode("append").saveAsTable(GOLD_DAILY_TABLE)

print(f"gold.daily_price_summary row count: {spark.table(GOLD_DAILY_TABLE).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Compute ticker_performance_summary
# MAGIC
# MAGIC One row per ticker per run (as_of_date = today):
# MAGIC   - cumulative_return_ytd: compounded daily_return since Jan 1 of the
# MAGIC     current year
# MAGIC   - max_drawdown: largest peak-to-trough decline in cumulative
# MAGIC     return over the full available history
# MAGIC   - avg_daily_volume_20d: trailing 20-day average volume (reuse from
# MAGIC     the metrics_df computed above)

# COMMAND ----------

current_year = F.year(F.current_date())

ytd_df = (
    metrics_df
    .filter(F.year("trade_date") == current_year)
    .withColumn("daily_return_filled", F.coalesce(F.col("daily_return"), F.lit(0.0)))
    .withColumn("growth_factor", F.lit(1.0) + F.col("daily_return_filled"))
)

ytd_window = Window.partitionBy("ticker").orderBy("trade_date")

ytd_cumulative_df = ytd_df.withColumn(
    "cumulative_growth",
    F.exp(F.sum(F.log("growth_factor")).over(ytd_window)),
)

cumulative_return_df = (
    ytd_cumulative_df
    .withColumn(
        "rank_desc", F.row_number().over(Window.partitionBy("ticker").orderBy(F.desc("trade_date")))
    )
    .filter("rank_desc = 1")
    .withColumn("cumulative_return_ytd", F.col("cumulative_growth") - 1)
    .select("ticker", "cumulative_return_ytd")
)

# COMMAND ----------

drawdown_window = Window.partitionBy("ticker").orderBy("trade_date")

full_history_growth_df = (
    metrics_df
    .withColumn("daily_return_filled", F.coalesce(F.col("daily_return"), F.lit(0.0)))
    .withColumn("growth_factor", F.lit(1.0) + F.col("daily_return_filled"))
    .withColumn(
        "cumulative_growth",
        F.exp(F.sum(F.log("growth_factor")).over(drawdown_window)),
    )
    .withColumn(
        "running_peak",
        F.max("cumulative_growth").over(
            drawdown_window.rowsBetween(Window.unboundedPreceding, 0)
        ),
    )
    .withColumn(
        "drawdown",
        (F.col("cumulative_growth") - F.col("running_peak")) / F.col("running_peak"),
    )
)

max_drawdown_df = (
    full_history_growth_df
    .groupBy("ticker")
    .agg(F.min("drawdown").alias("max_drawdown"))
)

# COMMAND ----------

latest_volume_df = (
    gold_daily_df
    .withColumn(
        "rank_desc", F.row_number().over(Window.partitionBy("ticker").orderBy(F.desc("trade_date")))
    )
    .filter("rank_desc = 1")
    .select("ticker", F.col("volume_avg_20d").alias("avg_daily_volume_20d"))
)

# COMMAND ----------

gold_performance_df = (
    cumulative_return_df
    .join(max_drawdown_df, on="ticker", how="left")
    .join(latest_volume_df, on="ticker", how="left")
    .withColumn("as_of_date", F.current_date())
    .withColumn("last_updated", F.current_timestamp())
    .select(
        "ticker", "as_of_date", "cumulative_return_ytd", "max_drawdown",
        "avg_daily_volume_20d", "last_updated",
    )
)

display(gold_performance_df.orderBy("ticker"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: MERGE into gold.ticker_performance_summary
# MAGIC
# MAGIC Keyed on (ticker, as_of_date) -- reruns on the same day update that
# MAGIC day's snapshot rather than duplicating it.

# COMMAND ----------

if not spark.catalog.tableExists(GOLD_PERFORMANCE_TABLE):
    (
        gold_performance_df.limit(0).write
        .format("delta")
        .saveAsTable(GOLD_PERFORMANCE_TABLE)
    )

gold_performance_df.createOrReplaceTempView("gold_performance_updates")

spark.sql(f"""
    MERGE INTO {GOLD_PERFORMANCE_TABLE} AS target
    USING gold_performance_updates AS source
    ON target.ticker = source.ticker AND target.as_of_date = source.as_of_date
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"gold.ticker_performance_summary row count: "
      f"{spark.table(GOLD_PERFORMANCE_TABLE).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks

# COMMAND ----------

print("=== Gold daily summary: latest row per ticker ===")
display(
    spark.table(GOLD_DAILY_TABLE)
    .withColumn(
        "rn", F.row_number().over(Window.partitionBy("ticker").orderBy(F.desc("trade_date")))
    )
    .filter("rn = 1")
    .drop("rn")
    .orderBy("ticker")
)

print("=== Gold performance summary: today's snapshot ===")
display(
    spark.table(GOLD_PERFORMANCE_TABLE)
    .filter(F.col("as_of_date") == F.current_date())
    .orderBy(F.desc("cumulative_return_ytd"))
)
