# Data Dictionary

This document describes every table the pipeline creates, what each column means, and how derived values are calculated. It reflects the notebooks as they run (`notebooks/01_bronze_load.py`, `02_bronze_to_silver.py`, `03_silver_to_gold.py`) and the fetch script (`src/ingestion/fetch_yfinance.py`).

## Overview

```
Yahoo Finance              -> landing volume
landing volume             -> bronze.stock_prices_raw
bronze.stock_prices_raw    -> silver.stock_prices_clean      (rows that pass validation)
bronze.stock_prices_raw    -> silver.stock_prices_rejected   (rows that fail validation)
silver.stock_prices_clean  -> gold.daily_price_summary
silver.stock_prices_clean  -> gold.ticker_performance_summary

control.ingestion_watermark   read by the fetch task, written by the Bronze notebook
control.pipeline_run_log      written by all three notebooks
```

**Conventions**

- All timestamps are UTC.
- Table names are two-level (`schema.table`) and resolve in the default `workspace` catalog.
- The basket is 12 tickers: AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA, JPM, V, UNH, XOM and JNJ. History starts at 2023-01-01 (`src/config/tickers.yaml`).
- Prices come from yfinance with `auto_adjust=False`: `close` is Yahoo's closing price (already split-adjusted) and `adj_close` additionally adjusts for dividends.

## Source fields

The fetch task writes one CSV per ticker per batch. Spark reads the CSVs with a header row and renames the columns:

| CSV column | Bronze column |
|---|---|
| `Date` | `trade_date` |
| `Open` | `open` |
| `High` | `high` |
| `Low` | `low` |
| `Close` | `close` |
| `Adj Close` | `adj_close` |
| `Volume` | `volume` |
| `ticker` (added by the fetch script) | `ticker` |

The fetch script also writes an `ingestion_timestamp` column into each CSV. Bronze replaces it with the load time.

## Bronze

### `bronze.stock_prices_raw`

Raw landing table. Append-only and partitioned by `ingestion_date`. Duplicates are expected, because overlapping fetch windows and re-run batches load the same trading day more than once. Schema drift is tolerated (`mergeSchema`).

| Column | Type | Description |
|---|---|---|
| `ticker` | string | Symbol as received |
| `trade_date` | string | Trading day as text, not yet validated. Parsed with `to_date` in Silver |
| `open` | double | Opening price (type inferred from the CSV) |
| `high` | double | Session high |
| `low` | double | Session low |
| `close` | double | Closing price |
| `adj_close` | double | Closing price adjusted for dividends |
| `volume` | long | Shares traded |
| `ingestion_timestamp` | timestamp | When the Bronze notebook loaded the row. Silver uses it to keep the latest copy of a duplicate |
| `ingestion_date` | date | Date part of `ingestion_timestamp`. Partition column |
| `source_file` | string | Full path of the CSV the row came from (`_metadata.file_path`) |
| `batch_id` | string | Fetch batch, a UTC timestamp such as `20261023T112421Z`. Matches the landing folder name |

## Silver

Silver reads the whole Bronze table on every run, so its row counts equal the size of the deduplicated history.

### `silver.stock_prices_clean`

One row per `(ticker, trade_date)`. Loaded with `MERGE`: matching rows are updated and new rows are inserted.

| Column | Type | Description |
|---|---|---|
| `ticker` | string | Symbol, trimmed and upper-cased. Part of the key |
| `trade_date` | date | Trading day. Part of the key |
| `open`, `high`, `low`, `close`, `adj_close` | double | Prices, cast to double |
| `volume` | long | Shares traded. A negative value in the source is stored as NULL |
| `daily_return` | double | `(close - prev_close) / prev_close`, where `prev_close` is the previous trading day's close for the same ticker. NULL for a ticker's first day or when `prev_close` is 0 |
| `is_valid` | boolean | Always true in this table. Kept so the schema matches the rejected table |
| `dq_failure_reason` | string | Always NULL in this table. Kept for the same reason |
| `source_batch_id` | string | `batch_id` of the Bronze row that was kept |
| `silver_updated_at` | timestamp | When Silver last wrote the row. It refreshes on every run, because every row is re-merged |

**Deduplication.** If Bronze holds several rows for the same `(ticker, trade_date)`, the one with the latest `ingestion_timestamp` is kept. This happens before the `MERGE`, because Delta requires the source side to have no duplicate keys.

### `silver.stock_prices_rejected`

Quarantine for rows that failed validation, with the same columns as the clean table except `daily_return`. Here `is_valid` is always false and `dq_failure_reason` is always filled in.

Loaded with `MERGE` on `(ticker, trade_date, dq_failure_reason)`. Silver re-validates all of Bronze on every run, so a row that stays bad is rejected again each time. The merge keeps one row per failure instead of adding a copy per run. The match is null-safe (`<=>`), because a row rejected for a missing ticker or an invalid date has a NULL key column.

A row is checked in the order below, and the first failing rule becomes its `dq_failure_reason`. The table stays empty while all data passes.

| Order | `dq_failure_reason` | Condition |
|---|---|---|
| 1 | `invalid_trade_date` | `trade_date` is NULL after parsing |
| 2 | `missing_ticker` | `ticker` is NULL |
| 3 | `missing_close_price` | `close` is NULL |
| 4 | `high_less_than_low` | `high < low` |
| 5 | `high_less_than_open` | `high < open` |
| 6 | `high_less_than_close` | `high < close` |
| 7 | `low_greater_than_open` | `low > open` |
| 8 | `low_greater_than_close` | `low > close` |

**Batch gate.** After validation, if more than 5% of the rows are rejected, the notebook raises an error and the Airflow task fails. That level of rejection points to a source problem and not to noise in individual rows.

## Gold

### `gold.daily_price_summary`

One row per `(ticker, trade_date)` for the trailing 90 trading days of each ticker. Each run deletes the rows it is about to rewrite (`MERGE ... WHEN MATCHED THEN DELETE`) and then appends the recomputed window.

The rolling metrics are computed over the full Silver history before the 90-day slice is taken, so rows at the start of the slice still use their full 20 or 50 days of context.

| Column | Type | Description |
|---|---|---|
| `ticker` | string | Symbol. Part of the key |
| `trade_date` | date | Trading day. Part of the key |
| `close` | double | Closing price |
| `daily_return` | double | Same as in Silver |
| `sma_20` | double | Average `close` over the last 20 trading days, including the current one |
| `sma_50` | double | Average `close` over the last 50 trading days, including the current one |
| `volatility_20d` | double | Sample standard deviation of `daily_return` over the last 20 trading days. Daily, not annualized |
| `volume` | long | Shares traded |
| `volume_avg_20d` | double | Average `volume` over the last 20 trading days |
| `gold_updated_at` | timestamp | When Gold wrote the row |

### `gold.ticker_performance_summary`

One row per `(ticker, as_of_date)`: a daily snapshot that is kept over time. Loaded with `MERGE`, so re-running on the same day updates that day's snapshot instead of adding a row.

| Column | Type | Description |
|---|---|---|
| `ticker` | string | Symbol. Part of the key |
| `as_of_date` | date | Date the Gold job ran. Part of the key |
| `cumulative_return_ytd` | double | Compounded return for the current calendar year (formula below) |
| `max_drawdown` | double | Worst peak-to-trough decline over the full history (formula below). Between -1 and 0 |
| `avg_daily_volume_20d` | double | `volume_avg_20d` from the ticker's latest row |
| `last_updated` | timestamp | When Gold wrote the row |

**Formulas**

- **YTD return:** the product of `(1 + daily_return)` over the trading days of the current calendar year, minus 1. Because the first trading day's return is measured against the previous year's final close, this equals the latest close divided by that close, minus 1. It is computed as `exp(sum(log(1 + r))) - 1`.
- **Max drawdown:** build a growth curve from the compounded daily returns, track its running peak, and take the minimum of `(growth - peak) / peak`. A ticker that only ever rose has a drawdown of 0.
- **Returns are price returns.** They use `close`, not `adj_close`, so dividends are not included.
- A ticker with no rows in the current year has no snapshot row.

## Control tables

### `control.ingestion_watermark`

Incremental ingestion state, one row per ticker.

| Column | Type | Description |
|---|---|---|
| `ticker` | string, NOT NULL | Symbol. One row per ticker |
| `last_successful_trade_date` | date | Latest trading day loaded into Bronze for the ticker |
| `last_run_timestamp` | timestamp | When the Bronze notebook last updated the row |

How it is used:

- The fetch task reads it and never writes it. A ticker with no row, or a NULL date, gets a full backfill from the configured start date.
- Otherwise the fetch starts 5 days before `last_successful_trade_date` (`trailing_refetch_days`), so late corrections from the source are picked up.
- The Bronze notebook updates it with a `MERGE`, only after the Bronze write has succeeded. New tickers are inserted by the same statement.
- The freshness gate fails the run if this table does not have exactly one row per configured ticker.

### `control.pipeline_run_log`

One row per stage per run. There is no primary key. A run is identified by `(run_id, stage, started_at)`.

| Column | Type | Description |
|---|---|---|
| `run_id` | string | Bronze uses the batch ID, so re-loading a batch reuses it. Silver and Gold use the UTC time the notebook started, for example `20261023T113821Z` |
| `stage` | string | `bronze`, `silver` or `gold` |
| `rows_in` | long | Rows entering the stage (see below) |
| `rows_out` | long | Rows leaving the stage (see below) |
| `rows_rejected` | long | Rows rejected by validation. Only Silver can be non-zero |
| `started_at` | timestamp | When the notebook started |
| `ended_at` | timestamp | When the notebook finished |
| `status` | string | `SUCCESS`. Failed runs raise an error before the log is written, so they leave no row. Airflow's task state is the record of failures |

**What the counts mean for each stage**

| Stage | `rows_in` | `rows_out` | `rows_rejected` |
|---|---|---|---|
| bronze | Rows read from the batch's CSV files | Rows appended to Bronze | 0 |
| silver | Rows after deduplication, across all of Bronze | Valid rows merged into Silver | Rows that failed validation |
| gold | Valid Silver rows used for the rolling metrics | Rows written to `gold.daily_price_summary` (90 days x tickers) | 0 |

Because Silver reprocesses all of Bronze each run, its `rows_in` and `rows_out` track the total size of the history, not the size of the latest batch.

## Known limitations

- **Failures are not in the run log.** Only successful runs write a row. Use the Airflow task history for failures.
- **Returns exclude dividends.** `daily_return`, YTD return and max drawdown are based on `close`. `adj_close` is stored but not used in any metric.
- **Short histories.** For a new ticker, the moving averages and the volume average use the rows that exist when a window is not yet full. Volatility needs at least two daily returns and is NULL before that.