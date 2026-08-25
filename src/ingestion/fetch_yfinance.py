"""
fetch_yfinance.py

Ingestion script: pulls daily OHLCV data from Yahoo Finance for the configured
ticker basket and lands it as raw CSV files locally.

Responsibilities (and ONLY these):
  1. Read ticker basket + config from src/config/tickers.yaml
  2. Read (never write) control.ingestion_watermark from Databricks, to know
     how far back to pull per ticker
  3. Call yfinance for each ticker's date range
  4. Write raw results to data/landing/<batch_id>/<ticker>.csv
  5. Print a per-ticker summary

This script does NOT clean, validate, deduplicate, or write to Delta tables.
That is Bronze/Silver/Gold's job, run separately in Databricks notebooks.

The watermark table is only ever UPDATED by the Bronze notebook, after a
Bronze write succeeds -- never by this script. See docs/architecture.md, §7.
"""

import os
# import sys
import yaml
import yfinance as yf
import pandas as pd
from datetime import date, datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from databricks import sql

# ---------------------------------------------------------------------------
# Config / paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[2]
TICKERS_CONFIG_PATH = REPO_ROOT / "src" / "config" / "tickers.yaml"
LANDING_DIR = REPO_ROOT / "data" / "landing"


def load_config() -> dict:
    """Load tickers.yaml -- ticker basket + ingestion config."""
    if not TICKERS_CONFIG_PATH.exists():
        raise FileNotFoundError(f"Could not find config at {TICKERS_CONFIG_PATH}")
    with open(TICKERS_CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def load_env():
    """Load .env and return the three required Databricks connection values."""
    load_dotenv()
    hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    token = os.getenv("DATABRICKS_TOKEN")

    missing = [
        name
        for name, val in [
            ("DATABRICKS_SERVER_HOSTNAME", hostname),
            ("DATABRICKS_HTTP_PATH", http_path),
            ("DATABRICKS_TOKEN", token),
        ]
        if not val
    ]
    if missing:
        raise EnvironmentError(
            f"Missing required .env values: {', '.join(missing)}. "
            f"Check .env.example for the expected keys."
        )
    return hostname, http_path, token


# ---------------------------------------------------------------------------
# Watermark (read-only)
# ---------------------------------------------------------------------------

def read_watermark(hostname: str, http_path: str, token: str) -> dict:
    """
    Read control.ingestion_watermark and return a dict:
        { ticker: last_successful_trade_date (date or None) }

    This function ONLY reads. It never writes to the watermark table --
    that happens in the Bronze notebook, after Bronze write succeeds.
    """
    watermark = {}
    with sql.connect(
        server_hostname=hostname, http_path=http_path, access_token=token
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT ticker, last_successful_trade_date "
                "FROM control.ingestion_watermark"
            )
            rows = cursor.fetchall()
            for row in rows:
                ticker, last_date = row
                watermark[ticker] = last_date  # None if never ingested
    return watermark


# ---------------------------------------------------------------------------
# Date range logic
# ---------------------------------------------------------------------------

def compute_date_range(
    ticker: str,
    watermark: dict,
    history_start_date: str,
    trailing_refetch_days: int,
) -> tuple[date, date]:
    """
    Decide the [start_date, end_date] to pull for this ticker.

    - If the ticker has never been ingested (watermark is None), pull full
      history from history_start_date.
    - Otherwise, pull from (last_successful_trade_date - trailing_refetch_days)
      through today, to catch late-arriving corrections (§9).
    """
    end_date = date.today()
    last_date = watermark.get(ticker)

    if last_date is None:
        start_date = datetime.strptime(history_start_date, "%Y-%m-%d").date()
    else:
        # last_date may come back as a datetime.date or datetime.datetime
        # depending on the connector -- normalize to date.
        if isinstance(last_date, datetime):
            last_date = last_date.date()
        start_date = last_date - timedelta(days=trailing_refetch_days)

    return start_date, end_date


# ---------------------------------------------------------------------------
# yfinance pull
# ---------------------------------------------------------------------------

def fetch_ticker_data(ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
    """Pull OHLCV data for a single ticker over the given date range."""
    df = yf.download(
        ticker,
        start=start_date.isoformat(),
        end=(end_date + timedelta(days=1)).isoformat(),  # yfinance end is exclusive
        progress=False,
        auto_adjust=False,
    )
    if df.empty:
        return df
        # yfinance sometimes returns a MultiIndex column header (e.g. columns
    # like ('Close', 'AAPL')) even for a single ticker. If left as-is,
    # df.to_csv() writes this as two header rows, which downstream readers
    # (Spark) misinterpret as a real data row -- producing a bogus row with
    # a null ticker. Flatten to plain column names before anything else.
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.reset_index()
    df["ticker"] = ticker
    df["ingestion_timestamp"] = datetime.utcnow().isoformat()
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    config = load_config()
    tickers = config["tickers"]
    history_start_date = config["config"]["history_start_date"]
    trailing_refetch_days = config["config"]["trailing_refetch_days"]

    hostname, http_path, token = load_env()

    print(f"Reading watermark for {len(tickers)} tickers...")
    watermark = read_watermark(hostname, http_path, token)

    batch_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    batch_dir = LANDING_DIR / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)

    print(f"Landing batch: {batch_id}")
    print(f"{'Ticker':<8} {'Start':<12} {'End':<12} {'Rows':<6}")
    print("-" * 42)

    summary = []
    for ticker in tickers:
        start_date, end_date = compute_date_range(
            ticker, watermark, history_start_date, trailing_refetch_days
        )
        df = fetch_ticker_data(ticker, start_date, end_date)

        row_count = len(df)
        summary.append((ticker, start_date, end_date, row_count))
        print(f"{ticker:<8} {str(start_date):<12} {str(end_date):<12} {row_count:<6}")

        if row_count == 0:
            print(f"  WARNING: no data returned for {ticker} in this range.")
            continue

        out_path = batch_dir / f"{ticker}.csv"
        df.to_csv(out_path, index=False)

    total_rows = sum(s[3] for s in summary)
    print("-" * 42)
    print(f"Done. {total_rows} total rows written to {batch_dir}")


if __name__ == "__main__":
    main()