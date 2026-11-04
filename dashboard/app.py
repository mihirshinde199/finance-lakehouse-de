"""
dashboard/app.py

Streamlit dashboard over the Gold tables and the pipeline run log.

Run from the repo root with the project venv active:
    streamlit run dashboard/app.py

Credentials come from .env (or environment variables / Streamlit secrets):
    DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN
The token needs the `sql` scope. This app only runs SELECT statements.

Layout:
    Prices           close + SMA-20/50, volatility and volume for one ticker
    Performance      YTD return and max drawdown across the basket
    Pipeline health  freshness, last run per stage, run durations, watermark
"""

import os

import altair as alt
import pandas as pd
import streamlit as st
from databricks import sql
from dotenv import load_dotenv

load_dotenv()

# Keep in sync with freshness_gate in dags/finance_pipeline_dag.py.
MAX_STALENESS_DAYS = 5
CACHE_TTL_SECONDS = 300
STAGES = ["bronze", "silver", "gold"]

REQUIRED_SETTINGS = {
    "server_hostname": "DATABRICKS_SERVER_HOSTNAME",
    "http_path": "DATABRICKS_HTTP_PATH",
    "access_token": "DATABRICKS_TOKEN",
}

PRICES_SQL = """
    SELECT ticker, trade_date, close, daily_return, sma_20, sma_50,
           volatility_20d, volume, volume_avg_20d
    FROM gold.daily_price_summary
    ORDER BY ticker, trade_date
"""

PERFORMANCE_SQL = """
    SELECT ticker, as_of_date, cumulative_return_ytd, max_drawdown,
           avg_daily_volume_20d
    FROM gold.ticker_performance_summary
    WHERE as_of_date = (SELECT MAX(as_of_date) FROM gold.ticker_performance_summary)
    ORDER BY cumulative_return_ytd DESC
"""

# started_at is part of the partition key so a legitimate re-run of the same
# batch is kept, while an exact duplicate row (one execution logged twice)
# collapses to a single row.
RUN_LOG_SQL = """
    SELECT run_id, stage, rows_in, rows_out, rows_rejected,
           started_at, ended_at, status
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY run_id, stage, started_at ORDER BY ended_at DESC
        ) AS rn
        FROM control.pipeline_run_log
    )
    WHERE rn = 1
    ORDER BY started_at DESC
    LIMIT 200
"""

WATERMARK_SQL = """
    SELECT ticker, last_successful_trade_date, last_run_timestamp
    FROM control.ingestion_watermark
    ORDER BY ticker
"""


# ---------------------------------------------------------------------------
# Settings and data access
# ---------------------------------------------------------------------------

def get_setting(name):
    """Environment variable first, then Streamlit secrets, else None."""
    value = os.getenv(name)
    if value:
        return value
    try:
        return st.secrets.get(name)
    except Exception:  # no secrets file configured
        return None


def get_credentials():
    creds = {arg: get_setting(env) for arg, env in REQUIRED_SETTINGS.items()}
    missing = [REQUIRED_SETTINGS[arg] for arg, value in creds.items() if not value]
    return creds, missing


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def run_query(query: str) -> pd.DataFrame:
    creds, _ = get_credentials()
    with sql.connect(**creds) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
    return pd.DataFrame([tuple(row) for row in rows], columns=columns)


# ---------------------------------------------------------------------------
# Data preparation and formatting (plain functions, easy to unit test)
# ---------------------------------------------------------------------------

def to_numeric(df, columns):
    for col in columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def prepare_prices(df):
    df = df.copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return to_numeric(
        df,
        ["close", "daily_return", "sma_20", "sma_50",
         "volatility_20d", "volume", "volume_avg_20d"],
    )


def prepare_performance(df):
    df = df.copy()
    return to_numeric(
        df, ["cumulative_return_ytd", "max_drawdown", "avg_daily_volume_20d"]
    )


def prepare_run_log(df):
    """Timestamps become naive UTC; adds duration_min."""
    df = df.copy()
    for col in ("started_at", "ended_at"):
        df[col] = pd.to_datetime(df[col], utc=True).dt.tz_localize(None)
    df = to_numeric(df, ["rows_in", "rows_out", "rows_rejected"])
    df["duration_min"] = (df["ended_at"] - df["started_at"]).dt.total_seconds() / 60
    return df


def pct(value):
    return "n/a" if pd.isna(value) else f"{value:.2%}"


def usd(value):
    return "n/a" if pd.isna(value) else f"${value:,.2f}"


def signed_pct(value):
    return None if pd.isna(value) else f"{value:+.2%}"


def freshness(newest_trade_date, today, limit=MAX_STALENESS_DAYS):
    """Returns (age_in_days, is_fresh)."""
    age_days = (today - newest_trade_date).days
    return age_days, age_days <= limit


def latest_run_per_stage(run_log):
    latest = run_log.sort_values("started_at").groupby("stage").tail(1)
    return {row.stage: row for row in latest.itertuples()}


def describe_run(run):
    rows = "n/a" if pd.isna(run.rows_out) else f"{int(run.rows_out):,}"
    return f"{run.duration_min:.1f} min · {rows} rows"


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------

def price_chart(df):
    long = df.melt(
        id_vars="trade_date",
        value_vars=["close", "sma_20", "sma_50"],
        var_name="series",
        value_name="price",
    ).dropna(subset=["price"])
    return (
        alt.Chart(long)
        .mark_line()
        .encode(
            x=alt.X("trade_date:T", title=None),
            y=alt.Y("price:Q", title="Price (USD)", scale=alt.Scale(zero=False)),
            color=alt.Color("series:N", title=None),
            tooltip=[
                alt.Tooltip("trade_date:T", title="Date"),
                alt.Tooltip("series:N", title="Series"),
                alt.Tooltip("price:Q", title="Price", format=",.2f"),
            ],
        )
        .properties(height=340)
        .interactive()
    )


def volatility_chart(df):
    return (
        alt.Chart(df.dropna(subset=["volatility_20d"]))
        .mark_line(color="#6a1b9a")
        .encode(
            x=alt.X("trade_date:T", title=None),
            y=alt.Y(
                "volatility_20d:Q",
                title="20-day volatility",
                axis=alt.Axis(format=".1%"),
            ),
            tooltip=[
                alt.Tooltip("trade_date:T", title="Date"),
                alt.Tooltip("volatility_20d:Q", title="Volatility", format=".2%"),
            ],
        )
        .properties(height=200)
    )


def volume_chart(df):
    base = alt.Chart(df).encode(x=alt.X("trade_date:T", title=None))
    bars = base.mark_bar(opacity=0.45).encode(
        y=alt.Y("volume:Q", title="Volume"),
        tooltip=[
            alt.Tooltip("trade_date:T", title="Date"),
            alt.Tooltip("volume:Q", title="Volume", format=",.0f"),
        ],
    )
    average = base.mark_line(color="#ef6c00").encode(
        y=alt.Y("volume_avg_20d:Q", title="Volume")
    )
    return (bars + average).properties(height=200)


def ytd_chart(perf):
    return (
        alt.Chart(perf)
        .mark_bar()
        .encode(
            x=alt.X(
                "cumulative_return_ytd:Q",
                title="YTD return",
                axis=alt.Axis(format=".0%"),
            ),
            y=alt.Y("ticker:N", sort="-x", title=None),
            color=alt.condition(
                alt.datum.cumulative_return_ytd >= 0,
                alt.value("#2e7d32"),
                alt.value("#c62828"),
            ),
            tooltip=[
                alt.Tooltip("ticker:N", title="Ticker"),
                alt.Tooltip(
                    "cumulative_return_ytd:Q", title="YTD return", format=".1%"
                ),
            ],
        )
        .properties(height=340, title="YTD return")
    )


def drawdown_chart(perf):
    return (
        alt.Chart(perf)
        .mark_bar(color="#c62828")
        .encode(
            x=alt.X(
                "max_drawdown:Q",
                title="Max drawdown",
                axis=alt.Axis(format=".0%"),
            ),
            y=alt.Y("ticker:N", sort="x", title=None),
            tooltip=[
                alt.Tooltip("ticker:N", title="Ticker"),
                alt.Tooltip("max_drawdown:Q", title="Max drawdown", format=".1%"),
            ],
        )
        .properties(height=340, title="Max drawdown")
    )


def duration_chart(run_log):
    return (
        alt.Chart(run_log.dropna(subset=["duration_min"]))
        .mark_circle(size=70)
        .encode(
            x=alt.X("started_at:T", title=None),
            y=alt.Y("duration_min:Q", title="Duration (minutes)"),
            color=alt.Color("stage:N", title=None, sort=STAGES),
            tooltip=[
                alt.Tooltip("stage:N", title="Stage"),
                alt.Tooltip("started_at:T", title="Started (UTC)"),
                alt.Tooltip("duration_min:Q", title="Minutes", format=".1f"),
                alt.Tooltip("rows_out:Q", title="Rows out", format=",.0f"),
            ],
        )
        .properties(height=260)
    )


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

def render_prices(prices):
    ticker = st.selectbox("Ticker", sorted(prices["ticker"].unique()))
    df = prices[prices["ticker"] == ticker].sort_values("trade_date")
    latest = df.iloc[-1]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Close", usd(latest["close"]), signed_pct(latest["daily_return"]))
    col2.metric("SMA-20", usd(latest["sma_20"]))
    col3.metric("SMA-50", usd(latest["sma_50"]))
    col4.metric("20-day volatility", pct(latest["volatility_20d"]))

    st.caption(
        f"Latest trade date in Gold: {latest['trade_date']:%Y-%m-%d}. "
        "Gold keeps the trailing 90 trading days per ticker."
    )

    st.subheader("Price and moving averages")
    st.altair_chart(price_chart(df), use_container_width=True)

    left, right = st.columns(2)
    with left:
        st.subheader("Volatility")
        st.altair_chart(volatility_chart(df), use_container_width=True)
    with right:
        st.subheader("Volume")
        st.altair_chart(volume_chart(df), use_container_width=True)


def render_performance(performance):
    if performance.empty:
        st.info("No performance snapshot yet. Run the pipeline first.")
        return

    st.subheader(f"Snapshot as of {performance['as_of_date'].iloc[0]}")
    left, right = st.columns(2)
    left.altair_chart(ytd_chart(performance), use_container_width=True)
    right.altair_chart(drawdown_chart(performance), use_container_width=True)

    table = performance[
        ["ticker", "cumulative_return_ytd", "max_drawdown", "avg_daily_volume_20d"]
    ].copy()
    table["cumulative_return_ytd"] = table["cumulative_return_ytd"] * 100
    table["max_drawdown"] = table["max_drawdown"] * 100
    st.dataframe(
        table,
        hide_index=True,
        use_container_width=True,
        column_config={
            "ticker": "Ticker",
            "cumulative_return_ytd": st.column_config.NumberColumn(
                "YTD return", format="%.1f%%"
            ),
            "max_drawdown": st.column_config.NumberColumn(
                "Max drawdown", format="%.1f%%"
            ),
            "avg_daily_volume_20d": st.column_config.NumberColumn(
                "Avg daily volume (20d)", format="%.0f"
            ),
        },
    )
    st.caption(
        "YTD return compounds daily returns since Jan 1 of the current year. "
        "Max drawdown is the worst peak-to-trough decline over the full "
        "available history, not just this year."
    )


def render_health(prices, run_log, watermark):
    newest = prices["trade_date"].max().date()
    today = pd.Timestamp.now(tz="UTC").date()
    age_days, is_fresh = freshness(newest, today)

    if is_fresh:
        st.success(
            f"Data is fresh: newest trade date {newest}, {age_days} day(s) old "
            f"(limit {MAX_STALENESS_DAYS})."
        )
    else:
        st.error(
            f"Data is stale: newest trade date {newest} is {age_days} day(s) old "
            f"(limit {MAX_STALENESS_DAYS})."
        )

    watermark_rows = len(watermark)
    watermark_tickers = watermark["ticker"].nunique()
    if watermark_rows != watermark_tickers:
        st.warning(
            f"The watermark table has {watermark_rows} rows for "
            f"{watermark_tickers} tickers; it should be one row per ticker."
        )

    st.subheader("Last run per stage")
    if run_log.empty:
        st.info("No pipeline runs have been logged yet.")
    else:
        latest = latest_run_per_stage(run_log)
        for col, stage in zip(st.columns(len(STAGES)), STAGES):
            run = latest.get(stage)
            label = f"{stage.title()} (last run)"
            if run is None:
                col.metric(label, "no runs")
            else:
                col.metric(label, run.status, describe_run(run), delta_color="off")

        st.subheader("Run duration by stage")
        st.altair_chart(duration_chart(run_log), use_container_width=True)

        st.subheader("Recent runs")
        recent = run_log.sort_values("started_at", ascending=False).head(15)
        st.dataframe(
            recent[
                ["stage", "run_id", "started_at", "duration_min",
                 "rows_in", "rows_out", "rows_rejected", "status"]
            ],
            hide_index=True,
            use_container_width=True,
            column_config={
                "started_at": st.column_config.DatetimeColumn(
                    "Started (UTC)", format="YYYY-MM-DD HH:mm"
                ),
                "duration_min": st.column_config.NumberColumn(
                    "Duration (min)", format="%.1f"
                ),
            },
        )

    st.subheader("Ingestion watermark")
    st.caption(
        "Latest trade date ingested per ticker. The next fetch starts a few "
        "days before this date to pick up late corrections."
    )
    st.dataframe(
        watermark,
        hide_index=True,
        use_container_width=True,
        column_config={
            "ticker": "Ticker",
            "last_successful_trade_date": "Last trade date",
            "last_run_timestamp": "Updated (UTC)",
        },
    )


# ---------------------------------------------------------------------------
# App entry point
# ---------------------------------------------------------------------------

def main():
    st.set_page_config(page_title="Finance Lakehouse", page_icon="📈", layout="wide")
    st.title("Finance Lakehouse")
    st.caption(
        "Daily OHLCV for a fixed ticker basket (Yahoo Finance), landed in Delta "
        "Lake Bronze / Silver / Gold on Databricks and orchestrated by Airflow. "
        "All times are UTC."
    )

    _, missing = get_credentials()
    if missing:
        st.error(
            "Missing settings: " + ", ".join(missing)
            + ". Add them to .env (see .env.example)."
        )
        st.stop()

    with st.sidebar:
        st.header("Controls")
        if st.button("Refresh data"):
            st.cache_data.clear()
            st.rerun()
        st.caption(f"Each query is cached for {CACHE_TTL_SECONDS // 60} minutes.")

    try:
        with st.spinner(
            "Querying Databricks (a cold SQL warehouse can take a minute to start)..."
        ):
            prices = prepare_prices(run_query(PRICES_SQL))
            performance = prepare_performance(run_query(PERFORMANCE_SQL))
            run_log = prepare_run_log(run_query(RUN_LOG_SQL))
            watermark = run_query(WATERMARK_SQL)
    except Exception as exc:
        st.error(
            "Could not query Databricks. Check that the SQL warehouse is "
            "reachable and that the token in .env has not expired and still "
            "has the sql scope."
        )
        st.code(f"{type(exc).__name__}: {exc}")
        st.stop()

    if prices.empty:
        st.warning("gold.daily_price_summary is empty. Run the pipeline first.")
        st.stop()

    prices_tab, performance_tab, health_tab = st.tabs(
        ["Prices", "Performance", "Pipeline health"]
    )
    with prices_tab:
        render_prices(prices)
    with performance_tab:
        render_performance(performance)
    with health_tab:
        render_health(prices, run_log, watermark)


if __name__ == "__main__":
    main()