"""
finance_pipeline_dag.py

Orchestrates the daily finance lakehouse pipeline:
    fetch_and_upload_raw_data
        -> validate_raw_landing
            -> bronze_load           (Databricks job)
                -> bronze_to_silver  (Databricks job; DQ gate is embedded
                                       inside the notebook itself -- see
                                       the design note below)
                    -> silver_to_gold (Databricks job)
                        -> notify_success
    (any task failure) -> notify_failure

Design note on DQ gates (deviation from the original architecture diagram):
The Silver notebook (02_bronze_to_silver.py) already raises an exception
and halts if the rejected-row percentage exceeds the threshold (§10, Step 4
in that notebook). That means the Databricks job itself fails when DQ is
bad -- which fails the Airflow task automatically. A separate Airflow-side
DQ-gate task would just be re-checking something the notebook already
checks, so it was simplified out. The gate still exists; it just lives in
the notebook, not as a duplicate Airflow task.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DATABRICKS_CONN_ID = "databricks_default"

# Cluster spec for the Databricks jobs this DAG triggers. Free Edition is
# serverless-only (Phase 3 decision) -- verify against current Databricks
# Free Edition job-submission docs whether this new_cluster block is even
# needed, or whether job submission defaults to serverless automatically
# when no billable cluster spec is given. Flagged as an open item to
# confirm during first real DAG run, not guessed at here.
NOTEBOOK_BASE_PARAMS = {
    "new_cluster": {
        "spark_version": "15.4.x-scala2.12",
        "num_workers": 0,  # serverless-style job cluster
    }
}

WORKSPACE_NOTEBOOK_PATH_BRONZE = "/Workspace/finance-lakehouse/01_bronze_load"
WORKSPACE_NOTEBOOK_PATH_SILVER = "/Workspace/finance-lakehouse/02_bronze_to_silver"
WORKSPACE_NOTEBOOK_PATH_GOLD = "/Workspace/finance-lakehouse/03_silver_to_gold"

SLACK_WEBHOOK_ENV_VAR = "SLACK_WEBHOOK_URL"  # read from Airflow's .env mount

default_args = {
    "owner": "mihir",
    "retries": 0,  # per-task retry policy set individually below, per §11
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# Python callables
# ---------------------------------------------------------------------------

def fetch_and_upload_raw_data(**context):
    """
    Runs the same ingestion logic as src/ingestion/fetch_yfinance.py, then
    uploads the resulting batch folder to the Databricks Volume landing
    path using the Databricks SDK -- replacing the manual UI upload used
    during development (Phase 5/6). This is what makes the DAG runnable
    unattended.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/src")

    from ingestion.fetch_yfinance import (
        load_config, load_env, read_watermark, compute_date_range,
        fetch_ticker_data, LANDING_DIR,
    )
    from databricks.sdk import WorkspaceClient
    from datetime import datetime as dt

    config = load_config()
    tickers = config["tickers"]
    history_start_date = config["config"]["history_start_date"]
    trailing_refetch_days = config["config"]["trailing_refetch_days"]

    hostname, http_path, token = load_env()
    watermark = read_watermark(hostname, http_path, token)

    batch_id = dt.utcnow().strftime("%Y%m%dT%H%M%SZ")
    batch_dir = LANDING_DIR / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    for ticker in tickers:
        start_date, end_date = compute_date_range(
            ticker, watermark, history_start_date, trailing_refetch_days
        )
        df = fetch_ticker_data(ticker, start_date, end_date)
        if df.empty:
            continue
        out_path = batch_dir / f"{ticker}.csv"
        df.to_csv(out_path, index=False)
        total_rows += len(df)

    if total_rows == 0:
        raise ValueError(
            f"fetch_and_upload_raw_data: zero rows fetched across all "
            f"tickers for batch {batch_id}. Failing task rather than "
            f"proceeding with an empty Bronze load."
        )

    # Upload to the Databricks Volume landing path
    w = WorkspaceClient()  # picks up auth from Databricks connection env vars
    volume_path = f"/Volumes/workspace/default/landing/{batch_id}"
    for csv_file in batch_dir.glob("*.csv"):
        with open(csv_file, "rb") as f:
            w.files.upload(f"{volume_path}/{csv_file.name}", f, overwrite=True)

    # Pass batch_id downstream to the Bronze task via XCom
    context["ti"].xcom_push(key="batch_id", value=batch_id)
    print(f"Uploaded batch {batch_id} ({total_rows} rows) to {volume_path}")


def validate_raw_landing(**context):
    """Fail fast if the fetch task somehow produced no batch_id."""
    batch_id = context["ti"].xcom_pull(
        task_ids="fetch_and_upload_raw_data", key="batch_id"
    )
    if not batch_id:
        raise ValueError("No batch_id found from fetch task -- failing early.")
    print(f"Validated batch_id: {batch_id}")


def notify_slack(context, status: str):
    """Posts a simple failure/success message to Slack via webhook."""
    import os
    import requests

    webhook_url = os.getenv(SLACK_WEBHOOK_ENV_VAR)
    if not webhook_url:
        print("SLACK_WEBHOOK_URL not set -- skipping Slack notification.")
        return

    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    message = f"finance-lakehouse-de pipeline `{status}` — DAG: {dag_id}, run: {run_id}"

    try:
        requests.post(webhook_url, json={"text": message}, timeout=10)
    except Exception as e:
        # Never let a notification failure fail the pipeline itself
        print(f"Slack notification failed (non-fatal): {e}")


def on_failure_callback(context):
    notify_slack(context, "FAILED")


def notify_success(**context):
    notify_slack(context, "SUCCESS")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="finance_pipeline_dag",
    default_args=default_args,
    description="Daily finance lakehouse: fetch -> Bronze -> Silver -> Gold",
    schedule_interval="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    on_failure_callback=on_failure_callback,
    tags=["finance-lakehouse"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_and_upload_raw_data",
        python_callable=fetch_and_upload_raw_data,
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
    )

    validate_task = PythonOperator(
        task_id="validate_raw_landing",
        python_callable=validate_raw_landing,
        retries=0,
    )

    bronze_task = DatabricksSubmitRunOperator(
        task_id="bronze_load",
        databricks_conn_id=DATABRICKS_CONN_ID,
        notebook_task={
            "notebook_path": WORKSPACE_NOTEBOOK_PATH_BRONZE,
            "base_parameters": {
                "batch_id": "{{ ti.xcom_pull(task_ids='fetch_and_upload_raw_data', key='batch_id') }}",
            },
        },
        **NOTEBOOK_BASE_PARAMS,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    silver_task = DatabricksSubmitRunOperator(
        task_id="bronze_to_silver",
        databricks_conn_id=DATABRICKS_CONN_ID,
        notebook_task={"notebook_path": WORKSPACE_NOTEBOOK_PATH_SILVER},
        **NOTEBOOK_BASE_PARAMS,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    gold_task = DatabricksSubmitRunOperator(
        task_id="silver_to_gold",
        databricks_conn_id=DATABRICKS_CONN_ID,
        notebook_task={"notebook_path": WORKSPACE_NOTEBOOK_PATH_GOLD},
        **NOTEBOOK_BASE_PARAMS,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    success_task = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        retries=0,
    )

    fetch_task >> validate_task >> bronze_task >> silver_task >> gold_task >> success_task