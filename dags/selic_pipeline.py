from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from bronze.ingest import fetch_selic_raw
from silver.transform import transform_selic
from gold.aggregate import aggregate_selic

_DEFAULT_ARGS = {
    "owner": "beanalytic",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="selic_pipeline",
    description=(
        "SELIC daily rates (BCB API) — Bronze → Silver → Gold. "
        "Covers 2020-01-01 to 2024-12-31. Trigger manually."
    ),
    default_args=_DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Historical dataset: trigger on demand
    catchup=False,
    tags=["bcb", "selic", "medallion", "beanalytic"],
    doc_md="""
## SELIC Pipeline

Ingests SELIC daily rates from the Brazilian Central Bank (BCB) public API
and processes them through three Medallion layers:

| Layer  | Task               | Output                          |
|--------|--------------------|---------------------------------|
| Bronze | `bronze_ingest`    | `data/bronze/selic_raw.parquet` |
| Silver | `silver_transform` | `data/silver/selic_trusted.parquet` |
| Gold   | `gold_aggregate`   | `data/gold/selic_mensal.parquet` + `selic_anual.parquet` |

Each task includes a quality gate that fails fast if data does not meet
expectations, preventing silent data corruption downstream.
    """,
) as dag:

    bronze_task = PythonOperator(
        task_id="bronze_ingest",
        python_callable=fetch_selic_raw,
        op_kwargs={"start": "01/01/2020", "end": "31/12/2024"},
        doc_md=(
            "Fetch raw SELIC data from BCB API and save as Parquet. "
            "Date range is explicit via op_kwargs — override to reprocess a different window. "
            "Row count is validated against dynamic bounds derived from the requested range."
        ),
    )

    silver_task = PythonOperator(
        task_id="silver_transform",
        python_callable=transform_selic,
        doc_md=(
            "Parse types, drop nulls, add derived columns (ano, mes, ano_mes, dia_semana). "
            "Quality gate: validates value range [0, 0.2] and year coverage [2020, 2024]."
        ),
    )

    gold_task = PythonOperator(
        task_id="gold_aggregate",
        python_callable=aggregate_selic,
        doc_md=(
            "Compute monthly (mean, compound rate, variation pp) and annual "
            "(mean, compound rate) metrics. Quality gate: validates business day "
            "completeness and annual rate bounds."
        ),
    )

    bronze_task >> silver_task >> gold_task
