"""
dbt transformatsioonid — käivitub pärast kõigi kolme sissevõtu DAG-i.

Sammud:
    dbt_seed   — laeb seeds/allikad.csv, seeds/teadaolevad_dokumendid.csv
    dbt_run    — ehitab staging vaated, intermediate vaate, marts tabelid
    dbt_test   — käivitab schema.yml-is defineeritud kvaliteeditestid

Ajakava: iga päev pärast keskööd (@daily), kuid 1h hiljem kui sissevõtu DAG-id.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt_project"

with DAG(
    dag_id="dbt_pipeline",
    description="Käivitab dbt seed + run + test pärast sissevõtu DAG-e",
    schedule="0 1 * * *",                # iga päev kell 01:00, sissevõtt teeb 00:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["dbt", "tekstiandmete-kvaliteet", "transform"],
) as dag:

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_DIR} && dbt seed --profiles-dir .",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir .",
    )

    dbt_seed >> dbt_run >> dbt_test
