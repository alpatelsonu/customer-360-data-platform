from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

from datetime import timedelta

DBT_DIR = "/Users/sonupatel/aws2026/snowflake-dbt-project/customer-360-platform/dbt/customer_360"
DBT_BIN = "/Users/sonupatel/aws2026/snowflake-dbt-project/.venv-dbt/bin/dbt"

default_args = {

    "owner": "data-engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # Alerts
    "email": ["alpatelsonu@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="customer360_dbt_pipeline",
    default_args=default_args,
    description="Customer 360 dbt pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "customer360"],
) as dag:

    # Step 1: Run dbt models till bronze
    dbt_run_upstream = BashOperator(
        task_id="dbt_run_upstream",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} run -s tag:upstream",
    )

    # Step 2: Run dbt snapshot (SCD2)
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} snapshot",
    )

    # Step 3 Run dbt models downstream silver & gold
    dbt_run_downstream = BashOperator(
        task_id="dbt_run_downstream",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} run -s tag:downstream",
    )

    # Step 3 Run dbt models downstream gold
    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} run -s path:models/gold",
    )

    # Step 4: Run tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} test",
    )

    dbt_run_upstream >> dbt_snapshot >> dbt_run_downstream >> dbt_run_gold >> dbt_test