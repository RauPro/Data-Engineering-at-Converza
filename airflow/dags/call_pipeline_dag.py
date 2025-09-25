from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


with DAG(
    dag_id="call_pipeline_minutely",
    description="Generate sample data then run ETL every minute",
    schedule="* * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["pipeline"],
) as dag:
    generate_calls = BashOperator(
        task_id="generate_calls",
        bash_command=(
            "set -euo pipefail; cd /opt/airflow/project && /home/airflow/.venv/bin/python main.py generate --num-calls 50"
        ),
    )

    run_etl = BashOperator(
        task_id="run_etl",
        bash_command=(
            "set -euo pipefail; cd /opt/airflow/project && /home/airflow/.venv/bin/python main.py etl --batch-size 50"
        ),
    )

    generate_calls >> run_etl


