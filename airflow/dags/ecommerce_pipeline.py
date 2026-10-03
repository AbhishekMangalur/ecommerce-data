from datetime import datetime

from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="ecommerce_data_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "data-engineering"],
) as dag:

    ingest_data = BashOperator(
        task_id="ingest_data",
        bash_command="python /opt/airflow/ingestion/ingest.py",
        cwd="/opt/airflow",
    )

    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command="python /opt/airflow/ingestion/load_to_postgres.py",
        cwd="/opt/airflow",
    )

    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command="python /opt/airflow/spark/transform_orders.py",
    )

    data_quality = BashOperator(
        task_id="data_quality",
        bash_command="python /opt/airflow/tests/data_quality.py",
    )

    create_gold = BashOperator(
        task_id="create_gold",
        bash_command="python /opt/airflow/spark/create_gold.py",
    )

    ingest_data >> load_postgres >> transform_silver >> data_quality >> create_gold
