from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def debug_func():
    logging.info("✅ Airflow test DAG is working and logging correctly!")

with DAG(
    dag_id="test_scheduler_debug",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
) as dag:

    test_task = PythonOperator(
        task_id="log_test",
        python_callable=debug_func
    )
