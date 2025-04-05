from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


# Simple Python function
def hello_world():
    print("Hello from your first DAG in PrecisCase! Comolocco")

# Default DAG arguments
default_args = {
    "owner": "you",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="example_hello_world",
    default_args=default_args,
    description="A simple DAG for PrecisCase",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "precis"],
) as dag:

    task_hello = PythonOperator(
        task_id="print_hello",
        python_callable=hello_world,
    )

    task_hello
