from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from google.cloud import bigquery
from datetime import datetime, timedelta
import logging

# Logging setup
logger = logging.getLogger("airflow")
logger.setLevel(logging.INFO)

# Load variables
PROJECT_ID = Variable.get("GCP_PROJECT_ID")
DATASET_ID = Variable.get("DATASET_ID")
client = bigquery.Client(project=PROJECT_ID)

# Table schema for test table
TEST_TABLE = "debug_test_table"
TEST_SCHEMA = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
]

def create_debug_table():
    logger.info("🚀 Starting create_debug_table task...")

    table_ref = client.dataset(DATASET_ID).table(TEST_TABLE)

    try:
        client.get_table(table_ref)
        logger.info(f"✅ Table {TEST_TABLE} already exists in {DATASET_ID}.")
    except Exception as e:
        logger.warning(f"⚠️ Table {TEST_TABLE} not found. Creating it now. Error: {e}")
        table = bigquery.Table(table_ref, schema=TEST_SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_at"
        )
        client.create_table(table)
        logger.info(f"✅ Table {TEST_TABLE} created successfully with schema: {TEST_SCHEMA}")

# DAG definition
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="debug_create_bigquery_table",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["debug", "bigquery"],
) as dag:

    create_table_task = PythonOperator(
        task_id="create_debug_table",
        python_callable=create_debug_table,
        execution_timeout=timedelta(minutes=5),
    )
