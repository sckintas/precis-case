from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.email import send_email
from google.cloud import bigquery
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import uuid
import os
import logging
import json
from typing import Dict, List, Optional

# Logger configuration
logger = logging.getLogger("airflow")
logger.setLevel(logging.INFO)

# Variables from Airflow UI
PROJECT_ID = Variable.get("GCP_PROJECT_ID")
DATASET_ID = Variable.get("DATASET_ID")
BQ_REGION = Variable.get("BQ_REGION", default_var="us")
ALERT_EMAILS = Variable.get("ALERT_EMAILS", default_var="").split(",")

# BigQuery client
client = bigquery.Client(project=PROJECT_ID)

# Metadata table for logging
METADATA_TABLE = f"{PROJECT_ID}.{DATASET_ID}.pipeline_metadata"

# GitHub-hosted mock JSON endpoints
MOCK_API_URLS = {
    "campaigns": "https://raw.githubusercontent.com/sckintas/preciscase-mock-google-ads-api/main/precis/campaigns.json",
    "ad_groups": "https://raw.githubusercontent.com/sckintas/preciscase-mock-google-ads-api/main/precis/ad_groups.json",
    "ads": "https://raw.githubusercontent.com/sckintas/preciscase-mock-google-ads-api/main/precis/ads.json",
    "metrics": "https://raw.githubusercontent.com/sckintas/preciscase-mock-google-ads-api/main/precis/metrics.json",
    "budgets": "https://raw.githubusercontent.com/sckintas/preciscase-mock-google-ads-api/main/precis/budgets.json"
}

# Schema definitions for each table
TABLE_SCHEMAS = {
    "campaigns": [
        {"name": "campaign_id", "type": "INTEGER"},
        {"name": "campaign_name", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "start_date", "type": "DATE"},
        {"name": "end_date", "type": "DATE"},
        {"name": "budget", "type": "FLOAT"}
    ],
    "ad_groups": [
        {"name": "ad_group_id", "type": "INTEGER"},
        {"name": "campaign_id", "type": "INTEGER"},
        {"name": "ad_group_name", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "start_date", "type": "DATE"},
        {"name": "end_date", "type": "DATE"}
    ],
    "ads": [
        {"name": "ad_id", "type": "INTEGER"},
        {"name": "ad_group_id", "type": "INTEGER"},
        {"name": "headline", "type": "STRING"},
        {"name": "description", "type": "STRING"},
        {"name": "final_url", "type": "STRING"},
        {"name": "start_date", "type": "DATE"},
        {"name": "end_date", "type": "DATE"}
    ],
    "metrics": [
        {"name": "campaign_id", "type": "STRING"},  # Changed from INTEGER to STRING
        {"name": "ad_group_id", "type": "STRING"},  # Also check if this needs changing
        {"name": "ad_id", "type": "STRING"},       # Also check if this needs changing
        {"name": "date", "type": "DATE"},
        {"name": "impressions", "type": "INTEGER"},
        {"name": "clicks", "type": "INTEGER"},
        {"name": "cost", "type": "FLOAT"},
        {"name": "conversions", "type": "FLOAT"}
    ],
    "budgets": [
        {"name": "campaign_id", "type": "INTEGER"},
        {"name": "start_date", "type": "DATE"},
        {"name": "end_date", "type": "DATE"},
        {"name": "budget_amount", "type": "FLOAT"},
        {"name": "budget_status", "type": "STRING"}
    ]
}

# Partitioning fields
REFERENCE_FIELDS = {
    "campaigns": "start_date",
    "ad_groups": "start_date",
    "ads": "start_date",
    "metrics": "date",
    "budgets": "start_date"
}

# Required fields for each table (data validation)
REQUIRED_FIELDS = {
    "campaigns": ["campaign_id", "campaign_name", "start_date"],
    "ad_groups": ["ad_group_id", "campaign_id", "start_date"],
    "ads": ["ad_id", "ad_group_id", "start_date"],
    "metrics": ["date", "campaign_id"],
    "budgets": ["campaign_id", "start_date"]
}

def log_pipeline_metadata(
    table_name: str,
    status: str,
    rows_processed: int = 0,
    error_message: Optional[str] = None,
    execution_date: Optional[datetime] = None
):
    """Log pipeline execution metadata to BigQuery."""
    metadata = {
        "run_id": str(uuid.uuid4()),
        "table_name": table_name,
        "execution_date": (execution_date or datetime.utcnow()).isoformat(),  # Convert to string
        "status": status,
        "rows_processed": rows_processed,
        "error_message": error_message,
        "timestamp": datetime.utcnow().isoformat()  # Convert to string
    }
    
    try:
        errors = client.insert_rows_json(METADATA_TABLE, [metadata])
        if errors:
            logger.error(f"❌ Failed to log metadata: {errors}")
    except Exception as e:
        logger.error(f"❌ Error logging metadata: {str(e)}")

def notify_failure(context):
    """Send email notification on task failure."""
    task_instance = context.get('task_instance')
    execution_date = context.get('execution_date')
    
    subject = f"Airflow Alert: Failed Task in DAG {task_instance.dag_id}"
    body = f"""
    <h3>Task Failed</h3>
    <ul>
        <li><b>Task:</b> {task_instance.task_id}</li>
        <li><b>DAG:</b> {task_instance.dag_id}</li>
        <li><b>Execution Time:</b> {execution_date}</li>
        <li><b>Log URL:</b> <a href="{task_instance.log_url}">{task_instance.log_url}</a></li>
    </ul>
    """
    
    try:
        for email in ALERT_EMAILS:
            if email.strip():
                send_email(to=email.strip(), subject=subject, html_content=body)
        logger.info("✅ Sent failure notifications")
    except Exception as e:
        logger.error(f"❌ Failed to send notification email: {str(e)}")

def fetch_data_from_api(url: str) -> pd.DataFrame:
    """Fetch data from API endpoint with error handling."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return pd.read_json(response.text)
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Failed to fetch data from {url}: {str(e)}")
        raise
    except ValueError as e:
        logger.error(f"❌ Invalid JSON data from {url}: {str(e)}")
        raise

def validate_data(df: pd.DataFrame, table_name: str) -> bool:
    """Validate data against required fields and constraints."""
    required_fields = REQUIRED_FIELDS.get(table_name, [])
    
    # Check required fields exist
    missing_fields = [field for field in required_fields if field not in df.columns]
    if missing_fields:
        logger.error(f"❌ Missing required fields in {table_name}: {missing_fields}")
        return False
    
    # Check for nulls in required fields
    for field in required_fields:
        if df[field].isnull().any():
            logger.error(f"❌ Null values found in required field {field} in {table_name}")
            return False
    
    # Check date fields are valid
    date_field = REFERENCE_FIELDS.get(table_name)
    if date_field and date_field in df.columns:
        try:
            df[date_field] = pd.to_datetime(df[date_field])
        except Exception as e:
            logger.error(f"❌ Invalid date values in {date_field} for {table_name}: {str(e)}")
            return False
    
    return True

def create_bigquery_tables():
    """Create BigQuery tables with proper schemas and partitioning."""
    # First ensure metadata table exists
    metadata_schema = [
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("table_name", "STRING"),
        bigquery.SchemaField("execution_date", "TIMESTAMP"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("rows_processed", "INTEGER"),
        bigquery.SchemaField("error_message", "STRING"),
        bigquery.SchemaField("timestamp", "TIMESTAMP")
    ]
    
    try:
        metadata_table_ref = client.dataset(DATASET_ID).table("pipeline_metadata")
        client.get_table(metadata_table_ref)
        logger.info("✅ Metadata table already exists")
    except Exception:
        metadata_table = bigquery.Table(metadata_table_ref, schema=metadata_schema)
        client.create_table(metadata_table)
        logger.info("✅ Created metadata table")
    
    # Create data tables
    for table_name, schema_def in TABLE_SCHEMAS.items():
        table_ref = client.dataset(DATASET_ID).table(table_name)
        schema = [bigquery.SchemaField(field["name"], field["type"]) for field in schema_def]
        
        try:
            existing_table = client.get_table(table_ref)
            
            # Compare schemas
            existing_fields = {field.name: field.field_type for field in existing_table.schema}
            new_fields = {field["name"]: field["type"] for field in schema_def}
            
            # Check for new fields
            added_fields = set(new_fields.keys()) - set(existing_fields.keys())
            if added_fields:
                logger.info(f"⚠️ New fields detected in {table_name}: {added_fields}")
                # Update schema (in real implementation, you might want to handle this more carefully)
                updated_schema = existing_table.schema + [
                    bigquery.SchemaField(field, new_fields[field]) 
                    for field in added_fields
                ]
                table = bigquery.Table(table_ref, schema=updated_schema)
                client.update_table(table, ["schema"])
                logger.info(f"✅ Updated schema for {table_name}")
            
            logger.info(f"✅ Table {table_name} already exists")
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            
            # Add partitioning if specified
            partition_field = REFERENCE_FIELDS.get(table_name)
            if partition_field and any(field.name == partition_field for field in schema):
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition_field
                )
                logger.info(f"✅ Partitioning {table_name} on {partition_field}")
            
            client.create_table(table)
            logger.info(f"✅ Created table {table_name}")

def extract_and_load(table: str, execution_date: datetime):
    """Extract data from API and load to BigQuery with incremental logic."""
    run_id = str(uuid.uuid4())
    logger.info(f"🏁 Starting processing for {table} (run_id: {run_id})")
    
    try:
        # Step 1: Fetch data from API
        url = MOCK_API_URLS[table]
        logger.info(f"🌐 Fetching data from {url}")
        df = fetch_data_from_api(url)
        
        if df.empty:
            logger.info(f"⚠️ No data returned for {table}")
            log_pipeline_metadata(table, "NO_DATA", 0, None, execution_date)
            return
        
        # Step 2: Validate data
        if not validate_data(df, table):
            error_msg = f"Data validation failed for {table}"
            logger.error(f"❌ {error_msg}")
            log_pipeline_metadata(table, "FAILED", 0, error_msg, execution_date)
            raise ValueError(error_msg)
        
        # Step 3: Apply incremental logic
        date_field = REFERENCE_FIELDS.get(table)
        if date_field and date_field in df.columns:
            latest_date = get_latest_date(table)
            if latest_date:
                df[date_field] = pd.to_datetime(df[date_field])
                df = df[df[date_field] > pd.to_datetime(latest_date)]
                logger.info(f"⏱️ Filtered data after {latest_date}, remaining rows: {len(df)}")
        
        if df.empty:
            logger.info(f"⚠️ No new rows to load for {table}")
            log_pipeline_metadata(table, "NO_NEW_DATA", 0, None, execution_date)
            return
        
        # Step 4: Prepare data for BigQuery
        temp_file = f"/tmp/{table}_{run_id}.parquet"

        if table == "metrics":
        # Convert ID fields to the correct type (STRING in this case)
            df["campaign_id"] = df["campaign_id"].astype(str)
            if "ad_group_id" in df.columns:
                df["ad_group_id"] = df["ad_group_id"].astype(str)
            if "ad_id" in df.columns:
                df["ad_id"] = df["ad_id"].astype(str)
        
        # Convert date fields to proper format
        for field in df.columns:
            if "date" in field.lower() or field == REFERENCE_FIELDS.get(table):
                df[field] = pd.to_datetime(df[field]).dt.date
        
        # Write to parquet
        pq.write_table(pa.Table.from_pandas(df), temp_file)
        
        # Step 5: Load to BigQuery
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_APPEND",
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
            ]
        )
        
        logger.info(f"📤 Loading {len(df)} rows to BigQuery table {table}")
        with open(temp_file, "rb") as f:
            job = client.load_table_from_file(
                f,
                f"{PROJECT_ID}.{DATASET_ID}.{table}",
                job_config=job_config
            )
            job.result()  # Wait for job to complete
        
        # Clean up
        os.remove(temp_file)
        
        # Log success
        log_pipeline_metadata(table, "SUCCESS", len(df), None, execution_date)
        logger.info(f"✅ Successfully loaded {len(df)} rows to {table}")
        
    except Exception as e:
        error_msg = f"Error processing {table}: {str(e)}"
        logger.error(f"❌ {error_msg}")
        log_pipeline_metadata(table, "FAILED", 0, error_msg, execution_date)
        raise

def get_latest_date(table: str) -> Optional[datetime]:
    """Get the latest date from a BigQuery table."""
    date_field = REFERENCE_FIELDS.get(table)
    if not date_field:
        return None
    
    query = f"""
    SELECT MAX({date_field}) as latest_date 
    FROM `{PROJECT_ID}.{DATASET_ID}.{table}`
    """
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        row = next(results, None)
        return row[0] if row else None
    except Exception as e:
        logger.error(f"❌ Failed to get latest date for {table}: {str(e)}")
        return None

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
    "email_on_failure": False,
    "email_on_retry": False
}

# DAG definition
with DAG(
    dag_id="google_ads_ingestion_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    description="Ingest Google Ads JSON data from GitHub into BigQuery with incremental logic.",
    tags=["google_ads", "ingestion", "bigquery"]
) as dag:

    init_tables = PythonOperator(
        task_id="create_bigquery_tables",
        python_callable=create_bigquery_tables,
        execution_timeout=timedelta(minutes=10)
    )

    extract_load_tasks = []
    for table in MOCK_API_URLS.keys():
        task = PythonOperator(
            task_id=f"extract_load_{table}",
            python_callable=extract_and_load,
            op_kwargs={
                "table": table,
                "execution_date": "{{ ds }}"
            },
            execution_timeout=timedelta(minutes=30),
            retries=1
        )
        init_tables >> task
        extract_load_tasks.append(task)

    dbt_run = BashOperator(
        task_id="run_dbt_build",
        bash_command="dbt build --project-dir /home/airflow/gcs/dags/dbt_project --profiles-dir /home/airflow/.dbt",
        execution_timeout=timedelta(hours=1),
        retries=1
    )

    # Set dependencies
    for task in extract_load_tasks:
        task >> dbt_run