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
import tempfile
from airflow.utils.task_group import TaskGroup


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
        {"name": "campaign_id", "type": "STRING"},
        {"name": "campaign_name", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "optimization_score", "type": "FLOAT"},
        {"name": "advertising_channel_type", "type": "STRING"},
        {"name": "bidding_strategy_type", "type": "STRING"}
    ],
    "ad_groups": [
        {"name": "ad_group_id", "type": "INTEGER"},
        {"name": "campaign_id", "type": "INTEGER"},
        {"name": "ad_group_name", "type": "STRING"},
        {"name": "status", "type": "STRING"}
    ],
    "ads": [
        {"name": "ad_id", "type": "STRING"},
        {"name": "ad_group_id", "type": "STRING"},
        {"name": "headline", "type": "STRING"},
        {"name": "final_url", "type": "STRING"},
        {"name": "status", "type": "STRING"}
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
    {"name": "campaign_id", "type": "STRING"},
    {"name": "budget_amount", "type": "FLOAT"}
]

}

# Partitioning fields
REFERENCE_FIELDS = {
    "campaigns": ["campaign_id", "campaign_name"],
    "ad_groups": "start_date",
    "ads": "start_date",
    "metrics": "date",
    "budgets": "start_date"
}

# Required fields for each table (data validation)
REQUIRED_FIELDS = {
    "campaigns": ["campaign_id", "campaign_name", "start_date"],
    "ad_groups": ["ad_group_id", "campaign_id"],
    "ads": ["ad_id", "ad_group_id"],
    "metrics": ["date", "campaign_id"],
    "budgets": ["campaign_id"]
}

def log_pipeline_metadata(
    table_name: str,
    status: str,
    rows_processed: int = 0,
    error_message: Optional[str] = None,
    execution_date: Optional[datetime] = None
):
    """Log pipeline execution metadata to BigQuery."""
    # Handle execution_date whether it's a string or datetime object
    if isinstance(execution_date, str):
        try:
            exec_date = datetime.strptime(execution_date, "%Y-%m-%d")
        except ValueError:
            exec_date = datetime.utcnow()
    else:
        exec_date = execution_date or datetime.utcnow()
    
    metadata = {
        "run_id": str(uuid.uuid4()),
        "table_name": table_name,
        "execution_date": exec_date.isoformat() if exec_date else None,
        "status": status,
        "rows_processed": rows_processed,
        "error_message": error_message,
        "timestamp": datetime.utcnow().isoformat()
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
    """Fetch data from API endpoint with field name normalization."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Normalize field names for ad_groups
        if "ad_groups" in url:
            if isinstance(data, dict) and 'ad_groups' in data:
                data = data['ad_groups']
            
            # Normalize field names to match our schema
            for item in data:
                if 'id' in item and 'ad_group_id' not in item:
                    item['ad_group_id'] = item.pop('id')
                if 'name' in item and 'ad_group_name' not in item:
                    item['ad_group_name'] = item.pop('name')

        # Normalize field names for ads
        if "ads" in url:
            if isinstance(data, dict) and 'ads' in data:
                data = data['ads']

            for item in data:
                if 'id' in item and 'ad_id' not in item:
                    item['ad_id'] = item.pop('id')
        
        if "budgets" in url:
            if isinstance(data, dict) and 'budgets' in data:
                data = data['budgets']

            for item in data:
                # Convert micros to float dollars
                item["budget_amount"] = round(item.get("amount_micros", 0) / 1_000_000, 2)

        if "campaigns" in url:
            if isinstance(data, dict) and 'campaigns' in data:
                data = data['campaigns']
            for item in data:
                if 'id' in item and 'campaign_id' not in item:
                    item['campaign_id'] = item.pop('id')
                if 'name' in item and 'campaign_name' not in item:
                    item['campaign_name'] = item.pop('name')
                    
        return pd.DataFrame(data)
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Failed to fetch data from {url}: {str(e)}")
        raise
    except ValueError as e:
        logger.error(f"❌ Invalid JSON data from {url}: {str(e)}")
        raise

def validate_data(df: pd.DataFrame, table_name: str) -> bool:
    """Validate data with field name normalization."""
    required_fields = REQUIRED_FIELDS.get(table_name, [])
    
    # Normalize field names first
    if table_name == "ad_groups":
        if 'id' in df.columns and 'ad_group_id' not in df.columns:
            df = df.rename(columns={'id': 'ad_group_id'})
        if 'name' in df.columns and 'ad_group_name' not in df.columns:
            df = df.rename(columns={'name': 'ad_group_name'})
    
    # Check required fields exist
    missing_fields = [field for field in required_fields if field not in df.columns]
    if missing_fields:
        logger.error(f"❌ Missing required fields in {table_name}: {missing_fields}")
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

def migrate_metrics_table_schema():
    """Migrate the metrics table schema to accept string campaign_id"""
    table_ref = client.dataset(DATASET_ID).table("metrics")
    
    try:
        table = client.get_table(table_ref)
        
        # Create new schema with string campaign_id
        new_schema = []
        for field in table.schema:
            if field.name == "campaign_id":
                new_schema.append(bigquery.SchemaField("campaign_id", "STRING"))
            else:
                new_schema.append(field)
        
        # Create a new temporary table
        temp_table_ref = client.dataset(DATASET_ID).table("metrics_temp")
        temp_table = bigquery.Table(temp_table_ref, schema=new_schema)
        client.create_table(temp_table)
        
        # Copy data with type conversion
        job_config = bigquery.QueryJobConfig(
            destination=temp_table_ref,
            write_disposition="WRITE_TRUNCATE"
        )
        query = f"""
        SELECT 
            CAST(campaign_id AS STRING) AS campaign_id,
            CAST(ad_group_id AS STRING) AS ad_group_id,
            CAST(ad_id AS STRING) AS ad_id,
            date,
            impressions,
            clicks,
            cost,
            conversions
        FROM `{PROJECT_ID}.{DATASET_ID}.metrics`
        """
        client.query(query, job_config=job_config).result()
        
        # Replace the original table
        client.delete_table(table_ref)
        client.create_table(bigquery.Table(table_ref, schema=new_schema))
        client.copy_table(temp_table_ref, table_ref)
        client.delete_table(temp_table_ref)
        
        logger.info("✅ Successfully migrated metrics table schema")
    except Exception as e:
        logger.error(f"❌ Failed to migrate schema: {str(e)}")
        raise

def migrate_ad_groups_schema():
    """Migrate the ad_groups table schema to STRING ad_group_id and campaign_id."""
    table_ref = client.dataset(DATASET_ID).table("ad_groups")

    try:
        table = client.get_table(table_ref)
        new_schema = []
        for field in table.schema:
            if field.name == "ad_group_id":
                new_schema.append(bigquery.SchemaField("ad_group_id", "STRING"))
            elif field.name == "campaign_id":
                new_schema.append(bigquery.SchemaField("campaign_id", "STRING"))
            else:
                new_schema.append(field)

        temp_table_ref = client.dataset(DATASET_ID).table("ad_groups_temp")
        temp_table = bigquery.Table(temp_table_ref, schema=new_schema)
        client.create_table(temp_table)

        job_config = bigquery.QueryJobConfig(
            destination=temp_table_ref,
            write_disposition="WRITE_TRUNCATE"
        )
        query = f"""
        SELECT 
            CAST(ad_group_id AS STRING) AS ad_group_id,
            CAST(campaign_id AS STRING) AS campaign_id,
            ad_group_name,
            status
        FROM `{PROJECT_ID}.{DATASET_ID}.ad_groups`
        """
        client.query(query, job_config=job_config).result()

        client.delete_table(table_ref)
        client.create_table(bigquery.Table(table_ref, schema=new_schema))
        client.copy_table(temp_table_ref, table_ref)
        client.delete_table(temp_table_ref)

        logger.info("✅ Successfully migrated ad_groups table schema")

    except Exception as e:
        logger.error(f"❌ Failed to migrate ad_groups schema: {str(e)}")
        raise

def migrate_ads_schema():
    """Migrate the ads table schema to STRING ad_id and ad_group_id."""
    table_ref = client.dataset(DATASET_ID).table("ads")

    try:
        table = client.get_table(table_ref)
        new_schema = []
        for field in table.schema:
            if field.name == "ad_id":
                new_schema.append(bigquery.SchemaField("ad_id", "STRING"))
            elif field.name == "ad_group_id":
                new_schema.append(bigquery.SchemaField("ad_group_id", "STRING"))
            else:
                new_schema.append(field)

        temp_table_ref = client.dataset(DATASET_ID).table("ads_temp")
        temp_table = bigquery.Table(temp_table_ref, schema=new_schema)
        client.create_table(temp_table)

        job_config = bigquery.QueryJobConfig(
            destination=temp_table_ref,
            write_disposition="WRITE_TRUNCATE"
        )

        query = f"""
        SELECT 
            CAST(ad_id AS STRING) AS ad_id,
            CAST(ad_group_id AS STRING) AS ad_group_id,
            headline,
            description,
            final_url,
            start_date,
            end_date
        FROM `{PROJECT_ID}.{DATASET_ID}.ads`
        """

        client.query(query, job_config=job_config).result()

        client.delete_table(table_ref)
        client.create_table(bigquery.Table(table_ref, schema=new_schema))
        client.copy_table(temp_table_ref, table_ref)
        client.delete_table(temp_table_ref)

        logger.info("✅ Successfully migrated ads table schema")

    except Exception as e:
        logger.error(f"❌ Failed to migrate ads schema: {str(e)}")
        raise

def migrate_budgets_schema():
    """Migrate budgets table schema to accept STRING campaign_id and budget_amount."""
    table_ref = client.dataset(DATASET_ID).table("budgets")
    try:
        new_schema = [
            bigquery.SchemaField("campaign_id", "STRING"),
            bigquery.SchemaField("budget_amount", "FLOAT")
        ]

        temp_table_ref = client.dataset(DATASET_ID).table("budgets_temp")
        client.create_table(bigquery.Table(temp_table_ref, schema=new_schema))

        query = f"""
        SELECT 
            CAST(campaign_id AS STRING) AS campaign_id,
            budget_amount
        FROM `{PROJECT_ID}.{DATASET_ID}.budgets`
        """
        job_config = bigquery.QueryJobConfig(destination=temp_table_ref, write_disposition="WRITE_TRUNCATE")
        client.query(query, job_config=job_config).result()

        client.delete_table(table_ref)
        client.create_table(bigquery.Table(table_ref, schema=new_schema))
        client.copy_table(temp_table_ref, table_ref)
        client.delete_table(temp_table_ref)

        logger.info("✅ Successfully migrated budgets table schema")

    except Exception as e:
        logger.error(f"❌ Failed to migrate budgets schema: {str(e)}")
        raise
def migrate_campaign_table_schema():
    """Migrate the campaigns table schema to match updated definition."""
    table_ref = client.dataset(DATASET_ID).table("campaigns")

    try:
        # Build new schema based on updated TABLE_SCHEMAS
        new_schema = [bigquery.SchemaField(field["name"], field["type"]) for field in TABLE_SCHEMAS["campaigns"]]

        # Create a temporary table
        temp_table_ref = client.dataset(DATASET_ID).table("campaigns_temp")

        # 💡 Drop the temp table if it already exists
        try:
            client.delete_table(temp_table_ref)
            logger.info("🧹 Deleted existing campaigns_temp table")
        except Exception:
            logger.info("ℹ️ campaigns_temp table did not exist")

        temp_table = bigquery.Table(temp_table_ref, schema=new_schema)
        client.create_table(temp_table)

        # Copy data with necessary field casting
        job_config = bigquery.QueryJobConfig(
            destination=temp_table_ref,
            write_disposition="WRITE_TRUNCATE"
        )

        query = f"""
        SELECT
            campaign_id,
            campaign_name,
            status,
            CAST(optimization_score AS FLOAT64) AS optimization_score,
            advertising_channel_type,
            bidding_strategy_type
        FROM `{PROJECT_ID}.{DATASET_ID}.campaigns`
        """

        client.query(query, job_config=job_config).result()

        # Replace original table
        client.delete_table(table_ref)
        client.create_table(bigquery.Table(table_ref, schema=new_schema))
        client.copy_table(temp_table_ref, table_ref)
        client.delete_table(temp_table_ref)

        logger.info("✅ Successfully migrated campaigns table schema")

    except Exception as e:
        logger.error(f"❌ Failed to migrate campaigns schema: {str(e)}")
        raise


def extract_and_load(table: str, execution_date: datetime):
    run_id = str(uuid.uuid4())
    logger.info(f"🏁 Starting processing for {table} (run_id: {run_id})")
    temp_file = f"/tmp/{table}_{run_id}.parquet"
    
    try:
        # Fetch and validate data
        url = MOCK_API_URLS[table]
        logger.info(f"🌐 Fetching data from {url}")
        df = fetch_data_from_api(url)
        
        if df.empty:
            logger.info(f"⚠️ No data returned for {table}")
            log_pipeline_metadata(table, "NO_DATA", 0, None, execution_date)
            return
        
        # Special handling for ad_groups
        if table == "ad_groups":
            if 'id' in df.columns:
                df = df.rename(columns={'id': 'ad_group_id'})
            if 'name' in df.columns:
                df = df.rename(columns={'name': 'ad_group_name'})
            if 'status' not in df.columns:
                df['status'] = 'ENABLED'
    # Do NOT cast to int




        
        if not validate_data(df, table):
            error_msg = f"Data validation failed for {table}"
            logger.error(f"❌ {error_msg}")
            log_pipeline_metadata(table, "FAILED", 0, error_msg, execution_date)
            raise ValueError(error_msg)

        # Step 3: Type conversion for metrics table
        if table == "metrics":
            df["campaign_id"] = df["campaign_id"].astype(str)
            for id_field in ["ad_group_id", "ad_id"]:
                if id_field in df.columns:
                    df[id_field] = df[id_field].astype(str)
        
        # Step 4: Apply incremental logic
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
        
        # Step 5: Prepare data for BigQuery
        # Convert date fields to proper format
        for field in df.columns:
            if "date" in field.lower() or field == REFERENCE_FIELDS.get(table):
                df[field] = pd.to_datetime(df[field]).dt.date
        
        # Write to parquet
        pq.write_table(pa.Table.from_pandas(df), temp_file)
        
        # Step 6: Load to BigQuery
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
        if os.path.exists(temp_file):
            os.remove(temp_file)
        
        # Log success
        log_pipeline_metadata(table, "SUCCESS", len(df), None, execution_date)
        logger.info(f"✅ Successfully loaded {len(df)} rows to {table}")
        
    except Exception as e:
        # Clean up temp file if it exists
        if 'temp_file' in locals() and os.path.exists(temp_file):
            os.remove(temp_file)
            
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

    # ✅ Grouped BigQuery schema migration tasks
    with TaskGroup("schema_migrations", tooltip="BigQuery schema migrations") as schema_migrations:
        migrate_metrics_schema = PythonOperator(
            task_id="migrate_metrics_schema",
            python_callable=migrate_metrics_table_schema,
            execution_timeout=timedelta(minutes=15)
        )

        migrate_ad_groups_schema_task = PythonOperator(
            task_id="migrate_ad_groups_schema",
            python_callable=migrate_ad_groups_schema,
            execution_timeout=timedelta(minutes=10)
        )

        migrate_ads_schema_task = PythonOperator(
            task_id="migrate_ads_schema",
            python_callable=migrate_ads_schema,
            execution_timeout=timedelta(minutes=10)
        )

        migrate_budgets_schema_task = PythonOperator(
            task_id="migrate_budgets_schema",
            python_callable=migrate_budgets_schema,
            execution_timeout=timedelta(minutes=10)
        )

        migrate_campaigns_schema_task = PythonOperator(
        task_id="migrate_campaigns_schema",
        python_callable=migrate_campaign_table_schema,  # ✅ Correct function name
        execution_timeout=timedelta(minutes=10)
     )


    # ✅ Extract/load tasks
    extract_load_campaigns = PythonOperator(
        task_id="extract_load_campaigns",
        python_callable=extract_and_load,
        op_kwargs={"table": "campaigns", "execution_date": "{{ execution_date }}"},
        execution_timeout=timedelta(minutes=30),
        retries=1
    )

    extract_load_ad_groups = PythonOperator(
        task_id="extract_load_ad_groups",
        python_callable=extract_and_load,
        op_kwargs={"table": "ad_groups", "execution_date": "{{ execution_date }}"},
        execution_timeout=timedelta(minutes=30),
        retries=1
    )

    extract_load_ads = PythonOperator(
        task_id="extract_load_ads",
        python_callable=extract_and_load,
        op_kwargs={"table": "ads", "execution_date": "{{ execution_date }}"},
        execution_timeout=timedelta(minutes=30),
        retries=1
    )

    extract_load_metrics = PythonOperator(
        task_id="extract_load_metrics",
        python_callable=extract_and_load,
        op_kwargs={"table": "metrics", "execution_date": "{{ execution_date }}"},
        execution_timeout=timedelta(minutes=30),
        retries=1
    )

    extract_load_budgets = PythonOperator(
        task_id="extract_load_budgets",
        python_callable=extract_and_load,
        op_kwargs={"table": "budgets", "execution_date": "{{ execution_date }}"},
        execution_timeout=timedelta(minutes=30),
        retries=1
    )

    # ✅ dbt run command
    dbt_run = BashOperator(
        task_id="run_dbt_build",
        bash_command="dbt build --project-dir /home/airflow/gcs/dags/dbt_project --profiles-dir /home/airflow/.dbt",
        execution_timeout=timedelta(hours=1),
        retries=1
    )

    # ✅ DAG dependencies
    init_tables >> schema_migrations

    schema_migrations >> [
        extract_load_campaigns,
        extract_load_ad_groups,
        extract_load_ads,
        extract_load_metrics,
        extract_load_budgets
    ]

    [
        extract_load_campaigns,
        extract_load_ad_groups,
        extract_load_ads,
        extract_load_metrics,
        extract_load_budgets
    ] >> dbt_run
