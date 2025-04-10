from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.email import send_email
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
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
from typing import Union, Optional
import tempfile
from airflow.utils.task_group import TaskGroup
from datetime import datetime


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

# Local mock JSON file paths (replace GitHub URLs)
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
        {"name": "campaign_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "campaign_name", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "advertising_channel_type", "type": "STRING"},
        {"name": "bidding_strategy_type", "type": "STRING"},
        {"name": "budget_id", "type": "STRING"},
        {"name": "date", "type": "DATE", "mode": "REQUIRED"}
    ],
    "ad_groups": [
        {"name": "ad_group_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "campaign_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "ad_group_name", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "date", "type": "DATE", "mode": "REQUIRED"}
    ],
    "ads": [
        {"name": "ad_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "ad_group_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "headline", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "date", "type": "DATE", "mode": "REQUIRED"}
    ],
    "metrics": [
        {"name": "ad_group_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "date", "type": "DATE", "mode": "REQUIRED"},
        {"name": "clicks", "type": "INTEGER"},
        {"name": "impressions", "type": "INTEGER"},
        {"name": "ctr", "type": "FLOAT"},
        {"name": "average_cpc", "type": "FLOAT"},
        {"name": "cost_micros", "type": "INTEGER"},
        {"name": "conversions", "type": "FLOAT"}
    ],
    "budgets": [
        {"name": "budget_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "budget_name", "type": "STRING"},
        {"name": "budget_amount", "type": "FLOAT"},
        {"name": "date", "type": "DATE", "mode": "REQUIRED"}
    ]
}

REFERENCE_FIELDS = {
    "campaigns": "date",
    "ad_groups": "date",
    "ads": "date",
    "metrics": "date",
    "budgets": "date"
}

# Partitioning fields
PARTITION_FIELDS = {
    "campaigns": "date",
    "ad_groups": "date",
    "ads": "date",
    "metrics": "date",
    "budgets": "date"
}

# Clustering fields
CLUSTERING_FIELDS = {
    "campaigns": ["campaign_id", "status"],
    "ad_groups": ["campaign_id", "ad_group_id", "status"],
    "ads": ["ad_group_id", "status"],
    "metrics": ["ad_group_id"],
    "budgets": ["budget_id"]
}

# Required fields for validation
REQUIRED_FIELDS = {
    "campaigns": ["campaign_id", "campaign_name", "date"],
    "ad_groups": ["ad_group_id", "campaign_id", "date"],
    "ads": ["ad_id", "ad_group_id", "date"],
    "metrics": ["ad_group_id", "date"],
    "budgets": ["budget_id", "date"] 
}




def create_metadata_table():
    """Create the metadata table if it doesn't exist."""
    schema = [
        bigquery.SchemaField("run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("execution_date", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("rows_processed", "INTEGER"),
        bigquery.SchemaField("error_message", "STRING"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED")
    ]
    
    table_ref = client.dataset(DATASET_ID).table("pipeline_metadata")
    table = bigquery.Table(table_ref, schema=schema)
    
    try:
        client.get_table(table_ref)
        logger.info("Metadata table already exists")
    except Exception:
        client.create_table(table)
        logger.info("Created metadata table")

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

        # Extract list if nested under a key
        for key in ["campaigns", "ad_groups", "ads", "metrics", "budgets"]:
            if key in url and isinstance(data, dict) and key in data:
                data = data[key]

        for item in data:
            if "campaigns" in url:
                if 'id' in item and 'campaign_id' not in item:
                    item['campaign_id'] = item.pop('id')
                if 'name' in item and 'campaign_name' not in item:
                    item['campaign_name'] = item.pop('name')
                if 'campaign_id' not in item:
                    item['campaign_id'] = f"default_{uuid.uuid4().hex[:8]}"
                if 'optimization_score' in item:
                    item['optimization_score'] = float(item['optimization_score'])

            elif "ad_groups" in url:
                if 'id' in item and 'ad_group_id' not in item:
                    item['ad_group_id'] = item.pop('id')
                if 'name' in item and 'ad_group_name' not in item:
                    item['ad_group_name'] = item.pop('name')

            elif "ads" in url:
                if 'id' in item and 'ad_id' not in item:
                    item['ad_id'] = item.pop('id')

            if "budgets" in url:
                logger.info("🧪 Normalizing budgets...")

                for item in data:
                    item["budget_amount"] = round(item.get("amount_micros", 0) / 1_000_000, 2)

                    if 'id' in item and 'budget_id' not in item:
                        item['budget_id'] = item.pop('id')

                    if 'name' in item and 'budget_name' not in item:
                        item['budget_name'] = item.pop('name')

                    if 'campaign_id' not in item:
                        item['campaign_id'] = "default_campaign"

                logger.info(f"🧪 Sample normalized budget row: {json.dumps(data[0], indent=2)}")


            elif "metrics" in url:
                if 'ad_group_id' not in item:
                    item['ad_group_id'] = 'unknown'
                if 'date' not in item:
                    item['date'] = datetime.now().strftime('%Y-%m-%d')

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

    # Normalize field names
    if table_name == "ad_groups":
        if 'id' in df.columns and 'ad_group_id' not in df.columns:
            df = df.rename(columns={'id': 'ad_group_id'})
        if 'name' in df.columns and 'ad_group_name' not in df.columns:
            df = df.rename(columns={'name': 'ad_group_name'})

    if table_name == "budgets":
        pass

    if table_name == "ads":
        if 'id' in df.columns and 'ad_id' not in df.columns:
            df = df.rename(columns={'id': 'ad_id'})

    if table_name == "campaigns":
        if 'id' in df.columns and 'campaign_id' not in df.columns:
            df = df.rename(columns={'id': 'campaign_id'})
        if 'name' in df.columns and 'campaign_name' not in df.columns:
            df = df.rename(columns={'name': 'campaign_name'})

    if table_name == "metrics":
        for field in ["campaign_id", "ad_group_id", "ad_id"]:
            if field in df.columns:
                df[field] = df[field].astype(str)

    # Check required fields exist
    missing_fields = [field for field in required_fields if field not in df.columns]
    if missing_fields:
        logger.error(f"❌ Missing required fields in {table_name}: {missing_fields}")
        return False

    return True

def create_bigquery_tables():
    create_metadata_table()

    for table_name, schema_fields in TABLE_SCHEMAS.items():
        table_ref = client.dataset(DATASET_ID).table(table_name)
        full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

        schema = [
            bigquery.SchemaField(
                name=field["name"],
                field_type=field["type"],
                mode=field.get("mode", "NULLABLE")
            ) for field in schema_fields
        ]
        partition_field = PARTITION_FIELDS.get(table_name)
        clustering_fields = CLUSTERING_FIELDS.get(table_name, [])

        recreate = False

        try:
            table = client.get_table(table_ref)

            current_partition = table.time_partitioning.field if table.time_partitioning else None
            current_clustering = set(table.clustering_fields or [])

            if current_partition != partition_field or current_clustering != set(clustering_fields):
                logger.warning(f"⚠️ Table {full_table_id} exists but has incorrect partitioning or clustering. Recreating.")
                client.delete_table(table_ref)
                recreate = True
            else:
                logger.info(f"✅ Table {full_table_id} exists with correct schema.")
        except Exception:
            recreate = True

        if recreate:
            try:
                table = bigquery.Table(table_ref, schema=schema)

                if partition_field:
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field=partition_field
                    )

                if clustering_fields:
                    table.clustering_fields = clustering_fields

                client.create_table(table)
                logger.info(f"✅ Created table {full_table_id} with partitioning and clustering.")
            except Exception as e:
                logger.error(f"❌ Failed to create table {full_table_id}: {str(e)}")



def validate_data(df: pd.DataFrame, table_name: str) -> bool:
    """Validate the incoming data for missing fields and invalid values."""
    required_fields = REQUIRED_FIELDS.get(table_name, [])

    # Check for missing required fields
    missing_fields = [field for field in required_fields if field not in df.columns]
    if missing_fields:
        logger.error(f"❌ Missing required fields in {table_name}: {missing_fields}")
        return False

    # Ensure no negative values for certain columns (e.g., clicks, cost)
    if 'clicks' in df.columns and (df['clicks'] < 0).any():
        logger.error("❌ Found negative values in 'clicks'.")
        return False

    # Add any additional validation checks for other fields here, if needed

    return True



def deduplicate_data(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Remove duplicate records based on key fields."""
    logger.info(f"Initial row count before deduplication: {len(df)}")
    
    # Define unique keys for deduplication based on the table
    if table_name == "campaigns":
        df = df.drop_duplicates(subset=['campaign_id', 'date'])
    elif table_name == "ad_groups":
        df = df.drop_duplicates(subset=['ad_group_id', 'date'])
    elif table_name == "ads":
        df = df.drop_duplicates(subset=['ad_id', 'date'])
    elif table_name == "metrics":
        df = df.drop_duplicates(subset=['ad_group_id', 'date'])
    elif table_name == "budgets":
        df = df.drop_duplicates(subset=['budget_id', 'date'])
    
    logger.info(f"Row count after deduplication: {len(df)}")
    return df



def filter_incremental_data(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Filter the data to only include records after the latest date in BigQuery."""
    latest_date = get_latest_date(table_name)
    logger.info(f"Latest date in BigQuery for {table_name}: {latest_date}")

    # Filter the data to only include records after the latest date
    df['date'] = pd.to_datetime(df['date'])
    df = df[df['date'] > pd.to_datetime(latest_date)]
    
    logger.info(f"Row count after filtering for new data: {len(df)}")
    return df


def load_data_to_bigquery(df: pd.DataFrame, table_name: str) -> None:
    """Load data into BigQuery with partitioning and clustering."""
    table_ref = client.dataset(DATASET_ID).table(table_name)
    
    try:
        # Get the table to check its partitioning/clustering
        table = client.get_table(table_ref)
        partition_field = table.time_partitioning.field if table.time_partitioning else None
        clustering_fields = table.clustering_fields if table.clustering_fields else []
    except Exception as e:
        logger.error(f"❌ Failed to get table info for {table_name}: {str(e)}")
        raise

    # Job config with partitioning
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_APPEND",
        schema_update_options=[  # Allow schema updates if needed
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
        ]
    )
    
    if partition_field:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        )
    
    # Convert DataFrame to Parquet
    temp_file = f"/tmp/{table_name}.parquet"
    pq.write_table(pa.Table.from_pandas(df), temp_file)

    # Load data
    with open(temp_file, "rb") as f:
        job = client.load_table_from_file(
            f,
            table_ref,
            job_config=job_config
        )
        job.result()

    os.remove(temp_file)
    logger.info(f"✅ Successfully loaded data to {table_name} with partitioning and clustering")


def extract_and_load(table: str, execution_date: datetime):
    run_id = str(uuid.uuid4())
    logger.info(f"🏁 Starting processing for {table} (run_id: {run_id})")
    
    try:
        # Step 1: Fetch and validate data
        url = MOCK_API_URLS.get(table)
        df = fetch_data_from_api(url)
        
        if not df.empty:
            # Step 2: Data Validation
            if not validate_data(df, table):
                log_pipeline_metadata(table, "FAILED", 0, "Validation failed", execution_date)
                return

            # Step 3: Deduplication
            df = deduplicate_data(df, table)

            # Step 4: Incremental Filtering
            df = filter_incremental_data(df, table)
            
            if df.empty:
                log_pipeline_metadata(table, "NO_NEW_DATA", 0, None, execution_date)
                return

            # Step 5: Load data to BigQuery (Partitioning and Clustering Applied)
            load_data_to_bigquery(df, table)

            # Step 6: Log metadata after loading data
            log_pipeline_metadata(table, "SUCCESS", len(df), None, execution_date)

    except Exception as e:
        log_pipeline_metadata(table, "FAILED", 0, str(e), execution_date)
        logger.error(f"❌ Error processing {table}: {str(e)}")


def log_pipeline_metadata(
    table_name: str,
    status: str,
    rows_processed: int = 0,
    error_message: Optional[str] = None,
    execution_date: Optional[Union[datetime, str]] = None
):
    """
    Log pipeline execution metadata to BigQuery table 'pipeline_metadata'.
    Automatically handles datetime parsing and logs relevant status info.
    """
    try:
        # Convert execution_date to datetime if it’s a string (Jinja or ISO format)
        if isinstance(execution_date, str):
            try:
                execution_date = datetime.fromisoformat(execution_date.replace("Z", "+00:00"))
            except Exception:
                logger.warning("⚠️ Could not parse execution_date string. Defaulting to current UTC time.")
                execution_date = datetime.utcnow()
        elif not isinstance(execution_date, datetime):
            execution_date = datetime.utcnow()

        # Construct metadata row
        metadata = {
            "run_id": str(uuid.uuid4()),
            "table_name": table_name,
            "execution_date": execution_date.isoformat(),
            "status": status,
            "rows_processed": rows_processed,
            "error_message": error_message or "",
            "timestamp": datetime.utcnow().isoformat()
        }

        # Insert metadata into BigQuery
        errors = client.insert_rows_json(METADATA_TABLE, [metadata])

        if errors:
            logger.error(f"❌ Failed to log metadata for {table_name}: {errors}")
        else:
            logger.info(f"✅ Logged metadata for {table_name}: status={status}, rows={rows_processed}")

    except Exception as e:
        logger.error(f"❌ Error while logging metadata for {table_name}: {str(e)}")

def create_partitioned_clustered_table(table_name: str, schema: List[bigquery.SchemaField]) -> bigquery.Table:
    table_ref = client.dataset(DATASET_ID).table(table_name)
    table = bigquery.Table(table_ref, schema=schema)

    partition_field = PARTITION_FIELDS.get(table_name)
    clustering_fields = CLUSTERING_FIELDS.get(table_name, [])

    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        )

    if clustering_fields:
        table.clustering_fields = clustering_fields

    return client.create_table(table)


def migrate_metrics_schema():
    table_name = "metrics"
    table_ref = client.dataset(DATASET_ID).table(table_name)
    temp_table_ref = client.dataset(DATASET_ID).table(f"{table_name}_temp")

    new_schema = [
        bigquery.SchemaField("ad_group_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("impressions", "INTEGER"),
        bigquery.SchemaField("clicks", "INTEGER"),
        bigquery.SchemaField("ctr", "FLOAT"),
        bigquery.SchemaField("average_cpc", "FLOAT"),
        bigquery.SchemaField("cost_micros", "INTEGER"),
        bigquery.SchemaField("conversions", "FLOAT"),
        bigquery.SchemaField("campaign_id", "STRING"),
        bigquery.SchemaField("ad_id", "STRING")
    ]

    try:
        client.delete_table(temp_table_ref, not_found_ok=True)
        temp_table = bigquery.Table(temp_table_ref, schema=new_schema)
        client.create_table(temp_table)

        query = f"""
        SELECT 
            CAST(ad_group_id AS STRING) AS ad_group_id,
            DATE(date) AS date,
            impressions,
            clicks,
            ctr,
            average_cpc,
            cost_micros,
            conversions,
            NULL AS campaign_id,
            NULL AS ad_id
        FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`
        """

        job_config = bigquery.QueryJobConfig(destination=temp_table_ref, write_disposition="WRITE_TRUNCATE")
        client.query(query, job_config=job_config).result()

        client.delete_table(table_ref, not_found_ok=True)
        create_partitioned_clustered_table(table_name, new_schema)
        client.copy_table(temp_table_ref, table_ref)
        client.delete_table(temp_table_ref)
        logger.info(f"✅ Successfully migrated {table_name} schema with partitioning and clustering")

    except Exception as e:
        logger.error(f"❌ Failed to migrate {table_name} schema: {str(e)}")
        raise


def migrate_ad_groups_schema():
    table_name = "ad_groups"
    table_ref = client.dataset(DATASET_ID).table(table_name)
    temp_table_ref = client.dataset(DATASET_ID).table(f"{table_name}_temp")

    new_schema = [
        bigquery.SchemaField("ad_group_id", "STRING"),
        bigquery.SchemaField("campaign_id", "STRING"),
        bigquery.SchemaField("ad_group_name", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("date", "DATE")
    ]

    try:
        client.delete_table(temp_table_ref, not_found_ok=True)
        temp_table = bigquery.Table(temp_table_ref, schema=new_schema)
        client.create_table(temp_table)

        query = f"""
        SELECT 
            CAST(ad_group_id AS STRING) AS ad_group_id,
            CAST(campaign_id AS STRING) AS campaign_id,
            ad_group_name,
            status,
            date
        FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`
        """

        job_config = bigquery.QueryJobConfig(destination=temp_table_ref, write_disposition="WRITE_TRUNCATE")
        client.query(query, job_config=job_config).result()

        client.delete_table(table_ref, not_found_ok=True)
        create_partitioned_clustered_table(table_name, new_schema)
        client.copy_table(temp_table_ref, table_ref)
        client.delete_table(temp_table_ref)
        logger.info(f"✅ Successfully migrated {table_name} schema with partitioning and clustering")

    except Exception as e:
        logger.error(f"❌ Failed to migrate {table_name} schema: {str(e)}")
        raise


def migrate_ads_schema():
    table_name = "ads"
    table_ref = client.dataset(DATASET_ID).table(table_name)
    temp_table_ref = client.dataset(DATASET_ID).table(f"{table_name}_temp")

    new_schema = [
        bigquery.SchemaField("ad_id", "STRING"),
        bigquery.SchemaField("ad_group_id", "STRING"),
        bigquery.SchemaField("headline", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("date", "DATE")
    ]

    try:
        client.delete_table(temp_table_ref, not_found_ok=True)
        temp_table = bigquery.Table(temp_table_ref, schema=new_schema)
        client.create_table(temp_table)

        query = f"""
        SELECT 
            CAST(ad_id AS STRING) AS ad_id,
            CAST(ad_group_id AS STRING) AS ad_group_id,
            headline,
            status,
            date
        FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`
        """

        job_config = bigquery.QueryJobConfig(destination=temp_table_ref, write_disposition="WRITE_TRUNCATE")
        client.query(query, job_config=job_config).result()

        client.delete_table(table_ref, not_found_ok=True)
        create_partitioned_clustered_table(table_name, new_schema)
        client.copy_table(temp_table_ref, table_ref)
        client.delete_table(temp_table_ref)
        logger.info(f"✅ Successfully migrated {table_name} schema with partitioning and clustering")

    except Exception as e:
        logger.error(f"❌ Failed to migrate {table_name} schema: {str(e)}")
        raise


def migrate_budgets_schema():
    table_name = "budgets"
    table_ref = client.dataset(DATASET_ID).table(table_name)
    temp_table_ref = client.dataset(DATASET_ID).table(f"{table_name}_temp")

    new_schema = [
        bigquery.SchemaField("budget_id", "STRING"),
        bigquery.SchemaField("budget_amount", "FLOAT"),
        bigquery.SchemaField("date", "DATE")
    ]

    try:
        client.delete_table(temp_table_ref, not_found_ok=True)
        temp_table = bigquery.Table(temp_table_ref, schema=new_schema)
        client.create_table(temp_table)

        query = f"""
        SELECT 
            budget_id,
            budget_amount,
            date
        FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`
        """

        job_config = bigquery.QueryJobConfig(destination=temp_table_ref, write_disposition="WRITE_TRUNCATE")
        client.query(query, job_config=job_config).result()

        client.delete_table(table_ref, not_found_ok=True)
        create_partitioned_clustered_table(table_name, new_schema)
        client.copy_table(temp_table_ref, table_ref)
        client.delete_table(temp_table_ref)
        logger.info(f"✅ Successfully migrated {table_name} schema with partitioning and clustering")

    except Exception as e:
        logger.error(f"❌ Failed to migrate {table_name} schema: {str(e)}")
        raise


def migrate_campaigns_schema():
    table_name = "campaigns"
    table_ref = client.dataset(DATASET_ID).table(table_name)
    temp_table_ref = client.dataset(DATASET_ID).table(f"{table_name}_temp")

    new_schema = [
        bigquery.SchemaField("campaign_id", "STRING"),
        bigquery.SchemaField("campaign_name", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("optimization_score", "FLOAT"),
        bigquery.SchemaField("advertising_channel_type", "STRING"),
        bigquery.SchemaField("bidding_strategy_type", "STRING"),
        bigquery.SchemaField("date", "DATE")
    ]

    try:
        client.delete_table(temp_table_ref, not_found_ok=True)
        temp_table = bigquery.Table(temp_table_ref, schema=new_schema)
        client.create_table(temp_table)

        query = f"""
        SELECT
            CAST(campaign_id AS STRING) AS campaign_id,
            campaign_name,
            status,
            advertising_channel_type,
            bidding_strategy_type,
            NULL AS optimization_score,
            date
        FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`
        """

        job_config = bigquery.QueryJobConfig(destination=temp_table_ref, write_disposition="WRITE_TRUNCATE")
        client.query(query, job_config=job_config).result()

        client.delete_table(table_ref, not_found_ok=True)
        create_partitioned_clustered_table(table_name, new_schema)
        client.copy_table(temp_table_ref, table_ref)
        client.delete_table(temp_table_ref)
        logger.info(f"✅ Successfully migrated {table_name} schema with partitioning and clustering")

    except Exception as e:
        logger.error(f"❌ Failed to migrate {table_name} schema: {str(e)}")
        raise



def extract_and_load(table: str, execution_date: datetime):
    run_id = str(uuid.uuid4())
    logger.info(f"🏁 Starting processing for {table} (run_id: {run_id})")
    temp_file = f"/tmp/{table}_{run_id}.parquet"
    
    try:
        # Fetch and validate data
        url = MOCK_API_URLS.get(table)  # Fetch the URL for the table
        if not url:
            raise ValueError(f"❌ No URL found for table: {table}")
        
        logger.info(f"🌐 Fetching data from {url}")
        df = fetch_data_from_api(url)  # Use the fetch_data_from_api function
        
        if df.empty:
            logger.info(f"⚠️ No data returned for {table}")
            log_pipeline_metadata(table, "NO_DATA", 0, None, execution_date)
            return

        # Step 1: Ensure all required fields are present in the data
        if not validate_data(df, table):
            msg = f"Data validation failed for {table}. Missing required fields."
            logger.error(f"❌ {msg}")
            log_pipeline_metadata(table, "FAILED", 0, msg, execution_date)
            raise ValueError(msg)

        # Step 2: Apply data type conversions as needed
        if table == "metrics":
            # Ensure required fields exist
            if 'ad_group_id' not in df.columns:
                raise ValueError("Missing required field: ad_group_id")
            if 'date' not in df.columns:
                raise ValueError("Missing required field: date")
            
            # Convert data types
            df['ad_group_id'] = df['ad_group_id'].astype(str)
            df['date'] = pd.to_datetime(df['date']).dt.date
            
            # Convert micro values
            if 'average_cpc' in df.columns:
                df['average_cpc'] = df['average_cpc'] / 1000000  # Convert to standard dollars
            
            # Ensure numeric fields are properly typed
            numeric_fields = {
                'clicks': 'int64',
                'impressions': 'int64', 
                'ctr': 'float64',
                'cost_micros': 'int64',
                'conversions': 'float64'
            }
            
            for field, dtype in numeric_fields.items():
                if field in df.columns:
                    df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0).astype(dtype)

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

        # Step 4: Convert date fields to proper format
        for field in df.columns:
            if "date" in field.lower() or field == REFERENCE_FIELDS.get(table):
                df[field] = pd.to_datetime(df[field]).dt.date

        # Step 5: Write to Parquet file
        pq.write_table(pa.Table.from_pandas(df), temp_file)

        # Step 6: Load to BigQuery
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_APPEND",
            schema_update_options=[  # Allow schema updates if needed
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

        os.remove(temp_file)
        log_pipeline_metadata(table, "SUCCESS", len(df), None, execution_date)
        logger.info(f"✅ Successfully loaded {len(df)} rows to {table}")

    except Exception as e:
        # Cleanup any temporary files in case of failure
        if os.path.exists(temp_file):
            os.remove(temp_file)

        # Log the error and metadata
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

    # Initialize tables
    init_tables = PythonOperator(
        task_id="create_bigquery_tables",
        python_callable=create_bigquery_tables,
        execution_timeout=timedelta(minutes=10)
    )

    # ✅ Grouped BigQuery schema migration tasks
    with TaskGroup("schema_migrations", tooltip="BigQuery schema migrations") as schema_migrations:
        migrate_metrics_schema = PythonOperator(
            task_id="migrate_metrics_schema",
            python_callable=migrate_metrics_schema,
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
            python_callable=migrate_campaigns_schema,
            execution_timeout=timedelta(minutes=10)
        )

    # ✅ Data extraction and loading tasks
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
    op_kwargs={"table": "metrics", "execution_date": "{{ execution_date.isoformat() }}"},
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
   
  
    run_dbt_model = KubernetesPodOperator(
    task_id='run_dbt_model',
    name='dbt-model-runner',
    namespace='airflow',
    image = "europe-west1-docker.pkg.dev/silicon-window-456317-n1/airflow-gke/dbt-runner:1.0.7"
,
    cmds=["dbt"],
    arguments=["run", "--project-dir", "/dbt", "--profiles-dir", "/home/airflow/.dbt"],
    get_logs=True,
    is_delete_operator_pod=False,
    in_cluster=True
)
    
    # ✅ DAG dependencies
    init_tables >> schema_migrations >> [
        extract_load_campaigns,
        extract_load_ad_groups,
        extract_load_ads,
        extract_load_metrics,
        extract_load_budgets
    ]

    # Run dbt after all the data loading tasks are complete
    [
    extract_load_campaigns,
    extract_load_ad_groups,
    extract_load_ads,
    extract_load_metrics,
    extract_load_budgets
   ] >> run_dbt_model