"""
# Monthly Account Balance Generation DAG

This Directed Acyclic Graph (DAG) orchestrates the process of generating a monthly account balance file by performing the following steps:

1. **Create Snowflake Table:** Executes an SQL query to create a table in Snowflake.
2. **Load Data to Snowflake:** Executes an SQL query to load data into Snowflake.
3. **Generate Pre-signed URL:** Generates a pre-signed URL for the uploaded CSV file in S3, making it available for download.
4. **Notify Slack:** Sends a Slack message with the pre-signed URL to notify stakeholders.

## Parameters:
- **snowflake_conn_id:** The connection ID for Snowflake.
- **aws_conn_id:** The connection ID for AWS, including credentials for accessing S3.
- **queries_base_path:** The path where SQL queries are stored.
- **db:** The database name in Snowflake.
- **schema_origin:** The source schema in Snowflake.
- **schema_destination:** The destination schema in Snowflake for storing results.
- **stage:** The external stage in Snowflake for loading data.
- **path:** Path for output files in S3.
- **bucket_s3:** The S3 bucket where the files are stored.
- **filename:** The name of the CSV file containing the account balance.
- **files_dir:** Local directory for saving the generated files before uploading to S3.

## Tasks:
1. **create_table_snowflake:** Runs the SQL script to create a table in Snowflake.
2. **load_to_s3:** Loads the data into Snowflake from an S3 bucket.
3. **generate_presigned_url:** Generates a pre-signed URL for accessing the CSV file in S3.
4. **notify_slack:** Sends a Slack message with the link to the generated CSV file.

## Use case:
This DAG is useful for monthly financial reporting where the balance is calculated in Snowflake and made available for download via an S3 link.


**Owner:** Andres Marciales
**Default retries:** 3  
"""
import os
import boto3
from datetime import datetime
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

PARAMS_DEFINITION = {
    'snowflake_conn_id': 'snowflake_conn_id',
    'aws_conn_id': 'aws_conn_id',
    'queries_base_path': os.path.join(os.path.dirname(__file__), 'queries'),
    'db': 'NU_CO_DEV_BUSINESS',
    'schema_origin': 'POSTGRES_RDS_PUBLIC',
    'schema_destination': 'FINANCE_SILVER_TABLES',
    'stage': '@NU_CO_DEV_BUSINESS.POSTGRES_RDS_PUBLIC.S3_STAGE_STUDY_CASE',
    'path': 'output_files',
    'bucket_s3': 'terraform-nu-db-pg-48baa6b7',
    'filename': 'account_monthly_balance.csv',
    'files_dir': os.path.join('/tmp', 'balance')
}

@dag(
    start_date=datetime(2025, 5, 1),
    schedule=None,
    template_searchpath=PARAMS_DEFINITION['queries_base_path'],
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "AFMP", "retries": 3},
    description='Run queries into Snowflake and upload results to S3',
    tags=["snowflake", "s3"]
)
def account_monthly_balance_generation():

    create_table_snowflake = SQLExecuteQueryOperator(
        task_id="create_table_snowflake",
        sql='create_table_snowflake.sql',
        conn_id=PARAMS_DEFINITION['snowflake_conn_id'],
        params=PARAMS_DEFINITION
    )

    load_to_s3 = SQLExecuteQueryOperator(
        task_id="load_to_s3",
        sql='load_to_s3.sql',
        conn_id=PARAMS_DEFINITION['snowflake_conn_id'],
        params=PARAMS_DEFINITION
    )

    @task()
    def generate_presigned_url(bucket_name: str, object_key: str, expires_in_sec: int) -> str:
        conn = BaseHook.get_connection(PARAMS_DEFINITION['aws_conn_id'])
        extras = conn.extra_dejson

        session = boto3.Session(
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
            region_name=extras.get("region_name", "us-east-1")
        )

        s3 = session.client("s3", region_name="us-east-1")

        url = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': bucket_name, 'Key': object_key},
            ExpiresIn=expires_in_sec
        )

        return url

    @task()
    def notify_slack(url: str):
        SlackWebhookOperator(
            task_id="send_slack_message",
            slack_webhook_conn_id="slack_webhook_conn",
            message=f"✅ CSV file for monthly account balance is available at:\n{url}",
            username="airflow-bot"
        ).execute({})

    create_table_snowflake >> load_to_s3

    presigned_url = generate_presigned_url(
        PARAMS_DEFINITION['bucket_s3'],
        f"{PARAMS_DEFINITION['path']}/{PARAMS_DEFINITION['filename']}",
        604800  # 7 days
    )

    load_to_s3 >> presigned_url >> notify_slack(presigned_url)

dag = account_monthly_balance_generation()
