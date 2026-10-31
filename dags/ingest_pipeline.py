# This is airflow pipeline for data move to silver layer and gold layer

## DAG for ingestion pipeline
## below steps will be performed in this pipeline
# step 1 :- Ingest data from S3 bucket (landing zone) to silver layer using glue job
# step 2 :- Move data from silver layer to gold layer using glue job

from datetime import datetime
from airflow.sdk import dag
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

AWS_CONN_ID = "dev_aws_airflow_connections"
AWS_REGION = "ap-south-1"
S3_BUCKET = "aws-airflow-data-project-1"
PROCESS_DATE = "2026-10-23"


@dag(
    dag_id='ingest_pipeline',
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def ingest_pipeline():

    start_silver_layer_job = GlueJobOperator(
        task_id='start_silver_layer_job',
        job_name='clean_orders_job',
        script_args={
            '--bucket': S3_BUCKET,
            '--process_date': PROCESS_DATE,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        deferrable=True,  # Asynchronous via Airflow Triggerer (zero worker threads blocked)
        wait_for_completion=True,
        verbose=True,
    )

    start_gold_layer_job = GlueJobOperator(
        task_id='start_gold_layer_job',
        job_name='aggregation_orders_job',
        script_args={
            '--bucket': S3_BUCKET,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        deferrable=True,  # Asynchronous via Airflow Triggerer (zero worker threads blocked)
        wait_for_completion=True,
        verbose=True,
    )

    start_silver_layer_job >> start_gold_layer_job


ingest_pipeline()