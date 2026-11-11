from airflow.sdk import dag, task
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

AWS_CONN_ID = "dev_aws_airflow_connections"
AWS_REGION = "ap-south-1"
S3_BUCKET = "aws-airflow-data-project-1"
PROCESS_DATE = "2026-10-23"
LANDING_ZONE_FOLDER_PATH = "project_2/landing_zone"
SILVER_ZONE_FOLDER_PATH = "project_2/silver_zone"
GOLD_ZONE_FOLDER_PATH = "project_2/gold_zone"
AGRREGATION_JOB_NAME = "Project_2_Aggregation_Orders_Job"
CLEAN_JOB_NAME = "Project_2_Clean_Orders_Job"

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

@dag(
    dag_id="project_2_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 0 * * *",
    catchup=False,
    default_args=default_args,
    tags=['s3', 'glue']
)

def project_2_pipeline():

    # -------------------------------------------------------------------------
    # Task 1: Check S3 bucket and MANUALLY PUSH file list to XCom
    # -------------------------------------------------------------------------

    @task.python(task_id='list_landing_files')

    def list_landing_files(**context):
        ti = context['ti']
        ds = context['ds']
        previous_days = ['2026-10-23','2026-10-24']
        previous_days = [PROCESS_DATE]
        processed_day = ""
        valid_files = []

        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
        bucket = S3_BUCKET

        for day in previous_days:
            prefix = f"project_2/landing_zone/{day}/"
            # prefix = f"airflow-glue-project-1/landing/{day}/"            
            files = s3.list_keys(bucket, prefix)    

            if not files:
               raise FileNotFoundError(f"No files found in s3://{bucket}/{prefix}")
            else:
                for file in files:
                    valid_files.append(file)          

            processed_day = day

        print(f"Task 1: Discovered {len(valid_files)} files: {valid_files}") 

        # Explicit manual XCom push with custom key
        ti.xcom_push(key="raw_files_list", value=valid_files)
        ti.xcom_push(key="s3_bucket", value=bucket)
        ti.xcom_push(key="processed_day", value=processed_day)

    # -------------------------------------------------------------------------
    # Task 2: MANUALLY PULL files from Task 1, verify, and MANUALLY PUSH clean list
    # -------------------------------------------------------------------------

    @task.python(task_id="verify_landing_files")
    def verify_landing_files(**context):
        ti = context['ti']
        ds = context['ds']
        valid_files = []
        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
        # Explicit manual XCom pull from Task 1 using custom key
        raw_files = ti.xcom_pull(task_ids="list_landing_files", key="raw_files_list")
        bucket = ti.xcom_pull(task_ids="list_landing_files", key="s3_bucket")
        print("bucket name ::::",bucket)
        if not bucket:
            raise ValueError("Task 2: Bucket name is empty!")
        if not raw_files:
            raise ValueError("Task 2: No files received from upstream task!")

        for file in raw_files:
            print(file.endswith('.csv'))
            if file.endswith('.csv'):
                obj = s3.get_key(file, bucket_name=bucket)
                if obj and obj.content_length > 0:
                    valid_files.append(f"s3://{bucket}/{file}")      

        if not valid_files:
            raise ValueError("Task 2: All files failed validation or are 0 bytes.")

        print(f"Task 2: Verified {len(valid_files)} valid files: {valid_files}")
        # Explicit manual XCom push with custom key
        ti.xcom_push(key="valid_files_list", value=valid_files)

    # -------------------------------------------------------------------------
    # Intermediate Task: Format parameters & MANUALLY PUSH custom keys for Glue
    # -------------------------------------------------------------------------

    @task.python(task_id="format_glue_arguments")
    def format_glue_arguments(**context):
        ti = context['ti']
        valid_files = ti.xcom_pull(task_ids='verify_landing_files', key='valid_files_list')
        processed_day = ti.xcom_pull(task_ids='list_landing_files', key='processed_day')
        active_files = [f for f in valid_files if "temp" not in f]
        if not active_files:
            raise ValueError("No active files remaining after filtering.")      

        # adding glue Job name for next pipeline
        ti.xcom_push(key='silver_layer_ingest_job_name', value='Project_2_Clean_Orders_Job')
        ti.xcom_push(key='process_date', value=processed_day)
        ti.xcom_push(key='valid_files', value=valid_files)

    # -------------------------------------------------------------------------
    # Task 3: Glue Job (Landing -> Silver)
    # Pulls manual XCom keys using Jinja templating
    # -------------------------------------------------------------------------

    run_silver_glue = GlueJobOperator(
        task_id="glue_landing_to_silver",
        retry_delay = timedelta(seconds=60),
        wait_for_completion=True, # Blocks downstream tasks until complete
        deferrable=True,    # Handled asynchronously by the Airflow Triggerer
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        job_name="{{ ti.xcom_pull(task_ids='format_glue_arguments', key='silver_layer_ingest_job_name') }}",
        script_args={
            "--process_date" : "{{ ti.xcom_pull(task_ids='format_glue_arguments', key='process_date') }}",
            "--bucket" : "{{ ti.xcom_pull(task_ids='list_landing_files', key='s3_bucket') }}",
            "--landing_zone_folder_path" : LANDING_ZONE_FOLDER_PATH,
            "--silver_zone_folder_path" : SILVER_ZONE_FOLDER_PATH,
            "--datalake-formats": "delta",
            "--conf": "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        }
    )


    # -------------------------------------------------------------------------
    # Task 4: Glue Job (Silver -> Gold)
    # Pulls manual XCom keys using Jinja templating
    # -------------------------------------------------------------------------

    run_gold_glue = GlueJobOperator(
        task_id="glue_silver_to_gold",
        retry_delay = timedelta(seconds=60),
        wait_for_completion=True, # Blocks downstream tasks until complete
        deferrable=True,    # Handled asynchronously by the Airflow Triggerer
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        job_name=AGRREGATION_JOB_NAME,
        script_args={
            "--bucket":S3_BUCKET,
            "--silver_zone_folder_path":SILVER_ZONE_FOLDER_PATH,
            "--gold_zone_folder_path":GOLD_ZONE_FOLDER_PATH,
            "--datalake-formats": "delta",
            "--conf": "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        }
    )

    # -------------------------------------------------------------------------
    # Task 5a: S3 task to verify Gold folder output
    # Manually pulls Glue JobRunIds (return_value) and target folder path
    # -------------------------------------------------------------------------

    @task(task_id="verify_gold_folder")
    def verify_gold_folder(**context):
        ti = context["ti"]
        ds = context["ds"]

        # GlueJobOperator automatically pushes its AWS JobRunId to key='return_value'
        silver_run_id = ti.xcom_pull(task_ids="glue_landing_to_silver", key="return_value")
        gold_run_id = ti.xcom_pull(task_ids="glue_silver_to_gold", key="return_value")
        gold_prefix_full = ti.xcom_pull(task_ids="format_glue_arguments", key="gold_path")

        print(f"Task 5a: Verified upstream Glue runs completed:")
        print(f"  - Silver Run ID: {silver_run_id}")
        print(f"  - Gold Run ID: {gold_run_id}")

        s3 = S3Hook(aws_conn_id="aws_default")
        bucket = "my-gold-lake"
        prefix = f"gold/daily_sales/order_date={ds}/"

        keys = s3.list_keys(bucket_name=bucket, prefix=prefix)
        if not keys:
            raise FileNotFoundError(f"Gold output missing in s3://{bucket}/{prefix}")

        print(f"Task 5a: Confirmed {len(keys)} files present at {gold_prefix_full}")

    # -------------------------------------------------------------------------
    # Task 5b: Trigger AWS Glue Crawler
    # -------------------------------------------------------------------------

    # =========================================================================
    # Explicit Task Instantiation & Dependency Sequencing
    # =========================================================================

    list_landing_files = list_landing_files()
    verify_landing_files = verify_landing_files()
    format_glue_arguments = format_glue_arguments()

    # Linear bitshift (>>) dependency chain
    list_landing_files >> verify_landing_files >> format_glue_arguments >> run_silver_glue >> run_gold_glue

# Instantiate the DAG
project_2_pipeline()