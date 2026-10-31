import logging

from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)


class AwsUtils:

    def __init__(
        self,
        aws_conn_id: str = "dev_aws_airflow_connections",
        region_name: str = "ap-south-1",
        glue_client=None,
        s3_client=None,
    ):
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

        # Connect Glue using Airflow GlueJobHook
        if glue_client is not None:
            self.glue_client = glue_client
        else:
            self.glue_hook = GlueJobHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
            self.glue_client = self.glue_hook.get_conn()

        # Connect S3 using Airflow S3Hook
        if s3_client is not None:
            self.s3_client = s3_client
        else:
            self.s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
            self.s3_client = self.s3_hook.get_conn()

    def start_glue_job(
        self,
        job_name: str,
        bucket_name: str,
        process_date: str = None,
        **kwargs,
    ) -> str:
        """
        Starts an AWS Glue job with parameters and returns the job run ID.
        Non-blocking trigger.
        """
        arguments = {
            "--bucket": bucket_name,
        }
        if process_date:
            arguments["--process_date"] = process_date

        for key, value in kwargs.items():
            arguments[f"--{key}"] = str(value)

        response = self.glue_client.start_job_run(
            JobName=job_name,
            Arguments=arguments,
        )
        job_run_id = response["JobRunId"]
        logger.info("Glue job '%s' started with Run ID: %s", job_name, job_run_id)
        return job_run_id
