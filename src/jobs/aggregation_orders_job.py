import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Glue Context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

print("=== Spark Session & Glue Context Initialized Successfully ===")

# This file is responsible for below actions:-
# Step 1 :- Read data from s3 bucket Silver layer
# Step 2 :- Do some aggregations
    # Daily Financial & Performance Rollup: Total revenue, completed order count, and average order value (AOV) grouped by date
# Step 3 :- Move to gold layer

# List arguments without the '--' prefix

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'bucket'
])

# Access the values
job_name = args['JOB_NAME']
bucket = args['bucket']                      # 'aws-airflow-data-project-1'

print("=== Arguments are passed from command line ",args)

silver_s3_path = f"s3://{bucket}/airflow-glue-project-1/silver/"
gold_s3_path = f"s3://{bucket}/airflow-glue-project-1/gold/obt"

silver_layer_df = spark.read.format('delta').load(silver_s3_path)
silver_layer_df = silver_layer_df \
                    .withColumnRenamed("order_date","order_date_time") \
                    .withColumn("order_date", F.date_format("order_date_time", "yyyy-MM-dd"))
print("=== Silver Dataframe Schema ===")
silver_layer_df.printSchema()
silver_layer_df.show()

grouped_df = silver_layer_df.groupBy(['order_date']) \
        .agg(F.sum('amount').alias("total_revenue"), 
                F.round(F.avg('amount'), 2).alias("avg_order_value"),
                F.count('order_id').alias("total_roders"))
grouped_df.show()

grouped_df.write.format('delta').mode('overwrite').save(gold_s3_path)

gold_df = spark.read.format('delta').load(gold_s3_path)
gold_df.show()
job.commit()