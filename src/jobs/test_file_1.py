import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue Context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

print("=== Spark Session & Glue Context Initialized Successfully ===")
data = [("jansy", 28), ("Bob", 35), ("Charlie", 42)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)
df.show()
# Write as Parquet (standard for data lakes)
df.write.mode("overwrite").parquet("s3://aws-airflow-data-project-1/bronze/test_data_1")