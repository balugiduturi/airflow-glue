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
# Step 1 :- Read data from s3 bucket landing zone by date
# Step 2 :- Clean the data
        # Remove duplicates values from the data
        # Fill null values from the data
        # Remove the unwanted columns from the data
        # Create new column for month and year
        # Change the data type of the columns to the required format
# Step 3 :- Upsert the latest data
# Step 4 :- Move to silver layer
# List arguments without the '--' prefix
args = getResolvedOptions(sys.argv, [

    'JOB_NAME',
    'process_date',
    'bucket'
])
# Access the values
job_name = args['JOB_NAME']
process_date = args['process_date']          # '2026-10-24'
bucket = args['bucket']                      # 'aws-airflow-data-project-1'

print("=== Arguments are passed from command line ",args)

landing_zone_s3_path = f"s3://{bucket}/airflow-glue-project-1/landing/{process_date}/orders.csv"
silver_s3_path = f"s3://{bucket}/airflow-glue-project-1/silver/"

bronze_df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv(landing_zone_s3_path)
bronze_df.show()

print("=== Bronze Dataframe Schema ===")
bronze_df.printSchema()

# ------------------ Remove duplicates values from the data ---------------------------------

window_spec = Window.partitionBy("order_id").orderBy(col("order_date").desc())
de_duplicate_df = bronze_df.withColumn("row_number", F.row_number().over(window_spec))
de_dup_df = de_duplicate_df.filter(col("row_number") < 2)


# -----------------  Fill null values from the data -------------------------------------
df = de_dup_df.fillna(0, "amount")

# -----------------  Remove the unwanted columns from the data --------------------------  
df = df.drop(col("row_number"))
df.show()

# -----------------  Create new column for month and year
df = df.withColumn('month', F.month(col('order_date')))
df = df.withColumn('next_4_months', F.add_months(col('order_date'),4))
df = df.withColumn("year", F.year(col("order_date")))
df.show()

# ----------------- Upsert the latest data 
from delta.tables import DeltaTable
# Check if Delta table exists before reading
if DeltaTable.isDeltaTable(spark, silver_s3_path):   
    # do upsert
    df_historic_data = spark.read.format("delta").load(silver_s3_path)
    print(f"Delta table found at {silver_s3_path}. and the data below...")
    df_historic_data.show()
    
    print(f"Delta table found at {silver_s3_path}. Performing upsert (MERGE)...")
    target_table = DeltaTable.forPath(spark, silver_s3_path)
    
    target_table.alias('trg').merge(df.alias('src'), 'src.order_id = trg.order_id') \
        .whenMatchedUpdateAll(
            # GUARDRAIL: Only update if the incoming record is strictly newer than what is already stored!
            condition="src.order_date > trg.order_date"
        ) \
            .whenNotMatchedInsertAll() \
                .execute()
    
    print("=== Upsert completed successfully ===")
else:
    # insert the data for first time
    print(f"Delta table not found at {silver_s3_path}. Creating initial Delta table...")
    df.write \
        .mode("append") \
        .format("delta") \
        .save(silver_s3_path)

print("====== Read data from silver layer after adding the data ====== ")
df_final = spark.read.format('delta').load(silver_s3_path)
df_final.show()
job.commit()