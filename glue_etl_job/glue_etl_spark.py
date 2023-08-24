import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

glue_dynamic_frame_initial = glueContext.create_dynamic_frame.from_catalog(database='books-lambda-parquet-dogukan-ulu', table_name='books_parquet')

df_spark = glue_dynamic_frame_initial.toDF()

# Transformation for the price column
df_spark = df_spark.withColumn("price", F.regexp_replace("price", "[^0-9.]", "").cast("float"))

# Transformation for the num_reviews column
num_review_mapping = {
    "One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5,
    "Six": 6, "Seven": 7, "Eight": 8, "Nine": 9, "Ten": 10
}

df_spark = df_spark.drop("upc")

df_final = df_spark.withColumn("num_reviews", when(df_spark["num_reviews"].isin(num_review_mapping.keys()), num_review_mapping[df_spark["num_reviews"]]).otherwise(df_spark["num_reviews"].cast("int")))


# From Spark dataframe to glue dynamic frame
glue_dynamic_frame_final = DynamicFrame.fromDF(df_final, glueContext, "glue_etl")

# Write the data in the DynamicFrame to a location in Amazon S3 and a table for it in the AWS Glue Data Catalog
s3output = glueContext.getSink(
  path="s3://books-glue-etl-job-spark-parquet-dogukan-ulu/books_parquet",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)

s3output.setCatalogInfo(
  catalogDatabase="books-glue-etl-job-spark-parquet-dogukan-ulu", catalogTableName="books_parquet"
)

s3output.setFormat("glueparquet")
s3output.writeFrame(glue_dynamic_frame_final)

job.commit()