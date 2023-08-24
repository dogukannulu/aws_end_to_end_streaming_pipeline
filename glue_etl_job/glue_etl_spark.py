import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

# num_reviews column mapping
num_review_mapping = {
    "One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5
}

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def create_initial_df():
    glue_dynamic_frame_initial = glueContext.create_dynamic_frame.from_catalog(database='glue-etl-books-parquet-dogukan-ulu', table_name='dirty_books_parquet')
    df_spark = glue_dynamic_frame_initial.toDF()

    return df_spark


def modify_and_create_final_spark_df(df):
    df = df.withColumn("price", F.regexp_replace("price", "[^0-9.]", "").cast("float"))
    df = df.drop("upc")
    df_final = df.withColumn("num_reviews", F.when(df_spark["num_reviews"].isin(num_review_mapping.keys()), num_review_mapping[df_spark["num_reviews"]]).otherwise(df_spark["num_reviews"].cast("int")))

    return df_final

df_spark = create_initial_df()
df_final = create_initial_df(df_spark)

# From Spark dataframe to glue dynamic frame
glue_dynamic_frame_final = DynamicFrame.fromDF(df_final, glueContext, "glue_etl")

# Write the data in the DynamicFrame to a location in Amazon S3 and a table for it in the AWS Glue Data Catalog
s3output = glueContext.getSink(
  path="s3://aws-glue-clean-books-parquet-dogukan-ulu/clean_books_parquet",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)

s3output.setCatalogInfo(
  catalogDatabase="glue-etl-books-parquet-dogukan-ulu", catalogTableName="clean_books_parquet"
)

s3output.setFormat("glueparquet")
s3output.writeFrame(glue_dynamic_frame_final)

job.commit()
