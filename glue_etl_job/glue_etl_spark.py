import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType

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
    df = glue_dynamic_frame_initial.toDF()

    return df


def map_num_reviews(review):
    return num_review_mapping.get(review, review)

map_num_reviews_udf = F.udf(map_num_reviews, IntegerType())

# Define the UDF for price formatting
def format_price(price):
    price = price.replace("£", "").replace(",", "")
    return round(float(price), 2)

format_price_udf = F.udf(format_price, FloatType())

df = create_initial_df()
# Modify and create the final DataFrame
df_final = df.withColumn("price", format_price_udf("price")) \
             .drop("upc") \
             .withColumn("num_reviews", map_num_reviews_udf("num_reviews")) \
             .withColumn("availability", F.when(df["availability"] == "In stock", 1).otherwise(0))


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
