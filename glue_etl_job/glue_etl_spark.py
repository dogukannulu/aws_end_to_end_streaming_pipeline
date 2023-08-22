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

glue_dynamic_frame_initial = glueContext.create_dynamic_frame.from_catalog(database='glue-etl-from-parquet-to-redshift', table_name='parquet_data')

df_spark = glue_dynamic_frame_initial.toDF()

##############################
# Insert all the transformations here
# transformed_data

###############################


jdbc_url = "jdbc:redshift://your-redshift-cluster-endpoint:5439/your-database"
redshift_username = "your-username"
redshift_password = "your-password"
redshift_table = "target_table_name"

filtered_df.write \
    .format("com.databricks.spark.redshift") \
    .option("url", jdbc_url) \
    .option("dbtable", redshift_table) \
    .option("user", redshift_username) \
    .option("password", redshift_password) \
    .mode("append") \
    .save()

job.commit()