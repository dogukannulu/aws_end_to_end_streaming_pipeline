create external schema redshift_parquet
from data catalog
database 'glue-etl-books-parquet-dogukan-ulu-external'
iam_role '<iam_role_arn>'
create external database if not exists;