CREATE EXTERNAL TABLE redshift_parquet.clean_books_parquet(
    title VARCHAR(100),
    price FLOAT,
    num_reviews INTEGER,
    availability INTEGER
)
stored as parquet
LOCATION 's3://aws-glue-clean-books-parquet-dogukan-ulu/clean_books_parquet/';