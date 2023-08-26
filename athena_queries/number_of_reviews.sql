SELECT
    num_reviews,
    COUNT(*) as total_number_of_reviews
FROM 'glue-etl-books-parquet-dogukan-ulu.clean_books_parquet'
GROUP BY num_reviews