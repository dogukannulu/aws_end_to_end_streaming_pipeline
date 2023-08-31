WITH max_price AS (
    SELECT
        title,
        price,
        num_reviews,
        ROW_NUMBER() OVER(PARTITION BY num_reviews ORDER BY price DESC) as price_row_number
    FROM `glue-etl-books-parquet-dogukan-ulu.clean_books_parquet`
)


SELECT
    title,
    price,
    num_reviews
FROM max_price
WHERE price_row_number = 1
