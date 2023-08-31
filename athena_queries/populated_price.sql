WITH mean_price AS (
    SELECT AVG(price) AS mean_price_value
    FROM `glue-etl-books-parquet-dogukan-ulu.clean_books_parquet`
    WHERE price IS NOT NULL
)

SELECT
    title,
    num_reviews,
    availability,
    IFNULL(price, (SELECT mean_price_value FROM mean_price)) as populated_price
FROM `glue-etl-books-parquet-dogukan-ulu.clean_books_parquet`
