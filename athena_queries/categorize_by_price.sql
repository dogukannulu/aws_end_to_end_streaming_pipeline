WITH all_price_categories AS (
    SELECT
        title,
        CASE
            WHEN price <= 15 THEN 'Cheap'
            WHEN price <= 25 AND price < 40 THEN 'Middle'
            ELSE 'Expensive'
        END as price_category
    FROM 'glue-etl-books-parquet-dogukan-ulu.clean_books_parquet'
)

SELECT
    title,
    price_category = 'Cheap' as is_cheap,
    price_category = 'Middle' as is_middle,
    price_category = 'Expensive' as is_expensive
FROM all_price_categories
