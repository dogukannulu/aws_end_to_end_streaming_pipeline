WITH numbers AS (
    SELECT
        COUNT(
            CASE
                WHEN availability = 1 THEN 1
            END
        ) AS number_of_available_books,
        COUNT(*) as total_number_of_books
    FROM `glue-etl-books-parquet-dogukan-ulu.clean_books_parquet`
)

SELECT
    number_of_available_books/total_number_of_books as availability_ratio
FROM numbers
