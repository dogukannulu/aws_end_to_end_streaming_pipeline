SELECT
    *,
    CASE
        WHEN CHAR_LENGTH(title) <= 10 THEN 'Short Name'
        WHEN CHAR_LENGTH(title) < 20 AND CHAR_LENGTH(title) > 10 THEN 'Middle Name'
        ELSE 'Long Name'
    END AS name_length_category
FROM 'glue-etl-books-parquet-dogukan-ulu.clean_books_parquet'
