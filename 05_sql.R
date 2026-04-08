
con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

approp_account_path <- path.expand(file.path(clean_path, 'approp_account.parquet'))
program_path <- path.expand(file.path(clean_path, 'program.parquet'))

approp_account_query <- glue("
WITH base AS (
    SELECT 
        row_id,
        pdf_filename,
        pdf_page,
        book_year,
        \"offset\",
        (book_year - \"offset\") AS fy,
        dept_id_root,
        CAST(SUBSTRING(CAST(approp_account AS VARCHAR), 1, 2) AS INTEGER) AS approp_account_category,
        CASE
            WHEN approp_account_category = 51 THEN 1
            ELSE 0
        END AS flag_personnel,
        approp_account,
        amount,
        CASE 
            WHEN \"offset\" >= 2 THEN 1
            WHEN \"offset\" = 1 THEN 2
            WHEN \"offset\" = 0 THEN 3
        END AS quality_score
    FROM read_parquet('{approp_account_path}')
),

deduped AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fy, dept_id_root, approp_account 
        ORDER BY book_year DESC, quality_score ASC, pdf_page DESC
    ) = 1
)

SELECT 
    fy,
    dept_id_root,
    flag_personnel,
    approp_account_category,
    approp_account,
    amount,
    pdf_filename,
    pdf_page,
    row_id
FROM deduped
WHERE amount > 0;
")

program_query <- glue("
WITH base AS (
    SELECT 
        row_id,
        pdf_filename,
        pdf_page,
        book_year,
        \"offset\",
        (book_year - \"offset\") AS fy,
        dept_id_root,
        dept_id,
        flag_personnel,
        amount,
        CASE 
            WHEN \"offset\" >= 2 THEN 1
            WHEN \"offset\" = 1 THEN 2
            WHEN \"offset\" = 0 THEN 3
        END AS quality_score
    FROM read_parquet('{program_path}')
),

deduped AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fy, dept_id, flag_personnel 
        ORDER BY book_year DESC, quality_score ASC, pdf_page DESC
    ) = 1
)

SELECT 
    fy,
    dept_id_root,
    dept_id,
    flag_personnel,
    amount,
    pdf_filename,
    pdf_page,
    row_id
FROM deduped
WHERE amount > 0;
")



clean_approp_account <- dbGetQuery(con, approp_account_query)
clean_program <- dbGetQuery(con, program_query)

dbDisconnect(con, shutdown = TRUE)
