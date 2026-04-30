{{ config(
    materialized='external',
    format='parquet',
    location=var('output_path') ~ '/' ~ this.name ~ '.parquet'
) }}

WITH deduped AS (
    SELECT *,
    CASE 
        WHEN offset_years >= 2 THEN 1
        WHEN offset_years = 1 THEN 2
        WHEN offset_years = 0 THEN 3
    END AS quality_score
    FROM {{ ref('non_discretionary_history_all') }}
    QUALIFY ROW_NUMBER() OVER (
    PARTITION BY fy, dept_id_root
    ORDER BY book_year DESC, quality_score ASC, pdf_page DESC
    ) = 1
)

SELECT 
    fy,
    dept_id_root,
    amount,
    pdf_filename,
    pdf_page,
    row_id
FROM deduped
WHERE amount > 0