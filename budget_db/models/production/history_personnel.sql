{{ config(
    materialized='external',
    format='parquet',
    location=var('output_path') ~ '/' ~ this.name ~ '.parquet'
) }}

SELECT 
    row_id,
    pdf_filename,
    pdf_page,
    book_year AS fy,
    dept_id_root,
    title,
    union_affiliation,
    grade,
    position,
    salary,
    (salary / position) AS avg_salary_pp
FROM read_parquet('{{ var("input_path") }}/personnel_history.parquet')