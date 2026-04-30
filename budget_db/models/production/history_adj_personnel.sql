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
    adj_type,
    adj_amount
FROM read_parquet('{{ var("input_path") }}/personnel_adj_history.parquet')