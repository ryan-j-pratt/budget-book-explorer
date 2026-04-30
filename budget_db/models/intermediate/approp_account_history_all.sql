SELECT 
    row_id,
    pdf_filename,
    pdf_page,
    book_year,
    offset_years,
    (book_year - offset_years) AS fy,
    dept_id_root,
    CAST(SUBSTRING(CAST(approp_account AS VARCHAR), 1, 2) AS INTEGER) AS approp_account_category,
    CASE
        WHEN approp_account_category = 51 THEN 1
        ELSE 0
    END AS flag_personnel,
    approp_account,
    amount
FROM read_parquet('{{ var("input_path") }}/approp_account_history.parquet')