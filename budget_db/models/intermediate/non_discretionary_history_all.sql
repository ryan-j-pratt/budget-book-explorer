SELECT 
    row_id,
    pdf_filename,
    pdf_page,
    book_year,
    offset_years,
    (book_year - offset_years) AS fy,
    dept_id_root,
    amount
FROM read_parquet('{{ var("input_path") }}/non_discretionary_history.parquet')