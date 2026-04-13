
process_non_discretionary <- function() {
  dept_pages <- read_rds(file.path(intermediate_path, "labeled_pages", "dept.rds"))
  
  non_discretionary_programs <- c(
    139, # Medicare Payments
    148, # Health Insurance
    199, # Unemployment Compensation
    331, # Snow and Winter Management
    333, # Execution of Courts
    341, # Workers' Compensation
    374, # Pensions and Annuities
    749  # Pensions and Annuities - County
  )
  
  non_discretionary_pages <- dept_pages |> 
    filter(dept_id_root %in% non_discretionary_programs)
  
  row_pattern <- "(.*?) ([-\\d,()]+) ([-\\d,()]+) ([-\\d,()]+) ([-\\d,()]+)"
  
  col_names <- c(
    "item",
    "offset_years_3",
    "offset_years_2",
    "offset_years_1",
    "offset_years_0"
  )
  
  non_discretionary_extracted <- non_discretionary_pages |>
    mutate(
      matches = str_extract_all(squish_text, row_pattern)
    ) |> 
    filter(lengths(matches) > 0) |> 
    mutate(first_match = map_chr(matches,1)) |> 
    extract(
      first_match,
      into = col_names,
      regex = row_pattern,
      convert = FALSE
    ) |> 
    mutate(item = str_squish(item))
  
  # Execution of Courts didn't have a page in 2025 book, ignoring
  # chk <- non_discretionary_extracted |> 
  #   filter(dept_id_root == 333)
  #   group_by(dept_id_root) |> 
  #   summarize(distinct_years = n_distinct(book_year)) |> 
  #   ungroup()
  
  non_discretionary_long <- non_discretionary_extracted |>
    pivot_longer(
      cols = starts_with("offset_years_"),
      names_to = "offset_years",
      names_pattern = "offset_years_(\\d)",
      values_to = "amount"
    )
  
  non_discretionary_hashed <- non_discretionary_long |> 
    mutate(
      key_string = paste0(pdf_filename, pdf_page, book_year, offset_years, dept_id_root),
      row_id = map_chr(key_string, ~digest(.x, algo = "xxh3_64"))
    )
  
  # print(length(unique(non_discretionary_hashed$row_id)))
  
  
  non_discretionary_parsed <- non_discretionary_hashed |> 
    select(
      row_id,
      pdf_filename,
      pdf_page,
      book_year,
      offset_years,
      dept_id_root,
      amount
    ) |> 
    mutate(
      book_year = as.integer(book_year),
      offset_years = as.integer(offset_years),
      dept_id_root = as.integer(dept_id_root),
      amount = str_replace_all(amount, "^\\((.*)\\)$", "-\\1"),
      amount = parse_number(amount)
    )
  
  write_parquet(non_discretionary_parsed, file.path(clean_path, "stg/non_discretionary_history.parquet"))
  
}

