
process_approp_account <- function() {
  
  approp_account_pages <- read_rds(file.path(intermediate_path, "labeled_pages", "approp_account.rds"))
  
  row_pattern <- "(\\d{5}) (.+?) ([-\\d,()]+) ([-\\d,()]+) ([-\\d,()]+) ([-\\d,()]+) ([-\\d,()]+|N/A)"
  
  col_names <- c(
    "approp_account",
    "description",
    "offset_years_3",
    "offset_years_2",
    "offset_years_1",
    "offset_years_0",
    "delta"
  )
  
  approp_account_extracted <- approp_account_pages |>
    mutate(matches = str_extract_all(squish_text, row_pattern)) |>
    unnest(matches) |>
    extract(
      matches,
      into = col_names,
      regex = row_pattern,
      convert = FALSE
    )
  
  approp_account_long <- approp_account_extracted |>
    # Drop delta here because we can recalculate it
    select(-c(delta)) |> 
    pivot_longer(
      cols = starts_with("offset_years_"),
      names_to = "offset_years",
      names_pattern = "offset_years_(\\d)",
      values_to = "amount"
    )
  
  
  
  approp_account_hashed <- approp_account_long |> 
    mutate(
      key_string = paste0(pdf_filename, pdf_page, book_year, offset_years, dept_id_root, approp_account),
      row_id = map_chr(key_string, ~digest(.x, algo = "xxh3_64"))
    )
  
  # duplicates <- approp_account_hashed |> 
  #   count(key_string) |> 
  #   filter(n > 1)
  
  approp_account_parsed <- approp_account_hashed |> 
    select(
      row_id,
      pdf_filename,
      pdf_page,
      book_year,
      offset_years,
      dept_id_root,
      approp_account,
      amount
    ) |> 
    mutate(
      book_year = as.integer(book_year),
      offset_years = as.integer(offset_years),
      dept_id_root = as.integer(dept_id_root),
      approp_account = as.integer(approp_account),
      amount = str_replace_all(amount, "^\\((.*)\\)$", "-\\1"),
      amount = parse_number(amount)
    )
  
  approp_account_patch <- read_csv(file.path(manual_path, "approp_account_patch.csv")) |> 
    select(-review_note)
  
  approp_account_patched <- approp_account_parsed |> 
    rows_update(approp_account_patch)
  
  write_parquet(approp_account_patched, file.path(clean_path, "stg/approp_account_history.parquet"))
  
}
