
process_program <- function() {
  
  program_pages <- read_rds(file.path(intermediate_path, "labeled_pages", "program.rds"))
  
  row_pattern <- "(.*?) ([-\\d,()]+) ([-\\d,()]+) ([-\\d,()]+) ([-\\d,()]+)"
  
  col_names <- c(
    "item",
    "offset_3",
    "offset_2",
    "offset_1",
    "offset_0"
  )
  
  program_extracted <- program_pages |>
    mutate(
      matches = str_extract_all(squish_text, row_pattern),
      dept_id = str_extract(squish_text, "(?<=(?:Appropriation|Organization|Oragnization):? )\\d{6}")
    ) |>
    group_by(pdf_filename) |> 
    fill(dept_id, .direction = "down") |> 
    ungroup() |> 
    unnest(matches) |>
    extract(
      matches,
      into = col_names,
      regex = row_pattern,
      convert = FALSE
    ) |> 
    mutate(
      flag_personnel = case_when(
        str_detect(item, regex("Non[- ]Personnel$", ignore_case = TRUE)) ~ 0,
        str_detect(item, regex("Personnel Services$", ignore_case = TRUE)) ~ 1,
        TRUE ~ NA_integer_
      )
    ) |>
    filter(!is.na(flag_personnel))
  
  program_long <- program_extracted |>
    pivot_longer(
      cols = starts_with("offset_"),
      names_to = "offset",
      names_pattern = "offset_(\\d)",
      values_to = "amount"
    )
  
  program_hashed <- program_long |> 
    mutate(
      key_string = paste0(pdf_filename, pdf_page, book_year, offset, dept_id_root, dept_id, flag_personnel),
      row_id = map_chr(key_string, ~digest(.x, algo = "xxh3_64"))
    )
  
  program_parsed <- program_hashed |> 
    select(
      row_id,
      pdf_filename,
      pdf_page,
      book_year,
      offset,
      dept_id_root,
      dept_id,
      flag_personnel,
      amount
    ) |> 
    mutate(
      book_year = as.integer(book_year),
      offset = as.integer(offset),
      dept_id_root = as.integer(dept_id_root),
      dept_id = as.integer(dept_id),
      flag_personnel = as.integer(flag_personnel),
      amount = str_replace_all(amount, "^\\((.*)\\)$", "-\\1"),
      amount = parse_number(amount)
    )
  
  program_patch <- read_csv(file.path(manual_path, "program_patch.csv")) |>
    select(-review_note)

  program_patched <- program_parsed |>
    rows_update(program_patch)
  
  write_parquet(program_parsed, file.path(clean_path, "program.parquet"))
  
}