
process_personnel <- function() {
  
  personnel_pages <- read_rds(file.path(intermediate_path, "labeled_pages", "personnel.rds"))
  
  row_pattern <- "(\\S.*?) ([A-Za-z0-9|]{3,4}) (?:([A-Z0-9]{1,4}) )?([0-9.]+) ([0-9,]{3,})"
  
  col_names <- c(
    "title",
    "union",
    "grade",
    "position",
    "salary"
  )
  
  personnel_extracted <- personnel_pages |>
    mutate(matches = str_extract_all(squish_text, row_pattern)) |>
    unnest(matches) |>
    extract(
      matches,
      into = col_names,
      regex = row_pattern,
      convert = FALSE
    )
  
  personnel_hashed <- personnel_extracted |> 
    mutate(
      key_string = paste0(pdf_filename, pdf_page, book_year, dept_id_root, title, union, grade, position, salary),
      row_id = map_chr(key_string, ~digest(.x, algo = "xxh3_64"))
    )
  
  # duplicates <- personnel_hashed |>
  #   count(key_string) |>
  #   filter(n > 1)
  
  personnel_parsed <- personnel_hashed |> 
    select(
      row_id,
      pdf_filename,
      pdf_page,
      book_year,
      dept_id_root,
      title,
      union,
      grade,
      position,
      salary
    ) |> 
    # Rename to play nice with SQL
    rename(
      union_affiliation = union
    ) |> 
    mutate(
      book_year = as.integer(book_year),
      dept_id_root = as.integer(dept_id_root),
      salary = str_replace_all(salary, "^\\((.*)\\)$", "-\\1"),
      position = parse_number(position),
      salary = parse_number(salary)
    )
  
  #print(unique(personnel_parsed$position))
  
  adj_row_pattern <- "(Differential Payments|Other|Chargebacks|Salary Savings) ([-\\d,.()]++)"
  
  adj_extracted <- personnel_pages |> 
    mutate(matches = str_extract_all(squish_text, adj_row_pattern)) |> 
    unnest(matches) |>
    extract(
      matches,
      into = c("adj_type", "adj_amount"),
      regex = adj_row_pattern,
      convert = FALSE
    )
  
  # test <- adj_extracted |>
  #   count(adj_type, sort = TRUE)
    
  adj_hashed <- adj_extracted |> 
    mutate(
      key_string = paste0(pdf_filename, pdf_page, book_year, dept_id_root, adj_type, adj_amount),
      row_id = map_chr(key_string, ~digest(.x, algo = "xxh3_64"))
    )
  
  # duplicates <- adj_hashed |>
  #   count(key_string) |>
  #   filter(n > 1)
  
  adj_parsed <- adj_hashed |> 
    select(
      row_id,
      pdf_filename,
      pdf_page,
      book_year,
      dept_id_root,
      adj_type,
      adj_amount
    ) |> 
    mutate(
      book_year = as.integer(book_year),
      dept_id_root = as.integer(dept_id_root),
      adj_amount = str_replace_all(adj_amount, "^\\((.*)\\)$", "-\\1"),
      adj_amount = parse_number(adj_amount)
    )
  
  write_parquet(personnel_parsed, file.path(clean_path, "stg/personnel_history.parquet"))
  write_parquet(adj_parsed, file.path(clean_path, "stg/personnel_adj_history.parquet"))
}