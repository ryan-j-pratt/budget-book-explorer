# Helper function to extract data

extract_table <- function(data, regex_pattern, col_names) {
  data |>
    mutate(
      value = str_replace_all(value, "[\\h\\v]", " "),
      value = str_replace_all(value, "(?<=\\d), (?=\\d)", ","),
      value = str_squish(value)
    ) |>
    mutate(matches = str_extract_all(value, regex_pattern)) |>
    unnest(matches) |>
    extract(
      matches,
      into = col_names,
      regex = regex_pattern,
      convert = FALSE
    )
}

# Helper function to read raw data
read_raw_data <- function(input_year) {
  rds_path <- file.path(intermediate_path, glue("raw_pdf/fy{input_year}_pages_tagged.rds"))

  raw_data <- read_rds(rds_path)
}

# Read history tables

read_history_tables <- function(input_year) {
  history_pages <- read_raw_data(input_year) |>
    filter(is_history_page)

  row_pattern <- "\\s*(\\d{5})\\s+(.+?)\\s+([-\\d,()]+)\\s+([-\\d,()]+)\\s+([-\\d,()]+)\\s+([-\\d,()]+)\\s+([-\\d,()]+|N/A)"

  col_names <- c(
    "approp_account",
    "description",
    glue("fy{input_year - 3}_expend"),
    glue("fy{input_year - 2}_expend"),
    glue("fy{input_year - 1}_approp"),
    glue("fy{input_year}_adopt"),
    "delta"
  )

  history_tables <- history_pages |>
    extract_table(row_pattern, col_names) |>
    filter(!is.na(approp_account)) |>
    select(-c(delta)) #|>
    #mutate(across(starts_with("fy"), ~ parse_number(.)))

  history_tables_long <- history_tables |>
    pivot_longer(
      cols = starts_with("fy"),
      names_to = "reporting_fy",
      values_to = "amount"
    ) |>
    extract(
      col = "reporting_fy",
      into = c("reporting_fy", "reporting_type"),
      regex = "fy(\\d{4})_(.*)",
      convert = FALSE
    )

  history_tables_long

  write_rds(
    history_tables_long,
    file.path(intermediate_path, glue("raw_history/fy{input_year}_history.rds"))
  )
}

# Read personnel tables

read_personnel_tables <- function(input_year) {
  personnel_pages <- read_raw_data(input_year) |>
    filter(is_personnel_page)

  row_pattern <- "(.*?)\\s+([A-Z0-9]{3})\\s+(?:([A-Z0-9]+)\\s+)?([0-9.]+)\\s+([0-9,]{4,})"
  col_names <- c("title", "union", "grade", "position", "salary")

  personnel_tables <- personnel_pages |>
    extract_table(row_pattern, col_names) |>
    mutate(title = str_squish(title))

  personnel_tables

  write_rds(
    personnel_tables,
    file.path(intermediate_path, glue("raw_personnel/fy{input_year}_personnel.rds"))
  )
}

# Read summary tables

read_summary_tables <- function(input_year) {
  summary_pages <- read_raw_data(input_year) |>
    filter(is_dept_summary_page | is_program_summary_page)
  
  lk <- summary_pages |> 
    filter(dept_name == "Consumer Affairs & Licensing")

  row_pattern <- "(.*?)\\s+([-\\d,()]+)\\s+([-\\d,()]+)\\s+([-\\d,()]+)\\s+([-\\d,()]+)"

  col_names <- c(
    "item",
    glue("fy{input_year - 3}_expend"),
    glue("fy{input_year - 2}_expend"),
    glue("fy{input_year - 1}_approp"),
    glue("fy{input_year}_adopt")
  )

  summary_tables <- summary_pages |>
    extract_table(row_pattern, col_names) |>
    mutate(item = case_when(
      str_detect(item, regex("Non[- ]Personnel$", ignore_case = TRUE)) ~ "Non Personnel",
      str_detect(item, regex("Personnel Services$", ignore_case = TRUE)) ~ "Personnel Services",
      TRUE ~ item 
    )) |>
    filter(item %in% c("Personnel Services", "Non Personnel"))

  summary_tables_long <- summary_tables |>
    pivot_longer(
      cols = starts_with("fy"),
      names_to = "reporting_fy",
      values_to = "amount"
    ) |>
    extract(
      col = "reporting_fy",
      into = c("reporting_fy", "reporting_type"),
      regex = "fy(\\d{4})_(.*)",
      convert = FALSE
    )

  summary_tables_long

  write_rds(
    summary_tables_long,
    file.path(intermediate_path, glue("raw_summary/fy{input_year}_summary.rds"))
  )
}
