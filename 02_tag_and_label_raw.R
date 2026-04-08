tag_and_label_raw <- function() {
  raw_pages <- read_rds(file.path(intermediate_path, "raw_pages.rds"))
  
  tagged_pages <- raw_pages |> 
    mutate(
      # Remove leading/trailing whitespace
      squish_text = str_trim(pdf_text),
      # Replace duplicate spaces with single spaces, except newline
      squish_text = str_replace_all(squish_text,"\\h+"," "),
      # 100, 000 -> 100,000
      squish_text = str_replace_all(squish_text, "(?<=\\d), (?=\\d)", ","),
      book_year = str_extract(pdf_filename, "(?<=^fy)\\d{4}"),
      page_type = case_when(
        # These appear verbatim in first line of page
        str_detect(squish_text, "^(Department|Division) History") ~ "approp_account",
        str_detect(squish_text, "^(Department|Division) Personnel") ~ "personnel",
        str_detect(squish_text, "^External Funds History") ~ "external_approp_account",
        str_detect(squish_text, "^External Funds Personnel") ~ "external_personnel",
        str_detect(squish_text, "^External Funds Projects") ~ "external_project",
        # This also appears in the first line of a page
        str_detect(squish_text, "^Program \\d.") ~ "program",
        # These follow the department name on the first line of the page
        str_detect(squish_text, "^.*\\s+Operating\\s+") ~ "dept",
        str_detect(squish_text, "^.*\\s+Capital\\s+") ~ "capital",
        str_detect(squish_text, "^.*\\s+Project\\s+") ~ "project",
        # This can be anywhere on the page and are case insensitive
        str_detect(squish_text, "(?i)Cabinet Mission(?-i)") ~ "cabinet",
        str_detect(squish_text, "(?i)Revenue (Summary|Detail)(?-i)") ~ "revenue",
        TRUE ~ NA_character_
      )
    )
  
  labeled_pages <- tagged_pages |> 
    mutate(
      dept_id_root = if_else(
        page_type == "dept" | page_type == "program",
        # typo "Oragnization" in 2026 book for Human Rights Commission Program 1
        str_extract(squish_text, "(?<=(?:Appropriation|Organization|Oragnization):? )\\d{3}"),
        NA_character_
      )
      # These will move downstream
      # dept_name = if_else(
      #   page_type == "dept",
      #   str_extract(squish_text, "^.+?(?= Operating\\s+Budget)"),
      #   NA_character_
      # ),
      # program_name = if_else(
      #   page_type == "program",
      #   str_extract(squish_text, "(?<=^Program \\d\\. ).+"),
      #   NA_character_
      # )
    ) |> 
    # Fill down within a given pdf
    group_by(pdf_filename) |> 
    fill(page_type, dept_id_root, .direction = "down") |> 
    ungroup()
  
  # Starting in book year 2026, personnel is single col which uses more pages
  # diagnostic <- tagged_pages |> 
  #   group_by(page_type, book_year) |> 
  #   count()
  
  page_types <- labeled_pages |> 
    distinct(page_type) |> 
    filter(!is.na(page_type)) |> 
    pull()
  
  save_pages <- function(page_type) {
    spec_page_type <- page_type
    
    df <- labeled_pages |> 
      filter(page_type == spec_page_type)
    
    write_rds(df, file.path(intermediate_path, "labeled_pages", glue("{spec_page_type}.rds")))
  }
  
  walk(page_types, save_pages)

  write_rds(labeled_pages, file.path(intermediate_path, "labeled_pages.rds"))
}
