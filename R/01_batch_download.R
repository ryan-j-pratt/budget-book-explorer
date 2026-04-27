# Use this script to download

library(tidyverse)
library(rvest)
library(glue)
library(purrr)

# Setup
base_url <- "https://www.boston.gov/departments/budget"
download_dir <- "~/Budgets/Budget Book PDFs"

# Fetch and Parse
page <- read_html(base_url)

xpath_selector <- "//*[starts-with(normalize-space(text()), 'Fiscal Year')] | //a[contains(@href, '.pdf')]"

nodes <- page |>
  html_elements(xpath = xpath_selector)

budget_files <- tibble(
    link_text = html_text(nodes, trim = TRUE),
    link_location = html_attr(nodes, "href")
  ) |>
  mutate(
    fy_header = str_extract(
      link_text,
      "(?<=Fiscal Year )\\d{4}(?= (Recommended|Adopted) Budget)")
  ) |>
  fill(fy_header, .direction = "down") |>
  mutate(
    fy_header = glue("fy{fy_header}")
  ) |>
  filter(
    link_text != "Full Budget document",
    !is.na(link_location),
    str_detect(link_location, "(?i)\\.pdf$")
  ) |>
  mutate(
    clean_text = str_remove_all(link_text, "\\'"),
    clean_text = str_replace_all(clean_text, "[^a-zA-Z0-9]+", "_"),
    clean_text = str_to_lower(clean_text)
  ) |>
  group_by(fy_header, clean_text) |> 
  mutate(
    dupe_id = row_number(),  # Counts 1, 2, 3... for each duplicate in the group
    dupe_count = n()             # Counts total duplicates in the group
  ) |>
  ungroup() |>
  mutate(
    clean_filename = ifelse(
      dupe_count > 1,
      paste0(fy_header, "_", clean_text, "_", dupe_id, ".pdf"),
      paste0(fy_header, "_", clean_text, ".pdf")
    ),
    link_location = if_else(
      str_starts(link_location, "http"),
      link_location,
      paste0("https://www.boston.gov", link_location)
    ),
    link_location = str_replace_all(link_location, " ", "%20")
  ) |>
  select(link_location, clean_filename)

budget_files <- budget_files |> 
  filter(str_detect(clean_filename, "fy2027"))

# Batch Download
cat(paste("Found", nrow(budget_files), "PDFs. Starting download...\n"))

download_pdf <- function(link_location, clean_filename) {
  dest_path <- file.path(download_dir, clean_filename)

  if (!file.exists(dest_path)) {
    tryCatch({
      download.file(link_location, dest_path, mode = "wb", quiet = TRUE)
      cat(paste("Downloaded:", clean_filename, "\n"))
    }, error = function(e) {
      cat(paste("Failed:", clean_filename, "-", e$message, "\n"))
    })
  } else {
    cat(paste("Skipping (exists):", clean_filename, "\n"))
  }
}

walk2(budget_files$link_location, budget_files$clean_filename, download_pdf)
