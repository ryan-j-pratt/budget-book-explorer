# Before proceeding, make sure you have PDF copies of past budget books
# If these are missing, use the batch download script to get them

# Load packages

library(tidyverse)
library(pdftools)
library(glue)
library(openxlsx2)
library(digest)
library(nanoparquet)
library(DBI)
library(duckdb)
library(yaml)
library(rvest)

# Confirm that your wd points to the repository or modify
repo_root <- getwd()

# Create paths to data
config <- read_yaml(file.path(repo_root, "config.yml"))

pdf_path <- config$locations$pdf_path
manual_path <- config$locations$manual_inputs_path
intermediate_path <- config$locations$interemdiate_path
clean_path <- config$locations$stg_path

con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

scripts <- list.files(file.path(repo_root, "R"), "^(0[1-9]|[1-9][0-9]).*\\.R$", full.names = TRUE)

walk(scripts, source)

load_pdf()
tag_and_label_raw()
process_approp_account()
process_program()
process_non_discretionary()
process_personnel()
load_dim_tables()
create_tables()
clean_tables()
save_tables()

dbDisconnect(con, shutdown = TRUE)
