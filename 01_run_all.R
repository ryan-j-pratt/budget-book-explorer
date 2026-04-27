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

# Confirm that your wd points to the repository or modify
repo_root <- getwd()

# Create paths to data
data_root <- file.path("~/Budgets")
pdf_path <- file.path(data_root, "Budget Book PDFs")
manual_path <- file.path(data_root, "Manual")
intermediate_path <- file.path(data_root, "Intermediate")
clean_path <- file.path(data_root, "Budget Data")

con <- dbConnect(duckdb::duckdb(), dbdir = ":memory:")

scripts <- list.files(repo_root, "^(0[1-9]|[1-9][0-9]).*\\.R$")

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
