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