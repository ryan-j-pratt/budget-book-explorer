# Read a single PDF file

load_single_pdf <- function(pdf_filename) {
  raw <- pdf_text(file.path(pdf_path, pdf_filename)) |>
    as_tibble() |>
    rename(pdf_text = value) |> 
    mutate(
      pdf_filename = basename(pdf_filename),
      pdf_page = row_number()
    )

  raw
}

load_pdf <- function() {
  list_pdfs <- list.files(
    pdf_path,
    recursive = FALSE
  )
  
  concat_pdfs <- map(list_pdfs, load_single_pdf, .progress = TRUE) |>
    list_rbind()
  
  write_rds(
    concat_pdfs,
    file.path(intermediate_path, "raw_pages.rds")
  )
}
