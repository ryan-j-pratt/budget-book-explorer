
create_tables <- function() {
  
  create_table <- function(name) {
    file_path <- path.expand(file.path(clean_path, "stg", glue("{name}.parquet")))
    
    view_name <- glue("cc_stg_{name}")
    
    statement <- glue("
      CREATE OR REPLACE VIEW {view_name} AS
        SELECT *
        FROM read_parquet('{file_path}');
    ")
    
    dbExecute(con, statement)
  }
  
  tables <- c(
    "approp_account_history",
    "program_history",
    "non_discretionary_history",
    "dept_id_labels",
    "approp_account_labels"
  )
  
  walk(tables, create_table)
}


clean_tables <- function() {
  
  non_discretionary <- paste(
    c(
      101,
      139,
      148,
      158,
      159,
      199,
      331,
      333,
      341,
      374,
      749,
      999
    ), 
  collapse = ", "
  )
  
  statement_approp_account <- glue("
    CREATE OR REPLACE VIEW cc_budget_approp_account_history AS
    WITH base AS (
      SELECT 
        row_id,
        pdf_filename,
        pdf_page,
        book_year,
        offset_years,
        (book_year - offset_years) AS fy,
        dept_id_root,
        CAST(SUBSTRING(CAST(approp_account AS VARCHAR), 1, 2) AS INTEGER) AS approp_account_category,
        CASE
          WHEN approp_account_category = 51 THEN 1
          ELSE 0
        END AS flag_personnel,
        approp_account,
        amount,
        CASE 
          WHEN offset_years >= 2 THEN 1
          WHEN offset_years = 1 THEN 2
          WHEN offset_years = 0 THEN 3
        END AS quality_score
      FROM cc_stg_approp_account_history
    ),
    
    deduped AS (
      SELECT *
      FROM base
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fy, dept_id_root, approp_account 
        ORDER BY book_year DESC, quality_score ASC, pdf_page DESC
      ) = 1
    )
  
    SELECT 
      fy,
      dept_id_root,
      flag_personnel,
      approp_account_category,
      approp_account,
      amount,
      pdf_filename,
      pdf_page,
      row_id
    FROM deduped
    WHERE amount > 0 AND fy >= 2017 AND dept_id_root NOT IN ({non_discretionary});
  ")
  
  statement_program <- glue("
    CREATE OR REPLACE VIEW cc_budget_program_history AS
    WITH base AS (
      SELECT 
        row_id,
        pdf_filename,
        pdf_page,
        book_year,
        offset_years,
        (book_year - offset_years) AS fy,
        dept_id_root,
        dept_id,
        flag_personnel,
        amount,
        CASE 
          WHEN offset_years >= 2 THEN 1
          WHEN offset_years = 1 THEN 2
          WHEN offset_years = 0 THEN 3
        END AS quality_score
      FROM cc_stg_program_history
    ),
    
    deduped AS (
      SELECT *
      FROM base
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fy, dept_id, flag_personnel 
        ORDER BY book_year DESC, quality_score ASC, pdf_page DESC
      ) = 1
    )
    
    SELECT 
      fy,
      dept_id_root,
      dept_id,
      flag_personnel,
      amount,
      pdf_filename,
      pdf_page,
      row_id
    FROM deduped
    WHERE amount > 0 AND fy >= 2017  AND dept_id_root NOT IN ({non_discretionary});
  ")
  
  statement_non_discretionary <- glue("
    CREATE OR REPLACE VIEW cc_budget_non_discretionary_history AS
    WITH base AS (
      SELECT 
        row_id,
        pdf_filename,
        pdf_page,
        book_year,
        offset_years,
        (book_year - offset_years) AS fy,
        dept_id_root,
        amount,
        CASE 
          WHEN offset_years >= 2 THEN 1
          WHEN offset_years = 1 THEN 2
          WHEN offset_years = 0 THEN 3
        END AS quality_score
      FROM cc_stg_non_discretionary_history
    ),
    
    deduped AS (
      SELECT *
      FROM base
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fy, dept_id_root
        ORDER BY book_year DESC, quality_score ASC, pdf_page DESC
      ) = 1
    )
    
    SELECT 
      fy,
      dept_id_root,
      amount,
      pdf_filename,
      pdf_page,
      row_id
    FROM deduped
    WHERE amount > 0 AND fy >= 2017;
  ")
  
  statement_approp_account_label <- "
    CREATE OR REPLACE VIEW cc_budget_label_approp_account AS
      SELECT * 
      FROM cc_stg_approp_account_labels
  "
  
  statement_approp_account_category_label <- "
    CREATE OR REPLACE VIEW cc_budget_label_approp_account_category AS
      SELECT DISTINCT ON (approp_account_category) approp_account_category, flag_personnel,
        approp_account_category_label, flag_personnel_label
      FROM cc_stg_approp_account_labels
  "
  
  statement_flag_personnel_label <- "
    CREATE OR REPLACE VIEW cc_budget_label_flag_personnel AS
      SELECT DISTINCT ON (flag_personnel) flag_personnel, flag_personnel_label
      FROM cc_stg_approp_account_labels
  "
  
  statement_dept_id_label <- "
    CREATE OR REPLACE VIEW cc_budget_label_dept_id AS
      SELECT *
      FROM cc_stg_dept_id_labels
      WHERE dept_id IS NOT NULL
  "
  
  statement_dept_id_root_label <- "
    CREATE OR REPLACE VIEW cc_budget_label_dept_id_root AS
      SELECT DISTINCT ON (dept_id_root) dept_id_root, dept_id_root_label
      FROM cc_stg_dept_id_labels
  "
  
  statement_approp_account_variance <- glue("
    CREATE OR REPLACE VIEW cc_budget_approp_account_variance AS
    WITH base AS (
      SELECT 
        (book_year - offset_years) AS fy,
        dept_id_root,
        CAST(SUBSTRING(CAST(approp_account AS VARCHAR), 1, 2) AS INTEGER) AS approp_account_category,
        CASE
          WHEN approp_account_category = 51 THEN 1
          ELSE 0
        END AS flag_personnel,
        approp_account,
        amount,
        offset_years,
        CASE WHEN offset_years IN (0, 1) THEN 'target' ELSE 'history' END AS val_type,
        row_id
      FROM cc_stg_approp_account_history
      WHERE (book_year - offset_years) >= 2017
        AND dept_id_root NOT IN ({non_discretionary})
    ),
    best_budget AS (
      SELECT * FROM base 
      WHERE val_type = 'target'
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fy, dept_id_root, approp_account 
        ORDER BY offset_years ASC
      ) = 1
    ),
    best_actual AS (
      SELECT * FROM base 
      WHERE val_type = 'history'
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY fy, dept_id_root, approp_account 
        ORDER BY offset_years DESC
      ) = 1
    )
    
    SELECT 
      b.fy,
      b.dept_id_root,
      b.flag_personnel,
      b.approp_account_category,
      b.approp_account,
      b.amount AS amount_budget,
      b.row_id AS row_id_budget,
      a.amount AS amount_actual,
      a.row_id AS row_id_actual,
      (b.amount - a.amount) AS variance
    FROM best_budget b
    INNER JOIN best_actual a 
      ON  b.fy = a.fy 
      AND b.dept_id_root = a.dept_id_root 
      AND b.approp_account = a.approp_account;
  ")

  dbExecute(con, statement_approp_account)
  dbExecute(con, statement_program)
  dbExecute(con, statement_non_discretionary)
  dbExecute(con, statement_approp_account_label)
  dbExecute(con, statement_approp_account_category_label)
  dbExecute(con, statement_flag_personnel_label)
  dbExecute(con, statement_dept_id_label)
  dbExecute(con, statement_dept_id_root_label)
  dbExecute(con, statement_approp_account_variance)
  
  #lk <- dbGetQuery(con, "SELECT * FROM cc_budget_approp_account_variance")
}

save_tables <- function() {
  
  save_table <- function(name) {
    file_path <- path.expand(file.path(clean_path, "prod", glue("{name}.parquet")))
    
    statement <- glue("
      COPY {name} TO '{file_path}' (FORMAT parquet);
    ")
    
    dbExecute(con, statement)
  }
  
  tables <- c(
    "cc_budget_approp_account_history",
    "cc_budget_program_history",
    "cc_budget_non_discretionary_history",
    "cc_budget_label_approp_account",
    "cc_budget_label_approp_account_category",
    "cc_budget_label_flag_personnel",
    "cc_budget_label_dept_id",
    "cc_budget_label_dept_id_root",
    "cc_budget_approp_account_variance"
  )
  
  walk(tables, save_table)
}
