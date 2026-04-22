
load_dim_tables <- function() {
  approp_account_label <- read_csv(file.path(manual_path, "approp_account_label.csv"))
  
  approp_account_category_label <- read_csv(file.path(manual_path, "approp_account_category_label.csv"))
  
  flag_personnel_label <- read_csv(file.path(manual_path, "flag_personnel_label.csv"))
  
  dept_id_root_label <- read_csv(file.path(manual_path, "dept_id_root_label.csv"))
  
  dept_id_label <- read_csv(file.path(manual_path, "dept_id_label.csv"))
  
  approp_account_labels <- approp_account_label |> 
    mutate(
      approp_account_category = as.integer(substr(as.character(approp_account), 1, 2)),
      flag_personnel = ifelse(approp_account_category == 51, 1, 0)
    ) |> 
    left_join(approp_account_category_label, by = "approp_account_category") |> 
    left_join(flag_personnel_label, by = "flag_personnel") |> 
    select(
      approp_account, 
      approp_account_category, 
      flag_personnel,
      approp_account_label, 
      approp_account_category_label, 
      flag_personnel_label
    )
  
  write_parquet(approp_account_labels, file.path(clean_path, "stg", "approp_account_labels.parquet"))
  
  dept_id_labels <- dept_id_label |>
    mutate(
      dept_id_root = as.integer(substr(as.character(dept_id), 1, 3))
    ) |> 
    full_join(dept_id_root_label, by = "dept_id_root") |> 
    select(
      dept_id,
      dept_id_root,
      dept_id_label,
      dept_id_root_label
    )
  
  write_parquet(dept_id_labels, file.path(clean_path, "stg", "dept_id_labels.parquet")) 
}
  

