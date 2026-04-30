{{ config(
    materialized='external',
    format='parquet',
    location=var('output_path') ~ '/' ~ this.name ~ '.parquet'
) }}

WITH base AS (
    SELECT *,
    CASE WHEN offset_years IN (0, 1) THEN 'target' ELSE 'history' END AS val_type,
    FROM {{ ref('approp_account_history_all') }}
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
FULL JOIN best_actual a 
    ON  b.fy = a.fy 
    AND b.dept_id_root = a.dept_id_root 
    AND b.approp_account = a.approp_account