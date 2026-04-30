import subprocess
import os
import yaml
import json

with open("config.yml") as f:
    config = yaml.safe_load(f)

stg_path = config['locations']['stg_path']
prod_path = config['locations']['prod_path']

repo_root = os.getcwd()
dbt_path = os.path.join(repo_root, "budget_db")

dbt_vars = {
    "input_path": stg_path,
    "output_path": prod_path
}

dbt_command = [
    "dbt", "run",
    "--vars", json.dumps(dbt_vars),
    "--project-dir", str(dbt_path)
]

subprocess.run(dbt_command)