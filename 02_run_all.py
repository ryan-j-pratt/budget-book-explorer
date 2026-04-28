import subprocess
import os
import yaml
import sys

with open("config.yml") as f:
    config = yaml.safe_load(f)

stg_path = config['locations']['stg_path']
prod_path = config['locations']['prod_path']

print(f"Python is looking at: {stg_path}")
