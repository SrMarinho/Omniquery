import os
from functools import lru_cache
from pathlib import Path
import re
import yaml
from src.config.settings import base_path

def get_database_config(database: str) -> dict:
    """
    Get database configuration from 'databases.yaml' in root path
    """
    database_file = "databases.yaml"
    config_file = base_path / database_file
    if not config_file.exists():
        raise FileNotFoundError(f"File {database_file} not found in root path")
    with open(config_file, "r", encoding="utf-8") as file:
        data = yaml.safe_load(file)
        if database not in data:
            raise ValueError(f"Database {database} not found in {database_file}")
        if "connection_string" not in data[database]:
            print(f"connection_string not configured for this source: {database}")
            raise
        data[database]["connection_string"] = substitute_env_variables(data[database]["connection_string"])
        return data[database]

def substitute_env_variables(connection_string):
    """
    Substitui variáveis entre {{ }} pelos valores correspondentes do ambiente
    """
    def replace_match(match):
        env_var = match.group(1)
        return os.getenv(env_var.strip(), f"{{{env_var}}}")
    
    pattern = r'\{\{\s*([^}]+)\s*\}\}'
    
    return re.sub(pattern, replace_match, connection_string)