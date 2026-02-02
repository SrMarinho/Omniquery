from pathlib import Path
import yaml
from src.config.settings import base_path

def get_database_config(database: str) -> None:
    database_file = "databases.yaml"
    config_file = base_path / database_file
    if not config_file.exists():
        raise FileNotFoundError("File 'databases.yaml' not found in root path")
    with open(config_file, "r", encoding="utf-8") as file: 
        data = yaml.safe_load(file)
        print(data)