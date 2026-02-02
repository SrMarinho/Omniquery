import yaml

def get_database_config(database: str) -> dict:
    with open("databases.yaml", "r", encoding="utf-8") as file: 
        data = yaml.safe_load(file)
    return data