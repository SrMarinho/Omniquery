def read_json_file(file_path: str) -> dict:
    import json
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data