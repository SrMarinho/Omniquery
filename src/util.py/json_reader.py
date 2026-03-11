import json
from typing import Any


def read_json_file(file_path: str) -> dict[str, Any]:
    with open(file_path) as f:
        data: dict[str, Any] = json.load(f)
    return data
