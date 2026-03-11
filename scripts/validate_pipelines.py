"""Valida todos os pipelines YAML em pipelines/ contra o JSON Schema."""

import json
import sys
from pathlib import Path

import jsonschema
import yaml

ROOT = Path(__file__).parent.parent
SCHEMA_FILE = ROOT / "schemas" / "pipeline.schema.json"
PIPELINES_DIR = ROOT / "pipelines"


def main() -> int:
    schema = json.loads(SCHEMA_FILE.read_text(encoding="utf-8"))
    validator = jsonschema.Draft7Validator(schema)

    pipelines = sorted(PIPELINES_DIR.glob("*.yaml"))
    if not pipelines:
        print("Nenhum pipeline encontrado em pipelines/")
        return 0

    errors_found = False
    for pipeline_file in pipelines:
        data = yaml.safe_load(pipeline_file.read_text(encoding="utf-8"))
        errors = list(validator.iter_errors(data))
        if errors:
            errors_found = True
            print(f"[FAIL] {pipeline_file.name}")
            for e in errors:
                path = " > ".join(str(p) for p in e.absolute_path) or "(raiz)"
                print(f"       {path}: {e.message}")
        else:
            print(f"[OK]   {pipeline_file.name}")

    return 1 if errors_found else 0


if __name__ == "__main__":
    sys.exit(main())
