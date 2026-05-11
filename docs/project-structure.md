# Project structure

```
omniquery/
├── cli/
│   └── commands.py              # Typer CLI (list, run, --dry-run, --params)
├── pipelines/                   # Pipeline YAML files
├── schemas/
│   └── pipeline.schema.json     # JSON Schema for VS Code autocomplete and CI validation
├── scripts/
│   └── validate_pipelines.py    # Validate every pipeline against the JSON schema
├── tests/
│   ├── conftest.py              # Shared fixtures (minimal_pipeline_file, --rows, --repeat)
│   ├── unit/                    # Unit tests — no external database
│   │   ├── test_app.py          # App._substitute_parameters
│   │   ├── test_entities.py     # Loader, Output, Pipeline, etc.
│   │   └── test_utils.py        # substitute_env_variables
│   └── e2e/                     # E2E tests against real databases
│       ├── conftest.py          # ResourceMonitor + credential-skip fixtures
│       ├── oracle_sim/          # DuckDB-backed Oracle simulation
│       ├── test_connections.py
│       ├── test_loader_mssql.py
│       ├── test_oracle_sim.py
│       ├── test_output_postgres.py
│       └── test_pipeline_divergencia.py
├── src/
│   ├── app.py                   # Orchestrator (load YAML, substitute params, run pipeline)
│   ├── exceptions.py            # OmniQueryError hierarchy
│   ├── config/
│   │   ├── database.py          # DuckDB in-memory singleton
│   │   ├── logging_config.py    # Rich logging setup
│   │   └── settings.py          # Env-var-backed runtime constants
│   ├── entities/
│   │   ├── loader.py            # DatabaseLoader, FileLoader, LoaderFactory
│   │   ├── output.py            # DatabaseOutput, FileOutput, OutputFactory
│   │   ├── pipeline.py          # Pipeline model + factory hooks
│   │   ├── parameter.py         # Parameter model
│   │   └── table.py             # Table model
│   ├── types/
│   │   └── table_types.py       # TableTypes enum (inline | sql)
│   └── utils/
│       ├── database_config_reader.py  # databases.yaml + {{ENV_VAR}} substitution
│       └── retry.py                   # Tenacity decorator (db_retry)
├── databases.yaml               # Named connection strings
├── main.py                      # Entry point — wires Typer + dotenv + logging
└── pyproject.toml
```
