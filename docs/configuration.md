# Configuration

OmniQuery reads configuration from two files:

- `.env` — secrets and tuning knobs (loaded via `python-dotenv`)
- `databases.yaml` — named connection strings (with `{{ENV_VAR}}` substitution)

## `.env`

Copy `.env.example` and edit. Variables fall into three groups:

### Database credentials

| Variable | Purpose |
|---|---|
| `DATABASE_*` | PostgreSQL destination (`USER`, `PASSWORD`, `HOST`, `PORT`, `NAME`) |
| `PROCFIT_DATABASE_*` | SQL Server source (`USER`, `PASSWORD`, `HOST`, `PORT`, `NAME`) |
| `SENIOR_DATABASE_*` | Oracle source (`USER`, `PASSWORD`, `HOST`, `PORT`, `SERVICE_NAME`) |

### Logging

| Variable | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `INFO` | One of `DEBUG`, `INFO`, `WARNING`, `ERROR` |

### Transfer tuning (optional)

| Variable | Default | Description |
|---|---|---|
| `DB_CHUNK_SIZE` | `500000` | Rows per chunk when reading from a database (pandas fallback, used for Oracle) |
| `DB_THREADS` | `4` | DuckDB worker threads |
| `DB_MEMORY_LIMIT` | `4GB` | DuckDB memory limit |
| `PIPELINE_WORKERS` | `4` | Tables loaded in parallel by each loader |
| `PG_WORK_MEM` | `256MB` | `work_mem` per session on the destination PostgreSQL |
| `PG_MAINTENANCE_WORK_MEM` | `1GB` | `maintenance_work_mem` per session on the destination PostgreSQL |
| `ORACLE_ODBC_DRIVER` | `Oracle` | Name of the installed ODBC driver for Oracle |
| `FILE_CHUNK_SIZE` | `100000` | Rows per chunk when writing CSV |
| `DB_BATCH_SIZE` | `500000` | Rows per batch when writing to a database |

## `databases.yaml`

Defines named connection strings. Anything inside `{{ VAR }}` is substituted from the environment.

```yaml
postgresql:
  connection_string: "postgresql://{{DATABASE_USER}}:{{DATABASE_PASSWORD}}@{{DATABASE_HOST}}:{{DATABASE_PORT}}/{{DATABASE_NAME}}"

procfit:
  connection_string: "mssql+pyodbc://{{PROCFIT_DATABASE_USER}}:{{PROCFIT_DATABASE_PASSWORD}}@{{PROCFIT_DATABASE_HOST}}/{{PROCFIT_DATABASE_NAME}}?driver=ODBC+Driver+17+for+SQL+Server"

senior:
  connection_string: "oracle+oracledb://{{SENIOR_DATABASE_USER}}:{{SENIOR_DATABASE_PASSWORD}}@{{SENIOR_DATABASE_HOST}}:{{SENIOR_DATABASE_PORT}}/{{SENIOR_DATABASE_NAME}}"
```

The keys (`postgresql`, `procfit`, `senior`) are the names referenced as `source:` (loaders) and `output_database:` (database outputs) in pipeline YAMLs.

## Two substitution syntaxes — do not confuse them

OmniQuery uses `{{ … }}` for **two different things** in two different files:

| Where | Syntax | Substituted from | Resolved by |
|---|---|---|---|
| `databases.yaml` | `{{VAR_NAME}}` (no spaces required) | OS environment / `.env` | `src/utils/database_config_reader.py::substitute_env_variables` |
| `pipelines/*.yml` | `{{ param_name }}` (spaces optional) | CLI flags declared under `parameters:` | `src/app.py::App._substitute_parameters` |

Both use the same regex (`r"\{\{\s*([^}]+)\s*\}\}"`), but they run at different times and against different value spaces. A missing env var resolves to `{ VAR_NAME }` (single braces); a missing pipeline parameter leaves the `{{ param }}` placeholder in place and logs a warning.
