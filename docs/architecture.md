# Architecture

OmniQuery is a thin Python orchestrator around DuckDB. The CLI loads a pipeline YAML, validates it with Pydantic, runs all loaders into a single in-memory DuckDB instance, then runs the outputs as SQL queries against that DuckDB.

## Data flow

```
main.py
  └── cli/commands.py (Typer)            ── run subcommand
        └── src/app.py::App
              ├── load YAML
              ├── substitute {{ param }}    (regex over str/dict/list)
              ├── validate via Pydantic Pipeline
              └── pipeline.run()
                    ├── _run_parallel(loads, "loaders")
                    │     └── ThreadPoolExecutor(max_workers=PIPELINE_WORKERS)
                    │           └── DatabaseLoader / FileLoader .run()
                    │                 └── transfer source → DuckDB memory_database
                    └── _run_parallel(outputs, "outputs")
                          └── DatabaseOutput / FileOutput .run()
                                └── DuckDB → destination
```

## Components

### CLI (`cli/commands.py`)

Typer app with two subcommands:

- `list` — lists pipelines under `./pipelines/`.
- `run <pipeline>` — accepts a path/filename, plus `--dry-run` and `--params`. Extra `--name value` flags are parsed against the pipeline's declared `parameters:` (unknown flags raise `BadParameter`; missing required parameters do the same).

The pipeline path is resolved by `_resolve_pipeline_path` against either the working directory or `./pipelines/`.

### App orchestrator (`src/app.py`)

`App.__init__` loads the YAML, recursively substitutes `{{ param }}` placeholders via `_substitute_parameters` (regex `r"\{\{\s*([^}]+)\s*\}\}"`), and builds the `Pipeline` Pydantic model. On `ValidationError`, it logs each field error and raises `PipelineError`.

`App.run` calls `Pipeline.run()` and, in `--dry-run` mode, prints a structured summary instead.

### Pipeline (`src/entities/pipeline.py`)

Pydantic model with two `@model_validator(mode="before")` hooks:

- `create_concrete_loaders` — replaces raw dicts in `loads` with instances built by `LoaderFactory.create`.
- `create_concrete_outputs` — same for `outputs` via `OutputFactory.create`.

`Pipeline.run` calls `_run_parallel(loads, ...)` then `_run_parallel(outputs, ...)`, each using a `ThreadPoolExecutor` bounded by `PIPELINE_WORKERS`.

### Loaders (`src/entities/loader.py`)

`LoaderFactory.create` dispatches on the YAML `type:`:

- `database` → `DatabaseLoader`
- `file` → `FileLoader`
- anything else → base `Loader` (which raises on `.run()`).

`DatabaseLoader._transfer` picks the fastest path available for the source dialect:

| Dialect | Path | Mechanism |
|---|---|---|
| PostgreSQL / SQL Server | `_transfer_via_arrow` | `connectorx.read_sql(..., return_type="arrow")` → DuckDB |
| Oracle | `_transfer_via_turbodbc` | turbodbc cursor → `fetchallarrow()` → DuckDB |
| Oracle (fallback) | `_transfer_via_pandas` | `pandas.read_sql(chunksize=DB_CHUNK_SIZE)` → Arrow → DuckDB |

The Arrow paths strip timezone info from timestamp columns (`field.type.tz`) so DuckDB does not require ICU/tzdata at runtime.

Tables inside one loader run in parallel, again bounded by `PIPELINE_WORKERS`.

### Outputs (`src/entities/output.py`)

`OutputFactory.create` dispatches on `type:` (`database` or `file`).

`DatabaseOutput._transfer`:
1. Open a raw psycopg2 connection on top of the SQLAlchemy engine.
2. `SET synchronous_commit TO OFF`, `SET work_mem`, `SET maintenance_work_mem`.
3. `DROP TABLE IF EXISTS` when `if_exists=replace`.
4. `DESCRIBE SELECT * FROM (<query>) __q` to derive the schema.
5. Map DuckDB types to PostgreSQL types (`_DUCKDB_TO_PG`); `_cast_binary_to_hex` converts binary columns to `\xHEX` strings for `BYTEA` import.
6. `CREATE UNLOGGED TABLE` with the mapped schema.
7. Write the Arrow table as CSV into a `BytesIO` buffer, then `COPY FROM STDIN`. On `ArrowInvalid` (e.g. invalid UTF-8 in SQL Server NVARCHAR), fall back to `pandas.to_csv(errors="replace")`.

`FileOutput._transfer`:
- CSV/XLS via DuckDB's native `COPY ... TO ... (FORMAT CSV, HEADER TRUE)`.
- XLSX via `pandas.DataFrame.to_excel(engine="openpyxl")`.
- Empty results delete the file and log `no data`.

### In-memory DuckDB (`src/config/`)

`src/config/database.py` exports `memory_database = duckdb.connect(":memory:")` — a process-wide singleton. `src/config/__init__.py` also exports `memory_database_lock = threading.Lock()` because DuckDB connections are not safe to share across threads without serialization. Every loader and output that touches `memory_database` does so inside `with memory_database_lock: …`.

`App.run` calls `memory_database.close()` at the end of each run.

### Configuration substitution (`src/utils/database_config_reader.py`)

`get_database_config(name)` reads `databases.yaml`, validates the requested key exists, and calls `substitute_env_variables` to replace `{{ENV_VAR}}` with `os.getenv(ENV_VAR)`. Missing env vars resolve to single-brace `{ENV_VAR}` (intentional — it surfaces in error messages downstream).

### Retry (`src/utils/retry.py`)

`db_retry` wraps engine creation calls (`DatabaseLoader.get_engine`, `DatabaseOutput._get_engine`) with Tenacity:

- 3 attempts
- exponential backoff `min=2`, `max=30`, `multiplier=1`
- logs each retry at `WARNING` via `before_sleep_log`

### Exception hierarchy (`src/exceptions.py`)

```
OmniQueryError
├── ConfigError       # databases.yaml / .env / pipeline YAML problems
├── PipelineError     # pipeline validation
├── LoaderError       # source-side failure (caught and re-raised at App level)
└── OutputError       # destination-side failure
```

`main.py` catches `OmniQueryError` and exits with code 1 after logging the message — anything else propagates and shows a Rich traceback.

## Performance notes

Logs print rows/s and MB/s per table to make diagnosis straightforward:

```
procfit | nf_faturamento          998,621 rows  11.18s  89,295 r/s  12.0 MB/s
```

The Arrow paths are zero-copy from source → DuckDB → PostgreSQL whenever data fits in memory. The only intentional materializations are the pandas chunk fallback for Oracle and the XLSX export.

## Tech stack

| Library | Use |
|---|---|
| [DuckDB](https://duckdb.org/) | In-memory analytical engine |
| [connectorx](https://github.com/sfu-db/connector-x) | Fast SQL Server/PostgreSQL → Arrow reads |
| [turbodbc](https://turbodbc.readthedocs.io/) | Fast Oracle (ODBC) → Arrow reads |
| [PyArrow](https://arrow.apache.org/docs/python/) | Zero-copy data transfer |
| [SQLAlchemy](https://www.sqlalchemy.org/) | Engine / URL handling |
| [psycopg2](https://www.psycopg.org/) | `COPY FROM STDIN` to PostgreSQL |
| [Pydantic](https://docs.pydantic.dev/) | Pipeline validation |
| [Pandas](https://pandas.pydata.org/) | Oracle chunk fallback, XLSX writer |
| [Typer](https://typer.tiangolo.com/) | CLI |
| [Rich](https://github.com/Textualize/rich) | Coloured CLI and logging |
| [Tenacity](https://tenacity.readthedocs.io/) | Retry with exponential backoff |
| [uv](https://docs.astral.sh/uv/) | Package manager |
| [Ruff](https://docs.astral.sh/ruff/) | Linter and formatter |
| [Mypy](https://mypy-lang.org/) | Static type checker |
| [pytest](https://pytest.org/) | Test framework |
