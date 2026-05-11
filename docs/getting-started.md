# Getting started

## Prerequisites

- **Python 3.12+**
- **[uv](https://docs.astral.sh/uv/)** — package manager
- Database drivers, depending on the sources you target:
  - **SQL Server**: ODBC Driver 17 for SQL Server
  - **Oracle**: Oracle Instant Client + ODBC driver (only required when using `turbodbc`)
  - **PostgreSQL**: `libpq` (typically already installed)

## Install

```bash
git clone <repo-url>
cd omniquery
uv sync                         # runtime dependencies
uv sync --group dev             # add dev dependencies (ruff, mypy, pytest, pre-commit)
```

## First run

1. Copy the example environment file and fill in credentials:

   ```bash
   cp .env.example .env
   ```

   See [configuration.md](configuration.md) for the full variable reference.

2. List the pipelines available in `./pipelines/`:

   ```bash
   uv run main.py list
   ```

3. Run a pipeline:

   ```bash
   uv run main.py run pipelines/divergencia_pbs_snr.yml \
       --data_inicio 2024-01-01 --data_fim 2024-12-31
   ```

## CLI overview

The CLI is built with [Typer](https://typer.tiangolo.com/). Top-level subcommands:

| Command | Purpose |
|---|---|
| `list` | List available pipelines under `./pipelines/` |
| `run <pipeline>` | Run a pipeline. Extra `--name value` flags map to the pipeline's declared parameters |

Flags on `run`:

| Flag | Effect |
|---|---|
| `--dry-run` | Validate and print the pipeline without executing loaders or outputs |
| `--params` | Print the pipeline's accepted parameters and exit |
| `-h`, `--help` | Show usage |

The `<pipeline>` argument accepts an absolute path, a path relative to the working directory, or a bare filename inside `./pipelines/`.

## Examples

```bash
# Show pipeline-specific parameters
uv run main.py run pipelines/divergencia_pbs_snr.yml --params

# Validate without executing
uv run main.py run pipelines/divergencia_pbs_snr.yml --dry-run \
    --data_inicio 2024-01-01 --data_fim 2024-12-31

# Run with parameters using --key=value syntax (also supported)
uv run main.py run pipelines/divergencia_pbs_snr.yml \
    --data_inicio=2024-01-01 --data_fim=2024-12-31
```
