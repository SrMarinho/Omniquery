# Testing

## Unit tests

CI default. No external dependencies.

```bash
uv run pytest tests/unit/ -v
uv run pytest tests/unit/test_app.py::TestSubstituteParameters -v   # one class
uv run pytest tests/unit/test_app.py::TestSubstituteParameters::test_substitutes_simple_string -v
```

`pyproject.toml` sets `addopts = "--ignore=tests/e2e --import-mode=importlib"` and `testpaths = ["tests/unit"]`, so plain `uv run pytest` only runs unit tests.

## E2E tests

Live in `tests/e2e/`. They hit real databases and are isolated from the default CI run.

```bash
# Full suite — requires credentials in .env
uv run pytest tests/e2e/ -v -s -m homolog

# Tests that do not need an external database (Oracle simulated)
uv run pytest tests/e2e/ -v -s -k "not postgres and not mssql"
```

Tests are skipped automatically when the matching credential set is missing. Skip fixtures (`tests/e2e/conftest.py`):

| Fixture | Requires |
|---|---|
| `require_procfit` | `PROCFIT_DATABASE_*` env vars |
| `require_postgresql` | `DATABASE_*` env vars |
| `require_senior` | `SENIOR_DATABASE_*` env vars |

What each E2E file needs:

| File | Requirements |
|---|---|
| `test_connections.py` | Procfit / PostgreSQL / Oracle (independent probes) |
| `test_loader_mssql.py` | Procfit (SQL Server) |
| `test_output_postgres.py` | PostgreSQL (some tests also need Procfit) |
| `test_oracle_sim.py` | None — Oracle simulated through DuckDB |
| `test_pipeline_divergencia.py` | Procfit + PostgreSQL (Oracle real or simulated) |

## Useful flags

| Flag | Default | Effect |
|---|---|---|
| `--rows=N` | `500000` | Synthetic rows in output benchmarks |
| `--repeat=N` | `3` | Repetitions per benchmark |

Both are registered in `tests/conftest.py` and consumed by `tests/e2e/conftest.py` fixtures `rows` and `repeat`.

## Oracle simulation

`tests/e2e/oracle_sim/schema.py` ships a DuckDB-backed simulation of the Senior Oracle database. It exposes:

- `build_oracle_sim(n_rows=...)` — returns a fresh in-memory DuckDB connection with `E140NFV` (sales) and `E440NFC` (purchases) populated with synthetic rows that match the columns consumed by `divergencia_pbs_snr.yml`.
- `register_oracle_sim_tables(sim_con, target_con)` — copies those tables into the target DuckDB connection under the aliases `vendas_senior` and `compras_senior` — the same aliases the pipeline expects.

The pipeline tests use this together with a `monkeypatch` that turns the `senior` `DatabaseLoader.run` into a no-op:

```python
def patched_run(self):
    if self.source == "senior":
        return
    return original_run(self)

monkeypatch.setattr(DatabaseLoader, "run", patched_run)
```

This lets the full pipeline run end-to-end (SQL Server → DuckDB → PostgreSQL) without a real Oracle instance.

## ResourceMonitor

`tests/e2e/conftest.py` defines a `ResourceMonitor` that samples RAM, CPU, network, and disk in a background thread:

```python
monitor = ResourceMonitor()
monitor.start()
do_work()
monitor.stop()
print(monitor.ram_peak_mb, monitor.cpu_mean_pct, monitor.net_recv_mb)
```

`print_homolog_results(...)` renders a Rich table summarizing these metrics — used by the loader and pipeline E2E tests.

## Useful fixtures

| Fixture | Where | Purpose |
|---|---|---|
| `minimal_pipeline_file` | `tests/conftest.py` | Writes a one-line valid YAML to `tmp_path` for App constructor tests |
| `fresh_memory_database` | `tests/e2e/conftest.py` | Replaces the global DuckDB singleton between pipeline tests (App.run closes it) |
| `pipeline_date_params` | `tests/e2e/conftest.py` | `{"data_inicio": today-30d, "data_fim": today}` for the divergencia pipeline |
