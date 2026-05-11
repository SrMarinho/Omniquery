# Development

## Setup

```bash
uv sync --group dev
uv run pre-commit install
```

Pre-commit hooks: Ruff (lint + format), Mypy, trailing whitespace, end-of-file.

## Quality gates

```bash
uv run ruff check . --fix     # lint + auto-fix
uv run ruff format .          # format
uv run mypy src/ cli/ main.py # static types
```

Lint config (`pyproject.toml` → `[tool.ruff.lint]`):

- Enabled rule families: `E`, `W`, `F`, `I`, `UP`
- `E501` (line too long) is ignored — the formatter owns line length at 120

Mypy config:

- `python_version = "3.12"`, `check_untyped_defs = true`, `warn_return_any = true`
- Missing-imports are ignored for `duckdb.*`, `oracledb.*`, `pyodbc.*`, `psycopg2.*`

## Schema validation

```bash
uv run python scripts/validate_pipelines.py
```

Walks `pipelines/*.yml`, validates each against `schemas/pipeline.schema.json`, prints `[OK]` / `[FAIL]` per file. Exit code is non-zero when any pipeline fails.

## Commit message convention

Use Conventional Commits with an optional scope:

| Type | Use for | Example |
|---|---|---|
| `feat` | New feature | `feat(loader): add Oracle wallet support` |
| `fix` | Bug fix | `fix(cli): respect --dry-run when params are missing` |
| `refactor` | Refactor without behavior change | `refactor(output): extract type mapping helper` |
| `test` | Tests only | `test(entities): cover OutputFactory.create fallbacks` |
| `chore` | Maintenance | `chore: bump duckdb to 1.4.4` |
| `docs` | Docs only | `docs: split README into docs/` |
| `style` | Formatting | `style: ruff format` |
| `perf` | Performance | `perf(loader): drop unused Arrow timezone cast` |
| `ci` | CI changes | `ci: switch dependabot to monthly` |
| `build` | Build system | `build: migrate to uv` |
| `revert` | Revert a prior commit | `revert: feat(loader): add Oracle wallet support` |
