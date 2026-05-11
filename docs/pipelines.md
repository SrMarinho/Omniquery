# Writing pipelines

Pipeline definitions live under `pipelines/` as YAML files. The [YAML (Red Hat)](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml) extension in VS Code enables autocomplete and validation via `schemas/pipeline.schema.json`.

## Anatomy

A pipeline has up to four sections:

| Section | Purpose |
|---|---|
| `name` / `description` | Identification (shown in logs and `--dry-run`) |
| `parameters` | Dynamic values supplied on the CLI as `--name value` |
| `loads` | Sources: each table becomes a DuckDB table |
| `outputs` | Destinations: SQL queries against DuckDB → database/file |

## Full structure

```yaml
name: my_pipeline
description: Optional description

# Parameters declared here become CLI flags substituted into the YAML via {{ name }}.
parameters:
  - name: start_date
    type: date              # string | integer | float | boolean | date
    required: true
    description: Start date (YYYY-MM-DD)

  - name: end_date
    type: date
    required: true
    description: End date (YYYY-MM-DD)

# Data sources — each table becomes a DuckDB table.
loads:
  - type: database
    source: procfit         # name defined in databases.yaml
    tables:
      - alias: sales        # table name in DuckDB
        type: inline        # "inline" (SQL directly) or "sql" (path to a .sql file)
        content: |
          SELECT * FROM ORDERS
          WHERE DATE >= '{{ start_date }}'
          AND DATE <= '{{ end_date }}'

  - type: database
    source: senior
    tables:
      - alias: purchases
        type: inline
        content: SELECT * FROM PURCHASES

  - type: file
    source: data/targets.csv  # CSV or XLSX
    tables:
      - alias: targets

# Destinations — queries over the tables loaded above.
outputs:
  - type: database
    name: sales_report      # destination table in the database
    output_database: postgresql
    query: |
      SELECT s.*, t.target
      FROM sales s
      LEFT JOIN targets t ON s.sector = t.sector
    options:
      if_exists: replace    # "replace" (default) or "append"

  - type: file
    name: outputs/report.xlsx  # extension drives the format: .csv or .xlsx
    query: SELECT * FROM sales
```

## Dynamic parameters

Parameters declared under `parameters:` are exposed as CLI flags (`--name`) and substituted into all string values of the YAML via `{{ name }}` before validation runs.

```bash
uv run main.py run pipelines/sales_report.yml \
    --start_date 2024-01-01 --end_date 2024-12-31
```

Supported parameter types:

| `type:` | CLI parsing |
|---|---|
| `string` | Raw string (default) |
| `integer` / `int` | `int(value)` |
| `float` | `float(value)` |
| `boolean` / `bool` | `true`, `1`, `yes`, `y` → True (case-insensitive); anything else → False |
| `date` | `datetime.strptime(value, "%Y-%m-%d").date()` |

If a parameter is marked `required: true`, the CLI rejects the run when it is missing. If `default:` is set, the parameter is optional and the default value is used when the flag is omitted.

## SQL in a separate file

For long queries, use `type: sql` and point at a `.sql` file:

```yaml
tables:
  - alias: sales
    type: sql
    content: queries/sales.sql
```

`{{ param }}` substitution still applies inside the loaded file content.

## Field reference

All fields are validated by Pydantic models in `src/entities/`. Unknown fields are silently ignored, but the JSON schema flags them in the editor.

### `Parameter` — `src/entities/parameter.py`

| Field | Type | Default | Notes |
|---|---|---|---|
| `name` | str | required | Identifier — must match `^[a-zA-Z_][a-zA-Z0-9_]*$` |
| `type` | str | `"string"` | One of `string`, `integer`, `float`, `boolean`, `date` |
| `description` | str | `""` | Shown in `--params` output |
| `required` | bool | `false` | If true, the CLI errors out when the flag is missing |
| `default` | any | — | Used when the flag is omitted (only when `required: false`) |

### `Table` — `src/entities/table.py`

| Field | Type | Default | Notes |
|---|---|---|---|
| `alias` | str | required | DuckDB table name |
| `description` | str | `""` | Documentation |
| `type` | `TableTypes` | `inline` | `inline` or `sql` |
| `content` | str | `""` | SQL string or path to a `.sql` file |
| `options.if_exists` | str | `"replace"` | `replace` or `append` |

### `Loader` — `src/entities/loader.py`

Concrete types are resolved by `LoaderFactory.create` based on `type`.

`DatabaseLoader`:

| Field | Type | Default |
|---|---|---|
| `type` | str | `"database"` |
| `source` | str | name in `databases.yaml` |
| `database` | str | `"memory"` (target DuckDB connection name) |
| `tables` | list[Table] | `[]` |

`FileLoader`:

| Field | Type | Default |
|---|---|---|
| `type` | str | `"file"` |
| `source` | str | file path (`.csv`, `.xlsx`, `.xls`) |
| `tables` | list[Table] | `[]` (exactly one allowed by the schema) |

### `Output` — `src/entities/output.py`

`DatabaseOutput`:

| Field | Type | Default | Notes |
|---|---|---|---|
| `type` | str | `"database"` | |
| `name` | str | required | Destination table — supports `schema.table` |
| `query` | str | required | SQL run against DuckDB |
| `output_database` | str | `"postgresql"` | Name from `databases.yaml` |
| `options.if_exists` | str | `"replace"` | `replace` drops and recreates; `append` inserts only |

`FileOutput`:

| Field | Type | Default | Notes |
|---|---|---|---|
| `type` | str | `"file"` | |
| `name` | str | required | File path; format inferred from extension (`.csv`, `.xlsx`, `.xls`) |
| `query` | str | required | SQL run against DuckDB |
| `options.if_exists` | str | `"replace"` | `replace` overwrites; `append` only valid for CSV |

## Worked example — `divergencia_pbs_snr.yml`

The repository ships with a real pipeline that joins data from three databases and produces two reconciliation tables.

- **Parameters**: `data_inicio` and `data_fim` (both `date`, both required) — they bound the date range of the SQL queries.
- **Loads**:
  - `procfit` (SQL Server) — four tables: `nf_faturamento`, `nf_faturamento_devolucoes`, `nf_compras`, `nf_compras_devolucoes`.
  - `senior` (Oracle) — two tables: `vendas_senior`, `compras_senior`, sourced from `E140NFV` and `E440NFC`.
- **Outputs**: two PostgreSQL tables — `divergencia_saidas` (sales-side reconciliation) and `divergencia_entradas` (purchase-side reconciliation). Each runs a SQL query that LEFT JOINs the SQL Server data against the Oracle data, keeping only rows where the Oracle side is `NULL`.

Run it:

```bash
uv run main.py run pipelines/divergencia_pbs_snr.yml \
    --data_inicio 2024-01-01 --data_fim 2024-12-31
```

For testing without a real Oracle, see [testing.md](testing.md) — the suite ships a DuckDB-backed Oracle simulation.
