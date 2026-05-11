# Database permissions

## Source databases (SQL Server, PostgreSQL, Oracle)

The user only needs read access to the tables referenced by the pipeline's `loads`.

| Database | Permission |
|---|---|
| SQL Server | `SELECT` on the queried tables |
| PostgreSQL | `SELECT` on the queried tables |
| Oracle | `SELECT` on the queried tables |

## Destination database (PostgreSQL)

| Permission | When |
|---|---|
| `CREATE` | Always — creates the destination table |
| `DROP` | Only with `if_exists: replace` (default) |
| `INSERT` | Always — bulk load via `COPY FROM STDIN` |
| `SELECT` | Always — post-load row count check |

`UPDATE`, `DELETE`, `ALTER`, and `TRUNCATE` are **not required** — OmniQuery recreates the destination table from scratch under the default `replace` mode.
