"""
DuckDB-backed simulation of the Senior Oracle database.

Builds tables E140NFV (sales) and E440NFC (purchases) using the exact schema
consumed by the divergencia_pbs_snr pipeline, populated with synthetic data.

This lets the full pipeline run without a real Oracle connection — the
'senior' loader is replaced with pre-loaded DuckDB tables.

Schema derived from pipelines/divergencia_pbs_snr.yml:
    E140NFV: CODEMP, CODFIL, DATEMI, NUMNFV, VLRBPR, DATGER, VLRLIQ
    E440NFC: CODEMP, CODFIL, DATENT, NUMNFC, VLRBPR, DATGER, VLRLIQ
"""

import duckdb


def build_oracle_sim(n_rows: int = 50_000) -> duckdb.DuckDBPyConnection:
    """
    Return an isolated DuckDB connection with E140NFV and E440NFC populated.

    Args:
        n_rows: number of rows to generate in each table.
    """
    con = duckdb.connect(":memory:")

    con.execute(f"""
        CREATE TABLE E140NFV AS
        SELECT
            2                                                AS CODEMP,
            ((i % 16) + 1)::INTEGER                         AS CODFIL,
            (DATE '2024-01-01' + (i % 365)::INTEGER)::DATE  AS DATEMI,
            'NF' || lpad(i::VARCHAR, 10, '0')               AS NUMNFV,
            (random() * 100000)::DECIMAL(15, 2)             AS VLRBPR,
            (DATE '2024-01-01' + (i % 365)::INTEGER)::DATE  AS DATGER,
            (random() * 120000)::DECIMAL(15, 2)             AS VLRLIQ
        FROM range(1, {n_rows} + 1) t(i)
    """)

    con.execute(f"""
        CREATE TABLE E440NFC AS
        SELECT
            2                                                AS CODEMP,
            ((i % 16) + 1)::INTEGER                         AS CODFIL,
            (DATE '2024-01-01' + (i % 365)::INTEGER)::DATE  AS DATENT,
            'NF' || lpad(i::VARCHAR, 10, '0')               AS NUMNFC,
            (random() * 100000)::DECIMAL(15, 2)             AS VLRBPR,
            (DATE '2024-01-01' + (i % 365)::INTEGER)::DATE  AS DATGER,
            (random() * 120000)::DECIMAL(15, 2)             AS VLRLIQ
        FROM range(1, {n_rows} + 1) t(i)
    """)

    return con


def register_oracle_sim_tables(sim_con: duckdb.DuckDBPyConnection, target_con: duckdb.DuckDBPyConnection) -> None:
    """
    Register the simulated Oracle tables in `target_con` as
    'vendas_senior' and 'compras_senior' — the aliases expected by the pipeline.

    After this call, the 'senior' loader can be skipped (no-op) because the
    tables are already present in DuckDB for the outputs to JOIN against.
    """
    mappings = [
        ("vendas_senior", "E140NFV"),
        ("compras_senior", "E440NFC"),
    ]

    for alias, table_name in mappings:
        arrow_table = sim_con.execute(f"SELECT * FROM {table_name}").to_arrow_table()
        target_con.register("_oracle_sim_tmp", arrow_table)
        target_con.execute(f"CREATE OR REPLACE TABLE {alias} AS SELECT * FROM _oracle_sim_tmp")
        target_con.unregister("_oracle_sim_tmp")
        del arrow_table
