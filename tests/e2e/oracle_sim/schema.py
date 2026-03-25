"""
Simulação do banco Oracle Senior em DuckDB.

Cria as tabelas E140NFV (vendas) e E440NFC (compras) com o schema exato
usado pelo pipeline divergencia_pbs_snr.yaml, populadas com dados sintéticos.

Isso permite testar o pipeline completo sem precisar de uma conexão Oracle real,
substituindo o loader 'senior' por tabelas pré-carregadas no DuckDB.

Schema derivado de pipelines/divergencia_pbs_snr.yaml:
    E140NFV: CODEMP, CODFIL, DATEMI, NUMNFV, VLRBPR, DATGER, VLRLIQ
    E440NFC: CODEMP, CODFIL, DATENT, NUMNFC, VLRBPR, DATGER, VLRLIQ
"""

import duckdb


def build_oracle_sim(n_rows: int = 50_000) -> duckdb.DuckDBPyConnection:
    """
    Retorna uma conexão DuckDB isolada com E140NFV e E440NFC populadas.

    Args:
        n_rows: número de linhas a gerar em cada tabela.
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
    Registra as tabelas do Oracle simulado no memory_database como
    'vendas_senior' e 'compras_senior' — os aliases esperados pelo pipeline.

    Após essa chamada, o loader 'senior' pode ser pulado (no-op) porque as
    tabelas já estão disponíveis no DuckDB para os outputs executarem o JOIN.
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
