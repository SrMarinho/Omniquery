"""
Testes da simulação Oracle (sem dependências externas).

Valida que o schema sintético é compatível com as queries do pipeline
divergencia_pbs_snr.yaml. Estes testes sempre rodam — não precisam de
credenciais de banco real.

Execução:
    uv run pytest tests/homolog/test_oracle_sim.py -v -s
"""

import time

import duckdb

from tests.homolog.conftest import ResourceMonitor, print_homolog_results
from tests.homolog.oracle_sim.schema import build_oracle_sim, register_oracle_sim_tables


def test_oracle_sim_cria_tabelas() -> None:
    """build_oracle_sim() deve criar E140NFV e E440NFC com linhas corretas."""
    n = 1_000
    con = build_oracle_sim(n_rows=n)

    count_nfv = con.execute("SELECT COUNT(*) FROM E140NFV").fetchone()[0]  # type: ignore[index]
    count_nfc = con.execute("SELECT COUNT(*) FROM E440NFC").fetchone()[0]  # type: ignore[index]

    assert count_nfv == n, f"E140NFV: esperado {n}, obtido {count_nfv}"
    assert count_nfc == n, f"E440NFC: esperado {n}, obtido {count_nfc}"


def test_oracle_sim_schema_e140nfv() -> None:
    """E140NFV deve ter exatamente as colunas usadas pelo pipeline."""
    con = build_oracle_sim(n_rows=10)
    cols = {row[0].upper() for row in con.execute("DESCRIBE E140NFV").fetchall()}

    esperadas = {"CODEMP", "CODFIL", "DATEMI", "NUMNFV", "VLRBPR", "DATGER", "VLRLIQ"}
    assert esperadas.issubset(cols), f"Colunas faltando em E140NFV: {esperadas - cols}"


def test_oracle_sim_schema_e440nfc() -> None:
    """E440NFC deve ter exatamente as colunas usadas pelo pipeline."""
    con = build_oracle_sim(n_rows=10)
    cols = {row[0].upper() for row in con.execute("DESCRIBE E440NFC").fetchall()}

    esperadas = {"CODEMP", "CODFIL", "DATENT", "NUMNFC", "VLRBPR", "DATGER", "VLRLIQ"}
    assert esperadas.issubset(cols), f"Colunas faltando em E440NFC: {esperadas - cols}"


def test_oracle_sim_register_tables() -> None:
    """register_oracle_sim_tables() deve criar vendas_senior e compras_senior no target."""
    sim_con = build_oracle_sim(n_rows=500)
    target_con = duckdb.connect(":memory:")

    register_oracle_sim_tables(sim_con, target_con)

    vendas_count = target_con.execute("SELECT COUNT(*) FROM vendas_senior").fetchone()[0]  # type: ignore[index]
    compras_count = target_con.execute("SELECT COUNT(*) FROM compras_senior").fetchone()[0]  # type: ignore[index]

    assert vendas_count == 500
    assert compras_count == 500


def test_oracle_sim_join_divergencia_saidas() -> None:
    """
    Executa a query exata de divergencia_saidas do pipeline contra a simulação.
    Valida que o schema é compatível com o JOIN definido no YAML.
    """
    sim_con = build_oracle_sim(n_rows=1_000)
    target_con = duckdb.connect(":memory:")

    register_oracle_sim_tables(sim_con, target_con)

    # Cria nf_faturamento sintético com colunas compatíveis com o pipeline
    target_con.execute("""
        CREATE TABLE nf_faturamento AS
        SELECT
            i::INTEGER                                   AS unidade,
            'Empresa ' || i                              AS nome,
            ((i % 5) + 1)::INTEGER                      AS codigo_integracao,
            ((i % 16) + 1)::INTEGER                     AS filial_senior,
            i::INTEGER                                   AS codigo_cliente,
            'NF' || lpad(i::VARCHAR, 10, '0')            AS nota_fiscal,
            (DATE '2024-01-01' + (i % 365)::INTEGER)     AS data_emissao,
            (DATE '2024-01-01' + (i % 365)::INTEGER)     AS data_autorizacao,
            'CHAVE' || i                                 AS chave_nfe,
            (random() * 100000)::DECIMAL(12, 2)          AS total_produtos,
            (random() * 120000)::DECIMAL(12, 2)          AS total_liquido
        FROM range(1, 1001) t(i)
    """)

    target_con.execute("""
        CREATE TABLE nf_compras_devolucoes AS
        SELECT * FROM nf_faturamento LIMIT 0
    """)

    # Query exata do pipeline (divergencia_saidas)
    divergencia_saidas_query = """
        SELECT A.*, B.*
        FROM nf_faturamento A
        LEFT JOIN vendas_senior B
            ON B.CODEMP = A.codigo_integracao
            AND B.CODFIL = A.filial_senior
            AND B.NUMNFV = A.nota_fiscal
            AND B.DATEMI = A.data_emissao
        WHERE B.NUMNFV IS NULL

        UNION ALL

        SELECT A.*, B.*
        FROM nf_compras_devolucoes A
        LEFT JOIN vendas_senior B
            ON B.CODEMP = A.codigo_integracao
            AND B.CODFIL = A.filial_senior
            AND B.NUMNFV = A.nota_fiscal
            AND B.DATEMI = A.data_emissao
        WHERE B.NUMNFV IS NULL
    """

    result = target_con.execute(divergencia_saidas_query).fetchall()
    # Não importa quantas divergências — importa que a query executou sem erro
    assert isinstance(result, list)
    print(f"\n  OK divergencia_saidas: {len(result)} divergencias encontradas na simulacao")


def test_oracle_sim_join_divergencia_entradas() -> None:
    """
    Executa a query exata de divergencia_entradas do pipeline contra a simulação.
    """
    sim_con = build_oracle_sim(n_rows=1_000)
    target_con = duckdb.connect(":memory:")

    register_oracle_sim_tables(sim_con, target_con)

    # Cria nf_compras sintético
    target_con.execute("""
        CREATE TABLE nf_compras AS
        SELECT
            i::INTEGER                                   AS unidade,
            'Empresa ' || i                              AS nome,
            ((i % 5) + 1)::INTEGER                      AS codigo_integracao,
            ((i % 16) + 1)::INTEGER                     AS filial_senior,
            i::INTEGER                                   AS codigo_emitente,
            'NF' || lpad(i::VARCHAR, 10, '0')            AS nota_fiscal,
            (DATE '2024-01-01' + (i % 365)::INTEGER)     AS data_emissao,
            (DATE '2024-01-01' + (i % 365)::INTEGER)     AS data_movimento_contabil,
            CURRENT_TIMESTAMP                            AS ultima_nota_mdlog,
            'CHAVE' || i                                 AS chave_nfe,
            (random() * 100000)::DECIMAL(12, 2)          AS total_produtos,
            (random() * 120000)::DECIMAL(12, 2)          AS total_liquido
        FROM range(1, 1001) t(i)
    """)

    target_con.execute("CREATE TABLE nf_faturamento_devolucoes AS SELECT * FROM nf_compras LIMIT 0")

    divergencia_entradas_query = """
        SELECT A.*, B.*
        FROM nf_compras A
        LEFT JOIN compras_senior B
            ON B.CODEMP = A.codigo_integracao
            AND B.CODFIL = A.filial_senior
            AND B.NUMNFC = A.nota_fiscal
            AND B.DATENT = A.data_movimento_contabil
        WHERE B.NUMNFC IS NULL

        UNION ALL

        SELECT A.*, B.*
        FROM nf_faturamento_devolucoes A
        LEFT JOIN compras_senior B
            ON B.CODEMP = A.codigo_integracao
            AND B.CODFIL = A.filial_senior
            AND B.NUMNFC = A.nota_fiscal
            AND B.DATENT = A.data_movimento_contabil
        WHERE B.NUMNFC IS NULL
    """

    result = target_con.execute(divergencia_entradas_query).fetchall()
    assert isinstance(result, list)
    print(f"\n  OK divergencia_entradas: {len(result)} divergencias encontradas na simulacao")


def test_oracle_sim_performance() -> None:
    """Mede o custo de gerar e registrar a simulação Oracle (referência de overhead)."""
    results = []

    for n_rows in [10_000, 50_000, 100_000]:
        monitor = ResourceMonitor()
        monitor.start()
        t0 = time.perf_counter()

        sim_con = build_oracle_sim(n_rows=n_rows)
        target_con = duckdb.connect(":memory:")
        register_oracle_sim_tables(sim_con, target_con)

        duration = time.perf_counter() - t0
        monitor.stop()

        arrow_mb = sim_con.execute("SELECT * FROM E140NFV").fetch_arrow_table().nbytes / (1024 * 1024)
        mb_s = (arrow_mb * 2) / duration if duration > 0 else 0.0

        results.append(
            (
                f"build_oracle_sim + register ({n_rows:,} rows)",
                duration,
                duration,
                duration,
                mb_s,
                monitor.ram_peak_mb,
                monitor.cpu_mean_pct,
                0.0,
                "E140NFV + E440NFC",
            )
        )

    print_homolog_results("Oracle Simulation — overhead de geração e registro", results)
