"""Testes de output DuckDB → PostgreSQL."""

import time

import pytest

from src.entities.loader import DatabaseLoader
from src.entities.output import DatabaseOutput
from src.entities.table import Table
from tests.e2e.conftest import print_homolog_results


@pytest.mark.homolog
def test_output_postgres_sintetico(
    require_postgresql: None,
    fresh_memory_database: object,
    rows: int,
    repeat: int,
) -> None:
    """Transfere N linhas sintéticas do DuckDB para o PostgreSQL via COPY FROM STDIN."""
    import src.config as cfg

    cfg.memory_database.execute(f"""
        CREATE OR REPLACE TABLE synth_output AS
        SELECT
            i::INTEGER                                   AS id,
            'empresa_' || (i % 20)                       AS empresa,
            (DATE '2024-01-01' + (i % 365)::INTEGER)     AS data_emissao,
            (random() * 100000)::DECIMAL(12, 2)          AS total_produtos,
            (random() * 120000)::DECIMAL(12, 2)          AS total_liquido,
            'NF' || lpad(i::VARCHAR, 8, '0')             AS nota_fiscal,
            repeat('x', 20)                              AS descricao
        FROM range(1, {rows} + 1) t(i)
    """)

    output = DatabaseOutput(
        type="database",
        name="omniquery_homolog_synth",
        query="SELECT * FROM synth_output",
    )

    durations = []
    for _ in range(repeat):
        output.options = {"if_exists": "replace"}
        t0 = time.perf_counter()
        output.run()
        durations.append(time.perf_counter() - t0)

    data_mb = cfg.memory_database.execute("SELECT * FROM synth_output").to_arrow_table().nbytes / (1024 * 1024)
    avg = sum(durations) / len(durations)
    mb_s = data_mb / avg if avg > 0 else 0.0

    print_homolog_results(
        f"DuckDB → PostgreSQL — {rows:,} rows × {repeat} repetições",
        [
            (
                f"DuckDB → PostgreSQL COPY FROM STDIN ({rows:,} rows)",
                min(durations),
                avg,
                max(durations),
                mb_s,
                0.0,
                0.0,
                0.0,
                f"{rows:,} rows | {data_mb:.1f} MB",
            )
        ],
    )

    assert all(d > 0 for d in durations)


@pytest.mark.homolog
def test_output_postgres_tipos_variados(
    require_postgresql: None,
    fresh_memory_database: object,
) -> None:
    """Verifica que o mapeamento de tipos DuckDB → PostgreSQL funciona corretamente."""
    from sqlalchemy import create_engine, text

    import src.config as cfg
    from src.utils.database_config_reader import get_database_config

    cfg.memory_database.execute("""
        CREATE OR REPLACE TABLE tipos_test AS SELECT
            42::INTEGER                AS col_int,
            9999999999::BIGINT         AS col_bigint,
            3.14::DOUBLE               AS col_double,
            1.23::DECIMAL(10, 2)       AS col_decimal,
            TRUE::BOOLEAN              AS col_bool,
            'texto longo'::VARCHAR     AS col_text,
            CURRENT_DATE::DATE         AS col_date,
            CURRENT_TIMESTAMP::TIMESTAMP AS col_ts
    """)

    output = DatabaseOutput(
        type="database",
        name="omniquery_homolog_tipos",
        query="SELECT * FROM tipos_test",
    )
    output.run()

    config = get_database_config("postgresql")
    engine = create_engine(config["connection_string"])
    with engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM omniquery_homolog_tipos")).fetchone()

    assert row is not None
    assert row[0] == 42
    assert row[2] == 3.14


@pytest.mark.homolog
def test_output_postgres_real_nf_faturamento(
    require_procfit: None,
    require_postgresql: None,
    fresh_memory_database: object,
) -> None:
    """Fluxo real: SQL Server (NF_FATURAMENTO 1k rows) → DuckDB → PostgreSQL."""
    import src.config as cfg

    n = 1_000
    alias = "nf_fat_real"

    loader = DatabaseLoader(
        type="database",
        source="procfit",
        tables=[Table(alias=alias, content=f"SELECT TOP {n} * FROM NF_FATURAMENTO WITH(NOLOCK)")],
    )
    loader.run()

    total_rows = cfg.memory_database.execute(f"SELECT COUNT(*) FROM {alias}").fetchone()[0]  # type: ignore[index]
    assert total_rows == n

    output = DatabaseOutput(
        type="database",
        name="homolog_nf_faturamento",
        query=f"SELECT * FROM {alias}",
        options={"if_exists": "replace"},
    )
    output.run()


@pytest.mark.homolog
def test_output_postgres_real_vs_sintetico(
    require_procfit: None,
    require_postgresql: None,
    fresh_memory_database: object,
) -> None:
    """Compara output real vs sintético (10k rows) para o mesmo volume."""
    import src.config as cfg

    n = 10_000

    # real
    alias_real = "nf_fat_cmp_real"
    DatabaseLoader(
        type="database",
        source="procfit",
        tables=[Table(alias=alias_real, content=f"SELECT TOP {n} * FROM NF_FATURAMENTO WITH(NOLOCK)")],
    ).run()

    t0 = time.perf_counter()
    DatabaseOutput(
        type="database",
        name="homolog_cmp_real",
        query=f"SELECT * FROM {alias_real}",
        options={"if_exists": "replace"},
    ).run()
    out_dur_real = time.perf_counter() - t0

    # sintético
    cfg.memory_database.execute(f"""
        CREATE OR REPLACE TABLE synth_cmp AS
        SELECT
            i::INTEGER                                  AS id,
            'empresa_' || (i % 20)                      AS empresa,
            (DATE '2024-01-01' + (i % 365)::INTEGER)    AS data_emissao,
            (random() * 100000)::DECIMAL(12, 2)         AS total_produtos,
            (random() * 120000)::DECIMAL(12, 2)         AS total_liquido,
            'NF' || lpad(i::VARCHAR, 8, '0')            AS nota_fiscal,
            repeat('x', 20)                             AS descricao
        FROM range(1, {n} + 1) t(i)
    """)

    t0 = time.perf_counter()
    DatabaseOutput(
        type="database",
        name="homolog_cmp_sintetico",
        query="SELECT * FROM synth_cmp",
        options={"if_exists": "replace"},
    ).run()
    out_dur_synth = time.perf_counter() - t0

    assert out_dur_real > 0
    assert out_dur_synth > 0
