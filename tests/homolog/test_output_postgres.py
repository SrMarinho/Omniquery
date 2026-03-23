"""
Teste de carga isolado: DuckDB → PostgreSQL.

Usa dados sintéticos no DuckDB (sem precisar do SQL Server) para medir
exclusivamente o caminho DatabaseOutput._transfer() — Arrow → CSV BytesIO →
COPY FROM STDIN — contra o PostgreSQL real.

Permite variar volume com --rows e número de repetições com --repeat.

Execução:
    uv run pytest tests/homolog/test_output_postgres.py -v -s -m homolog
    uv run pytest tests/homolog/test_output_postgres.py -v -s -m homolog --rows=500000 --repeat=3
"""

import time

import pytest

from src.entities.output import DatabaseOutput
from tests.homolog.conftest import ResourceMonitor, print_homolog_results


@pytest.mark.homolog
def test_output_postgres_sintetico(
    require_postgresql: None,
    fresh_memory_database: object,
    rows: int,
    repeat: int,
) -> None:
    """
    Transfere N linhas sintéticas do DuckDB para o PostgreSQL via COPY FROM STDIN.
    Repete `repeat` vezes e exibe tabela com min/media/max.
    """
    import src.config as cfg

    # Tabela sintética com schema similar aos pipelines reais
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

    results = []
    for run_i in range(repeat):
        # recria a tabela de destino a cada run (replace)
        output.options = {"if_exists": "replace"}

        monitor = ResourceMonitor()
        monitor.start()
        t0 = time.perf_counter()
        output.run()
        duration = time.perf_counter() - t0
        monitor.stop()

        data_mb = cfg.memory_database.execute("SELECT * FROM synth_output").fetch_arrow_table().nbytes / (1024 * 1024)
        mb_s = data_mb / duration if duration > 0 else 0.0

        results.append(
            (
                f"run {run_i + 1}/{repeat}",
                duration,
                duration,
                duration,
                mb_s,
                monitor.ram_peak_mb,
                monitor.cpu_mean_pct,
                monitor.net_sent_mb,
                f"{rows:,} rows | {data_mb:.1f} MB",
            )
        )

    # Calcula agregações para exibir min/media/max
    durations = [r[1] for r in results]
    mb_s_vals = [r[4] for r in results]
    ram_peaks = [r[5] for r in results]
    cpu_avgs = [r[6] for r in results]
    net_vals = [r[7] for r in results]
    data_mb_label = results[0][8]

    summary = [
        (
            f"DuckDB → PostgreSQL COPY FROM STDIN ({rows:,} rows)",
            min(durations),
            sum(durations) / len(durations),
            max(durations),
            sum(mb_s_vals) / len(mb_s_vals),
            max(ram_peaks),
            sum(cpu_avgs) / len(cpu_avgs),
            sum(net_vals) / len(net_vals),
            data_mb_label,
        )
    ]

    print_homolog_results(f"DuckDB → PostgreSQL — {rows:,} rows × {repeat} repetições", summary)

    # Garante que os dados chegaram
    for row_result in results:
        assert row_result[1] > 0, "Duração não pode ser zero"


@pytest.mark.homolog
def test_output_postgres_tipos_variados(
    require_postgresql: None,
    fresh_memory_database: object,
) -> None:
    """
    Verifica que o mapeamento de tipos DuckDB → PostgreSQL funciona
    corretamente para os principais tipos usados nos pipelines.
    """
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

    monitor = ResourceMonitor()
    monitor.start()
    t0 = time.perf_counter()
    output.run()
    duration = time.perf_counter() - t0
    monitor.stop()

    # Verifica que os dados chegaram com os tipos corretos
    config = get_database_config("postgresql")
    engine = create_engine(config["connection_string"])
    with engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM omniquery_homolog_tipos")).fetchone()

    assert row is not None
    assert row[0] == 42  # col_int
    assert row[2] == 3.14  # col_double (aprox)

    print_homolog_results(
        "Mapeamento de tipos DuckDB → PostgreSQL",
        [
            (
                "INTEGER, BIGINT, DOUBLE, DECIMAL, BOOLEAN, VARCHAR, DATE, TIMESTAMP",
                duration,
                duration,
                duration,
                0.0,
                monitor.ram_peak_mb,
                monitor.cpu_mean_pct,
                monitor.net_sent_mb,
                "1 row — validação de tipos",
            )
        ],
    )
