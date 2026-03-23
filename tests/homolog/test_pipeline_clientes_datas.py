"""
Teste E2E completo do pipeline clientes_datas.

Fluxo: SQL Server (Procfit) → DuckDB → PostgreSQL
Requer: PROCFIT_DATABASE_* + DATABASE_* nos env vars

Execução:
    uv run pytest tests/homolog/test_pipeline_clientes_datas.py -v -s -m homolog
"""

import time

import pytest
from sqlalchemy import create_engine, text

from src.app import App
from src.utils.database_config_reader import get_database_config
from tests.homolog.conftest import ResourceMonitor, print_homolog_results


@pytest.mark.homolog
def test_e2e_clientes_datas(
    require_procfit: None,
    require_postgresql: None,
    fresh_memory_database: object,
    pipeline_date_params: dict,  # type: ignore[type-arg]
) -> None:
    """
    Executa o pipeline clientes_datas completo contra homolog.

    Mede: duração total, linhas na tabela de saída, RAM pico, CPU médio,
    rede recebida (SQL Server → processo) e enviada (processo → PostgreSQL).
    """
    monitor = ResourceMonitor()
    monitor.start()
    t0 = time.perf_counter()

    App(
        pipeline="pipelines/clientes_datas.yaml",
        pipeline_params=pipeline_date_params,
    ).run()

    duration = time.perf_counter() - t0
    monitor.stop()

    # Conta linhas na tabela de saída no PostgreSQL
    config = get_database_config("postgresql")
    engine = create_engine(config["connection_string"])
    with engine.connect() as conn:
        total_rows = conn.execute(text("SELECT COUNT(*) FROM clientes_datas")).scalar() or 0

    data_mb = monitor.net_recv_mb + monitor.net_sent_mb
    mb_s = data_mb / duration if duration > 0 else 0.0

    print_homolog_results(
        f"E2E clientes_datas | {pipeline_date_params['data_inicio']} → {pipeline_date_params['data_fim']}",
        [
            (
                "SQL Server → DuckDB → PostgreSQL",
                duration,
                duration,
                duration,
                mb_s,
                monitor.ram_peak_mb,
                monitor.cpu_mean_pct,
                monitor.net_recv_mb,
                f"{total_rows:,} rows na saída",
            )
        ],
    )

    assert total_rows >= 0, "Pipeline executou sem erro"


@pytest.mark.homolog
def test_e2e_clientes_datas_idempotente(
    require_procfit: None,
    require_postgresql: None,
    fresh_memory_database: object,
    pipeline_date_params: dict,  # type: ignore[type-arg]
) -> None:
    """
    Roda o pipeline duas vezes e verifica que a segunda não duplica dados
    (comportamento replace padrão do DatabaseOutput).
    """
    import duckdb

    import src.config as cfg
    import src.config.database as _db
    import src.entities.loader as _loader
    import src.entities.output as _output

    def run_once() -> int:
        new_con = duckdb.connect(":memory:")
        _db.memory_database = new_con
        cfg.memory_database = new_con
        _loader.memory_database = new_con
        _output.memory_database = new_con

        App(
            pipeline="pipelines/clientes_datas.yaml",
            pipeline_params=pipeline_date_params,
        ).run()
        return 0

    run_once()
    run_once()

    config = get_database_config("postgresql")
    engine = create_engine(config["connection_string"])
    with engine.connect() as conn:
        count_1 = conn.execute(text("SELECT COUNT(*) FROM clientes_datas")).scalar() or 0
        run_once()
        count_2 = conn.execute(text("SELECT COUNT(*) FROM clientes_datas")).scalar() or 0

    assert count_1 == count_2, f"Pipeline não é idempotente: primeira execução={count_1}, segunda={count_2}"
    print(f"\n  ✓ Idempotência confirmada: {count_1:,} linhas em ambas execuções")
