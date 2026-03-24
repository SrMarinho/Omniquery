"""
Teste de carga isolado: SQL Server (Procfit) → DuckDB.

Mede exclusivamente o caminho DatabaseLoader._transfer_via_arrow() (connectorx)
sem depender do PostgreSQL de destino. Útil para diagnosticar gargalos na
extração dos dados de origem antes de qualquer transformação.

Execução:
    uv run pytest tests/homolog/test_loader_mssql.py -v -s -m homolog
"""

import time

import pytest

from src.entities.loader import DatabaseLoader
from src.entities.table import Table
from tests.e2e.conftest import ResourceMonitor, print_homolog_results


@pytest.mark.homolog
def test_loader_nf_faturamento(require_procfit: None, fresh_memory_database: object) -> None:
    """
    Carrega amostra de NF_FATURAMENTO do SQL Server no DuckDB.
    Mede: duração, linhas transferidas, MB/s, RAM pico, CPU médio, rede recebida.
    """
    loader = DatabaseLoader(
        type="database",
        source="procfit",
        tables=[
            Table(
                alias="nf_faturamento_sample",
                content="SELECT TOP 10000 * FROM NF_FATURAMENTO WITH(NOLOCK)",
            )
        ],
    )

    monitor = ResourceMonitor()
    monitor.start()
    t0 = time.perf_counter()
    loader.run()
    duration = time.perf_counter() - t0
    monitor.stop()

    import src.config as cfg

    total_rows = cfg.memory_database.execute("SELECT COUNT(*) FROM nf_faturamento_sample").fetchone()[0]  # type: ignore[index]
    data_bytes = cfg.memory_database.execute("SELECT * FROM nf_faturamento_sample").fetch_arrow_table().nbytes
    data_mb = data_bytes / (1024 * 1024)
    mb_s = data_mb / duration if duration > 0 else 0.0

    assert total_rows == 10000, f"Esperado 10000 linhas, obtido {total_rows}"

    print_homolog_results(
        "SQL Server → DuckDB | NF_FATURAMENTO (TOP 10k)",
        [
            (
                "connectorx → Arrow → DuckDB",
                duration,
                duration,
                duration,
                mb_s,
                monitor.ram_peak_mb,
                monitor.cpu_mean_pct,
                monitor.net_recv_mb,
                f"{total_rows:,} rows | {data_mb:.1f} MB",
            )
        ],
    )


@pytest.mark.homolog
def test_loader_nf_compra(require_procfit: None, fresh_memory_database: object) -> None:
    """Carrega amostra de NF_COMPRA do SQL Server no DuckDB."""
    loader = DatabaseLoader(
        type="database",
        source="procfit",
        tables=[
            Table(
                alias="nf_compra_sample",
                content="SELECT TOP 10000 * FROM NF_COMPRA WITH(NOLOCK)",
            )
        ],
    )

    monitor = ResourceMonitor()
    monitor.start()
    t0 = time.perf_counter()
    loader.run()
    duration = time.perf_counter() - t0
    monitor.stop()

    import src.config as cfg

    total_rows = cfg.memory_database.execute("SELECT COUNT(*) FROM nf_compra_sample").fetchone()[0]  # type: ignore[index]
    data_bytes = cfg.memory_database.execute("SELECT * FROM nf_compra_sample").fetch_arrow_table().nbytes
    data_mb = data_bytes / (1024 * 1024)
    mb_s = data_mb / duration if duration > 0 else 0.0

    assert total_rows > 0

    print_homolog_results(
        "SQL Server → DuckDB | NF_COMPRA (TOP 10k)",
        [
            (
                "connectorx → Arrow → DuckDB",
                duration,
                duration,
                duration,
                mb_s,
                monitor.ram_peak_mb,
                monitor.cpu_mean_pct,
                monitor.net_recv_mb,
                f"{total_rows:,} rows | {data_mb:.1f} MB",
            )
        ],
    )


@pytest.mark.homolog
def test_loader_multiplas_tabelas_paralelo(require_procfit: None, fresh_memory_database: object) -> None:
    """
    Carrega múltiplas tabelas em paralelo (ThreadPoolExecutor interno do DatabaseLoader).
    Mede o tempo total e o overhead do paralelismo.
    """
    loader = DatabaseLoader(
        type="database",
        source="procfit",
        tables=[
            Table(alias="nf_fat", content="SELECT TOP 5000 * FROM NF_FATURAMENTO WITH(NOLOCK)"),
            Table(alias="nf_cmp", content="SELECT TOP 5000 * FROM NF_COMPRA WITH(NOLOCK)"),
        ],
    )

    monitor = ResourceMonitor()
    monitor.start()
    t0 = time.perf_counter()
    loader.run()
    duration = time.perf_counter() - t0
    monitor.stop()

    import src.config as cfg

    rows_fat = cfg.memory_database.execute("SELECT COUNT(*) FROM nf_fat").fetchone()[0]  # type: ignore[index]
    rows_cmp = cfg.memory_database.execute("SELECT COUNT(*) FROM nf_cmp").fetchone()[0]  # type: ignore[index]

    assert rows_fat == 5000
    assert rows_cmp == 5000

    print_homolog_results(
        "SQL Server → DuckDB | 2 tabelas em paralelo (5k + 5k)",
        [
            (
                "nf_fat + nf_cmp (ThreadPoolExecutor)",
                duration,
                duration,
                duration,
                0.0,
                monitor.ram_peak_mb,
                monitor.cpu_mean_pct,
                monitor.net_recv_mb,
                f"fat={rows_fat:,} cmp={rows_cmp:,}",
            )
        ],
    )
