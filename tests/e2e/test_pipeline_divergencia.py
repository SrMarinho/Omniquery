"""
End-to-end test for the divergencia_pbs_snr pipeline.

Two scenarios:
  A) Real Oracle      — requires PROCFIT + POSTGRESQL + SENIOR
  B) Simulated Oracle — requires only PROCFIT + POSTGRESQL
     (Senior tables are pre-loaded into DuckDB via build_oracle_sim)

Flow: SQL Server + Oracle → DuckDB → 2 PostgreSQL tables

Run:
    # Scenario B (no real Oracle):
    uv run pytest tests/e2e/test_pipeline_divergencia.py -v -s -m homolog

    # Scenario A (with real Oracle):
    uv run pytest tests/e2e/test_pipeline_divergencia.py -v -s -m homolog -k oracle_real
"""

import time

import pytest
from sqlalchemy import create_engine, text

from src.app import App
from src.entities.loader import DatabaseLoader
from src.utils.database_config_reader import get_database_config
from tests.e2e.conftest import ResourceMonitor, print_homolog_results
from tests.e2e.oracle_sim.schema import build_oracle_sim, register_oracle_sim_tables


def _count_postgres_outputs(tables: list[str]) -> dict[str, int]:
    """Count rows in the destination PostgreSQL tables."""
    config = get_database_config("postgresql")
    engine = create_engine(config["connection_string"])
    counts = {}
    with engine.connect() as conn:
        for table in tables:
            try:
                counts[table] = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar() or 0
            except Exception:
                counts[table] = -1
    return counts


# ---------------------------------------------------------------------------
# Scenario B: simulated Oracle (always available with Procfit + PostgreSQL).
# ---------------------------------------------------------------------------


@pytest.mark.homolog
def test_e2e_divergencia_oracle_simulated(
    require_procfit: None,
    require_postgresql: None,
    fresh_memory_database: object,
    pipeline_date_params: dict,  # type: ignore[type-arg]
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Run divergencia_pbs_snr with Oracle replaced by a DuckDB-backed simulation.

    The 'senior' loader is intercepted — vendas_senior and compras_senior are
    pre-loaded via build_oracle_sim(), allowing the test to run without a real
    Oracle instance.
    """
    import src.config as cfg

    # Pre-load the Oracle simulation into DuckDB.
    n_oracle_rows = 50_000
    sim_con = build_oracle_sim(n_rows=n_oracle_rows)
    register_oracle_sim_tables(sim_con, cfg.memory_database)

    # Monkey-patch: the 'senior' loader becomes a no-op (tables are already in DuckDB).
    original_run = DatabaseLoader.run

    def patched_run(self: DatabaseLoader) -> None:
        if self.source == "senior":
            return
        return original_run(self)  # type: ignore[return-value]

    monkeypatch.setattr(DatabaseLoader, "run", patched_run)

    monitor = ResourceMonitor()
    monitor.start()
    t0 = time.perf_counter()

    App(
        pipeline="pipelines/divergencia_pbs_snr.yml",
        pipeline_params=pipeline_date_params,
    ).run()

    duration = time.perf_counter() - t0
    monitor.stop()

    counts = _count_postgres_outputs(["divergencia_saidas", "divergencia_entradas"])

    data_mb = monitor.net_recv_mb + monitor.net_sent_mb
    mb_s = data_mb / duration if duration > 0 else 0.0

    print_homolog_results(
        f"E2E divergencia_pbs_snr | SIMULATED Oracle | {pipeline_date_params['data_inicio']} → {pipeline_date_params['data_fim']}",
        [
            (
                "SQL Server → DuckDB + Oracle sim → PostgreSQL",
                duration,
                duration,
                duration,
                mb_s,
                monitor.ram_peak_mb,
                monitor.cpu_mean_pct,
                monitor.net_recv_mb,
                f"out={counts.get('divergencia_saidas', '?'):,}  in={counts.get('divergencia_entradas', '?'):,}",
            )
        ],
    )

    assert counts.get("divergencia_saidas", -1) >= 0
    assert counts.get("divergencia_entradas", -1) >= 0


# ---------------------------------------------------------------------------
# Scenario A: real Oracle.
# ---------------------------------------------------------------------------


@pytest.mark.homolog
def test_e2e_divergencia_oracle_real(
    require_procfit: None,
    require_postgresql: None,
    require_senior: None,
    fresh_memory_database: object,
    pipeline_date_params: dict,  # type: ignore[type-arg]
) -> None:
    """
    Run the full divergencia_pbs_snr pipeline against the real homolog databases.
    Requires SENIOR_DATABASE_* credentials in addition to PROCFIT and DATABASE.
    """
    monitor = ResourceMonitor()
    monitor.start()
    t0 = time.perf_counter()

    App(
        pipeline="pipelines/divergencia_pbs_snr.yml",
        pipeline_params=pipeline_date_params,
    ).run()

    duration = time.perf_counter() - t0
    monitor.stop()

    counts = _count_postgres_outputs(["divergencia_saidas", "divergencia_entradas"])

    data_mb = monitor.net_recv_mb + monitor.net_sent_mb
    mb_s = data_mb / duration if duration > 0 else 0.0

    print_homolog_results(
        f"E2E divergencia_pbs_snr | REAL Oracle | {pipeline_date_params['data_inicio']} → {pipeline_date_params['data_fim']}",
        [
            (
                "SQL Server + Oracle → DuckDB → PostgreSQL",
                duration,
                duration,
                duration,
                mb_s,
                monitor.ram_peak_mb,
                monitor.cpu_mean_pct,
                monitor.net_recv_mb,
                f"out={counts.get('divergencia_saidas', '?'):,}  in={counts.get('divergencia_entradas', '?'):,}",
            )
        ],
    )

    assert counts.get("divergencia_saidas", -1) >= 0
    assert counts.get("divergencia_entradas", -1) >= 0


# ---------------------------------------------------------------------------
# Consistency check: simulated vs real (when both are available).
# ---------------------------------------------------------------------------


@pytest.mark.homolog
def test_divergencia_sim_vs_real_row_count(
    require_procfit: None,
    require_postgresql: None,
    require_senior: None,
    pipeline_date_params: dict,  # type: ignore[type-arg]
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Compare divergence counts between the real and the simulated scenarios.

    The simulated scenario uses synthetic Senior data, so the counts will differ
    — the test only asserts that both runs complete without error and that the
    real count is >= 0 (sanity check).

    Useful as a baseline: if the real divergence count changes drastically
    between runs, it may indicate an environment issue.
    """
    import duckdb

    import src.config as cfg
    import src.config.database as _db
    import src.entities.loader as _loader
    import src.entities.output as _output

    def fresh_db() -> duckdb.DuckDBPyConnection:
        new_con = duckdb.connect(":memory:")
        _db.memory_database = new_con
        cfg.memory_database = new_con
        _loader.memory_database = new_con
        _output.memory_database = new_con
        return new_con

    # Real run.
    fresh_db()
    App(pipeline="pipelines/divergencia_pbs_snr.yml", pipeline_params=pipeline_date_params).run()
    counts_real = _count_postgres_outputs(["divergencia_saidas", "divergencia_entradas"])

    # Simulated run.
    sim_con = build_oracle_sim(n_rows=50_000)
    new_db = fresh_db()
    register_oracle_sim_tables(sim_con, new_db)

    original_run = DatabaseLoader.run

    def patched_run(self: DatabaseLoader) -> None:
        if self.source == "senior":
            return
        return original_run(self)  # type: ignore[return-value]

    monkeypatch.setattr(DatabaseLoader, "run", patched_run)
    App(pipeline="pipelines/divergencia_pbs_snr.yml", pipeline_params=pipeline_date_params).run()
    counts_sim = _count_postgres_outputs(["divergencia_saidas", "divergencia_entradas"])

    print(
        f"\n  Real:      out={counts_real.get('divergencia_saidas'):,}  in={counts_real.get('divergencia_entradas'):,}"
    )
    print(f"  Simulated: out={counts_sim.get('divergencia_saidas'):,}  in={counts_sim.get('divergencia_entradas'):,}")

    assert counts_real.get("divergencia_saidas", -1) >= 0
    assert counts_real.get("divergencia_entradas", -1) >= 0
