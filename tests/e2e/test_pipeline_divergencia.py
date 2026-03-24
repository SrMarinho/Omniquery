"""
Teste E2E completo do pipeline divergencia_pbs_snr.

Dois cenários:
  A) Oracle real  — requer PROCFIT + POSTGRESQL + SENIOR
  B) Oracle simulado — requer apenas PROCFIT + POSTGRESQL
     (tabelas Senior pré-carregadas no DuckDB via build_oracle_sim)

Fluxo: SQL Server + Oracle → DuckDB → 2 tabelas PostgreSQL

Execução:
    # Cenário B (sem Oracle real):
    uv run pytest tests/homolog/test_pipeline_divergencia.py -v -s -m homolog

    # Cenário A (com Oracle real):
    uv run pytest tests/homolog/test_pipeline_divergencia.py -v -s -m homolog -k oracle_real
"""

import time

import pytest
from sqlalchemy import create_engine, text

from src.app import App
from src.entities.loader import DatabaseLoader
from src.utils.database_config_reader import get_database_config
from tests.e2e.conftest import ResourceMonitor, print_homolog_results
from tests.e2e.oracle_sim.schema import build_oracle_sim, register_oracle_sim_tables


def _contar_saidas_postgres(tabelas: list[str]) -> dict[str, int]:
    """Conta linhas nas tabelas de saída no PostgreSQL."""
    config = get_database_config("postgresql")
    engine = create_engine(config["connection_string"])
    counts = {}
    with engine.connect() as conn:
        for tabela in tabelas:
            try:
                counts[tabela] = conn.execute(text(f"SELECT COUNT(*) FROM {tabela}")).scalar() or 0
            except Exception:
                counts[tabela] = -1
    return counts


# ---------------------------------------------------------------------------
# Cenário B: Oracle simulado (sempre disponível com Procfit + PostgreSQL)
# ---------------------------------------------------------------------------


@pytest.mark.homolog
def test_e2e_divergencia_oracle_simulado(
    require_procfit: None,
    require_postgresql: None,
    fresh_memory_database: object,
    pipeline_date_params: dict,  # type: ignore[type-arg]
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Executa divergencia_pbs_snr com Oracle substituído por simulação DuckDB.

    O loader 'senior' é interceptado — as tabelas vendas_senior e compras_senior
    são pré-carregadas via build_oracle_sim(), permitindo o teste sem Oracle real.
    """
    import src.config as cfg

    # Pré-carrega simulação Oracle no DuckDB
    n_oracle_rows = 50_000
    sim_con = build_oracle_sim(n_rows=n_oracle_rows)
    register_oracle_sim_tables(sim_con, cfg.memory_database)

    # Monkey-patch: loader 'senior' vira no-op (tabelas já estão no DuckDB)
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

    counts = _contar_saidas_postgres(["divergencia_saidas", "divergencia_entradas"])

    data_mb = monitor.net_recv_mb + monitor.net_sent_mb
    mb_s = data_mb / duration if duration > 0 else 0.0

    print_homolog_results(
        f"E2E divergencia_pbs_snr | Oracle SIMULADO | {pipeline_date_params['data_inicio']} → {pipeline_date_params['data_fim']}",
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
                f"saidas={counts.get('divergencia_saidas', '?'):,}  entradas={counts.get('divergencia_entradas', '?'):,}",
            )
        ],
    )

    assert counts.get("divergencia_saidas", -1) >= 0
    assert counts.get("divergencia_entradas", -1) >= 0


# ---------------------------------------------------------------------------
# Cenário A: Oracle real
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
    Executa divergencia_pbs_snr completo contra os bancos reais de homolog.
    Requer credenciais SENIOR_DATABASE_* além de PROCFIT e DATABASE.
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

    counts = _contar_saidas_postgres(["divergencia_saidas", "divergencia_entradas"])

    data_mb = monitor.net_recv_mb + monitor.net_sent_mb
    mb_s = data_mb / duration if duration > 0 else 0.0

    print_homolog_results(
        f"E2E divergencia_pbs_snr | Oracle REAL | {pipeline_date_params['data_inicio']} → {pipeline_date_params['data_fim']}",
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
                f"saidas={counts.get('divergencia_saidas', '?'):,}  entradas={counts.get('divergencia_entradas', '?'):,}",
            )
        ],
    )

    assert counts.get("divergencia_saidas", -1) >= 0
    assert counts.get("divergencia_entradas", -1) >= 0


# ---------------------------------------------------------------------------
# Teste de consistência: simulado vs real (quando ambos disponíveis)
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
    Compara a contagem de divergências do cenário real vs simulado.
    O cenário simulado usa dados sintéticos do Senior, então as contagens
    serão diferentes — o teste valida apenas que ambos executam sem erro
    e que o real ≥ 0 (critério de sanidade).

    Este teste é útil como baseline: se o número real de divergências
    mudar drasticamente entre runs, pode indicar problema no ambiente.
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

    # Run real
    fresh_db()
    App(pipeline="pipelines/divergencia_pbs_snr.yml", pipeline_params=pipeline_date_params).run()
    counts_real = _contar_saidas_postgres(["divergencia_saidas", "divergencia_entradas"])

    # Run simulado
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
    counts_sim = _contar_saidas_postgres(["divergencia_saidas", "divergencia_entradas"])

    print(
        f"\n  Real:     saidas={counts_real.get('divergencia_saidas'):,}  entradas={counts_real.get('divergencia_entradas'):,}"
    )
    print(
        f"  Simulado: saidas={counts_sim.get('divergencia_saidas'):,}  entradas={counts_sim.get('divergencia_entradas'):,}"
    )

    assert counts_real.get("divergencia_saidas", -1) >= 0
    assert counts_real.get("divergencia_entradas", -1) >= 0
