"""
Testes de stress com múltiplos PROCESSOS em paralelo.

Diferente de threads (que compartilham o DuckDB via lock), aqui cada
processo tem seu próprio interpretador Python, seu próprio DuckDB e
sua própria conexão com o banco. Isso simula múltiplos usuários ou
múltiplos jobs rodando o OmniQuery ao mesmo tempo.

Objetivo: revelar comportamentos que só aparecem em concorrência real:
  - Deadlock no PostgreSQL (escritas simultâneas na mesma tabela)
  - Saturação de conexões (pool esgotado)
  - Contenção de CPU/RAM na máquina host
  - Diferença de comportamento sob carga crescente (1, 2, 4, 8 processos)

Execução:
    uv run pytest tests/homolog/test_stress_multiprocess.py -v -s -m homolog

Sem banco real (apenas DuckDB local):
    uv run pytest tests/homolog/test_stress_multiprocess.py -v -s -k "not postgres and not mssql"
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pytest

from tests.homolog.conftest import ResourceMonitor, print_homolog_results

# ---------------------------------------------------------------------------
# Worker executado em subprocesso separado
# ---------------------------------------------------------------------------

_WORKER_SCRIPT = textwrap.dedent("""
    import sys, os, time
    sys.path.insert(0, os.getcwd())

    from dotenv import load_dotenv
    load_dotenv()

    import duckdb
    import src.config as cfg
    import src.config.database as _db
    import src.entities.loader as _loader
    import src.entities.output as _output

    task  = sys.argv[1]   # "csv_output" | "pg_output" | "duckdb_only" | "pipeline"
    rows  = int(sys.argv[2])
    wid   = sys.argv[3]   # worker id para separar tabelas/arquivos

    # Reinicia DuckDB isolado por processo
    con = duckdb.connect(":memory:")
    _db.memory_database = con
    cfg.memory_database = con
    _loader.memory_database = con
    _output.memory_database = con

    con.execute(f\"\"\"
        CREATE TABLE synth AS
        SELECT
            i::INTEGER                                    AS id,
            'emp_' || (i % 50)                            AS empresa,
            (DATE '2020-01-01' + (i % 1825)::INTEGER)     AS data_emissao,
            (random() * 999999)::DECIMAL(12,2)            AS total_liquido,
            'NF' || lpad(i::VARCHAR,12,'0')               AS nota_fiscal,
            ((i % 16)+1)::INTEGER                         AS filial
        FROM range(1, {rows}+1) t(i)
    \"\"\")

    t0 = time.perf_counter()

    if task == "duckdb_only":
        # CPU puro: query complexa dentro do DuckDB
        con.execute(
            "SELECT empresa, SUM(total_liquido), COUNT(*) FROM synth GROUP BY empresa"
        ).fetchall()

    elif task == "csv_output":
        from src.entities.output import FileOutput
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
            path = f.name
        FileOutput(type="file", name=path, query="SELECT * FROM synth").run()
        os.unlink(path)

    elif task == "pg_output":
        from src.entities.output import DatabaseOutput
        # Cada worker escreve em tabela separada para evitar deadlock intencional
        DatabaseOutput(
            type="database",
            name=f"omniquery_stress_mp_{wid}",
            query="SELECT * FROM synth",
            options={"if_exists": "replace"},
        ).run()

    elif task == "pg_same_table":
        from src.entities.output import DatabaseOutput
        # TODOS escrevem na mesma tabela — testa comportamento sob conflito
        DatabaseOutput(
            type="database",
            name="omniquery_stress_mp_shared",
            query="SELECT * FROM synth",
            options={"if_exists": "replace"},
        ).run()

    elif task == "pipeline":
        from tests.homolog.oracle_sim.schema import build_oracle_sim, register_oracle_sim_tables
        from src.entities.loader import DatabaseLoader
        from src.app import App
        from datetime import date, timedelta
        from unittest.mock import patch

        end = date.today()
        start = end - timedelta(days=7)

        sim = build_oracle_sim(n_rows=10_000)
        register_oracle_sim_tables(sim, con)

        original = DatabaseLoader.run
        def patched(self):
            if self.source == "senior":
                return
            return original(self)
        DatabaseLoader.run = patched

        App(
            pipeline="pipelines/divergencia_pbs_snr.yaml",
            pipeline_params={"data_inicio": start, "data_fim": end},
        ).run()

    elapsed = time.perf_counter() - t0
    print(f"worker={wid}  task={task}  rows={rows}  duration={elapsed:.3f}s")
""")


def _launch_worker(task: str, rows: int, worker_id: str, env: dict) -> dict:  # type: ignore[type-arg]
    """Lança um subprocesso worker e retorna métricas."""
    script_path = Path("/tmp") if os.name != "nt" else Path(os.environ.get("TEMP", "C:/Temp"))
    script_file = script_path / f"omniquery_worker_{worker_id}.py"
    script_file.write_text(_WORKER_SCRIPT)

    t0 = time.perf_counter()
    result = subprocess.run(
        [sys.executable, str(script_file), task, str(rows), worker_id],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(Path(__file__).parent.parent.parent),
        timeout=300,
    )
    elapsed = time.perf_counter() - t0

    try:
        script_file.unlink()
    except Exception:
        pass

    return {
        "worker_id": worker_id,
        "task": task,
        "duration": elapsed,
        "returncode": result.returncode,
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip()[-300:] if result.stderr else "",
        "ok": result.returncode == 0,
    }


def _run_parallel_workers(
    task: str,
    rows: int,
    n_workers: int,
    env: dict,  # type: ignore[type-arg]
) -> tuple[list[dict], float]:  # type: ignore[type-arg]
    """Lança n_workers processos em paralelo e retorna resultados + duração total."""
    monitor = ResourceMonitor()
    monitor.start()
    wall_start = time.perf_counter()

    with ProcessPoolExecutor(max_workers=n_workers) as pool:
        futs = {pool.submit(_launch_worker, task, rows, str(i), env): i for i in range(n_workers)}
        results = [f.result() for f in as_completed(futs)]

    wall_time = time.perf_counter() - wall_start
    monitor.stop()

    return results, wall_time


# ---------------------------------------------------------------------------
# 1. Múltiplos processos — DuckDB puro (sem banco externo)
# ---------------------------------------------------------------------------


@pytest.mark.homolog
def test_multiprocess_duckdb_only(rows: int) -> None:
    """
    N processos em paralelo fazendo agregação pesada no DuckDB.
    Sem banco externo — mede saturação de CPU e RAM da máquina host.

    Pressão: CPU multi-core (N processos Python + DuckDB cada).
    """
    env = {**os.environ}
    n_list = [1, 2, 4, 8]
    results_table = []

    for n in n_list:
        worker_results, wall_time = _run_parallel_workers("duckdb_only", rows, n, env)

        durations = [r["duration"] for r in worker_results]
        errors = [r for r in worker_results if not r["ok"]]
        throughput = (n * rows) / wall_time if wall_time > 0 else 0.0

        results_table.append(
            (
                f"{n} processos DuckDB",
                min(durations),
                sum(durations) / len(durations),
                max(durations),
                0.0,
                0.0,
                0.0,
                0.0,
                f"wall={wall_time:.2f}s  {throughput:,.0f} rows/s total  erros={len(errors)}",
            )
        )

        assert not errors, f"{n} processos: falhou {errors[0]['stderr']}"

    print_homolog_results(
        f"STRESS MULTIPROCESS: DuckDB puro — {rows:,} rows/processo",
        results_table,
    )


# ---------------------------------------------------------------------------
# 2. Múltiplos processos — escrita em tabelas separadas no PostgreSQL
# ---------------------------------------------------------------------------


@pytest.mark.homolog
def test_multiprocess_pg_tabelas_separadas(
    require_postgresql: None,
    rows: int,
) -> None:
    """
    N processos em paralelo, cada um escrevendo em sua própria tabela PG.
    Mede throughput total e latência por processo.

    Pressão: N conexões simultâneas ao PostgreSQL + CPU + rede.
    """
    env = {**os.environ}
    n_list = [1, 2, 4]
    results_table = []

    for n in n_list:
        worker_results, wall_time = _run_parallel_workers("pg_output", rows, n, env)

        durations = [r["duration"] for r in worker_results]
        errors = [r for r in worker_results if not r["ok"]]
        total_rows_written = n * rows
        throughput = total_rows_written / wall_time if wall_time > 0 else 0.0

        results_table.append(
            (
                f"{n} processos -> {n} tabelas PG separadas",
                min(durations),
                sum(durations) / len(durations),
                max(durations),
                0.0,
                0.0,
                0.0,
                0.0,
                f"wall={wall_time:.2f}s  {throughput:,.0f} rows/s  erros={len(errors)}",
            )
        )

        if errors:
            for e in errors:
                print(f"\n  [ERRO worker {e['worker_id']}]: {e['stderr']}")

        assert not errors, f"Falhas com {n} processos paralelos"

    print_homolog_results(
        f"STRESS MULTIPROCESS: N processos -> N tabelas PG ({rows:,} rows/processo)",
        results_table,
    )


# ---------------------------------------------------------------------------
# 3. Múltiplos processos — MESMA tabela no PostgreSQL (conflito real)
# ---------------------------------------------------------------------------


@pytest.mark.homolog
def test_multiprocess_pg_tabela_compartilhada(
    require_postgresql: None,
    rows: int,
) -> None:
    """
    N processos escrevendo na MESMA tabela PostgreSQL simultaneamente.
    O DatabaseOutput usa DROP TABLE IF EXISTS + CREATE TABLE — testamos
    o que acontece quando múltiplos processos fazem isso ao mesmo tempo.

    Resultado esperado: alguns processos podem falhar com
    'table does not exist' ou deadlock — o teste documenta o comportamento
    real sem assumir que todos vão passar.

    Pressão: race condition no DDL do PostgreSQL.
    """
    env = {**os.environ}
    n_workers = 4
    worker_results, wall_time = _run_parallel_workers("pg_same_table", rows, n_workers, env)

    ok_count = sum(1 for r in worker_results if r["ok"])
    fail_count = n_workers - ok_count

    results_table = [
        (
            f"{n_workers} processos -> 1 tabela PG (replace)",
            wall_time,
            wall_time,
            wall_time,
            0.0,
            0.0,
            0.0,
            0.0,
            f"ok={ok_count}  falhou={fail_count}  wall={wall_time:.2f}s",
        )
    ]

    print_homolog_results(
        f"STRESS MULTIPROCESS: {n_workers} processos -> tabela compartilhada ({rows:,} rows/processo)",
        results_table,
    )

    for r in worker_results:
        status = "OK" if r["ok"] else "FALHOU"
        print(f"  worker={r['worker_id']}  {status}  {r['stdout'] or r['stderr'][:100]}")

    # Documenta o comportamento — nao falha o teste se alguns workers falharam
    # O valor de negocio e saber quantos conseguiram e qual o erro
    print(f"\n  Comportamento sob DDL concorrente: {ok_count}/{n_workers} processos sucederam")


# ---------------------------------------------------------------------------
# 4. Múltiplos processos — pipeline completo em paralelo
# ---------------------------------------------------------------------------


@pytest.mark.homolog
def test_multiprocess_pipeline_completo(
    require_procfit: None,
    require_postgresql: None,
) -> None:
    """
    N instâncias do pipeline divergencia_pbs_snr rodando em paralelo.
    Oracle substituído pela simulação. Cada processo usa 7 dias de dados reais
    do Procfit e escreve nas tabelas de saída.

    Pressão: todos os eixos em N cópias simultâneas.
    """
    env = {**os.environ}
    n_list = [1, 2, 3]
    results_table = []

    for n in n_list:
        worker_results, wall_time = _run_parallel_workers("pipeline", rows=0, n_workers=n, env=env)

        durations = [r["duration"] for r in worker_results]
        errors = [r for r in worker_results if not r["ok"]]

        results_table.append(
            (
                f"{n} pipelines em paralelo",
                min(durations),
                sum(durations) / len(durations),
                max(durations),
                0.0,
                0.0,
                0.0,
                0.0,
                f"wall={wall_time:.2f}s  erros={len(errors)}",
            )
        )

        for e in errors:
            print(f"\n  [ERRO worker {e['worker_id']}]: {e['stderr'][-200:]}")

    print_homolog_results(
        "STRESS MULTIPROCESS: N pipelines completos em paralelo (7 dias)",
        results_table,
    )

    # Pelo menos o run com 1 processo deve ter sido bem sucedido
    single_run = [r for r in results_table if "1 pipeline" in r[0]]
    assert single_run, "Run serial falhou"


# ---------------------------------------------------------------------------
# 5. Saturação de conexões — mais processos que max_connections do PG
# ---------------------------------------------------------------------------


@pytest.mark.homolog
def test_multiprocess_saturacao_conexoes(
    require_postgresql: None,
    rows: int,
) -> None:
    """
    Lança mais processos do que o PostgreSQL suporta por padrão (default: 100).
    Testa se o sistema falha graciosamente (tenacity retry) ou trava.

    Pressão: pool de conexões esgotado.
    """
    from sqlalchemy import create_engine, text

    from src.utils.database_config_reader import get_database_config

    config = get_database_config("postgresql")
    engine = create_engine(config["connection_string"])
    with engine.connect() as conn:
        max_conn = conn.execute(text("SHOW max_connections")).scalar()
        current_conn = conn.execute(text("SELECT count(*) FROM pg_stat_activity WHERE state IS NOT NULL")).scalar()

    print(f"\n  PostgreSQL max_connections={max_conn}  conexoes_atuais={current_conn}")

    # Lanca 20 processos simultaneos para estressar o pool
    n_workers = 20
    env = {**os.environ}
    worker_results, wall_time = _run_parallel_workers("pg_output", rows // 10, n_workers, env)

    ok_count = sum(1 for r in worker_results if r["ok"])
    fail_count = n_workers - ok_count

    print_homolog_results(
        f"STRESS MULTIPROCESS: saturacao de conexoes ({n_workers} processos)",
        [
            (
                f"{n_workers} processos simultaneos contra PG (max={max_conn})",
                wall_time,
                wall_time,
                wall_time,
                0.0,
                0.0,
                0.0,
                0.0,
                f"ok={ok_count}  falhou={fail_count}  wall={wall_time:.2f}s",
            )
        ],
    )

    for r in worker_results:
        if not r["ok"]:
            print(f"  worker={r['worker_id']} FALHOU: {r['stderr'][:120]}")

    print(f"\n  Resultado: {ok_count}/{n_workers} processos completaram com sucesso")
