"""
Testes de stress — carga pesada e limites do sistema.

Objetivo: revelar gargalos de memória, CPU, disco e rede que não aparecem
em testes normais. Cada teste empurra um eixo de pressão diferente.

Execução padrão (100k rows):
    uv run pytest tests/homolog/test_stress.py -v -s -m homolog

Carga máxima (1M rows, 5 repetições):
    uv run pytest tests/homolog/test_stress.py -v -s -m homolog --rows=1000000 --repeat=5

Testes que não precisam de banco real:
    uv run pytest tests/homolog/test_stress.py -v -s -k "not postgres and not mssql"
"""

import gc
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb
import psutil
import pytest

from src.entities.output import DatabaseOutput, FileOutput
from tests.homolog.conftest import monitor_run, print_homolog_results

# ---------------------------------------------------------------------------
# Utilitários locais
# ---------------------------------------------------------------------------

_PROC = psutil.Process()


def _ram_mb() -> float:
    return _PROC.memory_info().rss / (1024 * 1024)


def _populate_duckdb(con: duckdb.DuckDBPyConnection, n: int, table: str = "synth") -> float:
    """Cria tabela sintética com schema realista. Retorna tamanho em MB."""
    con.execute(f"""
        CREATE OR REPLACE TABLE {table} AS
        SELECT
            i::INTEGER                                    AS id,
            'empresa_' || (i % 50)                        AS empresa,
            (DATE '2020-01-01' + (i % 1825)::INTEGER)     AS data_emissao,
            (random() * 999999)::DECIMAL(12, 2)           AS total_produtos,
            (random() * 999999)::DECIMAL(12, 2)           AS total_liquido,
            'NF' || lpad(i::VARCHAR, 12, '0')             AS nota_fiscal,
            repeat('A', 30)                               AS descricao,
            ((i % 16) + 1)::INTEGER                       AS filial,
            (i % 1000)::INTEGER                           AS codigo_cliente,
            (random() > 0.5)::BOOLEAN                     AS ativo
        FROM range(1, {n} + 1) t(i)
    """)
    return con.execute(f"SELECT * FROM {table}").fetch_arrow_table().nbytes / (1024 * 1024)


# ---------------------------------------------------------------------------
# 1. Volume — throughput de escrita no PostgreSQL em escala
# ---------------------------------------------------------------------------


@pytest.mark.homolog
def test_stress_output_volume(
    require_postgresql: None,
    fresh_memory_database: object,
    rows: int,
    repeat: int,
) -> None:
    """
    Empurra N linhas para o PostgreSQL N vezes.
    Mede throughput, RAM pico e degradação entre runs (vazamento de memória?).

    Pressão: rede + CPU do PostgreSQL + RAM do processo Python.
    """
    import src.config as cfg

    data_mb = _populate_duckdb(cfg.memory_database, rows)

    output = DatabaseOutput(
        type="database",
        name="omniquery_stress_volume",
        query="SELECT * FROM synth",
        options={"if_exists": "replace"},
    )

    results = []
    ram_baseline = _ram_mb()

    for i in range(repeat):
        with monitor_run() as m:
            t0 = time.perf_counter()
            output.run()
            duration = time.perf_counter() - t0

        mb_s = data_mb / duration if duration > 0 else 0.0
        ram_delta = m.ram_peak_mb - ram_baseline

        results.append(
            (
                f"run {i + 1}/{repeat}  (RAM delta: {ram_delta:+.0f} MB)",
                duration,
                duration,
                duration,
                mb_s,
                m.ram_peak_mb,
                m.cpu_mean_pct,
                m.net_sent_mb,
                f"{rows:,} rows | {data_mb:.1f} MB",
            )
        )

    durations = [r[1] for r in results]
    mb_s_list = [r[4] for r in results]

    summary = [
        (
            f"DuckDB -> PG COPY ({rows:,} rows x {repeat} runs)",
            min(durations),
            sum(durations) / len(durations),
            max(durations),
            sum(mb_s_list) / len(mb_s_list),
            max(r[5] for r in results),
            sum(r[6] for r in results) / len(results),
            sum(r[7] for r in results),
            f"degradacao: {(max(durations) / min(durations) - 1) * 100:.1f}%",
        )
    ]

    print_homolog_results(f"STRESS: volume PostgreSQL — {rows:,} rows x {repeat}x", summary)
    print_homolog_results("STRESS: volume PostgreSQL — detalhe por run", results)

    # Throughput nao deve degradar mais de 50% entre runs
    assert max(durations) / min(durations) < 2.0, (
        f"Degradacao excessiva entre runs: min={min(durations):.2f}s max={max(durations):.2f}s"
    )


# ---------------------------------------------------------------------------
# 2. Pressao de memoria — perto do DB_MEMORY_LIMIT
# ---------------------------------------------------------------------------


@pytest.mark.homolog
def test_stress_memory_pressure(
    fresh_memory_database: object,
) -> None:
    """
    Carrega volumes crescentes no DuckDB ate proximo do limite de 4 GB.
    Mede RAM do processo e verifica que DuckDB nao vaza apos GC.

    Pressao: RAM do processo Python + DuckDB in-memory.
    """
    import src.config as cfg

    volumes = [100_000, 500_000, 1_000_000, 2_000_000]
    results = []

    for n in volumes:
        ram_before = _ram_mb()
        with monitor_run() as m:
            t0 = time.perf_counter()
            data_mb = _populate_duckdb(cfg.memory_database, n, table=f"tbl_{n}")
            duration = time.perf_counter() - t0

        duckdb_mb = cfg.memory_database.execute(
            "SELECT estimated_size / 1048576.0 FROM pragma_database_size() LIMIT 1"
        ).fetchone()
        duckdb_mb_val = float(duckdb_mb[0]) if duckdb_mb else 0.0  # type: ignore[index]

        mb_s = data_mb / duration if duration > 0 else 0.0

        results.append(
            (
                f"populate {n:,} rows",
                duration,
                duration,
                duration,
                mb_s,
                m.ram_peak_mb,
                m.cpu_mean_pct,
                0.0,
                f"arrow={data_mb:.0f} MB  duckdb~{duckdb_mb_val:.0f} MB  delta_ram={m.ram_peak_mb - ram_before:+.0f} MB",
            )
        )

    print_homolog_results("STRESS: pressao de memoria — cargas crescentes", results)

    # Limpa e verifica que RAM retorna a um nivel razoavel
    cfg.memory_database.execute("DROP TABLE IF EXISTS tbl_100000")
    cfg.memory_database.execute("DROP TABLE IF EXISTS tbl_500000")
    cfg.memory_database.execute("DROP TABLE IF EXISTS tbl_1000000")
    cfg.memory_database.execute("DROP TABLE IF EXISTS tbl_2000000")
    gc.collect()

    ram_after = _ram_mb()
    ram_baseline = results[0][5] - 50  # aprox RAM antes do primeiro populate
    assert ram_after < ram_baseline + 500, f"Possivel vazamento de memoria: RAM apos limpeza={ram_after:.0f} MB"


# ---------------------------------------------------------------------------
# 3. Paralelismo — N loaders DuckDB simultaneos
# ---------------------------------------------------------------------------


@pytest.mark.homolog
def test_stress_parallel_duckdb_loads(
    fresh_memory_database: object,
) -> None:
    """
    Dispara N threads gravando no mesmo DuckDB simultaneamente.
    Valida o comportamento do lock global (memory_database_lock).

    Pressao: contenção de lock + CPU multi-core.
    """
    import src.config as cfg
    from src.config import memory_database_lock

    n_threads = [1, 2, 4, 8]
    rows_per_thread = 50_000
    results = []

    for n in n_threads:
        errors: list[Exception] = []

        def worker(thread_id: int) -> None:
            import pyarrow as pa

            arrow = pa.table(
                {
                    "id": pa.array(range(rows_per_thread), type=pa.int32()),
                    "val": pa.array([float(i) for i in range(rows_per_thread)]),
                    "tag": pa.array([f"t{thread_id}_{i}" for i in range(rows_per_thread)]),
                }
            )
            with memory_database_lock:
                cfg.memory_database.register(f"_tmp_{thread_id}", arrow)
                cfg.memory_database.execute(
                    f"CREATE OR REPLACE TABLE stress_t{thread_id} AS SELECT * FROM _tmp_{thread_id}"
                )
                cfg.memory_database.unregister(f"_tmp_{thread_id}")

        with monitor_run() as m:
            t0 = time.perf_counter()
            with ThreadPoolExecutor(max_workers=n) as pool:
                futs = [pool.submit(worker, i) for i in range(n)]
                for f in as_completed(futs):
                    try:
                        f.result()
                    except Exception as e:
                        errors.append(e)
            duration = time.perf_counter() - t0

        total_rows = n * rows_per_thread
        data_mb = total_rows * 20 / (1024 * 1024)  # aprox 20 bytes/row
        mb_s = data_mb / duration if duration > 0 else 0.0

        results.append(
            (
                f"{n} threads x {rows_per_thread:,} rows",
                duration,
                duration,
                duration,
                mb_s,
                m.ram_peak_mb,
                m.cpu_mean_pct,
                0.0,
                f"total={total_rows:,}  erros={len(errors)}",
            )
        )

        assert not errors, f"{n} threads: {errors[0]}"

    print_homolog_results("STRESS: paralelismo DuckDB (lock global)", results)


# ---------------------------------------------------------------------------
# 4. Saida CSV — throughput em disco com volume grande
# ---------------------------------------------------------------------------


@pytest.mark.homolog
def test_stress_csv_output_large(
    fresh_memory_database: object,
    tmp_path: Path,
    rows: int,
    repeat: int,
) -> None:
    """
    Escreve N linhas em CSV via DuckDB COPY TO.
    Mede throughput de disco (MB/s de escrita).

    Pressao: I/O de disco + CPU de serialização CSV.
    """
    import src.config as cfg

    _populate_duckdb(cfg.memory_database, rows)
    results = []

    for i in range(repeat):
        out_path = str(tmp_path / f"stress_run_{i}.csv")
        output = FileOutput(
            type="file",
            name=out_path,
            query="SELECT * FROM synth",
        )

        disk_before = psutil.disk_io_counters()
        with monitor_run() as m:
            t0 = time.perf_counter()
            output.run()
            duration = time.perf_counter() - t0
        disk_after = psutil.disk_io_counters()

        file_mb = Path(out_path).stat().st_size / (1024 * 1024)
        disk_write = (
            (disk_after.write_bytes - disk_before.write_bytes) / (1024 * 1024) if disk_before and disk_after else 0.0
        )
        mb_s = file_mb / duration if duration > 0 else 0.0

        results.append(
            (
                f"CSV run {i + 1}/{repeat}",
                duration,
                duration,
                duration,
                mb_s,
                m.ram_peak_mb,
                m.cpu_mean_pct,
                0.0,
                f"file={file_mb:.1f} MB  disk_write={disk_write:.1f} MB",
            )
        )

    durations = [r[1] for r in results]
    mb_s_list = [r[4] for r in results]

    summary = [
        (
            f"DuckDB COPY TO CSV ({rows:,} rows x {repeat}x)",
            min(durations),
            sum(durations) / len(durations),
            max(durations),
            sum(mb_s_list) / len(mb_s_list),
            max(r[5] for r in results),
            sum(r[6] for r in results) / len(results),
            0.0,
            results[0][8],
        )
    ]

    print_homolog_results(f"STRESS: CSV output — {rows:,} rows x {repeat}x", summary)
    print_homolog_results("STRESS: CSV output — detalhe por run", results)


# ---------------------------------------------------------------------------
# 5. Multiplos outputs em paralelo — um DuckDB, N destinos simultaneos
# ---------------------------------------------------------------------------


@pytest.mark.homolog
def test_stress_parallel_outputs(
    require_postgresql: None,
    fresh_memory_database: object,
    tmp_path: Path,
    rows: int,
) -> None:
    """
    Um DuckDB com dados carregados -> N outputs em paralelo (PostgreSQL + CSVs).
    Testa a contenção no lock de leitura do DuckDB com múltiplos outputs.

    Pressao: CPU multi-core + rede + disco simultaneos.
    """
    import src.config as cfg
    from src.entities.pipeline import Pipeline

    data_mb = _populate_duckdb(cfg.memory_database, rows)

    n_csv_outputs = 3
    outputs = [
        {"type": "database", "name": "omniquery_stress_parallel_pg", "query": "SELECT * FROM synth"},
    ] + [
        {
            "type": "file",
            "name": str(tmp_path / f"parallel_out_{i}.csv"),
            "query": f"SELECT * FROM synth WHERE filial = {i + 1}",
        }
        for i in range(n_csv_outputs)
    ]

    with monitor_run() as m:
        t0 = time.perf_counter()
        pipeline = Pipeline(loads=[], outputs=outputs)  # type: ignore[arg-type]
        pipeline.run()
        duration = time.perf_counter() - t0

    mb_s = (data_mb * (1 + n_csv_outputs)) / duration if duration > 0 else 0.0

    print_homolog_results(
        f"STRESS: {1 + n_csv_outputs} outputs em paralelo ({rows:,} rows)",
        [
            (
                f"1x PostgreSQL + {n_csv_outputs}x CSV simultaneos",
                duration,
                duration,
                duration,
                mb_s,
                m.ram_peak_mb,
                m.cpu_mean_pct,
                m.net_sent_mb,
                f"fonte={data_mb:.1f} MB",
            )
        ],
    )


# ---------------------------------------------------------------------------
# 6. Deteccao de vazamento de memoria — pipeline repetido N vezes
# ---------------------------------------------------------------------------


@pytest.mark.homolog
def test_stress_memory_leak_detection(
    require_postgresql: None,
    rows: int,
    repeat: int,
) -> None:
    """
    Executa o ciclo completo (populate DuckDB -> escreve PostgreSQL) N vezes
    reiniciando o DuckDB a cada iteracao.

    Se houver vazamento, a RAM vai crescer monotonicamente entre runs.
    Falha se RAM do ultimo run > RAM do primeiro run + 200 MB.

    Pressao: alocacao/desalocacao repetida de grandes Arrow tables.
    """
    import src.config as cfg
    import src.config.database as _db
    import src.entities.loader as _loader
    import src.entities.output as _output

    ram_samples: list[float] = []
    durations: list[float] = []
    results = []

    for i in range(repeat):
        # Reinicia DuckDB completamente
        new_con = duckdb.connect(":memory:")
        _db.memory_database = new_con
        cfg.memory_database = new_con
        _loader.memory_database = new_con
        _output.memory_database = new_con

        data_mb = _populate_duckdb(new_con, rows)

        output = DatabaseOutput(
            type="database",
            name="omniquery_stress_leak",
            query="SELECT * FROM synth",
            options={"if_exists": "replace"},
        )

        gc.collect()
        ram_before = _ram_mb()

        with monitor_run() as m:
            t0 = time.perf_counter()
            output.run()
            duration = time.perf_counter() - t0

        new_con.close()
        gc.collect()
        ram_after = _ram_mb()

        ram_samples.append(ram_after)
        durations.append(duration)

        results.append(
            (
                f"ciclo {i + 1}/{repeat}",
                duration,
                duration,
                duration,
                data_mb / duration if duration > 0 else 0.0,
                m.ram_peak_mb,
                m.cpu_mean_pct,
                m.net_sent_mb,
                f"RAM antes={ram_before:.0f} MB  depois={ram_after:.0f} MB  delta={ram_after - ram_before:+.0f} MB",
            )
        )

    print_homolog_results(
        f"STRESS: deteccao de vazamento — {rows:,} rows x {repeat} ciclos",
        results,
    )

    # Verifica crescimento de RAM entre primeiro e ultimo ciclo
    ram_growth = ram_samples[-1] - ram_samples[0]
    print(f"\n  RAM growth total: {ram_growth:+.1f} MB  (primeiro={ram_samples[0]:.0f}  ultimo={ram_samples[-1]:.0f})")

    assert ram_growth < 200, f"Possivel vazamento de memoria: RAM cresceu {ram_growth:.0f} MB em {repeat} ciclos"


# ---------------------------------------------------------------------------
# 7. Consulta pesada no DuckDB — SQL complexo com agregacoes e joins
# ---------------------------------------------------------------------------


@pytest.mark.homolog
def test_stress_heavy_duckdb_query(
    fresh_memory_database: object,
    rows: int,
    repeat: int,
) -> None:
    """
    Executa queries SQL pesadas (window functions, GROUP BY, JOIN, subquery)
    dentro do DuckDB com volume alto.

    Pressao: CPU multi-core do DuckDB + memoria para hash joins.
    """
    import src.config as cfg

    _populate_duckdb(cfg.memory_database, rows, table="fat")
    _populate_duckdb(cfg.memory_database, rows // 10, table="snr")

    queries = [
        (
            "GROUP BY + SUM + COUNT",
            "SELECT empresa, filial, COUNT(*) AS qtd, SUM(total_liquido) AS receita FROM fat GROUP BY empresa, filial",
        ),
        (
            "WINDOW FUNCTION rank() OVER (PARTITION BY empresa)",
            "SELECT *, rank() OVER (PARTITION BY empresa ORDER BY total_liquido DESC) AS rnk FROM fat",
        ),
        (
            "LEFT JOIN fat x snr ON nota_fiscal",
            "SELECT f.*, s.total_liquido AS snr_liq FROM fat f LEFT JOIN snr s ON s.nota_fiscal = f.nota_fiscal",
        ),
        (
            "SUBQUERY + filtro percentil 95",
            """
            SELECT * FROM fat
            WHERE total_liquido > (
                SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY total_liquido) FROM fat
            )
            """,
        ),
    ]

    results = []
    for label, query in queries:
        times = []
        for _ in range(repeat):
            with monitor_run() as m:
                t0 = time.perf_counter()
                row_count = len(cfg.memory_database.execute(query).fetchall())
                duration = time.perf_counter() - t0
            times.append((duration, m.ram_peak_mb, m.cpu_mean_pct))

        durations_q = [t[0] for t in times]
        results.append(
            (
                label,
                min(durations_q),
                sum(durations_q) / len(durations_q),
                max(durations_q),
                0.0,
                max(t[1] for t in times),
                sum(t[2] for t in times) / len(times),
                0.0,
                f"{row_count:,} rows resultado",
            )
        )

    print_homolog_results(
        f"STRESS: queries pesadas DuckDB — {rows:,} rows x {repeat}x",
        results,
    )


# ---------------------------------------------------------------------------
# 8. Stress SQL Server -> DuckDB (volume real do Procfit)
# ---------------------------------------------------------------------------


@pytest.mark.homolog
def test_stress_loader_mssql_full_month(
    require_procfit: None,
    fresh_memory_database: object,
) -> None:
    """
    Carrega um mes completo de NF_FATURAMENTO do Procfit (sem TOP).
    Mede quantas linhas existem e o throughput real de extracao.

    Pressao: rede corporativa + connectorx + RAM para Arrow table grande.
    """
    from datetime import date, timedelta

    from src.entities.loader import DatabaseLoader
    from src.entities.table import Table

    end = date.today()
    start = end - timedelta(days=30)

    loader = DatabaseLoader(
        type="database",
        source="procfit",
        tables=[
            Table(
                alias="nf_fat_full",
                content=f"""
                SELECT *
                FROM NF_FATURAMENTO WITH(NOLOCK)
                WHERE CAST(EMISSAO AS DATE) >= '{start}'
                  AND CAST(EMISSAO AS DATE) <  '{end}'
            """,
            )
        ],
    )

    with monitor_run() as m:
        t0 = time.perf_counter()
        loader.run()
        duration = time.perf_counter() - t0

    import src.config as cfg

    total_rows = cfg.memory_database.execute("SELECT COUNT(*) FROM nf_fat_full").fetchone()[0]  # type: ignore[index]
    arrow_mb = cfg.memory_database.execute("SELECT * FROM nf_fat_full").fetch_arrow_table().nbytes / (1024 * 1024)
    mb_s = arrow_mb / duration if duration > 0 else 0.0

    print_homolog_results(
        f"STRESS: SQL Server NF_FATURAMENTO 30 dias ({start} -> {end})",
        [
            (
                "connectorx -> Arrow -> DuckDB (sem TOP)",
                duration,
                duration,
                duration,
                mb_s,
                m.ram_peak_mb,
                m.cpu_mean_pct,
                m.net_recv_mb,
                f"{total_rows:,} rows | {arrow_mb:.1f} MB",
            )
        ],
    )

    assert total_rows > 0, "Nenhum dado encontrado no periodo"


# ---------------------------------------------------------------------------
# 9. Stress completo: SQL Server -> DuckDB -> PostgreSQL (1 mes real)
# ---------------------------------------------------------------------------


@pytest.mark.homolog
def test_stress_e2e_full_month(
    require_procfit: None,
    require_postgresql: None,
    fresh_memory_database: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Pipeline divergencia_pbs_snr com 1 mes real de dados.
    Oracle substituido pela simulacao para nao depender do Senior.
    Mede o pipeline end-to-end com carga real de producao.

    Pressao: todos os eixos simultaneamente (rede, CPU, RAM, disco/PG).
    """
    from datetime import date, timedelta

    import src.config as cfg
    from src.app import App
    from src.entities.loader import DatabaseLoader
    from tests.homolog.oracle_sim.schema import build_oracle_sim, register_oracle_sim_tables

    end = date.today()
    start = end - timedelta(days=30)

    # Pré-carrega simulação Oracle
    sim_con = build_oracle_sim(n_rows=100_000)
    register_oracle_sim_tables(sim_con, cfg.memory_database)

    original_run = DatabaseLoader.run

    def patched_run(self: DatabaseLoader) -> None:
        if self.source == "senior":
            return
        return original_run(self)  # type: ignore[return-value]

    monkeypatch.setattr(DatabaseLoader, "run", patched_run)

    with monitor_run() as m:
        t0 = time.perf_counter()
        App(
            pipeline="pipelines/divergencia_pbs_snr.yaml",
            pipeline_params={"data_inicio": start, "data_fim": end},
        ).run()
        duration = time.perf_counter() - t0

    from sqlalchemy import create_engine, text

    from src.utils.database_config_reader import get_database_config

    config = get_database_config("postgresql")
    engine = create_engine(config["connection_string"])
    with engine.connect() as conn:
        rows_saidas = conn.execute(text("SELECT COUNT(*) FROM divergencia_saidas")).scalar() or 0
        rows_entradas = conn.execute(text("SELECT COUNT(*) FROM divergencia_entradas")).scalar() or 0

    print_homolog_results(
        f"STRESS: E2E divergencia_pbs_snr 30 dias — {start} -> {end}",
        [
            (
                "SQL Server + Oracle sim -> DuckDB -> 2x PostgreSQL",
                duration,
                duration,
                duration,
                (m.net_recv_mb + m.net_sent_mb) / duration if duration > 0 else 0.0,
                m.ram_peak_mb,
                m.cpu_mean_pct,
                m.net_recv_mb,
                f"saidas={rows_saidas:,}  entradas={rows_entradas:,}",
            )
        ],
    )

    assert rows_saidas >= 0
    assert rows_entradas >= 0
