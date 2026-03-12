"""
Benchmarks das operações críticas de transferência de dados.

Execução:
    uv run pytest tests/bench_transfer.py -s -v

Flags úteis:
    --rows=N        tamanho da tabela sintética (default: 500_000)
    --repeat=N      repetições por benchmark (default: 3)
"""

import io
import os
import threading
import time
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.csv as pa_csv
import pytest
from rich.console import Console
from rich.table import Table

console = Console()

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def rows(request: pytest.FixtureRequest) -> int:
    return request.config.getoption("--rows")  # type: ignore[no-any-return]


@pytest.fixture(scope="module")
def repeat(request: pytest.FixtureRequest) -> int:
    return request.config.getoption("--repeat")  # type: ignore[no-any-return]


@pytest.fixture(scope="module")
def db(rows: int) -> duckdb.DuckDBPyConnection:
    """Banco DuckDB em memória com tabela sintética."""
    con = duckdb.connect(":memory:")
    con.execute(f"""
        CREATE TABLE dados AS
        SELECT
            i                                           AS id,
            'empresa_' || (i % 20)                     AS empresa,
            DATE '2024-01-01' + (i % 365)::INTEGER        AS data_emissao,
            (random() * 100000)::DECIMAL(12,2)         AS total_produtos,
            (random() * 120000)::DECIMAL(12,2)         AS total_liquido,
            'NF' || lpad(i::VARCHAR, 8, '0')           AS nota_fiscal,
            repeat('x', 20)                            AS descricao
        FROM range(1, {rows} + 1) t(i)
    """)
    return con


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------


def timeit(fn: Any, repeat: int) -> tuple[float, float, float]:
    """Executa fn `repeat` vezes. Retorna (min, mean, max) em segundos."""
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return min(times), sum(times) / len(times), max(times)


def print_results(title: str, results: list[tuple[str, float, float, float, str]]) -> None:
    table = Table(title=title, border_style="cyan", show_lines=True)
    table.add_column("Operacao", style="bold white", min_width=35)
    table.add_column("Min", justify="right")
    table.add_column("Media", justify="right")
    table.add_column("Max", justify="right")
    table.add_column("Nota", style="dim")
    for name, mn, mean, mx, note in results:
        table.add_row(name, f"{mn:.3f}s", f"{mean:.3f}s", f"{mx:.3f}s", note)
    console.print()
    console.print(table)
    console.print()


# ---------------------------------------------------------------------------
# Benchmark 1: métodos de fetch do DuckDB
# ---------------------------------------------------------------------------


def test_bench_fetch_methods(db: duckdb.DuckDBPyConnection, rows: int, repeat: int) -> None:
    """Compara fetch_arrow_table vs fetchdf vs fetchall."""
    query = "SELECT * FROM dados"
    results = []

    mn, mean, mx = timeit(lambda: db.execute(query).fetch_arrow_table(), repeat)
    results.append(("fetch_arrow_table()", mn, mean, mx, "Arrow Table em memoria"))

    mn, mean, mx = timeit(lambda: db.execute(query).fetchdf(), repeat)
    results.append(("fetchdf()", mn, mean, mx, "pandas DataFrame"))

    mn, mean, mx = timeit(lambda: db.execute(query).fetchall(), repeat)
    results.append(("fetchall()", mn, mean, mx, "lista de tuplas Python"))

    print_results(f"Fetch methods — {rows:,} rows", results)


# ---------------------------------------------------------------------------
# Benchmark 2: serialização Arrow → CSV (estratégias de batch)
# ---------------------------------------------------------------------------


def test_bench_arrow_to_csv(db: duckdb.DuckDBPyConnection, rows: int, repeat: int) -> None:
    """Compara tamanhos de batch ao serializar Arrow -> CSV bytes."""
    arrow_table = db.execute("SELECT * FROM dados").fetch_arrow_table()
    results = []

    for batch_size in [50_000, 100_000, 250_000, 500_000, rows]:
        label = f"batch={batch_size:,}"

        def run(bt: int = batch_size) -> None:
            buf = io.BytesIO()
            opts = pa_csv.WriteOptions(include_header=False)
            for batch in arrow_table.to_batches(max_chunksize=bt):
                pa_csv.write_csv(pa.Table.from_batches([batch]), buf, write_options=opts)

        mn, mean, mx = timeit(run, repeat)
        results.append((label, mn, mean, mx, ""))

    print_results(f"Arrow -> CSV serialization — {rows:,} rows", results)


# ---------------------------------------------------------------------------
# Benchmark 3: pipe anônimo vs BytesIO (estratégias de streaming)
# ---------------------------------------------------------------------------


def test_bench_pipe_vs_bytesio(db: duckdb.DuckDBPyConnection, rows: int, repeat: int) -> None:
    """Compara acumular tudo em BytesIO vs escrever em pipe anônimo."""
    query = "SELECT * FROM dados"
    write_opts = pa_csv.WriteOptions(include_header=False)
    results = []

    # BytesIO: acumula tudo em memória antes de "enviar"
    def via_bytesio() -> bytes:
        arrow_table = db.execute(query).fetch_arrow_table()
        buf = io.BytesIO()
        for batch in arrow_table.to_batches(max_chunksize=500_000):
            pa_csv.write_csv(pa.Table.from_batches([batch]), buf, write_options=write_opts)
        return buf.getvalue()

    mn, mean, mx = timeit(via_bytesio, repeat)
    results.append(("BytesIO (acumula em memoria)", mn, mean, mx, "sem paralelismo"))

    # Pipe: writer thread + reader thread (simula o DatabaseOutput atual)
    def via_pipe() -> None:
        r_fd, w_fd = os.pipe()
        write_error: list[Exception] = []

        def writer() -> None:
            try:
                with os.fdopen(w_fd, "wb") as wf:
                    arrow_table = db.execute(query).fetch_arrow_table()
                    for batch in arrow_table.to_batches(max_chunksize=500_000):
                        buf = io.BytesIO()
                        pa_csv.write_csv(pa.Table.from_batches([batch]), buf, write_options=write_opts)
                        wf.write(buf.getvalue())
            except Exception as exc:
                write_error.append(exc)

        t = threading.Thread(target=writer, daemon=True)
        t.start()
        with os.fdopen(r_fd, "rb") as rf:
            rf.read()
        t.join()
        if write_error:
            raise write_error[0]

    mn, mean, mx = timeit(via_pipe, repeat)
    results.append(("Pipe anonimo (writer + reader threads)", mn, mean, mx, "simula COPY FROM STDIN"))

    print_results(f"Streaming strategies — {rows:,} rows", results)


# ---------------------------------------------------------------------------
# Benchmark 4: custo do strip de timezone em Arrow
# ---------------------------------------------------------------------------


def test_bench_timezone_strip(rows: int, repeat: int) -> None:
    """Mede custo de remover timezone de colunas timestamp (feito no _transfer_via_arrow)."""
    con = duckdb.connect(":memory:")
    con.execute(f"""
        CREATE TABLE tstz AS
        SELECT
            i AS id,
            (TIMESTAMPTZ '2024-01-01 00:00:00+00' + INTERVAL (i % 86400) SECOND) AS ts_with_tz,
            (TIMESTAMP '2024-01-01 00:00:00' + INTERVAL (i % 86400) SECOND)       AS ts_no_tz
        FROM range(1, {rows} + 1) t(i)
    """)
    results = []

    # sem strip
    def without_strip() -> None:
        con.execute("SELECT ts_no_tz FROM tstz").fetch_arrow_table()

    mn, mean, mx = timeit(without_strip, repeat)
    results.append(("fetch sem TIMESTAMPTZ", mn, mean, mx, ""))

    # com strip via cast DuckDB
    def with_duckdb_cast() -> None:
        con.execute("SELECT CAST(ts_with_tz AS TIMESTAMP) AS ts FROM tstz").fetch_arrow_table()

    mn, mean, mx = timeit(with_duckdb_cast, repeat)
    results.append(("CAST(TIMESTAMPTZ AS TIMESTAMP) via SQL", mn, mean, mx, ""))

    # com strip via pyarrow
    def with_arrow_cast() -> pa.Table:
        tbl = con.execute("SELECT ts_with_tz FROM tstz").fetch_arrow_table()
        for i, field in enumerate(tbl.schema):
            if pa.types.is_timestamp(field.type) and field.type.tz is not None:
                tbl = tbl.set_column(i, field.name, tbl.column(i).cast(pa.timestamp(field.type.unit)))
        return tbl

    mn, mean, mx = timeit(with_arrow_cast, repeat)
    results.append(("cast via PyArrow pos-fetch", mn, mean, mx, "abordagem atual"))

    print_results(f"Timezone strip strategies — {rows:,} rows", results)


# ---------------------------------------------------------------------------
# Benchmark 5: inserção no DuckDB (Arrow vs pandas)
# ---------------------------------------------------------------------------


def test_bench_duckdb_insert(rows: int, repeat: int) -> None:
    """Compara inserção de dados no DuckDB via Arrow Table vs pandas DataFrame."""
    src = duckdb.connect(":memory:")
    src.execute(f"""
        CREATE TABLE src AS
        SELECT i AS id, random()::DOUBLE AS valor, 'txt_' || i AS texto
        FROM range(1, {rows} + 1) t(i)
    """)
    arrow_table = src.execute("SELECT * FROM src").fetch_arrow_table()
    df = src.execute("SELECT * FROM src").fetchdf()

    results = []

    def insert_arrow() -> None:
        con = duckdb.connect(":memory:")
        con.register("tmp", arrow_table)
        con.execute("CREATE TABLE t AS SELECT * FROM tmp")
        con.unregister("tmp")

    mn, mean, mx = timeit(insert_arrow, repeat)
    results.append(("Arrow Table -> DuckDB", mn, mean, mx, ""))

    def insert_pandas() -> None:
        con = duckdb.connect(":memory:")
        con.register("tmp", df)
        con.execute("CREATE TABLE t AS SELECT * FROM tmp")
        con.unregister("tmp")

    mn, mean, mx = timeit(insert_pandas, repeat)
    results.append(("pandas DataFrame -> DuckDB", mn, mean, mx, ""))

    print_results(f"DuckDB insert strategies — {rows:,} rows", results)
