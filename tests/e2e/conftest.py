"""
Shared fixtures and utilities for the homolog (E2E) test suite.

Requires credentials in env vars (see .env.example).
Tests are skipped automatically when the matching credentials are missing.

Run:
    uv run pytest tests/e2e/ -v -s -m homolog

Useful flags:
    --rows=N        synthetic rows for output benchmarks (default: 100_000)
    --repeat=N      repetitions per benchmark (default: 3)
"""

from __future__ import annotations

import io
import os
import sys
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from datetime import date, timedelta

import duckdb
import psutil
import pytest
from rich.console import Console
from rich.table import Table

console = Console(
    file=io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace"),
    width=160,
    highlight=False,
)

# ---------------------------------------------------------------------------
# CLI options (--rows and --repeat are already registered in tests/conftest.py).
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def rows(request: pytest.FixtureRequest) -> int:
    return request.config.getoption("--rows")  # type: ignore[no-any-return]


@pytest.fixture(scope="module")
def repeat(request: pytest.FixtureRequest) -> int:
    return request.config.getoption("--repeat")  # type: ignore[no-any-return]


# ---------------------------------------------------------------------------
# ResourceMonitor
# ---------------------------------------------------------------------------


class ResourceMonitor:
    """Sample system metrics in a background thread while a workload runs."""

    def __init__(self, interval_s: float = 0.25) -> None:
        self._interval = interval_s
        self._proc = psutil.Process()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._ram_mb: list[float] = []
        self._cpu_pct: list[float] = []
        self._net_start: psutil._common.snetio | None = None  # type: ignore[name-defined]
        self._disk_start: psutil._common.sdiskio | None = None  # type: ignore[name-defined]
        self._net_end: psutil._common.snetio | None = None  # type: ignore[name-defined]
        self._disk_end: psutil._common.sdiskio | None = None  # type: ignore[name-defined]

    def start(self) -> None:
        self._net_start = psutil.net_io_counters()
        self._disk_start = psutil.disk_io_counters()
        self._stop.clear()
        self._ram_mb.clear()
        self._cpu_pct.clear()
        # Warm up cpu_percent — the first call always returns 0.0.
        self._proc.cpu_percent(interval=None)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> ResourceMonitor:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
        self._net_end = psutil.net_io_counters()
        self._disk_end = psutil.disk_io_counters()
        return self

    def _run(self) -> None:
        while not self._stop.is_set():
            self._ram_mb.append(self._proc.memory_info().rss / (1024 * 1024))
            self._cpu_pct.append(self._proc.cpu_percent(interval=None))
            time.sleep(self._interval)

    @property
    def ram_peak_mb(self) -> float:
        return max(self._ram_mb, default=0.0)

    @property
    def ram_mean_mb(self) -> float:
        return sum(self._ram_mb) / len(self._ram_mb) if self._ram_mb else 0.0

    @property
    def cpu_peak_pct(self) -> float:
        return max(self._cpu_pct, default=0.0)

    @property
    def cpu_mean_pct(self) -> float:
        return sum(self._cpu_pct) / len(self._cpu_pct) if self._cpu_pct else 0.0

    @property
    def net_recv_mb(self) -> float:
        if self._net_start and self._net_end:
            return (self._net_end.bytes_recv - self._net_start.bytes_recv) / (1024 * 1024)
        return 0.0

    @property
    def net_sent_mb(self) -> float:
        if self._net_start and self._net_end:
            return (self._net_end.bytes_sent - self._net_start.bytes_sent) / (1024 * 1024)
        return 0.0

    @property
    def disk_read_mb(self) -> float:
        d_start = self._disk_start
        d_end = self._disk_end
        if d_start is not None and d_end is not None:
            return (d_end.read_bytes - d_start.read_bytes) / (1024 * 1024)
        return 0.0

    @property
    def disk_write_mb(self) -> float:
        d_start = self._disk_start
        d_end = self._disk_end
        if d_start is not None and d_end is not None:
            return (d_end.write_bytes - d_start.write_bytes) / (1024 * 1024)
        return 0.0


@contextmanager
def monitor_run(interval_s: float = 0.25) -> Generator[ResourceMonitor, None, None]:
    """Context manager that starts and stops a ResourceMonitor automatically."""
    m = ResourceMonitor(interval_s)
    m.start()
    try:
        yield m
    finally:
        m.stop()


# ---------------------------------------------------------------------------
# Reporting (follows the bench_transfer.py format).
# ---------------------------------------------------------------------------


def print_homolog_results(
    title: str,
    results: list[tuple[str, float, float, float, float, float, float, float, str]],
) -> None:
    """
    Print a Rich table with homolog metrics.

    Columns: Operation | Min | Mean | Max | MB/s | RAM peak (MB) | CPU avg (%) | Net recv (MB) | Note
    """
    table = Table(title=title, border_style="cyan", show_lines=True)
    table.add_column("Operation", style="bold white", min_width=45)
    table.add_column("Min", justify="right")
    table.add_column("Mean", justify="right")
    table.add_column("Max", justify="right")
    table.add_column("MB/s", justify="right", style="cyan")
    table.add_column("RAM peak (MB)", justify="right", style="yellow")
    table.add_column("CPU avg (%)", justify="right", style="magenta")
    table.add_column("Net recv (MB)", justify="right", style="green")
    table.add_column("Note", style="dim")

    for name, mn, mean, mx, mb_s, ram_peak, cpu_avg, net_recv, note in results:
        mb_s_str = f"{mb_s:,.1f}" if mb_s > 0 else "-"
        ram_str = f"{ram_peak:.0f}" if ram_peak > 0 else "-"
        cpu_str = f"{cpu_avg:.1f}" if cpu_avg > 0 else "-"
        net_str = f"{net_recv:.1f}" if net_recv > 0 else "-"
        table.add_row(name, f"{mn:.3f}s", f"{mean:.3f}s", f"{mx:.3f}s", mb_s_str, ram_str, cpu_str, net_str, note)

    console.print()
    console.print(table)
    console.print()


# ---------------------------------------------------------------------------
# Fixture: reset the memory_database singleton between pipeline tests.
# ---------------------------------------------------------------------------


@pytest.fixture
def fresh_memory_database() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """
    Replace the global memory_database singleton with a fresh connection.

    Required because App.run() calls memory_database.close() at the end, which
    invalidates the singleton for the next test.
    """
    import src.config as _cfg
    import src.config.database as _db
    import src.entities.loader as _loader
    import src.entities.output as _output

    new_con = duckdb.connect(":memory:")
    _db.memory_database = new_con
    _cfg.memory_database = new_con
    _loader.memory_database = new_con
    _output.memory_database = new_con

    yield new_con

    try:
        new_con.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Skip fixtures grouped by credential set.
# ---------------------------------------------------------------------------

_PROCFIT_VARS = [
    "PROCFIT_DATABASE_USER",
    "PROCFIT_DATABASE_PASSWORD",
    "PROCFIT_DATABASE_HOST",
    "PROCFIT_DATABASE_PORT",
    "PROCFIT_DATABASE_NAME",
]

_POSTGRESQL_VARS = [
    "DATABASE_USER",
    "DATABASE_PASSWORD",
    "DATABASE_HOST",
    "DATABASE_PORT",
    "DATABASE_NAME",
]

_SENIOR_VARS = [
    "SENIOR_DATABASE_USER",
    "SENIOR_DATABASE_PASSWORD",
    "SENIOR_DATABASE_HOST",
    "SENIOR_DATABASE_PORT",
    "SENIOR_DATABASE_SERVICE_NAME",
]


@pytest.fixture
def require_procfit() -> None:
    missing = [v for v in _PROCFIT_VARS if not os.getenv(v)]
    if missing:
        pytest.skip(f"Procfit (SQL Server) credentials missing: {missing}")


@pytest.fixture
def require_postgresql() -> None:
    missing = [v for v in _POSTGRESQL_VARS if not os.getenv(v)]
    if missing:
        pytest.skip(f"PostgreSQL credentials missing: {missing}")


@pytest.fixture
def require_senior() -> None:
    missing = [v for v in _SENIOR_VARS if not os.getenv(v)]
    if missing:
        pytest.skip(f"Senior (Oracle) credentials missing: {missing}")


# ---------------------------------------------------------------------------
# Fixture: date range — last 30 days.
# ---------------------------------------------------------------------------


@pytest.fixture
def pipeline_date_params() -> dict:  # type: ignore[type-arg]
    """Date parameters for pipelines — last 30 days, to keep volume bounded."""
    end = date.today()
    start = end - timedelta(days=30)
    return {"data_inicio": start, "data_fim": end}
