"""
Connectivity probes against the real databases.

Each test is independent and is skipped when the matching credentials are
missing from the environment. They run first to surface network/credential
issues before the heavier load tests.

Run:
    uv run pytest tests/e2e/test_connections.py -v -s -m homolog
"""

import pytest
from sqlalchemy import create_engine, text

from src.utils.database_config_reader import get_database_config


@pytest.mark.homolog
def test_connection_procfit(require_procfit: None) -> None:
    """Validate connectivity to SQL Server (Procfit PBS)."""
    config = get_database_config("procfit")
    engine = create_engine(config["connection_string"])

    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 AS ok")).fetchone()

    assert result is not None
    assert result[0] == 1
    print(f"\n  OK Procfit connected: {config['connection_string'].split('@')[-1]}")


@pytest.mark.homolog
def test_connection_postgresql(require_postgresql: None) -> None:
    """Validate connectivity to PostgreSQL (the pipeline destination)."""
    import os

    import psycopg2

    conn = psycopg2.connect(
        host=os.environ["DATABASE_HOST"],
        port=int(os.environ["DATABASE_PORT"]),
        dbname=os.environ["DATABASE_NAME"],
        user=os.environ["DATABASE_USER"],
        password=os.environ["DATABASE_PASSWORD"],
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT version()")
        version = cur.fetchone()[0]  # type: ignore[index]
        cur.close()
    finally:
        conn.close()

    assert version is not None
    print(f"\n  OK PostgreSQL connected: {version.split(',')[0]}")


@pytest.mark.homolog
def test_connection_oracle_real(require_senior: None) -> None:
    """
    Validate connectivity to Oracle (Senior ERP).
    Skipped when turbodbc is not installed or credentials are missing.
    """
    try:
        import turbodbc  # type: ignore[import-untyped]  # noqa: F401
    except ImportError:
        pytest.skip("turbodbc not installed — Oracle via ODBC unavailable")

    config = get_database_config("senior")
    engine = create_engine(config["connection_string"])

    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 FROM DUAL")).fetchone()

    assert result is not None
    print(f"\n  OK Oracle (Senior) connected: {config['connection_string'].split('@')[-1]}")


@pytest.mark.homolog
def test_procfit_tables_exist(require_procfit: None) -> None:
    """Confirm the tables used by the pipeline exist in Procfit."""
    config = get_database_config("procfit")
    engine = create_engine(config["connection_string"])

    tables = ["NF_FATURAMENTO", "NF_COMPRA", "NF_FATURAMENTO_DEVOLUCOES", "NF_COMPRA_DEVOLUCOES"]

    with engine.connect() as conn:
        for table in tables:
            result = conn.execute(text(f"SELECT TOP 1 1 AS ok FROM {table} WITH(NOLOCK)")).fetchone()
            assert result is not None, f"Table {table} inaccessible or empty"
            print(f"\n  OK {table} accessible")
