"""
Probes de conectividade com os bancos de dados reais.

Cada teste é independente e pulado quando as credenciais do respectivo
banco não estão no ambiente. São os primeiros a rodar para diagnosticar
problemas de rede/credenciais antes dos testes de carga.

Execução:
    uv run pytest tests/homolog/test_connections.py -v -s -m homolog
"""

import pytest
from sqlalchemy import create_engine, text

from src.utils.database_config_reader import get_database_config


@pytest.mark.homolog
def test_connection_procfit(require_procfit: None) -> None:
    """Valida conectividade com SQL Server (Procfit PBS)."""
    config = get_database_config("procfit")
    engine = create_engine(config["connection_string"])

    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 AS ok")).fetchone()

    assert result is not None
    assert result[0] == 1
    print(f"\n  OK Procfit conectado: {config['connection_string'].split('@')[-1]}")


@pytest.mark.homolog
def test_connection_postgresql(require_postgresql: None) -> None:
    """Valida conectividade com PostgreSQL (destino dos pipelines)."""
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
    print(f"\n  OK PostgreSQL conectado: {version.split(',')[0]}")


@pytest.mark.homolog
def test_connection_oracle_real(require_senior: None) -> None:
    """
    Valida conectividade com Oracle (Senior ERP).
    Pulado quando turbodbc não está instalado ou credenciais ausentes.
    """
    try:
        import turbodbc  # type: ignore[import-untyped]  # noqa: F401
    except ImportError:
        pytest.skip("turbodbc não instalado — Oracle via ODBC indisponível")

    config = get_database_config("senior")
    engine = create_engine(config["connection_string"])

    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 FROM DUAL")).fetchone()

    assert result is not None
    print(f"\n  OK Oracle (Senior) conectado: {config['connection_string'].split('@')[-1]}")


@pytest.mark.homolog
def test_procfit_tabelas_existem(require_procfit: None) -> None:
    """Confirma que as tabelas usadas pelo pipeline existem no Procfit."""
    config = get_database_config("procfit")
    engine = create_engine(config["connection_string"])

    tabelas = ["NF_FATURAMENTO", "NF_COMPRA", "NF_FATURAMENTO_DEVOLUCOES", "NF_COMPRA_DEVOLUCOES"]

    with engine.connect() as conn:
        for tabela in tabelas:
            result = conn.execute(text(f"SELECT TOP 1 1 AS ok FROM {tabela} WITH(NOLOCK)")).fetchone()
            assert result is not None, f"Tabela {tabela} inacessível ou vazia"
            print(f"\n  OK {tabela} acessível")
