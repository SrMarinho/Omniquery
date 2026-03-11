import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import connectorx as cx
import pandas as pd
import pyarrow as pa
from duckdb import DuckDBPyConnection
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.engine.url import make_url
from tqdm import tqdm

from src.config import memory_database
from src.config.settings import DB_CHUNK_SIZE, DB_MEMORY_LIMIT, DB_THREADS, ORACLE_ODBC_DRIVER, PIPELINE_WORKERS
from src.entities.table import Table
from src.exceptions import LoaderError, OmniQueryError
from src.utils.database_config_reader import get_database_config
from src.utils.retry import db_retry

_duckdb_lock = threading.Lock()

logger = logging.getLogger(__name__)

# Dialetos suportados pelo connectorx (leitura direta para Arrow, sem pandas)
_CX_DIALECTS = {"postgresql", "postgres", "mssql"}

# Dialetos suportados via turbodbc (Oracle via ODBC)
_TURBODBC_DIALECTS = {"oracle"}

try:
    import turbodbc as _turbodbc  # type: ignore[import-untyped]

    _TURBODBC_AVAILABLE = True
except ImportError:
    _turbodbc = None  # type: ignore[assignment]
    _TURBODBC_AVAILABLE = False


def _to_cx_url(sqlalchemy_url: str) -> str | None:
    """
    Converte URL SQLAlchemy para formato connectorx.
    Retorna None para dialetos não suportados (ex: Oracle).
    """
    try:
        url = make_url(sqlalchemy_url)
        dialect = url.drivername.split("+")[0]
        if dialect not in _CX_DIALECTS:
            return None
        cx_dialect = "postgresql" if dialect in ("postgresql", "postgres") else dialect
        user = url.username or ""
        password = f":{url.password}" if url.password else ""
        host = url.host or "localhost"
        port = f":{url.port}" if url.port else ""
        database = f"/{url.database}" if url.database else ""
        return f"{cx_dialect}://{user}{password}@{host}{port}{database}"
    except Exception:
        return None


def _to_turbodbc_connstr(sqlalchemy_url: str) -> str | None:
    """
    Constrói connection string ODBC para turbodbc (Oracle).
    Retorna None para dialetos não suportados.
    """
    try:
        url = make_url(sqlalchemy_url)
        dialect = url.drivername.split("+")[0]
        if dialect not in _TURBODBC_DIALECTS:
            return None
        host = url.host or "localhost"
        port = url.port or 1521
        service = url.database or ""
        user = url.username or ""
        password = url.password or ""
        return f"Driver={{{ORACLE_ODBC_DRIVER}}};DBQ={host}:{port}/{service};Uid={user};Pwd={password}"
    except Exception:
        return None


class Loader(BaseModel):
    type: str
    source: str = Field(default_factory=str)
    tables: list[Table] = Field(default_factory=list)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def run(self) -> None:
        raise NotImplementedError("Loader not implemented yet")


class DatabaseLoader(Loader):
    type: str = "database"
    database: str = "memory"

    @db_retry
    def get_engine(self, database: str) -> Engine:
        config = get_database_config(database)
        connection_string: str = config["connection_string"]
        engine = create_engine(connection_string)
        with engine.connect():
            pass
        return engine

    def run(self) -> None:
        tag = f"[load:{self.source}]"
        job_start = time.time()

        table_names = ", ".join(t.alias for t in self.tables[:3])
        if len(self.tables) > 3:
            table_names += f" +{len(self.tables) - 3}"
        logger.info("%s Starting — %d table(s): %s", tag, len(self.tables), table_names)

        try:
            source_engine = self.get_engine(self.source)
            logger.debug("%s Connected to %s", tag, self.source)

            workers = min(PIPELINE_WORKERS, len(self.tables))
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(self._transfer, source_engine, memory_database, table): table
                    for table in self.tables
                }
                tables_processed = 0
                for future in as_completed(futures):
                    future.result()
                    tables_processed += 1

            total_time = time.time() - job_start
            logger.info("%s Done — %d/%d tables | %.2fs", tag, tables_processed, len(self.tables), total_time)

        except OmniQueryError:
            raise
        except Exception as e:
            elapsed = time.time() - job_start
            logger.error("%s Failed — %s (%.2fs)", tag, e, elapsed)
            if "tables_processed" in locals():
                logger.error("%s Tables completed before error: %d/%d", tag, tables_processed, len(self.tables))
            raise LoaderError(f"Failed to load from source '{self.source}'") from e

    def _transfer(self, source_engine: Engine, to_engine: DuckDBPyConnection, table: Table) -> None:
        tag = f"[load:{self.source}]"

        to_engine.execute(f"PRAGMA threads={DB_THREADS}")
        to_engine.execute(f"PRAGMA memory_limit='{DB_MEMORY_LIMIT}'")

        logger.info("%s %s — fetching...", tag, table.alias)

        transfer_start = time.time()
        cx_url = _to_cx_url(source_engine.url.render_as_string(hide_password=False))

        turbodbc_connstr = _to_turbodbc_connstr(source_engine.url.render_as_string(hide_password=False))

        try:
            if cx_url:
                self._transfer_via_arrow(cx_url, to_engine, table, tag)
            elif turbodbc_connstr and _TURBODBC_AVAILABLE:
                try:
                    self._transfer_via_turbodbc(turbodbc_connstr, to_engine, table, tag)
                except Exception as e:
                    logger.warning("%s %s — turbodbc failed (%s), falling back to pandas", tag, table.alias, e)
                    self._transfer_via_pandas(source_engine, to_engine, table, tag)
            else:
                logger.debug("%s connectorx not supported for this dialect — using pandas", tag)
                self._transfer_via_pandas(source_engine, to_engine, table, tag)

            transfer_time = time.time() - transfer_start
            total_rows = to_engine.execute(f"SELECT COUNT(*) FROM {table.alias}").fetchone()[0]  # type: ignore[index]
            avg_speed = total_rows / transfer_time if transfer_time > 0 else 0
            logger.info(
                "%s %s — %s rows | %.2fs | %s rows/s",
                tag,
                table.alias,
                f"{total_rows:,}",
                transfer_time,
                f"{avg_speed:,.0f}",
            )

        except Exception as e:
            logger.error("%s %s — Error: %s", tag, table.alias, e)
            raise LoaderError(f"Failed to transfer table '{table.alias}'") from e

    def _transfer_via_arrow(self, cx_url: str, to_engine: DuckDBPyConnection, table: Table, tag: str) -> None:
        """Leitura via connectorx → Arrow → DuckDB (sem serialização Python)."""
        logger.debug("%s %s — using connectorx + Arrow", tag, table.alias)

        arrow_table: pa.Table = cx.read_sql(cx_url, table.content, return_type="arrow")  # type: ignore[assignment]
        arrow_table = arrow_table.rename_columns([c.lower() for c in arrow_table.column_names])

        tmp_name = f"__cx_{threading.get_ident()}"
        with _duckdb_lock:
            to_engine.register(tmp_name, arrow_table)
            to_engine.execute(f"CREATE OR REPLACE TABLE {table.alias} AS SELECT * FROM {tmp_name}")
            to_engine.unregister(tmp_name)
        del arrow_table

    def _transfer_via_pandas(
        self, source_engine: Engine, to_engine: DuckDBPyConnection, table: Table, tag: str
    ) -> None:
        """Fallback: leitura via pandas em chunks (para Oracle e outros não suportados pelo connectorx)."""
        logger.debug(
            "%s %s — using pandas | chunk=%s | threads=%d | memory=%s",
            tag,
            table.alias,
            f"{DB_CHUNK_SIZE:,}",
            DB_THREADS,
            DB_MEMORY_LIMIT,
        )

        total_rows = 0
        first_chunk = True
        chunks = pd.read_sql(table.content, source_engine, chunksize=DB_CHUNK_SIZE)
        tmp_name = f"__pd_{threading.get_ident()}"

        with tqdm(desc=f"{self.source}/{table.alias}", unit="chunk", leave=False) as pbar:
            for i, chunk_df in enumerate(chunks, 1):
                chunk_start = time.time()
                chunk_df.columns = chunk_df.columns.str.lower()
                chunk_rows = len(chunk_df)

                with _duckdb_lock:
                    to_engine.register(tmp_name, chunk_df)
                    if first_chunk:
                        to_engine.execute(f"CREATE OR REPLACE TABLE {table.alias} AS SELECT * FROM {tmp_name}")
                        first_chunk = False
                        operation = "Created"
                    else:
                        to_engine.execute(f"INSERT INTO {table.alias} SELECT * FROM {tmp_name}")
                        operation = "Appended"
                    to_engine.unregister(tmp_name)

                chunk_time = time.time() - chunk_start
                total_rows += chunk_rows
                pbar.update(1)
                pbar.set_postfix({"rows": f"{total_rows:,}", "op": operation})
                logger.debug("%s %s chunk #%d: %s rows in %.2fs", tag, table.alias, i, f"{chunk_rows:,}", chunk_time)
                del chunk_df

    def _transfer_via_turbodbc(self, connstr: str, to_engine: DuckDBPyConnection, table: Table, tag: str) -> None:
        """Leitura via turbodbc → Arrow → DuckDB (para Oracle via ODBC)."""
        logger.debug("%s %s — using turbodbc + Arrow", tag, table.alias)

        conn = _turbodbc.connect(connection_string=connstr)
        try:
            cursor = conn.cursor()
            cursor.execute(table.content)
            arrow_table: pa.Table = cursor.fetchallarrow()
            arrow_table = arrow_table.rename_columns([c.lower() for c in arrow_table.column_names])
        finally:
            conn.close()

        tmp_name = f"__td_{threading.get_ident()}"
        with _duckdb_lock:
            to_engine.register(tmp_name, arrow_table)
            to_engine.execute(f"CREATE OR REPLACE TABLE {table.alias} AS SELECT * FROM {tmp_name}")
            to_engine.unregister(tmp_name)
        del arrow_table


class FileLoader(Loader):
    type: str = "file"

    def _transfer(self, source: str, to_engine: DuckDBPyConnection, table: Table) -> None:
        ext = Path(source).suffix.lower()
        if ext in (".xlsx", ".xls"):
            df = pd.read_excel(source)
            df.columns = df.columns.str.lower()
            to_engine.register("temp_df", df)
            to_engine.execute(f"CREATE OR REPLACE TABLE {table.alias} AS SELECT * FROM temp_df")
            to_engine.unregister("temp_df")
        else:
            # DuckDB native read_csv_auto — sem pandas
            duckdb_path = source.replace("\\", "/")
            to_engine.execute(f"CREATE OR REPLACE TABLE {table.alias} AS SELECT * FROM read_csv_auto('{duckdb_path}')")

    def run(self) -> None:
        tag = f"[load:{Path(self.source).name}]"
        logger.info("%s Loading from file: %s", tag, self.source)
        for table in self.tables:
            try:
                self._transfer(self.source, memory_database, table)
                logger.info("%s %s — loaded", tag, table.alias)
            except OmniQueryError:
                raise
            except Exception as e:
                raise LoaderError(f"Failed to load table '{table.alias}' from file '{self.source}'") from e


class LoaderFactory:
    loader_types = {
        "database": DatabaseLoader,
        "file": FileLoader,
    }

    @staticmethod
    def create(config: dict[str, Any]) -> Loader:
        loader_type: str = config.get("type", "")
        if loader_type.lower() in LoaderFactory.loader_types:
            return LoaderFactory.loader_types[loader_type.lower()](**config)  # type: ignore[no-any-return]

        return Loader(**config)
