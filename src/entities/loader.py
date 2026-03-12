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

from src.config import memory_database, memory_database_lock
from src.config.settings import DB_CHUNK_SIZE, DB_MEMORY_LIMIT, DB_THREADS, ORACLE_ODBC_DRIVER, PIPELINE_WORKERS
from src.entities.table import Table
from src.exceptions import LoaderError, OmniQueryError
from src.utils.database_config_reader import get_database_config
from src.utils.retry import db_retry

_duckdb_lock = memory_database_lock

logger = logging.getLogger(__name__)

_CX_DIALECTS = {"postgresql", "postgres", "mssql"}
_TURBODBC_DIALECTS = {"oracle"}

try:
    import turbodbc as _turbodbc  # type: ignore[import-untyped]

    _TURBODBC_AVAILABLE = True
except ImportError:
    _turbodbc = None  # type: ignore[assignment]
    _TURBODBC_AVAILABLE = False


def _to_cx_url(sqlalchemy_url: str) -> str | None:
    """Converte URL SQLAlchemy para formato connectorx. Retorna None para dialetos nao suportados."""
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
    """Constroi connection string ODBC para turbodbc. Retorna None para dialetos nao suportados."""
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
        job_start = time.time()

        try:
            source_engine = self.get_engine(self.source)

            with _duckdb_lock:
                memory_database.execute(f"PRAGMA threads={DB_THREADS}")
                memory_database.execute(f"PRAGMA memory_limit='{DB_MEMORY_LIMIT}'")

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
            logger.info(
                "[dim]%s[/dim] | [bold green]done[/bold green] %d/%d in %.2fs",
                self.source,
                tables_processed,
                len(self.tables),
                total_time,
            )

        except OmniQueryError:
            raise
        except Exception as e:
            elapsed = time.time() - job_start
            logger.error("[dim]%s[/dim] | falhou em %.2fs -- %s", self.source, elapsed, e)
            if "tables_processed" in locals():
                logger.error("  %d/%d tabelas concluidas", tables_processed, len(self.tables))
            raise LoaderError(f"Failed to load from source '{self.source}'") from e

    def _transfer(self, source_engine: Engine, to_engine: DuckDBPyConnection, table: Table) -> None:
        transfer_start = time.time()
        cx_url = _to_cx_url(source_engine.url.render_as_string(hide_password=False))
        turbodbc_connstr = _to_turbodbc_connstr(source_engine.url.render_as_string(hide_password=False))

        try:
            if cx_url:
                data_bytes = self._transfer_via_arrow(cx_url, to_engine, table)
            elif turbodbc_connstr and _TURBODBC_AVAILABLE:
                try:
                    data_bytes = self._transfer_via_turbodbc(turbodbc_connstr, to_engine, table)
                except Exception as e:
                    logger.warning(
                        "[dim]%s[/dim] | %s -- turbodbc falhou (%s), usando pandas", self.source, table.alias, e
                    )
                    data_bytes = self._transfer_via_pandas(source_engine, to_engine, table)
            else:
                data_bytes = self._transfer_via_pandas(source_engine, to_engine, table)

            transfer_time = time.time() - transfer_start
            with _duckdb_lock:
                total_rows = to_engine.execute(f"SELECT COUNT(*) FROM {table.alias}").fetchone()[0]  # type: ignore[index]
            avg_speed = total_rows / transfer_time if transfer_time > 0 else 0
            mb_s = data_bytes / transfer_time / (1024 * 1024) if transfer_time > 0 else 0
            logger.info(
                "[dim]%s[/dim] | %-30s  %10s rows  %6.2fs  %s r/s  %.1f MB/s",
                self.source,
                table.alias,
                f"{total_rows:,}",
                transfer_time,
                f"{avg_speed:,.0f}",
                mb_s,
            )

        except Exception as e:
            logger.error("[dim]%s[/dim] | %s -- %s", self.source, table.alias, e)
            raise LoaderError(f"Failed to transfer table '{table.alias}'") from e

    def _transfer_via_arrow(self, cx_url: str, to_engine: DuckDBPyConnection, table: Table) -> int:
        """Le via connectorx -> Arrow -> DuckDB. Remove timezone de colunas timestamp para evitar dependencia de ICU tzdata."""
        arrow_table: pa.Table = cx.read_sql(cx_url, table.content, return_type="arrow")  # type: ignore[assignment]
        arrow_table = arrow_table.rename_columns([c.lower() for c in arrow_table.column_names])

        new_cols = {}
        for i, field in enumerate(arrow_table.schema):
            if pa.types.is_timestamp(field.type) and field.type.tz is not None:
                new_cols[field.name] = arrow_table.column(i).cast(pa.timestamp(field.type.unit))
        for col_name, col_data in new_cols.items():
            idx = arrow_table.schema.get_field_index(col_name)
            arrow_table = arrow_table.set_column(idx, col_name, col_data)

        data_bytes = int(arrow_table.nbytes)
        tmp_name = f"__cx_{threading.get_ident()}"
        with _duckdb_lock:
            to_engine.register(tmp_name, arrow_table)
            to_engine.execute(f"CREATE OR REPLACE TABLE {table.alias} AS SELECT * FROM {tmp_name}")
            to_engine.unregister(tmp_name)
        del arrow_table
        return data_bytes

    def _transfer_via_pandas(self, source_engine: Engine, to_engine: DuckDBPyConnection, table: Table) -> int:
        """Le via pandas em chunks e converte para Arrow antes de inserir no DuckDB."""
        logger.info("[dim]%s[/dim] | %-30s  carregando...", self.source, table.alias)

        total_rows = 0
        total_bytes = 0
        first_chunk = True
        chunks = pd.read_sql(table.content, source_engine, chunksize=DB_CHUNK_SIZE)
        tmp_name = f"__pd_{threading.get_ident()}"

        for i, chunk_df in enumerate(chunks, 1):
            chunk_start = time.time()
            chunk_df.columns = chunk_df.columns.str.lower()
            chunk_rows = len(chunk_df)
            chunk_arrow = pa.Table.from_pandas(chunk_df, preserve_index=False)
            del chunk_df
            total_bytes += chunk_arrow.nbytes

            with _duckdb_lock:
                to_engine.register(tmp_name, chunk_arrow)
                if first_chunk:
                    to_engine.execute(f"CREATE OR REPLACE TABLE {table.alias} AS SELECT * FROM {tmp_name}")
                    first_chunk = False
                else:
                    to_engine.execute(f"INSERT INTO {table.alias} SELECT * FROM {tmp_name}")
                to_engine.unregister(tmp_name)

            chunk_time = time.time() - chunk_start
            total_rows += chunk_rows
            logger.debug(
                "[dim]%s[/dim] | %s chunk #%d: %s rows in %.2fs",
                self.source,
                table.alias,
                i,
                f"{chunk_rows:,}",
                chunk_time,
            )
            del chunk_arrow

        return total_bytes

    def _transfer_via_turbodbc(self, connstr: str, to_engine: DuckDBPyConnection, table: Table) -> int:
        """Le via turbodbc -> Arrow -> DuckDB. Usado para Oracle via ODBC."""
        conn = _turbodbc.connect(connection_string=connstr)
        try:
            cursor = conn.cursor()
            cursor.execute(table.content)
            arrow_table: pa.Table = cursor.fetchallarrow()
            arrow_table = arrow_table.rename_columns([c.lower() for c in arrow_table.column_names])
        finally:
            conn.close()

        data_bytes = int(arrow_table.nbytes)
        tmp_name = f"__td_{threading.get_ident()}"
        with _duckdb_lock:
            to_engine.register(tmp_name, arrow_table)
            to_engine.execute(f"CREATE OR REPLACE TABLE {table.alias} AS SELECT * FROM {tmp_name}")
            to_engine.unregister(tmp_name)
        del arrow_table
        return data_bytes


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
            duckdb_path = source.replace("\\", "/")
            to_engine.execute(f"CREATE OR REPLACE TABLE {table.alias} AS SELECT * FROM read_csv_auto('{duckdb_path}')")

    def run(self) -> None:
        tag = f"[load:{Path(self.source).name}]"
        logger.info("%s Loading from file: %s", tag, self.source)
        for table in self.tables:
            try:
                self._transfer(self.source, memory_database, table)
                logger.info("[dim]%s[/dim] | %s -- ok", self.source, table.alias)
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
