import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd
from duckdb import DuckDBPyConnection
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.engine import Engine, create_engine
from tqdm import tqdm

from src.config import memory_database
from src.config.settings import DB_CHUNK_SIZE, DB_MEMORY_LIMIT, DB_THREADS
from src.entities.table import Table
from src.exceptions import LoaderError, OmniQueryError
from src.utils.database_config_reader import get_database_config
from src.utils.retry import db_retry

logger = logging.getLogger(__name__)


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

            tables_processed = 0
            for table in self.tables:
                self._transfer(source_engine, memory_database, table)
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
        logger.debug(
            "%s threads=%d | memory_limit=%s | chunk=%s", tag, DB_THREADS, DB_MEMORY_LIMIT, f"{DB_CHUNK_SIZE:,}"
        )

        total_rows = 0
        transfer_start = time.time()

        try:
            query = table.content
            first_chunk = True
            chunks = pd.read_sql(query, source_engine, chunksize=DB_CHUNK_SIZE)

            with tqdm(desc=f"{self.source}/{table.alias}", unit="chunk", leave=False) as pbar:
                for i, chunk_df in enumerate(chunks, 1):
                    chunk_start = time.time()
                    chunk_df.columns = chunk_df.columns.str.lower()
                    chunk_rows = len(chunk_df)

                    if to_engine:
                        to_engine.register("temp_df", chunk_df)
                        if first_chunk:
                            to_engine.execute(f"""
                                CREATE OR REPLACE TABLE {table.alias} AS
                                SELECT * FROM temp_df
                            """)
                            first_chunk = False
                            operation = "Created"
                        else:
                            to_engine.execute(f"""
                                INSERT INTO {table.alias}
                                SELECT * FROM temp_df
                            """)
                            operation = "Appended"
                        to_engine.unregister("temp_df")

                    chunk_time = time.time() - chunk_start
                    total_rows += chunk_rows
                    pbar.update(1)
                    pbar.set_postfix({"rows": f"{total_rows:,}", "op": operation})
                    logger.debug(
                        "%s %s chunk #%d: %s rows in %.2fs", tag, table.alias, i, f"{chunk_rows:,}", chunk_time
                    )
                    del chunk_df

            transfer_time = time.time() - transfer_start
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
            logger.error("%s %s — Error: %s (rows so far: %s)", tag, table.alias, e, f"{total_rows:,}")
            raise LoaderError(f"Failed to transfer table '{table.alias}'") from e


class FileLoader(Loader):
    type: str = "file"

    def _transfer(self, source: str, to_engine: DuckDBPyConnection, table: Table) -> None:
        ext = Path(source).suffix.lower()
        if ext in (".xlsx", ".xls"):
            df = pd.read_excel(source)
        else:
            df = pd.read_csv(source)
        df.columns = df.columns.str.lower()
        to_engine.register("temp_df", df)
        to_engine.execute(f"""
            CREATE OR REPLACE TABLE {table.alias} AS
            SELECT * FROM temp_df
        """)
        to_engine.unregister("temp_df")

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
