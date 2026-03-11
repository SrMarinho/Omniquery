import logging
import time
from typing import Any

import pandas as pd
from duckdb import DuckDBPyConnection
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.engine import Engine, create_engine

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
        """Executa a transferência de dados da fonte para o DuckDB."""
        job_start = time.time()

        logger.info("─" * 60)
        logger.info("Starting bulk transfer from source: %s", self.source)
        logger.info("Tables to process: %d", len(self.tables))
        for i, table in enumerate(self.tables[:3], 1):
            logger.info("  %d. %s", i, table.alias)
        if len(self.tables) > 3:
            logger.info("  ... and %d more", len(self.tables) - 3)
        logger.info("─" * 60)

        try:
            source_engine = self.get_engine(self.source)
            logger.info("Source connection established: %s", self.source)

            tables_processed = 0

            for table in self.tables:
                table_start = time.time()
                logger.info("Processing table: %s", table.alias)

                self._transfer(source_engine, memory_database, table)

                table_time = time.time() - table_start
                tables_processed += 1
                logger.info("Table completed: %s (%.2fs)", table.alias, table_time)

            total_time = time.time() - job_start

            logger.info("─" * 60)
            logger.info("BULK TRANSFER COMPLETED SUCCESSFULLY")
            logger.info("  Source:           %s", self.source)
            logger.info("  Tables processed: %d/%d", tables_processed, len(self.tables))
            logger.info("  Total time:       %.2fs", total_time)
            logger.info("─" * 60)

        except OmniQueryError:
            raise
        except Exception as e:
            job_time = time.time() - job_start
            logger.error("BULK TRANSFER FAILED — source: %s, error: %s (%.2fs)", self.source, e, job_time)
            if "tables_processed" in locals():
                logger.error("Tables processed before error: %d/%d", tables_processed, len(self.tables))
            raise LoaderError(f"Failed to load from source '{self.source}'") from e

    def _transfer(self, source_engine: Engine, to_engine: DuckDBPyConnection, table: Table) -> None:
        """Transfers data from a SQLAlchemy engine to DuckDB."""

        to_engine.execute(f"PRAGMA threads={DB_THREADS}")
        to_engine.execute(f"PRAGMA memory_limit='{DB_MEMORY_LIMIT}'")

        logger.info("Starting transfer: %s → DuckDB [table: %s]", self.source, table.alias)
        logger.debug("Settings: threads=%d | memory_limit=%s", DB_THREADS, DB_MEMORY_LIMIT)
        logger.info("─" * 60)

        duck_conn = to_engine
        total_rows = 0
        transfer_start = time.time()

        try:
            query = table.content
            first_chunk = True

            for i, chunk_df in enumerate(pd.read_sql(query, source_engine, chunksize=DB_CHUNK_SIZE), 1):
                chunk_start = time.time()

                chunk_df.columns = chunk_df.columns.str.lower()
                chunk_rows = len(chunk_df)

                if duck_conn:
                    duck_conn.register("temp_df", chunk_df)

                    if first_chunk:
                        duck_conn.execute(f"""
                            CREATE OR REPLACE TABLE {table.alias} AS
                            SELECT * FROM temp_df
                        """)
                        first_chunk = False
                        operation = "Created"
                    else:
                        duck_conn.execute(f"""
                            INSERT INTO {table.alias}
                            SELECT * FROM temp_df
                        """)
                        operation = "Appended"

                    duck_conn.unregister("temp_df")

                chunk_time = time.time() - chunk_start
                total_rows += chunk_rows

                logger.info("  Batch #%2d %-10s : %10s rows  %.2fs", i, operation, f"{chunk_rows:,}", chunk_time)

                del chunk_df

            transfer_time = time.time() - transfer_start

            avg_speed = total_rows / transfer_time if transfer_time > 0 else 0

            logger.info("─" * 60)
            logger.info("Transfer completed: %s", table.alias)
            logger.info("  Total records:  %s", f"{total_rows:,}")
            logger.info("  Transfer time:  %.2fs", transfer_time)
            logger.info("  Average speed:  %s rows/s", f"{avg_speed:,.0f}")

        except Exception as e:
            logger.error(
                "ERROR transferring data for %s: %s (records processed: %s)", table.alias, e, f"{total_rows:,}"
            )
            raise LoaderError(f"Failed to transfer table '{table.alias}'") from e


class FileLoader(Loader):
    type: str = "file"

    def _transfer(self, source: str, to_engine: DuckDBPyConnection, table: Table) -> None:
        """Transfers data from a file to DuckDB."""
        df = pd.read_csv(source)
        df.columns = df.columns.str.lower()
        to_engine.register("temp_df", df)
        to_engine.execute(f"""
            CREATE OR REPLACE TABLE {table.alias} AS
            SELECT * FROM temp_df
        """)
        to_engine.unregister("temp_df")

    def run(self) -> None:
        logger.info("Running loads from source: %s", self.source)
        for table in self.tables:
            self._transfer(self.source, memory_database, table)


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
