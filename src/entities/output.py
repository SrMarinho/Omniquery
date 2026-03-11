import io
import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd
from duckdb import DuckDBPyConnection
from pydantic import BaseModel, Field
from sqlalchemy.engine import Engine, create_engine

from src.config import memory_database
from src.utils.database_config_reader import get_database_config

logger = logging.getLogger(__name__)


class Output(BaseModel):
    type: str
    name: str
    query: str
    options: dict = Field(default_factory=dict)

    def run(self) -> None:
        logger.info("Writing output type: %s", type(self).__name__)

class DatabaseOutput(Output):
    type: str = "database"
    output_database: str = Field(default="postgresql")

    def _transfer(self, source_database: DuckDBPyConnection, output_database: Engine, name: str, query: str) -> None:
        """
        Transfere dados do DuckDB para PostgreSQL em batches usando fetchmany.
        """
        BATCH_SIZE = 500000

        conn = output_database.raw_connection()

        logger.info("Starting transfer: DuckDB → PostgreSQL [table: %s]", name)
        logger.debug("Settings: synchronous_commit=OFF | if_exists=%s | batch_size=%s", self.options.get('if_exists', 'replace'), BATCH_SIZE)
        logger.info("─" * 60)

        try:
            cur = conn.cursor()
            cur.execute("SET synchronous_commit TO OFF")

            if self.options.get("if_exists", "replace") == "replace":
                cur.execute(f'DROP TABLE IF EXISTS {name}')

            logger.info("Executing query...")
            query_start = time.time()
            result = source_database.execute(query)
            columns = [desc[0].lower() for desc in result.description]
            query_time = time.time() - query_start
            logger.debug("Query executed — columns: %s (%.2fs)", columns, query_time)

            columns_def = ', '.join([f'"{col}" TEXT' for col in columns])
            cur.execute(f'CREATE TABLE {name} ({columns_def})')
            conn.commit()

            transfer_start = time.time()
            logger.info("Transferring data in batches of %s rows...", f"{BATCH_SIZE:,}")

            batch_num = 0
            total_rows = 0
            data_size = 0

            while True:
                rows = result.fetchmany(BATCH_SIZE)
                if not rows:
                    break

                batch_num += 1
                total_rows += len(rows)

                output = io.StringIO()
                for row in rows:
                    line = '\t'.join(['\\N' if v is None else str(v) for v in row])
                    data_size += len(line.encode('utf-8')) + 1
                    output.write(line + '\n')

                output.seek(0)

                cur.copy_from(output, name, sep='\t', null='\\N', columns=columns)  # type: ignore[attr-defined]
                conn.commit()

            cur.execute(f"ANALYZE {name}")
            conn.commit()

            transfer_time = time.time() - transfer_start

            avg_speed = total_rows / transfer_time if transfer_time > 0 else 0
            data_size_mb = data_size / (1024 * 1024)
            speed_mb_s = data_size_mb / transfer_time if transfer_time > 0 else 0

            logger.info("─" * 60)
            logger.info("Transfer completed: %s", name)
            logger.info("  Total records:     %s", f"{total_rows:,}")
            logger.info("  Data size:         %.2f MB", data_size_mb)
            logger.info("  Batches processed: %d", batch_num)
            logger.info("  Transfer time:     %.2fs", transfer_time)
            logger.info("  Average speed:     %s rows/s (%.2f MB/s)", f"{avg_speed:,.0f}", speed_mb_s)

        except Exception as e:
            logger.error("ERROR transferring data for %s: %s", name, e)
            if 'total_rows' in locals():
                logger.error("  Records processed before error: %s", f"{total_rows:,}")
            if 'batch_num' in locals():
                logger.error("  Batches completed before error: %d", batch_num)

        finally:
            cur.close()
            conn.close()

    def run(self) -> None:
        """Executa a transferência de dados do DuckDB para o banco de dados de destino."""
        job_start = time.time()

        logger.info("─" * 60)
        logger.info("Starting job: %s → %s", self.output_database, self.name)
        logger.info("─" * 60)

        try:
            output_connection_string: str = get_database_config(self.output_database)["connection_string"]
            output_engine = create_engine(output_connection_string)

            self._transfer(memory_database, output_engine, self.name, self.query)

            job_time = time.time() - job_start
            logger.info("Job completed: %s › %s (%.2fs)", self.output_database, self.name, job_time)

        except Exception as e:
            job_time = time.time() - job_start
            logger.error("JOB FAILED: %s › %s — %s (%.2fs)", self.output_database, self.name, e, job_time)
            raise

class FileOutput(Output):
    type: str = "file"

    def _transfer(self, source_engine: DuckDBPyConnection, file_path: str, query: str) -> None:
        """
        Transfere dados do DuckDB para arquivo CSV em chunks.
        """
        filepath = Path(file_path)
        filepath.parent.mkdir(parents=True, exist_ok=True)

        chunk_size = 100000

        logger.info("Starting transfer: DuckDB → CSV [file: %s]", filepath.name)
        logger.debug("Settings: chunk_size=%s | location=%s", f"{chunk_size:,}", file_path)
        logger.info("─" * 60)

        total_rows = 0
        transfer_start = time.time()
        first_chunk = True
        chunk_count = 0

        cursor = source_engine.execute(query)
        columns = [desc[0] for desc in cursor.description]

        try:
            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break

                chunk_count += 1
                chunk_start = time.time()

                chunk_df = pd.DataFrame(rows, columns=columns)
                chunk_df.columns = chunk_df.columns.str.lower()
                chunk_rows = len(chunk_df)

                if first_chunk:
                    chunk_df.to_csv(filepath, index=False)
                    operation = "Created"
                    first_chunk = False
                else:
                    chunk_df.to_csv(filepath, mode="a", index=False, header=False)
                    operation = "Appended"

                chunk_time = time.time() - chunk_start
                total_rows += chunk_rows

                logger.info("  Batch #%2d %-10s : %10s rows  %.2fs", chunk_count, operation, f"{chunk_rows:,}", chunk_time)

                del chunk_df

            transfer_time = time.time() - transfer_start

            if total_rows > 0:
                file_size = filepath.stat().st_size
                avg_speed = total_rows / transfer_time if transfer_time > 0 else 0
                file_size_mb = file_size / (1024 * 1024)
                speed_mb_s = file_size_mb / transfer_time if transfer_time > 0 else 0

                logger.info("─" * 60)
                logger.info("Transfer completed: %s", filepath.name)
                logger.info("  Total records:    %s", f"{total_rows:,}")
                logger.info("  File size:        %.2f MB", file_size_mb)
                logger.info("  Chunks:           %s", f"{chunk_count:,}")
                logger.info("  Transfer time:    %.2fs", transfer_time)
                logger.info("  Average speed:    %s rows/s (%.2f MB/s)", f"{avg_speed:,.0f}", speed_mb_s)
            else:
                logger.warning("No data transferred for %s", filepath.name)

        except Exception as e:
            logger.error("ERROR transferring data to %s: %s (records processed: %s)", filepath.name, e, f"{total_rows:,}")
            if filepath.exists() and total_rows == 0:
                filepath.unlink()
                logger.info("Removed incomplete file: %s", filepath.name)
            raise


    def run(self) -> None:
        """Executa a transferência de dados."""

        logger.info("─" * 60)
        logger.info("Starting job: %s", self.name)
        logger.info("─" * 60)

        try:
            self._transfer(memory_database, self.name, self.query)
            logger.info("Job completed: %s", self.name)

        except Exception as e:
            logger.error("JOB FAILED: %s — %s", self.name, e)
            raise

class OutputFactory:
    output_types = {
        "database": DatabaseOutput,
        "file": FileOutput,
    }
    @staticmethod
    def create(config: dict[str, Any]) -> Output:
        output_type: str = config.get("type", "")
        if output_type.lower() in OutputFactory.output_types:
            return OutputFactory.output_types[output_type.lower()](**config)  # type: ignore[no-any-return]

        return Output(**config)
