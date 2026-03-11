import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Any

import pandas as pd
from duckdb import DuckDBPyConnection
from pydantic import BaseModel, Field
from sqlalchemy.engine import Engine, create_engine
from tqdm import tqdm

from src.config import memory_database
from src.config.settings import FILE_CHUNK_SIZE
from src.exceptions import OmniQueryError, OutputError
from src.utils.database_config_reader import get_database_config
from src.utils.retry import db_retry

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
        Transfere dados do DuckDB para PostgreSQL via arquivo CSV temporário.
        DuckDB exporta nativamente (sem serialização Python), PostgreSQL importa via COPY.
        """
        tag = f"[out:{name}]"
        conn = output_database.raw_connection()
        tmp_path: str | None = None

        logger.debug("%s if_exists=%s", tag, self.options.get("if_exists", "replace"))

        try:
            cur = conn.cursor()
            cur.execute("SET synchronous_commit TO OFF")

            if self.options.get("if_exists", "replace") == "replace":
                cur.execute(f"DROP TABLE IF EXISTS {name}")

            # Obtém schema sem buscar dados
            schema = source_database.execute(f"SELECT * FROM ({query}) __q LIMIT 0")
            columns = [desc[0].lower() for desc in schema.description]
            columns_def = ", ".join([f'"{col}" TEXT' for col in columns])
            cur.execute(f"CREATE UNLOGGED TABLE {name} ({columns_def})")
            conn.commit()

            # DuckDB exporta diretamente para CSV — sem loop Python
            tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv")
            os.close(tmp_fd)
            duckdb_path = tmp_path.replace("\\", "/")

            transfer_start = time.time()

            logger.info("%s Exporting from DuckDB...", tag)
            export_start = time.time()
            source_database.execute(f"COPY ({query}) TO '{duckdb_path}' (FORMAT CSV, HEADER FALSE, NULL '\\N')")
            export_time = time.time() - export_start
            file_size = os.path.getsize(tmp_path)
            logger.debug("%s DuckDB export — %.2f MB in %.2fs", tag, file_size / (1024 * 1024), export_time)

            # PostgreSQL importa via streaming do arquivo
            columns_sql = ", ".join(f'"{col}"' for col in columns)
            logger.info("%s Importing to PostgreSQL...", tag)
            with tqdm(desc=name, unit="B", unit_scale=True, unit_divisor=1024, total=file_size, leave=False) as pbar:

                class _ProgressReader:
                    def __init__(self, f: Any, pb: Any) -> None:
                        self._f = f
                        self._pb = pb

                    def read(self, size: int = -1) -> str:
                        chunk: str = self._f.read(size)
                        self._pb.update(len(chunk.encode("utf-8")))
                        return chunk

                with open(tmp_path, encoding="utf-8") as f:
                    cur.copy_expert(  # type: ignore[attr-defined]
                        f"COPY {name} ({columns_sql}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                        _ProgressReader(f, pbar),
                    )

            conn.commit()

            cur.execute(f"SELECT COUNT(*) FROM {name}")
            total_rows = cur.fetchone()[0]  # type: ignore[index]

            transfer_time = time.time() - transfer_start
            file_size_mb = file_size / (1024 * 1024)
            avg_speed = total_rows / transfer_time if transfer_time > 0 else 0
            speed_mb_s = file_size_mb / transfer_time if transfer_time > 0 else 0

            logger.info(
                "%s Done — %s rows | %.2f MB | %.2fs | %s rows/s (%.2f MB/s)",
                tag,
                f"{total_rows:,}",
                file_size_mb,
                transfer_time,
                f"{avg_speed:,.0f}",
                speed_mb_s,
            )

        except OmniQueryError:
            raise
        except Exception as e:
            logger.error("%s Failed — %s", tag, e)
            raise OutputError(f"Failed to transfer data to table '{name}'") from e

        finally:
            cur.close()
            conn.close()
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)

    @db_retry
    def _get_engine(self) -> Engine:
        output_connection_string: str = get_database_config(self.output_database)["connection_string"]
        engine = create_engine(output_connection_string)
        with engine.connect():
            pass
        return engine

    def run(self) -> None:
        """Executa a transferência de dados do DuckDB para o banco de dados de destino."""
        tag = f"[out:{self.name}]"
        job_start = time.time()

        logger.info("%s -> %s", tag, self.output_database)

        try:
            output_engine = self._get_engine()
            self._transfer(memory_database, output_engine, self.name, self.query)

        except OmniQueryError:
            raise
        except Exception as e:
            elapsed = time.time() - job_start
            logger.error("%s Failed — %s (%.2fs)", tag, e, elapsed)
            raise OutputError(f"Job failed for '{self.name}'") from e


class FileOutput(Output):
    type: str = "file"

    def _transfer(self, source_engine: DuckDBPyConnection, file_path: str, query: str) -> None:
        """
        Transfere dados do DuckDB para arquivo (CSV em chunks, Excel em memória).
        """
        filepath = Path(file_path)
        filepath.parent.mkdir(parents=True, exist_ok=True)

        ext = filepath.suffix.lower()
        is_excel = ext in (".xlsx", ".xls")
        fmt_label = "Excel" if is_excel else "CSV"
        tag = f"[out:{filepath.name}]"

        logger.info("%s Writing %s...", tag, fmt_label)
        logger.debug("%s chunk_size=%s | path=%s", tag, f"{FILE_CHUNK_SIZE:,}", file_path)

        total_rows = 0
        transfer_start = time.time()

        cursor = source_engine.execute(query)
        columns = [desc[0] for desc in cursor.description]

        try:
            if is_excel:
                all_rows = cursor.fetchall()
                df = pd.DataFrame(all_rows, columns=columns)
                df.columns = df.columns.str.lower()
                total_rows = len(df)

                with tqdm(desc=filepath.name, unit="file", leave=False, total=1) as pbar:
                    df.to_excel(filepath, index=False, engine="openpyxl")
                    pbar.update(1)
                    pbar.set_postfix({"rows": f"{total_rows:,}"})

                del df
            else:
                first_chunk = True
                chunk_count = 0

                with tqdm(desc=filepath.name, unit="chunk", leave=False) as pbar:
                    while True:
                        rows = cursor.fetchmany(FILE_CHUNK_SIZE)
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
                        pbar.update(1)
                        pbar.set_postfix({"rows": f"{total_rows:,}", "op": operation})
                        logger.debug(
                            "%s chunk #%d %s — %s rows in %.2fs",
                            tag,
                            chunk_count,
                            operation,
                            f"{chunk_rows:,}",
                            chunk_time,
                        )
                        del chunk_df

            transfer_time = time.time() - transfer_start

            if total_rows > 0:
                file_size = filepath.stat().st_size
                avg_speed = total_rows / transfer_time if transfer_time > 0 else 0
                file_size_mb = file_size / (1024 * 1024)
                speed_mb_s = file_size_mb / transfer_time if transfer_time > 0 else 0
                logger.info(
                    "%s Done — %s rows | %.2f MB | %.2fs | %s rows/s (%.2f MB/s)",
                    tag,
                    f"{total_rows:,}",
                    file_size_mb,
                    transfer_time,
                    f"{avg_speed:,.0f}",
                    speed_mb_s,
                )
            else:
                logger.warning("%s No data written", tag)

        except OmniQueryError:
            raise
        except Exception as e:
            logger.error("%s Failed — %s (rows so far: %s)", tag, e, f"{total_rows:,}")
            if filepath.exists() and total_rows == 0:
                filepath.unlink()
                logger.info("%s Removed incomplete file", tag)
            raise OutputError(f"Failed to write file '{filepath.name}'") from e

    def run(self) -> None:
        tag = f"[out:{Path(self.name).name}]"
        job_start = time.time()

        logger.info("%s Writing file...", tag)

        try:
            self._transfer(memory_database, self.name, self.query)

        except OmniQueryError:
            raise
        except Exception as e:
            elapsed = time.time() - job_start
            logger.error("%s Failed — %s (%.2fs)", tag, e, elapsed)
            raise OutputError(f"Job failed for '{self.name}'") from e


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
