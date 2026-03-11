import io
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pa_csv
from duckdb import DuckDBPyConnection
from pydantic import BaseModel, Field
from sqlalchemy.engine import Engine, create_engine
from tqdm import tqdm

from src.config import memory_database
from src.config.settings import PG_MAINTENANCE_WORK_MEM, PG_WORK_MEM
from src.exceptions import OmniQueryError, OutputError
from src.utils.database_config_reader import get_database_config
from src.utils.retry import db_retry

logger = logging.getLogger(__name__)

# Mapeamento de tipos DuckDB → PostgreSQL
_DUCKDB_TO_PG: dict[str, str] = {
    "INTEGER": "INTEGER",
    "INT4": "INTEGER",
    "INT": "INTEGER",
    "SIGNED": "INTEGER",
    "BIGINT": "BIGINT",
    "INT8": "BIGINT",
    "LONG": "BIGINT",
    "HUGEINT": "NUMERIC",
    "UINTEGER": "BIGINT",
    "UBIGINT": "NUMERIC",
    "SMALLINT": "SMALLINT",
    "INT2": "SMALLINT",
    "SHORT": "SMALLINT",
    "TINYINT": "SMALLINT",
    "INT1": "SMALLINT",
    "FLOAT": "REAL",
    "FLOAT4": "REAL",
    "REAL": "REAL",
    "DOUBLE": "DOUBLE PRECISION",
    "FLOAT8": "DOUBLE PRECISION",
    "BOOLEAN": "BOOLEAN",
    "BOOL": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ",
    "TIMESTAMPTZ": "TIMESTAMPTZ",
    "TIME": "TIME",
    "BLOB": "BYTEA",
    "BYTEA": "BYTEA",
    "UUID": "UUID",
    "JSON": "JSONB",
    "INTERVAL": "INTERVAL",
}


def _duckdb_type_to_pg(duckdb_type: str) -> str:
    """Mapeia tipo DuckDB para PostgreSQL equivalente. Fallback: TEXT."""
    t = duckdb_type.upper()
    base = t.split("(")[0].strip()
    if base in ("DECIMAL", "NUMERIC"):
        return duckdb_type.upper().replace("DECIMAL", "NUMERIC")
    if base in ("VARCHAR", "TEXT", "STRING", "CHAR", "CHARACTER VARYING", "BPCHAR"):
        return "TEXT"
    return _DUCKDB_TO_PG.get(base, "TEXT")


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
        Transfere dados do DuckDB para PostgreSQL via pipe anônimo.
        DuckDB → Arrow batches → CSV bytes → pipe → PostgreSQL COPY FROM STDIN.
        Elimina I/O de disco e serialização Python linha a linha.
        """
        tag = f"[out:{name}]"
        conn = output_database.raw_connection()

        logger.debug("%s if_exists=%s", tag, self.options.get("if_exists", "replace"))

        try:
            cur = conn.cursor()
            cur.execute("SET synchronous_commit TO OFF")
            cur.execute(f"SET work_mem = '{PG_WORK_MEM}'")
            cur.execute(f"SET maintenance_work_mem = '{PG_MAINTENANCE_WORK_MEM}'")

            if self.options.get("if_exists", "replace") == "replace":
                cur.execute(f"DROP TABLE IF EXISTS {name}")

            # Schema com tipos reais
            describe = source_database.execute(f"DESCRIBE SELECT * FROM ({query}) __q")
            schema_info = describe.fetchall()
            columns = [row[0].lower() for row in schema_info]
            columns_def = ", ".join(f'"{row[0].lower()}" {_duckdb_type_to_pg(row[1])}' for row in schema_info)
            cur.execute(f"CREATE UNLOGGED TABLE {name} ({columns_def})")
            conn.commit()

            columns_sql = ", ".join(f'"{col}"' for col in columns)
            transfer_start = time.time()

            logger.info("%s Streaming DuckDB → PostgreSQL...", tag)

            # Pipe anônimo: writer thread (DuckDB → CSV bytes) → reader (PostgreSQL COPY)
            r_fd, w_fd = os.pipe()
            write_error: list[Exception] = []
            write_opts = pa_csv.WriteOptions(include_header=False, null_string="\\N")

            def _write_batches() -> None:
                try:
                    with os.fdopen(w_fd, "wb") as wf:
                        reader = source_database.execute(query).fetch_record_batch(rows_per_batch=500_000)
                        for batch in reader:
                            buf = io.BytesIO()
                            pa_csv.write_csv(pa.Table.from_batches([batch]), buf, write_options=write_opts)
                            wf.write(buf.getvalue())
                except Exception as exc:
                    write_error.append(exc)

            writer = threading.Thread(target=_write_batches, daemon=True)
            writer.start()

            with os.fdopen(r_fd, "rb") as rf:
                cur.copy_expert(  # type: ignore[attr-defined]
                    f"COPY {name} ({columns_sql}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                    rf,
                )

            writer.join()
            if write_error:
                raise write_error[0]

            conn.commit()

            cur.execute(f"SELECT COUNT(*) FROM {name}")
            total_rows = cur.fetchone()[0]  # type: ignore[index]

            transfer_time = time.time() - transfer_start
            avg_speed = total_rows / transfer_time if transfer_time > 0 else 0

            logger.info(
                "%s Done — %s rows | %.2fs | %s rows/s",
                tag,
                f"{total_rows:,}",
                transfer_time,
                f"{avg_speed:,.0f}",
            )

        except OmniQueryError:
            raise
        except Exception as e:
            logger.error("%s Failed — %s", tag, e)
            raise OutputError(f"Failed to transfer data to table '{name}'") from e

        finally:
            cur.close()
            conn.close()

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
        Transfere dados do DuckDB para arquivo.
        CSV: DuckDB native COPY (sem pandas). Excel: pandas em memória.
        """
        filepath = Path(file_path)
        filepath.parent.mkdir(parents=True, exist_ok=True)

        ext = filepath.suffix.lower()
        is_excel = ext in (".xlsx", ".xls")
        fmt_label = "Excel" if is_excel else "CSV"
        tag = f"[out:{filepath.name}]"

        logger.info("%s Writing %s...", tag, fmt_label)

        total_rows = 0
        transfer_start = time.time()

        try:
            if is_excel:
                cursor = source_engine.execute(query)
                columns = [desc[0] for desc in cursor.description]
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
                # DuckDB native COPY — sem loop Python nem pandas
                duckdb_path = str(filepath).replace("\\", "/")
                source_engine.execute(f"COPY ({query}) TO '{duckdb_path}' (FORMAT CSV, HEADER TRUE)")
                with open(filepath, encoding="utf-8") as f:
                    total_rows = sum(1 for _ in f) - 1  # descontar cabeçalho

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
