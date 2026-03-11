import time
from typing import Any

import pandas as pd
from duckdb import DuckDBPyConnection
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.engine import Engine, create_engine

from src.config import memory_database
from src.entities.table import Table
from src.utils.database_config_reader import get_database_config


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

    def get_engine(self, database: str) -> Engine:
        config = get_database_config(database)

        connection_string: str = config["connection_string"]

        return create_engine(connection_string)

    def run(self) -> None:
        """Executa a transferência de dados da fonte para o DuckDB."""
        job_start = time.time()

        print("─" * 60)
        print(f"🚀 Starting bulk transfer from source: {self.source}")
        print(f"📋 Tables to process: {len(self.tables)}")
        for i, table in enumerate(self.tables[:3], 1):
            print(f"   {i}. {table.alias}")
        if len(self.tables) > 3:
            print(f"   ... and {len(self.tables) - 3} more")
        print("─" * 60)

        try:
            source_engine = self.get_engine(self.source)
            print(f"✅ Source connection established: {self.source}")

            tables_processed = 0

            for table in self.tables:
                table_start = time.time()
                print(f"\n📦 Processing table: {table.alias}")

                self._transfer(source_engine, memory_database, table)

                table_time = time.time() - table_start
                tables_processed += 1
                print(f"  ✅ Table completed: {table.alias} ⏱️  {table_time:.2f}s")

            total_time = time.time() - job_start

            print("─" * 60)
            print("🎉 BULK TRANSFER COMPLETED SUCCESSFULLY!")
            print("📊 Summary:")
            print(f"   • Source:           {self.source}")
            print(f"   • Tables processed: {tables_processed}/{len(self.tables)}")
            print(f"   • Total time:       {total_time:>15.2f}s")
            print("─" * 60)

        except Exception as e:
            job_time = time.time() - job_start
            print("❌" * 30)
            print("🔴 BULK TRANSFER FAILED")
            print(f"⚠️  Source: {self.source}")
            print(f"⚠️  Error: {str(e)}")
            if 'tables_processed' in locals():
                print(f"📋 Tables processed before error: {tables_processed}/{len(self.tables)}")
            print(f"⏱️  Failed after: {job_time:.2f}s")
            print("❌" * 30)
            raise

    def _transfer(self, source_engine: Engine, to_engine: DuckDBPyConnection, table: Table) -> None:
        """Transfers data from a SQLAlchemy engine to DuckDB."""

        to_engine.execute("PRAGMA threads=4")
        to_engine.execute("PRAGMA memory_limit='4GB'")

        print(f"📤 Starting transfer: {self.source} ➔ DuckDB [Table: {table.alias}]")
        print("⚙️  Settings: threads=4 | memory_limit=4GB")
        print("─" * 60)

        duck_conn = to_engine
        total_rows = 0
        transfer_start = time.time()  # ⬅️ Tempo da transferência

        try:
            query = table.content
            chunk_size = 500000

            first_chunk = True

            for i, chunk_df in enumerate(pd.read_sql(query, source_engine, chunksize=chunk_size), 1):
                chunk_start = time.time()

                chunk_df.columns = chunk_df.columns.str.lower()
                chunk_rows = len(chunk_df)

                if duck_conn:
                    duck_conn.register('temp_df', chunk_df)

                    if first_chunk:
                        duck_conn.execute(f"""
                            CREATE OR REPLACE TABLE {table.alias} AS
                            SELECT * FROM temp_df
                        """)
                        first_chunk = False
                        operation = "🆗 Created"
                    else:
                        duck_conn.execute(f"""
                            INSERT INTO {table.alias}
                            SELECT * FROM temp_df
                        """)
                        operation = "➕ Appended"

                    duck_conn.unregister('temp_df')

                chunk_time = time.time() - chunk_start
                total_rows += chunk_rows

                print(f"  Batch #{i:2d} {operation:12s} : {chunk_rows:10,} rows "
                    f"⏱️  {chunk_time:5.2f}s")

                del chunk_df

            transfer_time = time.time() - transfer_start

            avg_speed = total_rows / transfer_time if transfer_time > 0 else 0

            print("─" * 60)
            print("✅ Transfer completed successfully!")
            print(f"📊 Final summary for table {table.alias}:")
            print(f"   • Total records:     {total_rows:>15,}")
            print(f"   • Transfer time:     {transfer_time:>15.2f}s")
            print(f"   • Average speed:     {avg_speed:>15,.0f} rows/s")
            print("\n")

        except Exception as e:
            print("❌" * 30)
            print(f"🔴 ERROR transferring data for {table.alias}")
            print(f"⚠️  Details: {str(e)}")
            print(f"📋 Records processed before error: {total_rows:,}")
            print("❌" * 30)

class FileLoader(Loader):
    type: str = "file"

    def _transfer(self, source: str, to_engine: DuckDBPyConnection, table: Table) -> None:
        """Transfers data from a file to DuckDB."""
        df = pd.read_csv(source)
        df.columns = df.columns.str.lower()
        to_engine.register('temp_df', df)
        to_engine.execute(f"""
            CREATE OR REPLACE TABLE {table.alias} AS
            SELECT * FROM temp_df
        """)
        to_engine.unregister('temp_df')

    def run(self) -> None:
        print(f"Running loads from source: {self.source}")
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
