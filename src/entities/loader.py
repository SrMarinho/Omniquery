import time
import pandas as pd
import yaml
from pydantic import BaseModel, Field, ConfigDict, model_validator
from typing import List, Dict, Any, Optional
from sqlalchemy.engine import create_engine, Engine
from sqlalchemy import text
from duckdb import DuckDBPyConnection
from src.entities.table import Table
from src.utils.database_config_reader import get_database_config
from src.config import memory_database


class Loader(BaseModel):
    type: str
    source: str = Field(default_factory=str)
    tables: List[Table] = Field(default_factory=list)
    
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
        print(f"Running loads from source: {self.source}")
        try:
            source_engine = self.get_engine(self.source)
        except Exception as e:
            print(e)
        else:
            for table in self.tables:
                self._transfer(source_engine, memory_database, table)
    
    def _transfer(self, source_engine: Engine, to_engine: DuckDBPyConnection, table: Table) -> None:
        """Transfere dados de um engine SQLAlchemy para DuckDB."""
        to_engine.execute("PRAGMA threads=4")
        to_engine.execute("PRAGMA memory_limit='4GB'")

        total_rows = 0
        start_time = time.time()

        
        print(f"Transfering data from {self.source} to DuckDB table: {table.alias}")
        
        duck_conn = to_engine
        try:
            query = table.content
            chunk_size = 100000
            
            first_chunk = True
            
            for chunk_df in pd.read_sql(query, source_engine, chunksize=chunk_size):
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
                    else:
                        duck_conn.execute(f"""
                            INSERT INTO {table.alias} 
                            SELECT * FROM temp_df
                        """)
                    
                    duck_conn.unregister('temp_df')
                
                chunk_time = time.time() - chunk_start
                total_rows += chunk_rows
                
                print(f"  → Lote de {chunk_rows:,} linhas")
                
                del chunk_df
            
            total_time = time.time() - start_time
            print(f"✅ Completed transfer: {total_rows} total rows to {table.alias} "
                f"em {total_time:.2f}s (média: {total_rows/total_time:.0f} linhas/s)\n")
        except Exception as e:
            print(f"❌ Erro ao transferir dados para {table.alias}: {e}")

class FileLoader(Loader):
    type: str = "file"

    def run(self) -> None:
        print(f"Running loads from source: {self.source}")
        for table in self.tables:
            pass

class LoaderFactory:
    loader_types = {
        "database": DatabaseLoader,
        "file": FileLoader,
    }
    @staticmethod
    def create(config: Dict[str, Any]) -> Loader:
        loader_type: str = config.get("type", "")
        if loader_type.lower() in LoaderFactory.loader_types:
            return LoaderFactory.loader_types[loader_type.lower()](**config)
        
        return Loader(**config)