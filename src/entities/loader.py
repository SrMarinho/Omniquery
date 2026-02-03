import time
import pandas as pd
import yaml
from pydantic import BaseModel, Field, ConfigDict, model_validator
from typing import List, Dict, Any, Optional
from sqlalchemy.engine import create_engine, Engine
from sqlalchemy import text
from src.entities.table import Table
from src.utils.database_config_reader import get_database_config


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
            database_engine = self.get_engine(self.database)
        except Exception as e:
            print(e)
        else:
            for table in self.tables:
                self._transfer_with_chunking(source_engine, database_engine, table)
    
    def _transfer_with_chunking(self, source_engine: Engine, to_engine: Engine, table: Table) -> None:
        """Transfere dados em chunks para evitar sobrecarga de memória."""
        
        chunk_size = 50000
        total_rows = 0
        
        first_chunk = True

        query = table.content

        for chunk_df in pd.read_sql(query, source_engine, chunksize=chunk_size):
            print(f"Processing transfering data from {self.source} to application database")
            
            if first_chunk:
                chunk_df.to_sql(
                    name=table.alias,
                    con=to_engine,
                    if_exists=table.options.get("if_exists", "replace"),
                    index=False,
                    chunksize=chunk_size,
                    method='multi'
                )
                first_chunk = False
            else:
                chunk_df.to_sql(
                    name=table.alias,
                    con=to_engine,
                    if_exists='append',
                    index=False,
                    chunksize=chunk_size,
                    method='multi'
                )
            
            total_rows += len(chunk_df)
            del chunk_df
        
        print(f"Completed transfer: {total_rows} total rows to {table.alias}")

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