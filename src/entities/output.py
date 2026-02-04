from pathlib import Path
from pydantic import BaseModel, Field, model_validator
from typing import Dict, Any, Type
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import create_engine, Engine
from src.types.file_output_format_types import FileOutputFormatTypes
from src.utils.database_config_reader import get_database_config
from src.config import memory_database

class Output(BaseModel):
    type: str
    name: str
    query: str
    options: Dict = Field(default_factory=dict)

    def run(self) -> None:
        print(f"Writing output type: {type(self)}")

class DatabaseOutput(Output):
    type: str = "database" 
    source_database: str = "memory"
    output_database: str = Field(default="postgresql")
    
    def _transfer(self, source_database: Engine, output_database: Engine, name: str, query: str) -> None:
        """Transfere dados em chunks para evitar sobrecarga de memória."""
        
        chunk_size = 50000
        total_rows = 0
        
        first_chunk = True

        for chunk_df in pd.read_sql(query, source_database, chunksize=chunk_size):
            print(f"Processing transfering data from {self.source_database} to application database table: {name}")
            
            chunk_df.columns = chunk_df.columns.str.lower()

            if first_chunk:
                chunk_df.to_sql(
                    name=name,
                    con=output_database,
                    if_exists=self.options.get("if_exists", "replace"),
                    index=False,
                    chunksize=chunk_size,
                    method='multi'
                )
                first_chunk = False
            else:
                chunk_df.to_sql(
                    name=name,
                    con=output_database,
                    if_exists='append',
                    index=False,
                    chunksize=chunk_size,
                    method='multi'
                )
            
            total_rows += len(chunk_df)
            del chunk_df
        
        print(f"Completed transfer: {total_rows} total rows to {self.name}")

    def run(self) -> None:
        print(f"Writing in database {self.source_database}")
        output_connection_string: str = get_database_config(self.output_database)["connection_string"]
        output_engine = create_engine(output_connection_string)
        self._transfer(memory_database, output_engine, self.name, self.query)

class FileOutput(Output):
    type: str = "file" 

    def _transfer(self, source_engine: Engine, file_path: str, query: str) -> None:
        filepath = Path(file_path)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        chunk_size = 50000
        total_rows = 0
        
        first_chunk = True

        for chunk_df in pd.read_sql(query, source_engine, chunksize=chunk_size):
            print(f"Processing transfering data to file: {file_path}")
            
            chunk_df.columns = chunk_df.columns.str.lower()

            if first_chunk:
                chunk_df.to_csv(file_path, index=False)
                first_chunk = False
            else:
                chunk_df.to_csv(file_path, mode="a", index=False)
            total_rows += len(chunk_df)
            del chunk_df


    def run(self) -> None:
        print(f"Writing in file: {self.name}")
        self._transfer(memory_database, self.name, self.query)


class APIOutput(Output):
    type: str = "api" 
    endpoint: str = Field(default="")
    method: str = Field(default="POST")

    def run(self) -> None:
        print("Writing in API")

class OutputFactory:
    output_types = {
        "database": DatabaseOutput,
        "file": FileOutput,
        "api": APIOutput
    }
    @staticmethod
    def create(config: Dict[str, Any]) -> Output:
        output_type: str = config.get("type", "")
        if output_type.lower() in OutputFactory.output_types:
            return OutputFactory.output_types[output_type.lower()](**config)
        
        return Output(**config)
