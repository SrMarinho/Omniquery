import time
import yaml
from pydantic import BaseModel, Field, ConfigDict
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
    connection_string: str = "duckdb:///:memory:"
    engine: Engine = create_engine(connection_string)
    
    def run(self) -> None:
        print(f"Running loads from source: {self.source}")

        for table in self.tables:
            with self.engine.connect() as conn:
                result = conn.execute(text(table.content))
                
                while rows := result.fetchmany(1000):
                    print(rows)

class FileLoader(Loader):
    type: str = "file"

    def run(self) -> None:
        print(f"Running loads from source: {self.source}")
        for table in self.tables:
            table.run()

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