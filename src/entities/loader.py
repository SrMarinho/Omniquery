import time
import yaml
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from sqlalchemy.engine import create_engine, Engine
from src.entities.table import Table


class Loader(BaseModel):
    source: str = Field(default_factory=str)
    tables: List[Table] = Field(default_factory=list)

    def _get_type(self) -> str:
        return "database"

    def run(self) -> None:
        raise NotImplementedError("Loader not implemented yet")

class DatabaseLoader(Loader):
    engine: Optional[Engine] = None
    database_config_file: str = "databases"
    config: dict = Field(default_factory=dict)
    
    def __init__(self, **data):
        super().__init__(**data)
        if self.engine is None:
            self.engine = self._create_engine()
    
    def _create_engine(self) -> Engine:
        connection_string = self.config.get(
            "connection_string", 
            "duckdb:///:memory:"
        )
        return create_engine(connection_string)
    
    def get_config(self):
        database_config = yaml.safe_load(self.database_confi_file)
        print(database_config)

    
    def run(self) -> None:
        print(f"Running loads from source: {self.source}")
        for table in self.tables:
            table.run()

class FileLoader(Loader):
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