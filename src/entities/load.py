import time
from pydantic import BaseModel, Field
from typing import List
from src.entities.table import Table


class Load(BaseModel):
    source: str = Field(default_factory=str)
    tables: List[Table] = Field(default_factory=list)

    def run(self) -> None:
        print(f"Running loads from source: {self.source}")
        for table in self.tables:
            table.run()