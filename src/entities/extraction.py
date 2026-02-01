from pydantic import BaseModel, Field
from typing import List
from src.entities.extract import Extract


class Extraction(BaseModel):
    source: str = Field(default_factory=str)
    extract: List[Extract] = Field(default_factory=list)

    def run(self) -> None:
        print(f"Running extraction from source: {self.source}")
        for ext in self.extract:
            ext.run()