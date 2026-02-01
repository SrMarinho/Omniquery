from pydantic import BaseModel, Field
from typing import List
from src.entities.extraction import Extraction


class Definition(BaseModel):
    extraction: List[Extraction] = Field(default_factory=list)
    transformation: List[dict] = Field(default_factory=list)
    load: dict = Field(default_factory=dict)

    def run(self) -> None:
        for ext in self.extraction:
            ext.run()
        print("Executing transformations")
        print("Loading data")