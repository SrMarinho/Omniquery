from pydantic import BaseModel, Field
from typing import List
from src.entities.load import Load
from src.entities.output import Output

class Pipeline(BaseModel):
    name: str = Field(default="")
    description: str = Field(default="")
    loads: List[Load] = Field(default_factory=list)
    outputs: List[Output] = Field(default_factory=list)

    def run(self) -> None:
        if self.name:
            print(f"Running pipeline: {self.name}")
        for load in self.loads:
            load.run()