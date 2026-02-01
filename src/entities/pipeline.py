from pydantic import BaseModel, Field
from typing import List
from src.entities.definition import Definition
from src.entities.output import Output

class Pipeline(BaseModel):
    name: str = Field(default="")
    description: str = Field(default="")
    definition: Definition = Field(default_factory=Definition)
    outputs: List[Output] = Field(default_factory=list)

    def run(self) -> None:
        if self.name:
            print(f"Running pipeline: {self.name}")
        self.definition.run()