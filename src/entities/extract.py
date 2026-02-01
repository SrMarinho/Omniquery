from pydantic import BaseModel, Field
from src.types.extract_types import ExtractType


class Extract(BaseModel):
    alias: str = Field()
    description: str = Field(default="")
    type: ExtractType = Field(default=ExtractType.INLINE)
    content: str = Field(default="")

    def run(self) -> None:
        print(f"Running extract: {self.alias} of type {self.type.value}")
 