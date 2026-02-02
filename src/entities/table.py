from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from sqlalchemy.engine import Engine
from src.types.table_types import TableTypes

class Table(BaseModel):
    alias: str = Field()
    description: str = Field(default="")
    type: TableTypes = Field(default=TableTypes.INLINE)
    content: str = Field(default="")
    columns: Optional[List] = Field(default=[])
    destination: Optional[Engine] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    def create(self):
        if not self.destination:
            raise ValueError("Empty destination")

    def run(self) -> None:
        print(f"Running table: {self.alias} of type {self.type.value}")
 