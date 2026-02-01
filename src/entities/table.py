from pydantic import BaseModel, Field

from src.types.table_types import TableTypes

class Table(BaseModel):
    alias: str = Field()
    description: str = Field(default="")
    type: TableTypes = Field(default=TableTypes.INLINE)
    content: str = Field(default="")


    def run(self) -> None:
        print(f"Running table: {self.alias} of type {self.type.value}")
 