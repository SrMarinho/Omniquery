from pydantic import BaseModel, ConfigDict, Field

from src.types.table_types import TableTypes


class Table(BaseModel):
    alias: str = Field()
    description: str = Field(default="")
    type: TableTypes = Field(default=TableTypes.INLINE)
    content: str = Field(default="")
    options: dict = Field(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True)
