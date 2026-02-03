from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict
from sqlalchemy.engine import Engine
from src.types.table_types import TableTypes

class Table(BaseModel):
    alias: str = Field()
    description: str = Field(default="")
    type: TableTypes = Field(default=TableTypes.INLINE)
    content: str = Field(default="")
    columns: Optional[List] = Field(default=[])
    options: Dict = Field(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True)
 