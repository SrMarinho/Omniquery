from pydantic import BaseModel, Field
from src.types.extract_types import ExtractType


class Extract(BaseModel):
    alias: str = Field(default="")
    description: str = Field(default="")
    type: ExtractType = Field(default=ExtractType.INLINE)
    content: str = Field(default="")
