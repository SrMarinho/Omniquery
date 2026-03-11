from pydantic import BaseModel, Field


class Parameter(BaseModel):
    name: str
    type: str = Field(default="string")
    description: str | None = Field(default="")
    required: bool = Field(default=False)
