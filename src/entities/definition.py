from pydantic import BaseModel, Field
from typing import List
from src.entities.extract import Extract


class Definition(BaseModel):
    extraction: List[Extract] = Field(default_factory=list)
    transformation: List[dict] = Field(default_factory=list)
    load: dict = Field(default_factory=dict)