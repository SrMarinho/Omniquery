from pydantic import BaseModel, Field, model_validator
from typing import Dict, Any, Type
from src.types.file_output_format_types import FileOutputFormatTypes

class Output(BaseModel):
    type: str
    query: str

    def write(self) -> None:
        print(f"Writing output type: {type(self)}")

class DatabaseOutput(Output):
    type: str = "database" 
    table: str
    connection: str = Field(default="default")

    def write(self) -> None:
        print("Writing in Database")

class FileOutput(Output):
    type: str = "file" 
    file_path: str = Field(default="")
    format: FileOutputFormatTypes = FileOutputFormatTypes.CSV

    def write(self) -> None:
        print("Writing in File")

class APIOutput(Output):
    type: str = "api" 
    endpoint: str = Field(default="")
    method: str = Field(default="POST")
    options: dict = {}

    def write(self) -> None:
        print("Writing in API")

class OutputFactory:
    output_types = {
        "database": DatabaseOutput,
        "file": FileOutput,
        "api": APIOutput
    }
    @staticmethod
    def create(config: Dict[str, Any]) -> Output:
        output_type: str = config.get("type", "")
        if output_type.lower() in OutputFactory.output_types:
            return OutputFactory.output_types[output_type.lower()](**config)
        
        return Output(**config)