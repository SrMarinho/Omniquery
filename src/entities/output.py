from pydantic import BaseModel, Field
from src.types.file_output_format_types import FileOutputFormatTypes

class Output(BaseModel):
    type: str = Field(default="")

class DatabaseOutput(Output):
    table_name: str
    connection: str = Field(default="default")

class FileOutput(Output):
    file_path: str = Field(default="")
    format: FileOutputFormatTypes = FileOutputFormatTypes.CSV

class APIOutput(Output):
    endpoint: str = Field(default="")
    method: str = Field(default="POST")
    options: dict = {}