from dataclasses import dataclass
from src.types.file_output_format_types import FileOutputFormatTypes

@dataclass
class Output:
    type: str

@dataclass
class DatabaseOutput(Output):
    connection: str = "default"
    table_name: str

@dataclass
class FileOutput(Output):
    file_path: str
    format: FileOutputFormatTypes = FileOutputFormatTypes.CSV

@dataclass
class APIOutput(Output):
    endpoint: str
    method: str = "POST"
    options: dict = {}