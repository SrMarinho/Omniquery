from enum import Enum

class ExtractType(Enum):
    INLINE = "inline"
    SQL = "sql"
    API = "api"
    FILE = "file"