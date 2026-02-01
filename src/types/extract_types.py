from Enurmerate import Enum

class ExtractType(str, Enum):
    INLINE = "inline"
    SQL = "sql"
    API = "api"
    FILE = "file"