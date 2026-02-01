from Enumeration import Enum


class FileOutputFormatTypes(str, Enum):
    CSV = "csv"
    XLSX = "xlsx"
    XLS = "xls"