import os
from pathlib import Path

base_path = Path().parent.parent.absolute()

# Database loader
DB_CHUNK_SIZE: int = int(os.getenv("DB_CHUNK_SIZE", "500000"))
DB_THREADS: int = int(os.getenv("DB_THREADS", "4"))
DB_MEMORY_LIMIT: str = os.getenv("DB_MEMORY_LIMIT", "4GB")

# File output
FILE_CHUNK_SIZE: int = int(os.getenv("FILE_CHUNK_SIZE", "100000"))

# Database output
DB_BATCH_SIZE: int = int(os.getenv("DB_BATCH_SIZE", "500000"))

# Pipeline parallelism
PIPELINE_WORKERS: int = int(os.getenv("PIPELINE_WORKERS", "4"))

# Oracle ODBC (turbodbc) — nome do driver instalado no sistema
ORACLE_ODBC_DRIVER: str = os.getenv("ORACLE_ODBC_DRIVER", "Oracle")
