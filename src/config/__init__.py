import threading

from .database import memory_database

# Lock global para acesso thread-safe ao memory_database (DuckDB não é thread-safe)
memory_database_lock = threading.Lock()

__all__ = ["memory_database", "memory_database_lock"]
