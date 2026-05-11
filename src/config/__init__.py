import threading

from .database import memory_database

# Global lock for thread-safe access to memory_database (DuckDB is not thread-safe).
memory_database_lock = threading.Lock()

__all__ = ["memory_database", "memory_database_lock"]
