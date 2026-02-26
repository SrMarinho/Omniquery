from typing import Optional
from sqlalchemy import create_engine
import duckdb

memory_database = duckdb.connect(database=':memory:')
