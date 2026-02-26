from typing import Optional
from sqlalchemy import create_engine
import duckdb

memory_database = create_engine("duckdb:///:memory:")
