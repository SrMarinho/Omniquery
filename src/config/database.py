from sqlalchemy import create_engine
from typing import Optional

memory_database = create_engine("duckdb:///:memory:")