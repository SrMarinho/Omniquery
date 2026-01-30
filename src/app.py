from sqlalchemy import create_engine, text, Table, Column
from sqlalchemy.schema import MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import Engine
from src.config.database import databases
from src.core.extract.database_extractor import DatabaseExtractor

Base = declarative_base()

# class NfFaturemento(Base):
#     __tablename__ = "nf_faturemento"
    

class App():
    def __init__(self, sources: list) -> None:
        # self.engine = create_engine("duckdb:///application.duckdb")
        self.sources = sources
        self.engines: dict[str, Engine] = {}

    def run(self) -> None:
        print("App is running")
        for source in self.sources:
            conn_str = databases.get_connection_string(source)
            engine = create_engine(conn_str)
            self.engines[source] = engine
            print(f"Connected to {source} database.") 
        