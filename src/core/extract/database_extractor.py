from typing import Iterator, Any
from sqlalchemy.engine import Engine, text
from sqlalchemy.schema import Sequence
from src.core.extract.extractor import Extractor


class DatabaseExtractor(Extractor):
    def __init__(self, engine: Engine, query: str, batch_size: int = 1000) -> None:
        self.engine = engine
        self.query = query
        self.batch_size = batch_size

    def extract(self) -> Iterator[Any]:
        with self.engine.connect() as conn:
            result = conn.execution_options(stream_results=True).execute(text(self.query))
            
            while batch := result.fetchmany(self.batch_size):
                if not batch:
                    break
                yield batch