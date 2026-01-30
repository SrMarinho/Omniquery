from abc import ABC, abstractmethod
from typing import Iterator, Any
from sqlalchemy.schema import Sequence

class Extractor(ABC):
    @abstractmethod
    def extract(self) -> Iterator[Any]:
        pass