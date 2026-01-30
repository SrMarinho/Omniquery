from abc import ABC, abstractmethod

class DatabaseBeforeETL(ABC):
    def __init__(self, engine: object) -> None:
        self.engine = engine

    @abstractmethod
    def execute(self) -> None:
        pass