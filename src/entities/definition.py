from dataclasses import dataclass
from src.entities.extract import Extract


@dataclass
class Definition:
    extraction: list[Extract] = []
    transformation: list[dict] = []
    load: dict = {}