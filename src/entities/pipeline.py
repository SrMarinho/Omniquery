from dataclasses import dataclass
from src.entities.definition import Definition
from src.entities.output import Output

@dataclass
class Pipeline:
    name: str = ""
    description: str = ""
    definition: Definition = None
    outputs: list[Output] = None