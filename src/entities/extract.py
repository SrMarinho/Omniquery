from dataclasses import dataclass
from src.types.extract_types import ExtractType


@dataclass
class Extract:
    alias: str
    description: str = ""
    type: ExtractType = ExtractType.INLINE
    content: str = ""
