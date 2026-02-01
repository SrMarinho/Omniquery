from dataclasses import dataclass


@dataclass
class ExtractionSource:
    source: str
    extraction: list[dict]