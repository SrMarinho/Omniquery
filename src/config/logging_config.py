import logging
import os

from rich.console import Console
from rich.logging import RichHandler


def setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    console = Console(stderr=True, force_terminal=True)
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True, show_path=False, markup=True)],
    )
