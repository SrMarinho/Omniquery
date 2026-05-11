import logging
import sys

from dotenv import load_dotenv

from cli.commands import app
from src.config.logging_config import setup_logging
from src.exceptions import OmniQueryError

logger = logging.getLogger(__name__)


def main() -> None:
    load_dotenv()
    setup_logging()
    try:
        app()
    except OmniQueryError as e:
        logger.error("%s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
