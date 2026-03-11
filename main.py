import logging
import sys

from dotenv import load_dotenv

from cli.commands import parse_args
from src.app import App
from src.config.logging_config import setup_logging
from src.exceptions import OmniQueryError

logger = logging.getLogger(__name__)


def main() -> None:
    load_dotenv()
    setup_logging()
    args = parse_args()
    try:
        app = App(**vars(args))
        app.run()
    except OmniQueryError as e:
        logger.error("%s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
