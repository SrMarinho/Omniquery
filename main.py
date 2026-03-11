from dotenv import load_dotenv

from cli.commands import parse_args
from src.app import App
from src.config.logging_config import setup_logging


def main() -> None:
    load_dotenv()
    setup_logging()
    args = parse_args()
    args_dict = vars(args)
    app = App(**args_dict)
    app.run()

if __name__ == "__main__":
    main()
