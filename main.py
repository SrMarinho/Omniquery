from dotenv import load_dotenv

from cli.commands import parse_args
from src.app import App


def main() -> None:
    load_dotenv()
    args = parse_args()
    args_dict = vars(args)
    app = App(**args_dict)
    app.run()

if __name__ == "__main__":
    main()
