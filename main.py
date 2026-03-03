import json
from dotenv import load_dotenv
from src.app import App
from src.entities.pipeline import Pipeline
from cli.commands import parse_args


def main() -> None:
    load_dotenv()
    args = parse_args()
    args_dict = vars(args)
    print(f"Parsed arguments: {args_dict}")
    app = App(**args_dict)
    app.run()

if __name__ == "__main__":
    main()