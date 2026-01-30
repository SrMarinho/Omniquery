from dotenv import load_dotenv
from src.app import App


def main() -> None:
    load_dotenv()
    sources = ["procfit",]
    app = App(sources)
    app.run()

if __name__ == "__main__":
    main()