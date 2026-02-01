from src.entities.pipeline import Pipeline

class App:
    def __init__(self, pipeline: Pipeline) -> None:
        self.pipeline = pipeline

    def run(self) -> None:
        print("App is running")
