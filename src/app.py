import yaml
from src.entities.pipeline import Pipeline

class App:
    def __init__(self, **kwargs) -> None:
        self.pipeline_args = kwargs.get('pipeline', None)
        if not self.pipeline_args:
            raise ValueError("Pipeline argument is required")
        
        self.pipeline = Pipeline(**self.load_pipeline())

    def load_pipeline(self) -> dict:
        if isinstance(self.pipeline_args, str):
            with open(self.pipeline_args, 'r') as f:
                data = yaml.safe_load(f)
            return data
        else:
            raise ValueError("Invalid type for pipeline argument")

    def run(self) -> None:
        print("App is running")
        self.pipeline.run()