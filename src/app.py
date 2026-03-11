import time
from typing import Any

import yaml

from src.config import memory_database
from src.entities.pipeline import Pipeline


class App:
    def __init__(self, **kwargs: Any) -> None:
        self.pipeline_args = kwargs.get('pipeline', None)
        if not self.pipeline_args:
            raise ValueError("Pipeline argument is required")

        self.pipeline = Pipeline(**self.load_pipeline(), **kwargs)

    def load_pipeline(self) -> dict[str, Any]:
        if isinstance(self.pipeline_args, str):
            with open(self.pipeline_args) as f:
                return yaml.safe_load(f)  # type: ignore[no-any-return]
        else:
            raise ValueError("Invalid type for pipeline argument")

    def pipeline_parameters(self) -> dict:
        """Retorna os parâmetros do pipeline, se existirem."""
        return getattr(self.pipeline, 'parameters', {})

    def run(self) -> None:
        print("🚀 App is running")
        start_time = time.time()
        try:
            self.pipeline.run()
        except Exception as e:
            print(f"Error running pipeline: {e}")
        finally:
            total_time = time.time() - start_time
            print(f"✅ Pipeline execution completed in {total_time:.2f} seconds")
            memory_database.close()
