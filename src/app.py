import logging
import time
from typing import Any

import yaml
from pydantic import ValidationError

from src.config import memory_database
from src.entities.pipeline import Pipeline
from src.exceptions import PipelineError

logger = logging.getLogger(__name__)


class App:
    def __init__(self, **kwargs: Any) -> None:
        self.pipeline_args = kwargs.get("pipeline", None)
        if not self.pipeline_args:
            raise ValueError("Pipeline argument is required")

        try:
            self.pipeline = Pipeline(**self.load_pipeline(), **kwargs)
        except ValidationError as e:
            errors = e.errors()
            logger.error("Pipeline YAML inválido — %d erro(s) encontrado(s):", len(errors))
            for err in errors:
                field = " → ".join(str(loc) for loc in err["loc"])
                logger.error("  Campo '%s': %s", field, err["msg"])
            raise PipelineError(f"Pipeline inválido: {self.pipeline_args}") from e

    def load_pipeline(self) -> dict[str, Any]:
        if isinstance(self.pipeline_args, str):
            with open(self.pipeline_args) as f:
                return yaml.safe_load(f)  # type: ignore[no-any-return]
        else:
            raise ValueError("Invalid type for pipeline argument")

    def pipeline_parameters(self) -> dict:  # type: ignore[type-arg]
        """Retorna os parâmetros do pipeline, se existirem."""
        return getattr(self.pipeline, "parameters", {})

    def run(self) -> None:
        logger.info("App is running")
        start_time = time.time()
        try:
            self.pipeline.run()
        except Exception as e:
            logger.error("Error running pipeline: %s", e)
            raise
        finally:
            total_time = time.time() - start_time
            logger.info("Pipeline execution completed in %.2f seconds", total_time)
            memory_database.close()
