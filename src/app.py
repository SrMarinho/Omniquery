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
        self.dry_run: bool = bool(kwargs.get("dry_run", False))
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

    def _print_dry_run_summary(self) -> None:
        p = self.pipeline
        logger.info("DRY RUN — pipeline: %s", p.name or "(sem nome)")
        if p.description:
            logger.info("  Descrição: %s", p.description)
        logger.info("  Loaders (%d):", len(p.loads))
        for loader in p.loads:
            logger.info("    [%s] source=%s — %d tabela(s)", loader.type, loader.source, len(loader.tables))
            for table in loader.tables:
                logger.info("      • %s", table.alias)
        logger.info("  Outputs (%d):", len(p.outputs))
        for output in p.outputs:
            logger.info("    [%s] name=%s", output.type, output.name)
        if p.parameters:
            logger.info("  Parâmetros (%d):", len(p.parameters))
            for param in p.parameters:
                logger.info("      • %s (%s)", param.name, param.type)

    def run(self) -> None:
        if self.dry_run:
            logger.info("Modo dry-run: pipeline validado com sucesso, nenhuma execução será feita.")
            self._print_dry_run_summary()
            memory_database.close()
            return

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
