import logging
import re
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
            raw = self.load_pipeline()
            pipeline_params: dict[str, Any] = kwargs.get("pipeline_params", {}) or {}
            if pipeline_params:
                params_str = {
                    k: v.isoformat() if hasattr(v, "isoformat") else str(v) for k, v in pipeline_params.items()
                }
                raw = self._substitute_parameters(raw, params_str)
                logger.debug("Parâmetros aplicados: %s", params_str)
            self.pipeline = Pipeline(**raw, **kwargs)
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

    def _substitute_parameters(self, data: Any, params: dict[str, str]) -> Any:
        """Substitui {{ param }} nos valores de string do pipeline recursivamente."""

        def replace_match(match: re.Match) -> str:  # type: ignore[type-arg]
            key = str(match.group(1)).strip()
            if key not in params:
                logger.warning("Parâmetro '{{ %s }}' não foi fornecido via CLI", key)
                return str(match.group(0))
            return params[key]

        if isinstance(data, str):
            return re.sub(r"\{\{\s*([^}]+)\s*\}\}", replace_match, data)
        if isinstance(data, dict):
            return {k: self._substitute_parameters(v, params) for k, v in data.items()}
        if isinstance(data, list):
            return [self._substitute_parameters(item, params) for item in data]
        return data

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
        name = self.pipeline.name or "pipeline"
        tag = f"[pipeline:{name}]"

        if self.dry_run:
            logger.info("%s Dry-run — valid, no execution", tag)
            self._print_dry_run_summary()
            memory_database.close()
            return

        n_loads = len(self.pipeline.loads)
        n_outputs = len(self.pipeline.outputs)
        logger.info("%s Starting — %d loader(s), %d output(s)", tag, n_loads, n_outputs)

        start_time = time.time()
        try:
            self.pipeline.run()
        except Exception as e:
            logger.error("%s Failed — %s", tag, e)
            raise
        finally:
            total_time = time.time() - start_time
            logger.info("%s Completed in %.2fs", tag, total_time)
            memory_database.close()
