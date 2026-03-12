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
                logger.debug("params: %s", params_str)
            self.pipeline = Pipeline(**raw, **kwargs)
        except ValidationError as e:
            errors = e.errors()
            logger.error("Pipeline invalido -- %d erro(s):", len(errors))
            for err in errors:
                field = " -> ".join(str(loc) for loc in err["loc"])
                logger.error("  '%s': %s", field, err["msg"])
            raise PipelineError(f"Pipeline invalido: {self.pipeline_args}") from e

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
                logger.warning("parametro '{{ %s }}' nao fornecido", key)
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
        """Retorna os parametros do pipeline, se existirem."""
        return getattr(self.pipeline, "parameters", {})

    def _print_dry_run_summary(self) -> None:
        p = self.pipeline
        logger.info("dry-run: [bold]%s[/bold]", p.name or "(sem nome)")
        if p.description:
            logger.info("  %s", p.description)
        logger.info("  loaders (%d):", len(p.loads))
        for loader in p.loads:
            logger.info("    [%s] %s -- %d tabela(s)", loader.type, loader.source, len(loader.tables))
            for table in loader.tables:
                logger.info("      - %s", table.alias)
        logger.info("  outputs (%d):", len(p.outputs))
        for output in p.outputs:
            logger.info("    [%s] %s", output.type, output.name)
        if p.parameters:
            logger.info("  parametros (%d):", len(p.parameters))
            for param in p.parameters:
                logger.info("      - %s (%s)", param.name, param.type)

    def run(self) -> None:
        name = self.pipeline.name or "pipeline"

        if self.dry_run:
            logger.info("[bold]%s[/bold] dry-run -- ok", name)
            self._print_dry_run_summary()
            memory_database.close()
            return

        n_loads = len(self.pipeline.loads)
        n_outputs = len(self.pipeline.outputs)
        logger.info("[bold cyan]%s[/bold cyan] | %d loader(s), %d output(s)", name, n_loads, n_outputs)

        start_time = time.time()
        try:
            self.pipeline.run()
        except Exception as e:
            logger.error("[bold]%s[/bold] | falhou -- %s", name, e)
            raise
        finally:
            total_time = time.time() - start_time
            logger.info("[bold cyan]%s[/bold cyan] | concluido em [bold]%.2fs[/bold]", name, total_time)
            memory_database.close()
