import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from pydantic import BaseModel, Field, model_validator

from src.config.settings import PIPELINE_WORKERS
from src.entities.loader import Loader, LoaderFactory
from src.entities.output import Output, OutputFactory
from src.entities.parameter import Parameter

logger = logging.getLogger(__name__)


class Pipeline(BaseModel):
    name: str = Field(default="")
    description: str = Field(default="")
    parameters: list[Parameter] = Field(default_factory=list)
    loads: list[Loader] = Field(default=[])
    outputs: list[Output] = Field(default=[])

    def run(self) -> None:
        self._run_parallel(self.loads, "loaders")
        self._run_parallel(self.outputs, "outputs")

    def _run_parallel(self, items: list, label: str) -> None:
        if not items:
            return
        tag = f"[pipeline:{self.name or 'pipeline'}]"
        workers = min(PIPELINE_WORKERS, len(items))
        logger.debug("%s Running %d %s with %d worker(s)", tag, len(items), label, workers)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(item.run): item for item in items}
            for future in as_completed(futures):
                future.result()

    @model_validator(mode="before")
    @classmethod
    def create_concrete_loaders(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Factory method para criar instâncias concretas de Loader"""
        if "loads" in data and isinstance(data["loads"], list):
            concrete_loaders: list[Loader] = []

            for loader_data in data["loads"]:
                if isinstance(loader_data, dict):
                    loader_instance = LoaderFactory.create(loader_data)
                    concrete_loaders.append(loader_instance)
                elif isinstance(loader_data, Loader):
                    concrete_loaders.append(loader_data)
                else:
                    raise ValueError(f"Tipo inválido para output: {type(loader_data)}")

            data["loads"] = concrete_loaders

        return data

    @model_validator(mode="before")
    @classmethod
    def create_concrete_outputs(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Factory method para criar instâncias concretas de Output"""
        if "outputs" in data and isinstance(data["outputs"], list):
            concrete_outputs = []

            for output_data in data["outputs"]:
                if isinstance(output_data, dict):
                    output_instance = OutputFactory.create(output_data)
                    concrete_outputs.append(output_instance)
                elif isinstance(output_data, Output):
                    concrete_outputs.append(output_data)
                else:
                    raise ValueError(f"Tipo inválido para output: {type(output_data)}")

            data["outputs"] = concrete_outputs

        return data
