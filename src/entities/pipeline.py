from threading import Thread
from pydantic import BaseModel, Field, model_validator
from typing import List, Dict, Any
from src.entities.loader import Loader, LoaderFactory
from src.entities.output import OutputFactory, Output


class Pipeline(BaseModel):
    name: str = Field(default="")
    description: str = Field(default="")
    loads: List[Loader] = Field(default=[])
    outputs: List[Output] = Field(default=[])

    def run(self) -> None:
        for load in self.loads:
            load.run()
        
        for output in self.outputs:
            output.run()

    @model_validator(mode='before')
    @classmethod
    def create_concrete_loaders(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Factory method para criar instâncias concretas de Loader"""
        if 'loads' in data and isinstance(data['loads'], list):
            concrete_loaders: list[Loader] = []
            
            for loader_data in data['loads']:
                if isinstance(loader_data, dict):
                    loader_instance = LoaderFactory.create(loader_data)
                    concrete_loaders.append(loader_instance)
                elif isinstance(loader_data, Loader):
                    concrete_loaders.append(loader_data)
                else:
                    raise ValueError(f"Tipo inválido para output: {type(loader_data)}")

            data['loads'] = concrete_loaders
        
        return data
    
    @model_validator(mode='before')
    @classmethod
    def create_concrete_outputs(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Factory method para criar instâncias concretas de Output"""
        if 'outputs' in data and isinstance(data['outputs'], list):
            concrete_outputs = []
            
            for output_data in data['outputs']:
                if isinstance(output_data, dict):
                    output_instance = OutputFactory.create(output_data)
                    concrete_outputs.append(output_instance)
                elif isinstance(output_data, Output):
                    concrete_outputs.append(output_data)
                else:
                    raise ValueError(f"Tipo inválido para output: {type(output_data)}")
            
            data['outputs'] = concrete_outputs
        
        return data