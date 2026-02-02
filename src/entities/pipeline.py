from threading import Thread
from pydantic import BaseModel, Field, model_validator
from typing import List, Dict, Any
from src.entities.load import Load
from src.entities.output import OutputFactory, Output


class Pipeline(BaseModel):
    name: str = Field(default="")
    description: str = Field(default="")
    loads: List[Load] = Field(default_factory=list)
    outputs: List[Output] = Field(default_factory=list)

    def run(self) -> None:
        if self.name:
            print(f"Running pipeline: {self.name}")
        loads_theads = []
        for load in self.loads:
            t = Thread(target=load.run)
            loads_theads.append(t)
            t.start()

        for thread in loads_theads:
            thread.join()

        outputs_threads: List[Thread] = []
        for output in self.outputs:
            t = Thread(target=output.write)
            outputs_threads.append(t)
            t.start()
        
        for thread in outputs_threads:
            thread.join()
        
        
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