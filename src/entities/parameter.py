from datetime import date
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, model_validator

class Parameter(BaseModel):
    name: str
    type: str = Field(default="string")
    description: Optional[str] = Field(default="")
    required: bool = Field(default=False)
    
    @model_validator(mode='before')
    @classmethod
    def validate_type(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Valida o tipo do parâmetro e converte para o tipo correto."""
        param_type = data.get('type', '').lower()
        if param_type not in ['string', 'integer', 'int', 'float', 'boolean', 'bool', 'date']:
            raise ValueError(f"Invalid parameter type: {param_type}")

        return data