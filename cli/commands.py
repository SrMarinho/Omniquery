import os
import argparse
from datetime import datetime
import yaml
from src.config.settings import base_path

def get_pipelines():
    """Retorna a lista de pipelines disponíveis"""
    pipelines_dir = os.path.join(base_path, "pipelines")
    if not os.path.exists(pipelines_dir):
        return []
    return [f for f in os.listdir(pipelines_dir) if f.endswith(".yaml")]

def load_pipeline(pipeline_file: str) -> dict:
    if isinstance(pipeline_file, str):
        with open(pipeline_file, 'r') as f:
            data = yaml.safe_load(f)
        return data
    else:
        raise ValueError("Invalid type for pipeline argument")

def create_parser():
    """Cria e configura o parser de argumentos"""

    parser = argparse.ArgumentParser(
        prog='OmniQuery',
        description='Command line tool for file processing and analysis',
        epilog='Exemplo: python main.py --pipeline pipelines/meu_pipeline.yaml'
    )
    
    allowed_pipelines = get_pipelines()
    if not allowed_pipelines:
        print("⚠️  No pipelines found in the pipelines directory.")
    # Comando: pipeline (arquivo YAML do pipeline)
    parser_pipeline = parser.add_argument(
        '--pipeline',
        type=str,
        required=True,
        help='Path to the pipeline YAML file.'
    )

    # Mapeamento de tipos string para funções Python
    TYPE_MAP = {
        'str': str,
        'int': int,
        'float': float,
        'bool': lambda x: x.lower() in ('true', '1', 'yes', 'y'),
        'date': lambda x: datetime.strptime(x, '%Y-%m-%d').date(),
        'datetime': lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'),
    }
    
    pipeline_parometers = parser.add_argument_group("Pipeline Parameters")

    parser.add_subparsers(title="Pipelines", dest="pipeline_command")


    for pipeline in allowed_pipelines:
        pipeline_loaded = load_pipeline(os.path.join(base_path, "pipelines", pipeline))

        if 'parameters' in pipeline_loaded:
            for parameter in pipeline_loaded['parameters']:
                param_name = f"--{parameter['name']}"

                type_func = TYPE_MAP.get(parameter['type'], str)
                
                pipeline_parometers.add_argument(
                    param_name,
                    type=type_func,
                    required=parameter['required'],
                    help=parameter.get('description', '')
                )

    return parser


def parse_args():
    """Analisa os argumentos da linha de comando"""
    parser = create_parser()

    return parser.parse_args()
