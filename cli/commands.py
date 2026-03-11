import argparse
import logging
import os
import sys
from datetime import datetime

import yaml

from src.config.settings import base_path

logger = logging.getLogger(__name__)


def get_pipelines():
    """Retorna a lista de pipelines disponíveis"""
    pipelines_dir = os.path.join(base_path, "pipelines")
    if not os.path.exists(pipelines_dir):
        return []
    return [f for f in os.listdir(pipelines_dir) if f.endswith(".yaml")]


def load_pipeline(pipeline_file: str) -> dict:  # type: ignore[type-arg]
    """
    Carrega um pipeline a partir de um arquivo YAML.
    """
    if isinstance(pipeline_file, str):
        with open(pipeline_file) as f:
            return yaml.safe_load(f)  # type: ignore[no-any-return]
    else:
        raise ValueError("Invalid type for pipeline argument")


def create_base_parser():
    """Cria o parser base sem argumentos do pipeline"""
    parser = argparse.ArgumentParser(
        prog="OmniQuery",
        description="Command line tool for file processing and analysis",
        epilog="Exemplo: uv run main.py pipelines/meu_pipeline.yaml",
        add_help=True,
        conflict_handler="resolve",
    )

    parser.add_argument("pipeline", nargs="?", help="Caminho para o arquivo YAML da pipeline")

    return parser


def setup_pipeline_arguments(parser, pipeline_loaded, pipeline_name):
    """
    Configura os argumentos específicos da pipeline
    """
    TYPE_MAP = {
        "str": str,
        "int": int,
        "float": float,
        "bool": lambda x: x.lower() in ("true", "1", "yes", "y"),
        "date": lambda x: datetime.strptime(x, "%Y-%m-%d").date(),
    }

    if pipeline_loaded and "parameters" in pipeline_loaded and pipeline_loaded["parameters"]:
        group_title = f'Parâmetros da pipeline "{pipeline_name}"'
        group_description = (
            "Argumentos específicos para esta pipeline (use -h após especificar a pipeline para ver esta seção)"
        )

        pipeline_group = parser.add_argument_group(title=group_title, description=group_description)

        for parameter in pipeline_loaded["parameters"]:
            param_name = parameter["name"]
            param_type = parameter.get("type", "str")
            type_func = TYPE_MAP.get(param_type, str)

            help_parts = []
            if parameter.get("description"):
                help_parts.append(parameter["description"])
            if "default" in parameter:
                help_parts.append(f"default: {parameter['default']}")
            if parameter.get("required", False):
                help_parts.append("(obrigatório)")

            help_text = " | ".join(help_parts) if help_parts else ""

            kwargs = {"type": type_func, "required": parameter.get("required", False), "help": help_text}

            if "default" in parameter and not parameter.get("required", False):
                kwargs["default"] = parameter["default"]

            pipeline_group.add_argument(f"--{param_name}", **kwargs)


def parse_args():
    """
    Analisa os argumentos da linha de comando.
    Suporta -h tanto sem pipeline (help geral) quanto com pipeline (help específico)
    """
    parser = create_base_parser()

    if len(sys.argv) == 1 or (len(sys.argv) == 2 and sys.argv[1] in ["-h", "--help"]):
        parser.print_help()
        sys.exit(0)

    if len(sys.argv) > 1 and not sys.argv[1].startswith("-"):
        pipeline_arg = sys.argv[1]

        try:
            pipeline_path = pipeline_arg
            if not os.path.exists(pipeline_path):
                full_path = os.path.join(base_path, "pipelines", pipeline_path)
                if os.path.exists(full_path):
                    pipeline_path = full_path
                else:
                    pass

            if os.path.exists(pipeline_path):
                pipeline_loaded = load_pipeline(pipeline_path)
                pipeline_name = os.path.splitext(os.path.basename(pipeline_path))[0]

                setup_pipeline_arguments(parser, pipeline_loaded, pipeline_name)

                if len(sys.argv) == 3 and sys.argv[2] in ["-h", "--help"]:
                    parser.print_help()
                    sys.exit(0)
            else:
                logger.error("Pipeline não encontrada: %s", pipeline_arg)
                pipelines = get_pipelines()
                if pipelines:
                    logger.info("Pipelines disponíveis: %s", ", ".join(pipelines))
                sys.exit(1)

        except Exception as e:
            logger.error("Erro ao carregar pipeline: %s", e)
            sys.exit(1)

    args = parser.parse_args()

    if not hasattr(args, "pipeline_data"):
        try:
            pipeline_path = args.pipeline
            if not os.path.exists(pipeline_path):
                full_path = os.path.join(base_path, "pipelines", pipeline_path)
                if os.path.exists(full_path):
                    pipeline_path = full_path
                else:
                    raise FileNotFoundError(f"Pipeline não encontrada: {pipeline_path}")

            pipeline_loaded = load_pipeline(pipeline_path)
            args.pipeline_data = pipeline_loaded

            pipeline_params = {}
            if "parameters" in pipeline_loaded:
                for param in pipeline_loaded["parameters"]:
                    param_name = param["name"]
                    if hasattr(args, param_name):
                        pipeline_params[param_name] = getattr(args, param_name)

            args.pipeline_params = pipeline_params

        except Exception as e:
            logger.error("Erro ao carregar pipeline: %s", e)
            sys.exit(1)

    return args
