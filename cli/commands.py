import argparse
import logging
import os
import sys
from datetime import datetime

import yaml
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich_argparse import RichHelpFormatter

from src.config.settings import base_path

logger = logging.getLogger(__name__)
console = Console()

BANNER = (
    "[bold cyan]  ___  _ __ ___ _ __ (_) __ _ _   _  ___ _ __ _   _ [/bold cyan]\n"
    "[bold cyan] / _ \\| '_ ` _ \\ '_ \\| |/ _` | | | |/ _ \\ '__| | | |[/bold cyan]\n"
    "[bold cyan]| (_) | | | | | | | | | | (_| | |_| |  __/ |  | |_| |[/bold cyan]\n"
    "[bold cyan] \\___/|_| |_| |_|_| |_|_|\\__, |\\__,_|\\___|_|   \\__, |[/bold cyan]\n"
    "[bold cyan]                           |___/                 |___/ [/bold cyan]"
)


def print_banner() -> None:
    console.print(BANNER)
    console.print(
        Panel.fit(
            "[dim]Ferramenta de consulta e processamento de dados via pipelines YAML[/dim]",
            border_style="cyan",
        )
    )
    console.print()


def get_pipelines() -> list[str]:
    """Retorna a lista de pipelines disponíveis."""
    pipelines_dir = os.path.join(base_path, "pipelines")
    if not os.path.exists(pipelines_dir):
        return []
    return [f for f in os.listdir(pipelines_dir) if f.endswith(".yaml")]


def print_available_pipelines() -> None:
    pipelines = get_pipelines()
    if not pipelines:
        console.print("[yellow]Nenhuma pipeline encontrada em ./pipelines/[/yellow]")
        return

    table = Table(title="Pipelines disponíveis", border_style="cyan", show_lines=False)
    table.add_column("#", style="dim", width=4)
    table.add_column("Arquivo", style="bold white")
    for i, name in enumerate(sorted(pipelines), 1):
        table.add_row(str(i), name)
    console.print(table)


def load_pipeline(pipeline_file: str) -> dict:  # type: ignore[type-arg]
    """Carrega um pipeline a partir de um arquivo YAML."""
    if isinstance(pipeline_file, str):
        with open(pipeline_file) as f:
            return yaml.safe_load(f)  # type: ignore[no-any-return]
    else:
        raise ValueError("Invalid type for pipeline argument")


def create_base_parser() -> argparse.ArgumentParser:
    """Cria o parser base sem argumentos do pipeline."""
    RichHelpFormatter.styles["argparse.prog"] = "bold cyan"
    RichHelpFormatter.styles["argparse.args"] = "bold yellow"
    RichHelpFormatter.styles["argparse.groups"] = "bold cyan"
    RichHelpFormatter.styles["argparse.syntax"] = "bold"

    parser = argparse.ArgumentParser(
        prog="omniquery",
        description="Ferramenta de consulta e processamento de dados via pipelines YAML",
        epilog="Exemplo: [bold]uv run main.py pipelines/meu_pipeline.yaml[/bold]",
        add_help=True,
        conflict_handler="resolve",
        formatter_class=RichHelpFormatter,
    )

    parser.add_argument(
        "pipeline",
        nargs="?",
        help="Caminho para o arquivo YAML da pipeline",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Valida e exibe o pipeline sem executar loaders ou outputs",
    )

    return parser


def setup_pipeline_arguments(
    parser: argparse.ArgumentParser,
    pipeline_loaded: dict,  # type: ignore[type-arg]
    pipeline_name: str,
) -> None:
    """Configura os argumentos específicos da pipeline."""
    TYPE_MAP = {
        "string": str,
        "int": int,
        "integer": int,
        "float": float,
        "bool": lambda x: x.lower() in ("true", "1", "yes", "y"),
        "boolean": lambda x: x.lower() in ("true", "1", "yes", "y"),
        "date": lambda x: datetime.strptime(x, "%Y-%m-%d").date(),
    }

    if pipeline_loaded and "parameters" in pipeline_loaded and pipeline_loaded["parameters"]:
        pipeline_group = parser.add_argument_group(
            title=f'parâmetros da pipeline "[bold cyan]{pipeline_name}[/bold cyan]"',
            description="Argumentos específicos para esta pipeline",
        )

        for parameter in pipeline_loaded["parameters"]:
            param_name = parameter["name"]
            param_type = parameter.get("type", "string")
            type_func = TYPE_MAP.get(param_type, str)

            help_parts = []
            if parameter.get("description"):
                help_parts.append(parameter["description"])
            if "default" in parameter:
                help_parts.append(f"[dim]default: {parameter['default']}[/dim]")
            if parameter.get("required", False):
                help_parts.append("[bold red](obrigatório)[/bold red]")

            help_text = " | ".join(help_parts) if help_parts else ""

            kwargs: dict = {"type": type_func, "required": parameter.get("required", False), "help": help_text}

            if "default" in parameter and not parameter.get("required", False):
                kwargs["default"] = parameter["default"]

            pipeline_group.add_argument(f"--{param_name}", **kwargs)


def _resolve_pipeline_path(pipeline_arg: str) -> str | None:
    """Resolve o caminho do arquivo da pipeline."""
    if os.path.exists(pipeline_arg):
        return pipeline_arg
    full_path = os.path.join(base_path, "pipelines", pipeline_arg)
    if os.path.exists(full_path):
        return full_path
    return None


def parse_args() -> argparse.Namespace:
    """
    Analisa os argumentos da linha de comando.
    Suporta -h tanto sem pipeline (help geral) quanto com pipeline (help específico).
    """
    parser = create_base_parser()

    if len(sys.argv) == 1 or (len(sys.argv) == 2 and sys.argv[1] in ["-h", "--help"]):
        print_banner()
        print_available_pipelines()
        console.print()
        parser.print_help()
        sys.exit(0)

    if len(sys.argv) > 1 and not sys.argv[1].startswith("-"):
        pipeline_arg = sys.argv[1]

        try:
            pipeline_path = _resolve_pipeline_path(pipeline_arg)

            if pipeline_path:
                pipeline_loaded = load_pipeline(pipeline_path)
                pipeline_name = os.path.splitext(os.path.basename(pipeline_path))[0]
                setup_pipeline_arguments(parser, pipeline_loaded, pipeline_name)

                if len(sys.argv) == 3 and sys.argv[2] in ["-h", "--help"]:
                    print_banner()
                    parser.print_help()
                    sys.exit(0)
            else:
                console.print(f"[bold red]✗[/bold red] Pipeline não encontrada: [yellow]{pipeline_arg}[/yellow]")
                print_available_pipelines()
                sys.exit(1)

        except Exception as e:
            console.print(f"[bold red]✗[/bold red] Erro ao carregar pipeline: [red]{e}[/red]")
            sys.exit(1)

    args = parser.parse_args()

    if not hasattr(args, "pipeline_data"):
        try:
            pipeline_path = _resolve_pipeline_path(args.pipeline)
            if not pipeline_path:
                raise FileNotFoundError(f"Pipeline não encontrada: {args.pipeline}")

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
            console.print(f"[bold red]✗[/bold red] Erro ao carregar pipeline: [red]{e}[/red]")
            sys.exit(1)

    return args
