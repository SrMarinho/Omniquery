from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from typing import Any

import typer
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

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

TYPE_MAP: dict[str, Any] = {
    "string": str,
    "int": int,
    "integer": int,
    "float": float,
    "bool": lambda x: x.lower() in ("true", "1", "yes", "y"),
    "boolean": lambda x: x.lower() in ("true", "1", "yes", "y"),
    "date": lambda x: datetime.strptime(x, "%Y-%m-%d").date(),
}


app = typer.Typer(
    name="omniquery",
    help="YAML-driven ETL: load from multiple sources, transform via SQL in DuckDB, export to destinations.",
    rich_markup_mode="rich",
    add_completion=False,
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
)


def print_banner() -> None:
    console.print(BANNER)
    console.print(
        Panel.fit(
            "[dim]Query and process data through YAML pipelines[/dim]",
            border_style="cyan",
        )
    )
    console.print()


def get_pipelines() -> list[str]:
    """Return the list of available pipelines."""
    pipelines_dir = os.path.join(base_path, "pipelines")
    if not os.path.exists(pipelines_dir):
        return []
    return [f for f in os.listdir(pipelines_dir) if f.endswith(".yaml") or f.endswith(".yml")]


def _print_available_pipelines() -> None:
    pipelines = get_pipelines()
    if not pipelines:
        console.print("[yellow]No pipelines found under ./pipelines/[/yellow]")
        return

    table = Table(title="Available pipelines", border_style="cyan", show_lines=False)
    table.add_column("#", style="dim", width=4)
    table.add_column("File", style="bold white")
    for i, name in enumerate(sorted(pipelines), 1):
        table.add_row(str(i), name)
    console.print(table)


def load_pipeline(pipeline_file: str) -> dict[str, Any]:
    """Load a pipeline definition from a YAML file."""
    with open(pipeline_file, encoding="utf-8") as f:
        return yaml.safe_load(f)  # type: ignore[no-any-return]


def _resolve_pipeline_path(pipeline_arg: str) -> str | None:
    """Resolve a pipeline argument to an absolute file path."""
    if os.path.exists(pipeline_arg):
        return pipeline_arg
    full_path = os.path.join(base_path, "pipelines", pipeline_arg)
    if os.path.exists(full_path):
        return full_path
    return None


def _parse_extra_args(extras: list[str], parameters: list[dict[str, Any]]) -> dict[str, Any]:
    """Parse `--name value` / `--name=value` extras against the pipeline's declared parameters.

    Unknown flags raise typer.BadParameter. Required parameters missing from extras also raise.
    """
    declared = {p["name"]: p for p in parameters}
    pending: dict[str, str] = {}

    i = 0
    while i < len(extras):
        token = extras[i]
        if not token.startswith("--"):
            raise typer.BadParameter(f"Unexpected positional argument: {token!r}")
        key, _, inline_value = token[2:].partition("=")
        if key not in declared:
            raise typer.BadParameter(
                f"Unknown parameter '--{key}'. Pipeline accepts: {', '.join(declared) or '(none)'}"
            )
        if inline_value:
            pending[key] = inline_value
            i += 1
        else:
            if i + 1 >= len(extras):
                raise typer.BadParameter(f"Missing value for '--{key}'")
            pending[key] = extras[i + 1]
            i += 2

    parsed: dict[str, Any] = {}
    for name, spec in declared.items():
        type_func = TYPE_MAP.get(spec.get("type", "string"), str)
        if name in pending:
            try:
                parsed[name] = type_func(pending[name])
            except Exception as e:
                raise typer.BadParameter(f"Invalid value for '--{name}': {e}") from e
        elif "default" in spec:
            parsed[name] = spec["default"]
        elif spec.get("required", False):
            raise typer.BadParameter(f"Missing required parameter '--{name}'")
    return parsed


def _print_pipeline_param_help(pipeline_name: str, parameters: list[dict[str, Any]]) -> None:
    if not parameters:
        return
    table = Table(title=f'Parameters for pipeline "[bold cyan]{pipeline_name}[/bold cyan]"', border_style="cyan")
    table.add_column("Flag", style="bold yellow")
    table.add_column("Type", style="cyan")
    table.add_column("Required", style="magenta")
    table.add_column("Default", style="dim")
    table.add_column("Description")
    for p in parameters:
        flag = f"--{p['name']}"
        required = "yes" if p.get("required", False) else "no"
        default = str(p.get("default", ""))
        table.add_row(flag, p.get("type", "string"), required, default, p.get("description", ""))
    console.print(table)


@app.command("list", help="List available pipelines under ./pipelines/")
def list_command() -> None:
    _print_available_pipelines()


@app.command(
    "run",
    help="Run a pipeline. Extra `--name value` flags are mapped to the pipeline's declared parameters.",
    context_settings={
        "allow_extra_args": True,
        "ignore_unknown_options": True,
        "help_option_names": ["-h", "--help"],
    },
)
def run_command(
    ctx: typer.Context,
    pipeline: str = typer.Argument(..., help="Path to the pipeline YAML, or a filename under ./pipelines/"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate and print the pipeline without executing it."),
    show_params: bool = typer.Option(False, "--params", help="Print the pipeline's accepted parameters and exit."),
) -> None:
    pipeline_path = _resolve_pipeline_path(pipeline)
    if not pipeline_path:
        console.print(f"[bold red]✗[/bold red] Pipeline not found: [yellow]{pipeline}[/yellow]")
        _print_available_pipelines()
        raise typer.Exit(code=1)

    try:
        pipeline_loaded = load_pipeline(pipeline_path)
    except Exception as e:
        console.print(f"[bold red]✗[/bold red] Failed to load pipeline: [red]{e}[/red]")
        raise typer.Exit(code=1) from e

    parameters = pipeline_loaded.get("parameters", []) or []
    pipeline_name = os.path.splitext(os.path.basename(pipeline_path))[0]

    if show_params:
        _print_pipeline_param_help(pipeline_name, parameters)
        raise typer.Exit(code=0)

    pipeline_params = _parse_extra_args(list(ctx.args), parameters)

    from src.app import App

    namespace: dict[str, Any] = {
        "pipeline": pipeline_path,
        "pipeline_data": pipeline_loaded,
        "pipeline_params": pipeline_params,
        "dry_run": dry_run,
    }
    app_instance = App(**namespace)
    app_instance.run()


@app.callback(invoke_without_command=True)
def _root(ctx: typer.Context) -> None:
    if ctx.invoked_subcommand is None and len(sys.argv) == 1:
        print_banner()
