import logging
import os
import re

import yaml

from src.config.settings import base_path
from src.exceptions import ConfigError

logger = logging.getLogger(__name__)


def get_database_config(database: str) -> dict:  # type: ignore[type-arg]
    """
    Get database configuration from 'databases.yaml' in root path
    """
    database_file = "databases.yaml"
    config_file = base_path / database_file
    if not config_file.exists():
        raise FileNotFoundError(f"File {database_file} not found in root path")
    with open(config_file, encoding="utf-8") as file:
        data = yaml.safe_load(file)  # type: ignore[no-any-return]
        if database not in data:
            raise ConfigError(f"Database '{database}' not found in {database_file}")
        if "connection_string" not in data[database]:
            raise ConfigError(f"connection_string not configured for source '{database}'")
        data[database]["connection_string"] = substitute_env_variables(data[database]["connection_string"])
        return data[database]  # type: ignore[no-any-return]


def substitute_env_variables(connection_string: str) -> str:
    """
    Substitui variáveis entre {{ }} pelos valores correspondentes do ambiente
    """

    def replace_match(match: re.Match) -> str:  # type: ignore[type-arg]
        env_var = match.group(1)
        return os.getenv(env_var.strip(), f"{{{env_var}}}")

    pattern = r"\{\{\s*([^}]+)\s*\}\}"

    return re.sub(pattern, replace_match, connection_string)
