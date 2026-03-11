class OmniQueryError(Exception):
    """Exceção base do OmniQuery."""


class ConfigError(OmniQueryError):
    """Erro de configuração (databases.yaml, .env, pipeline YAML)."""


class PipelineError(OmniQueryError):
    """Erro na definição ou validação de um pipeline."""


class LoaderError(OmniQueryError):
    """Erro durante a carga de dados de uma fonte."""


class OutputError(OmniQueryError):
    """Erro durante a escrita de dados em um destino."""
