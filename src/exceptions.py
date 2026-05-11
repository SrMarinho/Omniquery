class OmniQueryError(Exception):
    """Base exception for OmniQuery."""


class ConfigError(OmniQueryError):
    """Configuration error (databases.yaml, .env, pipeline YAML)."""


class PipelineError(OmniQueryError):
    """Pipeline definition or validation error."""


class LoaderError(OmniQueryError):
    """Error while loading data from a source."""


class OutputError(OmniQueryError):
    """Error while writing data to a destination."""
