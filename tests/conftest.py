import pytest


@pytest.fixture
def minimal_pipeline_file(tmp_path):
    """Cria um arquivo YAML de pipeline mínimo válido para testes."""
    f = tmp_path / "pipeline.yaml"
    f.write_text("name: test\n")
    return str(f)
