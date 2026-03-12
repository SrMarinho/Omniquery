import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--rows", type=int, default=500_000, help="Numero de linhas na tabela sintetica")
    parser.addoption("--repeat", type=int, default=3, help="Repeticoes por benchmark")


@pytest.fixture
def minimal_pipeline_file(tmp_path):
    """Cria um arquivo YAML de pipeline mínimo válido para testes."""
    f = tmp_path / "pipeline.yaml"
    f.write_text("name: test\n")
    return str(f)
