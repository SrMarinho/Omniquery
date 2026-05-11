import pytest
from dotenv import load_dotenv

load_dotenv()


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--rows", type=int, default=500_000, help="Number of rows in the synthetic table")
    parser.addoption("--repeat", type=int, default=3, help="Repetitions per benchmark")


@pytest.fixture
def minimal_pipeline_file(tmp_path):
    """Create a minimal valid pipeline YAML file for tests."""
    f = tmp_path / "pipeline.yaml"
    f.write_text("name: test\n")
    return str(f)
