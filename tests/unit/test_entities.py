"""Unit tests for the entity layer: Parameter, Table, LoaderFactory, OutputFactory and Pipeline."""

import pytest
from pydantic import ValidationError

from src.entities.loader import DatabaseLoader, FileLoader, Loader, LoaderFactory
from src.entities.output import DatabaseOutput, FileOutput, Output, OutputFactory
from src.entities.parameter import Parameter
from src.entities.pipeline import Pipeline
from src.entities.table import Table
from src.types.table_types import TableTypes


class TestParameter:
    def test_creates_with_name_only(self):
        p = Parameter(name="start_date")
        assert p.name == "start_date"
        assert p.type == "string"
        assert p.required is False
        assert p.description == ""

    def test_creates_with_all_fields(self):
        p = Parameter(name="code", type="integer", description="Company code", required=True)
        assert p.type == "integer"
        assert p.required is True

    def test_name_is_required(self):
        with pytest.raises(ValidationError):
            Parameter()  # type: ignore[call-arg]

    def test_default_type_is_string(self):
        assert Parameter(name="x").type == "string"

    def test_default_required_is_false(self):
        assert Parameter(name="x").required is False


class TestTable:
    def test_creates_with_alias_only(self):
        t = Table(alias="sales")
        assert t.alias == "sales"
        assert t.type == TableTypes.INLINE
        assert t.content == ""
        assert t.description == ""
        assert t.options == {}

    def test_alias_is_required(self):
        with pytest.raises(ValidationError):
            Table()  # type: ignore[call-arg]

    def test_default_type_is_inline(self):
        assert Table(alias="t").type == TableTypes.INLINE

    def test_accepts_explicit_inline_type(self):
        t = Table(alias="t", type="inline")  # type: ignore[arg-type]
        assert t.type == TableTypes.INLINE

    def test_accepts_content(self):
        t = Table(alias="t", content="SELECT 1")
        assert t.content == "SELECT 1"

    def test_accepts_options(self):
        t = Table(alias="t", options={"if_exists": "append"})
        assert t.options["if_exists"] == "append"


class TestLoaderFactory:
    def test_creates_database_loader(self):
        loader = LoaderFactory.create(
            {
                "type": "database",
                "source": "procfit",
                "tables": [{"alias": "t1", "content": "SELECT 1"}],
            }
        )
        assert isinstance(loader, DatabaseLoader)
        assert loader.type == "database"
        assert loader.source == "procfit"

    def test_creates_file_loader(self):
        loader = LoaderFactory.create(
            {
                "type": "file",
                "source": "data/file.csv",
                "tables": [{"alias": "t1"}],
            }
        )
        assert isinstance(loader, FileLoader)
        assert loader.source == "data/file.csv"

    def test_unknown_type_returns_base_loader(self):
        loader = LoaderFactory.create(
            {
                "type": "unknown",
                "source": "x",
                "tables": [],
            }
        )
        assert type(loader) is Loader

    def test_database_loader_defaults_database_to_memory(self):
        loader = LoaderFactory.create(
            {
                "type": "database",
                "source": "db",
                "tables": [],
            }
        )
        assert isinstance(loader, DatabaseLoader)
        assert loader.database == "memory"

    def test_file_loader_accepts_tables_without_content(self):
        loader = LoaderFactory.create(
            {
                "type": "file",
                "source": "data.csv",
                "tables": [{"alias": "data"}],
            }
        )
        assert loader.tables[0].alias == "data"


class TestOutputFactory:
    def test_creates_database_output(self):
        output = OutputFactory.create(
            {
                "type": "database",
                "name": "result",
                "query": "SELECT 1",
            }
        )
        assert isinstance(output, DatabaseOutput)
        assert output.output_database == "postgresql"

    def test_creates_file_output(self):
        output = OutputFactory.create(
            {
                "type": "file",
                "name": "outputs/result.csv",
                "query": "SELECT * FROM t",
            }
        )
        assert isinstance(output, FileOutput)
        assert output.name == "outputs/result.csv"

    def test_unknown_type_returns_base_output(self):
        output = OutputFactory.create(
            {
                "type": "nonexistent",
                "name": "x",
                "query": "SELECT 1",
            }
        )
        assert type(output) is Output

    def test_database_output_accepts_custom_output_database(self):
        output = OutputFactory.create(
            {
                "type": "database",
                "name": "table",
                "query": "SELECT 1",
                "output_database": "senior",
            }
        )
        assert isinstance(output, DatabaseOutput)
        assert output.output_database == "senior"

    def test_output_accepts_options(self):
        output = OutputFactory.create(
            {
                "type": "file",
                "name": "out.csv",
                "query": "SELECT 1",
                "options": {"if_exists": "append"},
            }
        )
        assert output.options["if_exists"] == "append"


class TestPipeline:
    def test_creates_empty_pipeline(self):
        p = Pipeline()
        assert p.name == ""
        assert p.loads == []
        assert p.outputs == []
        assert p.parameters == []

    def test_creates_pipeline_with_name(self):
        p = Pipeline(name="my_pipeline")
        assert p.name == "my_pipeline"

    def test_model_validator_builds_file_loader(self):
        p = Pipeline(loads=[{"type": "file", "source": "data.csv", "tables": [{"alias": "t1"}]}])  # type: ignore[list-item]
        assert isinstance(p.loads[0], FileLoader)

    def test_model_validator_builds_database_loader(self):
        p = Pipeline(loads=[{"type": "database", "source": "db", "tables": []}])  # type: ignore[list-item]
        assert isinstance(p.loads[0], DatabaseLoader)

    def test_model_validator_builds_file_output(self):
        p = Pipeline(outputs=[{"type": "file", "name": "out.csv", "query": "SELECT 1"}])  # type: ignore[list-item]
        assert isinstance(p.outputs[0], FileOutput)

    def test_model_validator_builds_database_output(self):
        p = Pipeline(outputs=[{"type": "database", "name": "table", "query": "SELECT 1"}])  # type: ignore[list-item]
        assert isinstance(p.outputs[0], DatabaseOutput)

    def test_parameters_are_deserialized(self):
        p = Pipeline(parameters=[{"name": "start_date", "type": "date", "required": True}])  # type: ignore[list-item]
        assert isinstance(p.parameters[0], Parameter)
        assert p.parameters[0].required is True

    def test_accepts_already_instantiated_loader(self):
        loader = FileLoader(type="file", source="data.csv", tables=[])
        p = Pipeline(loads=[loader])
        assert p.loads[0] is loader

    def test_accepts_already_instantiated_output(self):
        output = FileOutput(type="file", name="out.csv", query="SELECT 1")
        p = Pipeline(outputs=[output])
        assert p.outputs[0] is output
