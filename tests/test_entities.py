"""Testes unitários para as entidades: Parameter, Table, LoaderFactory, OutputFactory e Pipeline."""

import pytest
from pydantic import ValidationError

from src.entities.loader import DatabaseLoader, FileLoader, Loader, LoaderFactory
from src.entities.output import DatabaseOutput, FileOutput, Output, OutputFactory
from src.entities.parameter import Parameter
from src.entities.pipeline import Pipeline
from src.entities.table import Table
from src.types.table_types import TableTypes


class TestParameter:
    def test_cria_com_apenas_nome(self):
        p = Parameter(name="data_inicio")
        assert p.name == "data_inicio"
        assert p.type == "string"
        assert p.required is False
        assert p.description == ""

    def test_cria_com_todos_os_campos(self):
        p = Parameter(name="codigo", type="integer", description="Código da empresa", required=True)
        assert p.type == "integer"
        assert p.required is True

    def test_nome_obrigatorio(self):
        with pytest.raises(ValidationError):
            Parameter()  # type: ignore[call-arg]

    def test_default_type_string(self):
        assert Parameter(name="x").type == "string"

    def test_default_required_false(self):
        assert Parameter(name="x").required is False


class TestTable:
    def test_cria_com_apenas_alias(self):
        t = Table(alias="vendas")
        assert t.alias == "vendas"
        assert t.type == TableTypes.INLINE
        assert t.content == ""
        assert t.description == ""
        assert t.options == {}

    def test_alias_obrigatorio(self):
        with pytest.raises(ValidationError):
            Table()  # type: ignore[call-arg]

    def test_tipo_padrao_inline(self):
        assert Table(alias="t").type == TableTypes.INLINE

    def test_aceita_type_inline_explícito(self):
        t = Table(alias="t", type="inline")
        assert t.type == TableTypes.INLINE

    def test_aceita_content(self):
        t = Table(alias="t", content="SELECT 1")
        assert t.content == "SELECT 1"

    def test_aceita_options(self):
        t = Table(alias="t", options={"if_exists": "append"})
        assert t.options["if_exists"] == "append"


class TestLoaderFactory:
    def test_cria_database_loader(self):
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

    def test_cria_file_loader(self):
        loader = LoaderFactory.create(
            {
                "type": "file",
                "source": "data/arquivo.csv",
                "tables": [{"alias": "t1"}],
            }
        )
        assert isinstance(loader, FileLoader)
        assert loader.source == "data/arquivo.csv"

    def test_type_desconhecido_retorna_loader_base(self):
        loader = LoaderFactory.create(
            {
                "type": "desconhecido",
                "source": "x",
                "tables": [],
            }
        )
        assert type(loader) is Loader

    def test_database_loader_tem_database_memory_por_padrao(self):
        loader = LoaderFactory.create(
            {
                "type": "database",
                "source": "db",
                "tables": [],
            }
        )
        assert isinstance(loader, DatabaseLoader)
        assert loader.database == "memory"

    def test_file_loader_aceita_tabelas_sem_content(self):
        loader = LoaderFactory.create(
            {
                "type": "file",
                "source": "data.csv",
                "tables": [{"alias": "dados"}],
            }
        )
        assert loader.tables[0].alias == "dados"


class TestOutputFactory:
    def test_cria_database_output(self):
        output = OutputFactory.create(
            {
                "type": "database",
                "name": "resultado",
                "query": "SELECT 1",
            }
        )
        assert isinstance(output, DatabaseOutput)
        assert output.output_database == "postgresql"

    def test_cria_file_output(self):
        output = OutputFactory.create(
            {
                "type": "file",
                "name": "outputs/resultado.csv",
                "query": "SELECT * FROM t",
            }
        )
        assert isinstance(output, FileOutput)
        assert output.name == "outputs/resultado.csv"

    def test_type_desconhecido_retorna_output_base(self):
        output = OutputFactory.create(
            {
                "type": "inexistente",
                "name": "x",
                "query": "SELECT 1",
            }
        )
        assert type(output) is Output

    def test_database_output_aceita_output_database_customizado(self):
        output = OutputFactory.create(
            {
                "type": "database",
                "name": "tabela",
                "query": "SELECT 1",
                "output_database": "senior",
            }
        )
        assert isinstance(output, DatabaseOutput)
        assert output.output_database == "senior"

    def test_output_aceita_options(self):
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
    def test_cria_pipeline_vazio(self):
        p = Pipeline()
        assert p.name == ""
        assert p.loads == []
        assert p.outputs == []
        assert p.parameters == []

    def test_cria_pipeline_com_nome(self):
        p = Pipeline(name="meu_pipeline")
        assert p.name == "meu_pipeline"

    def test_model_validator_cria_file_loader(self):
        p = Pipeline(loads=[{"type": "file", "source": "data.csv", "tables": [{"alias": "t1"}]}])
        assert isinstance(p.loads[0], FileLoader)

    def test_model_validator_cria_database_loader(self):
        p = Pipeline(loads=[{"type": "database", "source": "db", "tables": []}])
        assert isinstance(p.loads[0], DatabaseLoader)

    def test_model_validator_cria_file_output(self):
        p = Pipeline(outputs=[{"type": "file", "name": "out.csv", "query": "SELECT 1"}])
        assert isinstance(p.outputs[0], FileOutput)

    def test_model_validator_cria_database_output(self):
        p = Pipeline(outputs=[{"type": "database", "name": "tabela", "query": "SELECT 1"}])
        assert isinstance(p.outputs[0], DatabaseOutput)

    def test_parametros_sao_deserializados(self):
        p = Pipeline(parameters=[{"name": "data_inicio", "type": "date", "required": True}])
        assert isinstance(p.parameters[0], Parameter)
        assert p.parameters[0].required is True

    def test_loader_ja_instanciado_é_aceito(self):
        loader = FileLoader(type="file", source="data.csv", tables=[])
        p = Pipeline(loads=[loader])
        assert p.loads[0] is loader

    def test_output_ja_instanciado_é_aceito(self):
        output = FileOutput(type="file", name="out.csv", query="SELECT 1")
        p = Pipeline(outputs=[output])
        assert p.outputs[0] is output
