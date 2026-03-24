"""Testes unitários para src/app.py — método _substitute_parameters."""

import pytest

from src.app import App


@pytest.fixture
def app(minimal_pipeline_file):
    return App(pipeline=minimal_pipeline_file)


class TestSubstituteParameters:
    def test_substitui_string_simples(self, app):
        result = app._substitute_parameters("{{ data_inicio }}", {"data_inicio": "2024-01-01"})
        assert result == "2024-01-01"

    def test_substitui_em_dict(self, app):
        data = {"content": "SELECT * FROM t WHERE data >= '{{ data_inicio }}'"}
        result = app._substitute_parameters(data, {"data_inicio": "2024-01-01"})
        assert result["content"] == "SELECT * FROM t WHERE data >= '2024-01-01'"

    def test_substitui_em_lista(self, app):
        data = ["{{ a }}", "{{ b }}"]
        result = app._substitute_parameters(data, {"a": "foo", "b": "bar"})
        assert result == ["foo", "bar"]

    def test_substitui_recursivamente_em_estrutura_aninhada(self, app):
        data = {"loads": [{"content": "SELECT {{ col }} FROM {{ table }}"}]}
        result = app._substitute_parameters(data, {"col": "id", "table": "clientes"})
        assert result["loads"][0]["content"] == "SELECT id FROM clientes"

    def test_mantém_placeholder_quando_param_ausente(self, app):
        result = app._substitute_parameters("{{ nao_existe }}", {})
        assert result == "{{ nao_existe }}"

    def test_nao_altera_tipos_nao_string(self, app):
        assert app._substitute_parameters(42, {"x": "y"}) == 42
        assert app._substitute_parameters(True, {"x": "y"}) is True
        assert app._substitute_parameters(None, {"x": "y"}) is None

    def test_substitui_multiplos_params_na_mesma_string(self, app):
        result = app._substitute_parameters(
            "{{ inicio }} ate {{ fim }}",
            {"inicio": "2024-01-01", "fim": "2024-12-31"},
        )
        assert result == "2024-01-01 ate 2024-12-31"
