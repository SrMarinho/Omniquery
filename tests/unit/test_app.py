"""Unit tests for src/app.py — the _substitute_parameters method."""

import pytest

from src.app import App


@pytest.fixture
def app(minimal_pipeline_file):
    return App(pipeline=minimal_pipeline_file)


class TestSubstituteParameters:
    def test_substitutes_simple_string(self, app):
        result = app._substitute_parameters("{{ start_date }}", {"start_date": "2024-01-01"})
        assert result == "2024-01-01"

    def test_substitutes_in_dict(self, app):
        data = {"content": "SELECT * FROM t WHERE date >= '{{ start_date }}'"}
        result = app._substitute_parameters(data, {"start_date": "2024-01-01"})
        assert result["content"] == "SELECT * FROM t WHERE date >= '2024-01-01'"

    def test_substitutes_in_list(self, app):
        data = ["{{ a }}", "{{ b }}"]
        result = app._substitute_parameters(data, {"a": "foo", "b": "bar"})
        assert result == ["foo", "bar"]

    def test_substitutes_recursively_in_nested_structure(self, app):
        data = {"loads": [{"content": "SELECT {{ col }} FROM {{ table }}"}]}
        result = app._substitute_parameters(data, {"col": "id", "table": "customers"})
        assert result["loads"][0]["content"] == "SELECT id FROM customers"

    def test_keeps_placeholder_when_param_missing(self, app):
        result = app._substitute_parameters("{{ missing }}", {})
        assert result == "{{ missing }}"

    def test_leaves_non_string_types_unchanged(self, app):
        assert app._substitute_parameters(42, {"x": "y"}) == 42
        assert app._substitute_parameters(True, {"x": "y"}) is True
        assert app._substitute_parameters(None, {"x": "y"}) is None

    def test_substitutes_multiple_params_in_same_string(self, app):
        result = app._substitute_parameters(
            "{{ start }} to {{ end }}",
            {"start": "2024-01-01", "end": "2024-12-31"},
        )
        assert result == "2024-01-01 to 2024-12-31"
