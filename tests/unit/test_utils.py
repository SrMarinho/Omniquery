"""Unit tests for src/utils/database_config_reader.py — the substitute_env_variables function."""

from src.utils.database_config_reader import substitute_env_variables


class TestSubstituteEnvVariables:
    def test_substitutes_existing_variable(self, monkeypatch):
        monkeypatch.setenv("DB_HOST", "localhost")
        result = substitute_env_variables("postgresql://{{ DB_HOST }}/mydb")
        assert result == "postgresql://localhost/mydb"

    def test_substitutes_multiple_variables(self, monkeypatch):
        monkeypatch.setenv("DB_USER", "admin")
        monkeypatch.setenv("DB_PASS", "secret")
        result = substitute_env_variables("{{ DB_USER }}:{{ DB_PASS }}@host")
        assert result == "admin:secret@host"

    def test_keeps_placeholder_when_variable_missing(self):
        # When the env var is absent, the implementation returns { VAR } (single braces).
        result = substitute_env_variables("host://{{ MISSING_VAR }}/db")
        assert "MISSING_VAR" in result

    def test_ignores_string_without_placeholders(self):
        result = substitute_env_variables("postgresql://localhost/mydb")
        assert result == "postgresql://localhost/mydb"

    def test_substitutes_with_surrounding_whitespace(self, monkeypatch):
        monkeypatch.setenv("DB_HOST", "server01")
        result = substitute_env_variables("{{  DB_HOST  }}")
        assert result == "server01"

    def test_empty_string(self):
        assert substitute_env_variables("") == ""
