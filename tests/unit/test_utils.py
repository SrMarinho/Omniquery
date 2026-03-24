"""Testes unitários para src/utils/database_config_reader.py — função substitute_env_variables."""

from src.utils.database_config_reader import substitute_env_variables


class TestSubstituteEnvVariables:
    def test_substitui_variavel_existente(self, monkeypatch):
        monkeypatch.setenv("DB_HOST", "localhost")
        result = substitute_env_variables("postgresql://{{ DB_HOST }}/mydb")
        assert result == "postgresql://localhost/mydb"

    def test_substitui_multiplas_variaveis(self, monkeypatch):
        monkeypatch.setenv("DB_USER", "admin")
        monkeypatch.setenv("DB_PASS", "secret")
        result = substitute_env_variables("{{ DB_USER }}:{{ DB_PASS }}@host")
        assert result == "admin:secret@host"

    def test_mantém_placeholder_quando_variavel_ausente(self):
        # Quando a variável não existe, a implementação retorna { VAR } (chave simples)
        result = substitute_env_variables("host://{{ VAR_INEXISTENTE }}/db")
        assert "VAR_INEXISTENTE" in result

    def test_ignora_string_sem_placeholders(self):
        result = substitute_env_variables("postgresql://localhost/mydb")
        assert result == "postgresql://localhost/mydb"

    def test_substitui_com_espacos_ao_redor_do_nome(self, monkeypatch):
        monkeypatch.setenv("DB_HOST", "server01")
        result = substitute_env_variables("{{  DB_HOST  }}")
        assert result == "server01"

    def test_string_vazia(self):
        assert substitute_env_variables("") == ""
