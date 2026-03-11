# Changelog

Todas as mudanças notáveis deste projeto estão documentadas neste arquivo.

O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/),
e o projeto adere ao [Versionamento Semântico](https://semver.org/lang/pt-BR/).

---

## [Não lançado]

### Adicionado
- Suporte a Excel (XLSX/XLS) no `FileLoader` e `FileOutput`
- Modo `--dry-run` na CLI

---

## [0.3.0] - 2026-03-11

### Adicionado
- Barra de progresso `tqdm` nas transferências de banco e CSV (`loader.py`, `output.py`)
- Validação de schema do pipeline YAML com mensagem de erro por campo (`app.py`)
- Hierarquia de exceções customizadas: `OmniQueryError`, `LoaderError`, `OutputError`, `ConfigError`, `PipelineError` (`src/exceptions.py`)
- `.env.example` com todas as variáveis de ambiente documentadas
- Workflow de CI com GitHub Actions (lint, format, mypy)

### Alterado
- Constantes mágicas (`DB_CHUNK_SIZE`, `DB_THREADS`, `DB_MEMORY_LIMIT`, `FILE_CHUNK_SIZE`, `DB_BATCH_SIZE`) movidas para `src/config/settings.py` e configuráveis via variável de ambiente
- `src/util.py/json_reader.py` movido para `src/utils/json_reader.py`

---

## [0.2.0] - 2026-03-11

### Adicionado
- Retry logic com backoff exponencial via `tenacity` para conexões com banco de dados (`DatabaseLoader.get_engine`, `DatabaseOutput._get_engine`)
- Módulo `src/utils/retry.py` com decorator `db_retry` (3 tentativas, espera 2s→4s→8s)
- pre-commit configurado com hooks: `trailing-whitespace`, `end-of-file-fixer`, `check-yaml`, `check-merge-conflict`, `ruff`, `ruff-format`, `mypy`
- Logging estruturado com `logging.getLogger(__name__)` em todos os módulos, substituindo `print()`
- `src/config/logging_config.py` com `setup_logging()` configurável via `LOG_LEVEL`

### Adicionado (ferramentas)
- Ruff (lint + format) configurado em `pyproject.toml`
- Mypy (type checking) configurado em `pyproject.toml`

---

## [0.1.0] - 2025-03-01

### Adicionado
- Pipeline YAML com suporte a `loads` e `outputs`
- `DatabaseLoader`: carrega dados de PostgreSQL, SQL Server e Oracle via SQLAlchemy + DuckDB in-memory
- `FileLoader`: carrega dados de arquivos CSV para DuckDB
- `DatabaseOutput`: exporta do DuckDB para PostgreSQL usando COPY otimizado em batches
- `FileOutput`: exporta do DuckDB para CSV em chunks
- CLI com suporte a parâmetros dinâmicos por pipeline (`--param valor`)
- `databases.yaml` com substituição de variáveis de ambiente `{{ VAR }}`
- Suporte a parâmetros tipados nos pipelines (`str`, `int`, `float`, `bool`, `date`)
