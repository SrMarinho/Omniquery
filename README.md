# OmniQuery

Ferramenta de ETL via YAML para consulta e processamento de dados entre múltiplas fontes. O OmniQuery usa **DuckDB** como motor intermediário em memória para carregar dados de origens diversas (bancos de dados, arquivos), transformar via SQL e exportar para diferentes destinos.

[![CI](https://github.com/SrMarinho/Omniquery/actions/workflows/ci.yml/badge.svg)](https://github.com/SrMarinho/Omniquery/actions/workflows/ci.yml)

## Arquitetura

```
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│   Sources     │ ──►  │  DuckDB      │ ──►  │   Outputs    │
│  (Loaders)    │      │  (in-memory) │      │              │
├──────────────┤      └──────────────┘      ├──────────────┤
│ PostgreSQL    │                            │ PostgreSQL   │
│ SQL Server    │                            │ CSV / XLSX   │
│ Oracle        │                            │              │
│ CSV / XLSX    │                            │              │
└──────────────┘                            └──────────────┘
```

## Pré-requisitos

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (gerenciador de pacotes)
- Drivers de banco de dados conforme necessidade:
  - **SQL Server**: ODBC Driver 17 for SQL Server
  - **Oracle**: Oracle Instant Client
  - **PostgreSQL**: libpq

## Instalação

```bash
git clone <repo-url>
cd omniquery
uv sync
```

## Configuração

### Variáveis de ambiente

```bash
cp .env.example .env
# Preencha com suas credenciais
```

#### Credenciais de banco

| Variável | Descrição |
|---|---|
| `DATABASE_*` | Conexão PostgreSQL |
| `PROCFIT_DATABASE_*` | Conexão SQL Server (Procfit) |
| `SENIOR_DATABASE_*` | Conexão Oracle (Senior) |

#### Logging e tuning

| Variável | Padrão | Descrição |
|---|---|---|
| `LOG_LEVEL` | `INFO` | Nível de log: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `DB_CHUNK_SIZE` | `500000` | Linhas por chunk ao ler de banco de dados |
| `DB_THREADS` | `4` | Threads do DuckDB |
| `DB_MEMORY_LIMIT` | `4GB` | Limite de memória do DuckDB |
| `FILE_CHUNK_SIZE` | `100000` | Linhas por chunk ao escrever CSV |
| `DB_BATCH_SIZE` | `500000` | Linhas por batch ao escrever em banco de dados |

### Bancos de dados (`databases.yaml`)

Define as connection strings. As variáveis entre `{{ }}` são substituídas pelas variáveis de ambiente do `.env`:

```yaml
postgresql:
  connection_string: "postgresql://{{DATABASE_USER}}:{{DATABASE_PASSWORD}}@{{DATABASE_HOST}}:{{DATABASE_PORT}}/{{DATABASE_NAME}}"

procfit:
  connection_string: "mssql+pyodbc://{{PROCFIT_DATABASE_USER}}:...@{{PROCFIT_DATABASE_HOST}}/{{PROCFIT_DATABASE_NAME}}?driver=ODBC+Driver+17+for+SQL+Server"
```

## Uso

```bash
# Executar um pipeline
uv run main.py --pipeline pipelines/meu_pipeline.yaml

# Executar com parâmetros dinâmicos
uv run main.py --pipeline pipelines/relatorio.yaml --data_inicio 2024-01-01 --data_fim 2024-12-31

# Validar o pipeline sem executar (dry-run)
uv run main.py --pipeline pipelines/relatorio.yaml --dry-run

# Ver ajuda e parâmetros disponíveis de um pipeline
uv run main.py --pipeline pipelines/relatorio.yaml --help
```

## Pipelines

Os pipelines são definidos em arquivos YAML na pasta `pipelines/`. A extensão [YAML (Red Hat)](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml) no VS Code ativa autocompletar e validação automática via o schema em `schemas/pipeline.schema.json`.

### Estrutura

```yaml
name: nome_do_pipeline
description: Descrição opcional

parameters:
  - name: data_inicio       # Disponível como --data_inicio na CLI e {{ data_inicio }} no YAML
    type: date              # string | integer | float | boolean | date
    required: true
    description: Data de início (YYYY-MM-DD)

loads:
  - type: database          # "database" ou "file"
    source: procfit         # Nome do banco definido em databases.yaml
    tables:
      - alias: vendas       # Nome da tabela no DuckDB (referenciado nos outputs)
        description: Opcional
        type: inline        # "inline" (SQL direto) ou "sql" (caminho de arquivo .sql)
        content: |
          SELECT * FROM PEDIDOS
          WHERE DATA >= '{{ data_inicio }}'

  - type: file
    source: data/metas.csv
    tables:
      - alias: metas

outputs:
  - type: database
    name: relatorio_vendas  # Tabela de destino no banco
    query: |
      SELECT v.*, m.meta
      FROM vendas v
      LEFT JOIN metas m ON v.setor = m.setor
    output_database: postgresql  # Banco definido em databases.yaml
    options:
      if_exists: replace    # "replace" ou "append"

  - type: file
    name: outputs/relatorio.xlsx  # Formato inferido pela extensão (.csv, .xlsx, .xls)
    query: SELECT * FROM vendas
```

### Parâmetros dinâmicos

Parâmetros declarados em `parameters:` são injetados na CLI como flags (`--nome_param`) e substituídos no YAML via `{{ nome_param }}` antes da execução.

```bash
uv run main.py --pipeline pipelines/relatorio.yaml --data_inicio 2024-01-01 --data_fim 2024-12-31
```

## Estrutura do projeto

```
omniquery/
├── cli/
│   └── commands.py              # CLI com Rich — argumentos dinâmicos por pipeline
├── pipelines/                   # Definições de pipelines YAML
├── schemas/
│   └── pipeline.schema.json     # JSON Schema para autocompletar no VS Code
├── data/                        # Arquivos de dados de entrada
├── outputs/                     # Arquivos de saída gerados
├── tests/
│   ├── conftest.py              # Fixtures compartilhadas
│   ├── test_app.py              # Testes de App._substitute_parameters
│   ├── test_entities.py         # Testes de Parameter, Table, Factories, Pipeline
│   └── test_utils.py            # Testes de substitute_env_variables
├── src/
│   ├── app.py                   # Orquestrador principal
│   ├── exceptions.py            # Hierarquia de exceções customizadas
│   ├── config/
│   │   ├── database.py          # Conexão DuckDB in-memory
│   │   └── settings.py          # Constantes e variáveis de ambiente
│   ├── entities/
│   │   ├── loader.py            # DatabaseLoader, FileLoader, LoaderFactory
│   │   ├── output.py            # DatabaseOutput, FileOutput, OutputFactory
│   │   ├── pipeline.py          # Modelo e orquestrador do pipeline
│   │   ├── parameter.py         # Modelo de parâmetro
│   │   └── table.py             # Modelo de tabela
│   └── utils/
│       ├── database_config_reader.py  # Leitor de databases.yaml
│       └── retry.py                   # Retry com backoff exponencial (tenacity)
├── databases.yaml               # Configuração de conexões
├── main.py                      # Entrypoint
└── pyproject.toml
```

## Desenvolvimento

```bash
# Instalar dependências de desenvolvimento
uv sync --group dev

# Rodar testes
uv run pytest tests/ -v

# Lint
uv run ruff check .
uv run ruff check . --fix

# Formatação
uv run ruff format .

# Verificação de tipos
uv run mypy src/ cli/ main.py
```

### Pre-commit

O projeto usa [pre-commit](https://pre-commit.com/) para garantir qualidade antes de cada commit:

```bash
uv run pre-commit install
```

Hooks configurados: Ruff (lint + format), Mypy, trailing whitespace, end-of-file.

## Tecnologias

| Biblioteca | Uso |
|---|---|
| [DuckDB](https://duckdb.org/) | Banco analítico in-memory para processamento intermediário |
| [SQLAlchemy](https://www.sqlalchemy.org/) | Conexão com bancos relacionais |
| [Pydantic](https://docs.pydantic.dev/) | Validação de dados e modelos |
| [Pandas](https://pandas.pydata.org/) | Transferência em chunks entre fontes |
| [Rich](https://github.com/Textualize/rich) | CLI com formatação visual e logging colorido |
| [Tenacity](https://tenacity.readthedocs.io/) | Retry com backoff exponencial para conexões |
| [tqdm](https://tqdm.github.io/) | Barra de progresso nas transferências |
| [uv](https://docs.astral.sh/uv/) | Gerenciador de pacotes e ambientes |
| [Ruff](https://docs.astral.sh/ruff/) | Linter e formatador |
| [Mypy](https://mypy-lang.org/) | Verificação estática de tipos |
| [pytest](https://pytest.org/) | Framework de testes |
