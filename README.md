# OmniQuery

Ferramenta versátil para consulta e processamento de dados entre múltiplas fontes. O OmniQuery utiliza **DuckDB** como banco intermediário em memória para carregar dados de diversas origens (bancos de dados, arquivos), transformar via SQL e exportar para diferentes destinos.

## Arquitetura

```
┌─────────────┐      ┌──────────────┐      ┌──────────────┐
│   Sources    │ ──►  │  DuckDB      │ ──►  │   Outputs    │
│  (Loaders)   │      │  (in-memory) │      │              │
├─────────────┤      └──────────────┘      ├──────────────┤
│ PostgreSQL   │                            │ PostgreSQL   │
│ SQL Server   │                            │ CSV / XLSX   │
│ Oracle       │                            │              │
│ CSV          │                            │              │
└─────────────┘                            └──────────────┘
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
# Clone o repositório
git clone <repo-url>
cd omniquery

# Instale as dependências com uv
uv sync
```

## Configuração

### Variáveis de ambiente

Copie `.env.example` para `.env` e preencha com suas credenciais:

```bash
cp .env.example .env
```

#### Credenciais de banco

| Variável | Descrição |
|---|---|
| `DATABASE_*` | Conexão PostgreSQL |
| `PROCFIT_DATABASE_*` | Conexão SQL Server (Procfit) |
| `SENIOR_DATABASE_*` | Conexão Oracle (Senior) |

#### Logging

| Variável | Padrão | Descrição |
|---|---|---|
| `LOG_LEVEL` | `INFO` | Nível de log: `DEBUG`, `INFO`, `WARNING`, `ERROR` |

#### Tuning de transferência

| Variável | Padrão | Descrição |
|---|---|---|
| `DB_CHUNK_SIZE` | `500000` | Linhas por chunk ao ler de banco de dados |
| `DB_THREADS` | `4` | Threads do DuckDB para processamento |
| `DB_MEMORY_LIMIT` | `4GB` | Limite de memória do DuckDB |
| `FILE_CHUNK_SIZE` | `100000` | Linhas por chunk ao escrever CSV |
| `DB_BATCH_SIZE` | `500000` | Linhas por batch ao escrever em banco de dados |

### Bancos de dados (`databases.yaml`)

O arquivo `databases.yaml` define as connection strings dos bancos. As variáveis entre `{{ }}` são substituídas automaticamente pelas variáveis de ambiente do `.env`:

```yaml
postgresql:
  connection_string: "postgresql://{{DATABASE_USER}}:{{DATABASE_PASSWORD}}@{{DATABASE_HOST}}:{{DATABASE_PORT}}/{{DATABASE_NAME}}"

procfit:
  connection_string: "mssql+pyodbc://{{PROCFIT_DATABASE_USER}}:...@{{PROCFIT_DATABASE_HOST}}:{{PROCFIT_DATABASE_PORT}}/{{PROCFIT_DATABASE_NAME}}?driver=ODBC+Driver+17+for+SQL+Server"
```

## Uso

```bash
uv run main.py --pipeline pipelines/meu_pipeline.yaml
```

## Pipelines

Os pipelines são definidos em arquivos YAML dentro da pasta `pipelines/`. Cada pipeline possui duas seções principais: **loads** (fontes de dados) e **outputs** (destinos).

### Estrutura do pipeline

```yaml
name: nome_do_pipeline
description: Descrição opcional

loads:
  - type: database          # Tipo do loader: "database" ou "file"
    source: procfit          # Nome do banco definido em databases.yaml
    tables:
      - alias: minha_tabela  # Nome da tabela no DuckDB
        description: Descrição opcional
        type: inline         # "inline" (SQL direto) ou "sql" (arquivo .sql)
        content: |
          SELECT * FROM tabela_origem
          WHERE coluna = 'valor'

  - type: file
    source: data/arquivo.csv
    tables:
      - alias: dados_csv

outputs:
  - name: tabela_destino     # Nome da tabela/arquivo de saída
    query: |
      SELECT * FROM minha_tabela
      JOIN dados_csv ON ...
    type: database            # "database" ou "file"
    options:
      if_exists: replace      # "replace" para recriar a tabela
```

### Exemplo completo

```yaml
name: relatorio_vendas
description: Consolida dados de vendas de múltiplas fontes

loads:
  - type: database
    source: procfit
    tables:
      - alias: vendas
        type: inline
        content: |
          SELECT * FROM PEDIDOS_VENDAS
          WHERE DATA >= '2025-01-01'

  - type: file
    source: data/metas.csv
    tables:
      - alias: metas

outputs:
  - name: relatorio_vendas
    query: |
      SELECT v.*, m.meta_mensal
      FROM vendas v
      LEFT JOIN metas m ON v.setor = m.setor
    type: database

  - name: outputs/relatorio_vendas.csv
    query: |
      SELECT * FROM vendas
    type: file
```

## Estrutura do projeto

```
omniquery/
├── cli/
│   └── commands.py          # Parser de argumentos CLI
├── data/                    # Arquivos de dados de entrada
├── outputs/                 # Arquivos de saída gerados
├── pipelines/               # Definições de pipelines YAML
├── src/
│   ├── app.py               # Classe principal da aplicação
│   ├── config/
│   │   ├── database.py      # Conexão DuckDB in-memory
│   │   └── settings.py      # Configurações gerais
│   ├── entities/
│   │   ├── loader.py        # Loaders (DatabaseLoader, FileLoader)
│   │   ├── output.py        # Outputs (DatabaseOutput, FileOutput)
│   │   ├── pipeline.py      # Orquestrador do pipeline
│   │   └── table.py         # Modelo de tabela
│   ├── types/
│   │   ├── file_output_format_types.py
│   │   └── table_types.py
│   └── utils/
│       └── database_config_reader.py  # Leitor de databases.yaml
├── databases.yaml           # Configuração de conexões
├── main.py                  # Entrypoint
└── pyproject.toml
```

## Tipos de Loader

- **`database`** — Carrega dados de bancos via SQLAlchemy (PostgreSQL, SQL Server, Oracle)
- **`file`** — Carrega dados de arquivos CSV, XLSX e XLS

## Tipos de Output

- **`database`** — Exporta para banco de dados usando COPY otimizado (PostgreSQL)
- **`file`** — Exporta para arquivo CSV, XLSX ou XLS (formato inferido pela extensão do nome)

## Desenvolvimento

### Instalando dependências de desenvolvimento

```bash
uv sync --group dev
```

### Lint e formatação (Ruff)

```bash
# Verificar problemas
uv run ruff check .

# Corrigir automaticamente
uv run ruff check . --fix

# Formatar código
uv run ruff format .
```

### Verificação de tipos (Mypy)

```bash
uv run mypy src/ cli/ main.py
```

## Tecnologias

- **[DuckDB](https://duckdb.org/)** — Banco analítico in-memory para processamento intermediário
- **[SQLAlchemy](https://www.sqlalchemy.org/)** — ORM e conexão com bancos relacionais
- **[Pydantic](https://docs.pydantic.dev/)** — Validação de dados e modelos
- **[Pandas](https://pandas.pydata.org/)** — Manipulação de DataFrames para transferência em chunks
- **[uv](https://docs.astral.sh/uv/)** — Gerenciador de pacotes Python
- **[Ruff](https://docs.astral.sh/ruff/)** — Linter e formatador Python
- **[Mypy](https://mypy-lang.org/)** — Verificação estática de tipos
