# CI/CD

| Workflow | Trigger | What it does |
|---|---|---|
| **CI** (`.github/workflows/ci.yml`) | push / pull request | Ruff lint, Mypy, unit tests, pipeline schema validation |

E2E tests are not part of CI — they require real database credentials and are intended for local/manual homolog runs (see [testing.md](testing.md)).

Dependabot (`.github/dependabot.yml`) checks for `uv` dependency updates monthly.
