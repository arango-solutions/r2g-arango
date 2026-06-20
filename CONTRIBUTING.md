# Contributing to r2g

Thanks for your interest in improving `r2g`. This document covers how to set
up a development environment, the conventions the project follows, and the
workflow for landing changes.

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By
participating you agree to uphold its terms. Report unacceptable behaviour
to the maintainers (see [SECURITY.md](SECURITY.md) for the contact channel).

## Ways to contribute

- **Report a bug** — open an issue using the *Bug report* template. Include
  the smallest reproduction you can; CLI invocations, schema fragments, and
  stack traces are gold.
- **Request a feature** — open an issue using the *Feature request*
  template. Explain the use case before the proposed implementation.
- **Improve documentation** — typo fixes, clearer examples, and additional
  walk-throughs are very welcome.
- **Send a pull request** — see the workflow below.

## Development setup

Requirements:

- Python 3.10+
- Optional: Docker (for integration tests against PostgreSQL / MySQL / SQL Server + ArangoDB)

```bash
git clone https://github.com/ArthurKeen/r2g-arango.git
cd r2g-arango
python -m venv .venv && source .venv/bin/activate
pip install -e ".[test,dev,ui]"
```

Optional extras:

- `pip install -e ".[snowflake]"` — Snowflake source support.
- `pip install -e ".[kafka]"` — Kafka CDC consumer.
- `pip install -e ".[mcp]"` — MCP server for AI agent integration.

## Running tests

```bash
# Unit tests only (fast, no Docker needed)
pytest tests/ -m "not integration"

# Lint
ruff check src/ tests/

# Full suite, including integration tests
# Requires PostgreSQL / MySQL / SQL Server + ArangoDB reachable per tests/integration/conftest.py
pytest tests/ -v

# Type checking
mypy src/r2g
```

`docker-compose.yml` provisions the integration-test backends (PostgreSQL,
MySQL, SQL Server, ArangoDB). PostgreSQL and MySQL are seeded from
`docker/`; the SQL Server schema is seeded by the test fixture (the image has
no init-script hook):

```bash
docker compose up -d --wait        # start postgres + mysql + sqlserver + arangodb
PG_CONN=postgresql://r2g:r2g_test_2026@localhost:5432/northwind \
MYSQL_CONN=mysql://r2g:r2g_test_2026@localhost:3306/shop \
MSSQL_CONN='mssql://sa:r2g_Test_2026!@localhost:1433/shop' \
ARANGO_ENDPOINT=http://localhost:8540 ARANGO_PASSWORD=r2g_test_2026 \
  pytest tests/integration/ -m integration
docker compose down -v             # tear down + drop volumes
```

Integration tests skip automatically when a backend is unreachable, so the
default `pytest tests/ -m "not integration"` run needs no Docker.

The OpenMetadata catalog e2e (`tests/integration/test_openmetadata_e2e.py`) is
**not** part of `docker-compose.yml` — OpenMetadata's stack (server + DB +
search engine) is heavy and best started from its own
[official quickstart](https://docs.open-metadata.org/latest/quickstart). Point
the tests at it and they un-skip:

```bash
OPENMETADATA_ENDPOINT=http://localhost:8585 OPENMETADATA_TOKEN=<jwt> \
  pytest tests/integration/test_openmetadata_e2e.py -m integration
```

## Coding conventions

- **Style** — `ruff` enforces formatting and lint (`E`, `F`, `I`, `W`
  rule sets, 120-character line length, target Python 3.10).
- **Types** — public APIs are typed; new code should follow the same
  convention. `from __future__ import annotations` is used throughout.
- **Logging** — use `r2g.log.get_logger(__name__)` with structured kwargs,
  never `print` for runtime output.
- **CLI** — new commands go in `src/r2g/main.py` using Typer; keep flags
  consistent with existing commands (long form preferred, `--snake-case`).
- **Tests** — every new code path needs a unit test in `tests/`. Integration
  tests live under `tests/integration/` and are auto-marked.
- **Docs** — user-visible changes update `README.md` and, if scope or
  roadmap shifts, `docs/PRD.md`. Add a `CHANGELOG.md` entry under
  *Unreleased*.

## Pull request workflow

1. Fork the repo and create a topic branch (`git checkout -b my-change`).
2. Make focused commits with clear messages. Squash trivial fix-ups before
   review.
3. Run `ruff check src/ tests/` and `pytest tests/ -m "not integration"`
   locally. Both must pass.
4. Open a pull request against `main` using the PR template. Link any
   issues the change addresses.
5. CI runs lint and the unit-test matrix on Python 3.10 / 3.11 / 3.12.
   Fix any failures before requesting review.
6. A maintainer will review. Expect a round or two of feedback; be ready
   to push follow-up commits to the same branch.

## Commit messages

Short, imperative subject (≤ 72 chars), followed by a blank line and a
body explaining *why* if it isn't obvious. Examples:

```
Add --since-column flag for incremental streaming

Allows callers to override the auto-detected timestamp column when
the source table has multiple plausible candidates.
```

## Reporting security issues

Do **not** open a public issue for security problems. See
[SECURITY.md](SECURITY.md).
