# Development Notes

## Setup

The project requires Python 3.11+ and Rust. It is built with `hatchling` and `hatch-rs`.
Development dependencies live in `pyproject.toml` under the `develop` extra.

Prefer `uv` for Python commands in this repo. `uv run` creates or reuses the local
environment and makes validator dependencies such as `yaml` available without relying on
the system Python.

Common workflow:

```bash
uv run python /Users/ec666/.codex/skills/.system/skill-creator/scripts/quick_validate.py plugins/polars-io-tools/skills/polars-io-tools
make develop
make build
make test-py
make lint-py
make lint-rs
```

Run `make` with no arguments to list available targets.

## Skill Install and Validation

This plugin follows the rshioaji plugin layout:

```text
./.agents/plugins/marketplace.json
plugins/polars-io-tools/
├── .codex-plugin/plugin.json
├── .claude-plugin/plugin.json
├── .cursor-plugin/plugin.json
└── skills/polars-io-tools/
```

Install it into Codex from the repo-local marketplace:

```bash
codex plugin marketplace add /Users/ec666/yvictor/polars-io-tools
codex plugin add polars-io-tools@polars-io-tools
```

After editing the skill, validate it with `uv`:

```bash
uv run python /Users/ec666/.codex/skills/.system/skill-creator/scripts/quick_validate.py plugins/polars-io-tools/skills/polars-io-tools
```

If `uv run` creates or updates `uv.lock` only because of validation, treat it as a
temporary local artifact unless the user explicitly wants dependency metadata changed.

## Build and Test Targets

- `make build`: build the Python package and Rust extension.
- `make test-py`: run Python tests under `polars_io_tools/tests`.
- `make test-rs`: run Rust tests.
- `make lint-py` / `make fix-py`: run or apply Python `ruff` checks.
- `make lint-rs` / `make fix-rs`: run or apply Rust `clippy`/formatting checks.
- `make lint-docs` / `make fix-docs`: run or apply docs formatting and spelling checks.

Integration tests under `polars_io_tools/tests/integration` can require live services
or credentials. Do not assume they are available in a generic local session.

## Contribution Rules

Keep changes scoped and update tests near the behavior being changed. Public docs live
under `docs/wiki`; update them when changing public API semantics or examples.

The upstream project expects signed commits (`git commit -s`) and logical squash-ready
changes. Draft pull requests are appropriate for work in progress.

## Packaging Details

`pyproject.toml` defines package metadata, supported Python versions, optional develop
dependencies, cibuildwheel settings, pytest config, coverage config, and ruff/isort
settings. Version bumps must update:

- `polars_io_tools/__init__.py`
- `pyproject.toml`
- `Cargo.toml`
- `rust/Cargo.toml`
