# Contributing to edgeguard

edgeguard targets production edge devices, so correctness, crash-safety, and
deterministic behavior take priority over new features. Please read this
before opening a PR.

## Development setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pre-commit install
```

## Workflow

```bash
make test      # pytest
make lint      # ruff check
make format    # ruff format
make typecheck # mypy --strict
make check     # lint + typecheck + test
```

All four must pass before a PR is merged; CI enforces the same checks.

## Project structure

The codebase is organized by subsystem (`core`, `resilience`, `persistence`,
`network`, `process`, `storage`, `diagnostics`, ...), each with a small,
explicit interface. Before adding a module, check whether it belongs in an
existing subsystem rather than creating a new one.

The project is being built in phases (see `CHANGELOG.md`). Please don't jump
ahead to a later phase's features in a PR targeting an earlier one -- it
makes review (and the "is this actually tested against real failure modes"
question) much harder.

## Code style

- Python 3.11+, full type annotations, `from __future__ import annotations`.
- Format and lint with Ruff (`make format`, `make lint`); no manual style
  debates -- if Ruff doesn't flag it, it's fine.
- `mypy --strict` must pass with no new `# type: ignore`s unless justified
  in a comment.
- Public API surface is deliberately small (`edgeguard/__init__.py`).
  Internal modules (`edgeguard.core.*`, etc.) are not part of the stable
  API; if you need something from them at the top level, propose adding it
  to `__all__` rather than reaching into internals.

## Testing philosophy

This is a reliability library -- happy-path tests are necessary but not
sufficient. When adding a feature, also ask "what failure does this
component need to survive?" and write a test for that failure, not just for
successful execution. See `tests/` for examples (state-machine race
conditions, transaction rollback, concurrent SQLite writes).

Tests must be deterministic. Do not use real network access, real sleeps for
timing, or real hardware in the default test suite -- inject fakes/clocks
where you need to simulate time or failures.

## Commit messages

Clear, imperative summaries (`Add circuit breaker half-open timeout`, not
`fixes`). Reference the relevant issue/phase when useful.

## Reporting bugs / requesting features

Open a GitHub issue. For security vulnerabilities, see `SECURITY.md` instead
of a public issue.
