# Contributing to polyopen

Thank you for contributing to `polyopen`.

## Local development setup

1. Create and activate a Python 3.8+ virtual environment.
2. Install the package in editable mode together with `pytest`:

```bash
pip install -e . pytest
```

## Running tests

Run the full test suite before opening a pull request:

```bash
python -m pytest -v
```

Tests should verify observable behavior (outputs, exceptions, and side effects) rather than internal source structure.

## Submitting changes

1. Create a focused branch from `main`.
2. Add or update unit and regression tests for any behavior change or bug fix.
3. Update relevant documentation (`README.md`, `docs/API.md`, `docs/CLI.md`, `FUNCTIONAL_SPEC.md`, or `ARCHITECTURE.md`) when public behavior changes.
4. Keep commits scoped and free of secrets or machine-specific paths.
5. Open a pull request describing what changed and how you verified it.

## License

Contributions to `polyopen` are licensed under the Apache License, Version 2.0 (see `LICENSE` and `NOTICE`).
