"""Pytest configuration for lakeflow-community-connectors.

Many tests in this repo import top-level modules like `libs.*` and `tests.*`.
When running `pytest` locally, the repo root isn't always automatically on
`sys.path`, which causes import errors during collection.

This conftest ensures the repository root is importable regardless of how
pytest is invoked (CLI/IDE).
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
