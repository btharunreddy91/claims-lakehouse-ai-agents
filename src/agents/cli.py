"""Day 6 — Analytics Agent CLI entry point.

Thin wrapper over :mod:`src.agents.analytics` so both of these work:

    python -m src.agents.analytics "total claim amount by state"
    python -m src.agents.cli "total claim amount by state"
"""
from __future__ import annotations

import sys

from src.agents.analytics import main

if __name__ == "__main__":
    sys.exit(main())
