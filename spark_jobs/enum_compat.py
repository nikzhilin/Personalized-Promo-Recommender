"""Enum compatibility for the Python 3.10 runtime in the pinned Spark image."""

from __future__ import annotations

try:
    from enum import StrEnum
except ImportError:  # pragma: no cover - executed by the Spark Python 3.10 runtime
    from enum import Enum

    class StrEnum(str, Enum):  # noqa: UP042
        """Python 3.10 equivalent of the subset of StrEnum used by this project."""
