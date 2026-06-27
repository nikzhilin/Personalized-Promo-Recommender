"""UTC compatibility for the Python 3.10 runtime in the pinned Spark image."""

from __future__ import annotations

try:
    from datetime import UTC
except ImportError:  # pragma: no cover - executed by the Spark Python 3.10 runtime
    from datetime import timezone

    UTC = timezone.utc  # noqa: UP017
