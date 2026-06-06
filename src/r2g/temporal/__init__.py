"""Temporal graph mode (Phase 5): immutable-proxy time-travel pattern.

Temporal mode separates *stable identity* (proxy documents) from *mutable
state* (versioned entity documents). Topology edges attach to proxies and are
never rewritten when entities change, so relationships survive entity
versioning. See :mod:`r2g.temporal.models` for the interval/key conventions,
:mod:`r2g.temporal.applier` for the write strategy, and
:mod:`r2g.temporal.queries` for point-in-time AQL templates.
"""

from __future__ import annotations

from r2g.temporal.models import (
    NEVER_EXPIRES,
    TemporalConfig,
    TemporalNaming,
    is_current,
)

__all__ = [
    "NEVER_EXPIRES",
    "TemporalConfig",
    "TemporalNaming",
    "is_current",
]
