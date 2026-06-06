"""Conventions and configuration for temporal graph mode (Phase 5).

The immutable-proxy time-travel pattern stores three kinds of records per
mapped collection ``C``:

- ``{C}ProxyIn`` / ``{C}ProxyOut`` -- stable proxy documents that hold only the
  shard key and a stable ``_key``. Topology edges attach here (P5.2).
- ``C`` -- versioned entity documents. Each version carries an interval
  ``[created, expired)`` (P5.4). The current version has ``expired ==
  NEVER_EXPIRES``; historical versions have a finite ``expired``.
- ``hasVersion`` -- an edge collection linking ``ProxyIn -> Entity`` (inbound)
  and ``Entity -> ProxyOut`` (outbound), each edge mirroring the entity's
  interval (P5.3).

Keys
----
- Proxy ``_key`` == the base document key derived from the source primary key.
- Entity ``_key`` == ``{proxyKey}-{version}`` (version is a 0-based integer).
- With a smart field set (P5.8), keys are prefixed ``{shard}:{proxyKey}`` and
  ``{shard}:{proxyKey}-{version}`` for SmartGraph shard-key compatibility.
"""

from __future__ import annotations

import sys
import time

from pydantic import BaseModel, Field

# Sentinel "expired" value marking a currently-live version (P5.4).
NEVER_EXPIRES: int = sys.maxsize  # 9223372036854775807

# Field names carried on temporal documents/edges.
FIELD_CREATED = "created"
FIELD_EXPIRED = "expired"
FIELD_VERSION = "_version"
FIELD_PROXY = "_proxy"
FIELD_TTL = "ttlExpireAt"


def now_ts() -> float:
    """Current wall-clock time as a unix timestamp (float seconds)."""
    return time.time()


def is_current(expired: float | int | None) -> bool:
    """True when an ``expired`` value denotes a live (non-historical) version."""
    if expired is None:
        return True
    return int(expired) >= NEVER_EXPIRES


class TemporalConfig(BaseModel):
    """Tunables for temporal write strategy.

    Attributes:
        ttl_retain_seconds: How long historical versions are retained before
            TTL garbage collection (P5.5). Default 30 days.
        has_version_collection: Name of the shared version edge collection.
        smart_field: Optional shard-key attribute for SmartGraph isolation
            (P5.8). When set, the value is read from each entity document and
            prefixed onto proxy/entity keys.
        exclude_collections: Collections treated as static reference data;
            excluded from TTL aging and (optionally) versioning.
    """

    ttl_retain_seconds: int = Field(default=30 * 24 * 60 * 60, ge=0)
    has_version_collection: str = "hasVersion"
    smart_field: str | None = None
    exclude_collections: set[str] = Field(default_factory=set)


class TemporalNaming:
    """Derives temporal collection names and document keys for a config."""

    def __init__(self, config: TemporalConfig | None = None) -> None:
        self.config = config or TemporalConfig()

    # ── Collection names ──────────────────────────────────────────────
    @staticmethod
    def proxy_in(entity_collection: str) -> str:
        return f"{entity_collection}ProxyIn"

    @staticmethod
    def proxy_out(entity_collection: str) -> str:
        return f"{entity_collection}ProxyOut"

    @property
    def has_version(self) -> str:
        return self.config.has_version_collection

    # ── Keys ──────────────────────────────────────────────────────────
    def _shard_prefix(self, document: dict | None) -> str:
        field = self.config.smart_field
        if not field or not document:
            return ""
        value = document.get(field)
        if value is None or value == "":
            return ""
        return f"{value}:"

    def proxy_key(self, base_key: str, document: dict | None = None) -> str:
        """Stable proxy key for a base document key (optionally smart-prefixed)."""
        return f"{self._shard_prefix(document)}{base_key}"

    def entity_key(self, base_key: str, version: int, document: dict | None = None) -> str:
        """Versioned entity key: ``{shard:}{base_key}-{version}``."""
        return f"{self._shard_prefix(document)}{base_key}-{version}"
