"""Temporal write strategy (P5.1-P5.5): immutable-proxy versioned writes.

The :class:`TemporalApplier` translates logical INSERT / UPDATE / DELETE
operations on an entity collection into the immutable-proxy time-travel
pattern, applied through an :class:`~r2g.connectors.arango_writer.ArangoWriter`:

- **INSERT** creates ``ProxyIn`` + ``ProxyOut`` documents and entity version 0
  (``created=now``, ``expired=NEVER_EXPIRES``) plus the two ``hasVersion``
  edges.
- **UPDATE** expires the current entity version and its version edges
  (``expired=now``, ``ttlExpireAt=now+retain``) and inserts a fresh version.
- **DELETE** soft-deletes by expiring the current version and its edges;
  proxies and topology edges are preserved (queryable at any past time).

All inserts are replay-safe (``on_duplicate='ignore'`` with deterministic
keys), so a replayed INSERT does not create a phantom version.
"""

from __future__ import annotations

from typing import Any

from r2g.connectors.arango_writer import ArangoWriter
from r2g.log import get_logger
from r2g.temporal.models import (
    FIELD_CREATED,
    FIELD_EXPIRED,
    FIELD_PROXY,
    FIELD_TTL,
    FIELD_VERSION,
    NEVER_EXPIRES,
    TemporalConfig,
    TemporalNaming,
    now_ts,
)

logger = get_logger(__name__)


class TemporalApplier:
    """Applies versioned (temporal) writes for one logical operation at a time."""

    def __init__(
        self,
        writer: ArangoWriter,
        config: TemporalConfig | None = None,
    ) -> None:
        self.writer = writer
        self.config = config or TemporalConfig()
        self.naming = TemporalNaming(self.config)
        self._ensured: set[str] = set()

    # ── Collection / index management (P5.2, P5.3, P5.5, P5.6) ─────────

    def ensure_temporal_collections(self, entity_collection: str) -> None:
        """Create the entity, proxy, and ``hasVersion`` collections (idempotent)."""
        if entity_collection in self._ensured:
            return
        self.writer.ensure_collection(entity_collection, edge=False)
        self.writer.ensure_collection(self.naming.proxy_in(entity_collection), edge=False)
        self.writer.ensure_collection(self.naming.proxy_out(entity_collection), edge=False)
        self.writer.ensure_collection(self.naming.has_version, edge=True)
        self.ensure_temporal_indexes(entity_collection)
        self._ensured.add(entity_collection)

    def ensure_temporal_indexes(self, entity_collection: str) -> None:
        """Add interval + TTL indexes on the entity and version collections.

        TTL index (P5.5) is sparse so only expired documents (which carry
        ``ttlExpireAt``) are aged out. A persistent index on
        ``[created, expired]`` (P5.6) accelerates point-in-time queries.
        Failures are logged and swallowed so a missing index never blocks a
        load (e.g. against older ArangoDB builds).
        """
        for name in (entity_collection, self.naming.has_version):
            try:
                coll = self.writer.db.collection(name)
                coll.add_persistent_index(
                    fields=[FIELD_CREATED, FIELD_EXPIRED], sparse=False
                )
                coll.add_ttl_index(fields=[FIELD_TTL], expiry_time=0)
            except Exception as exc:  # noqa: BLE001
                logger.warning("temporal_index_failed", collection=name, error=str(exc))

    # ── Public operations (P5.1) ──────────────────────────────────────

    def apply_insert(
        self,
        entity_collection: str,
        document: dict[str, Any],
        now: float | None = None,
    ) -> None:
        now = now if now is not None else now_ts()
        self.ensure_temporal_collections(entity_collection)
        base_key = self._base_key(document)
        if base_key is None:
            logger.warning("temporal_insert_no_key", collection=entity_collection)
            return
        self._write_proxies(entity_collection, base_key, document)
        self._write_version(entity_collection, base_key, document, version=0, now=now)

    def apply_update(
        self,
        entity_collection: str,
        document: dict[str, Any],
        now: float | None = None,
    ) -> None:
        now = now if now is not None else now_ts()
        self.ensure_temporal_collections(entity_collection)
        base_key = self._base_key(document)
        if base_key is None:
            logger.warning("temporal_update_no_key", collection=entity_collection)
            return
        proxy_key = self.naming.proxy_key(base_key, document)
        current = self._find_current_version(entity_collection, proxy_key)
        if current is None:
            # Nothing live to supersede -- treat as a first insert.
            self._write_proxies(entity_collection, base_key, document)
            self._write_version(entity_collection, base_key, document, version=0, now=now)
            return
        self._expire_version(entity_collection, current["_key"], now)
        self._write_version(
            entity_collection,
            base_key,
            document,
            version=int(current.get(FIELD_VERSION, 0)) + 1,
            now=now,
        )

    def apply_delete(
        self,
        entity_collection: str,
        base_key: str,
        now: float | None = None,
        document: dict[str, Any] | None = None,
    ) -> None:
        now = now if now is not None else now_ts()
        self.ensure_temporal_collections(entity_collection)
        proxy_key = self.naming.proxy_key(base_key, document)
        current = self._find_current_version(entity_collection, proxy_key)
        if current is None:
            logger.debug("temporal_delete_no_current", collection=entity_collection, key=base_key)
            return
        self._expire_version(entity_collection, current["_key"], now)

    # ── Internals ─────────────────────────────────────────────────────

    @staticmethod
    def _base_key(document: dict[str, Any]) -> str | None:
        key = document.get("_key")
        return str(key) if key is not None and key != "" else None

    def _put(self, collection: str, docs: list[dict[str, Any]], on_duplicate: str) -> None:
        self.writer.import_batch(collection, docs, on_duplicate=on_duplicate)

    def _write_proxies(
        self, entity_collection: str, base_key: str, document: dict[str, Any]
    ) -> None:
        proxy_key = self.naming.proxy_key(base_key, document)
        proxy_doc: dict[str, Any] = {"_key": proxy_key}
        if self.config.smart_field and self.config.smart_field in document:
            proxy_doc[self.config.smart_field] = document[self.config.smart_field]
        # Proxies are stable; never overwrite an existing one (replay-safe).
        self._put(self.naming.proxy_in(entity_collection), [proxy_doc], "ignore")
        self._put(self.naming.proxy_out(entity_collection), [proxy_doc], "ignore")

    def _write_version(
        self,
        entity_collection: str,
        base_key: str,
        document: dict[str, Any],
        version: int,
        now: float,
    ) -> None:
        proxy_key = self.naming.proxy_key(base_key, document)
        entity_key = self.naming.entity_key(base_key, version, document)
        entity = {
            **document,
            "_key": entity_key,
            FIELD_PROXY: proxy_key,
            FIELD_VERSION: version,
            FIELD_CREATED: now,
            FIELD_EXPIRED: NEVER_EXPIRES,
        }
        self._put(entity_collection, [entity], "ignore")

        entity_id = f"{entity_collection}/{entity_key}"
        in_edge = {
            "_key": f"{entity_key}-in",
            "_from": f"{self.naming.proxy_in(entity_collection)}/{proxy_key}",
            "_to": entity_id,
            FIELD_CREATED: now,
            FIELD_EXPIRED: NEVER_EXPIRES,
        }
        out_edge = {
            "_key": f"{entity_key}-out",
            "_from": entity_id,
            "_to": f"{self.naming.proxy_out(entity_collection)}/{proxy_key}",
            FIELD_CREATED: now,
            FIELD_EXPIRED: NEVER_EXPIRES,
        }
        self._put(self.naming.has_version, [in_edge, out_edge], "ignore")

    def _find_current_version(
        self, entity_collection: str, proxy_key: str
    ) -> dict[str, Any] | None:
        query = (
            "FOR e IN @@coll "
            f"FILTER e.{FIELD_PROXY} == @pk AND e.{FIELD_EXPIRED} >= @never "
            f"SORT e.{FIELD_VERSION} DESC LIMIT 1 "
            f"RETURN {{ _key: e._key, {FIELD_VERSION}: e.{FIELD_VERSION} }}"
        )
        rows = self.writer.execute_aql(
            query,
            {"@coll": entity_collection, "pk": proxy_key, "never": NEVER_EXPIRES},
        )
        return rows[0] if rows else None

    def _expire_version(self, entity_collection: str, entity_key: str, now: float) -> None:
        """Close the interval on an entity version and its two version edges."""
        ttl_at = now + self.config.ttl_retain_seconds
        entity_query = (
            "FOR e IN @@coll FILTER e._key == @key "
            f"UPDATE e WITH {{ {FIELD_EXPIRED}: @now, {FIELD_TTL}: @ttl }} IN @@coll"
        )
        self.writer.execute_aql(
            entity_query,
            {"@coll": entity_collection, "key": entity_key, "now": now, "ttl": ttl_at},
        )
        entity_id = f"{entity_collection}/{entity_key}"
        edge_query = (
            "FOR ed IN @@hv "
            f"FILTER (ed._to == @eid OR ed._from == @eid) AND ed.{FIELD_EXPIRED} >= @never "
            f"UPDATE ed WITH {{ {FIELD_EXPIRED}: @now, {FIELD_TTL}: @ttl }} IN @@hv"
        )
        self.writer.execute_aql(
            edge_query,
            {
                "@hv": self.naming.has_version,
                "eid": entity_id,
                "never": NEVER_EXPIRES,
                "now": now,
                "ttl": ttl_at,
            },
        )
