"""End-to-end validation for temporal graph mode (PRD Phase 5).

Exercises the immutable-proxy time-travel pattern against a **live ArangoDB**:
INSERT → UPDATE → DELETE through :class:`TemporalApplier`, then runs the
point-in-time / version-history / interval AQL templates and asserts correct
results. This is the "field validation against a live temporal workload" the PRD
flagged as pending; it is skipped automatically when ArangoDB is unreachable.
"""

from __future__ import annotations

from r2g.connectors.arango_writer import ArangoWriter
from r2g.temporal import queries
from r2g.temporal.applier import TemporalApplier
from r2g.temporal.models import (
    FIELD_CREATED,
    FIELD_EXPIRED,
    FIELD_TTL,
    FIELD_VERSION,
    NEVER_EXPIRES,
    TemporalConfig,
)

from .conftest import (
    ARANGO_ENDPOINT,
    ARANGO_PASSWORD,
    ARANGO_USER,
    requires_arango,
)

ENTITY = "Person"
# Deterministic timestamps well apart so point-in-time windows are unambiguous.
T_INSERT = 1_000_000.0
T_BETWEEN_01 = 1_500_000.0
T_UPDATE = 2_000_000.0
T_BETWEEN_12 = 2_500_000.0
T_DELETE = 3_000_000.0
T_NOW = 4_000_000.0
TTL_RETAIN = 60


@requires_arango
class TestTemporalEndToEnd:
    def _writer(self, db_name: str) -> ArangoWriter:
        return ArangoWriter(
            endpoint=ARANGO_ENDPOINT,
            database=db_name,
            username=ARANGO_USER,
            password=ARANGO_PASSWORD,
        )

    def test_full_versioned_lifecycle(self, arango_test_db):
        db_name, db = arango_test_db
        writer = self._writer(db_name)
        applier = TemporalApplier(writer, TemporalConfig(ttl_retain_seconds=TTL_RETAIN))

        # ── INSERT (P5.1): proxies + entity v0 + two hasVersion edges ──────
        applier.apply_insert(ENTITY, {"_key": "1", "name": "Ada", "city": "NYC"}, now=T_INSERT)

        assert db.has_collection(ENTITY)
        assert db.has_collection("PersonProxyIn")
        assert db.has_collection("PersonProxyOut")
        assert db.has_collection("hasVersion")

        assert db.collection("PersonProxyIn").get("1") is not None
        assert db.collection("PersonProxyOut").get("1") is not None

        v0 = db.collection(ENTITY).get("1-0")
        assert v0 is not None
        assert v0[FIELD_VERSION] == 0
        assert v0[FIELD_CREATED] == T_INSERT
        assert v0[FIELD_EXPIRED] == NEVER_EXPIRES
        assert v0["name"] == "Ada"

        # Both version edges exist and are live.
        assert db.collection("hasVersion").get("1-0-in") is not None
        assert db.collection("hasVersion").get("1-0-out") is not None

        current = writer.execute_aql(queries.current_version(ENTITY), {"never": NEVER_EXPIRES})
        assert [e["_key"] for e in current] == ["1-0"]

        # ── UPDATE (P5.1): expire v0, insert v1 ────────────────────────────
        applier.apply_update(
            ENTITY, {"_key": "1", "name": "Ada Lovelace", "city": "London"}, now=T_UPDATE
        )

        v0 = db.collection(ENTITY).get("1-0")
        assert v0[FIELD_EXPIRED] == T_UPDATE
        assert v0[FIELD_TTL] == T_UPDATE + TTL_RETAIN  # P5.5: ttlExpireAt stamped
        v1 = db.collection(ENTITY).get("1-1")
        assert v1[FIELD_VERSION] == 1
        assert v1[FIELD_CREATED] == T_UPDATE
        assert v1[FIELD_EXPIRED] == NEVER_EXPIRES
        assert v1["name"] == "Ada Lovelace"

        # The v0 version edges were closed too.
        assert db.collection("hasVersion").get("1-0-in")[FIELD_EXPIRED] == T_UPDATE
        assert db.collection("hasVersion").get("1-0-out")[FIELD_EXPIRED] == T_UPDATE

        current = writer.execute_aql(queries.current_version(ENTITY), {"never": NEVER_EXPIRES})
        assert [e["_key"] for e in current] == ["1-1"]

        # ── Point-in-time snapshots (P5.7) ─────────────────────────────────
        snap_before = writer.execute_aql(queries.snapshot_at(ENTITY), {"t": T_BETWEEN_01})
        assert [e["_key"] for e in snap_before] == ["1-0"]
        assert snap_before[0]["city"] == "NYC"

        snap_after = writer.execute_aql(queries.snapshot_at(ENTITY), {"t": T_BETWEEN_12})
        assert [e["_key"] for e in snap_after] == ["1-1"]
        assert snap_after[0]["city"] == "London"

        # ── Version history via ProxyIn traversal (P5.7) ───────────────────
        history = writer.execute_aql(queries.version_history(ENTITY, "1"), {})
        assert [e["_key"] for e in history] == ["1-1", "1-0"]  # newest first

        # ── Interval overlap (P5.7) ────────────────────────────────────────
        overlap = writer.execute_aql(
            queries.interval_overlap(ENTITY), {"start": T_INSERT, "end": T_BETWEEN_01}
        )
        assert {e["_key"] for e in overlap} == {"1-0"}

        # ── DELETE (P5.1): soft-delete the live version ────────────────────
        applier.apply_delete(ENTITY, "1", now=T_DELETE)

        assert writer.execute_aql(queries.current_version(ENTITY), {"never": NEVER_EXPIRES}) == []
        assert writer.execute_aql(queries.snapshot_at(ENTITY), {"t": T_NOW}) == []
        # History is preserved: the entity is still queryable at a past instant.
        past = writer.execute_aql(queries.snapshot_at(ENTITY), {"t": T_BETWEEN_12})
        assert [e["_key"] for e in past] == ["1-1"]
        assert len(writer.execute_aql(queries.version_history(ENTITY, "1"), {})) == 2

    def test_indexes_created(self, arango_test_db):
        db_name, db = arango_test_db
        writer = self._writer(db_name)
        applier = TemporalApplier(writer, TemporalConfig(ttl_retain_seconds=TTL_RETAIN))
        applier.apply_insert(ENTITY, {"_key": "1", "name": "Ada"}, now=T_INSERT)

        by_name = {ix.get("name"): ix for ix in db.collection(ENTITY).indexes()}
        assert "temporal_interval" in by_name
        assert by_name["temporal_interval"]["type"] in ("mdi", "zkd", "persistent")
        assert by_name["temporal_interval"]["fields"] == [FIELD_CREATED, FIELD_EXPIRED]
        assert "temporal_ttl" in by_name
        assert by_name["temporal_ttl"]["type"] == "ttl"
        assert by_name["temporal_ttl"]["sparse"] is True

    def test_replayed_insert_does_not_create_phantom(self, arango_test_db):
        # last_write_wins / at-least-once: a duplicated INSERT must be a no-op.
        db_name, db = arango_test_db
        writer = self._writer(db_name)
        applier = TemporalApplier(writer, TemporalConfig(ttl_retain_seconds=TTL_RETAIN))
        doc = {"_key": "7", "name": "Grace"}
        applier.apply_insert(ENTITY, doc, now=T_INSERT)
        applier.apply_insert(ENTITY, doc, now=T_UPDATE)  # replay

        current = writer.execute_aql(queries.current_version(ENTITY), {"never": NEVER_EXPIRES})
        assert [e["_key"] for e in current] == ["7-0"]
        # The replay did not overwrite the original interval.
        assert db.collection(ENTITY).get("7-0")[FIELD_CREATED] == T_INSERT
