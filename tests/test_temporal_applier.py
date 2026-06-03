from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from r2g.connectors.arango_writer import ArangoWriter
from r2g.temporal.applier import TemporalApplier
from r2g.temporal.models import NEVER_EXPIRES, TemporalConfig


@pytest.fixture
def writer():
    w = MagicMock(spec=ArangoWriter)
    w.import_batch.return_value = {"created": 1, "errors": 0}
    w.execute_aql.return_value = []
    # db.collection(...).add_*_index used by ensure_temporal_indexes
    w.db = MagicMock()
    return w


def _imports_by_collection(writer):
    """Map collection -> list of imported docs across all import_batch calls."""
    out: dict[str, list] = {}
    for call in writer.import_batch.call_args_list:
        coll = call.args[0]
        docs = call.args[1]
        out.setdefault(coll, []).extend(docs)
    return out


class TestEnsureCollections:
    def test_creates_entity_proxies_and_hasversion(self, writer):
        applier = TemporalApplier(writer)
        applier.ensure_temporal_collections("Person")
        names = {c.args[0] for c in writer.ensure_collection.call_args_list}
        assert {"Person", "PersonProxyIn", "PersonProxyOut", "hasVersion"} <= names

    def test_ensure_is_idempotent(self, writer):
        applier = TemporalApplier(writer)
        applier.ensure_temporal_collections("Person")
        n = writer.ensure_collection.call_count
        applier.ensure_temporal_collections("Person")
        assert writer.ensure_collection.call_count == n  # cached, no extra calls


class TestTemporalIndexes:
    def _index_payloads(self, writer):
        coll = writer.db.collection.return_value
        return [c.args[0] for c in coll.add_index.call_args_list]

    def test_creates_mdi_interval_and_sparse_ttl(self, writer):
        applier = TemporalApplier(writer)
        applier.ensure_temporal_collections("Person")
        payloads = self._index_payloads(writer)
        interval = [p for p in payloads if p.get("name") == "temporal_interval"]
        ttl = [p for p in payloads if p.get("name") == "temporal_ttl"]
        assert interval and interval[0]["type"] == "mdi"
        assert interval[0]["fields"] == ["created", "expired"]
        assert interval[0]["fieldValueTypes"] == "double"
        assert ttl and ttl[0]["type"] == "ttl"
        assert ttl[0]["sparse"] is True
        assert ttl[0]["fields"] == ["ttlExpireAt"]

    def test_interval_index_falls_back_when_mdi_unsupported(self, writer):
        coll = writer.db.collection.return_value
        # First call (mdi) raises; zkd succeeds.
        coll.add_index.side_effect = [RuntimeError("no mdi"), {"id": "ok"}, {"id": "ttl"}]
        applier = TemporalApplier(writer)
        result = applier._ensure_interval_index("Person")
        assert result == "zkd"


class TestApplyInsert:
    def test_writes_proxies_entity_and_edges(self, writer):
        applier = TemporalApplier(writer)
        applier.apply_insert("Person", {"_key": "42", "name": "Ada"}, now=1000.0)

        imports = _imports_by_collection(writer)
        # proxies
        assert imports["PersonProxyIn"][0]["_key"] == "42"
        assert imports["PersonProxyOut"][0]["_key"] == "42"
        # entity v0 with interval
        ent = imports["Person"][0]
        assert ent["_key"] == "42-0"
        assert ent["_version"] == 0
        assert ent["created"] == 1000.0
        assert ent["expired"] == NEVER_EXPIRES
        assert ent["_proxy"] == "42"
        assert ent["name"] == "Ada"
        # two version edges
        edges = imports["hasVersion"]
        assert len(edges) == 2
        in_edge = next(e for e in edges if e["_key"] == "42-0-in")
        out_edge = next(e for e in edges if e["_key"] == "42-0-out")
        assert in_edge["_from"] == "PersonProxyIn/42"
        assert in_edge["_to"] == "Person/42-0"
        assert out_edge["_from"] == "Person/42-0"
        assert out_edge["_to"] == "PersonProxyOut/42"

    def test_no_key_is_noop(self, writer):
        applier = TemporalApplier(writer)
        applier.apply_insert("Person", {"name": "Ada"})
        assert writer.import_batch.call_count == 0

    def test_proxies_use_ignore_on_duplicate(self, writer):
        applier = TemporalApplier(writer)
        applier.apply_insert("Person", {"_key": "42"}, now=1.0)
        for call in writer.import_batch.call_args_list:
            if call.args[0].endswith("ProxyIn") or call.args[0].endswith("ProxyOut"):
                assert call.kwargs.get("on_duplicate") == "ignore"


class TestSmartField:
    def test_insert_prefixes_keys_and_carries_shard_attr(self, writer):
        applier = TemporalApplier(writer, TemporalConfig(smart_field="tenant"))
        applier.apply_insert("Person", {"_key": "42", "tenant": "acme", "name": "Ada"}, now=1.0)
        imports = _imports_by_collection(writer)
        # proxies + entity use the shard-prefixed keys and proxy carries the attr
        assert imports["PersonProxyIn"][0]["_key"] == "acme:42"
        assert imports["PersonProxyIn"][0]["tenant"] == "acme"
        ent = imports["Person"][0]
        assert ent["_key"] == "acme:42-0"
        assert ent["_proxy"] == "acme:42"
        in_edge = next(e for e in imports["hasVersion"] if e["_key"].endswith("-in"))
        assert in_edge["_from"] == "PersonProxyIn/acme:42"


class TestApplyUpdate:
    def test_expires_current_and_writes_next_version(self, writer):
        # find_current returns version 0; expiry queries return [].
        def aql(query, bind=None):
            if "SORT" in query and "RETURN {" in query:
                return [{"_key": "42-0", "_version": 0}]
            return []

        writer.execute_aql.side_effect = aql
        applier = TemporalApplier(writer, TemporalConfig(ttl_retain_seconds=100))
        applier.apply_update("Person", {"_key": "42", "name": "Ada2"}, now=2000.0)

        # new version v1 written
        imports = _imports_by_collection(writer)
        ent = imports["Person"][0]
        assert ent["_key"] == "42-1"
        assert ent["_version"] == 1
        assert ent["created"] == 2000.0
        assert ent["expired"] == NEVER_EXPIRES

        # an expiry UPDATE ran with expired=now and ttl=now+retain
        update_calls = [
            c for c in writer.execute_aql.call_args_list if "UPDATE" in c.args[0]
        ]
        assert update_calls, "expected an expiry UPDATE query"
        binds = update_calls[0].args[1]
        assert binds["now"] == 2000.0
        assert binds["ttl"] == 2100.0

    def test_update_without_current_inserts_v0(self, writer):
        writer.execute_aql.return_value = []  # no current version
        applier = TemporalApplier(writer)
        applier.apply_update("Person", {"_key": "7", "name": "x"}, now=5.0)
        imports = _imports_by_collection(writer)
        assert imports["Person"][0]["_key"] == "7-0"
        assert imports["PersonProxyIn"][0]["_key"] == "7"


class TestApplyDelete:
    def test_soft_delete_expires_current_only(self, writer):
        def aql(query, bind=None):
            if "SORT" in query and "RETURN {" in query:
                return [{"_key": "42-2", "_version": 2}]
            return []

        writer.execute_aql.side_effect = aql
        applier = TemporalApplier(writer, TemporalConfig(ttl_retain_seconds=10))
        applier.apply_delete("Person", "42", now=3000.0)

        # no new entity version inserted
        imports = _imports_by_collection(writer)
        assert "Person" not in imports
        # expiry update ran against entity key 42-2
        update_calls = [
            c for c in writer.execute_aql.call_args_list if "UPDATE" in c.args[0]
        ]
        assert update_calls
        entity_update = update_calls[0].args[1]
        assert entity_update["key"] == "42-2"
        assert entity_update["now"] == 3000.0
        assert entity_update["ttl"] == 3010.0

    def test_delete_no_current_is_noop(self, writer):
        writer.execute_aql.return_value = []
        applier = TemporalApplier(writer)
        applier.apply_delete("Person", "999", now=1.0)
        # only the find query ran; no UPDATE
        assert all("UPDATE" not in c.args[0] for c in writer.execute_aql.call_args_list)
