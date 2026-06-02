from __future__ import annotations

from r2g.temporal import queries


class TestSnapshotAt:
    def test_uses_half_open_interval(self):
        q = queries.snapshot_at("Person")
        assert "FOR e IN Person" in q
        assert "e.created <= @t" in q
        assert "e.expired > @t" in q


class TestIntervalOverlap:
    def test_overlap_predicate(self):
        q = queries.interval_overlap("Person")
        assert "e.created <= @end" in q
        assert "e.expired >= @start" in q


class TestChangedBetween:
    def test_created_and_expired_windows(self):
        q = queries.changed_between("Person")
        assert "@t1" in q and "@t2" in q
        assert "created_in_window" in q
        assert "expired_in_window" in q


class TestCurrentVersion:
    def test_filters_on_sentinel(self):
        q = queries.current_version("Person")
        assert "e.expired >= @never" in q


class TestVersionHistory:
    def test_traverses_proxy_in_and_sorts_desc(self):
        q = queries.version_history("Person", "42")
        assert "PersonProxyIn/42" in q
        assert "OUTBOUND" in q
        assert "hasVersion" in q
        assert "SORT v._version DESC" in q


class TestAllTemplates:
    def test_returns_named_set(self):
        t = queries.all_templates("Person")
        assert set(t.keys()) == {
            "snapshot_at", "interval_overlap", "changed_between", "current_version"
        }
        assert all(isinstance(v, str) and "Person" in v for v in t.values())
