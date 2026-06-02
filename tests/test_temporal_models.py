from __future__ import annotations

import sys

from r2g.temporal.models import (
    NEVER_EXPIRES,
    TemporalConfig,
    TemporalNaming,
    is_current,
)


class TestSentinel:
    def test_never_expires_is_maxsize(self):
        assert NEVER_EXPIRES == sys.maxsize == 9223372036854775807

    def test_is_current_for_sentinel_and_none(self):
        assert is_current(NEVER_EXPIRES) is True
        assert is_current(None) is True

    def test_is_current_false_for_finite(self):
        assert is_current(1000.0) is False
        assert is_current(NEVER_EXPIRES - 1) is False


class TestConfigDefaults:
    def test_defaults(self):
        cfg = TemporalConfig()
        assert cfg.ttl_retain_seconds == 30 * 24 * 60 * 60
        assert cfg.has_version_collection == "hasVersion"
        assert cfg.smart_field is None
        assert cfg.exclude_collections == set()


class TestNamingCollections:
    def test_proxy_names(self):
        assert TemporalNaming.proxy_in("Person") == "PersonProxyIn"
        assert TemporalNaming.proxy_out("Person") == "PersonProxyOut"

    def test_has_version_from_config(self):
        naming = TemporalNaming(TemporalConfig(has_version_collection="versions"))
        assert naming.has_version == "versions"


class TestNamingKeys:
    def test_proxy_and_entity_keys_without_smart_field(self):
        naming = TemporalNaming()
        assert naming.proxy_key("42") == "42"
        assert naming.entity_key("42", 0) == "42-0"
        assert naming.entity_key("42", 3) == "42-3"

    def test_smart_field_prefixes_keys(self):
        naming = TemporalNaming(TemporalConfig(smart_field="tenant"))
        doc = {"tenant": "acme", "_key": "42"}
        assert naming.proxy_key("42", doc) == "acme:42"
        assert naming.entity_key("42", 1, doc) == "acme:42-1"

    def test_smart_field_missing_value_no_prefix(self):
        naming = TemporalNaming(TemporalConfig(smart_field="tenant"))
        assert naming.proxy_key("42", {"_key": "42"}) == "42"
        assert naming.entity_key("42", 0, {"tenant": ""}) == "42-0"
