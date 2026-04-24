from __future__ import annotations

import pytest

from r2g.connectors.base import (
    SUPPORTED_SOURCE_TYPES,
    SourceConnector,
    create_source_connector,
)
from r2g.connectors.postgres import PostgresConnector


class TestSupportedTypes:
    def test_tuple_includes_postgresql_and_snowflake(self):
        assert "postgresql" in SUPPORTED_SOURCE_TYPES
        assert "snowflake" in SUPPORTED_SOURCE_TYPES


class TestFactory:
    def test_builds_postgres_connector_from_postgresql(self):
        conn = create_source_connector("postgresql", "postgresql://u:p@h/db")
        assert isinstance(conn, PostgresConnector)

    def test_accepts_short_aliases(self):
        for alias in ("postgres", "pg", "POSTGRESQL", "  PG  "):
            conn = create_source_connector(alias, "postgresql://u:p@h/db")
            assert isinstance(conn, PostgresConnector)

    def test_builds_snowflake_connector_from_snowflake(self):
        from r2g.connectors.snowflake import SnowflakeConnector

        conn = create_source_connector(
            "snowflake", "snowflake://u:p@xy12345/ANALYTICS/CORE"
        )
        assert isinstance(conn, SnowflakeConnector)

    def test_unknown_source_type_raises_value_error(self):
        with pytest.raises(ValueError, match="Unsupported source type"):
            create_source_connector("mysql", "mysql://u:p@h/db")


class TestProtocolConformance:
    def test_postgres_connector_satisfies_protocol(self):
        conn = PostgresConnector("postgresql://u:p@h/db")
        assert isinstance(conn, SourceConnector)

    def test_snowflake_connector_satisfies_protocol(self):
        from r2g.connectors.snowflake import SnowflakeConnector

        conn = SnowflakeConnector("snowflake://u:p@xy12345/ANALYTICS/CORE")
        assert isinstance(conn, SourceConnector)
