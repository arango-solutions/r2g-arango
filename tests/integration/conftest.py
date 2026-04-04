"""Integration test fixtures for PG + ArangoDB Docker environments.

Tests in this directory require running PostgreSQL and ArangoDB instances.
They are skipped automatically if the services are unreachable.

Configure via environment variables or .env file:
  PG_CONN            - PostgreSQL connection string
  ARANGO_ENDPOINT    - ArangoDB HTTP endpoint
  ARANGO_PASSWORD    - ArangoDB root password
"""

from __future__ import annotations

import os
import uuid

import pytest

PG_CONN = os.getenv("PG_CONN", "postgresql://arthurkeen@localhost:5432/r2g_test")
ARANGO_ENDPOINT = os.getenv("ARANGO_ENDPOINT", "http://localhost:8540")
ARANGO_USER = os.getenv("ARANGO_USER", "root")
ARANGO_PASSWORD = os.getenv("ARANGO_PASSWORD", "r2g_test_2026")


def _pg_available() -> bool:
    try:
        import psycopg

        with psycopg.connect(PG_CONN) as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False


def _arango_available() -> bool:
    try:
        from arango import ArangoClient

        client = ArangoClient(hosts=ARANGO_ENDPOINT)
        db = client.db("_system", username=ARANGO_USER, password=ARANGO_PASSWORD)
        db.version()
        client.close()
        return True
    except Exception:
        return False


requires_pg = pytest.mark.skipif(not _pg_available(), reason="PostgreSQL not available")
requires_arango = pytest.mark.skipif(not _arango_available(), reason="ArangoDB not available")
requires_both = pytest.mark.skipif(
    not (_pg_available() and _arango_available()),
    reason="PostgreSQL and/or ArangoDB not available",
)


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Auto-mark all tests in this directory as integration tests."""
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)


@pytest.fixture
def arango_test_db():
    """Create a temporary ArangoDB database and drop it after the test."""
    from arango import ArangoClient

    db_name = f"r2g_inttest_{uuid.uuid4().hex[:8]}"
    client = ArangoClient(hosts=ARANGO_ENDPOINT)
    sys_db = client.db("_system", username=ARANGO_USER, password=ARANGO_PASSWORD)
    sys_db.create_database(db_name)
    db = client.db(db_name, username=ARANGO_USER, password=ARANGO_PASSWORD)
    yield db_name, db
    sys_db.delete_database(db_name, ignore_missing=True)
    client.close()


@pytest.fixture
def pg_conn_string():
    return PG_CONN
