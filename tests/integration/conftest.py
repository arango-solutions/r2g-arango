"""Integration test fixtures for PG / MySQL + ArangoDB Docker environments.

Tests in this directory require running database + ArangoDB instances.
They are skipped automatically if the services are unreachable.

Configure via environment variables or .env file:
  PG_CONN            - PostgreSQL connection string
  MYSQL_CONN         - MySQL / MariaDB connection string
  ARANGO_ENDPOINT    - ArangoDB HTTP endpoint
  ARANGO_PASSWORD    - ArangoDB root password
"""

from __future__ import annotations

import os
import uuid

import pytest

PG_CONN = os.getenv("PG_CONN", "postgresql://postgres@localhost:5432/r2g_test")
MYSQL_CONN = os.getenv("MYSQL_CONN", "mysql://r2g:r2g_test_2026@localhost:3306/shop")
MSSQL_CONN = os.getenv("MSSQL_CONN", "mssql://sa:r2g_Test_2026!@localhost:1433/shop")
OPENMETADATA_ENDPOINT = os.getenv("OPENMETADATA_ENDPOINT", "http://localhost:8585")
OPENMETADATA_TOKEN = os.getenv("OPENMETADATA_TOKEN", "")
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


def _mysql_available() -> bool:
    try:
        from r2g.connectors.mysql import _load_pymysql, _parse_mysql_url

        pymysql = _load_pymysql()
        conn = pymysql.connect(**_parse_mysql_url(MYSQL_CONN))
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        finally:
            conn.close()
        return True
    except Exception:
        return False


def _mssql_available() -> bool:
    """Reachable if the *server* answers (checked against the master db, since
    the target database may not exist until the seed fixture runs)."""
    try:
        from r2g.connectors.mssql import _load_pymssql, _parse_mssql_url

        pymssql = _load_pymssql()
        params = {**_parse_mssql_url(MSSQL_CONN), "database": "master"}
        conn = pymssql.connect(**params)
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
        finally:
            conn.close()
        return True
    except Exception:
        return False


def _openmetadata_available() -> bool:
    """Reachable if the OpenMetadata server answers its version endpoint.

    Set OPENMETADATA_ENDPOINT (and OPENMETADATA_TOKEN for secured installs).
    Note: OpenMetadata's full stack is heavy; this is typically run against a
    separately-started OM instance, not r2g's docker-compose.
    """
    try:
        import httpx

        headers = {"Authorization": f"Bearer {OPENMETADATA_TOKEN}"} if OPENMETADATA_TOKEN else {}
        url = OPENMETADATA_ENDPOINT.rstrip("/") + "/api/v1/system/version"
        resp = httpx.get(url, headers=headers, timeout=5.0)
        return resp.status_code == 200
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
requires_mysql_arango = pytest.mark.skipif(
    not (_mysql_available() and _arango_available()),
    reason="MySQL and/or ArangoDB not available",
)
requires_mssql_arango = pytest.mark.skipif(
    not (_mssql_available() and _arango_available()),
    reason="SQL Server and/or ArangoDB not available",
)
requires_openmetadata = pytest.mark.skipif(
    not _openmetadata_available(),
    reason="OpenMetadata not available (set OPENMETADATA_ENDPOINT)",
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


@pytest.fixture
def mysql_conn_string():
    return MYSQL_CONN


@pytest.fixture
def sqlserver_conn_string():
    """Ensure the target database + demo schema exist, then yield the DSN.

    The SQL Server image has no init-script hook, so we create the database
    (via ``master``) and seed ``docker/mssql_demo/schema.sql`` ourselves. The
    seed file drops and recreates its tables, so row counts are deterministic.
    """
    import pathlib

    from r2g.connectors.mssql import _load_pymssql, _parse_mssql_url

    pymssql = _load_pymssql()
    params = _parse_mssql_url(MSSQL_CONN)
    db = params["database"]

    master = pymssql.connect(**{**params, "database": "master", "autocommit": True})
    try:
        cur = master.cursor()
        cur.execute(f"IF DB_ID('{db}') IS NULL CREATE DATABASE [{db}]")
        cur.close()
    finally:
        master.close()

    schema_sql = (
        pathlib.Path(__file__).resolve().parents[2]
        / "docker" / "mssql_demo" / "schema.sql"
    ).read_text(encoding="utf-8")
    conn = pymssql.connect(**{**params, "autocommit": True})
    try:
        cur = conn.cursor()
        cur.execute(schema_sql)
        cur.close()
    finally:
        conn.close()

    return MSSQL_CONN
