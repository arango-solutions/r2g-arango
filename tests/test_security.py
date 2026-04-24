from __future__ import annotations

import json
import os
import stat

import pytest
from cryptography.fernet import Fernet

from r2g.catalog import CatalogManager
from r2g.security import (
    ENC_PREFIX,
    SECRET_ENV,
    SECRET_FILENAME,
    CredentialCipher,
    SecretKeyError,
    load_secret_key,
    redact_connection_string,
    redact_for_display,
)


@pytest.fixture
def tmp_catalog_dir(tmp_path, monkeypatch):
    monkeypatch.delenv(SECRET_ENV, raising=False)
    d = tmp_path / "cat"
    d.mkdir()
    return d


class TestCredentialCipher:
    def test_roundtrip_preserves_plaintext(self, tmp_catalog_dir):
        key = load_secret_key(tmp_catalog_dir)
        cipher = CredentialCipher(key)
        token = cipher.encrypt("super-secret-dsn")
        assert token.startswith(ENC_PREFIX)
        assert cipher.decrypt(token) == "super-secret-dsn"

    def test_encrypt_empty_stays_empty(self, tmp_catalog_dir):
        cipher = CredentialCipher(load_secret_key(tmp_catalog_dir))
        assert cipher.encrypt("") == ""
        assert cipher.decrypt("") == ""

    def test_idempotent_encrypt(self, tmp_catalog_dir):
        cipher = CredentialCipher(load_secret_key(tmp_catalog_dir))
        token = cipher.encrypt("x")
        assert cipher.encrypt(token) == token

    def test_plaintext_is_passed_through_on_decrypt(self, tmp_catalog_dir):
        cipher = CredentialCipher(load_secret_key(tmp_catalog_dir))
        assert cipher.decrypt("plaintext-legacy") == "plaintext-legacy"

    def test_wrong_key_fails_to_decrypt(self, tmp_catalog_dir):
        first = CredentialCipher(load_secret_key(tmp_catalog_dir))
        token = first.encrypt("payload")
        other = CredentialCipher(Fernet.generate_key())
        with pytest.raises(SecretKeyError):
            other.decrypt(token)

    def test_is_encrypted_flag(self, tmp_catalog_dir):
        cipher = CredentialCipher(load_secret_key(tmp_catalog_dir))
        assert cipher.is_encrypted(cipher.encrypt("abc")) is True
        assert cipher.is_encrypted("abc") is False


class TestKeyLoading:
    def test_env_var_wins_over_file(self, tmp_catalog_dir, monkeypatch):
        key = Fernet.generate_key()
        monkeypatch.setenv(SECRET_ENV, key.decode("ascii"))
        (tmp_catalog_dir / SECRET_FILENAME).write_bytes(Fernet.generate_key())
        loaded = load_secret_key(tmp_catalog_dir)
        assert loaded == key

    def test_env_var_must_be_valid_fernet_key(self, tmp_catalog_dir, monkeypatch):
        monkeypatch.setenv(SECRET_ENV, "not-a-fernet-key")
        with pytest.raises(SecretKeyError):
            load_secret_key(tmp_catalog_dir)

    def test_file_is_created_with_0600_permissions(self, tmp_catalog_dir):
        load_secret_key(tmp_catalog_dir)
        path = tmp_catalog_dir / SECRET_FILENAME
        assert path.exists()
        mode = stat.S_IMODE(os.stat(path).st_mode)
        assert mode == 0o600, f"expected 0o600, got {oct(mode)}"

    def test_existing_file_is_reused(self, tmp_catalog_dir):
        first = load_secret_key(tmp_catalog_dir)
        second = load_secret_key(tmp_catalog_dir)
        assert first == second

    def test_malformed_file_is_rejected(self, tmp_catalog_dir):
        (tmp_catalog_dir / SECRET_FILENAME).write_bytes(b"garbage")
        with pytest.raises(SecretKeyError):
            load_secret_key(tmp_catalog_dir)


class TestRedaction:
    def test_redact_for_display_short_value(self):
        assert redact_for_display("ab") == "***"

    def test_redact_for_display_keeps_tail(self):
        assert redact_for_display("supersecret") == "***ret"

    def test_redact_for_display_empty(self):
        assert redact_for_display("") == ""

    def test_redact_for_display_encrypted_value(self):
        assert redact_for_display(ENC_PREFIX + "abc") == "***"

    def test_redact_connection_string_masks_password(self):
        out = redact_connection_string("postgresql://u:p@host:5432/db")
        assert "p" not in out.split("@")[0].split(":")[-1]
        assert ":***@" in out
        assert "host:5432" in out
        assert "/db" in out

    def test_redact_connection_string_preserves_username(self):
        out = redact_connection_string("postgresql://bob:hunter2@pg:5432/app")
        assert "bob:***@" in out

    def test_redact_connection_string_handles_no_password(self):
        out = redact_connection_string("postgresql://bob@pg:5432/app")
        assert out == "postgresql://bob@pg:5432/app"

    def test_redact_connection_string_handles_no_at(self):
        out = redact_connection_string("not-a-dsn")
        assert out.startswith("***")

    def test_redact_connection_string_empty(self):
        assert redact_connection_string("") == ""


class TestCatalogEncryption:
    def test_source_connection_string_is_encrypted_at_rest(self, tmp_catalog_dir, monkeypatch):
        monkeypatch.delenv(SECRET_ENV, raising=False)
        mgr = CatalogManager(tmp_catalog_dir)
        mgr.add_source(
            name="pg",
            source_type="postgresql",
            connection_string="postgresql://u:secret@host/db",
        )
        catalog_json = (tmp_catalog_dir / "catalog.json").read_text()
        assert "secret" not in catalog_json
        assert ENC_PREFIX in catalog_json

    def test_source_connection_string_round_trips_via_manager(self, tmp_catalog_dir):
        mgr = CatalogManager(tmp_catalog_dir)
        mgr.add_source(
            name="pg",
            source_type="postgresql",
            connection_string="postgresql://u:secret@host/db",
        )
        roundtrip = mgr.get_source("pg")
        assert roundtrip is not None
        assert roundtrip.connection_string == "postgresql://u:secret@host/db"

    def test_legacy_plaintext_catalog_is_readable(self, tmp_catalog_dir):
        catalog_path = tmp_catalog_dir / "catalog.json"
        catalog_path.write_text(json.dumps({
            "sources": {
                "pg": {
                    "name": "pg",
                    "source_type": "postgresql",
                    "connection_string": "postgresql://u:legacy@host/db",
                    "description": "",
                    "owner": "",
                    "source_params": {},
                    "created_at": "2026-04-16T00:00:00+00:00",
                    "updated_at": "2026-04-16T00:00:00+00:00",
                },
            },
            "snapshots": {},
            "projects": {},
            "load_history": [],
            "targets": {},
        }))
        mgr = CatalogManager(tmp_catalog_dir)
        src = mgr.get_source("pg")
        assert src.connection_string == "postgresql://u:legacy@host/db"

    def test_saving_upgrades_legacy_plaintext_to_encrypted(self, tmp_catalog_dir):
        catalog_path = tmp_catalog_dir / "catalog.json"
        catalog_path.write_text(json.dumps({
            "sources": {
                "pg": {
                    "name": "pg",
                    "source_type": "postgresql",
                    "connection_string": "postgresql://u:legacy@host/db",
                    "description": "",
                    "owner": "",
                    "source_params": {},
                    "created_at": "2026-04-16T00:00:00+00:00",
                    "updated_at": "2026-04-16T00:00:00+00:00",
                },
            },
            "snapshots": {},
            "projects": {},
            "load_history": [],
            "targets": {},
        }))
        mgr = CatalogManager(tmp_catalog_dir)
        mgr.update_source("pg", description="touched")
        on_disk = catalog_path.read_text()
        assert "legacy" not in on_disk
        assert ENC_PREFIX in on_disk

    def test_target_password_is_encrypted_at_rest(self, tmp_catalog_dir):
        mgr = CatalogManager(tmp_catalog_dir)
        mgr.add_target(
            name="g",
            endpoint="http://localhost:8529",
            database="_system",
            username="root",
            password="VERY-SECRET",
        )
        on_disk = (tmp_catalog_dir / "catalog.json").read_text()
        assert "VERY-SECRET" not in on_disk
        got = mgr.get_target("g")
        assert got.password == "VERY-SECRET"

    def test_target_empty_password_stays_empty(self, tmp_catalog_dir):
        mgr = CatalogManager(tmp_catalog_dir)
        mgr.add_target(name="g", password="")
        assert mgr.get_target("g").password == ""
        on_disk = (tmp_catalog_dir / "catalog.json").read_text()
        assert '"password": ""' in on_disk
