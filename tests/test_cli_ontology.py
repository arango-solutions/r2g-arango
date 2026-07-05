"""CLI tests for ``r2g ontology suggest`` with a fake provider (Phase 10a)."""
from __future__ import annotations

import json
import sys

import pytest
from typer.testing import CliRunner

from r2g.catalog import CatalogManager
from r2g.config import ConfigManager
from r2g.llm.base import OntologyProposal, ProposedEdge, ProposedRename
from r2g.main import app
from r2g.types import Classification, Column, Schema, Table

runner = CliRunner()


@pytest.fixture(autouse=True)
def _reset_structlog(monkeypatch):
    import structlog

    def _stderr_setup(level: str = "INFO", json_output: bool = False) -> None:
        structlog.configure(
            wrapper_class=structlog.make_filtering_bound_logger(0),
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
            cache_logger_on_first_use=False,
        )

    monkeypatch.setattr("r2g.log.setup_logging", _stderr_setup)
    monkeypatch.setattr("r2g.main.setup_logging", _stderr_setup)
    _stderr_setup()
    yield
    structlog.reset_defaults()


class _FakeProvider:
    """A canned, network-free LLM provider for tests."""

    provider_type = "fake"

    def __init__(self, proposal: OntologyProposal):
        self._proposal = proposal
        self.calls: list = []

    def propose_ontology(self, request):
        self.calls.append(request)
        return self._proposal


@pytest.fixture
def project(tmp_path, monkeypatch):
    catalog_dir = tmp_path / "catalog"
    mgr = CatalogManager(str(catalog_dir))
    mgr.add_source("shop", "postgresql", "postgresql://localhost/shop")
    # orders has NO declared FK; the "LLM" will surface the implicit edge.
    schema = Schema(
        tables={
            "customer": Table(
                name="customer",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(
                        name="email",
                        data_type="text",
                        classification=Classification(tags=["PII.Sensitive"]),
                    ),
                ],
                primary_key=["id"],
            ),
            "orders": Table(
                name="orders",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="customer_id", data_type="integer"),
                ],
                primary_key=["id"],
            ),
        }
    )
    mgr.create_snapshot("shop", schema, pg_schema="public")
    config = ConfigManager.generate_default_config(schema)
    mapping_path = tmp_path / "mapping.yaml"
    ConfigManager.save_config(config, str(mapping_path))
    mgr.create_project("proj", "shop", str(mapping_path))
    monkeypatch.setattr("r2g.main._get_catalog", lambda: CatalogManager(str(catalog_dir)))
    return "proj", str(mapping_path)


def _proposal() -> OntologyProposal:
    return OntologyProposal(
        edges=[
            ProposedEdge(
                edge_collection="orders_to_customer",
                from_collection="orders",
                to_collection="customer",
                from_fields=["customer_id"],
                to_fields=["id"],
                rationale="customer_id references customer.id",
                confidence=0.9,
            )
        ],
        renames=[
            ProposedRename(
                source_table="customer",
                column="email",
                target_property="email_address",
                confidence=0.7,
            )
        ],
    )


def _patch_provider(monkeypatch, proposal: OntologyProposal) -> _FakeProvider:
    fake = _FakeProvider(proposal)
    monkeypatch.setattr("r2g.llm.create_llm_provider", lambda *a, **k: fake)
    return fake


class TestOntologySuggest:
    def test_help(self):
        result = runner.invoke(app, ["ontology", "suggest", "--help"])
        assert result.exit_code == 0
        assert "--apply" in result.output
        assert "--domain" in result.output

    def test_preview_shows_proposed_edge_and_does_not_write(self, project, monkeypatch):
        name, mapping_path = project
        _patch_provider(monkeypatch, _proposal())
        before = ConfigManager.load_config(mapping_path)

        result = runner.invoke(app, ["ontology", "suggest", name])
        assert result.exit_code == 0, result.output
        assert "orders_to_customer" in result.output
        assert "Preview only" in result.output
        # Nothing written without --apply.
        after = ConfigManager.load_config(mapping_path)
        assert after.model_dump() == before.model_dump()

    def test_passes_schema_digest_to_provider(self, project, monkeypatch):
        name, _ = project
        fake = _patch_provider(monkeypatch, _proposal())
        result = runner.invoke(app, ["ontology", "suggest", name])
        assert result.exit_code == 0, result.output
        assert fake.calls, "provider was not called"
        digest = fake.calls[0].schema_digest
        # Redaction: the PII column is name-only, type never sent.
        assert "email : [redacted" in digest
        assert "email : text" not in digest

    def test_json_output(self, project, monkeypatch):
        name, _ = project
        _patch_provider(monkeypatch, _proposal())
        result = runner.invoke(app, ["ontology", "suggest", name, "--json"])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["provenance"]["proposed_edges"] == 1
        assert any(
            c.get("edge") == "orders_to_customer" or "orders_to_customer" in str(c)
            for c in payload["changes"]
        )

    def test_apply_writes_mapping_and_provenance(self, project, monkeypatch, tmp_path):
        name, mapping_path = project
        _patch_provider(monkeypatch, _proposal())
        result = runner.invoke(app, ["ontology", "suggest", name, "--apply", "--yes"])
        assert result.exit_code == 0, result.output

        applied = ConfigManager.load_config(mapping_path)
        assert any(e.edge_collection == "orders_to_customer" for e in applied.edges)
        assert applied.collections["customer"].field_mappings.get("email") == "email_address"
        prov = tmp_path / "llm-ontology-provenance.json"
        assert prov.exists()
        data = json.loads(prov.read_text())
        assert data["proposed_edges"] == 1

    def test_apply_declined_writes_nothing(self, project, monkeypatch):
        name, mapping_path = project
        _patch_provider(monkeypatch, _proposal())
        before = ConfigManager.load_config(mapping_path)
        result = runner.invoke(app, ["ontology", "suggest", name, "--apply"], input="n\n")
        assert result.exit_code == 0
        after = ConfigManager.load_config(mapping_path)
        assert after.model_dump() == before.model_dump()

    def test_unknown_project_exits_1(self, project, monkeypatch):
        _patch_provider(monkeypatch, _proposal())
        result = runner.invoke(app, ["ontology", "suggest", "nope"])
        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    def test_unknown_provider_exits_1(self, project):
        name, _ = project
        result = runner.invoke(app, ["ontology", "suggest", name, "--provider", "bogus"])
        assert result.exit_code == 1
        assert "Unsupported LLM provider" in result.output

    def test_ground_flag_passes_denorm_evidence_to_provider(self, project, monkeypatch):
        name, _ = project
        fake = _patch_provider(monkeypatch, _proposal())
        monkeypatch.setattr(
            "r2g.llm.grounding.build_grounding",
            lambda schema, **k: "GROUNDING: zip -> city, state",
        )
        result = runner.invoke(app, ["ontology", "suggest", name, "--ground"])
        assert result.exit_code == 0, result.output
        assert fake.calls[0].grounding == "GROUNDING: zip -> city, state"

    def test_sample_flag_grounds_non_sensitive_columns_only(self, project, monkeypatch):
        name, _ = project
        fake = _patch_provider(monkeypatch, _proposal())

        class _FakeSampler:
            def sample_values(self, table, column, limit=5):
                return {"customer_id": [10, 11], "id": [1, 2], "email": ["a@x.com"]}.get(
                    column, []
                )

            def close(self):
                pass

        monkeypatch.setattr(
            "r2g.llm.sampling.build_sampler_for_source", lambda source, **k: _FakeSampler()
        )
        result = runner.invoke(app, ["ontology", "suggest", name, "--sample"])
        assert result.exit_code == 0, result.output
        digest = fake.calls[0].schema_digest
        # Non-sensitive column is grounded with example values...
        assert "e.g. 10, 11" in digest
        # ...but the redacted PII column is never sampled even if the DB returns values.
        assert "a@x.com" not in digest


class TestOntologySuggestRsaEngine:
    """`--engine rsa`: deterministic conceptual model via relational-schema-analyzer."""

    def test_rsa_engine_uses_analyzer_not_llm(self, project, monkeypatch):
        name, _ = project
        # If the RSA path accidentally fell through to the LLM path this fake would
        # be hit; assert it never is.
        fake = _patch_provider(monkeypatch, _proposal())

        captured: dict = {}

        def _fake_propose(schema, *, provider=None, model=None, api_key=None):
            captured["provider"] = provider
            return _proposal(), {"confidence": 0.9, "detectedPatterns": ["join_table"]}

        monkeypatch.setattr(
            "r2g.rsa_ontology.propose_ontology_from_schema", _fake_propose
        )
        result = runner.invoke(app, ["ontology", "suggest", name, "--engine", "rsa"])
        assert result.exit_code == 0, result.output
        assert "orders_to_customer" in result.output
        assert not fake.calls, "LLM provider must not be called for --engine rsa"
        # Deterministic by default: no provider is passed to the analyzer.
        assert captured["provider"] is None

    def test_rsa_refine_passes_provider(self, project, monkeypatch):
        name, _ = project
        _patch_provider(monkeypatch, _proposal())
        captured: dict = {}

        def _fake_propose(schema, *, provider=None, model=None, api_key=None):
            captured["provider"] = provider
            return _proposal(), {"confidence": 0.8}

        monkeypatch.setattr(
            "r2g.rsa_ontology.propose_ontology_from_schema", _fake_propose
        )
        result = runner.invoke(
            app,
            ["ontology", "suggest", name, "--engine", "rsa", "--refine", "--provider", "anthropic"],
        )
        assert result.exit_code == 0, result.output
        assert captured["provider"] == "anthropic"

    def test_rsa_engine_json_provenance(self, project, monkeypatch):
        name, _ = project

        def _fake_propose(schema, *, provider=None, model=None, api_key=None):
            return _proposal(), {"confidence": 0.9, "detectedPatterns": ["join_table"]}

        monkeypatch.setattr(
            "r2g.rsa_ontology.propose_ontology_from_schema", _fake_propose
        )
        result = runner.invoke(
            app, ["ontology", "suggest", name, "--engine", "rsa", "--json"]
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["provenance"]["engine"] == "relational-schema-analyzer"
        assert payload["provenance"]["refined"] is False
        assert payload["provenance"]["analyzer_confidence"] == 0.9

    def test_unknown_engine_exits_1(self, project):
        name, _ = project
        result = runner.invoke(app, ["ontology", "suggest", name, "--engine", "bogus"])
        assert result.exit_code == 1
        assert "engine" in result.output.lower()

    def test_rsa_engine_real_library_end_to_end(self, project):
        """Exercise the real analyzer (skips cleanly when RSA isn't installed)."""
        pytest.importorskip("relational_schema_analyzer")
        name, _ = project
        result = runner.invoke(
            app, ["ontology", "suggest", name, "--engine", "rsa", "--json"]
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["provenance"]["engine"] == "relational-schema-analyzer"
        # customer -> Customer semantic rename is a deterministic collection hint.
        collections = payload["proposal"]["collections"]
        assert any(c["target_collection"] == "Customer" for c in collections)
