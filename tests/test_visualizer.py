from __future__ import annotations

import json

import pytest

from r2g.config import ConfigManager
from r2g.generators.visualizer import MappingVisualizer
from r2g.types import (
    CollectionMapping,
    Column,
    EdgeDefinition,
    ForeignKey,
    MappingConfig,
    Schema,
    Table,
)


@pytest.fixture
def viz_schema() -> Schema:
    users = Table(
        name="users",
        columns=[
            Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="name", data_type="text", is_nullable=False),
            Column(name="email", data_type="text", is_nullable=True),
            Column(name="is_active", data_type="boolean", is_nullable=False),
        ],
        primary_key=["id"],
        foreign_keys=[],
    )
    posts = Table(
        name="posts",
        columns=[
            Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="author_id", data_type="integer", is_nullable=False),
            Column(name="title", data_type="text", is_nullable=False),
            Column(name="body", data_type="text", is_nullable=True),
        ],
        primary_key=["id"],
        foreign_keys=[
            ForeignKey(column="author_id", foreign_table="users", foreign_column="id", constraint_name="fk_author"),
        ],
    )
    tags = Table(
        name="tags",
        columns=[
            Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="label", data_type="text", is_nullable=False),
        ],
        primary_key=["id"],
        foreign_keys=[],
    )
    post_tags = Table(
        name="post_tags",
        columns=[
            Column(name="post_id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="tag_id", data_type="integer", is_nullable=False, is_primary_key=True),
        ],
        primary_key=["post_id", "tag_id"],
        foreign_keys=[
            ForeignKey(column="post_id", foreign_table="posts", foreign_column="id", constraint_name="fk_post"),
            ForeignKey(column="tag_id", foreign_table="tags", foreign_column="id", constraint_name="fk_tag"),
        ],
    )
    return Schema(tables={"users": users, "posts": posts, "tags": tags, "post_tags": post_tags})


@pytest.fixture
def viz_config() -> MappingConfig:
    return MappingConfig(
        collections={
            "users": CollectionMapping(source_table="users", target_collection="users"),
            "posts": CollectionMapping(source_table="posts", target_collection="posts"),
            "tags": CollectionMapping(source_table="tags", target_collection="tags"),
            "post_tags": CollectionMapping(source_table="post_tags", target_collection="post_tags", is_join_table=True),
        },
        edges=[
            EdgeDefinition(
                edge_collection="posts_to_users",
                from_collection="posts",
                to_collection="users",
                from_field="author_id",
                to_field="id",
            ),
            EdgeDefinition(
                edge_collection="post_tags_to_posts",
                from_collection="post_tags",
                to_collection="posts",
                from_field="post_id",
                to_field="id",
            ),
            EdgeDefinition(
                edge_collection="post_tags_to_tags",
                from_collection="post_tags",
                to_collection="tags",
                from_field="tag_id",
                to_field="id",
            ),
        ],
    )


@pytest.fixture
def visualizer(viz_schema, viz_config) -> MappingVisualizer:
    return MappingVisualizer(viz_schema, viz_config)


class TestBuildGraphData:
    def test_returns_nodes_and_links(self, visualizer):
        data = visualizer._build_graph_data()
        assert "nodes" in data
        assert "links" in data

    def test_node_count_matches_collections(self, visualizer):
        data = visualizer._build_graph_data()
        assert len(data["nodes"]) == 4

    def test_link_count_matches_edges(self, visualizer):
        data = visualizer._build_graph_data()
        assert len(data["links"]) == 3

    def test_node_has_expected_fields(self, visualizer):
        data = visualizer._build_graph_data()
        node = next(n for n in data["nodes"] if n["id"] == "users")
        assert node["sourceTable"] == "users"
        assert node["type"] == "document"
        assert node["columns"] == 4
        assert node["pk"] == ["id"]
        assert node["isJoinTable"] is False

    def test_join_table_flagged(self, visualizer):
        data = visualizer._build_graph_data()
        node = next(n for n in data["nodes"] if n["id"] == "post_tags")
        assert node["isJoinTable"] is True

    def test_link_has_expected_fields(self, visualizer):
        data = visualizer._build_graph_data()
        link = next(l for l in data["links"] if l["edgeCollection"] == "posts_to_users")
        assert link["source"] == "posts"
        assert link["target"] == "users"
        assert link["fromField"] == "author_id"
        assert link["toField"] == "id"

    def test_adds_missing_nodes_from_edges(self, viz_schema):
        config = MappingConfig(
            collections={
                "posts": CollectionMapping(source_table="posts", target_collection="posts"),
            },
            edges=[
                EdgeDefinition(
                    edge_collection="posts_to_users",
                    from_collection="posts",
                    to_collection="users",
                    from_field="author_id",
                    to_field="id",
                ),
            ],
        )
        viz = MappingVisualizer(viz_schema, config)
        data = viz._build_graph_data()
        node_ids = {n["id"] for n in data["nodes"]}
        assert "users" in node_ids


class TestBuildTablesData:
    def test_returns_all_tables(self, visualizer):
        data = visualizer._build_tables_data()
        assert len(data) == 4

    def test_table_has_columns(self, visualizer):
        data = visualizer._build_tables_data()
        users = next(t for t in data if t["name"] == "users")
        assert len(users["columns"]) == 4

    def test_column_has_pk_flag(self, visualizer):
        data = visualizer._build_tables_data()
        users = next(t for t in data if t["name"] == "users")
        id_col = next(c for c in users["columns"] if c["name"] == "id")
        assert id_col["isPk"] is True
        assert id_col["isFk"] is False

    def test_column_has_fk_flag(self, visualizer):
        data = visualizer._build_tables_data()
        posts = next(t for t in data if t["name"] == "posts")
        author_col = next(c for c in posts["columns"] if c["name"] == "author_id")
        assert author_col["isFk"] is True
        assert author_col["fkTarget"] == "users"

    def test_nullable_flag(self, visualizer):
        data = visualizer._build_tables_data()
        users = next(t for t in data if t["name"] == "users")
        email_col = next(c for c in users["columns"] if c["name"] == "email")
        assert email_col["nullable"] is True
        name_col = next(c for c in users["columns"] if c["name"] == "name")
        assert name_col["nullable"] is False

    def test_join_table_flag(self, visualizer):
        data = visualizer._build_tables_data()
        post_tags = next(t for t in data if t["name"] == "post_tags")
        assert post_tags["isJoinTable"] is True

    def test_target_collection_name(self, visualizer):
        data = visualizer._build_tables_data()
        users = next(t for t in data if t["name"] == "users")
        assert users["targetCollection"] == "users"


class TestBuildEdgesData:
    def test_returns_all_edges(self, visualizer):
        data = visualizer._build_edges_data()
        assert len(data) == 3

    def test_edge_has_expected_fields(self, visualizer):
        data = visualizer._build_edges_data()
        edge = data[0]
        assert "edgeCollection" in edge
        assert "fromCollection" in edge
        assert "toCollection" in edge
        assert "fromField" in edge
        assert "toField" in edge


class TestGenerate:
    def test_writes_html_file(self, visualizer, tmp_path):
        path = tmp_path / "viz.html"
        visualizer.generate(str(path))
        assert path.exists()

    def test_html_contains_doctype(self, visualizer, tmp_path):
        path = tmp_path / "viz.html"
        html = visualizer.generate(str(path))
        assert "<!DOCTYPE html>" in html

    def test_html_contains_d3_script(self, visualizer, tmp_path):
        path = tmp_path / "viz.html"
        html = visualizer.generate(str(path))
        assert "d3.v7.min.js" in html

    def test_html_contains_graph_data(self, visualizer, tmp_path):
        path = tmp_path / "viz.html"
        html = visualizer.generate(str(path))
        assert "users" in html
        assert "posts" in html
        assert "posts_to_users" in html

    def test_html_contains_tabs(self, visualizer, tmp_path):
        path = tmp_path / "viz.html"
        html = visualizer.generate(str(path))
        assert "Graph Schema" in html
        assert "Relational Schema" in html
        assert "Edge Mapping" in html

    def test_html_contains_stats(self, visualizer, tmp_path):
        path = tmp_path / "viz.html"
        html = visualizer.generate(str(path))
        assert '"tables": 4' in html
        assert '"edges": 3' in html

    def test_creates_parent_directories(self, visualizer, tmp_path):
        path = tmp_path / "subdir" / "nested" / "viz.html"
        visualizer.generate(str(path))
        assert path.exists()


class TestBuildConfigData:
    def test_returns_collections_and_edges(self, visualizer):
        data = visualizer._build_config_data()
        assert "collections" in data
        assert "edges" in data
        assert "sourceSchema" in data
        assert "keySeparator" in data

    def test_collection_has_all_fields(self, visualizer):
        data = visualizer._build_config_data()
        coll = data["collections"]["users"]
        assert coll["sourceTable"] == "users"
        assert coll["targetCollection"] == "users"
        assert "allFields" in coll
        assert "id" in coll["allFields"]
        assert "name" in coll["allFields"]

    def test_join_table_flag_preserved(self, visualizer):
        data = visualizer._build_config_data()
        assert data["collections"]["post_tags"]["isJoinTable"] is True
        assert data["collections"]["users"]["isJoinTable"] is False

    def test_edges_match_config(self, visualizer):
        data = visualizer._build_config_data()
        assert len(data["edges"]) == 3
        edge_names = [e["edgeCollection"] for e in data["edges"]]
        assert "posts_to_users" in edge_names


class TestEditorHtml:
    def test_html_contains_editor_tab(self, visualizer, tmp_path):
        path = tmp_path / "viz.html"
        html = visualizer.generate(str(path))
        assert "Mapping Editor" in html

    def test_html_contains_export_button(self, visualizer, tmp_path):
        path = tmp_path / "viz.html"
        html = visualizer.generate(str(path))
        assert "Export YAML" in html

    def test_html_contains_config_data(self, visualizer, tmp_path):
        path = tmp_path / "viz.html"
        html = visualizer.generate(str(path))
        assert "configData" in html

    def test_html_contains_yaml_modal(self, visualizer, tmp_path):
        path = tmp_path / "viz.html"
        html = visualizer.generate(str(path))
        assert "yaml-modal" in html

    def test_html_contains_download_button(self, visualizer, tmp_path):
        path = tmp_path / "viz.html"
        html = visualizer.generate(str(path))
        assert "Download" in html


class TestFromAutoConfig:
    def test_round_trip_with_config_manager(self, viz_schema, tmp_path):
        config = ConfigManager.generate_default_config(viz_schema)
        viz = MappingVisualizer(viz_schema, config)
        path = str(tmp_path / "out.html")
        html = viz.generate(path)
        assert "<!DOCTYPE html>" in html
        assert "users" in html
        assert "posts_to_users" in html
