"""Generates a self-contained HTML visualizer for relational-to-graph mappings.

Renders:
  - Left pane: PG relational schema (table cards with PK/FK badges)
  - Right pane: Interactive D3 force-directed graph (vertex circles, edge arrows)
  - Bottom: Mapping detail table
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from r2g.log import get_logger
from r2g.types import MappingConfig, Schema

logger = get_logger(__name__)

_TYPE_BADGE_COLORS = {
    "integer": "#6366f1",
    "bigint": "#6366f1",
    "smallint": "#6366f1",
    "serial": "#6366f1",
    "text": "#059669",
    "boolean": "#d97706",
    "numeric": "#2563eb",
    "real": "#2563eb",
    "double precision": "#2563eb",
    "jsonb": "#7c3aed",
    "json": "#7c3aed",
    "ARRAY": "#dc2626",
    "timestamp without time zone": "#64748b",
}


class MappingVisualizer:
    """Generates a self-contained HTML file visualizing the PG→Graph mapping."""

    def __init__(self, schema: Schema, config: MappingConfig) -> None:
        self.schema = schema
        self.config = config

    def _build_graph_data(self) -> dict:
        nodes = []
        links = []
        node_set = set()

        for cm in self.config.collections.values():
            if cm.collection_type == "document" and cm.target_collection not in node_set:
                table = self.schema.tables.get(cm.source_table)
                col_count = len(table.columns) if table else 0
                pk = table.primary_key if table else []
                nodes.append({
                    "id": cm.target_collection,
                    "sourceTable": cm.source_table,
                    "type": "document",
                    "columns": col_count,
                    "pk": pk,
                    "isJoinTable": cm.is_join_table,
                })
                node_set.add(cm.target_collection)

        for edge in self.config.edges:
            for coll in [edge.from_collection, edge.to_collection]:
                if coll not in node_set:
                    nodes.append({
                        "id": coll,
                        "sourceTable": coll,
                        "type": "document",
                        "columns": 0,
                        "pk": [],
                        "isJoinTable": False,
                    })
                    node_set.add(coll)

            links.append({
                "source": edge.from_collection,
                "target": edge.to_collection,
                "edgeCollection": edge.edge_collection,
                "fromField": edge.from_field,
                "toField": edge.to_field,
            })

        return {"nodes": nodes, "links": links}

    def _build_tables_data(self) -> list[dict]:
        tables = []
        for table_name, table in self.schema.tables.items():
            fk_targets = {}
            for fk in table.foreign_keys:
                fk_targets[fk.column] = fk.foreign_table

            cols = []
            for col in table.columns:
                cols.append({
                    "name": col.name,
                    "type": col.data_type,
                    "isPk": col.is_primary_key,
                    "isFk": col.name in fk_targets,
                    "fkTarget": fk_targets.get(col.name, ""),
                    "nullable": col.is_nullable,
                })

            mapping = self.config.collections.get(table_name)
            tables.append({
                "name": table_name,
                "columns": cols,
                "pk": table.primary_key,
                "targetCollection": mapping.target_collection if mapping else table_name,
                "isJoinTable": mapping.is_join_table if mapping else False,
            })
        return tables

    def _build_edges_data(self) -> list[dict]:
        return [
            {
                "edgeCollection": e.edge_collection,
                "fromCollection": e.from_collection,
                "toCollection": e.to_collection,
                "fromField": e.from_field,
                "toField": e.to_field,
            }
            for e in self.config.edges
        ]

    def generate(self, output_path: str) -> str:
        graph_data = self._build_graph_data()
        tables_data = self._build_tables_data()
        edges_data = self._build_edges_data()
        generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        html = _HTML_TEMPLATE.replace("/* __GRAPH_DATA__ */", json.dumps(graph_data))
        html = html.replace("/* __TABLES_DATA__ */", json.dumps(tables_data))
        html = html.replace("/* __EDGES_DATA__ */", json.dumps(edges_data))
        html = html.replace("/* __GENERATED_AT__ */", generated_at)
        html = html.replace(
            "/* __STATS__ */",
            json.dumps({
                "tables": len(self.schema.tables),
                "collections": len([
                    c for c in self.config.collections.values()
                    if c.collection_type == "document"
                ]),
                "edges": len(self.config.edges),
            }),
        )

        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(html, encoding="utf-8")
        return html


_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>R2G Mapping Visualizer</title>
<script src="https://d3js.org/d3.v7.min.js"></script>
<style>
  :root {
    --bg: #0f172a; --surface: #1e293b; --surface2: #334155;
    --border: #475569; --text: #e2e8f0; --text-muted: #94a3b8;
    --accent: #38bdf8; --accent2: #818cf8; --green: #34d399;
    --yellow: #fbbf24; --red: #f87171; --pink: #f472b6;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'SF Mono', 'Fira Code', 'Cascadia Code', monospace;
    background: var(--bg); color: var(--text); line-height: 1.5;
  }
  header {
    background: var(--surface); border-bottom: 1px solid var(--border);
    padding: 16px 24px; display: flex; align-items: center; gap: 24px;
  }
  header h1 { font-size: 18px; font-weight: 600; color: var(--accent); }
  .stats { display: flex; gap: 16px; }
  .stat {
    background: var(--surface2); padding: 4px 12px; border-radius: 6px;
    font-size: 12px; color: var(--text-muted);
  }
  .stat strong { color: var(--text); }
  .generated { margin-left: auto; font-size: 11px; color: var(--text-muted); }
  .tabs {
    display: flex; gap: 0; background: var(--surface);
    border-bottom: 1px solid var(--border); padding: 0 24px;
  }
  .tab {
    padding: 10px 20px; cursor: pointer; font-size: 13px;
    color: var(--text-muted); border-bottom: 2px solid transparent;
    transition: all 0.2s;
  }
  .tab:hover { color: var(--text); }
  .tab.active { color: var(--accent); border-bottom-color: var(--accent); }
  .tab-content { display: none; }
  .tab-content.active { display: block; }

  /* Graph tab */
  #graph-container {
    width: 100%; height: calc(100vh - 140px); position: relative;
  }
  #graph-container svg { width: 100%; height: 100%; }
  .node circle {
    stroke-width: 2; cursor: grab; transition: r 0.2s;
  }
  .node circle:hover { filter: brightness(1.3); }
  .node text {
    font-size: 11px; fill: var(--text); pointer-events: none;
    text-anchor: middle; font-weight: 600;
  }
  .node .col-count {
    font-size: 9px; fill: var(--text-muted); font-weight: 400;
  }
  .link { stroke-opacity: 0.6; fill: none; }
  .link-label {
    font-size: 9px; fill: var(--text-muted);
    pointer-events: none;
  }
  .link-label-bg { fill: var(--bg); opacity: 0.85; }
  marker { fill: var(--accent); }
  .tooltip {
    position: absolute; background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 12px 16px; font-size: 12px;
    pointer-events: none; opacity: 0; transition: opacity 0.15s;
    max-width: 280px; z-index: 100;
  }
  .tooltip.visible { opacity: 1; }
  .tooltip h3 { color: var(--accent); margin-bottom: 6px; font-size: 13px; }
  .tooltip .detail { color: var(--text-muted); }

  /* Tables tab */
  .tables-grid {
    display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
    gap: 16px; padding: 24px;
  }
  .table-card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; overflow: hidden;
  }
  .table-card-header {
    padding: 10px 14px; background: var(--surface2);
    display: flex; align-items: center; gap: 8px;
    border-bottom: 1px solid var(--border);
  }
  .table-card-header h3 { font-size: 14px; color: var(--accent); }
  .table-card-header .arrow { color: var(--text-muted); font-size: 12px; }
  .table-card-header .target { color: var(--green); font-size: 13px; }
  .badge {
    font-size: 9px; padding: 1px 6px; border-radius: 3px;
    font-weight: 600; text-transform: uppercase;
  }
  .badge-join { background: var(--yellow); color: #000; }
  .col-row {
    display: flex; align-items: center; padding: 4px 14px; gap: 8px;
    font-size: 12px; border-bottom: 1px solid rgba(71,85,105,0.3);
  }
  .col-row:last-child { border-bottom: none; }
  .col-name { flex: 1; }
  .col-type {
    font-size: 10px; padding: 1px 6px; border-radius: 3px;
    color: #fff; opacity: 0.85;
  }
  .col-badge {
    font-size: 9px; padding: 1px 5px; border-radius: 3px;
    font-weight: 700;
  }
  .pk-badge { background: var(--green); color: #000; }
  .fk-badge { background: var(--accent); color: #000; }
  .nullable-badge { color: var(--text-muted); font-size: 10px; }
  .fk-target { color: var(--text-muted); font-size: 10px; }

  /* Edges tab */
  .edges-table-wrap { padding: 24px; overflow-x: auto; }
  .edges-table {
    width: 100%; border-collapse: collapse; font-size: 13px;
  }
  .edges-table th {
    text-align: left; padding: 10px 14px; background: var(--surface2);
    color: var(--text-muted); font-weight: 600; font-size: 11px;
    text-transform: uppercase; letter-spacing: 0.5px;
    border-bottom: 1px solid var(--border);
  }
  .edges-table td {
    padding: 8px 14px; border-bottom: 1px solid rgba(71,85,105,0.3);
  }
  .edges-table tr:hover td { background: rgba(56,189,248,0.05); }
  .edge-name { color: var(--pink); font-weight: 600; }
  .edge-from { color: var(--accent); }
  .edge-to { color: var(--green); }
  .edge-field { color: var(--yellow); }
  .edge-arrow { color: var(--text-muted); }

  /* Legend */
  .legend {
    position: absolute; bottom: 16px; left: 16px; background: var(--surface);
    border: 1px solid var(--border); border-radius: 8px; padding: 12px 16px;
    font-size: 11px; display: flex; flex-direction: column; gap: 6px;
  }
  .legend-item { display: flex; align-items: center; gap: 8px; }
  .legend-dot {
    width: 12px; height: 12px; border-radius: 50%;
    border: 2px solid; flex-shrink: 0;
  }
</style>
</head>
<body>

<header>
  <h1>R2G Mapping Visualizer</h1>
  <div class="stats" id="stats"></div>
  <div class="generated" id="generated-at"></div>
</header>

<div class="tabs">
  <div class="tab active" data-tab="graph">Graph Schema</div>
  <div class="tab" data-tab="tables">Relational Schema</div>
  <div class="tab" data-tab="edges">Edge Mapping</div>
</div>

<div id="graph" class="tab-content active">
  <div id="graph-container">
    <svg></svg>
    <div class="tooltip" id="tooltip"></div>
    <div class="legend">
      <div class="legend-item">
        <div class="legend-dot" style="background:rgba(56,189,248,0.15);border-color:#38bdf8"></div>
        <span>Document collection</span>
      </div>
      <div class="legend-item">
        <div class="legend-dot" style="background:rgba(251,191,36,0.15);border-color:#fbbf24"></div>
        <span>Join table</span>
      </div>
      <div class="legend-item">
        <svg width="30" height="12"><line x1="0" y1="6" x2="24" y2="6" stroke="#38bdf8" stroke-width="1.5"/><polygon points="24,3 30,6 24,9" fill="#38bdf8"/></svg>
        <span>Edge collection (FK relationship)</span>
      </div>
    </div>
  </div>
</div>

<div id="tables" class="tab-content">
  <div class="tables-grid" id="tables-grid"></div>
</div>

<div id="edges" class="tab-content">
  <div class="edges-table-wrap">
    <table class="edges-table" id="edges-table">
      <thead>
        <tr>
          <th>Edge Collection</th>
          <th>From</th>
          <th></th>
          <th>To</th>
          <th>FK Column</th>
          <th>Target PK</th>
        </tr>
      </thead>
      <tbody id="edges-tbody"></tbody>
    </table>
  </div>
</div>

<script>
const graphData = /* __GRAPH_DATA__ */;
const tablesData = /* __TABLES_DATA__ */;
const edgesData = /* __EDGES_DATA__ */;
const stats = /* __STATS__ */;
const generatedAt = "/* __GENERATED_AT__ */";

const typeBadgeColors = {
  "integer":"#6366f1","bigint":"#6366f1","smallint":"#6366f1","serial":"#6366f1",
  "text":"#059669","boolean":"#d97706","numeric":"#2563eb","real":"#2563eb",
  "double precision":"#2563eb","jsonb":"#7c3aed","json":"#7c3aed",
  "ARRAY":"#dc2626","timestamp without time zone":"#64748b"
};

// Stats
document.getElementById("stats").innerHTML =
  `<div class="stat"><strong>${stats.tables}</strong> PG tables</div>` +
  `<div class="stat"><strong>${stats.collections}</strong> document collections</div>` +
  `<div class="stat"><strong>${stats.edges}</strong> edge collections</div>`;
document.getElementById("generated-at").textContent = generatedAt;

// Tabs
document.querySelectorAll(".tab").forEach(tab => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
    document.querySelectorAll(".tab-content").forEach(tc => tc.classList.remove("active"));
    tab.classList.add("active");
    document.getElementById(tab.dataset.tab).classList.add("active");
    if (tab.dataset.tab === "graph") simulation.alpha(0.1).restart();
  });
});

// --- Graph ---
const container = document.getElementById("graph-container");
const svg = d3.select("#graph-container svg");
const width = container.clientWidth;
const height = container.clientHeight;
const tooltip = document.getElementById("tooltip");

svg.attr("viewBox", [0, 0, width, height]);

svg.append("defs").append("marker")
  .attr("id", "arrowhead").attr("viewBox", "0 -5 10 10")
  .attr("refX", 28).attr("refY", 0)
  .attr("markerWidth", 8).attr("markerHeight", 8)
  .attr("orient", "auto")
  .append("path").attr("d", "M0,-4L10,0L0,4").attr("fill", "#38bdf8");

const linkGroup = svg.append("g");
const nodeGroup = svg.append("g");
const labelGroup = svg.append("g");

const simulation = d3.forceSimulation(graphData.nodes)
  .force("link", d3.forceLink(graphData.links).id(d => d.id).distance(200))
  .force("charge", d3.forceManyBody().strength(-800))
  .force("center", d3.forceCenter(width / 2, height / 2))
  .force("collision", d3.forceCollide().radius(50));

const link = linkGroup.selectAll("path")
  .data(graphData.links).join("path")
  .attr("class", "link")
  .attr("stroke", "#38bdf8").attr("stroke-width", 1.5)
  .attr("marker-end", "url(#arrowhead)");

const linkLabelBg = labelGroup.selectAll("rect")
  .data(graphData.links).join("rect")
  .attr("class", "link-label-bg").attr("rx", 3);

const linkLabel = labelGroup.selectAll("text")
  .data(graphData.links).join("text")
  .attr("class", "link-label")
  .text(d => d.edgeCollection);

const node = nodeGroup.selectAll("g")
  .data(graphData.nodes).join("g")
  .attr("class", "node")
  .call(d3.drag()
    .on("start", (e, d) => { if (!e.active) simulation.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
    .on("drag", (e, d) => { d.fx = e.x; d.fy = e.y; })
    .on("end", (e, d) => { if (!e.active) simulation.alphaTarget(0); d.fx = null; d.fy = null; })
  );

node.append("circle")
  .attr("r", d => 20 + d.columns * 1.5)
  .attr("fill", d => d.isJoinTable ? "rgba(251,191,36,0.15)" : "rgba(56,189,248,0.15)")
  .attr("stroke", d => d.isJoinTable ? "#fbbf24" : "#38bdf8");

node.append("text").attr("dy", -2).text(d => d.id);
node.append("text").attr("class", "col-count").attr("dy", 12)
  .text(d => `${d.columns} cols | PK: ${d.pk.join(", ") || "–"}`);

node.on("mouseover", (e, d) => {
  const edges = graphData.links.filter(l =>
    (l.source.id || l.source) === d.id || (l.target.id || l.target) === d.id);
  tooltip.innerHTML = `<h3>${d.id}</h3>` +
    `<div class="detail">Source: ${d.sourceTable}</div>` +
    `<div class="detail">Columns: ${d.columns} | PK: ${d.pk.join(", ") || "–"}</div>` +
    `<div class="detail" style="margin-top:6px">Edges (${edges.length}):</div>` +
    edges.map(ed => `<div class="detail">  → ${ed.edgeCollection}</div>`).join("");
  tooltip.classList.add("visible");
}).on("mousemove", e => {
  tooltip.style.left = (e.pageX + 12) + "px";
  tooltip.style.top = (e.pageY - 12) + "px";
}).on("mouseout", () => tooltip.classList.remove("visible"));

function arcPath(d) {
  const dx = d.target.x - d.source.x, dy = d.target.y - d.source.y;
  const sameTarget = graphData.links.filter(l =>
    ((l.source.id||l.source)===(d.source.id||d.source) && (l.target.id||l.target)===(d.target.id||d.target)) ||
    ((l.source.id||l.source)===(d.target.id||d.target) && (l.target.id||l.target)===(d.source.id||d.source))
  );
  const idx = sameTarget.indexOf(d);
  if (sameTarget.length <= 1) {
    return `M${d.source.x},${d.source.y}L${d.target.x},${d.target.y}`;
  }
  const dr = Math.sqrt(dx*dx + dy*dy) * (0.8 + idx * 0.6);
  const sweep = idx % 2;
  return `M${d.source.x},${d.source.y}A${dr},${dr} 0 0,${sweep} ${d.target.x},${d.target.y}`;
}

simulation.on("tick", () => {
  link.attr("d", arcPath);
  node.attr("transform", d => `translate(${d.x},${d.y})`);
  linkLabel.each(function(d) {
    const mx = (d.source.x + d.target.x) / 2;
    const my = (d.source.y + d.target.y) / 2;
    d3.select(this).attr("x", mx).attr("y", my);
  });
  linkLabelBg.each(function(d, i) {
    const textEl = linkLabel.nodes()[i];
    if (!textEl) return;
    const bbox = textEl.getBBox();
    d3.select(this)
      .attr("x", bbox.x - 3).attr("y", bbox.y - 1)
      .attr("width", bbox.width + 6).attr("height", bbox.height + 2);
  });
});

// --- Tables ---
const tablesGrid = document.getElementById("tables-grid");
tablesData.forEach(t => {
  let html = `<div class="table-card">
    <div class="table-card-header">
      <h3>${t.name}</h3>
      <span class="arrow">→</span>
      <span class="target">${t.targetCollection}</span>
      ${t.isJoinTable ? '<span class="badge badge-join">join</span>' : ''}
    </div>`;
  t.columns.forEach(c => {
    const bgColor = typeBadgeColors[c.type] || "#475569";
    html += `<div class="col-row">
      ${c.isPk ? '<span class="col-badge pk-badge">PK</span>' : ''}
      ${c.isFk ? '<span class="col-badge fk-badge">FK</span>' : ''}
      <span class="col-name">${c.name}</span>
      <span class="col-type" style="background:${bgColor}">${c.type}</span>
      ${c.nullable ? '<span class="nullable-badge">NULL</span>' : ''}
      ${c.isFk ? `<span class="fk-target">→ ${c.fkTarget}</span>` : ''}
    </div>`;
  });
  html += '</div>';
  tablesGrid.innerHTML += html;
});

// --- Edges ---
const edgesTbody = document.getElementById("edges-tbody");
edgesData.forEach(e => {
  edgesTbody.innerHTML += `<tr>
    <td class="edge-name">${e.edgeCollection}</td>
    <td class="edge-from">${e.fromCollection}</td>
    <td class="edge-arrow">→</td>
    <td class="edge-to">${e.toCollection}</td>
    <td class="edge-field">${e.fromField}</td>
    <td>${e.toField}</td>
  </tr>`;
});
</script>
</body>
</html>
"""
