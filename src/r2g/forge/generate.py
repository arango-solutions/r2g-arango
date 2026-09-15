"""The ADR-0006 D-4 seam: ``generate(ontology, dialect, seed) -> {ddl, loader, rows}``."""

from __future__ import annotations

from .core import ForgeArtifacts, ForgeError, ForgeOntology, plan_schema, synthesize_rows
from .dialects import get_dialect


def generate(
    ontology: ForgeOntology,
    dialect: str = "postgres",
    seed: int = 0,
    *,
    rows_per_entity: int = 10,
) -> ForgeArtifacts:
    """Ontology -> ``ForgeArtifacts(ddl, load_sql, rows)`` for one dialect.

    Deterministic end to end (PLAN F-5): the same ``(ontology, dialect, seed,
    rows_per_entity)`` reproduces byte-identical artifacts. The schema plan
    and the rows are dialect-independent — data is synthesized once and only
    *projected* per system (ADR-0006 D-2), so ``rows`` is identical across
    dialects for the same ontology and seed. Unknown dialects and refused
    ontologies raise :class:`~r2g.forge.core.ForgeError` before anything is
    rendered.
    """
    target = get_dialect(dialect)
    if rows_per_entity < 1:
        raise ForgeError("rows_per_entity must be >= 1")
    plan = plan_schema(ontology)
    rows = synthesize_rows(plan, seed, rows_per_entity)
    return ForgeArtifacts(
        dialect=target.name,
        seed=seed,
        ddl=target.render_ddl(plan),
        load_sql=target.render_loader(plan, rows, seed),
        rows=rows,
    )


__all__ = ["generate"]
