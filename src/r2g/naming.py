"""Identifier naming conventions for generated graph names.

R2G passes source table and column names through verbatim by default. This
module provides a small, dependency-free engine to re-case those identifiers
into a chosen convention (``snake_case``, ``camelCase``, ``PascalCase``) and to
apply a :class:`~r2g.types.NamingConvention` across a whole
:class:`~r2g.types.MappingConfig`.

Why materialize instead of transform-on-read? Keeping the converted names
directly in ``target_collection`` / ``field_mappings`` / ``edge_collection``
means the UI and downstream pipeline see exactly what will be written, and the
user can still hand-tweak any individual name afterwards.

Edge endpoints (``_from`` / ``_to``) are resolved from the *target* collection
names by the transformer layer, so renaming a collection here does not break
edge references as long as :class:`~r2g.types.EdgeDefinition.from_collection`
(a source-table key) is left untouched.
"""

from __future__ import annotations

import re

from r2g.types import (
    RESERVED_ATTRIBUTES,
    MappingConfig,
    NamingConvention,
    NameCase,
    Schema,
)

# Split an identifier into words. Handles snake_case, kebab-case, dotted, spaced,
# camelCase, PascalCase and acronym runs (e.g. "HTTPServer" -> ["HTTP", "Server"]).
_WORD_RE = re.compile(r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z0-9]+|[A-Z]+|[0-9]+")


def split_identifier(name: str) -> list[str]:
    """Break ``name`` into normalized word tokens (order preserved)."""
    words: list[str] = []
    for part in re.split(r"[^A-Za-z0-9]+", name):
        if part:
            words.extend(_WORD_RE.findall(part))
    return [w for w in words if w]


def pluralize(word: str) -> str:
    """Best-effort English plural for table / relationship name heuristics.

    Intentionally simple (no irregular-noun table): ``y`` after a consonant →
    ``ies``; sibilant endings (``s``/``x``/``z``/``ch``/``sh``) → ``es``; else
    append ``s``. Used only for fuzzy name matching, never for stored names.
    """
    if not word:
        return word
    if word.endswith("y") and len(word) > 1 and word[-2] not in "aeiou":
        return word[:-1] + "ies"
    if word.endswith(("s", "x", "z", "ch", "sh")):
        return word + "es"
    return word + "s"


def singularize(word: str) -> str:
    """Best-effort English singular, the inverse of :func:`pluralize`.

    ``ies`` → ``y``; ``ses``/``ches``/``shes``/``xes``/``zes`` → drop ``es``; a
    trailing ``s`` (but not ``ss``) → drop ``s``. Used only for fuzzy name
    matching (e.g. ``orders`` ↔ ``order``), never for stored names.
    """
    if not word:
        return word
    if word.endswith("ies") and len(word) > 3:
        return word[:-3] + "y"
    if word.endswith(("ses", "ches", "shes", "xes", "zes")):
        return word[:-2]
    if word.endswith("s") and not word.endswith("ss"):
        return word[:-1]
    return word


def convert_identifier(name: str, style: NameCase) -> str:
    """Re-case ``name`` into ``style``.

    Returns ``name`` unchanged when ``style`` is ``"preserve"`` or when the
    identifier yields no word tokens (e.g. it is empty or all punctuation).
    """
    if style == "preserve" or not name:
        return name
    words = split_identifier(name)
    if not words:
        return name
    lower = [w.lower() for w in words]
    if style == "snake":
        return "_".join(lower)
    if style == "pascal":
        return "".join(w.capitalize() for w in lower)
    if style == "camel":
        return lower[0] + "".join(w.capitalize() for w in lower[1:])
    return name


def apply_naming_convention(
    config: MappingConfig,
    convention: NamingConvention,
    schema: Schema | None = None,
) -> MappingConfig:
    """Return a copy of ``config`` with ``convention`` materialized into names.

    - **Collections**: ``target_collection`` is re-cased.
    - **Properties**: every source column gets a ``field_mappings`` entry mapping
      it to its re-cased property name (when a ``schema`` is provided so columns
      are known). Existing manual renames are preserved. ``field_expressions``
      targets are re-cased too. System fields (``_key`` etc.) are never touched.
    - **Edges**: ``edge_collection`` is re-cased. ``from_collection`` /
      ``to_collection`` are left as source-table keys (the transformer resolves
      edge endpoints from the re-cased ``target_collection``).
    """
    new = config.model_copy(deep=True)

    for cm in new.collections.values():
        if convention.collections != "preserve":
            cm.target_collection = convert_identifier(cm.target_collection, convention.collections)

        if convention.properties != "preserve":
            if schema is not None:
                table = schema.tables.get(cm.source_table)
                if table is not None:
                    for col in table.columns:
                        cname = col.name
                        if cname in RESERVED_ATTRIBUTES or cname in cm.field_mappings:
                            continue
                        converted = convert_identifier(cname, convention.properties)
                        # Never rename to a reserved attribute, and skip no-ops.
                        if converted != cname and converted not in RESERVED_ATTRIBUTES:
                            cm.field_mappings[cname] = converted
            else:
                # No schema available: re-case the targets of explicit renames only.
                cm.field_mappings = {
                    src: convert_identifier(tgt, convention.properties)
                    for src, tgt in cm.field_mappings.items()
                    if convert_identifier(tgt, convention.properties) not in RESERVED_ATTRIBUTES
                }
            for fx in cm.field_expressions:
                if fx.target not in RESERVED_ATTRIBUTES:
                    fx.target = convert_identifier(fx.target, convention.properties)

    if convention.edges != "preserve":
        for edge in new.edges:
            edge.edge_collection = convert_identifier(edge.edge_collection, convention.edges)

    new.naming_convention = convention
    return new
