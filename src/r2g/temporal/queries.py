"""Point-in-time AQL query templates for temporal graph mode (P5.7).

These are pure string builders: given collection names they return
parameterized AQL covering the common temporal operations. Callers supply
bind values (``@t``, ``@start``, ``@end``) at execution time. The interval
convention is half-open ``[created, expired)`` so a snapshot at ``T`` matches
``created <= T AND expired > T``.
"""

from __future__ import annotations

from r2g.temporal.models import (
    FIELD_CREATED,
    FIELD_EXPIRED,
    FIELD_VERSION,
    TemporalConfig,
    TemporalNaming,
)


def snapshot_at(entity_collection: str) -> str:
    """Entities live at instant ``@t``."""
    return (
        f"FOR e IN {entity_collection} "
        f"FILTER e.{FIELD_CREATED} <= @t AND e.{FIELD_EXPIRED} > @t "
        "RETURN e"
    )


def version_history(
    entity_collection: str,
    proxy_key: str,
    config: TemporalConfig | None = None,
) -> str:
    """All versions of one entity, newest first, traversed via its ProxyIn.

    Binds: ``@pk`` (proxy key). Uses the ``hasVersion`` edge collection.
    """
    naming = TemporalNaming(config)
    proxy_in = naming.proxy_in(entity_collection)
    return (
        f"FOR v, e IN 1..1 OUTBOUND @pk {naming.has_version} "
        f"FILTER IS_SAME_COLLECTION('{entity_collection}', v) "
        f"SORT v.{FIELD_VERSION} DESC "
        "RETURN v"
    ).replace("@pk", f"'{proxy_in}/{proxy_key}'")


def interval_overlap(entity_collection: str) -> str:
    """Entities whose validity interval overlaps ``[@start, @end]``.

    Two intervals overlap when ``created <= @end AND expired >= @start``.
    """
    return (
        f"FOR e IN {entity_collection} "
        f"FILTER e.{FIELD_CREATED} <= @end AND e.{FIELD_EXPIRED} >= @start "
        "RETURN e"
    )


def changed_between(entity_collection: str) -> str:
    """Versions that began or ended within ``(@t1, @t2]`` -- "what changed".

    Returns each matching entity tagged with whether it was created or expired
    in the window.
    """
    return (
        f"FOR e IN {entity_collection} "
        f"FILTER (e.{FIELD_CREATED} > @t1 AND e.{FIELD_CREATED} <= @t2) "
        f"OR (e.{FIELD_EXPIRED} > @t1 AND e.{FIELD_EXPIRED} <= @t2) "
        "RETURN { "
        "entity: e, "
        f"created_in_window: e.{FIELD_CREATED} > @t1 AND e.{FIELD_CREATED} <= @t2, "
        f"expired_in_window: e.{FIELD_EXPIRED} > @t1 AND e.{FIELD_EXPIRED} <= @t2 "
        "}"
    )


def current_version(entity_collection: str) -> str:
    """The single live version of each entity (``expired`` is the sentinel)."""
    return (
        f"FOR e IN {entity_collection} "
        f"FILTER e.{FIELD_EXPIRED} >= @never "
        "RETURN e"
    )


def all_templates(entity_collection: str) -> dict[str, str]:
    """Return every template keyed by name (handy for the UI / docs)."""
    return {
        "snapshot_at": snapshot_at(entity_collection),
        "interval_overlap": interval_overlap(entity_collection),
        "changed_between": changed_between(entity_collection),
        "current_version": current_version(entity_collection),
    }
