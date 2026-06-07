"""ArangoDB document-key sanitization.

ArangoDB restricts the characters allowed in ``_key`` (and therefore in the
key portion of ``_from`` / ``_to``). Source primary-key values are not
guaranteed to satisfy those rules: a partitioned PostgreSQL table whose PK
includes a ``timestamp`` column (e.g. Pagila's ``payment``) yields keys
containing spaces and ``+``, which ArangoDB rejects with
``illegal document key``.

:func:`sanitize_key_component` maps an arbitrary value to a legal key fragment
by percent-encoding every disallowed byte. The transform is deterministic and
injective over arbitrary input strings (``%`` itself is always encoded, so
``"a b"`` and a literal ``"a%20b"`` cannot collide). Because the mapping is a
pure function of the value, a vertex ``_key`` and any ``_from`` / ``_to``
reference that points at it stay in agreement as long as both sides run their
PK / FK values through this function.
"""

from __future__ import annotations

# Characters ArangoDB permits in a document key (per the manual). ``%`` is
# permitted by ArangoDB but is deliberately excluded from the pass-through set
# because it is our escape character; encoding it keeps the mapping injective.
_PASSTHROUGH = frozenset(
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
    "_-:.@()+,=;$!*'"
)


def sanitize_key_component(value: object) -> str:
    """Return *value* as a string safe to embed in an ArangoDB document key.

    Disallowed characters are percent-encoded byte-by-byte (UTF-8), so the
    result contains only characters ArangoDB accepts in ``_key``.
    """
    text = value if isinstance(value, str) else str(value)
    out: list[str] = []
    for ch in text:
        if ch in _PASSTHROUGH:
            out.append(ch)
        else:
            for byte in ch.encode("utf-8"):
                out.append(f"%{byte:02X}")
    return "".join(out)
