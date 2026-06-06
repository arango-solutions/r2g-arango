from __future__ import annotations

import logging
import re
import sys
from typing import Any, cast

import structlog
from structlog.processors import CallsiteParameter, CallsiteParameterAdder

# Event-dict keys whose string values are secrets and should never be logged
# verbatim, regardless of where they come from.
_SENSITIVE_KEY_RE = re.compile(
    r"(password|passwd|secret|token|api[_-]?key|connection[_-]?string|dsn|conn[_-]?str|pg[_-]?conn)",
    re.IGNORECASE,
)


def _redact_secrets(_logger: Any, _method: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """structlog processor: mask secret-named fields and scrub embedded DSNs.

    Runs before the renderer so no sink (stdout, JSON) ever sees plaintext
    credentials, including connection strings accidentally interpolated into
    error messages.
    """
    from r2g.security import redact_connection_string, redact_for_display, scrub_dsn_credentials

    for key, value in list(event_dict.items()):
        if not isinstance(value, str) or not value:
            continue
        if _SENSITIVE_KEY_RE.search(key):
            event_dict[key] = (
                redact_connection_string(value) if "://" in value else redact_for_display(value)
            )
        elif "://" in value:
            event_dict[key] = scrub_dsn_credentials(value)
    return event_dict


def setup_logging(level: str = "INFO", json_output: bool = False) -> None:
    log_level = getattr(logging, level.upper(), logging.INFO)
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        CallsiteParameterAdder(parameters=[CallsiteParameter.MODULE]),
        _redact_secrets,
    ]
    if json_output:
        shared_processors.append(structlog.processors.JSONRenderer())
    else:
        shared_processors.append(structlog.dev.ConsoleRenderer(colors=True))
    structlog.configure(
        processors=shared_processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    return cast(structlog.BoundLogger, structlog.get_logger(name))
