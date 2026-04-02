from __future__ import annotations

import logging
import sys
from typing import Any, cast

import structlog
from structlog.processors import CallsiteParameter, CallsiteParameterAdder


def setup_logging(level: str = "INFO", json_output: bool = False) -> None:
    log_level = getattr(logging, level.upper(), logging.INFO)
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        CallsiteParameterAdder(parameters=[CallsiteParameter.MODULE]),
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
