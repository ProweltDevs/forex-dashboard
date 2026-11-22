"""structlog setup with a secret-redaction processor. Keep the redaction processor: it is a safety rule."""

from __future__ import annotations

import logging
import re
import sys
from collections.abc import Mapping, MutableMapping
from typing import Any, TextIO, cast

import structlog
from pydantic import SecretBytes, SecretStr

REDACTED = "***REDACTED***"

_SENSITIVE_KEY = re.compile(
    r"(pass(word|wd)?|secret|token|api[_-]?key|private[_-]?key|authorization|auth[_-]?header|"
    r"credential|signature|cookie|session[_-]?id|client[_-]?secret)",
    re.IGNORECASE,
)
# Values that look like credentials even when the key name is innocent.
_SENSITIVE_VALUE = [
    re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.DOTALL),
    re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b"),  # JWT
    re.compile(r"\d{5,}:[A-Za-z0-9_-]{30,}"),  # Telegram bot token (also inside a /bot<token>/ URL)
]
_MAX_DEPTH = 8


def is_sensitive_key(key: str) -> bool:
    return bool(_SENSITIVE_KEY.search(key))


def _scrub_str(value: str) -> str:
    for pattern in _SENSITIVE_VALUE:
        value = pattern.sub(REDACTED, value)
    return value


def redact(value: object, depth: int = 0) -> object:
    """Recursively redact secrets in mappings, sequences and strings."""
    if depth > _MAX_DEPTH:
        return value
    if isinstance(value, SecretStr | SecretBytes):
        return REDACTED
    if isinstance(value, str):
        return _scrub_str(value)
    if isinstance(value, Mapping):
        return {
            k: REDACTED if isinstance(k, str) and is_sensitive_key(k) else redact(v, depth + 1)
            for k, v in value.items()
        }
    if isinstance(value, list | tuple):
        items = [redact(v, depth + 1) for v in value]
        return items if isinstance(value, list) else tuple(items)
    return value


def redaction_processor(
    _logger: object, _method: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    for key in list(event_dict):
        if is_sensitive_key(key):
            event_dict[key] = REDACTED
        else:
            event_dict[key] = redact(event_dict[key])
    return event_dict


class _CurrentStderr:
    """Writes to whatever ``sys.stderr`` is at write time, not at configuration time. A handler bound to the
    original object breaks once that stream is replaced and closed (test capture, daemonisation)."""

    def write(self, text: str) -> int:
        return sys.stderr.write(text)

    def flush(self) -> None:
        sys.stderr.flush()


_STDERR = _CurrentStderr()


def configure_logging(level: str = "INFO", *, json: bool = True) -> None:
    """Configure structlog (and route stdlib logging through it). Redaction always runs last
    before rendering, so it also covers exception text."""
    log_level = logging.getLevelNamesMapping()[level.upper()]
    renderer: structlog.typing.Processor = (
        structlog.processors.JSONRenderer() if json else structlog.dev.ConsoleRenderer(colors=False)
    )
    shared: list[structlog.typing.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        redaction_processor,
    ]
    structlog.configure(
        processors=[*shared, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(file=cast("TextIO", _STDERR)),
        cache_logger_on_first_use=False,
    )
    handler = logging.StreamHandler(_STDERR)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=shared,
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                renderer,
            ],
        )
    )
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(log_level)


def get_logger(name: str | None = None) -> structlog.typing.FilteringBoundLogger:
    logger: structlog.typing.FilteringBoundLogger = structlog.get_logger(name)
    return logger
