"""Identifier helpers. The OMS generates a ULID ``client_order_id`` for every order; adapters pass it
to the broker as its idempotency key / client order ID, and retries reuse it."""

from __future__ import annotations

import re

from ulid import ULID

# Exness idempotency keys must be 5-128 chars; ULIDs are always 26 Crockford base32 chars.
_ULID_RE = re.compile(r"^[0-9A-HJKMNP-TV-Z]{26}$")


def new_client_order_id() -> str:
    return str(ULID())


def is_client_order_id(value: str) -> bool:
    return bool(_ULID_RE.fullmatch(value))
