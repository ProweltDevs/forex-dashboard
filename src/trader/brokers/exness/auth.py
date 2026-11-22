"""Exness Trader API request signing (https://www.exness-api.com/authentication/request-signing).

Every request (REST and WebSocket handshake) carries six ``EXN-*`` headers. ``EXN-DATA`` is the base64url
(no padding) of a compact JSON payload; ``EXN-SIGN`` is the Ed25519 signature of those exact payload
bytes. The payload binds the API key, idempotency key, timestamp, method, exact path+query and the
SHA-256 of the exact body. Checked against the official test vector in tests.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import time
from collections.abc import Callable

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from pydantic import SecretStr

from trader.domain.errors import ConfigError

SIGN_VERSION = 1
_HEX_KEY = re.compile(r"[0-9a-fA-F]{128}")
_IDEMPOTENCY_KEY = re.compile(r"[A-Za-z0-9._~-]{5,128}")


def b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def body_hash(body: bytes) -> str:
    return b64url(hashlib.sha256(body).digest())


def load_signing_key(text: SecretStr) -> Ed25519PrivateKey:
    """Accept the two documented formats: 128 hex chars (32-byte seed + 32-byte public key, as issued by
    Key Management) or an unencrypted PKCS#8 PEM (self-generated key pair). The hex form's public half is
    checked against its seed. Error messages never include key material."""
    value = text.get_secret_value().strip().replace("\\n", "\n")
    if value.startswith("-----BEGIN PRIVATE KEY-----"):
        try:
            key = serialization.load_pem_private_key(value.encode(), password=None)
        except ValueError as exc:
            raise ConfigError("Exness private key: invalid PEM") from exc
        if not isinstance(key, Ed25519PrivateKey):
            raise ConfigError("Exness private key: PEM is not an Ed25519 key")
        return key
    if not _HEX_KEY.fullmatch(value):
        raise ConfigError("Exness private key: expected 128 hex characters or an unencrypted PKCS#8 PEM")
    raw = bytes.fromhex(value)
    key = Ed25519PrivateKey.from_private_bytes(raw[:32])
    public = key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    if public != raw[32:]:
        raise ConfigError("Exness private key: public half does not match its seed")
    return key


def is_valid_idempotency_key(key: str) -> bool:
    return bool(_IDEMPOTENCY_KEY.fullmatch(key))


class Signer:
    """Builds signed ``EXN-*`` headers. Keep one per API key; it is cheap and stateless."""

    def __init__(
        self, api_key: str, private_key: Ed25519PrivateKey, *, now_ms: Callable[[], int] | None = None
    ) -> None:
        if not api_key:
            raise ConfigError("Exness API key is empty")
        self._api_key = api_key
        self._key = private_key
        self._now_ms = now_ms or (lambda: time.time_ns() // 1_000_000)

    def headers(
        self, method: str, path_with_query: str, body: bytes = b"", idempotency_key: str = ""
    ) -> dict[str, str]:
        """Signed headers for one request. ``path_with_query`` must be byte-for-byte what is sent."""
        method = method.upper()
        if method == "GET":
            if idempotency_key:
                raise ValueError("signed GET requests must use an empty idempotency key")
        elif not is_valid_idempotency_key(idempotency_key):
            raise ValueError("mutating requests need an idempotency key of 5..128 chars [A-Za-z0-9._~-]")
        if not path_with_query.startswith("/"):
            raise ValueError("sign the path (and query), not a full URL")
        timestamp = self._now_ms()
        payload = json.dumps(
            {
                "api_key": self._api_key,
                "idempotency_key": idempotency_key,
                "timestamp": timestamp,
                "sign_version": SIGN_VERSION,
                "method": method,
                "path": path_with_query,
                "body_hash": body_hash(body),
            },
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        return {
            "EXN-API-KEY": self._api_key,
            "EXN-IDEMPOTENCY-KEY": idempotency_key,
            "EXN-TIMESTAMP": str(timestamp),
            "EXN-SIGN-VERSION": str(SIGN_VERSION),
            "EXN-DATA": b64url(payload),
            "EXN-SIGN": b64url(self._key.sign(payload)),
        }
