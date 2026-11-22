"""cTrader OAuth 2.0 tokens (https://help.ctrader.com/open-api/account-authentication/).

- Authorisation code: valid ~1 minute, exchanged at ``GET https://openapi.ctrader.com/apps/token``.
- Access token: ``expiresIn`` seconds (2,628,000 ≈ 30 days by default).
- Refresh token: no expiry, but refreshing returns a NEW refresh token, so the pair must be persisted.
  Refresh is done over the Protobuf connection with ``ProtoOARefreshTokenReq`` (see adapter); the REST
  endpoint's refresh parameters are not documented, so it is used only for the code exchange.

Tokens are stored encrypted at rest (Fernet, key from ``TRADER_SECRETS_KEY``) in a 0600 file.
Values are ``SecretStr`` in memory and never logged.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode

import httpx
from cryptography.fernet import Fernet, InvalidToken
from pydantic import SecretStr

from trader.domain.errors import AuthExpired, ConfigError
from trader.domain.time import ensure_utc, from_epoch_ms, to_epoch_ms

TOKEN_URL = "https://openapi.ctrader.com/apps/token"
AUTHORIZE_URL = "https://id.ctrader.com/my/settings/openapi/grantingaccess/"
SECRETS_KEY_ENV = "TRADER_SECRETS_KEY"


@dataclass(frozen=True, slots=True)
class TokenSet:
    access_token: SecretStr
    refresh_token: SecretStr
    expires_at: datetime | None  # None = unknown (bootstrapped from env): refresh on first connect

    def expires_within(self, now: datetime, margin: timedelta) -> bool:
        return self.expires_at is None or self.expires_at - now <= margin

    @classmethod
    def from_response(cls, access: str, refresh: str, expires_in_s: int, now: datetime) -> TokenSet:
        return cls(SecretStr(access), SecretStr(refresh), ensure_utc(now) + timedelta(seconds=expires_in_s))


def authorize_url(client_id: str, redirect_uri: str, scope: str = "trading") -> str:
    """URL the user opens to grant access; the browser is redirected to ``redirect_uri?code=...``."""
    query = urlencode(
        {"client_id": client_id, "redirect_uri": redirect_uri, "scope": scope, "product": "web"}
    )
    return f"{AUTHORIZE_URL}?{query}"


async def _token_call(params: dict[str, str], now: datetime, client: httpx.AsyncClient | None) -> TokenSet:
    owns = client is None
    http = client or httpx.AsyncClient(timeout=20)
    try:
        resp = await http.get(TOKEN_URL, params=params, headers={"Accept": "application/json"})
    finally:
        if owns:
            await http.aclose()
    try:
        data = resp.json()
    except ValueError as exc:
        raise AuthExpired(f"token endpoint returned HTTP {resp.status_code} (not JSON)") from exc
    if resp.status_code != 200 or data.get("errorCode"):
        # The description never contains the secret; still, keep only code + description.
        raise AuthExpired(
            f"token request failed: {data.get('errorCode')} {data.get('description') or ''}".strip()
        )
    return TokenSet.from_response(data["accessToken"], data["refreshToken"], int(data["expiresIn"]), now)


async def exchange_code(
    code: str,
    *,
    client_id: str,
    client_secret: SecretStr,
    redirect_uri: str,
    now: datetime,
    client: httpx.AsyncClient | None = None,
) -> TokenSet:
    params = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "client_secret": client_secret.get_secret_value(),
    }
    return await _token_call(params, now, client)


class TokenStore:
    """Encrypted, 0600 token file. Without a key (``TRADER_SECRETS_KEY``) tokens stay in memory only."""

    def __init__(self, path: Path, key: SecretStr | None) -> None:
        self._path = path
        self._fernet: Fernet | None = None
        if key is not None:
            try:
                self._fernet = Fernet(key.get_secret_value().encode())
            except (ValueError, TypeError) as exc:
                raise ConfigError(f"{SECRETS_KEY_ENV} is not a valid Fernet key") from exc

    @property
    def persistent(self) -> bool:
        return self._fernet is not None

    @staticmethod
    def generate_key() -> str:
        return Fernet.generate_key().decode()

    def load(self) -> TokenSet | None:
        if self._fernet is None or not self._path.exists():
            return None
        try:
            raw = json.loads(self._fernet.decrypt(self._path.read_bytes()))
        except (InvalidToken, ValueError) as exc:
            raise ConfigError(f"cannot decrypt {self._path} (wrong {SECRETS_KEY_ENV}?)") from exc
        expires = raw.get("expires_at_ms")
        return TokenSet(
            SecretStr(raw["access_token"]),
            SecretStr(raw["refresh_token"]),
            from_epoch_ms(int(expires)) if expires is not None else None,
        )

    def save(self, tokens: TokenSet) -> bool:
        """Persist ``tokens``; returns False when no key is configured."""
        if self._fernet is None:
            return False
        payload = {
            "access_token": tokens.access_token.get_secret_value(),
            "refresh_token": tokens.refresh_token.get_secret_value(),
            "expires_at_ms": to_epoch_ms(tokens.expires_at) if tokens.expires_at else None,
        }
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "wb") as fh:
            fh.write(self._fernet.encrypt(json.dumps(payload).encode()))
        os.replace(tmp, self._path)
        os.chmod(self._path, 0o600)
        return True
