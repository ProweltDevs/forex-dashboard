"""Signed REST client for the Exness Trader API.

- The signed ``path`` must equal what is transmitted, so we build the query string ourselves (strict
  percent-encoding, no duplicates) and hand httpx a finished URL.
- Redirects are never followed: signed headers must not reach another host.
- Client-side limits come from ``GET /limits`` (per operation + account-wide, both "N per window");
  until they are loaded a conservative bootstrap limit applies.
- A mutating request that times out after being sent raises ``UnknownOrderState`` (never "failed");
  retrying with the same idempotency key is safe and returns the original operation.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from decimal import Decimal
from typing import Any
from urllib.parse import quote, urlencode, urlsplit

import httpx

from trader.brokers.base import RateLimit, RateLimits
from trader.brokers.ratelimit import SlidingWindow
from trader.domain.errors import BrokerError, BrokerUnavailable, ConfigError, RateLimited, UnknownOrderState
from trader.observability.logging import get_logger

from .auth import Signer
from .mapping import Json, map_error

_log = get_logger("trader.brokers.exness.rest")

USER_AGENT = "trader-platform/0.1"
_BOOTSTRAP = (5, Decimal(1))  # until /limits is loaded: 5 requests per second


def base_url(value: str, *, allow_insecure: bool = False) -> str:
    """Validate a Gateway/Access Point URL: https, host only (no credentials, path, query, fragment)."""
    url = urlsplit(value.strip())
    schemes = ("https", "http") if allow_insecure else ("https",)
    if (
        url.scheme not in schemes
        or not url.hostname
        or url.username
        or url.password
        or url.path not in ("", "/")
        or url.query
        or url.fragment
    ):
        raise ConfigError(f"expected an {'/'.join(schemes)} base URL without path or credentials: {value!r}")
    return f"{url.scheme}://{url.netloc}"


def build_query(params: Sequence[tuple[str, str]]) -> str:
    keys = [k for k, _ in params]
    if len(set(keys)) != len(keys):
        raise ValueError("duplicate query parameters are rejected by Exness")
    return urlencode(list(params), quote_via=quote, safe="")


class RequestLimits:
    """Sliding windows per REST operation id plus the account-wide window."""

    def __init__(self) -> None:
        self._global = SlidingWindow(*_BOOTSTRAP)
        self._ops: dict[str, SlidingWindow] = {}
        self._published: list[RateLimit] = []
        self.candles_per_request = 1000  # documented default; replaced by the limits endpoint
        self.history_range_days: int | None = None
        self.history_age_days: int | None = None

    def configure(self, payload: Json) -> None:
        rest = payload["limits"]["rest"]
        g = rest["global_account_rate"]
        self._global = SlidingWindow(int(g["limit"]), Decimal(str(g["window_seconds"])))
        published = [_rate("account", g)]
        self._ops = {}
        for m in rest.get("methods", []):
            op = m["api_operation_id"]
            self._ops[op] = SlidingWindow(
                int(m["rate_limit"]["limit"]), Decimal(str(m["rate_limit"]["window_seconds"]))
            )
            published.append(_rate(op, m["rate_limit"]))
            extra = m.get("limits") or {}
            if op == "getCandleHistory" and extra.get("max_candles_per_request"):
                self.candles_per_request = int(extra["max_candles_per_request"])
            if op == "getDealsHistory":
                self.history_range_days = extra.get("history_range_days")
                self.history_age_days = extra.get("history_age_days")
        self._published = published

    async def acquire(self, op: str) -> None:
        await self._global.acquire()
        window = self._ops.get(op)
        if window is not None:
            await window.acquire()

    def published(self) -> RateLimits:
        return RateLimits(buckets=tuple(self._published))


def _rate(name: str, raw: Json) -> RateLimit:
    limit, window = int(raw["limit"]), Decimal(str(raw["window_seconds"]))
    return RateLimit(name=name, rate_per_s=Decimal(limit) / window, burst=limit)


class RestClient:
    def __init__(
        self,
        base: str,
        signer: Signer,
        client: httpx.AsyncClient,
        limits: RequestLimits,
    ) -> None:
        self._base = base
        self._signer = signer
        self._client = client
        self._limits = limits

    @property
    def base(self) -> str:
        return self._base

    async def get(self, op: str, path: str, params: Sequence[tuple[str, str]] = ()) -> Json:
        return await self._request(op, "GET", path, params=params)

    async def send(
        self,
        op: str,
        method: str,
        path: str,
        *,
        idempotency_key: str,
        body: Json | None = None,
        params: Sequence[tuple[str, str]] = (),
    ) -> Json:
        return await self._request(
            op, method, path, params=params, body=body, idempotency_key=idempotency_key
        )

    async def _request(
        self,
        op: str,
        method: str,
        path: str,
        *,
        params: Sequence[tuple[str, str]] = (),
        body: Json | None = None,
        idempotency_key: str = "",
    ) -> Json:
        query = build_query(params)
        path_q = f"{path}?{query}" if query else path
        raw = (
            json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode() if body is not None else b""
        )
        headers = self._signer.headers(method, path_q, raw, idempotency_key)
        headers["User-Agent"] = USER_AGENT
        headers["Accept"] = "application/json"
        if body is not None:
            headers["Content-Type"] = "application/json"
        await self._limits.acquire(op)
        mutating = method != "GET"
        try:
            resp = await self._client.request(
                method, self._base + path_q, headers=headers, content=raw or None
            )
        except httpx.ConnectError as exc:  # nothing was sent
            raise BrokerUnavailable(f"{op}: cannot connect: {exc}") from exc
        except httpx.TimeoutException as exc:
            if mutating:
                raise UnknownOrderState(f"{op}: no response (sent with idempotency key)") from exc
            raise BrokerUnavailable(f"{op}: timeout") from exc
        except httpx.TransportError as exc:
            if mutating:
                raise UnknownOrderState(f"{op}: connection lost: {exc}") from exc
            raise BrokerUnavailable(f"{op}: {exc}") from exc
        return _parse(op, resp)


def _retry_after_ms(resp: httpx.Response, data: Json | None) -> int | None:
    raw: Any = resp.headers.get("Retry-After") or (data or {}).get("retry_after")
    try:
        return int(float(raw) * 1000) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _parse(op: str, resp: httpx.Response) -> Json:
    data: Json | None
    try:
        parsed = resp.json() if resp.content else {}
        data = parsed if isinstance(parsed, dict) else None
    except ValueError:
        data = None
    if 200 <= resp.status_code < 300:
        if data is None:
            raise BrokerUnavailable(f"{op}: HTTP {resp.status_code} with a non-JSON body")
        return data
    if data and isinstance(data.get("error_message"), str):
        err = map_error(data["error_message"], retry_after_ms=_retry_after_ms(resp, data))
        _log.info(
            "exness_rest_error",
            op=op,
            status=resp.status_code,
            code=data.get("code"),
            error=data["error_message"],
        )
        raise err
    if resp.status_code == 429:
        raise RateLimited(f"{op}: HTTP 429", retry_after_ms=_retry_after_ms(resp, data) or 1000)
    # HTML/plain-text errors come from an edge or Gateway layer, not the API (see troubleshooting guide).
    kind = resp.headers.get("content-type", "unknown content type")
    if resp.status_code in (502, 503, 504):
        raise BrokerUnavailable(f"{op}: HTTP {resp.status_code} ({kind})")
    raise BrokerError(f"{op}: HTTP {resp.status_code} ({kind}) without an API error body")
