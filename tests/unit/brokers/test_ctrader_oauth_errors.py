"""cTrader OAuth token call: failures become domain errors without echoing the request URL, which holds
client_secret and the authorisation code."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest
from pydantic import SecretStr

from trader.brokers.ctrader.oauth import exchange_code
from trader.domain.errors import AuthExpired, BrokerUnavailable

NOW = datetime(2026, 3, 2, tzinfo=UTC)
SECRET = "client-secret-value"


async def call(handler: object) -> None:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))  # type: ignore[arg-type]
    try:
        await exchange_code(
            "auth-code", client_id="id", client_secret=SecretStr(SECRET), redirect_uri="https://x", now=NOW,
            client=client,
        )  # fmt: skip
    finally:
        await client.aclose()


async def test_network_errors_do_not_leak_the_url() -> None:
    def broken(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(f"cannot connect to {req.url}")

    with pytest.raises(BrokerUnavailable) as info:
        await call(broken)
    assert SECRET not in str(info.value)
    assert "auth-code" not in str(info.value)
    assert info.value.__cause__ is None  # not chained: a traceback would print the URL too


@pytest.mark.parametrize(
    ("body", "match"),
    [
        ([1, 2], "not a JSON object"),
        ({"accessToken": "a"}, "lacks accessToken"),
        ({"expiresIn": "x"}, "lacks"),
    ],
)
async def test_malformed_responses_are_auth_errors(body: object, match: str) -> None:
    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    with pytest.raises(AuthExpired, match=match):
        await call(handler)


def test_exness_settings_repr_hides_the_keys() -> None:
    from trader.brokers.exness import ExnessSettings

    s = ExnessSettings(
        account_id="ex", exness_account_id="1", gateway_url="https://gw", api_key="API-KEY-VALUE",
        private_key=SecretStr("PRIVATE"),
    )  # fmt: skip
    assert "API-KEY-VALUE" not in repr(s)
    assert "PRIVATE" not in repr(s)
