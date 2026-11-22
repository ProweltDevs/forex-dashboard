"""Interactive OAuth helper: ``trader ctrader-auth``.

1. Opens nothing by itself: prints the cTrader authorisation URL for you to open in a browser.
2. You paste back the redirect URL (or just the ``code``); the code is valid for about one minute.
3. Exchanges it for access + refresh tokens, lists the trading accounts the token can access, and
   writes the tokens into your ``.env`` (0600). Tokens are never printed.
"""

from __future__ import annotations

import ssl
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from pydantic import SecretStr

from trader.config.secrets import update_env_file
from trader.domain.errors import ConfigError
from trader.domain.time import utc_now

from .oauth import TokenSet, authorize_url, exchange_code
from .proto import OpenApiMessages_pb2 as msgs
from .transport import Transport

HOSTS = {"demo": "demo.ctraderapi.com", "live": "live.ctraderapi.com"}


@dataclass(frozen=True, slots=True)
class TradingAccount:
    ctid_trader_account_id: int
    is_live: bool
    trader_login: int


def extract_code(pasted: str) -> str:
    """Accept either the full redirect URL (``...?code=XYZ``) or the bare code."""
    pasted = pasted.strip()
    if "code=" in pasted:
        codes = parse_qs(urlsplit(pasted).query).get("code") or parse_qs(pasted.split("?", 1)[-1]).get("code")
        if not codes:
            raise ConfigError("no code= parameter found in the pasted URL")
        return codes[0]
    if not pasted:
        raise ConfigError("nothing pasted")
    return pasted


async def list_accounts(
    tokens: TokenSet,
    *,
    client_id: str,
    client_secret: SecretStr,
    host: str,
    port: int = 5035,
    ssl_context: ssl.SSLContext | None = None,
    proxy: str | None = None,
) -> list[TradingAccount]:
    transport = Transport(
        host, port, on_message=lambda m: None, on_closed=lambda e: None, ssl_context=ssl_context, proxy=proxy
    )
    await transport.connect()
    try:
        await transport.request(
            msgs.ProtoOAApplicationAuthReq(clientId=client_id, clientSecret=client_secret.get_secret_value()),
            msgs.ProtoOAApplicationAuthRes,
        )
        res = await transport.request(
            msgs.ProtoOAGetAccountListByAccessTokenReq(accessToken=tokens.access_token.get_secret_value()),
            msgs.ProtoOAGetAccountListByAccessTokenRes,
        )
    finally:
        await transport.close()
    return [TradingAccount(a.ctidTraderAccountId, a.isLive, a.traderLogin) for a in res.ctidTraderAccount]


async def run_auth(
    *,
    env: Mapping[str, str],
    env_file: Path,
    secrets_ref: str,
    environment: str,
    redirect_uri: str | None,
    prompt: Callable[[str], str],
    out: Callable[[str], None],
    proxy: str | None = None,
) -> list[TradingAccount]:
    client_id = env.get(f"{secrets_ref}_CLIENT_ID")
    client_secret = env.get(f"{secrets_ref}_CLIENT_SECRET")
    redirect = redirect_uri or env.get(f"{secrets_ref}_REDIRECT_URI")
    missing = [
        name
        for name, value in (
            (f"{secrets_ref}_CLIENT_ID", client_id),
            (f"{secrets_ref}_CLIENT_SECRET", client_secret),
            (f"{secrets_ref}_REDIRECT_URI", redirect),
        )
        if not value
    ]
    if missing or client_id is None or client_secret is None or redirect is None:
        raise ConfigError(f"set these in {env_file} first: {', '.join(missing)}")

    out("1. Open this URL in your browser, log in with your cTrader ID and allow access:\n")
    out(f"   {authorize_url(client_id, redirect)}\n")
    out("2. You will be redirected to your redirect URI. The code expires in about a minute.")
    code = extract_code(prompt("   Paste the full redirect URL (or just the code): "))
    tokens = await exchange_code(
        code,
        client_id=client_id,
        client_secret=SecretStr(client_secret),
        redirect_uri=redirect,
        now=utc_now(),
    )
    accounts = await list_accounts(
        tokens,
        client_id=client_id,
        client_secret=SecretStr(client_secret),
        host=HOSTS[environment],
        ssl_context=ssl.create_default_context(),
        proxy=proxy,
    )
    update_env_file(
        env_file,
        {
            f"{secrets_ref}_ACCESS_TOKEN": tokens.access_token.get_secret_value(),
            f"{secrets_ref}_REFRESH_TOKEN": tokens.refresh_token.get_secret_value(),
        },
    )
    out(f"\n3. Tokens saved to {env_file} (not shown). Accounts this token can trade:")
    wanted_live = environment == "live"
    for acc in accounts:
        marker = (
            "" if acc.is_live == wanted_live else f"   (on the {'live' if acc.is_live else 'demo'} server)"
        )
        out(
            f"   ctidTraderAccountId={acc.ctid_trader_account_id}  login={acc.trader_login}  "
            f"{'LIVE' if acc.is_live else 'demo'}{marker}"
        )
    out(
        f"\nPut the ctidTraderAccountId you want as `account_id:` in your config "
        f"(or {secrets_ref}_ACCOUNT_ID in .env for the demo contract tests)."
    )
    return accounts
