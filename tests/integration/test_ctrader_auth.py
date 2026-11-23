"""OAuth helpers and CLI plumbing for cTrader (no network: httpx mock transport + fake server)."""

from __future__ import annotations

import os
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr

from tests.integration.ctrader_fake import FakeCTraderServer
from trader.brokers.ctrader.auth_cli import extract_code, list_accounts
from trader.brokers.ctrader.oauth import TOKEN_URL, TokenSet, exchange_code
from trader.cli import main
from trader.config.secrets import read_env_file, update_env_file
from trader.domain.errors import AuthExpired, ConfigError

ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 1, 5, tzinfo=UTC)


def test_extract_code() -> None:
    assert extract_code("https://example.com/cb?code=AbC_123&state=x") == "AbC_123"
    assert extract_code("  AbC_123 \n") == "AbC_123"
    with pytest.raises(ConfigError):
        extract_code("")


async def test_exchange_code_success_and_error() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.params["code"] == "good":
            return httpx.Response(
                200,
                json={"accessToken": "a1", "tokenType": "bearer", "expiresIn": 2628000, "refreshToken": "r1",
                      "errorCode": None, "description": None},
            )  # fmt: skip
        return httpx.Response(200, json={"errorCode": "ACCESS_DENIED", "description": "bad code"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        tokens = await exchange_code(
            "good",
            client_id="cid",
            client_secret=SecretStr("cs"),
            redirect_uri="https://x/cb",
            now=NOW,
            client=client,
        )
        assert tokens.access_token.get_secret_value() == "a1"
        assert tokens.expires_at == NOW + timedelta(seconds=2628000)
        with pytest.raises(AuthExpired, match="ACCESS_DENIED") as info:
            await exchange_code(
                "bad",
                client_id="cid",
                client_secret=SecretStr("cs"),
                redirect_uri="https://x/cb",
                now=NOW,
                client=client,
            )
        assert "cs" not in str(info.value)
    req = seen[0]
    assert str(req.url).startswith(TOKEN_URL)
    assert req.method == "GET"
    assert req.url.params["grant_type"] == "authorization_code"
    assert req.url.params["redirect_uri"] == "https://x/cb"


async def test_list_accounts_against_fake_server() -> None:
    server = FakeCTraderServer()
    port = await server.start()
    tokens = TokenSet(SecretStr(server.access_token), SecretStr(server.refresh_token), None)
    accounts = await list_accounts(
        tokens,
        client_id=server.client_id,
        client_secret=SecretStr(server.client_secret),
        host="127.0.0.1",
        port=port,
    )
    await server.stop()
    assert [(a.ctid_trader_account_id, a.is_live) for a in accounts] == [(1001, False), (1002, True)]


def test_update_env_file_replaces_appends_and_restricts(tmp_path: Path) -> None:
    f = tmp_path / ".env"
    f.write_text("# keys\nCTRADER_DEMO_CLIENT_ID=abc\nCTRADER_DEMO_ACCESS_TOKEN=old\n")
    update_env_file(f, {"CTRADER_DEMO_ACCESS_TOKEN": "new", "CTRADER_DEMO_REFRESH_TOKEN": "r"})
    assert read_env_file(f) == {
        "CTRADER_DEMO_CLIENT_ID": "abc",
        "CTRADER_DEMO_ACCESS_TOKEN": "new",
        "CTRADER_DEMO_REFRESH_TOKEN": "r",
    }
    assert f.read_text().startswith("# keys\n")
    assert stat.S_IMODE(os.stat(f).st_mode) == 0o600


def test_trader_mode_from_env_file_does_not_unlock_live(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("TRADER_MODE=live\n")
    env_file.chmod(0o600)
    config = str(ROOT / "config/live.yaml.example")
    assert main(["--env-file", str(env_file), "run", "--config", config, "--confirm-live"], env={}) == 2
    assert "TRADER_MODE=live" in capsys.readouterr().err


def test_secrets_key_command(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    env_file = tmp_path / ".env"
    assert main(["--env-file", str(env_file), "secrets-key"], env={}) == 0
    key = read_env_file(env_file)["TRADER_SECRETS_KEY"]
    assert len(key) == 44
    assert key not in capsys.readouterr().out  # never printed
    assert main(["--env-file", str(env_file), "secrets-key"], env={}) == 0
    assert read_env_file(env_file)["TRADER_SECRETS_KEY"] == key  # not rotated


def test_ctrader_auth_requires_app_credentials(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--env-file", str(tmp_path / ".env"), "ctrader-auth"], env={}) == 2
    assert "CTRADER_DEMO_CLIENT_ID" in capsys.readouterr().err


def test_a_dotenv_others_can_read_is_refused(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("TRADER_API_TOKEN=x\n")
    env_file.chmod(0o640)
    assert (
        main(
            ["--env-file", str(env_file), "check-config", "--config", str(ROOT / "config/paper.yaml")], env={}
        )
        == 2
    )
    assert "run `chmod 600" in capsys.readouterr().err
