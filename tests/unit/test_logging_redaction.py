from __future__ import annotations

import json
import logging

import pytest
import structlog
from pydantic import SecretStr

from trader.observability.logging import REDACTED, configure_logging, get_logger, redact, redaction_processor


@pytest.fixture(autouse=True)
def _reset_structlog() -> object:
    yield
    structlog.reset_defaults()
    logging.getLogger().handlers.clear()


@pytest.mark.parametrize(
    "key",
    [
        "api_key",
        "API-KEY",
        "secret",
        "client_secret",
        "access_token",
        "refresh_token",
        "password",
        "private_key",
        "Authorization",
        "signature",
    ],
)
def test_sensitive_keys_redacted(key: str) -> None:
    out = redaction_processor(None, "info", {key: "value123", "symbol": "EURUSD"})
    assert out[key] == REDACTED
    assert out["symbol"] == "EURUSD"


def test_nested_and_value_patterns() -> None:
    payload = {
        "headers": {"Authorization": "Bearer abcdefghijkl", "x": "1"},
        "items": [{"token": "t"}, "Bearer zzzzzzzzzzzz trailing"],
        "jwt": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abcdefghijklmnop",
        "pem": "-----BEGIN PRIVATE KEY-----\nMIIE\n-----END PRIVATE KEY-----",
        "s": SecretStr("hunter2"),
    }
    out = redact(payload)
    text = repr(out)
    for leaked in ("abcdefghijkl", "zzzzzzzzzzzz", "eyJhbGci", "MIIE", "hunter2"):
        assert leaked not in text
    assert isinstance(out, dict)
    assert out["headers"]["x"] == "1"
    assert out["items"][1].endswith("trailing")


def test_configured_logger_output_is_redacted(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", json=True)
    get_logger("t").info("auth", api_key="sk-live-123", detail="Bearer qwertyuiopasdf", symbol="EURUSD")
    logging.getLogger("stdlib").warning("header Bearer qwertyuiopasdf")
    err = capsys.readouterr().err
    assert "sk-live-123" not in err
    assert "qwertyuiopasdf" not in err
    first = json.loads(err.splitlines()[0])
    assert first["api_key"] == REDACTED
    assert first["symbol"] == "EURUSD"


def test_secret_query_parameters_are_redacted_in_any_handler(caplog: pytest.LogCaptureFixture) -> None:
    """httpx logs request URLs; cTrader's OAuth token call sends client_secret and the code as GET params."""
    import logging

    url = "https://openapi.ctrader.com/apps/token?grant_type=authorization_code&code=abc123&client_id=42&client_secret=s3cr3t"
    with caplog.at_level(logging.INFO, logger="httpx"):
        logging.getLogger("httpx").info("HTTP Request: GET %s", url)
    message = caplog.records[0].getMessage()
    assert "s3cr3t" not in message
    assert "abc123" not in message
    assert "client_secret=***REDACTED***" in message
    assert "client_id=42" in message  # not a secret: kept
