from __future__ import annotations

from pathlib import Path

import pytest

from trader.cli import main
from trader.config.loader import load_config, resolve_mode
from trader.config.secrets import load_secrets
from trader.config.settings import AppConfig
from trader.domain.enums import TradingMode
from trader.domain.errors import ConfigError, LiveTradingNotConfirmed

ROOT = Path(__file__).resolve().parents[2]

LIVE = {
    "mode": "live",
    "accounts": [
        {
            "id": "c-live",
            "broker": "ctrader",
            "environment": "live",
            "account_id": "1",
            "secrets_ref": "CT_LIVE",
        }
    ],
}


def cfg(data: dict[str, object]) -> AppConfig:
    return AppConfig.model_validate(data)


@pytest.mark.parametrize("name", ["paper.yaml", "demo.yaml", "live.yaml.example"])
def test_shipped_configs_are_valid(name: str) -> None:
    load_config(ROOT / "config" / name)


def test_default_mode_is_paper() -> None:
    assert AppConfig().mode is TradingMode.PAPER
    assert resolve_mode(AppConfig(), {}, confirm_live=False) is TradingMode.PAPER


@pytest.mark.parametrize(
    ("env", "confirm"),
    [({}, False), ({}, True), ({"TRADER_MODE": "live"}, False)],
)
def test_live_requires_env_and_flag(env: dict[str, str], confirm: bool) -> None:
    with pytest.raises(LiveTradingNotConfirmed):
        resolve_mode(cfg(LIVE), env, confirm_live=confirm)


def test_live_allowed_with_both() -> None:
    assert resolve_mode(cfg(LIVE), {"TRADER_MODE": "live"}, confirm_live=True) is TradingMode.LIVE


def test_env_mode_must_match_config() -> None:
    with pytest.raises(ConfigError, match="disagrees"):
        resolve_mode(AppConfig(), {"TRADER_MODE": "live"}, confirm_live=True)
    with pytest.raises(ConfigError, match="not one of"):
        resolve_mode(AppConfig(), {"TRADER_MODE": "yolo"}, confirm_live=False)


def test_confirm_live_without_live_mode_is_an_error() -> None:
    with pytest.raises(ConfigError, match="confirm-live"):
        resolve_mode(AppConfig(), {}, confirm_live=True)


def test_live_accounts_forbidden_outside_live_mode() -> None:
    data = {**LIVE, "mode": "demo"}
    with pytest.raises(ConfigError, match="live accounts"):
        resolve_mode(cfg(data), {}, confirm_live=False)


def test_paper_mode_only_trades_paper_accounts() -> None:
    demo_acc = {"id": "ct", "broker": "ctrader", "account_id": "1", "secrets_ref": "CT_DEMO"}
    with pytest.raises(ConfigError, match="paper mode"):
        resolve_mode(cfg({"accounts": [demo_acc]}), {}, confirm_live=False)
    data = {"accounts": [demo_acc, {"id": "p", "broker": "paper", "paper": {"market_data_from": "ct"}}]}
    assert resolve_mode(cfg(data), {}, confirm_live=False) is TradingMode.PAPER


@pytest.mark.parametrize(
    ("account", "match"),
    [
        ({"id": "x", "broker": "ctrader", "account_id": "1"}, "secrets_ref"),
        ({"id": "x", "broker": "ctrader", "secrets_ref": "CT"}, "account_id"),
        ({"id": "x", "broker": "paper", "environment": "live"}, "paper"),
        ({"id": "x", "broker": "paper", "symbols": ["EURUSD", "EURUSD"]}, "duplicate"),
        ({"id": "x", "broker": "paper", "symbols": ["eur usd"]}, "symbols"),
        ({"id": "x", "broker": "paper", "api_key": "oops"}, "Extra inputs"),
        ({"id": "x", "broker": "ctrader", "account_id": "1", "secrets_ref": "lower"}, "secrets_ref"),
    ],
)
def test_account_validation(account: dict[str, object], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        cfg({"accounts": [account]})


def test_duplicate_and_dangling_account_refs() -> None:
    with pytest.raises(ValueError, match="unique"):
        cfg({"accounts": [{"id": "p", "broker": "paper"}, {"id": "p", "broker": "paper"}]})
    with pytest.raises(ValueError, match="market_data_from"):
        cfg({"accounts": [{"id": "p", "broker": "paper", "paper": {"market_data_from": "nope"}}]})


def test_load_config_errors(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="cannot read"):
        load_config(tmp_path / "missing.yaml")
    bad = tmp_path / "bad.yaml"
    bad.write_text("- just\n- a list\n")
    with pytest.raises(ConfigError, match="mapping"):
        load_config(bad)
    bad.write_text("mode: sideways\n")
    with pytest.raises(ConfigError, match="invalid config"):
        load_config(bad)


def test_load_secrets() -> None:
    env = {"CT_DEMO_CLIENT_ID": "id", "CT_DEMO_CLIENT_SECRET": "s3cret", "OTHER_X": "y", "CT_DEMO_EMPTY": ""}
    secrets = load_secrets("CT_DEMO", env, required=("client_id", "client_secret"))
    assert set(secrets) == {"client_id", "client_secret"}
    assert secrets["client_secret"].get_secret_value() == "s3cret"
    assert "s3cret" not in repr(secrets)
    with pytest.raises(ConfigError, match="CT_DEMO_ACCESS_TOKEN") as info:
        load_secrets("CT_DEMO", env, required=("access_token",))
    assert "s3cret" not in str(info.value)


# ------------------------------------------------------------------ CLI


def test_cli_check_config_paper(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["check-config", "--config", str(ROOT / "config/paper.yaml")], env={}) == 0
    assert "mode=paper" in capsys.readouterr().out


def test_cli_refuses_live_without_confirmation(capsys: pytest.CaptureFixture[str]) -> None:
    path = str(ROOT / "config/live.yaml.example")
    assert main(["run", "--config", path], env={"TRADER_MODE": "live"}) == 2
    assert "confirm-live" in capsys.readouterr().err
    assert main(["run", "--config", path, "--confirm-live"], env={}) == 2


def test_cli_live_banner_when_confirmed(capsys: pytest.CaptureFixture[str]) -> None:
    path = str(ROOT / "config/live.yaml.example")
    assert main(["run", "--config", path, "--confirm-live"], env={"TRADER_MODE": "live"}) == 0
    assert "LIVE TRADING" in capsys.readouterr().err


def test_live_example_spells_out_exactly_the_risk_defaults() -> None:
    """config/live.yaml.example documents every risk default; this keeps them in sync (and makes any change
    to a default visible in review)."""
    from trader.config.risk import RiskConfig, RiskLimits

    config = load_config(ROOT / "config/live.yaml.example")
    assert config.accounts[0].risk == RiskConfig()
    assert set(config.accounts[0].risk.limits.model_fields_set) == set(RiskLimits.model_fields)
