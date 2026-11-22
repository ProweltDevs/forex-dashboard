"""Guards for the deployment files: what must hold for the container setup to stay safe."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]


def compose() -> dict[str, Any]:
    data = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    return data


def test_published_ports_are_local_only() -> None:
    for name, service in compose()["services"].items():
        for port in service.get("ports", []):
            assert str(port).startswith("127.0.0.1:"), f"{name} publishes {port} beyond localhost"


def test_trader_mode_never_comes_from_dotenv() -> None:
    app = compose()["services"]["app"]
    assert app["env_file"][0]["path"] == ".env"  # .env becomes process environment ...
    assert app["environment"]["TRADER_MODE"] == ""  # ... so the live gate is pinned off here
    assert "--confirm-live" not in app["command"]
    assert "live" not in " ".join(app["command"])


def test_graceful_stop_has_time_and_the_right_signal() -> None:
    app = compose()["services"]["app"]
    assert app["stop_signal"] == "SIGTERM"
    seconds = int(str(app["stop_grace_period"]).rstrip("s"))
    assert seconds >= 30  # in-flight orders settle (10 s) + persistence flush
    assert app["restart"] == "unless-stopped"  # a restart is safe: startup reconciles first


def test_image_runs_unprivileged_and_checks_liveness() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "\nUSER trader\n" in dockerfile
    assert "/health/live" in dockerfile.split("HEALTHCHECK", 1)[1]
    assert "STOPSIGNAL SIGTERM" in dockerfile
    runtime = dockerfile.split("AS runtime", 1)[1]
    assert "build_ca" not in runtime  # a build CA never reaches the final image


def test_secrets_never_enter_the_build_context() -> None:
    lines = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
    ignore = [line for line in lines if line and not line.startswith("#")]
    assert ignore[0] == "*"  # deny by default, then allow only what the image needs
    assert "**/.env" in ignore
    assert "!tests/" not in ignore
