"""Read account secrets from the environment by prefix. Values are wrapped in ``SecretStr`` so they
cannot leak through ``repr``/logs by accident."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from pydantic import SecretStr

from trader.domain.errors import ConfigError


def load_secrets(ref: str, env: Mapping[str, str], required: tuple[str, ...] = ()) -> dict[str, SecretStr]:
    """Collect ``{REF}_{NAME}`` env vars into ``{name_lower: SecretStr}``.

    >>> load_secrets("X", {"X_API_KEY": "k"})
    {'api_key': SecretStr('**********')}
    """
    prefix = f"{ref}_"
    found = {k[len(prefix) :].lower(): SecretStr(v) for k, v in env.items() if k.startswith(prefix) and v}
    missing = [name for name in required if name not in found]
    if missing:
        names = ", ".join(f"{prefix}{m.upper()}" for m in missing)
        raise ConfigError(f"missing secrets: {names}")  # names only, never values
    return found


def require_private(path: Path) -> None:
    """Secrets files must be readable by their owner only (SPEC §15: "a secrets file with 600
    permissions"). Group or other access is refused, with the fix in the message."""
    mode = path.stat().st_mode & 0o777
    if mode & 0o077:
        raise ConfigError(f"{path} holds secrets but is mode {mode:04o}: run `chmod 600 {path}`")


def read_env_file(path: Path) -> dict[str, str]:
    """Parse a ``.env`` file (KEY=VALUE lines, ``#`` comments, optional quotes). Missing file -> {}.
    The file must be 0600 (``require_private``). Never logs values."""
    if not path.exists():
        return {}
    require_private(path)
    result: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip().removeprefix("export ").strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        elif " #" in value:
            value = value.split(" #", 1)[0].rstrip()
        result[key] = value
    return result


def update_env_file(path: Path, values: Mapping[str, str]) -> None:
    """Set ``KEY=value`` lines in a ``.env`` file (replacing existing keys, appending new ones) and make
    it 0600. Used to store tokens without ever printing them."""
    import os

    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    pending = dict(values)
    out: list[str] = []
    for line in lines:
        key = line.split("=", 1)[0].strip().removeprefix("export ").strip()
        if "=" in line and not line.lstrip().startswith("#") and key in pending:
            out.append(f"{key}={pending.pop(key)}")
        else:
            out.append(line)
    out.extend(f"{k}={v}" for k, v in pending.items())
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write("\n".join(out) + "\n")
    os.replace(tmp, path)
    os.chmod(path, 0o600)
