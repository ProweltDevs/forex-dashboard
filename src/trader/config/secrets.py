"""Read account secrets from the environment by prefix. Values are wrapped in ``SecretStr`` so they
cannot leak through ``repr``/logs by accident."""

from __future__ import annotations

from collections.abc import Mapping

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
