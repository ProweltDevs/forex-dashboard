"""Access Point discovery (https://www.exness-api.com/trading/get-started/access-point-host-discovery).

The Gateway host comes from the DNS widget in Key Management. Every account-scoped REST request and
WebSocket connection goes to the account's Access Point, resolved here. Discovery is rate limited:
cache the result and re-resolve only after the Access Point fails.
"""

from __future__ import annotations

import httpx

from trader.domain.errors import BrokerUnavailable

from .auth import Signer
from .rest import RequestLimits, RestClient, base_url


async def resolve_access_point(
    gateway: str,
    account_id: str,
    signer: Signer,
    client: httpx.AsyncClient,
    *,
    allow_insecure: bool = False,
) -> str:
    """``GET {gateway}/v1/trading/access-point?account_id=…`` -> the account's Access Point base URL.
    Discovery is rate limited: callers cache the result and re-resolve only after connection failures."""
    rest = RestClient(base_url(gateway, allow_insecure=allow_insecure), signer, client, RequestLimits())
    data = await rest.get("getAccessPoint", "/v1/trading/access-point", [("account_id", account_id)])
    value = data.get("access_point")
    if not isinstance(value, str) or not value:
        raise BrokerUnavailable("discovery response is missing access_point")
    return base_url(value, allow_insecure=allow_insecure)
