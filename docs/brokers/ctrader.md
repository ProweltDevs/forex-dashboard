# cTrader Open API — verified facts

Adapter: `src/trader/brokers/ctrader/`. Design: [ADR 0004](../adr/0004-ctrader-adapter.md).
Protos vendored from <https://github.com/spotware/openapi-proto-messages> at commit
`a9676f27f40b60569fb8a510aeadedba901b74e9` (2026-11-04), see `proto/src/VERSION`. Regenerate the bindings with
`scripts/gen_ctrader_proto.sh` (grpcio-tools protoc 35.1, protobuf runtime ≥ 7.35).

Docs read on 2026-11-22. **V** = verified in the official docs/protos. **A** = assumption, with the demo probe
that settles it (`uv run pytest -m demo tests/contract/test_ctrader_probes.py -s`).

## Connection

| | Fact | Source |
|---|---|---|
| V | Demo `demo.ctraderapi.com`, live `live.ctraderapi.com`; **5035 = Protobuf**, 5036 = JSON. Both ports accept TCP and WebSocket. | [proxies-endpoints](https://help.ctrader.com/open-api/proxies-endpoints/) |
| V | Demo and live are fully separate: a live endpoint cannot use demo accounts and vice versa. One connection per environment. | same |
| V | The TCP connection **must use SSL**. | [connection](https://help.ctrader.com/open-api/connection/) |
| V | Frame = **4-byte big-endian length** + serialised `ProtoMessage{payloadType, payload, clientMsgId}`. The page's Python snippet shows `struct.pack("!H")` (2 bytes); that contradicts its C# code and its own read instructions (4 bytes). We use 4 bytes. | [sending-receiving-protobuf](https://help.ctrader.com/open-api/sending-receiving-protobuf/) |
| V | Messages sent before app authorisation get an error. Send `ProtoHeartbeatEvent` **every 10 s** to stay connected; idle connections are dropped. | [connection](https://help.ctrader.com/open-api/connection/), [faq](https://help.ctrader.com/open-api/faq/) |
| V | Rate limits: **50 req/s non-historical, 5 req/s historical, per connection**. Exceeding returns `REQUEST_FREQUENCY_EXCEEDED` / `BLOCKED_PAYLOAD_TYPE`; `ProtoOAErrorRes.retryAfter` holds the seconds until unblocked. | [index](https://help.ctrader.com/open-api/), `ProtoOAErrorRes` |
| V | Maintenance happens at weekends; the API can be unreachable then. | [faq](https://help.ctrader.com/open-api/faq/) |
| A | Every response to a request (including the `ProtoOAExecutionEvent` that answers `ProtoOANewOrderReq`) echoes the request's `clientMsgId`. This is how the official SDKs match responses; `test_broker_contract[ctrader-demo]` exercises it. | — |

## Authentication

| | Fact | Source |
|---|---|---|
| V | Order: `ProtoOAApplicationAuthReq(clientId, clientSecret)` → `ProtoOAGetAccountListByAccessTokenReq(accessToken)` → `ProtoOAAccountAuthReq(ctidTraderAccountId, accessToken)`. | [account-authentication](https://help.ctrader.com/open-api/account-authentication/) |
| V | Grant URL: `https://id.ctrader.com/my/settings/openapi/grantingaccess/?client_id=…&redirect_uri=…&scope=trading&product=web`. The code arrives as `?code=` on the redirect URI and expires in **~1 minute**. | same |
| V | Token exchange: `GET https://openapi.ctrader.com/apps/token?grant_type=authorization_code&code=…&redirect_uri=…&client_id=…&client_secret=…` returns `{accessToken, tokenType, expiresIn, refreshToken, errorCode, description}`. | same |
| V | Access token lifetime is **2,628,000 s (~30 days)**. The refresh token has **no expiry** but is invalidated once used (or on re-authorisation). It can be refreshed before or after expiry. | same, [faq](https://help.ctrader.com/open-api/faq/) |
| V | `ProtoOARefreshTokenReq(refreshToken)` → `ProtoOARefreshTokenRes{accessToken, expiresIn, refreshToken}` ("Your new Refresh Token"). The pair rotates, so we persist it (encrypted). | proto |
| V | Accounts created after the grant are not authorised until the user grants again. | [account-authentication](https://help.ctrader.com/open-api/account-authentication/) |
| — | The REST refresh parameters (`grant_type=refresh_token`) are not documented, so we refresh only over Protobuf. | — |

## Units and prices

| | Fact | Source |
|---|---|---|
| V | Order/position/deal volumes are in **0.01 of a unit** ("cents"): `1000` = 10.00 units. `minVolume`/`maxVolume`/`stepVolume`/`lotSize` are in cents too. So `wire = lots × lotSize` and contract size = `lotSize / 100` units. | `ProtoOANewOrderReq.volume`, `ProtoOASymbol` |
| V | Spot `bid`/`ask`, trendbar `low`, and relative SL/TP are integers in **1/100000** of a price unit (`123000` = 1.23). Divide and round to the symbol `digits`. | `ProtoOASpotEvent`, [symbol-data](https://help.ctrader.com/open-api/symbol-data/) |
| V | Spot events may omit `bid` or `ask`: keep the last value of each. `timestamp` is sent only with `subscribeToSpotTimestamp=true`. If markets are closed you get a single spot event. | [symbol-data](https://help.ctrader.com/open-api/symbol-data/), [faq](https://help.ctrader.com/open-api/faq/) |
| V | Trendbars: `open = low + deltaOpen`, `high = low + deltaHigh`, `close = low + deltaClose`; `utcTimestampInMinutes`; `volume` = **ticks**. Bars exist only for periods with ticks. Periods: M1 M2 M3 M4 M5 M10 M15 M30 H1 H4 H12 D1 W1 MN1 (no seconds). | `ProtoOATrendbar`, [symbol-data](https://help.ctrader.com/open-api/symbol-data/) |
| V | Absolute prices in orders/positions are `double`. Absolute SL/TP are **not supported on MARKET orders**; use `relativeStopLoss`/`relativeTakeProfit` (BUY: `SL = entry − rel`). | `ProtoOANewOrderReq` |
| V | Money fields are integers scaled by **10^moneyDigits** (`ProtoOATrader.moneyDigits`, also on positions/deals). | `ProtoOATrader` |
| V | `ProtoOATrader` has `balance` but **no equity**. Unrealized P&L per position comes from `ProtoOAGetPositionUnrealizedPnLReq` (`netUnrealizedPnL` excludes the closing commission); margin from `ProtoOAPosition.usedMargin`. | protos |
| V | `clientOrderId` max length **50**, `label` max 100, `comment` max 512. | `ProtoOANewOrderReq` |
| V | `ProtoOAReconcileReq` returns open positions and pending orders. | proto |
| V | Account type `HEDGED` / `NETTED` / `SPREAD_BETTING` (`ProtoOATrader.accountType`). | proto |
| A | **Commission sign:** deals report commission as a negative number (debit). Probe: `test_probe_commission_sign_and_unrealized`. | — |
| A | `AmendPositionSLTPReq` without `stopLoss`/`takeProfit` **clears** them (our `modify_position(sl=None)` relies on it). Probe: `test_probe_clear_sl_by_omission`. | — |
| A | STOP_LIMIT is `stopPrice` + `slippageInPoints`, where 1 point = 10^-digits. Probe: `test_probe_stop_limit_round_trip`. | — |
| A | Trendbar range per request: the docs mention per-period limits but give no numbers (only `INCORRECT_BOUNDARIES`). The adapter asks for ≤ 1000 bars per request and halves the window on that error. Probe: `test_probe_trendbar_range_limits`. | [symbol-data](https://help.ctrader.com/open-api/symbol-data/) |
| A | The server does **not** deduplicate a repeated `clientOrderId` (it is a FIX-style tag, not an idempotency key). The adapter deduplicates locally (`client_order_id → orderId`, plus in-flight requests). Across restarts the OMS reconciles. `Capabilities.idempotent_submit = False`, so the OMS needs two "not found" lookups before it resends (ADR 0007). | — |
| V | `ProtoOAOrderListReq(fromTimestamp, toTimestamp)` (Unix ms) returns the trader's orders "filtered by timestamp". `ProtoOAOrder` carries `clientOrderId` (≤ 50 chars), `utcLastUpdateTimestamp` and `positionId`. `ProtoOAOrderListRes.hasMore`: "the number of records by filter is larger than chunkSize, the response contains the number of records that is equal to chunkSize". It counts as a historical request (5 req/s). | proto, [messages](https://help.ctrader.com/open-api/messages/#protooaorderlistreq) |
| A | Neither the chunk size, the sort order nor a cursor is documented. So `find_order` bisects a truncated window until each slice is complete (≤ 64 requests, else `BrokerUnavailable`, never a false "not found"). It also assumes filled orders keep their `clientOrderId` in this history. Probe: `test_probe_order_history_lookup_after_restart`. | — |

## Errors (mapped in `mapping.py`)

`MARKET_CLOSED`, `SYMBOL_HAS_HOLIDAY`, `TRADING_DISABLED`, `NO_QUOTES` → `MarketClosed` · `NOT_ENOUGH_MONEY` →
`InsufficientMargin` · `TRADING_BAD_VOLUME` → `InvalidVolume` · `TRADING_BAD_STOPS`/`_PRICES`/
`PROTECTION_IS_TOO_CLOSE_TO_MARKET`/`TRADING_BAD_EXPIRATION_DATE` → `InvalidPrice` ·
`REQUEST_FREQUENCY_EXCEEDED`/`BLOCKED_PAYLOAD_TYPE` → `RateLimited` · `OA_AUTH_TOKEN_EXPIRED`/
`CH_ACCESS_TOKEN_INVALID`/`ACCOUNT_NOT_AUTHORIZED` → `AuthExpired` · `CH_SERVER_NOT_REACHABLE`/
`SERVER_IS_UNDER_MAINTENANCE`/`CANT_ROUTE_REQUEST` → `BrokerUnavailable` · `ORDER_NOT_FOUND` → `OrderNotFound` ·
`POSITION_NOT_FOUND`/`POSITION_NOT_OPEN` → `PositionNotFound` · `SYMBOL_NOT_FOUND`/`UNKNOWN_SYMBOL` →
`UnknownSymbol` · other trading errors → `OrderRejected` · unknown codes → `BrokerError` (raw code in `broker_code`).
There is no requote code: market orders fill at market, or as `MARKET_RANGE` within `slippageInPoints`.

## Setup (demo)

1. Create an app at <https://openapi.ctrader.com/apps> and wait for activation. Add a redirect URI.
2. Copy `.env.example` to `.env`, then fill in `CTRADER_DEMO_CLIENT_ID`, `_CLIENT_SECRET` and `_REDIRECT_URI`.
3. `uv run trader secrets-key` (encryption key for stored tokens).
4. `uv run trader ctrader-auth`: open the printed URL, allow access, paste the redirect URL back. The tokens are
   written to `.env` (never printed), and your `ctidTraderAccountId`s are listed.
5. Set `CTRADER_DEMO_ACCOUNT_ID` in `.env` and `account_id:` in `config/demo.yaml`.
6. `uv run pytest -m demo` runs the contract suite and the probes against your **demo** account. Record the
   probe output here.

## Open questions (to answer with the probes, then update this page)

- Commission sign on deals/positions; whether `netUnrealizedPnL` includes swap.
- SL/TP clearing semantics of `AmendPositionSLTPReq`.
- Trendbar range limits per period; actual history depth per period (the capabilities currently assume 30 days).
- Whether every execution event in response to a request carries its `clientMsgId`.
- `ProtoOAOrderListRes` chunk size and ordering; whether filled market orders keep `clientOrderId` in the
  history (`test_probe_order_history_lookup_after_restart`).
