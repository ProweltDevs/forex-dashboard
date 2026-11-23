# Notifications

Telegram (primary) and email (optional) alerts from the running process, plus Telegram commands
(SPEC §14, [ADR 0012](adr/0012-control-api-and-notifications.md)). Code: `src/trader/notify/`.

## What is sent

| Event | Severity | Notes |
|---|---|---|
| Start / stop of `trader run` | info | accounts, kill levels, strategy states; errors this run |
| Fill | info | off with `notify.fills: false` (very active strategies) |
| Position closed, **SL hit**, **TP hit** | info | always sent; see "SL/TP hits" |
| Order rejected (broker or risk) | warning | deduped per (account, strategy, rule) |
| Kill switch / circuit breaker up | critical | level, reason, active triggers |
| Kill switch lowered / cleared | warning / info | |
| Disconnected / degraded / reconnected | warning / info | the first connect is in the start notice |
| Stale feed / feed back | warning / info | |
| Reconcile correction | warning | the broker's state was applied |
| Alerts: `db_down`, `wal_write_failed`, `equity_drift`, `balance_mismatch`, `db_recovered` | as published | |
| Strategy error, strategy auto-paused | warning | not the pauses from a kill switch or shutdown |
| Daily report | info | realized P&L by strategy and symbol, trades, win rate, equity, drawdown |

Every message starts with the mode, e.g. `[live] CRITICAL: Kill switch FLATTEN_ALL`.

**SL/TP hits** are recognised from prices: a closing fill at, or beyond, the position's last known SL
(within 10% of the stop distance short of it) is "SL hit", likewise for TP. The brokers do not report why
a position closed in a form every adapter shares, so this says what the prices show.

## Delivery rules

- **Never blocks trading:** events become notices on bounded queues; a background worker per channel sends
  them.
- **Dedupe:** the same alert key within `dedupe_s` (default 300) is sent once; the next one after the
  window says how many were folded in.
- **Rate limit:** `rate_per_min` (default 20) messages per channel. Extra non-critical notices are counted
  and a summary follows once there is room. **Critical notices are never rate-limited.**
- **Per-channel minimum severity:** Telegram `info`, email `warning` by default.
- **Retries:** 3 attempts, honouring Telegram's `retry_after`; then the message is dropped and logged.
- **Secrets:** the bot token is part of every Bot API URL. It is never logged:
  - Our errors are reported by type and HTTP status, never by exception text.
  - httpx logs every request URL at INFO. Every stdlib log record is scrubbed when it is created
    (`install_record_redaction`), so every handler, not only ours, sees `bot***REDACTED***`. CI caught
    this: pytest's own log capture showed the (fake) test token before the fix.
  - SMTP errors are reported by type only.

## Telegram commands (`notify.telegram.commands: true`)

| Command | Does |
|---|---|
| `/status` | each account: connected, kill level, equity, positions, orders; strategies by status |
| `/pnl` | each account: realized today, unrealized, equity, drawdown |
| `/pause [strategy\|all]` | pause strategies (default all running) |
| `/kill [pause\|cancel\|flatten] [account\|all]` | kill switch; default `pause` (PAUSE_NEW) on all accounts |

- Accepted **only from the configured chat, and only if it is a private chat**. In a group every member
  could send them. Others are ignored and logged (without who or what).
- Messages older than **2 minutes** are ignored, so a `/kill` typed while the process was down is not
  replayed later.
- **Resetting the kill switch is not possible from Telegram:** use the API (`POST /risk/reset`), or
  `trader kill --reset` once `trader run` is stopped (it is refused while the process owns the account).
- Every command goes through the same `Control` facade as the API and is **audited with actor
  `telegram`**.

## Setup

1. Create a bot with [@BotFather](https://t.me/BotFather); it gives you the token.
2. Send your bot any message, then open `https://api.telegram.org/bot<token>/getUpdates` and read
   `message.chat.id`.
3. In `.env`: `TELEGRAM_BOT_TOKEN=...`, `TELEGRAM_CHAT_ID=...`.
4. In the config: `notify: {telegram: {commands: true}}`.
5. Check it: `uv run pytest -m demo tests/contract/test_telegram_probe.py -s` (sends 3 messages to your
   chat).

## Telegram Bot API: what the code relies on

Reference: <https://core.telegram.org/bots/api>.

> **Not yet verified against the official docs.** The documentation could not be fetched from this
> development environment: the host was allowed, but the automated fetch was refused. Everything below
> comes from prior knowledge of the Bot API. Run the probe (`tests/contract/test_telegram_probe.py`, demo
> marked) with a real bot to confirm it, and record the result here.

| Fact | Used in | Probe step |
|---|---|---|
| Methods are called as `POST https://api.telegram.org/bot<token>/<method>` with a JSON body | `TelegramChannel._call` | 1 |
| Responses are `{"ok": true, "result": ...}` or `{"ok": false, "error_code": n, "description": "..."}` | `_check` | 1, 2, 3 |
| `sendMessage` takes `chat_id` and `text`; without `parse_mode` the text is plain (nothing to escape) | `send` | 1 |
| A message text is at most 4096 characters | `MAX_TEXT` (we cut there) | 2 |
| HTTP 429 carries `parameters.retry_after` (seconds) | `_check` (`retry_after`) | not probed: needs a flood |
| `getUpdates` takes `offset`, `timeout` (long poll, seconds) and `allowed_updates`; `offset` = last `update_id` + 1 confirms the earlier updates | `get_updates`, `TelegramBot._poll` | 4 |
| An update's `message` has `chat.id`, `chat.type` (`"private"` for a one-to-one chat), `date` (unix seconds) and `text` | `TelegramBot.handle` | 4 |
| A bot cannot message a user who has not started a chat with it; an unknown chat answers 400 | setup step 2 | 3 |

`notify.telegram.api_base` can point to a self-hosted Bot API server. The integration tests point it at a
fake server (`tests/integration/telegram_fake.py`), which implements exactly the table above.

## Email

```yaml
notify:
  email: {host: smtp.example.com, port: 587, starttls: true, sender: trader@example.com,
          recipients: [me@example.com], min_severity: warning}
```

SMTP login from `TRADER_SMTP_USER` / `TRADER_SMTP_PASSWORD`. smtplib runs in a worker thread with STARTTLS
(default certificate checks). One email per notice that passes the severity, dedupe and rate rules.
