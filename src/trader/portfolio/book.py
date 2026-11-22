"""PortfolioBook: the account's computed money state (SPEC §9). Pure and synchronous.

- Unrealized P&L per position on every tick: exit price (bid for longs, ask for shorts) vs entry, times
  contract size, converted from the quote currency with the conversion pair's live tick.
- Computed balance = the broker balance at the baseline + net of every ledger deal after it; computed equity
  = computed balance + computed unrealized. Comparing them with the broker's numbers is the drift check.
- High-water mark and drawdown of the computed equity, updated with every sample.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from trader.domain.accounting import pnl_quote
from trader.domain.enums import Side
from trader.domain.errors import ConversionUnavailable
from trader.domain.fx import conversion_rate
from trader.domain.jsontypes import JsonObj
from trader.domain.models import AccountState, Instrument, Position, Tick
from trader.domain.time import from_epoch_ms, to_epoch_ms

from .ledger import ZERO, FillLedger, trading_day


@dataclass(frozen=True, slots=True, kw_only=True)
class EquityPoint:
    ts: datetime
    balance: Decimal  # computed
    unrealized: Decimal  # computed
    equity: Decimal  # computed
    broker_balance: Decimal | None
    broker_equity: Decimal | None
    realized_today: Decimal  # net, this trading day
    high_water: Decimal
    drawdown: Decimal  # high_water - equity
    unpriced: int  # open positions without a price or conversion rate (excluded from unrealized)

    @property
    def drawdown_pct(self) -> Decimal:
        return ZERO if self.high_water <= 0 else self.drawdown / self.high_water * 100

    def to_json(self) -> JsonObj:
        def s(v: Decimal | None) -> str | None:
            return None if v is None else str(v)

        return {
            "ts_ms": to_epoch_ms(self.ts),
            "balance": str(self.balance),
            "unrealized": str(self.unrealized),
            "equity": str(self.equity),
            "broker_balance": s(self.broker_balance),
            "broker_equity": s(self.broker_equity),
            "realized_today": str(self.realized_today),
            "high_water": str(self.high_water),
            "drawdown": str(self.drawdown),
            "drawdown_pct": str(self.drawdown_pct.quantize(Decimal("0.0001"))),
            "unpriced": self.unpriced,
        }

    @classmethod
    def from_json(cls, d: JsonObj) -> EquityPoint:
        def od(v: object) -> Decimal | None:
            return None if v is None else Decimal(str(v))

        return cls(
            ts=from_epoch_ms(int(str(d["ts_ms"]))),
            balance=Decimal(str(d["balance"])),
            unrealized=Decimal(str(d["unrealized"])),
            equity=Decimal(str(d["equity"])),
            broker_balance=od(d.get("broker_balance")),
            broker_equity=od(d.get("broker_equity")),
            realized_today=Decimal(str(d["realized_today"])),
            high_water=Decimal(str(d["high_water"])),
            drawdown=Decimal(str(d["drawdown"])),
            unpriced=int(str(d.get("unpriced") or 0)),
        )


class PortfolioBook:
    def __init__(self, account_id: str, ledger: FillLedger) -> None:
        self.account_id = account_id
        self.ledger = ledger
        self.account: AccountState | None = None
        self.positions: tuple[Position, ...] = ()
        self.instruments: Mapping[str, Instrument] = {}
        self.ticks: dict[str, Tick] = {}
        self.baseline: tuple[datetime, Decimal] | None = None  # (ts, broker balance)
        self.high_water: Decimal | None = None

    # ------------------------------------------------------------------ inputs

    def on_tick(self, tick: Tick) -> None:
        self.ticks[tick.symbol] = tick

    def on_account(self, state: AccountState) -> None:
        self.account = state
        if self.baseline is None:
            self.rebase(state.ts)

    def set_positions(self, positions: tuple[Position, ...], instruments: Mapping[str, Instrument]) -> None:
        self.positions = positions
        self.instruments = instruments

    def rebase(self, ts: datetime) -> None:
        """Take the broker balance as the new baseline (start, or after a deposit/withdrawal)."""
        if self.account is not None:
            self.baseline = (ts, self.account.balance)

    # ------------------------------------------------------------------ computed

    def unrealized_of(self, pos: Position) -> Decimal | None:
        inst, tick, account = self.instruments.get(pos.symbol), self.ticks.get(pos.symbol), self.account
        if inst is None or tick is None or account is None:
            return None
        exit_price = tick.bid if pos.side is Side.BUY else tick.ask
        try:
            rate = conversion_rate(inst.quote_currency, account.currency, self.ticks.get)
        except ConversionUnavailable:
            return None
        return pnl_quote(pos.side, pos.volume, pos.entry_price, exit_price, inst.contract_size) * rate

    def unrealized(self) -> tuple[Decimal, int]:
        """(sum of computed unrealized P&L, positions that could not be priced)."""
        total, unpriced = ZERO, 0
        for pos in self.positions:
            value = self.unrealized_of(pos)
            if value is None:
                unpriced += 1
            else:
                total += value
        return total, unpriced

    def computed_balance(self) -> Decimal | None:
        if self.baseline is None:
            return None
        since, balance = self.baseline
        return balance + sum((e.pnl.net for e in self.ledger.entries(since=since)), ZERO)

    def balance_gap(self) -> Decimal | None:
        """Broker balance - computed balance (non-zero: a missed deal or a balance operation)."""
        computed = self.computed_balance()
        if computed is None or self.account is None:
            return None
        return self.account.balance - computed

    def sample(self, now: datetime) -> EquityPoint | None:
        balance = self.computed_balance()
        if balance is None:
            return None
        unrealized, unpriced = self.unrealized()
        equity = balance + unrealized
        self.high_water = equity if self.high_water is None else max(self.high_water, equity)
        today = trading_day(now, self.ledger.day_offset)
        acct = self.account
        return EquityPoint(
            ts=now,
            balance=balance,
            unrealized=unrealized,
            equity=equity,
            broker_balance=acct.balance if acct else None,
            broker_equity=acct.equity if acct else None,
            realized_today=self.ledger.realized(day=today).net,
            high_water=self.high_water,
            drawdown=self.high_water - equity,
            unpriced=unpriced,
        )
