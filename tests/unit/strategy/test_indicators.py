"""Indicators vs independent batch references written straight from the textbook definitions (recomputed
from scratch at every step), over random bar series; plus hand-checkable values."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from trader.domain import Timeframe
from trader.domain.models import Bar
from trader.strategy.indicators import (
    ADX,
    ATR,
    EMA,
    MACD,
    RSI,
    SMA,
    VWAP,
    WMA,
    BollingerBands,
    Donchian,
    Pivots,
    Stochastic,
    SuperTrend,
    classic_pivots,
)

D = Decimal
T0 = datetime(2026, 1, 5, tzinfo=UTC)
TOL = D("1e-15")


def bar(
    i: int,
    o: str | Decimal,
    h: str | Decimal,
    low: str | Decimal,
    c: str | Decimal,
    v: str = "1",
    step: timedelta = timedelta(minutes=1),
) -> Bar:
    return Bar(
        symbol="EURUSD",
        timeframe=Timeframe.M1,
        start=T0 + i * step,
        open=D(o),
        high=D(h),
        low=D(low),
        close=D(c),
        volume=D(v),
        complete=True,
    )


moves = st.tuples(
    st.decimals(min_value=D("-0.0050"), max_value=D("0.0050"), places=5),
    st.decimals(min_value=D(0), max_value=D("0.0030"), places=5),
    st.decimals(min_value=D(0), max_value=D("0.0030"), places=5),
    st.integers(min_value=0, max_value=50),
)


@st.composite
def series(draw: st.DrawFn, min_size: int = 1, max_size: int = 120) -> list[Bar]:
    out: list[Bar] = []
    close = D("1.10000")
    for i, (change, up, down, vol) in enumerate(draw(st.lists(moves, min_size=min_size, max_size=max_size))):
        o, c = close, close + change
        out.append(bar(i, o, max(o, c) + up, min(o, c) - down, c, str(vol)))
        close = c
    return out


def close(bars: Sequence[Bar]) -> list[Decimal]:
    return [b.close for b in bars]


# ------------------------------------------------------------------------------------- references


def ref_sma(xs: Sequence[Decimal], n: int) -> list[Decimal | None]:
    return [None if i < n - 1 else sum(xs[i - n + 1 : i + 1], D(0)) / n for i in range(len(xs))]


def ref_ema(xs: Sequence[Decimal], n: int) -> list[Decimal | None]:
    out: list[Decimal | None] = []
    e: Decimal | None = None
    for i, x in enumerate(xs):
        if i == n - 1:
            e = sum(xs[:n], D(0)) / n
        elif e is not None:
            e = e + D(2) / (n + 1) * (x - e)
        out.append(e)
    return out


def ref_wma(xs: Sequence[Decimal], n: int) -> list[Decimal | None]:
    den = D(n * (n + 1) // 2)
    return [
        None if i < n - 1 else sum((j + 1) * xs[i - n + 1 + j] for j in range(n)) / den
        for i in range(len(xs))
    ]


def wilder_avg(xs: Sequence[Decimal], n: int) -> list[Decimal | None]:
    out: list[Decimal | None] = []
    a: Decimal | None = None
    for i, x in enumerate(xs):
        if i == n - 1:
            a = sum(xs[:n], D(0)) / n
        elif a is not None:
            a = (a * (n - 1) + x) / n
        out.append(a)
    return out


def ref_rsi(xs: Sequence[Decimal], n: int) -> list[Decimal | None]:
    changes = [xs[i] - xs[i - 1] for i in range(1, len(xs))]
    gains = wilder_avg([max(c, D(0)) for c in changes], n)
    losses = wilder_avg([max(-c, D(0)) for c in changes], n)
    out: list[Decimal | None] = [None]
    for g, lo in zip(gains, losses, strict=True):
        if g is None or lo is None:
            out.append(None)
        elif lo == 0:
            out.append(D(50) if g == 0 else D(100))
        else:
            out.append(100 - 100 / (1 + g / lo))
    return out


def trs(bars: Sequence[Bar]) -> list[Decimal]:
    out = []
    for i, b in enumerate(bars):
        if i == 0:
            out.append(b.high - b.low)
        else:
            pc = bars[i - 1].close
            out.append(max(b.high - b.low, abs(b.high - pc), abs(b.low - pc)))
    return out


def ref_atr(bars: Sequence[Bar], n: int) -> list[Decimal | None]:
    return wilder_avg(trs(bars), n)


def ref_stdev_bands(
    xs: Sequence[Decimal], n: int, k: Decimal
) -> list[tuple[Decimal, Decimal, Decimal] | None]:
    out: list[tuple[Decimal, Decimal, Decimal] | None] = []
    for i in range(len(xs)):
        if i < n - 1:
            out.append(None)
            continue
        w = xs[i - n + 1 : i + 1]
        m = sum(w, D(0)) / n
        sd = (sum((x - m) ** 2 for x in w) / n).sqrt()
        out.append((m + k * sd, m, m - k * sd))
    return out


def ref_donchian(bars: Sequence[Bar], n: int, exclude: bool) -> list[tuple[Decimal, Decimal] | None]:
    out: list[tuple[Decimal, Decimal] | None] = []
    for i in range(len(bars)):
        end = i if exclude else i + 1
        if end < n:
            out.append(None)
            continue
        w = bars[end - n : end]
        out.append((max(b.high for b in w), min(b.low for b in w)))
    return out


def ref_stoch(bars: Sequence[Bar], kp: int, dp: int) -> list[tuple[Decimal, Decimal] | None]:
    ks: list[Decimal | None] = []
    for i, b in enumerate(bars):
        if i < kp - 1:
            ks.append(None)
            continue
        w = bars[i - kp + 1 : i + 1]
        hh, ll = max(x.high for x in w), min(x.low for x in w)
        ks.append(D(50) if hh == ll else 100 * (b.close - ll) / (hh - ll))
    out: list[tuple[Decimal, Decimal] | None] = []
    for i, k in enumerate(ks):
        window = ks[i - dp + 1 : i + 1] if i >= dp - 1 else []
        if k is None or len(window) < dp or any(x is None for x in window):
            out.append(None)
        else:
            out.append((k, sum((x for x in window if x is not None), D(0)) / dp))
    return out


def ref_adx(bars: Sequence[Bar], n: int) -> list[tuple[Decimal, Decimal, Decimal] | None]:
    """Textbook DMI with Wilder *averages* (the implementation uses Wilder running sums: same ratios)."""
    plus_dm, minus_dm, tr = [], [], []
    for i in range(1, len(bars)):
        b, p = bars[i], bars[i - 1]
        up, down = b.high - p.high, p.low - b.low
        plus_dm.append(up if up > down and up > 0 else D(0))
        minus_dm.append(down if down > up and down > 0 else D(0))
        tr.append(max(b.high - b.low, abs(b.high - p.close), abs(b.low - p.close)))
    atr, pdm, mdm = wilder_avg(tr, n), wilder_avg(plus_dm, n), wilder_avg(minus_dm, n)
    dis: list[tuple[Decimal, Decimal] | None] = []
    dxs: list[Decimal] = []
    for a, p, m in zip(atr, pdm, mdm, strict=True):
        if a is None or p is None or m is None:
            dis.append(None)
            continue
        pdi = 100 * p / a if a > 0 else D(0)
        mdi = 100 * m / a if a > 0 else D(0)
        dis.append((pdi, mdi))
        dxs.append(100 * abs(pdi - mdi) / (pdi + mdi) if pdi + mdi > 0 else D(0))
    adx = wilder_avg(dxs, n)
    out: list[tuple[Decimal, Decimal, Decimal] | None] = [None]
    j = 0
    for d in dis:
        if d is None:
            out.append(None)
            continue
        a = adx[j]
        j += 1
        out.append(None if a is None else (a, d[0], d[1]))
    return out


def close_enough(a: Decimal | None, b: Decimal | None) -> bool:
    if a is None or b is None:
        return a is b
    return abs(a - b) <= TOL * max(D(1), abs(b))


# ------------------------------------------------------------------------------------- equivalence


def run[T](
    ind_factory: Callable[[], object], bars: Sequence[Bar], extract: Callable[[object], T]
) -> list[T | None]:
    ind = ind_factory()
    out: list[T | None] = []
    for b in bars:
        ind.update(b)  # type: ignore[attr-defined]
        v = ind.value  # type: ignore[attr-defined]
        assert ind.ready is (v is not None)  # type: ignore[attr-defined]
        out.append(None if v is None else extract(v))
    return out


def same(got: Sequence[object], want: Sequence[object]) -> None:
    assert len(got) == len(want)
    for i, (g, w) in enumerate(zip(got, want, strict=True)):
        if isinstance(w, tuple) and isinstance(g, tuple):
            assert all(close_enough(x, y) for x, y in zip(g, w, strict=True)), (i, g, w)
        else:
            assert close_enough(g, w), (i, g, w)  # type: ignore[arg-type]


PERIODS = st.integers(min_value=1, max_value=15)
CASES = settings(max_examples=60, deadline=None)


@CASES
@given(series(), PERIODS)
def test_moving_averages(bars: list[Bar], n: int) -> None:
    xs = close(bars)
    same(run(lambda: SMA(n), bars, lambda v: v), ref_sma(xs, n))
    same(run(lambda: EMA(n), bars, lambda v: v), ref_ema(xs, n))
    same(run(lambda: WMA(n), bars, lambda v: v), ref_wma(xs, n))


@CASES
@given(series(), PERIODS)
def test_rsi_and_atr(bars: list[Bar], n: int) -> None:
    same(run(lambda: RSI(n), bars, lambda v: v), ref_rsi(close(bars), n))
    same(run(lambda: ATR(n), bars, lambda v: v), ref_atr(bars, n))


@CASES
@given(series(), st.integers(1, 6), st.integers(7, 15), st.integers(1, 6))
def test_macd(bars: list[Bar], fast: int, slow: int, sig: int) -> None:
    xs = close(bars)
    f, s = ref_ema(xs, fast), ref_ema(xs, slow)
    macd = [None if a is None or b is None else a - b for a, b in zip(f, s, strict=True)]
    first = next((i for i, m in enumerate(macd) if m is not None), len(macd))
    signal = [None] * first + ref_ema([m for m in macd[first:] if m is not None], sig)
    want = [None if m is None or g is None else (m, g, m - g) for m, g in zip(macd, signal, strict=True)]
    same(run(lambda: MACD(fast, slow, sig), bars, lambda v: (v.macd, v.signal, v.histogram)), want)  # type: ignore[attr-defined]


@CASES
@given(series(), PERIODS)
def test_bands_channels_and_stochastic(bars: list[Bar], n: int) -> None:
    got = run(lambda: BollingerBands(n, D(2)), bars, lambda v: (v.upper, v.middle, v.lower))  # type: ignore[attr-defined]
    for g, w in zip(got, ref_stdev_bands(close(bars), n, D(2)), strict=True):
        if g is None or w is None:
            assert g is w
        else:  # sqrt of a variance computed two ways: compare at price precision
            assert all(abs(x - y) < D("1e-10") for x, y in zip(g, w, strict=True))
    for exclude in (False, True):
        same(
            run(lambda e=exclude: Donchian(n, exclude_current=e), bars, lambda v: (v.upper, v.lower)),  # type: ignore[attr-defined]
            ref_donchian(bars, n, exclude),
        )
    same(run(lambda: Stochastic(n, 3), bars, lambda v: (v.k, v.d)), ref_stoch(bars, n, 3))  # type: ignore[attr-defined]


@CASES
@given(series(min_size=10), st.integers(min_value=2, max_value=10))
def test_adx(bars: list[Bar], n: int) -> None:
    got = run(lambda: ADX(n), bars, lambda v: (v.adx, v.plus_di, v.minus_di))  # type: ignore[attr-defined]
    want = ref_adx(bars, n)
    for g, w in zip(got, want, strict=True):
        if g is None or w is None:
            assert g is w
        else:
            assert all(abs(x - y) < D("1e-12") for x, y in zip(g, w, strict=True))


# ------------------------------------------------------------------------------------- known values


def flat(xs: Sequence[str]) -> list[Bar]:
    return [bar(i, x, x, x, x) for i, x in enumerate(xs)]


def test_known_values() -> None:
    bars = flat(["1", "2", "3", "4", "5"])
    assert run(lambda: SMA(3), bars, lambda v: v) == [None, None, D(2), D(3), D(4)]
    assert run(lambda: WMA(3), bars, lambda v: v)[2] == D(14) / 6  # (1*1 + 2*2 + 3*3) / 6
    assert run(lambda: EMA(3), bars, lambda v: v)[2:] == [
        D(2),
        D(3),
        D(4),
    ]  # linear input: EMA lags by (n-1)/2
    assert run(lambda: RSI(3), bars, lambda v: v)[-1] == 100  # only gains
    # Wilder, by hand: first window gain 0.5 / loss 0.5 -> 50; then +1: 0.75/0.25 -> 75; then -1: 0.375/0.625
    assert run(lambda: RSI(2), flat(["1", "2", "1", "2", "1"]), lambda v: v)[2:] == [D(50), D(75), D("37.5")]
    bands = run(lambda: BollingerBands(3), flat(["2", "2", "2"]), lambda v: v.width)  # type: ignore[attr-defined]
    assert bands[-1] == 0
    hl = [bar(0, "10", "12", "9", "11"), bar(1, "11", "15", "10", "14"), bar(2, "14", "14", "12", "12")]
    # TR: 3, max(5, 4, 1) = 5, max(2, 0, 2) = 2 -> ATR(3) = 10/3
    assert run(lambda: ATR(3), hl, lambda v: v)[-1] == D(10) / 3
    k = run(lambda: Stochastic(3, 1), hl, lambda v: v.k)  # type: ignore[attr-defined]
    assert k[-1] == 100 * (D(12) - 9) / (15 - 9)  # 50


def test_pivots_and_vwap_by_session() -> None:
    p = classic_pivots(D(110), D(90), D(100))
    assert (p.pivot, p.r1, p.s1, p.r2, p.s2, p.r3, p.s3) == (
        D(100),
        D(110),
        D(90),
        D(120),
        D(80),
        D(130),
        D(70),
    )
    day = timedelta(days=1)
    piv, vwap = Pivots(), VWAP()
    session1 = [
        bar(0, "100", "110", "95", "105", "2", step=day / 4),
        bar(1, "105", "108", "90", "100", "1", step=day / 4),
    ]
    for b in session1:
        piv.update(b)
        vwap.update(b)
    assert piv.value is None  # no previous session yet
    # VWAP = (hlc3 * v) / v: (310/3 * 2 + 298/3 * 1) / 3
    assert vwap.value == (D(310) / 3 * 2 + D(298) / 3) / 3
    nxt = Bar(
        symbol="EURUSD",
        timeframe=Timeframe.M1,
        start=T0 + day,
        open=D(100),
        high=D(101),
        low=D(99),
        close=D(100),
        volume=D(4),
        complete=True,
    )
    piv.update(nxt)
    vwap.update(nxt)
    assert piv.value == classic_pivots(D(110), D(90), D(100))  # yesterday's H/L/C
    assert vwap.value == D(100)  # reset at the session start


def test_supertrend_flips_on_a_reversal() -> None:
    up = [bar(i, str(100 + i), str(101 + i), str(99 + i), str(100.5 + i)) for i in range(15)]
    down = [
        bar(15 + i, str(114 - 2 * i), str(114.5 - 2 * i), str(111 - 2 * i), str(111.5 - 2 * i))
        for i in range(10)
    ]
    st_ = SuperTrend(period=5, multiplier=D(2))
    directions = []
    for b in up + down:
        st_.update(b)
        if st_.value is not None:
            directions.append(st_.value.direction)
            assert (st_.value.line < b.close) is (st_.value.direction > 0)
    assert directions[0] == 1
    assert directions[-1] == -1


def test_invalid_parameters() -> None:
    with pytest.raises(ValueError, match="period"):
        SMA(0)
    with pytest.raises(ValueError, match="fast"):
        MACD(26, 12)
