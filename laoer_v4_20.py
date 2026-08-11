"""Pure Decimal kernel for the official Laoer V4 SOXL 20-split rules.

This module is intentionally side-effect free: no broker, file, Telegram, KIS,
Docker, or network integration belongs here.  All arithmetic is performed with
``Decimal`` values; numeric inputs are accepted by converting through ``str`` so
float callers do not leak binary floating-point artifacts into the kernel.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_FLOOR, ROUND_HALF_UP
from enum import Enum
from typing import Iterable, Optional

CENT = Decimal("0.01")
ZERO = Decimal("0")
ONE = Decimal("1")
TWENTY = Decimal("20")


class FillEvent(str, Enum):
    FULL = "FULL"
    HALF = "HALF"
    QUARTER = "QUARTER"
    TARGET_FULL = "TARGET_FULL"
    TARGET_HALF = "TARGET_HALF"

    # Backward-compatible internal aliases retained for existing callers.
    FULL_BUY = "FULL"
    HALF_BUY = "HALF"
    QUARTER_SELL = "QUARTER"
    TARGET_SELL_THEN_FULL_BUY = "TARGET_FULL"
    TARGET_SELL_THEN_HALF_BUY = "TARGET_HALF"

    @classmethod
    def _missing_(cls, value: object) -> "FillEvent | None":
        legacy_values = {
            "FULL_BUY": cls.FULL,
            "HALF_BUY": cls.HALF,
            "QUARTER_SELL": cls.QUARTER,
            "TARGET_SELL_THEN_FULL_BUY": cls.TARGET_FULL,
            "TARGET_SELL_THEN_HALF_BUY": cls.TARGET_HALF,
        }
        return legacy_values.get(value) if isinstance(value, str) else None


@dataclass(frozen=True)
class NormalState:
    ticker: str
    split: int
    quantity: object
    avg_price: object
    cash: object
    t: object
    reverse: bool = False


@dataclass(frozen=True)
class NormalPlan:
    ticker: str
    split: int
    t: Decimal
    phase: str
    star_percent: Decimal
    star_raw: Decimal
    star_point: Decimal
    star_buy_price: Decimal
    one_buy_budget: Decimal
    star_buy_budget: Decimal
    star_buy_quantity: int
    average_buy_price: Decimal
    average_buy_budget: Decimal
    average_buy_quantity: int
    quarter_sell_quantity: int
    target_sell_quantity: int
    target_sell_price: Decimal
    reverse_entry: bool
    fail_closed: bool = False
    reason: str = ""


@dataclass(frozen=True)
class ReverseState:
    ticker: str
    split: int
    quantity: object
    avg_price: object
    cash: object
    t: object
    day: int
    previous_quantity: Optional[object] = None
    previous_closes: Iterable[object] = ()
    confirmed_close: Optional[object] = None


@dataclass(frozen=True)
class ReversePlan:
    ticker: str
    split: int
    t: Decimal
    day: int
    reverse_entry: bool
    return_to_normal: bool
    t_carry: Decimal
    star_point: Decimal
    star_buy_price: Decimal
    sell_quantity: int
    sell_order_type: str
    buy_budget: Decimal
    buy_quantity: int
    t_after_sell: Decimal
    t_after_buy: Decimal
    moc_sell_required: bool
    fail_closed: bool = False
    reason: str = ""


def _decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        decimal_value = value
    else:
        try:
            decimal_value = Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"invalid Decimal value: {value!r}") from exc
    if not decimal_value.is_finite():
        raise ValueError(f"invalid non-finite Decimal value: {value!r}")
    return decimal_value


def _parse_decimal(value: object) -> Decimal | None:
    try:
        return _decimal(value)
    except ValueError:
        return None


def _money(value: object) -> Decimal:
    return _decimal(value).quantize(CENT, rounding=ROUND_HALF_UP)


def _floor_int(value: object) -> int:
    decimal_value = _parse_decimal(value)
    if decimal_value is None:
        return 0
    if decimal_value <= ZERO:
        return 0
    return int(decimal_value.to_integral_value(rounding=ROUND_FLOOR))


def _round_cent(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def _quantity_for_budget(budget: Decimal, price: Decimal) -> int:
    if budget <= ZERO or price <= ZERO:
        return 0
    return _floor_int(budget / price)


def _empty_normal_plan(
    state: NormalState,
    *,
    phase: str,
    reason: str = "",
    t: Decimal | None = None,
    avg_price: Decimal | None = None,
) -> NormalPlan:
    t = _parse_decimal(state.t) if t is None else t
    avg_price = _parse_decimal(state.avg_price) if avg_price is None else avg_price
    if t is None:
        t = ZERO
    if avg_price is None:
        avg_price = ZERO
    star_percent = TWENTY - (Decimal("2") * t)
    star_raw = avg_price * (ONE + (star_percent / Decimal("100"))) if avg_price > ZERO else ZERO
    star_point = _round_cent(star_raw) if star_raw > ZERO else ZERO
    star_buy_price = star_point - CENT if star_point > ZERO else ZERO
    target_sell_price = _round_cent(avg_price * Decimal("1.20")) if avg_price > ZERO else ZERO
    return NormalPlan(
        ticker=state.ticker,
        split=state.split,
        t=t,
        phase=phase,
        star_percent=star_percent,
        star_raw=star_raw,
        star_point=star_point,
        star_buy_price=star_buy_price,
        one_buy_budget=ZERO,
        star_buy_budget=ZERO,
        star_buy_quantity=0,
        average_buy_price=_round_cent(avg_price) if avg_price > ZERO else ZERO,
        average_buy_budget=ZERO,
        average_buy_quantity=0,
        quarter_sell_quantity=0,
        target_sell_quantity=0,
        target_sell_price=target_sell_price,
        reverse_entry=t > Decimal("19"),
        fail_closed=True,
        reason=reason,
    )


def calculate_normal_plan(state: NormalState) -> NormalPlan:
    """Calculate the normal-mode official SOXL 20-split plan."""

    if state.split != 20 or state.ticker.upper() != "SOXL":
        return _empty_normal_plan(state, phase="UNSUPPORTED", reason="only SOXL 20-split is supported")

    quantity = _floor_int(state.quantity)
    avg_price = _parse_decimal(state.avg_price)
    cash = _parse_decimal(state.cash)
    t = _parse_decimal(state.t)
    if avg_price is None or cash is None or t is None:
        return _empty_normal_plan(state, phase="FAIL_CLOSED", reason="invalid Decimal input")
    denominator = TWENTY - t

    if t < ZERO:
        return _empty_normal_plan(state, phase="FAIL_CLOSED", reason="invalid T domain: T must be non-negative", t=t, avg_price=avg_price)
    if t > Decimal("19"):
        return _empty_normal_plan(state, phase="REVERSE_ENTRY", reason="T exceeds normal-mode range", t=t, avg_price=avg_price)
    if quantity <= 0:
        return _empty_normal_plan(state, phase="FAIL_CLOSED", reason="quantity must be positive", t=t, avg_price=avg_price)
    if avg_price <= ZERO:
        return _empty_normal_plan(state, phase="FAIL_CLOSED", reason="average price must be positive", t=t, avg_price=avg_price)
    if cash <= ZERO:
        return _empty_normal_plan(state, phase="FAIL_CLOSED", reason="cash must be positive", t=t, avg_price=avg_price)
    if denominator <= ZERO:
        return _empty_normal_plan(state, phase="FAIL_CLOSED", reason="normal buy denominator must be positive", t=t, avg_price=avg_price)

    star_percent = TWENTY - (Decimal("2") * t)
    star_raw = avg_price * (ONE + (star_percent / Decimal("100")))
    star_point = _round_cent(star_raw)
    star_buy_price = star_point - CENT
    one_buy_budget = cash / denominator

    phase = "FIRST_HALF" if t < Decimal("10") else "SECOND_HALF"
    if phase == "FIRST_HALF":
        star_buy_budget = one_buy_budget / Decimal("2")
        average_buy_budget = one_buy_budget / Decimal("2")
        average_buy_price = _round_cent(avg_price)
    else:
        star_buy_budget = one_buy_budget
        average_buy_budget = ZERO
        average_buy_price = _round_cent(avg_price)

    star_budget_quantity = _quantity_for_budget(star_buy_budget, star_buy_price)
    average_budget_quantity = _quantity_for_budget(average_buy_budget, average_buy_price)
    if star_budget_quantity < 1 or (average_buy_budget > ZERO and average_budget_quantity < 1):
        return _empty_normal_plan(
            state,
            phase="FAIL_CLOSED",
            reason="insufficient cash: computed buy budget cannot buy one share",
            t=t,
            avg_price=avg_price,
        )

    star_buy_quantity = min(
        star_budget_quantity,
        _quantity_for_budget(cash, star_buy_price),
    )
    remaining_cash_after_star = cash - (Decimal(star_buy_quantity) * star_buy_price)
    average_buy_quantity = min(
        _quantity_for_budget(average_buy_budget, average_buy_price),
        _quantity_for_budget(remaining_cash_after_star, average_buy_price),
    )

    quarter_sell_quantity = quantity // 4
    target_sell_quantity = max(quantity - quarter_sell_quantity, 0)
    target_sell_price = _round_cent(avg_price * Decimal("1.20"))

    return NormalPlan(
        ticker=state.ticker,
        split=state.split,
        t=t,
        phase=phase,
        star_percent=star_percent,
        star_raw=star_raw,
        star_point=star_point,
        star_buy_price=star_buy_price,
        one_buy_budget=one_buy_budget,
        star_buy_budget=star_buy_budget,
        star_buy_quantity=star_buy_quantity,
        average_buy_price=average_buy_price,
        average_buy_budget=average_buy_budget,
        average_buy_quantity=average_buy_quantity,
        quarter_sell_quantity=quarter_sell_quantity,
        target_sell_quantity=target_sell_quantity,
        target_sell_price=target_sell_price,
        reverse_entry=False,
    )


def apply_fill_event(t_before: Decimal, event: FillEvent) -> Decimal:
    """Apply the official normal-mode T event table."""

    t = _decimal(t_before)
    fill_event = FillEvent(event)
    if fill_event is FillEvent.FULL_BUY:
        return t + ONE
    if fill_event is FillEvent.HALF_BUY:
        return t + Decimal("0.5")
    if fill_event is FillEvent.QUARTER_SELL:
        return t * Decimal("0.75")
    if fill_event is FillEvent.TARGET_SELL_THEN_FULL_BUY:
        return (t * Decimal("0.25")) + ONE
    if fill_event is FillEvent.TARGET_SELL_THEN_HALF_BUY:
        return (t * Decimal("0.25")) + Decimal("0.5")
    raise ValueError(f"unsupported fill event: {event!r}")


def _empty_reverse_plan(state: ReverseState, *, reason: str = "", t: Decimal | None = None) -> ReversePlan:
    t = _parse_decimal(state.t) if t is None else t
    if t is None:
        t = ZERO
    return ReversePlan(
        ticker=state.ticker,
        split=state.split,
        t=t,
        day=state.day,
        reverse_entry=t > Decimal("19"),
        return_to_normal=False,
        t_carry=t,
        star_point=ZERO,
        star_buy_price=ZERO,
        sell_quantity=0,
        sell_order_type="NONE",
        buy_budget=ZERO,
        buy_quantity=0,
        t_after_sell=t,
        t_after_buy=t,
        moc_sell_required=False,
        fail_closed=True,
        reason=reason,
    )


def _previous_five_close_average(previous_closes: Iterable[object]) -> Decimal:
    closes = [_parse_decimal(close) for close in previous_closes]
    if any(close is None for close in closes):
        return ZERO
    if len(closes) != 5 or any(close <= ZERO for close in closes):
        return ZERO
    return sum(closes, ZERO) / Decimal("5")


def calculate_reverse_plan(state: ReverseState) -> ReversePlan:
    """Calculate the reverse-mode official SOXL 20-split plan."""

    if state.split != 20 or state.ticker.upper() != "SOXL":
        return _empty_reverse_plan(state, reason="only SOXL 20-split is supported")
    if state.day <= 0:
        return _empty_reverse_plan(state, reason="invalid reverse day domain: day must be >= 1")

    quantity = _floor_int(state.quantity)
    previous_quantity = _floor_int(state.previous_quantity if state.previous_quantity is not None else state.quantity)
    avg_price = _parse_decimal(state.avg_price)
    cash = _parse_decimal(state.cash)
    t = _parse_decimal(state.t)
    if avg_price is None or cash is None or t is None:
        return _empty_reverse_plan(state, reason="invalid Decimal input")

    if quantity <= 0 or avg_price <= ZERO or t <= ZERO:
        return _empty_reverse_plan(state, reason="quantity, average price, and T must be positive", t=t)

    confirmed_close = _parse_decimal(state.confirmed_close) if state.confirmed_close is not None else None
    if state.confirmed_close is not None and confirmed_close is None:
        return _empty_reverse_plan(state, reason="invalid Decimal input", t=t)
    if confirmed_close is not None and confirmed_close > (avg_price * Decimal("0.80")):
        return ReversePlan(
            ticker=state.ticker,
            split=state.split,
            t=t,
            day=state.day,
            reverse_entry=False,
            return_to_normal=True,
            t_carry=t,
            star_point=ZERO,
            star_buy_price=ZERO,
            sell_quantity=0,
            sell_order_type="NONE",
            buy_budget=ZERO,
            buy_quantity=0,
            t_after_sell=t,
            t_after_buy=t,
            moc_sell_required=False,
        )

    if state.day <= 1:
        sell_quantity = quantity // 10
        t_after_sell = t * Decimal("0.9")
        return ReversePlan(
            ticker=state.ticker,
            split=state.split,
            t=t,
            day=state.day,
            reverse_entry=t > Decimal("19"),
            return_to_normal=False,
            t_carry=t_after_sell,
            star_point=ZERO,
            star_buy_price=ZERO,
            sell_quantity=sell_quantity,
            sell_order_type="MOC" if sell_quantity > 0 else "NONE",
            buy_budget=ZERO,
            buy_quantity=0,
            t_after_sell=t_after_sell,
            t_after_buy=t_after_sell,
            moc_sell_required=False,
        )

    star_average = _previous_five_close_average(state.previous_closes)
    if star_average <= ZERO or cash <= ZERO:
        return _empty_reverse_plan(state, reason="previous five closes and cash are required after first day", t=t)

    star_point = _round_cent(star_average)
    star_buy_price = star_point - CENT
    sell_quantity = previous_quantity // 10
    t_after_sell = t * Decimal("0.9")
    buy_budget = _round_cent(cash / Decimal("4"))
    buy_quantity = _quantity_for_budget(buy_budget, star_buy_price)
    moc_sell_required = buy_quantity < 1
    sell_order_type = "MOC" if moc_sell_required else "LOC"
    t_after_buy = t_after_sell if moc_sell_required else t_after_sell + ((TWENTY - t_after_sell) * Decimal("0.25"))

    return ReversePlan(
        ticker=state.ticker,
        split=state.split,
        t=t,
        day=state.day,
        reverse_entry=True,
        return_to_normal=False,
        t_carry=t_after_buy,
        star_point=star_point,
        star_buy_price=star_buy_price,
        sell_quantity=sell_quantity,
        sell_order_type=sell_order_type,
        buy_budget=buy_budget,
        buy_quantity=buy_quantity,
        t_after_sell=t_after_sell,
        t_after_buy=t_after_buy,
        moc_sell_required=moc_sell_required,
    )
