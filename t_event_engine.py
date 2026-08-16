"""Append-only T event validation and application for Laoer V4 state.

T is a strategy state value.  After the approved baseline it must be advanced
only by confirmed fill events, never by cost-basis inverse arithmetic.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Mapping, Any

from laoer_v4_20 import apply_fill_event as _kernel_apply_fill_event

# Gap 2: 리버스 쿼터매수 T 증가 정산 배선.
# 커널 laoer_v4_20.calculate_reverse_plan 은 리버스 쿼터매수 T 공식
#   t_after_buy = t_after_sell + (20 - t_after_sell) * 0.25
# 을 이미 정확히 계산하지만, 정산 레이어의 T 이벤트 테이블(apply_fill_event)에는
# 리버스 쿼터매수 이벤트가 없어 일반매수(T+1)로 잘못 연결된다. 커널을 건드리지 않고
# 정산 레이어에서 리버스 쿼터매수 이벤트를 별도 배선한다.
REVERSE_QUARTER_BUY_EVENT = "REVERSE_QUARTER_BUY"
REVERSE_SELL_EVENT = "REVERSE_SELL"
_REVERSE_SPLIT = Decimal("20")
_REVERSE_QUARTER_RATIO = Decimal("0.25")
_REVERSE_SELL_RATIO = Decimal("0.9")


def apply_fill_event_extended(t_before, event_type):
    """Apply a fill event to T, including the reverse sell/buy rules.

    Reverse quarter-buy (BUY in reverse mode) advances T by
    ``T + (20 - T) * 0.25``, distinct from the normal-mode FULL buy (``T + 1``).

    Reverse sell (SELL in reverse mode) scales T by ``T * 0.9``, distinct
    from the normal-mode QUARTER sell (``T * 0.75``).
    All other event types delegate to the kernel's normal-mode table.
    """
    if str(event_type).upper() == REVERSE_SELL_EVENT:
        t = parse_decimal(t_before, "t_before")
        return t * _REVERSE_SELL_RATIO
    if str(event_type).upper() == REVERSE_QUARTER_BUY_EVENT:
        t = parse_decimal(t_before, "t_before")
        return t + ((_REVERSE_SPLIT - t) * _REVERSE_QUARTER_RATIO)
    return _kernel_apply_fill_event(t_before, event_type)


@dataclass(frozen=True)
class TState:
    ticker: str
    t: Decimal
    revision: int
    available_cash: Decimal
    reverse_active: bool = False


REQUIRED_EVENT_KEYS = {
    "event_id",
    "ticker",
    "intent_id",
    "kis_order_no",
    "fill_key",
    "event_type",
    "filled_qty",
    "filled_amount",
    "t_before",
    "t_after",
    "revision_before",
    "revision_after",
    "occurred_at",
}


def parse_decimal(value: object, field: str) -> Decimal:
    if not isinstance(value, (str, int, float, Decimal)):
        raise ValueError(f"{field} must be decimal-compatible")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a finite Decimal") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field} must be a finite Decimal")
    return parsed


def parse_int(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    return value


def validate_t_event(event: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(event, Mapping):
        raise ValueError("T event must be a JSON object")
    keys = set(event.keys())
    if keys != REQUIRED_EVENT_KEYS:
        missing = sorted(REQUIRED_EVENT_KEYS - keys)
        extra = sorted(keys - REQUIRED_EVENT_KEYS)
        raise ValueError(f"T event schema mismatch missing={missing} extra={extra}")

    validated = dict(event)
    for field in ("event_id", "ticker", "intent_id", "kis_order_no", "fill_key", "event_type", "occurred_at"):
        if not isinstance(validated[field], str) or not validated[field]:
            raise ValueError(f"{field} must be a non-empty string")
    if validated["ticker"] != validated["ticker"].upper():
        raise ValueError("ticker must be uppercase")
    parse_int(validated["filled_qty"], "filled_qty")
    if validated["filled_qty"] < 0:
        raise ValueError("filled_qty must be non-negative")
    parse_decimal(validated["filled_amount"], "filled_amount")
    parse_decimal(validated["t_before"], "t_before")
    parse_decimal(validated["t_after"], "t_after")
    parse_int(validated["revision_before"], "revision_before")
    parse_int(validated["revision_after"], "revision_after")
    try:
        datetime.fromisoformat(validated["occurred_at"].replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("occurred_at must be ISO-8601") from exc
    return validated


def apply_t_event(current_t: Decimal, current_revision: int, event: Mapping[str, Any]) -> TState:
    validated = validate_t_event(event)
    t_before = parse_decimal(validated["t_before"], "t_before")
    t_after = parse_decimal(validated["t_after"], "t_after")
    revision_before = parse_int(validated["revision_before"], "revision_before")
    revision_after = parse_int(validated["revision_after"], "revision_after")

    if t_before != current_t:
        raise ValueError(f"t_before mismatch: expected {current_t}, got {t_before}")
    if revision_before != current_revision:
        raise ValueError(f"revision_before mismatch: expected {current_revision}, got {revision_before}")
    if revision_after != revision_before + 1:
        raise ValueError("revision_after must equal revision_before + 1")

    kernel_t_after = apply_fill_event_extended(t_before, validated["event_type"])
    if kernel_t_after != t_after:
        raise ValueError(f"t_after mismatch: kernel expected {kernel_t_after}, got {t_after}")

    return TState(
        ticker=validated["ticker"],
        t=t_after,
        revision=revision_after,
        available_cash=Decimal("0"),
        reverse_active=False,
    )


def get_current_t_from_ledger(baseline_path, events_path, ticker: str, actual_qty=None, actual_avg_price=None) -> TState:
    # actual_qty/actual_avg_price are intentionally ignored: T is not inferred
    # from cost basis.
    from trade_state_store import TradeStateStore

    return TradeStateStore(baseline_path, events_path).load_state(ticker)
