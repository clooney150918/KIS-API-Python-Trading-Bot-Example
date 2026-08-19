"""Append-only T event validation and application for Laoer V4 state.

T is a strategy state value.  After the approved baseline it must be advanced
only by confirmed fill events, never by cost-basis inverse arithmetic.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Mapping, Any

REVERSE_QUARTER_BUY_EVENT = "REVERSE_QUARTER_BUY"
REVERSE_SELL_EVENT = "REVERSE_SELL"


def infer_side_from_event_type(event_type: object) -> str:
    event = str(event_type).upper()
    if event in {"QUARTER", "QUARTER_SELL", "TARGET_FULL", "TARGET_HALF", "REVERSE_SELL"}:
        return "SELL"
    if event in {"FULL", "FULL_BUY", "HALF", "HALF_BUY", "BONUS", "BONUS_BUY", "REVERSE_QUARTER_BUY"}:
        return "BUY"
    raise ValueError(f"unsupported fill event side inference: {event_type!r}")


def apply_fill_event_extended(t_before, event_type, *, side=None, filled_amount=None, split_amount=None):
    """Apply proportional fill amount to T.

    New official events move T by ``filled_amount / split_amount``: BUY adds,
    SELL subtracts.  ``event_type`` remains audit metadata; it no longer
    contributes event coefficients.
    """
    if filled_amount is None or split_amount is None:
        raise ValueError("filled_amount and split_amount are required for proportional T")
    t = parse_decimal(t_before, "t_before")
    amount = parse_decimal(filled_amount, "filled_amount")
    portion = parse_decimal(split_amount, "split_amount")
    if amount < 0:
        raise ValueError("filled_amount must be non-negative")
    if portion <= 0:
        raise ValueError("split_amount must be positive")
    fill_side = str(side or infer_side_from_event_type(event_type)).upper()
    delta = amount / portion
    if fill_side == "BUY":
        return t + delta
    if fill_side == "SELL":
        return t - delta
    raise ValueError("side must be BUY or SELL")


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
OPTIONAL_EVENT_KEYS = {
    "side",
    "split_amount",
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
    if not REQUIRED_EVENT_KEYS.issubset(keys) or not keys.issubset(REQUIRED_EVENT_KEYS | OPTIONAL_EVENT_KEYS):
        missing = sorted(REQUIRED_EVENT_KEYS - keys)
        extra = sorted(keys - REQUIRED_EVENT_KEYS - OPTIONAL_EVENT_KEYS)
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
    if "side" in validated:
        if validated["side"] not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
    if "split_amount" in validated:
        split_amount = parse_decimal(validated["split_amount"], "split_amount")
        if split_amount <= 0:
            raise ValueError("split_amount must be positive")
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

    if "split_amount" in validated:
        proportional_t_after = apply_fill_event_extended(
            t_before,
            validated["event_type"],
            side=validated.get("side"),
            filled_amount=validated["filled_amount"],
            split_amount=validated["split_amount"],
        )
        if proportional_t_after != t_after:
            raise ValueError(f"t_after mismatch: proportional expected {proportional_t_after}, got {t_after}")

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
