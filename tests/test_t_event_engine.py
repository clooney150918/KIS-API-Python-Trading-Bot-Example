import json
from decimal import Decimal, localcontext

import pytest

from t_event_engine import apply_fill_event_extended, apply_t_event, get_current_t_from_ledger
from trade_state_store import APPROVED_BASELINE


def _valid_event(**overrides):
    split_amount = Decimal("882.95")
    expected = BASELINE_T + (Decimal("300.00") / split_amount)
    event = {
        "event_id": "evt-full",
        "ticker": "SOXL",
        "intent_id": "intent-1",
        "kis_order_no": "kis-1",
        "fill_key": "kis-1:fill-1",
        "event_type": "FULL_BUY",
        "side": "BUY",
        "filled_qty": 3,
        "filled_amount": "300.00",
        "split_amount": "882.95",
        "t_before": str(BASELINE_T),
        "t_after": str(expected),
        "revision_before": 1,
        "revision_after": 2,
        "occurred_at": "2026-08-11T20:30:00Z",
    }
    event.update(overrides)
    return event


BASELINE = dict(APPROVED_BASELINE)
BASELINE_T = Decimal(BASELINE["t"])


def write_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def test_apply_t_event_delegates_event_type_to_laoer_kernel():
    split_amount = Decimal("882.95")
    expected = BASELINE_T + (Decimal("300.00") / split_amount)
    event = {
        "event_id": "evt-full",
        "ticker": "SOXL",
        "intent_id": "intent-1",
        "kis_order_no": "kis-1",
        "fill_key": "kis-1:fill-1",
        "event_type": "FULL_BUY",
        "side": "BUY",
        "filled_qty": 3,
        "filled_amount": "300.00",
        "split_amount": "882.95",
        "t_before": str(BASELINE_T),
        "t_after": str(expected),
        "revision_before": 1,
        "revision_after": 2,
        "occurred_at": "2026-08-11T20:30:00Z",
    }

    state = apply_t_event(BASELINE_T, 1, event)

    assert state.t == expected
    assert state.revision == 2


def test_apply_fill_event_extended_moves_t_by_fill_amount_over_split_amount_for_sell():
    assert apply_fill_event_extended(
        Decimal("14.7400"),
        "QUARTER_SELL",
        filled_amount=Decimal("441.475"),
        split_amount=Decimal("882.95"),
    ) == Decimal("14.2400")


def test_apply_fill_event_extended_keeps_decimal_precision_for_fractional_delta():
    expected = Decimal("1.2345") + (Decimal("0.10") / Decimal("882.95"))
    assert apply_fill_event_extended(
        Decimal("1.2345"),
        "FULL_BUY",
        side="BUY",
        filled_amount=Decimal("0.10"),
        split_amount=Decimal("882.95"),
    ) == expected


def test_apply_t_event_validates_untyped_json_fields_before_kernel_dataclasses():
    event = {
        "event_id": "evt-bad",
        "ticker": "SOXL",
        "intent_id": "intent-1",
        "kis_order_no": "kis-1",
        "fill_key": "kis-1:fill-1",
        "event_type": "FULL_BUY",
        "side": "BUY",
        "filled_qty": "not-an-int",
        "filled_amount": "300.00",
        "split_amount": "882.95",
        "t_before": "18.32",
        "t_after": "19.32",
        "revision_before": 1,
        "revision_after": 2,
        "occurred_at": "2026-08-11T20:30:00Z",
    }

    with pytest.raises(ValueError, match="filled_qty"):
        apply_t_event(Decimal("18.32"), 1, event)


def test_get_current_t_from_ledger_ignores_qty_and_avg_price_arguments(tmp_path):
    baseline_path = tmp_path / "baseline.json"
    events_path = tmp_path / "events.jsonl"
    write_json(baseline_path, BASELINE)
    split_amount = Decimal("882.95")
    expected = BASELINE_T + (Decimal("100.00") / split_amount)
    event = {
        "event_id": "evt-half",
        "ticker": "SOXL",
        "intent_id": "intent-2",
        "kis_order_no": "kis-2",
        "fill_key": "kis-2:fill-1",
        "event_type": "HALF_BUY",
        "side": "BUY",
        "filled_qty": 1,
        "filled_amount": "100.00",
        "split_amount": "882.95",
        "t_before": str(BASELINE_T),
        "t_after": str(expected),
        "revision_before": 1,
        "revision_after": 2,
        "occurred_at": "2026-08-11T21:30:00Z",
    }
    events_path.write_text(json.dumps(event) + "\n", encoding="utf-8")

    first = get_current_t_from_ledger(baseline_path, events_path, "SOXL", actual_qty=98, actual_avg_price=158.0735)
    second = get_current_t_from_ledger(baseline_path, events_path, "SOXL", actual_qty=999, actual_avg_price=1)

    assert first.t == second.t == expected
    assert first.revision == second.revision == 2


def test_reverse_sell_uses_same_proportional_subtraction():
    expected = Decimal("19.25") - (Decimal("882.95") / Decimal("882.95"))
    assert apply_fill_event_extended(
        Decimal("19.25"),
        "REVERSE_SELL",
        side="SELL",
        filled_amount=Decimal("882.95"),
        split_amount=Decimal("882.95"),
    ) == expected


def test_apply_fill_event_extended_clamps_t_to_zero_when_sell_exceeds_current_t():
    # A large SELL fill would push T below zero; the lower-bound guard clamps it.
    result = apply_fill_event_extended(
        Decimal("1.00"),
        "QUARTER_SELL",
        side="SELL",
        filled_amount=Decimal("1765.90"),  # 2 * split_amount -> delta = 2
        split_amount=Decimal("882.95"),
    )
    assert result == Decimal("0")


def test_apply_fill_event_extended_pins_28_digit_precision_regardless_of_ambient_context():
    # Reference value computed under the default 28-digit context.
    expected = Decimal("14.7400") - (Decimal("441.475") / Decimal("882.95"))
    # Even if a caller shrinks the ambient decimal precision, T arithmetic must
    # stay at the fixed 28-digit working precision.
    with localcontext() as ctx:
        ctx.prec = 6
        result = apply_fill_event_extended(
            Decimal("14.7400"),
            "QUARTER_SELL",
            side="SELL",
            filled_amount=Decimal("441.475"),
            split_amount=Decimal("882.95"),
        )
    assert result == expected


def test_apply_t_event_requires_side():
    event = _valid_event()
    del event["side"]
    with pytest.raises(ValueError, match="schema mismatch"):
        apply_t_event(BASELINE_T, 1, event)


def test_apply_t_event_requires_split_amount():
    event = _valid_event()
    del event["split_amount"]
    with pytest.raises(ValueError, match="schema mismatch"):
        apply_t_event(BASELINE_T, 1, event)


def test_apply_t_event_rejects_t_after_not_derivable_from_rounded_filled_amount():
    # filled_amount is the stored 2-decimal value; t_after must be reproducible
    # from exactly that rounded amount (closed loop).  A t_after computed from an
    # unrounded amount must be rejected.
    unrounded_t_after = BASELINE_T + (Decimal("300.005") / Decimal("882.95"))
    event = _valid_event(filled_amount="300.00", t_after=str(unrounded_t_after))
    with pytest.raises(ValueError, match="t_after mismatch"):
        apply_t_event(BASELINE_T, 1, event)
