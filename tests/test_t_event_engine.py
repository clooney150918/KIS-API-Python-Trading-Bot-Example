import json
from decimal import Decimal

import pytest

from t_event_engine import apply_t_event, get_current_t_from_ledger


BASELINE = {
    "schema_version": 1,
    "ticker": "SOXL",
    "as_of": "2026-08-11",
    "qty": 98,
    "avg_price": "158.0735",
    "available_cash": "1482.88",
    "t": "18.32",
    "reverse_active": False,
    "source": "CEO_APPROVED_KIS_BASELINE",
    "legacy_execution_count": 72,
    "immutable": True,
}


def write_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def test_apply_t_event_delegates_event_type_to_laoer_kernel():
    event = {
        "event_id": "evt-full",
        "ticker": "SOXL",
        "intent_id": "intent-1",
        "kis_order_no": "kis-1",
        "fill_key": "kis-1:fill-1",
        "event_type": "FULL_BUY",
        "filled_qty": 3,
        "filled_amount": "300.00",
        "t_before": "18.32",
        "t_after": "19.32",
        "revision_before": 1,
        "revision_after": 2,
        "occurred_at": "2026-08-11T20:30:00Z",
    }

    state = apply_t_event(Decimal("18.32"), 1, event)

    assert state.t == Decimal("19.32")
    assert state.revision == 2


def test_apply_t_event_validates_untyped_json_fields_before_kernel_dataclasses():
    event = {
        "event_id": "evt-bad",
        "ticker": "SOXL",
        "intent_id": "intent-1",
        "kis_order_no": "kis-1",
        "fill_key": "kis-1:fill-1",
        "event_type": "FULL_BUY",
        "filled_qty": "not-an-int",
        "filled_amount": "300.00",
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
    event = {
        "event_id": "evt-half",
        "ticker": "SOXL",
        "intent_id": "intent-2",
        "kis_order_no": "kis-2",
        "fill_key": "kis-2:fill-1",
        "event_type": "HALF_BUY",
        "filled_qty": 1,
        "filled_amount": "100.00",
        "t_before": "18.32",
        "t_after": "18.82",
        "revision_before": 1,
        "revision_after": 2,
        "occurred_at": "2026-08-11T21:30:00Z",
    }
    events_path.write_text(json.dumps(event) + "\n", encoding="utf-8")

    first = get_current_t_from_ledger(baseline_path, events_path, "SOXL", actual_qty=98, actual_avg_price=158.0735)
    second = get_current_t_from_ledger(baseline_path, events_path, "SOXL", actual_qty=999, actual_avg_price=1)

    assert first.t == second.t == Decimal("18.82")
    assert first.revision == second.revision == 2
