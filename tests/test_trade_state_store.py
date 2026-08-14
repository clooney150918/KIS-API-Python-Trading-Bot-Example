import json
import os
from decimal import Decimal

import pytest

from trade_state_store import (
    BaselineImmutableError,
    BaselineValidationError,
    DuplicateTEventError,
    TEventLedgerCorruptError,
    TradeStateStore,
)


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


def event(**overrides):
    payload = {
        "event_id": "evt-1",
        "ticker": "SOXL",
        "intent_id": "intent-1",
        "kis_order_no": "kis-1",
        "fill_key": "kis-1:fill-1",
        "event_type": "FULL_BUY",
        "filled_qty": 1,
        "filled_amount": "100.00",
        "t_before": "18.32",
        "t_after": "19.32",
        "revision_before": 1,
        "revision_after": 2,
        "occurred_at": "2026-08-11T20:30:00Z",
    }
    payload.update(overrides)
    return payload


def test_load_baseline_requires_exact_immutable_schema(tmp_path):
    baseline_path = tmp_path / "baseline.json"
    events_path = tmp_path / "events.jsonl"
    write_json(baseline_path, BASELINE)

    loaded = TradeStateStore(baseline_path, events_path).load_baseline("SOXL")

    assert loaded["ticker"] == "SOXL"
    assert loaded["t"] == "18.32"
    assert loaded["immutable"] is True

    mutated = dict(BASELINE)
    mutated["unexpected"] = "not allowed"
    write_json(baseline_path, mutated)
    with pytest.raises(BaselineValidationError):
        TradeStateStore(baseline_path, events_path).load_baseline("SOXL")

    mutated = dict(BASELINE)
    del mutated["t"]
    write_json(baseline_path, mutated)
    with pytest.raises(BaselineValidationError):
        TradeStateStore(baseline_path, events_path).load_baseline("SOXL")

    mutated = dict(BASELINE)
    mutated["immutable"] = False
    write_json(baseline_path, mutated)
    with pytest.raises(BaselineImmutableError):
        TradeStateStore(baseline_path, events_path).load_baseline("SOXL")


@pytest.mark.parametrize(
    ("field", "mutated_value"),
    [
        ("as_of", "2026-08-12"),
        ("qty", 999),
        ("avg_price", "1.00"),
        ("available_cash", "9999.99"),
        ("t", "0.01"),
        ("reverse_active", True),
        ("legacy_execution_count", 73),
    ],
)
def test_approved_immutable_baseline_values_cannot_be_mutated(tmp_path, field, mutated_value):
    baseline_path = tmp_path / "baseline.json"
    events_path = tmp_path / "events.jsonl"
    mutated = dict(BASELINE)
    mutated[field] = mutated_value
    write_json(baseline_path, mutated)

    with pytest.raises(BaselineImmutableError, match="baseline mutation|approved baseline"):
        TradeStateStore(baseline_path, events_path).load_baseline("SOXL")


def test_append_event_is_atomic_and_revision_monotonic(tmp_path):
    baseline_path = tmp_path / "baseline.json"
    events_path = tmp_path / "events.jsonl"
    write_json(baseline_path, BASELINE)
    events_path.write_text("", encoding="utf-8")
    store = TradeStateStore(baseline_path, events_path)

    state = store.append_event(event())

    assert state.t == Decimal("19.32")
    assert state.revision == 2
    lines = events_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["event_id"] == "evt-1"
    assert not (tmp_path / "events.jsonl.tmp").exists()


def test_missing_event_ledger_halts_instead_of_returning_baseline_state(tmp_path):
    baseline_path = tmp_path / "baseline.json"
    events_path = tmp_path / "events.jsonl"
    write_json(baseline_path, BASELINE)

    store = TradeStateStore(baseline_path, events_path)

    with pytest.raises(TEventLedgerCorruptError, match="ledger.*missing|missing.*ledger"):
        store.load_state("SOXL")


def test_duplicate_event_id_or_fill_key_rejected_fail_closed(tmp_path):
    baseline_path = tmp_path / "baseline.json"
    events_path = tmp_path / "events.jsonl"
    write_json(baseline_path, BASELINE)
    events_path.write_text("", encoding="utf-8")
    store = TradeStateStore(baseline_path, events_path)
    store.append_event(event())

    with pytest.raises(DuplicateTEventError):
        store.append_event(event(event_id="evt-1", fill_key="kis-1:fill-2", t_before="19.32", t_after="20.32", revision_before=2, revision_after=3))

    with pytest.raises(DuplicateTEventError):
        store.append_event(event(event_id="evt-2", fill_key="kis-1:fill-1", t_before="19.32", t_after="20.32", revision_before=2, revision_after=3))

    assert len(events_path.read_text(encoding="utf-8").splitlines()) == 1


def test_broken_last_jsonl_line_halts_without_truncation(tmp_path):
    baseline_path = tmp_path / "baseline.json"
    events_path = tmp_path / "events.jsonl"
    write_json(baseline_path, BASELINE)
    events_path.write_text(json.dumps(event()) + "\n" + '{"event_id": "broken"', encoding="utf-8")
    original = events_path.read_text(encoding="utf-8")

    store = TradeStateStore(baseline_path, events_path)
    with pytest.raises(TEventLedgerCorruptError):
        store.load_state("SOXL")
    with pytest.raises(TEventLedgerCorruptError):
        store.append_event(event(event_id="evt-2", fill_key="fill-2"))

    assert events_path.read_text(encoding="utf-8") == original


def test_append_rejects_t_or_revision_mismatch(tmp_path):
    baseline_path = tmp_path / "baseline.json"
    events_path = tmp_path / "events.jsonl"
    write_json(baseline_path, BASELINE)
    events_path.write_text("", encoding="utf-8")
    store = TradeStateStore(baseline_path, events_path)

    with pytest.raises(ValueError, match="t_before"):
        store.append_event(event(t_before="18.31"))
    with pytest.raises(ValueError, match="revision_before"):
        store.append_event(event(revision_before=2, revision_after=3))
    with pytest.raises(ValueError, match="revision_after"):
        store.append_event(event(revision_after=4))
