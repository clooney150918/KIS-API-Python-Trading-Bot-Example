import hashlib
import json

import pytest

from order_intent_store import (
    DuplicateOrderIntentError,
    InvalidOrderIntentError,
    OrderIntentLedgerCorruptError,
    OrderIntentStore,
    StaleTRevisionError,
    compute_intent_id,
)


def current_revision_provider(_ticker):
    return 3


def make_store(tmp_path, provider=current_revision_provider):
    path = tmp_path / "order_intents_SOXL.jsonl"
    path.write_text("", encoding="utf-8")
    return OrderIntentStore(path, current_t_revision_provider=provider), path


def planned_intent(**overrides):
    data = {
        "strategy": "LAOER_V4_SOXL_20",
        "strategy_revision": 1,
        "t_revision": 3,
        "ticker": "SOXL",
        "trade_date": "2026-08-11",
        "event_type": "FULL_BUY",
        "side": "BUY",
        "order_type": "LOC",
        "price": "131.76",
        "qty": 6,
    }
    data.update(overrides)
    return data


def test_intent_id_is_exact_sha256_of_official_canonical_fields_and_stable(tmp_path):
    expected = hashlib.sha256(
        "SOXL|2026-08-11|1|FULL_BUY|BUY|131.76|6".encode("utf-8")
    ).hexdigest()

    assert compute_intent_id(planned_intent()) == expected
    assert compute_intent_id(planned_intent()) == expected
    assert compute_intent_id(planned_intent(strategy_revision=2)) != expected
    assert compute_intent_id(planned_intent(t_revision=4)) == expected


def test_create_planned_intent_appends_schema_and_rejects_required_event_type_missing(tmp_path):
    store, path = make_store(tmp_path)

    created = store.create_planned(planned_intent())

    assert created["intent_id"] == compute_intent_id(planned_intent())
    assert created["status"] == "PLANNED"
    assert created["created_at"].endswith("Z")
    assert json.loads(path.read_text(encoding="utf-8"))["intent_id"] == created["intent_id"]

    with pytest.raises(InvalidOrderIntentError, match="event_type"):
        store.create_planned(planned_intent(event_type=None))


def test_duplicate_intent_id_append_rejects_even_with_different_description(tmp_path):
    store, _path = make_store(tmp_path)
    store.create_planned(planned_intent(description="legacy first text"))

    with pytest.raises(DuplicateOrderIntentError):
        store.create_planned(planned_intent(description="legacy second text"))


def test_stale_t_revision_rejects_against_authoritative_revision_provider(tmp_path):
    store, _path = make_store(tmp_path, provider=lambda ticker: 4)

    with pytest.raises(StaleTRevisionError):
        store.create_planned(planned_intent(t_revision=3))


def test_missing_or_corrupt_ledger_fails_closed(tmp_path):
    missing_store = OrderIntentStore(
        tmp_path / "missing.jsonl", current_t_revision_provider=current_revision_provider
    )
    with pytest.raises(OrderIntentLedgerCorruptError, match="missing"):
        missing_store.list_intents("SOXL")

    corrupt_path = tmp_path / "corrupt.jsonl"
    corrupt_path.write_text(json.dumps(planned_intent())[:-1], encoding="utf-8")
    corrupt_store = OrderIntentStore(corrupt_path, current_t_revision_provider=current_revision_provider)
    with pytest.raises(OrderIntentLedgerCorruptError):
        corrupt_store.list_intents("SOXL")


def test_status_transitions_are_strict_and_persisted(tmp_path):
    store, path = make_store(tmp_path)
    created = store.create_planned(planned_intent())

    submitted = store.transition_status(created["intent_id"], "SUBMITTED")
    assert submitted["status"] == "SUBMITTED"

    partial = store.transition_status(created["intent_id"], "PARTIAL")
    assert partial["status"] == "PARTIAL"

    filled = store.transition_status(created["intent_id"], "FILLED")
    assert filled["status"] == "FILLED"

    assert [json.loads(line)["status"] for line in path.read_text(encoding="utf-8").splitlines()] == [
        "PLANNED",
        "SUBMITTED",
        "PARTIAL",
        "FILLED",
    ]

    with pytest.raises(InvalidOrderIntentError):
        store.transition_status(created["intent_id"], "SUBMITTED")


def test_invalid_status_transition_rejects_without_appending(tmp_path):
    store, path = make_store(tmp_path)
    created = store.create_planned(planned_intent())

    with pytest.raises(InvalidOrderIntentError):
        store.transition_status(created["intent_id"], "FILLED")

    assert len(path.read_text(encoding="utf-8").splitlines()) == 1
