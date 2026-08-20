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


@pytest.mark.parametrize(
    "field,value",
    [
        ("ticker", "TQQQ"),
        ("event_type", "DROP_TABLE"),
        ("order_type", "SHELL"),
    ],
)
def test_official_intent_rejects_values_outside_semantic_domain(tmp_path, field, value):
    store, _path = make_store(tmp_path)

    with pytest.raises(InvalidOrderIntentError, match=field):
        store.create_planned(planned_intent(**{field: value}))

    created = store.create_planned(planned_intent(ticker="SOXL", event_type="FULL_BUY", order_type="LOC"))
    assert created["ticker"] == "SOXL"
    assert created["event_type"] == "FULL_BUY"
    assert created["order_type"] == "LOC"


def test_manual_event_type_is_allowed_for_sync_manual_order_intents(tmp_path):
    store, _path = make_store(tmp_path)

    created = store.create_planned(planned_intent(event_type="MANUAL"))

    assert created["event_type"] == "MANUAL"


@pytest.mark.parametrize("field", ["qty", "strategy_revision", "t_revision"])
@pytest.mark.parametrize("value", [1.9, "1.9"])
def test_official_intent_rejects_non_integer_numeric_values(tmp_path, field, value):
    store, _path = make_store(tmp_path)

    with pytest.raises(InvalidOrderIntentError, match=field):
        store.create_planned(planned_intent(**{field: value}))


@pytest.mark.parametrize("price", ["abc", "", "NaN", "Infinity", "-1", "0"])
def test_compute_intent_id_rejects_invalid_official_prices(price):
    with pytest.raises(InvalidOrderIntentError, match="price"):
        compute_intent_id(planned_intent(price=price))


@pytest.mark.parametrize("price", ["abc", "", "NaN", "Infinity", "-1", "0"])
def test_create_planned_rejects_invalid_official_prices_without_appending(tmp_path, price):
    store, path = make_store(tmp_path)

    with pytest.raises(InvalidOrderIntentError, match="price"):
        store.create_planned(planned_intent(price=price))

    assert path.read_text(encoding="utf-8") == ""


@pytest.mark.parametrize("price", ["131.76", "0.01", "131.760"])
def test_official_price_accepts_positive_decimal_strings_and_preserves_canonical_text(
    tmp_path, price
):
    store, _path = make_store(tmp_path)

    created = store.create_planned(planned_intent(price=price))

    assert created["price"] == price
    expected = hashlib.sha256(
        f"SOXL|2026-08-11|1|FULL_BUY|BUY|{price}|6".encode("utf-8")
    ).hexdigest()
    assert created["intent_id"] == expected
    assert compute_intent_id(planned_intent(price=price)) == expected


@pytest.mark.parametrize("field", ["qty", "strategy_revision", "t_revision"])
def test_official_intent_accepts_digit_integer_strings(tmp_path, field):
    provider = (lambda _ticker: 1) if field == "t_revision" else current_revision_provider
    store, _path = make_store(tmp_path, provider=provider)
    overrides = {field: "1"}
    if field != "t_revision":
        overrides["t_revision"] = 3

    created = store.create_planned(planned_intent(**overrides))

    assert created[field] == 1


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


def test_ledger_rejects_invalid_created_at_timestamp(tmp_path):
    store, path = make_store(tmp_path)
    record = dict(planned_intent())
    record["intent_id"] = compute_intent_id(record)
    record["status"] = "PLANNED"
    record["created_at"] = "not-a-timestamp"
    path.write_text(json.dumps(record) + "\n", encoding="utf-8")

    with pytest.raises(OrderIntentLedgerCorruptError, match="created_at"):
        store.list_intents("SOXL")


def test_ledger_rejects_extra_fields_exact_schema(tmp_path):
    store, path = make_store(tmp_path)
    record = dict(planned_intent())
    record["intent_id"] = compute_intent_id(record)
    record["status"] = "PLANNED"
    record["created_at"] = "2026-08-11T12:34:56Z"
    record["unexpected"] = "must fail closed"
    path.write_text(json.dumps(record) + "\n", encoding="utf-8")

    with pytest.raises(OrderIntentLedgerCorruptError, match="schema"):
        store.list_intents("SOXL")


def test_record_accepted_order_persists_durable_matching_key_fields_without_changing_planned_schema(tmp_path):
    import order_intent_store
    store, path = make_store(tmp_path)
    created = store.create_planned(planned_intent())
    planned_raw = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    assert set(planned_raw) == set(order_intent_store.LEDGER_FIELDS)

    submitted = store.record_accepted_order(created["intent_id"], {
        "account_fingerprint": "acct-A",
        "ticker": "SOXL",
        "exchange": "AMEX",
        "trade_date": "20260811",
        "order_no": "ODNO-ACCEPTED-1",
    })

    assert submitted["status"] == "SUBMITTED"
    assert submitted["accepted_order"] == {
        "account_fingerprint": "acct-A",
        "ticker": "SOXL",
        "exchange": "AMEX",
        "trade_date": "20260811",
        "order_no": "ODNO-ACCEPTED-1",
        "matching_key": "acct-A|SOXL|AMEX|20260811|ODNO-ACCEPTED-1",
    }
    assert store.list_intents("SOXL")[-1]["accepted_order"]["matching_key"] == "acct-A|SOXL|AMEX|20260811|ODNO-ACCEPTED-1"


@pytest.mark.parametrize("missing", ["account_fingerprint", "ticker", "exchange", "trade_date", "order_no"])
def test_record_accepted_order_requires_full_key_and_rejects_odno_alone(tmp_path, missing):
    store, _path = make_store(tmp_path)
    created = store.create_planned(planned_intent())
    accepted = {
        "account_fingerprint": "acct-A",
        "ticker": "SOXL",
        "exchange": "AMEX",
        "trade_date": "20260811",
        "order_no": "ODNO-ACCEPTED-1",
    }
    accepted.pop(missing)

    with pytest.raises(InvalidOrderIntentError, match=missing):
        store.record_accepted_order(created["intent_id"], accepted)

    assert store.list_intents("SOXL")[-1]["status"] == "PLANNED"
