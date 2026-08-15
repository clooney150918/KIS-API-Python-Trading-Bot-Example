import json
from decimal import Decimal
from pathlib import Path

import pytest

from order_intent_store import OrderIntentStore, compute_intent_id
from trade_state_store import APPROVED_BASELINE, TradeStateStore
from t_event_engine import REQUIRED_EVENT_KEYS
from fill_reconciler import (
    FillReconciler,
    FillReconciliationError,
    ProcessedFillStore,
    build_fill_key,
    build_matching_key,
    normalize_kis_execution,
)


def write_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def planned_intent(**overrides):
    data = {
        "strategy": "LAOER_V4_SOXL_20",
        "strategy_revision": 1,
        "t_revision": 1,
        "ticker": "SOXL",
        "trade_date": "2026-08-12",
        "event_type": "FULL_BUY",
        "side": "BUY",
        "order_type": "LIMIT",
        "price": "100.00",
        "qty": 4,
    }
    data.update(overrides)
    return data


def make_stores(tmp_path):
    baseline_path = tmp_path / "strategy_baseline_SOXL_2026-08-11.json"
    events_path = tmp_path / "t_events_SOXL.jsonl"
    intents_path = tmp_path / "order_intents_SOXL.jsonl"
    processed_path = tmp_path / "processed_fills_SOXL.jsonl"
    write_json(baseline_path, APPROVED_BASELINE)
    events_path.write_text("", encoding="utf-8")
    intents_path.write_text("", encoding="utf-8")
    processed_path.write_text("", encoding="utf-8")
    intent_store = OrderIntentStore(intents_path, current_t_revision_provider=lambda _ticker: 1)
    trade_store = TradeStateStore(baseline_path, events_path)
    processed_store = ProcessedFillStore(processed_path)
    return intent_store, trade_store, processed_store, events_path, processed_path


def kis_fill(**overrides):
    row = {
        "odno": "ODNO-1",
        "ord_dt": "20260812",
        "ord_tmd": "223015",
        "pdno": "SOXL",
        "ovrs_excg_cd": "AMEX",
        "sll_buy_dvsn_cd": "02",
        "ft_ccld_qty": "4",
        "ft_ccld_unpr3": "100.00",
    }
    row.update(overrides)
    return row


def accepted_key(**overrides):
    data = {
        "account_fingerprint": "acct-A",
        "ticker": "SOXL",
        "exchange": "AMEX",
        "trade_date": "20260812",
        "order_no": "ODNO-1",
    }
    data.update(overrides)
    return data


def submitted_intent(intent_store, *, accepted_order=None, **overrides):
    created = intent_store.create_planned(planned_intent(**overrides))
    if accepted_order is None:
        accepted_order = accepted_key(order_no=overrides.get("odno", "ODNO-1"))
    intent_store.record_accepted_order(created["intent_id"], accepted_order)
    return created


def test_fill_key_is_stable_but_matching_key_includes_account_ticker_exchange_date_and_order_number():
    first = normalize_kis_execution(kis_fill(), account_fingerprint="acct-A")
    second = normalize_kis_execution(kis_fill(pdno="TQQQ"), account_fingerprint="acct-B")

    assert build_fill_key(first) == "acct-A|SOXL|AMEX|20260812|ODNO-1|223015|BUY|4|100.00"
    assert build_fill_key(first) != build_fill_key(second)
    assert build_matching_key(first) != build_matching_key(second)
    assert build_matching_key(first).startswith("acct-A|SOXL|AMEX|20260812|ODNO-1")
    assert build_matching_key(first) != "ODNO-1"


def test_unclassified_manual_fill_halt_persists_across_duplicate_reconcile_runs(tmp_path):
    intent_store, trade_store, processed_store, events_path, _processed_path = make_stores(tmp_path)
    submitted_intent(intent_store, accepted_order=accepted_key(order_no="ODNO-1"), event_type="FULL_BUY", qty=4, price="100.00")
    reconciler = FillReconciler(intent_store, trade_store, processed_store, account_fingerprint="acct-A")
    manual_row = kis_fill(odno="MANUAL-DURABLE-1", ft_ccld_qty="1")

    first = reconciler.reconcile("SOXL", [manual_row])
    second = reconciler.reconcile("SOXL", [manual_row])

    assert first["operator_halt"] is True
    assert "UNCLASSIFIED_FILL" in first["codes"]
    assert second["operator_halt"] is True
    assert second["partial_open"] is True
    assert reconciler.forbid_new_orders("SOXL") is True
    assert intent_store.list_intents("SOXL")[-1]["status"] == "SUBMITTED"
    assert events_path.read_text(encoding="utf-8") == ""


def test_fill_key_scope_prevents_cross_account_or_date_duplicate_suppression(tmp_path):
    intent_store, trade_store, processed_store, events_path, processed_path = make_stores(tmp_path)
    first = submitted_intent(
        intent_store,
        accepted_order=accepted_key(account_fingerprint="acct-A", trade_date="20260812", order_no="ODNO-SAME"),
        trade_date="2026-08-12",
        event_type="FULL_BUY",
        qty=4,
        price="100.00",
    )
    second = submitted_intent(
        intent_store,
        accepted_order=accepted_key(account_fingerprint="acct-B", trade_date="20260813", order_no="ODNO-SAME"),
        trade_date="2026-08-13",
        event_type="HALF_BUY",
        qty=4,
        price="100.00",
    )

    result_a = FillReconciler(intent_store, trade_store, processed_store, account_fingerprint="acct-A").reconcile(
        "SOXL", [kis_fill(odno="ODNO-SAME", ord_dt="20260812", ord_tmd="223015")]
    )
    result_b = FillReconciler(intent_store, trade_store, processed_store, account_fingerprint="acct-B").reconcile(
        "SOXL", [kis_fill(odno="ODNO-SAME", ord_dt="20260813", ord_tmd="223015")]
    )

    assert result_a["new_fill_count"] == 1
    assert result_b["new_fill_count"] == 1
    latest = {record["intent_id"]: record for record in intent_store.list_intents("SOXL")}
    assert latest[first["intent_id"]]["status"] == "FILLED"
    assert latest[second["intent_id"]]["status"] == "FILLED"
    assert len(events_path.read_text(encoding="utf-8").splitlines()) == 2
    processed_lines = processed_path.read_text(encoding="utf-8").splitlines()
    assert len(processed_lines) == 2
    assert json.loads(processed_lines[0])["fill_key"] != json.loads(processed_lines[1])["fill_key"]



def test_external_unrelated_odno_account_or_exchange_does_not_match_existing_intent(tmp_path):
    intent_store, trade_store, processed_store, events_path, _processed_path = make_stores(tmp_path)
    submitted_intent(intent_store, accepted_order=accepted_key(order_no="ODNO-ACCEPTED"))
    reconciler = FillReconciler(intent_store, trade_store, processed_store, account_fingerprint="acct-A")

    result = reconciler.reconcile("SOXL", [kis_fill(odno="EXT-UNRELATED-1")])

    assert result["operator_halt"] is True
    assert "UNCLASSIFIED_FILL" in result["codes"]
    assert intent_store.list_intents("SOXL")[-1]["status"] == "SUBMITTED"
    assert events_path.read_text(encoding="utf-8") == ""


@pytest.mark.parametrize(
    "accepted_overrides,fill_overrides,reconciler_account",
    [
        ({"account_fingerprint": "acct-A"}, {}, "acct-B"),
        ({"exchange": "NYSE"}, {}, "acct-A"),
        ({"trade_date": "20260811"}, {}, "acct-A"),
        ({"order_no": "ODNO-DIFFERENT"}, {}, "acct-A"),
    ],
)
def test_reconciler_only_matches_exact_accepted_order_key(tmp_path, accepted_overrides, fill_overrides, reconciler_account):
    intent_store, trade_store, processed_store, events_path, _processed_path = make_stores(tmp_path)
    submitted_intent(intent_store, accepted_order=accepted_key(**accepted_overrides))
    reconciler = FillReconciler(intent_store, trade_store, processed_store, account_fingerprint=reconciler_account)

    result = reconciler.reconcile("SOXL", [kis_fill(**fill_overrides)])

    assert result["operator_halt"] is True
    assert "UNCLASSIFIED_FILL" in result["codes"]
    assert intent_store.list_intents("SOXL")[-1]["status"] == "SUBMITTED"
    assert events_path.read_text(encoding="utf-8") == ""


def test_append_event_failure_appends_failure_record_and_leaves_fill_retryable(tmp_path, monkeypatch):
    intent_store, trade_store, processed_store, events_path, processed_path = make_stores(tmp_path)
    submitted_intent(intent_store, accepted_order=accepted_key(order_no="ODNO-FAIL"), event_type="FULL_BUY", qty=4)
    reconciler = FillReconciler(intent_store, trade_store, processed_store, account_fingerprint="acct-A")
    real_append = trade_store.append_event
    calls = {"n": 0}

    def flaky_append(event):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("simulated append failure")
        return real_append(event)

    monkeypatch.setattr(trade_store, "append_event", flaky_append)

    with pytest.raises(FillReconciliationError):
        reconciler.reconcile("SOXL", [kis_fill(odno="ODNO-FAIL")])

    assert intent_store.list_intents("SOXL")[-1]["status"] == "SUBMITTED"
    assert events_path.read_text(encoding="utf-8") == ""
    failed = json.loads(processed_path.read_text(encoding="utf-8"))
    assert failed["classification"] == "FINALIZATION_FAILED"

    result = reconciler.reconcile("SOXL", [kis_fill(odno="ODNO-FAIL")])
    assert result["operator_halt"] is False
    assert intent_store.list_intents("SOXL")[-1]["status"] == "FILLED"
    assert len(events_path.read_text(encoding="utf-8").splitlines()) == 1
    assert [json.loads(line)["classification"] for line in processed_path.read_text(encoding="utf-8").splitlines()] == [
        "FINALIZATION_FAILED",
        "FINAL",
    ]


def test_finalization_failure_appends_failure_record_without_removing_existing_lines(tmp_path, monkeypatch):
    intent_store, trade_store, processed_store, events_path, processed_path = make_stores(tmp_path)
    submitted_intent(intent_store, accepted_order=accepted_key(order_no="ODNO-ROLLBACK"), event_type="FULL_BUY", qty=4)
    reconciler = FillReconciler(intent_store, trade_store, processed_store, account_fingerprint="acct-A")

    def crash_after_t_event(intent_id, status):
        raise RuntimeError("simulated crash after snapshot while unrelated writer appended")

    monkeypatch.setattr(intent_store, "transition_status", crash_after_t_event)
    intent_store.create_planned(planned_intent(trade_date="2026-08-13", event_type="HALF_BUY"))
    before_intents = Path(intent_store.ledger_path).read_text(encoding="utf-8").splitlines()

    with pytest.raises(FillReconciliationError):
        reconciler.reconcile("SOXL", [kis_fill(odno="ODNO-ROLLBACK")])

    intents = intent_store.list_intents("SOXL")
    assert any(record["event_type"] == "HALF_BUY" and record["status"] == "PLANNED" for record in intents)
    assert any(record["event_type"] == "FULL_BUY" and record["status"] == "SUBMITTED" for record in intents)
    assert Path(intent_store.ledger_path).read_text(encoding="utf-8").splitlines() == before_intents
    assert len(events_path.read_text(encoding="utf-8").splitlines()) == 1
    assert [json.loads(line)["classification"] for line in processed_path.read_text(encoding="utf-8").splitlines()] == [
        "FINAL",
        "FINALIZATION_FAILED",
    ]

def test_order_acceptance_without_any_fill_keeps_submitted_and_does_not_append_t_event(tmp_path):
    intent_store, trade_store, processed_store, events_path, processed_path = make_stores(tmp_path)
    intent = submitted_intent(intent_store)
    reconciler = FillReconciler(intent_store, trade_store, processed_store, account_fingerprint="acct-A")

    result = reconciler.reconcile("SOXL", [])

    assert result["new_fill_count"] == 0
    assert result["operator_halt"] is False
    assert intent_store.list_intents("SOXL")[-1]["status"] == "SUBMITTED"
    assert events_path.read_text(encoding="utf-8") == ""
    assert processed_path.read_text(encoding="utf-8") == ""


def test_full_fill_transitions_intent_and_appends_one_t_event_atomically(tmp_path):
    intent_store, trade_store, processed_store, events_path, processed_path = make_stores(tmp_path)
    intent = submitted_intent(intent_store, accepted_order=accepted_key(order_no="ODNO-1"), event_type="FULL_BUY", qty=4, price="100.00")
    reconciler = FillReconciler(intent_store, trade_store, processed_store, account_fingerprint="acct-A")

    result = reconciler.reconcile("SOXL", [kis_fill(odno="ODNO-1", ft_ccld_qty="4", ft_ccld_unpr3="100.00")])

    assert result["new_fill_count"] == 1
    assert result["operator_halt"] is False
    assert intent_store.list_intents("SOXL")[-1]["status"] == "FILLED"
    event = json.loads(events_path.read_text(encoding="utf-8"))
    assert set(event) == REQUIRED_EVENT_KEYS
    assert event["intent_id"] == intent["intent_id"]
    assert event["kis_order_no"] == "ODNO-1"
    assert event["event_type"] == "FULL_BUY"
    assert event["filled_qty"] == 4
    assert event["filled_amount"] == "400.00"
    processed = json.loads(processed_path.read_text(encoding="utf-8"))
    assert processed["matching_key"] == "acct-A|SOXL|AMEX|20260812|ODNO-1"


def test_quarter_sell_fill_appends_t_event_with_seventy_five_percent_t(tmp_path):
    intent_store, trade_store, processed_store, events_path, processed_path = make_stores(tmp_path)
    intent = submitted_intent(
        intent_store,
        accepted_order=accepted_key(order_no="ODNO-QSELL"),
        event_type="QUARTER",
        side="SELL",
        qty=19,
        price="142.36",
    )
    reconciler = FillReconciler(intent_store, trade_store, processed_store, account_fingerprint="acct-A")

    result = reconciler.reconcile(
        "SOXL",
        [
            kis_fill(
                odno="ODNO-QSELL",
                sll_buy_dvsn_cd="01",
                ft_ccld_qty="19",
                ft_ccld_unpr3="144.95",
            )
        ],
    )

    assert result["new_fill_count"] == 1
    assert result["operator_halt"] is False
    assert intent_store.list_intents("SOXL")[-1]["status"] == "FILLED"
    event = json.loads(events_path.read_text(encoding="utf-8"))
    assert event["intent_id"] == intent["intent_id"]
    assert event["event_type"] == "QUARTER"
    assert event["filled_qty"] == 19
    assert event["t_before"] == "18.32"
    assert event["t_after"] == "13.7400"
    processed = json.loads(processed_path.read_text(encoding="utf-8"))
    assert processed["classification"] == "FINAL"


def test_previously_unclassified_quarter_sell_can_recover_once_intent_is_available(tmp_path):
    intent_store, trade_store, processed_store, events_path, processed_path = make_stores(tmp_path)
    reconciler = FillReconciler(intent_store, trade_store, processed_store, account_fingerprint="acct-A")
    fill = kis_fill(
        odno="ODNO-QSELL",
        sll_buy_dvsn_cd="01",
        ft_ccld_qty="19",
        ft_ccld_unpr3="144.95",
    )

    first = reconciler.reconcile("SOXL", [fill])
    assert first["operator_halt"] is True
    assert events_path.read_text(encoding="utf-8") == ""

    intent = submitted_intent(
        intent_store,
        accepted_order=accepted_key(order_no="ODNO-QSELL"),
        event_type="QUARTER",
        side="SELL",
        qty=19,
        price="142.36",
    )
    recovered = reconciler.reconcile("SOXL", [fill])

    assert recovered["new_fill_count"] == 1
    assert recovered["operator_halt"] is False
    assert intent_store.list_intents("SOXL")[-1]["status"] == "FILLED"
    event = json.loads(events_path.read_text(encoding="utf-8"))
    assert event["intent_id"] == intent["intent_id"]
    assert event["event_type"] == "QUARTER"
    assert event["filled_qty"] == 19
    assert event["t_before"] == "18.32"
    assert event["t_after"] == "13.7400"
    processed_records = [json.loads(line) for line in processed_path.read_text(encoding="utf-8").splitlines()]
    assert [record["classification"] for record in processed_records] == ["UNCLASSIFIED", "FINAL"]
    assert processed_records[-1]["intent_id"] == intent["intent_id"]

    duplicate = reconciler.reconcile("SOXL", [fill])
    assert duplicate["new_fill_count"] == 0
    assert len(events_path.read_text(encoding="utf-8").splitlines()) == 1


def test_partial_then_additional_fill_accumulates_but_updates_t_only_after_final(tmp_path):
    intent_store, trade_store, processed_store, events_path, _processed_path = make_stores(tmp_path)
    submitted_intent(intent_store, accepted_order=accepted_key(order_no="ODNO-2"), event_type="FULL_BUY", qty=4, price="100.00")
    reconciler = FillReconciler(intent_store, trade_store, processed_store, account_fingerprint="acct-A")

    partial = reconciler.reconcile("SOXL", [kis_fill(odno="ODNO-2", ord_tmd="223000", ft_ccld_qty="2")])

    assert partial["operator_halt"] is True
    assert partial["partial_open"] is True
    assert intent_store.list_intents("SOXL")[-1]["status"] == "PARTIAL"
    assert events_path.read_text(encoding="utf-8") == ""
    assert reconciler.forbid_new_orders("SOXL") is True

    final = reconciler.reconcile("SOXL", [
        kis_fill(odno="ODNO-2", ord_tmd="223000", ft_ccld_qty="2"),
        kis_fill(odno="ODNO-2", ord_tmd="223500", ft_ccld_qty="2"),
    ])

    assert final["operator_halt"] is False
    assert final["partial_open"] is False
    assert intent_store.list_intents("SOXL")[-1]["status"] == "FILLED"
    assert len(events_path.read_text(encoding="utf-8").splitlines()) == 1


def test_duplicate_fill_requery_is_ignored_without_second_t_event(tmp_path):
    intent_store, trade_store, processed_store, events_path, _processed_path = make_stores(tmp_path)
    submitted_intent(intent_store, accepted_order=accepted_key(order_no="ODNO-3"), event_type="FULL_BUY", qty=4)
    reconciler = FillReconciler(intent_store, trade_store, processed_store, account_fingerprint="acct-A")
    row = kis_fill(odno="ODNO-3")

    first = reconciler.reconcile("SOXL", [row])
    second = reconciler.reconcile("SOXL", [row])

    assert first["new_fill_count"] == 1
    assert second["new_fill_count"] == 0
    assert len(events_path.read_text(encoding="utf-8").splitlines()) == 1


def test_cancelled_order_with_residual_partial_fill_keeps_halt_and_never_updates_t(tmp_path):
    intent_store, trade_store, processed_store, events_path, _processed_path = make_stores(tmp_path)
    intent = submitted_intent(intent_store, accepted_order=accepted_key(order_no="ODNO-4"), event_type="FULL_BUY", qty=4)
    intent_store.transition_status(intent["intent_id"], "CANCELLED")
    reconciler = FillReconciler(intent_store, trade_store, processed_store, account_fingerprint="acct-A")

    result = reconciler.reconcile("SOXL", [kis_fill(odno="ODNO-4", ft_ccld_qty="2")])

    assert result["operator_halt"] is True
    assert "CANCELLED_PARTIAL_REMAINS" in result["codes"]
    assert intent_store.list_intents("SOXL")[-1]["status"] == "CANCELLED"
    assert events_path.read_text(encoding="utf-8") == ""


def test_external_manual_fill_is_unclassified_halt_and_no_t_update(tmp_path):
    intent_store, trade_store, processed_store, events_path, _processed_path = make_stores(tmp_path)
    submitted_intent(intent_store, accepted_order=accepted_key(order_no="ODNO-1"), event_type="FULL_BUY", qty=4, price="100.00")
    reconciler = FillReconciler(intent_store, trade_store, processed_store, account_fingerprint="acct-A")

    result = reconciler.reconcile("SOXL", [kis_fill(odno="MANUAL-1", ft_ccld_qty="1")])

    assert result["operator_halt"] is True
    assert "UNCLASSIFIED_FILL" in result["codes"]
    assert events_path.read_text(encoding="utf-8") == ""


@pytest.mark.parametrize(
    "overrides",
    [
        {"ft_ccld_qty": "5"},
        {"ft_ccld_unpr3": "110.00"},
        {"sll_buy_dvsn_cd": "01"},
    ],
)
def test_fill_outside_intent_qty_price_side_is_unclassified_halt_no_proportional_t(tmp_path, overrides):
    intent_store, trade_store, processed_store, events_path, _processed_path = make_stores(tmp_path)
    submitted_intent(intent_store, accepted_order=accepted_key(order_no="ODNO-5"), event_type="FULL_BUY", qty=4, price="100.00", side="BUY")
    reconciler = FillReconciler(intent_store, trade_store, processed_store, account_fingerprint="acct-A")

    result = reconciler.reconcile("SOXL", [kis_fill(odno="ODNO-5", **overrides)])

    assert result["operator_halt"] is True
    assert "UNCLASSIFIED_FILL" in result["codes"]
    assert events_path.read_text(encoding="utf-8") == ""


def test_middle_failure_between_t_event_and_intent_status_is_atomic_and_retryable(tmp_path, monkeypatch):
    intent_store, trade_store, processed_store, events_path, _processed_path = make_stores(tmp_path)
    submitted_intent(intent_store, accepted_order=accepted_key(order_no="ODNO-6"), event_type="FULL_BUY", qty=4)
    reconciler = FillReconciler(intent_store, trade_store, processed_store, account_fingerprint="acct-A")
    real_transition = intent_store.transition_status
    calls = {"n": 0}

    def flaky_transition(intent_id, status):
        calls["n"] += 1
        if calls["n"] == 1 and status == "FILLED":
            raise RuntimeError("simulated crash after classification")
        return real_transition(intent_id, status)

    monkeypatch.setattr(intent_store, "transition_status", flaky_transition)

    with pytest.raises(FillReconciliationError):
        reconciler.reconcile("SOXL", [kis_fill(odno="ODNO-6")])

    assert events_path.read_text(encoding="utf-8") == ""
    assert intent_store.list_intents("SOXL")[-1]["status"] == "SUBMITTED"

    result = reconciler.reconcile("SOXL", [kis_fill(odno="ODNO-6")])
    assert result["operator_halt"] is False
    assert len(events_path.read_text(encoding="utf-8").splitlines()) == 1
    assert intent_store.list_intents("SOXL")[-1]["status"] == "FILLED"


def test_required_kis_order_fixtures_exist_and_are_valid_json():
    fixture_dir = __import__("pathlib").Path(__file__).parent / "fixtures" / "kis_orders"
    required = {
        "order_accepted_no_fills.json",
        "full_fill.json",
        "partial_then_additional.json",
        "duplicate_requery.json",
        "cancelled_partial.json",
        "external_manual.json",
        "missing_order_number.json",
        "fill_outside_bounds.json",
    }

    loaded = {}
    for name in required:
        path = fixture_dir / name
        assert path.exists(), name
        loaded[name] = json.loads(path.read_text(encoding="utf-8"))
        assert loaded[name]["ticker"] == "SOXL"
        assert "executions" in loaded[name]

    assert loaded["order_accepted_no_fills.json"]["executions"] == []
    assert loaded["missing_order_number.json"]["acceptance_response"]["odno"] == ""
