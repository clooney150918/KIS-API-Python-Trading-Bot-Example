from callback_order_handler import _record_manual_accepted_intent
from order_intent_store import compute_intent_id
from runtime_safety import account_fingerprint
from test_runtime_safety import (
    SYNTHETIC_ACCOUNT_FINGERPRINT_KEY,
    SYNTHETIC_CANO,
    SYNTHETIC_PRODUCT_CODE,
)


class TrackingIntentStore:
    def __init__(self):
        self.planned = []
        self.accepted = []

    def create_planned(self, intent):
        self.planned.append(dict(intent))
        return {"intent_id": compute_intent_id(intent), **dict(intent), "status": "PLANNED"}

    def record_accepted_order(self, intent_id, accepted_order):
        self.accepted.append((intent_id, dict(accepted_order)))
        return {"intent_id": intent_id, "accepted_order": dict(accepted_order), "status": "SUBMITTED"}


class FakeBroker:
    account_fingerprint_key = SYNTHETIC_ACCOUNT_FINGERPRINT_KEY
    cano = SYNTHETIC_CANO
    acnt_prdt_cd = SYNTHETIC_PRODUCT_CODE
    last_pending_buy_amount = 0

    pass


def test_manual_force_exec_records_order_intent_after_kis_acceptance():

    order = {
        "strategy": "LAOER_V4_SOXL_20",
        "strategy_revision": 1,
        "t_revision": 3,
        "trade_date": "2026-08-18",
        "ticker": "SOXL",
        "event_type": "FULL_BUY",
        "side": "BUY",
        "order_type": "LIMIT",
        "type": "LIMIT",
        "price": "129.10",
        "qty": 5,
        "desc": "manual",
    }
    intent_store = TrackingIntentStore()
    broker = FakeBroker()

    _record_manual_accepted_intent(
        intent_store,
        broker,
        ticker="SOXL",
        order=order,
        order_type="LIMIT",
        response={"rt_cd": "0", "msg1": "OK", "odno": "0030308368"},
    )

    expected_intent_id = compute_intent_id(order)
    expected_fingerprint = account_fingerprint(
        SYNTHETIC_CANO,
        SYNTHETIC_PRODUCT_CODE,
        key=SYNTHETIC_ACCOUNT_FINGERPRINT_KEY,
    )
    assert intent_store.planned == [{
        "strategy": "LAOER_V4_SOXL_20",
        "strategy_revision": 1,
        "t_revision": 3,
        "ticker": "SOXL",
        "trade_date": "2026-08-18",
        "event_type": "FULL_BUY",
        "side": "BUY",
        "order_type": "LIMIT",
        "price": "129.10",
        "qty": 5,
    }]
    assert intent_store.accepted == [(expected_intent_id, {
        "account_fingerprint": expected_fingerprint,
        "ticker": "SOXL",
        "exchange": "AMEX",
        "trade_date": "20260818",
        "order_no": "0030308368",
    })]
