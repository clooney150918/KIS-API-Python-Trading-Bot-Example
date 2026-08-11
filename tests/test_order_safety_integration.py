import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from kis_order_engine import KisOrderEngine
from order_executor import execute_order_list
from runtime_safety import RuntimeSafetyGate, SafetyDecision, TrustedMarketQuote
from shadow_intent import ShadowIntentRecorder
from strategy_v14 import V4Strategy
from test_runtime_safety import (
    SYNTHETIC_ACCOUNT_FINGERPRINT_KEY,
    SYNTHETIC_CANO,
    SYNTHETIC_PRODUCT_CODE,
    write_state,
)


class ReverseDayOneConfig:
    def __init__(self, tmp_path):
        self.FILES = {
            "STRATEGY_BASELINE": str(tmp_path / "baseline.json"),
            "T_EVENTS": str(tmp_path / "events.jsonl"),
            "T_STATE": str(tmp_path / "t_state.json"),
        }
        baseline = {
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
        tmp_path.joinpath("baseline.json").write_text(json.dumps(baseline), encoding="utf-8")
        tmp_path.joinpath("events.jsonl").write_text("", encoding="utf-8")
        tmp_path.joinpath("t_state.json").write_text("{}", encoding="utf-8")

    def _load_json(self, path, default):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default

    def get_split_count(self, ticker):
        return 20

    def get_reverse_state(self, ticker):
        return {
            "is_active": True,
            "dynamic_t": 20.0,
            "rem_cash": 1000.0,
            "is_day_one": True,
        }

    def get_seed(self, ticker):
        return 2000.0

    def get_target_profit(self, ticker):
        return 10.0

    def get_absolute_t_val(self, ticker, qty, avg_price):
        return 20.0, None

    def set_reverse_state(self, *args, **kwargs):
        raise AssertionError("reverse state must remain active in this scenario")


class DummyOrderIntentStore:
    def __init__(self):
        self.submitted = []

    def record_accepted_order(self, intent_id, accepted_order):
        self.submitted.append((intent_id, accepted_order))


class TransitionToShadowGate:
    def __init__(self):
        self.calls = 0

    def authorize(self, ticker, side, quantity, price, **kwargs):
        self.calls += 1
        return SafetyDecision(
            code="LIVE_AUTHORIZED",
            reason="test initial live authorization",
            can_submit=True,
            shadow_only=False,
            revision=7,
            ticker=ticker,
            side=side,
        )

    def authorize_request(self, ticker, side, quantity, price, **kwargs):
        self.calls += 1
        return SafetyDecision(
            code="SHADOW_ONLY",
            reason="test state transition",
            can_submit=False,
            shadow_only=True,
            revision=8,
            ticker=ticker,
            side=side,
        ), None


def make_engine(gate, recorder):
    engine = object.__new__(KisOrderEngine)
    engine.runtime_safety_gate = gate
    engine.shadow_intent_recorder = recorder
    engine.account_fingerprint_key = SYNTHETIC_ACCOUNT_FINGERPRINT_KEY
    engine.trusted_quote_provider = lambda ticker: TrustedMarketQuote(
        price=Decimal("100"),
        as_of=datetime.now(timezone.utc),
        source="KIS",
        ticker=ticker,
    )
    engine.cano = SYNTHETIC_CANO
    engine.acnt_prdt_cd = SYNTHETIC_PRODUCT_CODE
    engine._safe_float = lambda value: float(value)
    engine._ceil_2 = lambda value: float(value)
    engine._get_exchange_code = lambda *args, **kwargs: "NASD"
    engine._excg_cd_cache = {}
    calls = []
    engine._call_api = lambda *args, **kwargs: calls.append((args, kwargs)) or {
        "rt_cd": "0",
        "msg1": "OK",
        "output": {"ODNO": "UNEXPECTED"},
    }
    return engine, calls


def read_records(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def build_reverse_day_one_plan(tmp_path, current_price):
    strategy = V4Strategy(ReverseDayOneConfig(tmp_path))
    strategy.load_daily_snapshot = lambda ticker: None
    return strategy.get_plan(
        "SOXL",
        current_price=current_price,
        avg_price=150.0,
        qty=40,
        prev_close=99.0,
        available_cash=1000.0,
        market_type="REG",
    )


def test_reverse_day_one_moc_current_price_flows_strategy_executor_to_kis(tmp_path):
    plan = build_reverse_day_one_plan(tmp_path, 100.0)
    moc = next(order for order in plan["orders"] if order["type"] == "MOC")
    assert moc["risk_reference_price"] == 100.0

    state_path = write_state(
        tmp_path / "runtime_safety.json",
        max_order_notional="1000.00",
    )
    engine, kis_calls = make_engine(
        RuntimeSafetyGate(state_path),
        ShadowIntentRecorder(tmp_path / "unused-shadow.jsonl"),
    )

    success, messages, failure = asyncio.run(
        execute_order_list(
            engine,
            "SOXL",
            plan["orders"],
            set(),
            True,
            "20260811",
            runtime_safety_gate=engine.runtime_safety_gate,
            order_intent_store=DummyOrderIntentStore(),
            current_t_revision_provider=lambda ticker: 1,
        )
    )

    assert success is True
    assert failure == ""
    assert "INVALID_RISK_REFERENCE_PRICE" not in messages
    assert len(kis_calls) == 1
    assert kis_calls[0][1]["body"]["OVRS_ORD_UNPR"] == "0"


def test_reverse_day_one_moc_reference_reaches_kis_reservation_boundary(tmp_path):
    plan = build_reverse_day_one_plan(tmp_path, 100.0)
    state_path = write_state(
        tmp_path / "runtime_safety.json",
        max_order_notional="1000.00",
    )
    engine, kis_calls = make_engine(
        RuntimeSafetyGate(state_path),
        ShadowIntentRecorder(tmp_path / "unused-shadow.jsonl"),
    )

    success, messages, failure = asyncio.run(
        execute_order_list(
            engine,
            "SOXL",
            plan["orders"],
            set(),
            False,
            "20260811",
            runtime_safety_gate=engine.runtime_safety_gate,
            order_intent_store=DummyOrderIntentStore(),
            current_t_revision_provider=lambda ticker: 1,
        )
    )

    assert success is True
    assert failure == ""
    assert "INVALID_RISK_REFERENCE_PRICE" not in messages
    assert len(kis_calls) == 1
    body = kis_calls[0][1]["body"]
    assert Decimal(body["FT_ORD_UNPR3"]) == Decimal("0")
    assert body["ORD_DVSN"] == "33"


@pytest.mark.parametrize("current_price", [0, -1, float("nan"), float("inf")])
def test_reverse_day_one_moc_is_not_planned_without_positive_finite_current_price(tmp_path, current_price):
    plan = build_reverse_day_one_plan(tmp_path, current_price)

    assert plan["orders"] == []
    assert plan["core_orders"] == []
    assert plan["safety"]["halted"] is True
    assert "price" in plan["safety"]["reason"].lower() or "가격" in plan["safety"]["reason"]


@pytest.mark.parametrize(
    "method_name,args,expected_type",
    [
        ("send_order", ("SOXL", "BUY", 2, "100", "LIMIT"), "LIMIT"),
        ("send_daytime_order", ("SOXL", "SELL", 2, "101"), "LIMIT"),
        ("send_reservation_order", ("SOXL", "BUY", 2, "99", "LOC"), "LOC"),
    ],
)
def test_direct_kis_initial_shadow_records_one_intent_and_calls_no_api(
    tmp_path, method_name, args, expected_type
):
    state_path = write_state(tmp_path / "runtime_safety.json", shadow_only=True)
    intent_path = tmp_path / f"{method_name}.jsonl"
    engine, kis_calls = make_engine(
        RuntimeSafetyGate(state_path), ShadowIntentRecorder(intent_path)
    )

    result = getattr(engine, method_name)(*args)

    assert result["safety_decision"]["code"] == "SHADOW_ONLY"
    assert kis_calls == []
    records = read_records(intent_path)
    assert len(records) == 1
    assert records[0]["order_type"] == expected_type
    assert SYNTHETIC_CANO not in json.dumps(records[0])


@pytest.mark.parametrize(
    "method_name,args,expected_type",
    [
        ("send_order", ("SOXL", "BUY", 2, "100", "LIMIT"), "LIMIT"),
        ("send_daytime_order", ("SOXL", "SELL", 2, "101"), "LIMIT"),
        ("send_reservation_order", ("SOXL", "BUY", 2, "99", "LOC"), "LOC"),
    ],
)
def test_direct_kis_final_gate_shadow_transition_records_one_intent_and_calls_no_api(
    tmp_path, method_name, args, expected_type
):
    intent_path = tmp_path / f"final-{method_name}.jsonl"
    gate = TransitionToShadowGate()
    engine, kis_calls = make_engine(gate, ShadowIntentRecorder(intent_path))

    result = getattr(engine, method_name)(*args)

    assert gate.calls == 2
    assert result["safety_decision"]["code"] == "SHADOW_ONLY"
    assert kis_calls == []
    records = read_records(intent_path)
    assert len(records) == 1
    assert records[0]["order_type"] == expected_type
    assert records[0]["safety_revision"] == 8


def test_direct_kis_shadow_recorder_failure_fails_closed(tmp_path):
    class BrokenRecorder:
        def record(self, **intent):
            raise OSError("disk full")

    state_path = write_state(tmp_path / "runtime_safety.json", shadow_only=True)
    engine, kis_calls = make_engine(RuntimeSafetyGate(state_path), BrokenRecorder())

    result = engine.send_order("SOXL", "BUY", 1, "100", "LIMIT")

    assert kis_calls == []
    assert set(("rt_cd", "msg1", "odno", "shadow", "safety_decision")) <= set(result)
    assert result["odno"] == ""
    assert result["shadow"] is True
    assert result["safety_decision"]["code"] == "SHADOW_INTENT_RECORD_FAILED"


def test_order_executor_and_kis_engine_record_shadow_intent_exactly_once(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json", shadow_only=True)
    intent_path = tmp_path / "exactly-once.jsonl"
    recorder = ShadowIntentRecorder(intent_path)
    engine, kis_calls = make_engine(RuntimeSafetyGate(state_path), recorder)

    success, messages, failure = asyncio.run(
        execute_order_list(
            engine,
            "SOXL",
            [{"side": "BUY", "qty": 2, "price": "100", "type": "LIMIT"}],
            set(),
            True,
            "20260811",
            runtime_safety_gate=engine.runtime_safety_gate,
            shadow_intent_recorder=recorder,
        )
    )

    assert success is False
    assert "SHADOW_ONLY" in messages
    assert "SHADOW_ONLY" in failure
    assert kis_calls == []
    assert len(read_records(intent_path)) == 1
