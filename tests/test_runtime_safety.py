import asyncio
from decimal import Decimal
import json
from pathlib import Path

import pytest

from runtime_safety import RuntimeSafetyGate


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_STATE = ROOT / "data" / "runtime_safety.json"


def write_state(path, **overrides):
    state = {
        "operator_halt": False,
        "live_armed": True,
        "shadow_only": False,
        "reason": "TEST",
        "revision": 1,
        "updated_at": "2026-08-11T00:00:00Z",
        "updated_by": "PYTEST",
        "allowed_tickers": ["SOXL"],
        "max_order_quantity": 100,
        "max_order_notional": "25000.00",
    }
    state.update(overrides)
    path.write_text(json.dumps(state), encoding="utf-8")
    return path


def authorize(path, **overrides):
    request = {
        "ticker": "SOXL",
        "side": "BUY",
        "quantity": 2,
        "price": "100.25",
    }
    request.update(overrides)
    return RuntimeSafetyGate(path).authorize(**request)


def test_missing_state_file_fails_closed(tmp_path):
    decision = authorize(tmp_path / "missing.json")

    assert decision.code == "SAFETY_STATE_MISSING"
    assert decision.can_submit is False


def test_corrupt_json_fails_closed(tmp_path):
    state_path = tmp_path / "runtime_safety.json"
    state_path.write_text("{not-json", encoding="utf-8")

    decision = authorize(state_path)

    assert decision.code == "SAFETY_STATE_INVALID_JSON"
    assert decision.can_submit is False


@pytest.mark.parametrize(
    "field,value",
    [
        ("operator_halt", 1),
        ("live_armed", "true"),
        ("shadow_only", None),
        ("revision", "1"),
        ("allowed_tickers", "SOXL"),
        ("max_order_quantity", 0),
        ("max_order_notional", "not-money"),
    ],
)
def test_state_type_errors_fail_closed(tmp_path, field, value):
    state_path = write_state(tmp_path / "runtime_safety.json", **{field: value})

    decision = authorize(state_path)

    assert decision.code == "SAFETY_STATE_INVALID_SCHEMA"
    assert decision.can_submit is False


def test_operator_halt_blocks_buy_and_sell(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json", operator_halt=True)

    buy = authorize(state_path, side="BUY")
    sell = authorize(state_path, side="SELL")

    assert buy.code == sell.code == "OPERATOR_HALT"
    assert not buy.can_submit and not sell.can_submit


def test_live_not_armed_blocks_order(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json", live_armed=False)

    decision = authorize(state_path)

    assert decision.code == "LIVE_NOT_ARMED"
    assert decision.can_submit is False


def test_shadow_only_returns_structured_simulation_and_does_not_call_sender(tmp_path):
    from order_executor import execute_order_list

    state_path = write_state(tmp_path / "runtime_safety.json", shadow_only=True)
    gate = RuntimeSafetyGate(state_path)

    class FakeBroker:
        def __init__(self):
            self.calls = 0

        def send_order(self, *args, **kwargs):
            self.calls += 1
            return {"rt_cd": "0", "odno": "SHOULD_NOT_EXIST"}

    broker = FakeBroker()
    result = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [{"side": "BUY", "qty": 2, "price": "100", "type": "LIMIT", "desc": "shadow"}],
            set(),
            True,
            "20260811",
            runtime_safety_gate=gate,
        )
    )

    assert broker.calls == 0
    assert result[0] is False
    assert "SHADOW_ONLY" in result[1]
    assert "SHADOW_ONLY" in result[2]


def test_invalid_ticker_is_blocked(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json")

    decision = authorize(state_path, ticker="TQQQ")

    assert decision.code == "TICKER_NOT_ALLOWED"
    assert decision.can_submit is False


@pytest.mark.parametrize(
    "quantity,price,expected_code",
    [
        (0, "100", "INVALID_QUANTITY"),
        (-1, "100", "INVALID_QUANTITY"),
        (1, "0", "INVALID_NOTIONAL"),
        (1, "-1", "INVALID_NOTIONAL"),
        (101, "100", "QUANTITY_LIMIT_EXCEEDED"),
        (100, "250.01", "NOTIONAL_LIMIT_EXCEEDED"),
    ],
)
def test_invalid_or_over_limit_order_is_blocked(tmp_path, quantity, price, expected_code):
    state_path = write_state(tmp_path / "runtime_safety.json")

    decision = authorize(state_path, quantity=quantity, price=price)

    assert decision.code == expected_code
    assert decision.can_submit is False


def test_revision_rollback_is_blocked_by_same_gate_instance(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json", revision=2)
    gate = RuntimeSafetyGate(state_path)

    first = gate.authorize("SOXL", "BUY", 1, "100")
    write_state(state_path, revision=1)
    second = gate.authorize("SOXL", "BUY", 1, "100")

    assert first.code == "LIVE_AUTHORIZED"
    assert second.code == "REVISION_ROLLBACK"
    assert second.revision == 1
    assert second.can_submit is False


def test_normal_live_authorization_uses_decimal_and_returns_structured_decision(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json")

    decision = authorize(state_path, quantity="2", price="100.25")

    assert decision.code == "LIVE_AUTHORIZED"
    assert decision.can_submit is True
    assert decision.shadow_only is False
    assert decision.revision == 1
    assert decision.quantity == Decimal("2")
    assert decision.notional == Decimal("200.50")
    assert isinstance(decision.as_dict(), dict)


def test_direct_kis_order_boundary_blocks_without_explicit_safety_injection():
    from kis_order_engine import KisOrderEngine

    engine = object.__new__(KisOrderEngine)
    engine._safe_float = lambda value: float(value)
    engine._ceil_2 = lambda value: float(value)
    engine._get_exchange_code = lambda *args, **kwargs: "NASD"
    calls = []
    engine._call_api = lambda *args, **kwargs: calls.append((args, kwargs)) or {"rt_cd": "0"}

    result = engine.send_order("SOXL", "BUY", 1, "100", "LIMIT")

    assert calls == []
    assert result["rt_cd"] == "999"
    assert result["safety_decision"]["code"] == "SAFETY_NOT_CONFIGURED"


def test_direct_kis_order_boundary_calls_kis_exactly_once_when_live_authorized(tmp_path):
    from kis_order_engine import KisOrderEngine

    state_path = write_state(tmp_path / "runtime_safety.json")
    engine = object.__new__(KisOrderEngine)
    engine.runtime_safety_gate = RuntimeSafetyGate(state_path)
    engine.cano = "TEST"
    engine.acnt_prdt_cd = "01"
    engine._safe_float = lambda value: float(value)
    engine._ceil_2 = lambda value: float(value)
    engine._get_exchange_code = lambda *args, **kwargs: "NASD"
    engine._excg_cd_cache = {}
    calls = []

    def fake_call(*args, **kwargs):
        calls.append((args, kwargs))
        return {"rt_cd": "0", "msg1": "OK", "output": {"ODNO": "1"}}

    engine._call_api = fake_call

    result = engine.send_order("SOXL", "BUY", 1, "100", "LIMIT")

    assert len(calls) == 1
    assert result["rt_cd"] == "0"
    assert result["odno"] == "1"


def test_production_runtime_state_is_safe_default_and_limits_are_explicit():
    raw = json.loads(PRODUCTION_STATE.read_text(encoding="utf-8"), parse_float=Decimal)

    assert raw["operator_halt"] is True
    assert raw["live_armed"] is False
    assert raw["shadow_only"] is True
    assert raw["revision"] == 1
    assert raw["allowed_tickers"] == ["SOXL"]
    assert Decimal(str(raw["max_order_quantity"])) > 0
    assert Decimal(str(raw["max_order_notional"])) > 0

    decision = RuntimeSafetyGate(PRODUCTION_STATE).authorize("SOXL", "BUY", 1, "100")
    assert decision.code == "OPERATOR_HALT"
    assert decision.can_submit is False
