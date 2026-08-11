import asyncio

import pytest

import order_executor
from order_executor import execute_order_list
from runtime_safety import RuntimeSafetyGate
from test_runtime_safety import SYNTHETIC_CANO, SYNTHETIC_PRODUCT_CODE, write_state


class LegacyPositionalBroker:
    def __init__(self, gate):
        self.runtime_safety_gate = gate
        self.cano = SYNTHETIC_CANO
        self.acnt_prdt_cd = SYNTHETIC_PRODUCT_CODE
        self.calls = []

    def send_order(self, ticker, side, qty, price, order_type, /):
        self.calls.append((ticker, side, qty, price, order_type))
        return {"rt_cd": "0", "msg1": "OK", "odno": "LEGACY-1"}

    def send_reservation_order(self, ticker, side, qty, price, order_type, /):
        self.calls.append((ticker, side, qty, price, order_type))
        return {"rt_cd": "0", "msg1": "OK", "odno": "LEGACY-R1"}


async def no_sleep(_delay):
    return None


@pytest.mark.parametrize("market_active", [True, False])
def test_limit_order_supports_legacy_positional_only_broker(
    tmp_path, monkeypatch, market_active
):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = LegacyPositionalBroker(gate)
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [{"side": "buy", "qty": 1, "price": "100", "type": "limit"}],
            set(),
            market_active,
            "20260811",
            runtime_safety_gate=gate,
        )
    )

    assert success is True
    assert failure == ""
    assert "✅" in messages
    assert broker.calls == [("SOXL", "BUY", 1, 100.0, "LIMIT")]


@pytest.mark.parametrize("market_active", [True, False])
def test_market_style_order_requires_broker_risk_reference_capability(
    tmp_path, monkeypatch, market_active
):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = LegacyPositionalBroker(gate)
    sleep_calls = []

    async def tracked_sleep(delay):
        sleep_calls.append(delay)

    monkeypatch.setattr(order_executor.asyncio, "sleep", tracked_sleep)

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [
                {
                    "side": "SELL",
                    "qty": 1,
                    "price": "0",
                    "type": "MOC",
                    "risk_reference_price": "100",
                }
            ],
            set(),
            market_active,
            "20260811",
            runtime_safety_gate=gate,
        )
    )

    assert success is False
    assert "BROKER_CAPABILITY_MISSING" in messages
    assert "BROKER_CAPABILITY_MISSING" in failure
    assert broker.calls == []
    assert sleep_calls == []
