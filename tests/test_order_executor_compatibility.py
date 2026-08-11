import asyncio
import threading

import pytest

import order_executor
from order_executor import execute_order_list
from runtime_safety import RuntimeSafetyGate, safety_block_result
from test_runtime_safety import (
    SYNTHETIC_ACCOUNT_FINGERPRINT_KEY,
    SYNTHETIC_CANO,
    SYNTHETIC_PRODUCT_CODE,
    write_state,
)
from test_runtime_safety_authorization import ORDER_REQUEST, POLICY_REQUEST


class LegacyPositionalBroker:
    def __init__(self, gate):
        self.runtime_safety_gate = gate
        self.account_fingerprint_key = SYNTHETIC_ACCOUNT_FINGERPRINT_KEY
        self.cano = SYNTHETIC_CANO
        self.acnt_prdt_cd = SYNTHETIC_PRODUCT_CODE
        self.calls = []

    def send_order(self, ticker, side, qty, price, order_type, /):
        self.calls.append((ticker, side, qty, price, order_type))
        return {"rt_cd": "0", "msg1": "OK", "odno": "LEGACY-1"}

    def send_reservation_order(self, ticker, side, qty, price, order_type, /):
        self.calls.append((ticker, side, qty, price, order_type))
        return {"rt_cd": "0", "msg1": "OK", "odno": "LEGACY-R1"}


class FailingBroker(LegacyPositionalBroker):
    def __init__(self, gate, outcome):
        super().__init__(gate)
        self.outcome = outcome

    def _submit(self, ticker, side, qty, price, order_type):
        self.calls.append((ticker, side, qty, price, order_type))
        if isinstance(self.outcome, BaseException):
            raise self.outcome
        return self.outcome

    def send_order(self, ticker, side, qty, price, order_type, /):
        return self._submit(ticker, side, qty, price, order_type)

    def send_reservation_order(self, ticker, side, qty, price, order_type, /):
        return self._submit(ticker, side, qty, price, order_type)


class RiskCapableFailingBroker(FailingBroker):
    def send_order(
        self, ticker, side, qty, price, order_type,
        *, risk_reference_price=None, idempotency_key=None
    ):
        return self._submit(ticker, side, qty, price, order_type)

    def send_reservation_order(
        self, ticker, side, qty, price, order_type,
        *, risk_reference_price=None, idempotency_key=None
    ):
        return self._submit(ticker, side, qty, price, order_type)


class BlockingBroker(LegacyPositionalBroker):
    def __init__(self, gate, entered, release):
        super().__init__(gate)
        self.entered = entered
        self.release = release

    def _submit(self, ticker, side, qty, price, order_type):
        self.calls.append((ticker, side, qty, price, order_type))
        self.entered.set()
        assert self.release.wait(timeout=2)
        return {"rt_cd": "0", "msg1": "late accepted", "odno": "LATE-1"}

    def send_order(self, ticker, side, qty, price, order_type, /):
        return self._submit(ticker, side, qty, price, order_type)

    def send_reservation_order(self, ticker, side, qty, price, order_type, /):
        return self._submit(ticker, side, qty, price, order_type)


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


@pytest.mark.parametrize("market_active", [True, False])
@pytest.mark.parametrize(
    "outcome",
    [ConnectionError("connection lost"), RuntimeError("unknown failure")],
)
def test_submission_exception_is_single_attempt_ambiguous_and_stops_order_list(
    tmp_path, monkeypatch, market_active, outcome
):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = FailingBroker(gate, outcome)
    cache = set()
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [
                {"side": "BUY", "qty": 1, "price": "100", "type": "LIMIT", "desc": "first"},
                {"side": "BUY", "qty": 1, "price": "99", "type": "LIMIT", "desc": "second"},
            ],
            cache,
            market_active,
            "20260811",
            runtime_safety_gate=gate,
        )
    )

    assert success is False
    assert len(broker.calls) == 1
    assert cache == set()
    assert "ORDER_SUBMISSION_AMBIGUOUS" in messages
    assert "HALT_REQUIRED" in messages
    assert "RECONCILIATION_REQUIRED" in messages
    assert "ORDER_SUBMISSION_AMBIGUOUS" in failure
    assert gate.authorize(
        "SOXL", "BUY", 1, 100, account_fingerprint_key_available=False
    ).code == "ORDER_SUBMISSION_AMBIGUOUS"


@pytest.mark.parametrize("market_active", [True, False])
def test_timed_out_worker_is_never_recalled_even_if_it_later_submits(
    tmp_path, monkeypatch, market_active
):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    entered = threading.Event()
    release = threading.Event()
    broker = BlockingBroker(gate, entered, release)
    cache = set()
    monkeypatch.setattr(order_executor, "ORDER_SUBMISSION_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)

    async def run_probe():
        task = asyncio.create_task(
            execute_order_list(
                broker,
                "SOXL",
                [
                    {"side": "BUY", "qty": 1, "price": "100", "type": "LIMIT", "desc": "first"},
                    {"side": "BUY", "qty": 1, "price": "99", "type": "LIMIT", "desc": "second"},
                ],
                cache,
                market_active,
                "20260811",
                runtime_safety_gate=gate,
            )
        )
        assert await asyncio.to_thread(entered.wait, 1)
        result = await asyncio.wait_for(task, timeout=1)
        assert len(broker.calls) == 1
        release.set()
        return result

    success, messages, failure = asyncio.run(run_probe())

    assert success is False
    assert len(broker.calls) == 1
    assert cache == set()
    assert "ORDER_SUBMISSION_AMBIGUOUS" in messages
    assert "HALT_REQUIRED" in messages
    assert "RECONCILIATION_REQUIRED" in messages
    assert "ORDER_SUBMISSION_AMBIGUOUS" in failure


@pytest.mark.parametrize("market_active", [True, False])
def test_broker_ambiguous_result_is_not_cached_and_stops_order_list(
    tmp_path, monkeypatch, market_active
):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = FailingBroker(
        gate,
        {
            "rt_cd": "999",
            "msg1": "response status unknown",
            "odno": "",
            "safety_decision": {"code": "ORDER_SUBMISSION_AMBIGUOUS"},
            "reconciliation_required": True,
        },
    )
    cache = set()
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [
                {"side": "BUY", "qty": 1, "price": "100", "type": "LIMIT", "desc": "first"},
                {"side": "BUY", "qty": 1, "price": "99", "type": "LIMIT", "desc": "second"},
            ],
            cache,
            market_active,
            "20260811",
            runtime_safety_gate=gate,
        )
    )

    assert success is False
    assert len(broker.calls) == 1
    assert cache == set()
    assert "ORDER_SUBMISSION_AMBIGUOUS" in messages
    assert "HALT_REQUIRED" in messages
    assert "RECONCILIATION_REQUIRED" in messages
    assert "ORDER_SUBMISSION_AMBIGUOUS" in failure


@pytest.mark.parametrize("market_active", [True, False])
def test_explicit_broker_rejection_has_no_retry_or_fallback_and_stops_list(
    tmp_path, monkeypatch, market_active
):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = FailingBroker(
        gate, {"rt_cd": "1", "msg1": "KIS explicit rejection", "odno": ""}
    )
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [
                {"side": "BUY", "qty": 1, "price": "100", "type": "LIMIT", "desc": "first"},
                {"side": "BUY", "qty": 1, "price": "99", "type": "LIMIT", "desc": "second"},
            ],
            set(),
            market_active,
            "20260811",
            runtime_safety_gate=gate,
        )
    )

    assert success is False
    assert len(broker.calls) == 1
    assert "KIS explicit rejection" in messages
    assert "KIS explicit rejection" in failure


def test_prior_success_remains_cached_when_later_order_is_ambiguous(tmp_path, monkeypatch):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    outcomes = iter(
        [
            {"rt_cd": "0", "msg1": "OK", "odno": "FIRST-1"},
            ConnectionError("connection lost"),
        ]
    )
    broker = FailingBroker(gate, None)

    def next_outcome(*args):
        broker.calls.append(args)
        outcome = next(outcomes)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    broker.send_order = next_outcome
    cache = set()
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [
                {"side": "BUY", "qty": 1, "price": "100", "type": "LIMIT", "desc": "first"},
                {"side": "BUY", "qty": 1, "price": "99", "type": "LIMIT", "desc": "second"},
                {"side": "BUY", "qty": 1, "price": "98", "type": "LIMIT", "desc": "third"},
            ],
            cache,
            True,
            "20260811",
            runtime_safety_gate=gate,
        )
    )

    assert success is False
    assert len(broker.calls) == 2
    assert "SOXL_first" in cache
    assert "SOXL_second" not in cache
    assert "SOXL_third" not in cache
    assert "ORDER_SUBMISSION_AMBIGUOUS" in failure


@pytest.mark.parametrize("market_active", [True, False])
def test_moc_submission_exception_is_single_attempt_and_halts(
    tmp_path, monkeypatch, market_active
):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = RiskCapableFailingBroker(gate, ConnectionError("connection lost"))
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)

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
                    "desc": "first",
                },
                {
                    "side": "SELL",
                    "qty": 1,
                    "price": "0",
                    "type": "MOC",
                    "risk_reference_price": "100",
                    "desc": "second",
                },
            ],
            set(),
            market_active,
            "20260811",
            runtime_safety_gate=gate,
        )
    )

    assert success is False
    assert len(broker.calls) == 1
    assert "ORDER_SUBMISSION_AMBIGUOUS" in messages
    assert "ORDER_SUBMISSION_AMBIGUOUS" in failure


def test_ambiguous_latch_block_result_carries_structured_halt_contract(tmp_path):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    gate.latch_ambiguous_submission("response lost")

    result = safety_block_result(
        gate.authorize(
            "SOXL", "BUY", 1, 100, account_fingerprint_key_available=False
        )
    )

    assert result["safety_decision"]["code"] == "ORDER_SUBMISSION_AMBIGUOUS"
    assert result["halt_required"] is True
    assert result["reconciliation_required"] is True
    assert result["odno"] == ""


def test_ambiguous_latch_blocks_future_request_authorization(tmp_path):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    gate.latch_ambiguous_submission("response lost")

    decision, authorization = gate.authorize_request(
        **POLICY_REQUEST,
        **ORDER_REQUEST,
        account_fingerprint_key=SYNTHETIC_ACCOUNT_FINGERPRINT_KEY,
    )

    assert decision.code == "ORDER_SUBMISSION_AMBIGUOUS"
    assert decision.can_submit is False
    assert authorization is None
