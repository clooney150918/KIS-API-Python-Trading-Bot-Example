import asyncio
import threading

import pytest

import order_executor
from order_executor import execute_order_list
from order_intent_store import compute_intent_id
from runtime_safety import RuntimeSafetyGate, safety_block_result
from test_runtime_safety import (
    SYNTHETIC_ACCOUNT_FINGERPRINT,
    SYNTHETIC_ACCOUNT_FINGERPRINT_KEY,
    SYNTHETIC_CANO,
    SYNTHETIC_PRODUCT_CODE,
    write_state,
)
from test_runtime_safety_authorization import ORDER_REQUEST, POLICY_REQUEST


def official_order(**overrides):
    order = {
        "ticker": "SOXL",
        "side": "BUY",
        "qty": 1,
        "price": "100",
        "order_type": "LIMIT",
        "strategy": "LAOER_V4_SOXL_20",
        "strategy_revision": 1,
        "t_revision": 3,
        "trade_date": "2026-08-11",
        "event_type": "FULL_BUY",
        "desc": "same legacy desc",
    }
    order.update(overrides)
    return order


def current_t_revision_provider(current_revision=3):
    def provider(ticker):
        assert ticker == "SOXL"
        return current_revision

    return provider


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


def test_success_cache_uses_official_intent_id_not_legacy_description(tmp_path, monkeypatch):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = LegacyPositionalBroker(gate)
    cache = set()
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)
    first = official_order(event_type="FULL_BUY", desc="same desc")
    second = official_order(event_type="HALF_BUY", desc="same desc")

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [first, second],
            cache,
            True,
            "20260811",
            runtime_safety_gate=gate,
            current_t_revision_provider=current_t_revision_provider(),
        )
    )

    assert success is True
    assert failure == ""
    assert len(broker.calls) == 2
    assert "SOXL_same desc" not in cache
    assert cache == {compute_intent_id(dict(first, ticker="SOXL", order_type="LIMIT")), compute_intent_id(dict(second, ticker="SOXL", order_type="LIMIT"))}


def test_success_cache_dedupes_same_intent_even_when_ui_description_changes(tmp_path, monkeypatch):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = LegacyPositionalBroker(gate)
    cache = set()
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)
    first = official_order(desc="first UI text")
    second = official_order(desc="second UI text")

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [first, second],
            cache,
            True,
            "20260811",
            runtime_safety_gate=gate,
            current_t_revision_provider=current_t_revision_provider(),
        )
    )

    assert success is True
    assert failure == ""
    assert len(broker.calls) == 1
    assert cache == {compute_intent_id(dict(first, ticker="SOXL", order_type="LIMIT"))}
    assert "✅(기장전 보존)" in messages


def test_official_order_rejects_forged_intent_id_before_broker_call(tmp_path, monkeypatch):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = LegacyPositionalBroker(gate)
    cache = set()
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)
    forged = official_order(intent_id="FORGED-SAME")

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [forged],
            cache,
            True,
            "20260811",
            runtime_safety_gate=gate,
            current_t_revision_provider=current_t_revision_provider(),
        )
    )

    assert success is False
    assert broker.calls == []
    assert cache == set()
    assert "ORDER_INTENT_ID_MISMATCH" in messages
    assert "ORDER_INTENT_ID_MISMATCH" in failure


def test_official_order_rejects_stale_t_revision_before_broker_call(tmp_path, monkeypatch):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = LegacyPositionalBroker(gate)
    cache = set()
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)
    stale = official_order(t_revision=1)

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [stale],
            cache,
            True,
            "20260811",
            runtime_safety_gate=gate,
            current_t_revision_provider=lambda ticker: {"status": "OK", "t_revision": 3},
        )
    )

    assert success is False
    assert broker.calls == []
    assert cache == set()
    assert "ORDER_INTENT_STALE_T_REVISION" in messages
    assert "ORDER_INTENT_STALE_T_REVISION" in failure


@pytest.mark.parametrize(
    "provider",
    [
        None,
        lambda ticker: None,
        lambda ticker: 0,
        lambda ticker: {"status": "CORRUPT", "t_revision": 3},
        lambda ticker: {"status": "OK"},
        lambda ticker: (_ for _ in ()).throw(RuntimeError("ledger unavailable")),
    ],
)
def test_official_order_rejects_missing_or_corrupt_t_revision_status_before_broker_call(
    tmp_path, monkeypatch, provider
):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = LegacyPositionalBroker(gate)
    cache = set()
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [official_order()],
            cache,
            True,
            "20260811",
            runtime_safety_gate=gate,
            current_t_revision_provider=provider,
        )
    )

    assert success is False
    assert broker.calls == []
    assert cache == set()
    assert "ORDER_INTENT_T_REVISION_UNAVAILABLE" in messages
    assert "ORDER_INTENT_T_REVISION_UNAVAILABLE" in failure


@pytest.mark.parametrize("market_active", [True, False])
def test_official_order_with_matching_t_revision_still_submits_and_dedupes(
    tmp_path, monkeypatch, market_active
):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = LegacyPositionalBroker(gate)
    cache = set()
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)
    same_intent = official_order()

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [same_intent, dict(same_intent, desc="changed UI desc")],
            cache,
            market_active,
            "20260811",
            runtime_safety_gate=gate,
            current_t_revision_provider=current_t_revision_provider(3),
        )
    )

    assert success is True
    assert failure == ""
    assert len(broker.calls) == 1
    assert cache == {compute_intent_id(dict(same_intent, ticker="SOXL", order_type="LIMIT"))}
    assert "✅(기장전 보존)" in messages


def test_official_order_rejects_missing_event_type_without_legacy_fallback(tmp_path, monkeypatch):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = LegacyPositionalBroker(gate)
    cache = set()
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)
    official_without_event_type = official_order()
    official_without_event_type.pop("event_type")

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [official_without_event_type],
            cache,
            True,
            "20260811",
            runtime_safety_gate=gate,
        )
    )

    assert success is False
    assert broker.calls == []
    assert cache == set()
    assert "ORDER_INTENT_INVALID" in messages
    assert "event_type" in messages
    assert "ORDER_INTENT_INVALID" in failure


@pytest.mark.parametrize(
    "missing_field,official_marker",
    [
        ("strategy", {"strategy_revision": 1}),
        ("strategy_revision", {"strategy": "LAOER_V4_SOXL_20"}),
        ("t_revision", {"strategy": "LAOER_V4_SOXL_20"}),
        ("trade_date", {"strategy": "LAOER_V4_SOXL_20"}),
        ("ticker", {"strategy": "LAOER_V4_SOXL_20"}),
        ("side", {"strategy": "LAOER_V4_SOXL_20"}),
        ("price", {"strategy": "LAOER_V4_SOXL_20"}),
        ("qty", {"strategy": "LAOER_V4_SOXL_20"}),
        ("event_type", {"strategy": "LAOER_V4_SOXL_20"}),
        ("order_type", {"strategy": "LAOER_V4_SOXL_20"}),
    ],
)
def test_official_order_missing_required_identity_field_fails_closed_without_defaults(
    tmp_path, monkeypatch, missing_field, official_marker
):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = LegacyPositionalBroker(gate)
    cache = set()
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)
    incomplete = official_order(**official_marker)
    incomplete.pop(missing_field)

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [incomplete],
            cache,
            True,
            "20991231",
            runtime_safety_gate=gate,
        )
    )

    assert success is False
    assert broker.calls == []
    assert cache == set()
    assert "ORDER_INTENT_INVALID" in messages
    assert missing_field in messages
    assert "ORDER_INTENT_INVALID" in failure


def test_official_order_missing_exact_order_type_rejects_legacy_type_alias_without_broker_call(
    tmp_path, monkeypatch
):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = LegacyPositionalBroker(gate)
    cache = set()
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)
    official_with_only_legacy_type = official_order(type="LIMIT")
    official_with_only_legacy_type.pop("order_type")

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [official_with_only_legacy_type],
            cache,
            True,
            "20260811",
            runtime_safety_gate=gate,
            current_t_revision_provider=current_t_revision_provider(),
        )
    )

    assert success is False
    assert broker.calls == []
    assert cache == set()
    assert "ORDER_INTENT_INVALID" in messages
    assert "order_type" in messages
    assert "ORDER_INTENT_INVALID" in failure


@pytest.mark.parametrize("field", ["qty", "strategy_revision", "t_revision"])
@pytest.mark.parametrize("value", [1.9, "1.9"])
def test_official_order_rejects_non_integer_numeric_values_before_broker_call(
    tmp_path, monkeypatch, field, value
):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = LegacyPositionalBroker(gate)
    cache = set()
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)
    invalid = official_order(**{field: value})

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [invalid],
            cache,
            True,
            "20260811",
            runtime_safety_gate=gate,
            current_t_revision_provider=current_t_revision_provider(),
        )
    )

    assert success is False
    assert broker.calls == []
    assert cache == set()
    assert "ORDER_INTENT_INVALID" in messages
    assert field in messages
    assert "ORDER_INTENT_INVALID" in failure


@pytest.mark.parametrize("price", ["abc", "", "NaN", "Infinity", "-1", "0"])
def test_official_order_rejects_invalid_price_before_broker_call(tmp_path, monkeypatch, price):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = LegacyPositionalBroker(gate)
    cache = set()
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)
    invalid = official_order(price=price)

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [invalid],
            cache,
            True,
            "20260811",
            runtime_safety_gate=gate,
            current_t_revision_provider=current_t_revision_provider(),
        )
    )

    assert success is False
    assert broker.calls == []
    assert cache == set()
    assert "ORDER_INTENT_INVALID" in messages
    assert "price" in messages
    assert "ORDER_INTENT_INVALID" in failure


@pytest.mark.parametrize("field", ["qty", "strategy_revision", "t_revision"])
def test_official_order_accepts_digit_integer_strings(tmp_path, monkeypatch, field):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = LegacyPositionalBroker(gate)
    cache = set()
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)
    order = official_order(**{field: "3" if field == "t_revision" else "1"})

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [order],
            cache,
            True,
            "20260811",
            runtime_safety_gate=gate,
            current_t_revision_provider=current_t_revision_provider(),
        )
    )

    assert success is True
    assert failure == ""
    assert broker.calls == [("SOXL", "BUY", 1, 100.0, "LIMIT")]


@pytest.mark.parametrize(
    "official_marker",
    [
        {"strategy": "LAOER_V4_SOXL_20"},
        {"intent_id": "official-looking-intent-id"},
        {"strategy_revision": 1},
        {"t_revision": 3},
        {"event_type": "FULL_BUY"},
    ],
)
def test_any_official_marker_requires_full_official_schema_before_broker_call(
    tmp_path, monkeypatch, official_marker
):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = LegacyPositionalBroker(gate)
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)
    official_looking_legacy_shape = {
        "side": "BUY",
        "qty": 1,
        "price": "100",
        "type": "LIMIT",
        **official_marker,
    }

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [official_looking_legacy_shape],
            set(),
            True,
            "20991231",
            runtime_safety_gate=gate,
        )
    )

    assert success is False
    assert broker.calls == []
    assert "ORDER_INTENT_INVALID" in messages
    assert "ORDER_INTENT_INVALID" in failure


def test_legacy_order_without_official_markers_still_uses_legacy_idempotency(tmp_path, monkeypatch):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = LegacyPositionalBroker(gate)
    cache = set()
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [{"side": "BUY", "qty": 1, "price": "100", "type": "LIMIT", "desc": "legacy"}],
            cache,
            True,
            "20260811",
            runtime_safety_gate=gate,
        )
    )

    assert success is True
    assert failure == ""
    assert len(broker.calls) == 1
    assert len(cache) == 1


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
    assert len(cache) == 1
    assert "SOXL_first" not in cache
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


class TrackingIntentStore:
    def __init__(self):
        self.transitions = []
        self.accepted = []

    def transition_status(self, intent_id, status):
        self.transitions.append((intent_id, status))
        return {"intent_id": intent_id, "status": status}

    def record_accepted_order(self, intent_id, accepted_order):
        self.accepted.append((intent_id, dict(accepted_order)))
        self.transitions.append((intent_id, "SUBMITTED"))
        return {"intent_id": intent_id, "status": "SUBMITTED", "accepted_order": dict(accepted_order)}


def test_order_acceptance_transitions_official_intent_to_submitted_without_fill_or_t_update(
    tmp_path, monkeypatch
):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = LegacyPositionalBroker(gate)
    intent_store = TrackingIntentStore()
    cache = set()
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)
    order = official_order()
    order["intent_id"] = compute_intent_id(dict(order, ticker="SOXL", order_type="LIMIT"))

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [order],
            cache,
            True,
            "20260811",
            runtime_safety_gate=gate,
            current_t_revision_provider=current_t_revision_provider(),
            order_intent_store=intent_store,
            t_event_store=object(),
        )
    )

    assert success is True
    assert failure == ""
    assert intent_store.transitions == [(order["intent_id"], "SUBMITTED")]
    assert intent_store.accepted == [(order["intent_id"], {
        "account_fingerprint": SYNTHETIC_ACCOUNT_FINGERPRINT,
        "ticker": "SOXL",
        "exchange": "AMEX",
        "trade_date": "20260811",
        "order_no": "LEGACY-1",
    })]
    assert len(broker.calls) == 1
    assert len(cache) == 1


def test_order_acceptance_missing_order_number_fails_closed_before_submitted_or_cache(
    tmp_path, monkeypatch
):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = FailingBroker(gate, {"rt_cd": "0", "msg1": "accepted", "odno": ""})
    intent_store = TrackingIntentStore()
    cache = set()
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)
    order = official_order()
    order["intent_id"] = compute_intent_id(dict(order, ticker="SOXL", order_type="LIMIT"))

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [order],
            cache,
            True,
            "20260811",
            runtime_safety_gate=gate,
            current_t_revision_provider=current_t_revision_provider(),
            order_intent_store=intent_store,
        )
    )

    assert success is False
    assert cache == set()
    assert intent_store.transitions == []
    assert "ORDER_ACCEPTED_WITHOUT_ORDER_NO" in messages
    assert "ORDER_ACCEPTED_WITHOUT_ORDER_NO" in failure


def test_official_order_partial_reconciliation_guard_blocks_before_broker_call(tmp_path, monkeypatch):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = LegacyPositionalBroker(gate)
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [official_order()],
            set(),
            True,
            "20260811",
            runtime_safety_gate=gate,
            current_t_revision_provider=current_t_revision_provider(),
            fill_reconciliation_guard=lambda ticker: True,
        )
    )

    assert success is False
    assert broker.calls == []
    assert "PARTIAL_FILL_OPEN" in messages
    assert "PARTIAL_FILL_OPEN" in failure


def test_legacy_order_ignores_partial_reconciliation_guard_for_backward_compatibility(tmp_path, monkeypatch):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    broker = LegacyPositionalBroker(gate)
    monkeypatch.setattr(order_executor.asyncio, "sleep", no_sleep)

    success, messages, failure = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [{"side": "BUY", "qty": 1, "price": "100", "type": "LIMIT", "desc": "legacy"}],
            set(),
            True,
            "20260811",
            runtime_safety_gate=gate,
            fill_reconciliation_guard=lambda ticker: True,
        )
    )

    assert success is True
    assert failure == ""
    assert len(broker.calls) == 1
