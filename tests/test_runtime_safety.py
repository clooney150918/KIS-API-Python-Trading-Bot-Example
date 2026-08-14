import asyncio
from datetime import datetime, timezone
from decimal import Decimal
import hmac
import hashlib
import json
from pathlib import Path

import pytest

from runtime_safety import RuntimeSafetyGate, TrustedMarketQuote


ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_STATE = ROOT / "data" / "runtime_safety.json"
PRODUCTION_CHECKPOINT = ROOT / "data" / "runtime_safety.revision.json"
SYNTHETIC_CANO = "00000000"
SYNTHETIC_PRODUCT_CODE = "01"
SYNTHETIC_ACCOUNT_FINGERPRINT_KEY = b"synthetic-test-only-hmac-key-32b!"
SYNTHETIC_ACCOUNT_FINGERPRINT = hmac.new(
    SYNTHETIC_ACCOUNT_FINGERPRINT_KEY,
    f"{SYNTHETIC_CANO}:{SYNTHETIC_PRODUCT_CODE}".encode("utf-8"),
    hashlib.sha256,
).hexdigest()


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
        "allowed_account_fingerprints": [SYNTHETIC_ACCOUNT_FINGERPRINT],
        "max_order_quantity": 100,
        "max_order_notional": "25000.00",
        "market_quote_max_age_seconds": 120,
        "market_slippage_buffer_percent": "5.00",
    }
    state.update(overrides)
    path.write_text(json.dumps(state), encoding="utf-8")
    path.chmod(0o600)
    checkpoint_path = path.with_name("runtime_safety.revision.json")
    if not checkpoint_path.exists():
        checkpoint_path.write_text(json.dumps({"revision": 1}), encoding="utf-8")
    return path


def authorize(path, **overrides):
    request = {
        "ticker": "SOXL",
        "side": "BUY",
        "quantity": 2,
        "price": "100.25",
        "account_fingerprint": SYNTHETIC_ACCOUNT_FINGERPRINT,
    }
    request.update(overrides)
    return RuntimeSafetyGate(path).authorize(**request)


def trusted_quote(price="100", ticker="SOXL"):
    return TrustedMarketQuote(
        price=Decimal(price),
        as_of=datetime.now(timezone.utc),
        source="KIS",
        ticker=ticker,
    )


def test_missing_state_file_fails_closed(tmp_path):
    decision = authorize(tmp_path / "missing.json")

    assert decision.code == "SAFETY_STATE_MISSING"
    assert decision.can_submit is False


def test_corrupt_json_fails_closed(tmp_path):
    state_path = tmp_path / "runtime_safety.json"
    state_path.write_text("{not-json", encoding="utf-8")
    state_path.chmod(0o600)

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
        ("allowed_account_fingerprints", None),
        ("max_order_quantity", 0),
        ("max_order_notional", "not-money"),
    ],
)
def test_state_type_errors_fail_closed(tmp_path, field, value):
    state_path = write_state(tmp_path / "runtime_safety.json", **{field: value})

    decision = authorize(state_path)

    assert decision.code == "SAFETY_STATE_INVALID_SCHEMA"
    assert decision.can_submit is False


@pytest.mark.parametrize(
    "field,value",
    [
        ("reason", None),
        ("reason", ""),
        ("updated_by", 7),
        ("updated_by", "   "),
        ("updated_at", "2026-08-11"),
        ("updated_at", "2026-08-11T00:00:00"),
        ("updated_at", "2026-13-11T00:00:00Z"),
    ],
)
def test_audit_metadata_requires_non_empty_strings_and_utc_timestamp(tmp_path, field, value):
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
    from shadow_intent import ShadowIntentRecorder

    state_path = write_state(tmp_path / "runtime_safety.json", shadow_only=True)
    gate = RuntimeSafetyGate(state_path)
    intent_path = tmp_path / "shadow_intents.jsonl"

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
            shadow_intent_recorder=ShadowIntentRecorder(intent_path),
        )
    )

    assert broker.calls == 0
    assert result[0] is False
    assert "SHADOW_ONLY" in result[1]
    assert "SHADOW_ONLY" in result[2]
    records = [json.loads(line) for line in intent_path.read_text(encoding="utf-8").splitlines()]
    assert len(records) == 1
    assert records[0]["schema_version"] == 2
    assert records[0]["status"] == "SHADOW_RECORDED"
    assert records[0]["ticker"] == "SOXL"
    assert records[0]["side"] == "BUY"
    assert records[0]["quantity"] == "2"
    assert records[0]["price"] == "100"
    assert records[0]["order_type"] == "LIMIT"
    assert records[0]["safety_revision"] == 1
    assert len(records[0]["intent_id"]) == 64
    assert records[0]["timestamp"].endswith("Z")
    assert SYNTHETIC_CANO not in json.dumps(records[0])


def test_shadow_recorder_failure_fails_closed_without_broker_call(tmp_path):
    from order_executor import execute_order_list

    state_path = write_state(tmp_path / "runtime_safety.json", shadow_only=True)

    class FakeBroker:
        calls = 0

        def send_order(self, *args, **kwargs):
            self.calls += 1

    class BrokenRecorder:
        def record(self, **intent):
            raise OSError("disk full")

    broker = FakeBroker()
    result = asyncio.run(
        execute_order_list(
            broker,
            "SOXL",
            [{"side": "SELL", "qty": 3, "price": "101", "type": "LIMIT"}],
            set(),
            True,
            "20260811",
            runtime_safety_gate=RuntimeSafetyGate(state_path),
            shadow_intent_recorder=BrokenRecorder(),
        )
    )

    assert broker.calls == 0
    assert result[0] is False
    assert "SHADOW_INTENT_RECORD_FAILED" in result[1]


def test_shadow_recorder_returns_existing_intent_for_same_retry_key(tmp_path):
    from shadow_intent import ShadowIntentRecorder

    intent_path = tmp_path / "shadow_intents.jsonl"
    recorder = ShadowIntentRecorder(intent_path)
    intent = {
        "ticker": "SOXL",
        "side": "BUY",
        "quantity": 2,
        "price": "100.00",
        "order_type": "LIMIT",
        "safety_revision": 1,
        "idempotency_key": "stable-retry-key",
    }

    first = recorder.record(**intent)
    second = recorder.record(**intent)
    records = [json.loads(line) for line in intent_path.read_text(encoding="utf-8").splitlines()]

    assert len(records) == 1
    assert records[0]["intent_id"] == first["intent_id"]
    assert second["intent_id"] == first["intent_id"]


def test_invalid_ticker_is_blocked(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json")

    decision = authorize(state_path, ticker="TQQQ")

    assert decision.code == "TICKER_NOT_ALLOWED"
    assert decision.can_submit is False


@pytest.mark.parametrize("fingerprint", [None, "", "0" * 64])
def test_missing_or_wrong_account_fingerprint_is_blocked(tmp_path, fingerprint):
    state_path = write_state(tmp_path / "runtime_safety.json")

    decision = authorize(state_path, account_fingerprint=fingerprint)

    assert decision.code == "ACCOUNT_NOT_ALLOWED"
    assert decision.can_submit is False
    assert SYNTHETIC_CANO not in json.dumps(decision.as_dict())


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


@pytest.mark.parametrize("order_type", ["MARKET", "MOC", "MOO"])
@pytest.mark.parametrize("reference", [None, "0", "-1", "NaN", "Infinity"])
def test_market_order_requires_positive_finite_risk_reference(
    tmp_path, order_type, reference
):
    state_path = write_state(tmp_path / "runtime_safety.json")

    decision = authorize(
        state_path,
        quantity=2,
        price="0",
        order_type=order_type,
        risk_reference_price=reference,
    )

    assert decision.code == "INVALID_RISK_REFERENCE_PRICE"
    assert decision.can_submit is False


def test_market_order_uses_trusted_quote_and_buffer_for_notional_limit(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json", max_order_notional="210")

    allowed = authorize(
        state_path,
        quantity=2,
        price="0",
        order_type="MOC",
        risk_reference_price="100",
        trusted_market_quote=trusted_quote("100"),
    )
    blocked = authorize(
        state_path,
        quantity=2,
        price="0",
        order_type="MOO",
        risk_reference_price="100.01",
        trusted_market_quote=trusted_quote("100.01"),
    )

    assert allowed.code == "LIVE_AUTHORIZED"
    assert allowed.notional == Decimal("210.00")
    assert blocked.code == "NOTIONAL_LIMIT_EXCEEDED"


@pytest.mark.parametrize("order_type", ["MARKET", "MOC", "MOO"])
def test_each_market_order_type_uses_positive_risk_reference(tmp_path, order_type):
    state_path = write_state(tmp_path / "runtime_safety.json")

    decision = authorize(
        state_path,
        quantity=2,
        price="0",
        order_type=order_type,
        risk_reference_price="100.25",
        trusted_market_quote=trusted_quote("100.25"),
    )

    assert decision.code == "LIVE_AUTHORIZED"
    assert decision.notional == Decimal("210.54")


def test_limit_price_zero_remains_blocked_even_with_risk_reference(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json")

    decision = authorize(
        state_path,
        price="0",
        order_type="LIMIT",
        risk_reference_price="100",
    )

    assert decision.code == "INVALID_NOTIONAL"


def test_revision_rollback_is_blocked_by_same_gate_instance(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json", revision=2)
    checkpoint_path = tmp_path / "runtime_safety.revision.json"
    gate = RuntimeSafetyGate(state_path, checkpoint_path=checkpoint_path)

    first = gate.authorize(
        "SOXL", "BUY", 1, "100", account_fingerprint=SYNTHETIC_ACCOUNT_FINGERPRINT
    )
    write_state(state_path, revision=1)
    second = gate.authorize(
        "SOXL", "BUY", 1, "100", account_fingerprint=SYNTHETIC_ACCOUNT_FINGERPRINT
    )

    assert first.code == "LIVE_AUTHORIZED"
    assert second.code == "REVISION_ROLLBACK"
    assert second.revision == 1
    assert second.can_submit is False


def test_revision_rollback_is_blocked_after_gate_recreation(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json", revision=2)
    checkpoint_path = tmp_path / "runtime_safety.revision.json"

    first = RuntimeSafetyGate(state_path, checkpoint_path=checkpoint_path).authorize(
        "SOXL", "BUY", 1, "100", account_fingerprint=SYNTHETIC_ACCOUNT_FINGERPRINT
    )
    write_state(state_path, revision=1)
    second = RuntimeSafetyGate(state_path, checkpoint_path=checkpoint_path).authorize(
        "SOXL", "BUY", 1, "100", account_fingerprint=SYNTHETIC_ACCOUNT_FINGERPRINT
    )

    assert first.code == "LIVE_AUTHORIZED"
    assert json.loads(checkpoint_path.read_text(encoding="utf-8"))["revision"] == 2
    assert second.code == "REVISION_ROLLBACK"
    assert second.can_submit is False


def test_corrupt_revision_checkpoint_fails_closed(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json", revision=2)
    checkpoint_path = tmp_path / "runtime_safety.revision.json"
    checkpoint_path.write_text("{broken", encoding="utf-8")

    decision = RuntimeSafetyGate(state_path, checkpoint_path=checkpoint_path).authorize(
        "SOXL", "BUY", 1, "100"
    )

    assert decision.code == "REVISION_CHECKPOINT_INVALID"
    assert decision.can_submit is False


def test_missing_revision_checkpoint_fails_closed(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json")
    state_path.with_name("runtime_safety.revision.json").unlink()

    decision = authorize(state_path)

    assert decision.code == "REVISION_CHECKPOINT_MISSING"
    assert decision.can_submit is False


def test_revision_checkpoint_update_uses_atomic_replace_and_file_and_directory_fsync(
    tmp_path, monkeypatch
):
    import runtime_safety

    state_path = write_state(tmp_path / "runtime_safety.json", revision=2)
    replace_calls = []
    fsync_calls = []
    real_replace = runtime_safety.os.replace
    real_fsync = runtime_safety.os.fsync

    def tracked_replace(source, destination):
        replace_calls.append((Path(source), Path(destination)))
        return real_replace(source, destination)

    def tracked_fsync(fd):
        fsync_calls.append(fd)
        return real_fsync(fd)

    monkeypatch.setattr(runtime_safety.os, "replace", tracked_replace)
    monkeypatch.setattr(runtime_safety.os, "fsync", tracked_fsync)

    decision = authorize(state_path)

    assert decision.code == "LIVE_AUTHORIZED"
    assert len(replace_calls) == 1
    assert replace_calls[0][1] == state_path.with_name("runtime_safety.revision.json")
    assert len(fsync_calls) == 2


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
    engine.account_fingerprint_key = SYNTHETIC_ACCOUNT_FINGERPRINT_KEY
    engine.cano = SYNTHETIC_CANO
    engine.acnt_prdt_cd = SYNTHETIC_PRODUCT_CODE
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


def test_direct_kis_boundary_derives_account_fingerprint_and_blocks_wrong_account(tmp_path):
    from kis_order_engine import KisOrderEngine

    state_path = write_state(tmp_path / "runtime_safety.json")
    engine = object.__new__(KisOrderEngine)
    engine.runtime_safety_gate = RuntimeSafetyGate(state_path)
    engine.account_fingerprint_key = SYNTHETIC_ACCOUNT_FINGERPRINT_KEY
    engine.cano = "11111111"
    engine.acnt_prdt_cd = SYNTHETIC_PRODUCT_CODE
    engine._safe_float = lambda value: float(value)
    engine._ceil_2 = lambda value: float(value)
    engine._get_exchange_code = lambda *args, **kwargs: "NASD"
    engine._excg_cd_cache = {}
    calls = []
    engine._call_api = lambda *args, **kwargs: calls.append((args, kwargs)) or {"rt_cd": "0"}

    result = engine.send_order("SOXL", "BUY", 1, "100", "LIMIT")

    assert calls == []
    assert result["safety_decision"]["code"] == "ACCOUNT_NOT_ALLOWED"
    assert "11111111" not in json.dumps(result)


def test_final_gate_uses_ceiled_kis_price_and_blocks_new_limit_breach(tmp_path):
    from decimal import ROUND_CEILING
    from kis_order_engine import KisOrderEngine

    state_path = write_state(tmp_path / "runtime_safety.json", max_order_notional="99.9995")
    engine = object.__new__(KisOrderEngine)
    engine.runtime_safety_gate = RuntimeSafetyGate(state_path)
    engine.account_fingerprint_key = SYNTHETIC_ACCOUNT_FINGERPRINT_KEY
    engine.cano = SYNTHETIC_CANO
    engine.acnt_prdt_cd = SYNTHETIC_PRODUCT_CODE
    engine._safe_float = lambda value: float(value)
    engine._ceil_2 = lambda value: float(
        Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_CEILING)
    )
    engine._get_exchange_code = lambda *args, **kwargs: "NASD"
    engine._excg_cd_cache = {}
    calls = []
    engine._call_api = lambda *args, **kwargs: calls.append((args, kwargs)) or {"rt_cd": "0"}

    result = engine.send_order("SOXL", "BUY", 1, "99.9994", "LIMIT")

    assert calls == []
    assert result["safety_decision"]["code"] == "NOTIONAL_LIMIT_EXCEEDED"
    assert result["safety_decision"]["notional"] == "100.0"


def test_reservation_final_gate_uses_ceiled_body_price_and_blocks_limit_breach(tmp_path):
    from decimal import ROUND_CEILING
    from kis_order_engine import KisOrderEngine

    state_path = write_state(tmp_path / "runtime_safety.json", max_order_notional="99.9995")
    engine = object.__new__(KisOrderEngine)
    engine.runtime_safety_gate = RuntimeSafetyGate(state_path)
    engine.account_fingerprint_key = SYNTHETIC_ACCOUNT_FINGERPRINT_KEY
    engine.cano = SYNTHETIC_CANO
    engine.acnt_prdt_cd = SYNTHETIC_PRODUCT_CODE
    engine._safe_float = lambda value: float(value)
    engine._ceil_2 = lambda value: float(
        Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_CEILING)
    )
    engine._get_exchange_code = lambda *args, **kwargs: "NASD"
    calls = []
    engine._call_api = lambda *args, **kwargs: calls.append((args, kwargs)) or {"rt_cd": "0"}

    result = engine.send_reservation_order("SOXL", "BUY", 1, "99.9994", "LIMIT")

    assert calls == []
    assert result["safety_decision"]["code"] == "NOTIONAL_LIMIT_EXCEEDED"
    assert result["safety_decision"]["notional"] == "100.0"


def test_kis_moc_accepts_explicit_risk_reference_and_submits_zero_price(tmp_path):
    from kis_order_engine import KisOrderEngine

    state_path = write_state(tmp_path / "runtime_safety.json", max_order_notional="105")
    engine = object.__new__(KisOrderEngine)
    engine.runtime_safety_gate = RuntimeSafetyGate(state_path)
    engine.account_fingerprint_key = SYNTHETIC_ACCOUNT_FINGERPRINT_KEY
    engine.trusted_quote_provider = lambda ticker: trusted_quote(ticker=ticker)
    engine.cano = SYNTHETIC_CANO
    engine.acnt_prdt_cd = SYNTHETIC_PRODUCT_CODE
    engine._safe_float = lambda value: float(value)
    engine._ceil_2 = lambda value: float(value)
    engine._get_exchange_code = lambda *args, **kwargs: "NASD"
    engine._excg_cd_cache = {}
    calls = []

    def fake_call(*args, **kwargs):
        calls.append((args, kwargs))
        return {"rt_cd": "0", "msg1": "OK", "output": {"ODNO": "M1"}}

    engine._call_api = fake_call

    result = engine.send_order(
        "SOXL", "SELL", 1, "0", "MOC", risk_reference_price="100"
    )

    assert result["rt_cd"] == "0"
    assert len(calls) == 1
    assert calls[0][1]["body"]["OVRS_ORD_UNPR"] == "0"


def test_production_runtime_state_is_safe_default_and_limits_are_explicit():
    raw = json.loads(PRODUCTION_STATE.read_text(encoding="utf-8"), parse_float=Decimal)
    checkpoint = json.loads(PRODUCTION_CHECKPOINT.read_text(encoding="utf-8"))

    assert raw["operator_halt"] is True
    assert raw["live_armed"] is False
    assert raw["shadow_only"] is True
    assert raw["revision"] == 1
    assert raw["allowed_tickers"] == ["SOXL"]
    assert raw["allowed_account_fingerprints"] == ["UNCONFIGURED"]
    assert Decimal(str(raw["max_order_quantity"])) > 0
    assert Decimal(str(raw["max_order_notional"])) > 0
    assert checkpoint == {"revision": 1}

    decision = RuntimeSafetyGate(PRODUCTION_STATE).authorize("SOXL", "BUY", 1, "100")
    assert decision.code == "OPERATOR_HALT"
    assert decision.can_submit is False
