from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import hmac
import json

import pytest

from kis_order_engine import KisOrderEngine
from runtime_safety import (
    RuntimeSafetyGate,
    TrustedMarketQuote,
    account_fingerprint,
    legacy_account_fingerprint,
)
from test_runtime_safety import (
    SYNTHETIC_ACCOUNT_FINGERPRINT,
    SYNTHETIC_ACCOUNT_FINGERPRINT_KEY,
    SYNTHETIC_CANO,
    SYNTHETIC_PRODUCT_CODE,
    authorize,
    write_state,
)


NOW = datetime(2026, 8, 11, 1, 2, 3, tzinfo=timezone.utc)


def quote(price="100", *, ticker="SOXL", source="KIS", as_of=NOW):
    return TrustedMarketQuote(
        price=Decimal(price), ticker=ticker, source=source, as_of=as_of
    )


def market_authorize(path, trusted_quote, **overrides):
    return authorize(
        path,
        price="0",
        order_type="MOC",
        risk_reference_price="100",
        trusted_market_quote=trusted_quote,
        **overrides,
    )


def test_market_order_caller_reference_alone_never_authorizes(tmp_path):
    state = write_state(tmp_path / "runtime_safety.json")

    decision = market_authorize(state, None)

    assert decision.code == "TRUSTED_MARKET_QUOTE_UNAVAILABLE"
    assert decision.can_submit is False


@pytest.mark.parametrize(
    "bad_quote,expected",
    [
        (quote(source="YAHOO"), "TRUSTED_MARKET_QUOTE_SOURCE_INVALID"),
        (quote(ticker="TQQQ"), "TRUSTED_MARKET_QUOTE_TICKER_MISMATCH"),
        (quote(price="0"), "TRUSTED_MARKET_QUOTE_PRICE_INVALID"),
        (quote(price="-1"), "TRUSTED_MARKET_QUOTE_PRICE_INVALID"),
        (quote(price="NaN"), "TRUSTED_MARKET_QUOTE_PRICE_INVALID"),
        (quote(as_of=datetime(2026, 8, 11, 1, 2, 3)), "TRUSTED_MARKET_QUOTE_TIMESTAMP_INVALID"),
        (quote(as_of=NOW + timedelta(seconds=6)), "TRUSTED_MARKET_QUOTE_FROM_FUTURE"),
        (quote(as_of=NOW - timedelta(seconds=121)), "TRUSTED_MARKET_QUOTE_STALE"),
    ],
)
def test_market_quote_validation_fails_closed_with_stable_codes(
    tmp_path, bad_quote, expected
):
    state = write_state(tmp_path / "runtime_safety.json")
    gate = RuntimeSafetyGate(state, clock=lambda: NOW)

    decision = gate.authorize(
        "SOXL",
        "BUY",
        1,
        "0",
        account_fingerprint=SYNTHETIC_ACCOUNT_FINGERPRINT,
        order_type="MOC",
        risk_reference_price="100",
        trusted_market_quote=bad_quote,
    )

    assert decision.code == expected
    assert decision.can_submit is False


@pytest.mark.parametrize(
    "field,value",
    [
        ("market_quote_max_age_seconds", 0),
        ("market_quote_max_age_seconds", True),
        ("market_quote_max_age_seconds", 3601),
        ("market_slippage_buffer_percent", 0),
        ("market_slippage_buffer_percent", 1.5),
        ("market_slippage_buffer_percent", "-0.01"),
        ("market_slippage_buffer_percent", "25.01"),
    ],
)
def test_market_risk_config_rejects_wrong_types_and_unreasonable_bounds(
    tmp_path, field, value
):
    state = write_state(tmp_path / "runtime_safety.json", **{field: value})

    decision = authorize(state)

    assert decision.code == "SAFETY_STATE_INVALID_SCHEMA"


def test_market_notional_uses_higher_quote_then_buffer_and_cent_ceiling(tmp_path):
    state = write_state(
        tmp_path / "runtime_safety.json",
        max_order_notional="210.00",
        market_slippage_buffer_percent="5.00",
    )
    gate = RuntimeSafetyGate(state, clock=lambda: NOW)

    decision = gate.authorize(
        "SOXL",
        "SELL",
        2,
        "0",
        account_fingerprint=SYNTHETIC_ACCOUNT_FINGERPRINT,
        order_type="MOC",
        risk_reference_price="1",
        trusted_market_quote=quote("100.001"),
    )

    assert decision.code == "NOTIONAL_LIMIT_EXCEEDED"
    assert decision.notional == Decimal("210.02")


def test_limit_order_never_calls_quote_provider(tmp_path):
    engine = make_engine(tmp_path)

    class ExplodingProvider:
        calls = 0

        def get_quote(self, ticker):
            self.calls += 1
            raise AssertionError("LIMIT must not request a quote")

    provider = ExplodingProvider()
    engine.trusted_quote_provider = provider
    calls = []
    engine._call_api = lambda *a, **kw: calls.append((a, kw)) or {
        "rt_cd": "0", "msg1": "OK", "output": {"ODNO": "1"}
    }

    result = engine.send_order("SOXL", "BUY", 1, "100", "LIMIT")

    assert result["rt_cd"] == "0"
    assert provider.calls == 0
    assert len(calls) == 1


def make_engine(tmp_path, *, provider=None, key=SYNTHETIC_ACCOUNT_FINGERPRINT_KEY):
    state = write_state(tmp_path / "runtime_safety.json", max_order_notional="1000")
    engine = object.__new__(KisOrderEngine)
    engine.runtime_safety_gate = RuntimeSafetyGate(state, clock=lambda: NOW)
    engine.shadow_intent_recorder = None
    engine.trusted_quote_provider = provider
    engine.account_fingerprint_key = key
    engine.cano = SYNTHETIC_CANO
    engine.acnt_prdt_cd = SYNTHETIC_PRODUCT_CODE
    engine._safe_float = lambda value: float(value)
    engine._ceil_2 = lambda value: float(value)
    engine._get_exchange_code = lambda *args, **kwargs: "NASD"
    engine._excg_cd_cache = {}
    return engine


def test_final_kis_boundary_blocks_provider_failure_before_http(tmp_path):
    class BrokenProvider:
        def get_quote(self, ticker):
            raise OSError("synthetic provider outage")

    engine = make_engine(tmp_path, provider=BrokenProvider())
    calls = []
    engine._call_api = lambda *a, **kw: calls.append((a, kw))

    result = engine.send_order(
        "SOXL", "SELL", 1, "0", "MOC", risk_reference_price="100"
    )

    assert calls == []
    assert result["safety_decision"]["code"] == "TRUSTED_MARKET_QUOTE_PROVIDER_FAILED"
    assert "synthetic provider outage" not in json.dumps(result)


def test_final_kis_boundary_blocks_missing_provider_before_http(tmp_path):
    engine = make_engine(tmp_path, provider=None)
    calls = []
    engine._call_api = lambda *a, **kw: calls.append((a, kw))

    result = engine.send_order(
        "SOXL", "SELL", 1, "0", "MOC", risk_reference_price="100"
    )

    assert calls == []
    assert result["safety_decision"]["code"] == "TRUSTED_MARKET_QUOTE_UNAVAILABLE"


def test_hmac_fingerprint_requires_key_and_legacy_hash_is_separate():
    expected = hmac.new(
        SYNTHETIC_ACCOUNT_FINGERPRINT_KEY,
        f"{SYNTHETIC_CANO}:{SYNTHETIC_PRODUCT_CODE}".encode(),
        hashlib.sha256,
    ).hexdigest()

    assert account_fingerprint(
        SYNTHETIC_CANO,
        SYNTHETIC_PRODUCT_CODE,
        key=SYNTHETIC_ACCOUNT_FINGERPRINT_KEY,
    ) == expected
    assert legacy_account_fingerprint(SYNTHETIC_CANO, SYNTHETIC_PRODUCT_CODE) != expected
    with pytest.raises(ValueError):
        account_fingerprint(SYNTHETIC_CANO, SYNTHETIC_PRODUCT_CODE, key=b"short")


@pytest.mark.parametrize("key", [None, b"raw-key-secret"])
def test_final_boundary_missing_or_short_hmac_key_fails_closed_without_leak(
    tmp_path, key
):
    engine = make_engine(tmp_path, provider=lambda ticker: quote(), key=key)
    calls = []
    engine._call_api = lambda *a, **kw: calls.append((a, kw))

    result = engine.send_order("SOXL", "BUY", 1, "100", "LIMIT")

    serialized = json.dumps(result)
    assert calls == []
    assert result["safety_decision"]["code"] == "ACCOUNT_FINGERPRINT_KEY_UNAVAILABLE"
    assert SYNTHETIC_CANO not in serialized
    if key is not None:
        assert key.decode("utf-8") not in serialized


def test_safety_state_requires_owner_only_permissions(tmp_path):
    state = write_state(tmp_path / "runtime_safety.json")
    state.chmod(0o644)

    decision = authorize(state)

    assert decision.code == "SAFETY_STATE_INSECURE_PERMISSIONS"