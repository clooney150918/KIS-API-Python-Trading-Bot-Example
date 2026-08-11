import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

import kis_api_client
from kis_api_client import KisApiClient
from kis_order_engine import KisOrderEngine
from runtime_safety import RuntimeSafetyGate, SafetyDecision, TrustedMarketQuote
from test_runtime_safety import (
    SYNTHETIC_ACCOUNT_FINGERPRINT_KEY,
    SYNTHETIC_CANO,
    SYNTHETIC_PRODUCT_CODE,
    write_state,
)


ORDER_PATH = "/uapi/overseas-stock/v1/trading/order"
ORDER_URL = f"https://openapi.koreainvestment.com:9443{ORDER_PATH}"
ORDER_BODY = {
    "CANO": SYNTHETIC_CANO,
    "ACNT_PRDT_CD": SYNTHETIC_PRODUCT_CODE,
    "OVRS_EXCG_CD": "NASD",
    "PDNO": "SOXL",
    "ORD_QTY": "1",
    "OVRS_ORD_UNPR": "100.0",
    "ORD_SVR_DVSN_CD": "0",
    "ORD_DVSN": "00",
    "SLL_TYPE": "",
}


class FakeResponse:
    status_code = 200

    def json(self):
        return {"rt_cd": "0", "msg1": "OK", "output": {"ODNO": "1"}}


def make_client():
    client = object.__new__(KisApiClient)
    client.app_key = "synthetic-app-key"
    client.app_secret = "synthetic-app-secret"
    client.cano = SYNTHETIC_CANO
    client.acnt_prdt_cd = SYNTHETIC_PRODUCT_CODE
    client.base_url = "https://openapi.koreainvestment.com:9443"
    client.token = "synthetic-token"
    client.token_file = "/tmp/nonexistent-token"
    return client


def install_fake_transport(monkeypatch):
    calls = []

    def fake_post(*args, **kwargs):
        calls.append((args, kwargs))
        return FakeResponse()

    monkeypatch.setattr(kis_api_client.GlobalThrottle, "wait_api_sync", lambda: None)
    monkeypatch.setattr(kis_api_client.requests, "post", fake_post)
    return calls


def live_decision():
    return SafetyDecision(
        code="LIVE_AUTHORIZED",
        reason="test authorization",
        can_submit=True,
        shadow_only=False,
        revision=1,
        ticker="SOXL",
        side="BUY",
    )


def configure_engine_security(engine):
    engine.account_fingerprint_key = SYNTHETIC_ACCOUNT_FINGERPRINT_KEY
    engine.trusted_quote_provider = lambda ticker: TrustedMarketQuote(
        price=Decimal("100"),
        as_of=datetime.now(timezone.utc),
        source="KIS",
        ticker=ticker,
    )


def test_direct_call_api_order_post_is_blocked_before_transport(monkeypatch):
    client = make_client()
    transport_calls = install_fake_transport(monkeypatch)

    result = client._call_api("TTTT1002U", ORDER_PATH, "POST", body=ORDER_BODY)

    assert transport_calls == []
    assert result["rt_cd"] == "999"
    assert result["msg1"]
    assert result["odno"] == ""
    assert result["shadow"] is False
    assert result["safety_decision"]["code"] == "ORDER_TRANSPORT_CAPABILITY_REQUIRED"


def test_direct_api_request_order_post_is_blocked_before_transport(monkeypatch):
    client = make_client()
    transport_calls = install_fake_transport(monkeypatch)
    headers = client._get_header("TTTT1002U")

    response, result = client._api_request(
        "POST", ORDER_URL, headers, data=ORDER_BODY
    )

    assert response is None
    assert transport_calls == []
    assert result["rt_cd"] == "999"
    assert result["odno"] == ""
    assert result["shadow"] is False
    assert result["safety_decision"]["code"] == "ORDER_TRANSPORT_CAPABILITY_REQUIRED"


def test_wrong_and_reused_order_transport_capabilities_are_blocked(monkeypatch):
    client = make_client()
    transport_calls = install_fake_transport(monkeypatch)
    headers = client._get_header("TTTT1002U")
    capability = client._issue_order_transport_capability(live_decision())

    first_response, first_result = client._api_request(
        "POST", ORDER_URL, dict(headers), data=ORDER_BODY,
        order_transport_capability=capability,
    )
    wrong_response, wrong_result = client._api_request(
        "POST", ORDER_URL, dict(headers), data=ORDER_BODY,
        order_transport_capability=object(),
    )
    reused_response, reused_result = client._api_request(
        "POST", ORDER_URL, dict(headers), data=ORDER_BODY,
        order_transport_capability=capability,
    )

    assert first_response is not None
    assert first_result["rt_cd"] == "0"
    assert len(transport_calls) == 1
    assert wrong_response is None
    assert wrong_result["safety_decision"]["code"] == "ORDER_TRANSPORT_CAPABILITY_INVALID"
    assert reused_response is None
    assert reused_result["safety_decision"]["code"] == "ORDER_TRANSPORT_CAPABILITY_INVALID"
    assert len(transport_calls) == 1


def test_kis_order_engine_final_gate_issues_one_capability_for_one_post(tmp_path, monkeypatch):
    state_path = write_state(tmp_path / "runtime_safety.json")
    engine = object.__new__(KisOrderEngine)
    engine.runtime_safety_gate = RuntimeSafetyGate(state_path)
    configure_engine_security(engine)
    engine.cano = SYNTHETIC_CANO
    engine.acnt_prdt_cd = SYNTHETIC_PRODUCT_CODE
    engine.app_key = "synthetic-app-key"
    engine.app_secret = "synthetic-app-secret"
    engine.base_url = "https://openapi.koreainvestment.com:9443"
    engine.token = "synthetic-token"
    engine.token_file = "/tmp/nonexistent-token"
    engine._safe_float = lambda value: float(value)
    engine._ceil_2 = lambda value: float(value)
    engine._get_exchange_code = lambda *args, **kwargs: "NASD"
    engine._excg_cd_cache = {}
    transport_calls = install_fake_transport(monkeypatch)

    result = engine.send_order("SOXL", "BUY", 1, "100", "LIMIT")

    assert result == {"rt_cd": "0", "msg1": "OK", "odno": "1"}
    assert len(transport_calls) == 1
    assert json.loads(transport_calls[0][1]["data"])["ORD_DVSN"] == "00"


def make_live_engine(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json", max_order_notional="1000")
    engine = object.__new__(KisOrderEngine)
    engine.runtime_safety_gate = RuntimeSafetyGate(state_path)
    configure_engine_security(engine)
    engine.cano = SYNTHETIC_CANO
    engine.acnt_prdt_cd = SYNTHETIC_PRODUCT_CODE
    engine.app_key = "synthetic-app-key"
    engine.app_secret = "synthetic-app-secret"
    engine.base_url = "https://openapi.koreainvestment.com:9443"
    engine.token = "synthetic-token"
    engine.token_file = "/tmp/nonexistent-token"
    engine._safe_float = lambda value: float(value)
    engine._ceil_2 = lambda value: float(value)
    engine._get_exchange_code = lambda *args, **kwargs: "NASD"
    engine._excg_cd_cache = {}
    return engine


def posted_request(transport_calls):
    assert len(transport_calls) == 1
    args, kwargs = transport_calls[0]
    return args[0], kwargs["headers"], json.loads(kwargs["data"])


def test_lowercase_buy_is_canonicalized_for_gate_and_buy_tr_id(tmp_path, monkeypatch):
    engine = make_live_engine(tmp_path)
    transport_calls = install_fake_transport(monkeypatch)

    result = engine.send_order("SOXL", "buy", 1, "100", "limit")

    assert result["rt_cd"] == "0"
    _, headers, body = posted_request(transport_calls)
    assert headers["tr_id"] == "TTTT1002U"
    assert body["SLL_TYPE"] == ""
    assert body["ORD_DVSN"] == "00"


def test_lowercase_moc_is_not_downgraded_to_limit_on_wire(tmp_path, monkeypatch):
    engine = make_live_engine(tmp_path)
    transport_calls = install_fake_transport(monkeypatch)

    result = engine.send_order(
        "SOXL", "sell", 1, "0", "moc", risk_reference_price="100"
    )

    assert result["rt_cd"] == "0"
    _, headers, body = posted_request(transport_calls)
    assert headers["tr_id"] == "TTTT1006U"
    assert body["SLL_TYPE"] == "00"
    assert body["ORD_DVSN"] == "33"
    assert body["OVRS_ORD_UNPR"] == "0"


def test_reservation_moc_wires_official_code_zero_price_and_risk_reference(
    tmp_path, monkeypatch
):
    engine = make_live_engine(tmp_path)
    transport_calls = install_fake_transport(monkeypatch)

    result = engine.send_reservation_order(
        "SOXL", "sell", 1, "0", "moc", risk_reference_price="100"
    )

    assert result["rt_cd"] == "0"
    _, headers, body = posted_request(transport_calls)
    assert headers["tr_id"] == "TTTT3016U"
    assert body["ORD_DVSN"] == "33"
    assert float(body["FT_ORD_UNPR3"]) == 0.0


@pytest.mark.parametrize(
    "invoke",
    [
        lambda engine: engine.send_order(
            "SOXL", "BUY", 1, "0", "MOC", risk_reference_price="100"
        ),
        lambda engine: engine.send_daytime_order(
            "SOXL", "SELL", 1, "0", "MOC", risk_reference_price="100"
        ),
        lambda engine: engine.send_reservation_order(
            "SOXL", "SELL", 1, "100", "VWAP"
        ),
    ],
)
def test_unsupported_endpoint_side_and_order_type_combinations_fail_before_http(
    tmp_path, monkeypatch, invoke
):
    engine = make_live_engine(tmp_path)
    transport_calls = install_fake_transport(monkeypatch)

    result = invoke(engine)

    assert transport_calls == []
    assert set(("rt_cd", "msg1", "odno", "shadow", "safety_decision")) <= set(result)
    assert result["rt_cd"] == "999"
    assert result["odno"] == ""
    assert result["shadow"] is False
    assert result["safety_decision"]["code"] == "UNSUPPORTED_ORDER_TYPE"
