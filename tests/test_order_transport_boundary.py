import json
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal

import pytest

import kis_api_client
from kis_api_client import KisApiClient
from kis_order_engine import KisOrderEngine
from runtime_safety import RuntimeSafetyGate, TrustedMarketQuote
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
    assert result["safety_decision"]["code"] == "ORDER_AUTHORIZATION_REQUIRED"


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
    assert result["safety_decision"]["code"] == "ORDER_AUTHORIZATION_REQUIRED"


def issue_wire_authorization(tmp_path, body=None):
    gate = RuntimeSafetyGate(write_state(tmp_path / "runtime_safety.json"))
    fingerprint = __import__("runtime_safety").account_fingerprint(
        SYNTHETIC_CANO,
        SYNTHETIC_PRODUCT_CODE,
        key=SYNTHETIC_ACCOUNT_FINGERPRINT_KEY,
    )
    decision, authorization = gate.authorize_request(
        "SOXL", "BUY", 1, "100.0",
        method="POST", tr_id="TTTT1002U", path=ORDER_PATH,
        body=body or ORDER_BODY,
        account_fingerprint=fingerprint,
    )
    assert decision.can_submit
    return gate, authorization


def test_old_transport_capability_minting_path_is_removed():
    assert not hasattr(KisApiClient, "_issue_order_transport_capability")


def test_wrong_gate_changed_body_and_reused_authorization_are_blocked(tmp_path, monkeypatch):
    client = make_client()
    transport_calls = install_fake_transport(monkeypatch)
    headers = client._get_header("TTTT1002U")
    gate, authorization = issue_wire_authorization(tmp_path)
    other_gate = RuntimeSafetyGate(gate.state_path)

    wrong_gate_response, wrong_gate_result = client._api_request(
        "POST", ORDER_URL, dict(headers), data=ORDER_BODY,
        _order_authorization=authorization, _order_gate=other_gate,
    )
    assert wrong_gate_response is None
    assert wrong_gate_result["safety_decision"]["code"] == "ORDER_AUTHORIZATION_INVALID"

    changed = dict(ORDER_BODY, ORD_QTY="2")
    changed_response, changed_result = client._api_request(
        "POST", ORDER_URL, dict(headers), data=changed,
        _order_authorization=authorization, _order_gate=gate,
    )
    reused_response, reused_result = client._api_request(
        "POST", ORDER_URL, dict(headers), data=ORDER_BODY,
        _order_authorization=authorization, _order_gate=gate,
    )

    assert changed_response is None
    assert changed_result["safety_decision"]["code"] == "ORDER_AUTHORIZATION_REQUEST_MISMATCH"
    assert reused_response is None
    assert reused_result["safety_decision"]["code"] == "ORDER_AUTHORIZATION_INVALID"
    assert transport_calls == []


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


def test_daytime_order_is_request_bound_and_posts_exactly_once(tmp_path, monkeypatch):
    engine = make_live_engine(tmp_path)
    transport_calls = install_fake_transport(monkeypatch)

    result = engine.send_daytime_order("SOXL", "BUY", 1, "100", "LIMIT")

    assert result["rt_cd"] == "0"
    _, headers, body = posted_request(transport_calls)
    assert headers["tr_id"] == "TTTS6036U"
    assert body["PDNO"] == "SOXL"
    assert body["ORD_QTY"] == "1"
    assert body["ORD_DVSN"] == "00"


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


@pytest.mark.parametrize(
    "field,value",
    [
        ("CANO", "99999999"),
        ("ACNT_PRDT_CD", "02"),
        ("PDNO", "TQQQ"),
        ("ORD_QTY", "2"),
        ("OVRS_ORD_UNPR", "101.0"),
        ("ORD_DVSN", "33"),
    ],
)
def test_engine_rejects_metadata_wire_body_mismatch_before_http(
    tmp_path, monkeypatch, field, value
):
    engine = make_live_engine(tmp_path)
    transport_calls = install_fake_transport(monkeypatch)
    body = dict(ORDER_BODY, **{field: value})

    result = engine._call_order_api(
        "SOXL", "BUY", 1, "100.0", "TTTT1002U", ORDER_PATH, body,
        order_type="LIMIT",
    )

    assert transport_calls == []
    assert result["rt_cd"] == "999"
    assert result["odno"] == ""
    assert result["shadow"] is False
    assert result["safety_decision"]["code"] == "WIRE_REQUEST_MISMATCH"


@pytest.mark.parametrize("failure", ["timeout", "connection", 500, 429, "auth"])
def test_order_post_never_retries_after_first_send(tmp_path, monkeypatch, failure):
    client = make_client()
    gate, authorization = issue_wire_authorization(tmp_path)
    calls = []

    class FailureResponse:
        status_code = failure if isinstance(failure, int) else 200

        def json(self):
            if failure == "auth":
                return {"rt_cd": "1", "msg1": "authorization token expired"}
            return {"rt_cd": "0", "msg1": "unexpected success"}

    def fake_post(*args, **kwargs):
        calls.append((args, kwargs))
        if failure == "timeout":
            raise kis_api_client.requests.Timeout("response lost")
        if failure == "connection":
            raise kis_api_client.requests.ConnectionError("connection reset")
        return FailureResponse()

    monkeypatch.setattr(kis_api_client.GlobalThrottle, "wait_api_sync", lambda: None)
    monkeypatch.setattr(kis_api_client.requests, "post", fake_post)

    response, result = client._api_request(
        "POST", ORDER_URL, client._get_header("TTTT1002U"), data=ORDER_BODY,
        _order_authorization=authorization, _order_gate=gate,
    )

    assert len(calls) == 1
    if failure in {"timeout", "connection", 500, 429, "auth"}:
        assert result["safety_decision"]["code"] == "ORDER_SUBMISSION_AMBIGUOUS"
        assert result["reconciliation_required"] is True


@pytest.mark.parametrize(
    "invoke",
    [
        lambda engine: engine.cancel_order("SOXL", "12345"),
        lambda engine: engine.cancel_daytime_order("SOXL", "12345", qty="1", price="100"),
        lambda engine: engine.cancel_reservation_order("20260811", "12345"),
    ],
)
def test_cancel_side_effect_paths_fail_closed_before_http_without_request_authorization(
    tmp_path, monkeypatch, invoke
):
    engine = make_live_engine(tmp_path)
    calls = install_fake_transport(monkeypatch)

    result = invoke(engine)

    assert calls == []
    assert result["rt_cd"] == "999"
    assert result["odno"] == ""
    assert result["shadow"] is False
    assert result["safety_decision"]["code"] == "ORDER_AUTHORIZATION_REQUIRED"


def test_non_order_get_keeps_existing_retry_behavior(monkeypatch):
    client = make_client()
    calls = []

    def fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        if len(calls) == 1:
            raise kis_api_client.requests.Timeout("temporary quote timeout")
        return FakeResponse()

    monkeypatch.setattr(kis_api_client.GlobalThrottle, "wait_api_sync", lambda: None)
    monkeypatch.setattr(kis_api_client.time, "sleep", lambda *_: None)
    monkeypatch.setattr(kis_api_client.requests, "get", fake_get)

    _, result = client._api_request(
        "GET", f"{client.base_url}/uapi/overseas-price/v1/quotations/price",
        client._get_header("HHDFS76200200"), params={"SYMB": "SOXL"},
    )

    assert result["rt_cd"] == "0"
    assert len(calls) == 2


def test_authorized_order_sends_immutable_body_snapshot(tmp_path, monkeypatch):
    client = make_client()
    body = dict(ORDER_BODY)
    gate, authorization = issue_wire_authorization(tmp_path, body=body)
    calls = install_fake_transport(monkeypatch)

    def mutate_after_authorization_check():
        body["ORD_QTY"] = "999"

    monkeypatch.setattr(
        kis_api_client.GlobalThrottle, "wait_api_sync", mutate_after_authorization_check
    )

    _, result = client._api_request(
        "POST", ORDER_URL, client._get_header("TTTT1002U"), data=body,
        _order_authorization=authorization, _order_gate=gate,
    )

    assert result["rt_cd"] == "0"
    assert len(calls) == 1
    assert json.loads(calls[0][1]["data"])["ORD_QTY"] == "1"


def test_string_number_digest_inequality_blocks_mutated_wire_before_http(
    tmp_path, monkeypatch
):
    client = make_client()
    approved_body = dict(ORDER_BODY, OVRS_ORD_UNPR="100.0")
    mutated_body = dict(approved_body, OVRS_ORD_UNPR=100.0)
    gate, authorization = issue_wire_authorization(tmp_path, body=approved_body)
    calls = install_fake_transport(monkeypatch)

    response, result = client._api_request(
        "POST", ORDER_URL, client._get_header("TTTT1002U"), data=mutated_body,
        _order_authorization=authorization, _order_gate=gate,
    )

    assert response is None
    assert calls == []
    assert result["safety_decision"]["code"] == "ORDER_AUTHORIZATION_REQUEST_MISMATCH"


def test_header_mutation_during_throttle_cannot_change_authorized_wire_tr_id(
    tmp_path, monkeypatch
):
    client = make_client()
    gate, authorization = issue_wire_authorization(tmp_path)
    calls = []
    headers = client._get_header("TTTT1002U")
    throttle_entered = threading.Event()
    release_throttle = threading.Event()
    outcome = {}

    def block_in_throttle():
        throttle_entered.set()
        assert release_throttle.wait(timeout=2)

    def fake_post(*args, **kwargs):
        calls.append((args, kwargs))
        return FakeResponse()

    def send_order():
        outcome["response"], outcome["result"] = client._api_request(
            "POST", ORDER_URL, headers, data=ORDER_BODY,
            _order_authorization=authorization, _order_gate=gate,
        )

    monkeypatch.setattr(
        kis_api_client.GlobalThrottle, "wait_api_sync", block_in_throttle
    )
    monkeypatch.setattr(kis_api_client.requests, "post", fake_post)

    worker = threading.Thread(target=send_order)
    worker.start()
    assert throttle_entered.wait(timeout=2)
    headers["tr_id"] = "TTTT1006U"
    release_throttle.set()
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert outcome["result"]["rt_cd"] == "0"
    assert len(calls) == 1
    assert calls[0][1]["headers"]["tr_id"] == "TTTT1002U"


def test_actual_order_io_holds_gate_lock_until_response_is_processed(tmp_path, monkeypatch):
    client = make_client()
    gate, authorization = issue_wire_authorization(tmp_path)
    second_gate = RuntimeSafetyGate(gate.state_path)
    worker_finished = threading.Event()
    worker = None

    def authorize_in_worker():
        fingerprint = __import__("runtime_safety").account_fingerprint(
            SYNTHETIC_CANO, SYNTHETIC_PRODUCT_CODE,
            key=SYNTHETIC_ACCOUNT_FINGERPRINT_KEY,
        )
        second_gate.authorize(
            "SOXL", "BUY", 1, "100", account_fingerprint=fingerprint
        )
        worker_finished.set()

    def fake_post(*args, **kwargs):
        nonlocal worker
        worker = threading.Thread(target=authorize_in_worker)
        worker.start()
        time.sleep(0.05)
        assert not worker_finished.is_set()
        return FakeResponse()

    monkeypatch.setattr(kis_api_client.GlobalThrottle, "wait_api_sync", lambda: None)
    monkeypatch.setattr(kis_api_client.requests, "post", fake_post)

    _, result = client._api_request(
        "POST", ORDER_URL, client._get_header("TTTT1002U"), data=ORDER_BODY,
        _order_authorization=authorization, _order_gate=gate,
    )
    worker.join(timeout=1)

    assert result["rt_cd"] == "0"
    assert worker_finished.is_set()


def test_revision_change_after_authorization_blocks_before_http(tmp_path, monkeypatch):
    client = make_client()
    gate, authorization = issue_wire_authorization(tmp_path)
    write_state(gate.state_path, revision=2)
    calls = install_fake_transport(monkeypatch)

    response, result = client._api_request(
        "POST", ORDER_URL, client._get_header("TTTT1002U"), data=ORDER_BODY,
        _order_authorization=authorization, _order_gate=gate,
    )

    assert response is None
    assert calls == []
    assert result["safety_decision"]["code"] == "ORDER_AUTHORIZATION_REVISION_MISMATCH"


@pytest.mark.parametrize("mutation", ["method", "tr_id", "path"])
def test_intermediate_wire_envelope_mutation_blocks_before_http(
    tmp_path, monkeypatch, mutation
):
    client = make_client()
    gate, authorization = issue_wire_authorization(tmp_path)
    calls = install_fake_transport(monkeypatch)
    monkeypatch.setattr(
        kis_api_client.requests, "get",
        lambda *args, **kwargs: calls.append((args, kwargs)) or FakeResponse(),
    )
    method = "GET" if mutation == "method" else "POST"
    tr_id = "TTTT1006U" if mutation == "tr_id" else "TTTT1002U"
    url = (
        f"{client.base_url}/uapi/overseas-stock/v1/trading/daytime-order"
        if mutation == "path" else ORDER_URL
    )

    response, result = client._api_request(
        method, url, client._get_header(tr_id), data=ORDER_BODY,
        _order_authorization=authorization, _order_gate=gate,
    )

    assert response is None
    assert calls == []
    assert result["safety_decision"]["code"] == "ORDER_AUTHORIZATION_REQUEST_MISMATCH"


@pytest.mark.parametrize(
    "invoke",
    [
        lambda engine: engine.send_order("SOXL", "BUY", 1, "100", "LIMIT"),
        lambda engine: engine.send_daytime_order("SOXL", "SELL", 1, "100"),
        lambda engine: engine.send_reservation_order("SOXL", "BUY", 1, "100", "LIMIT"),
    ],
)
def test_all_order_paths_return_consistent_shape_for_broker_rejection(tmp_path, invoke):
    engine = make_live_engine(tmp_path)
    engine._call_api = lambda *args, **kwargs: {
        "rt_cd": "7", "msg1": "definite broker rejection", "output": {}
    }

    result = invoke(engine)

    assert set(("rt_cd", "msg1", "odno", "shadow", "safety_decision")) <= set(result)
    assert result["rt_cd"] == "7"
    assert result["odno"] == ""
    assert result["shadow"] is False
