from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import fcntl
import json
import multiprocessing
import os
from pathlib import Path
import threading

import pytest

import runtime_safety
from runtime_safety import (
    AuthorizationError,
    OrderAuthorization,
    RuntimeSafetyGate,
    canonical_request_digest,
    serialize_request_body_bytes,
)
from test_runtime_safety import (
    SYNTHETIC_ACCOUNT_FINGERPRINT,
    SYNTHETIC_ACCOUNT_FINGERPRINT_KEY,
    write_state,
)


ORDER_BODY = {
    "CANO": "00000000",
    "ACNT_PRDT_CD": "01",
    "OVRS_EXCG_CD": "NASD",
    "PDNO": "SOXL",
    "ORD_QTY": "1",
    "OVRS_ORD_UNPR": "100.00",
    "ORD_DVSN": "00",
}
ORDER_REQUEST = {
    "method": "POST",
    "tr_id": "TTTT1002U",
    "path": "/uapi/overseas-stock/v1/trading/order",
    "body": ORDER_BODY,
}
POLICY_REQUEST = {
    "ticker": "SOXL",
    "side": "BUY",
    "quantity": 1,
    "price": "100.00",
    "account_fingerprint": SYNTHETIC_ACCOUNT_FINGERPRINT,
}


def issue(gate):
    return gate.authorize_request(**POLICY_REQUEST, **ORDER_REQUEST)


def try_checkpoint_lock(lock_path, connection):
    fd = os.open(lock_path, os.O_RDONLY)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            connection.send("blocked")
        else:
            connection.send("acquired")
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)
        connection.close()


def try_consume_in_fork(gate, authorization, connection):
    try:
        with gate.authorized_post(authorization, **ORDER_REQUEST):
            connection.send("accepted")
    except AuthorizationError as exc:
        connection.send(exc.code)
    finally:
        connection.close()


def test_canonical_request_digest_binds_method_tr_id_path_and_canonical_json():
    reordered = dict(reversed(list(ORDER_BODY.items())))
    decimal_body = {**ORDER_BODY, "OVRS_ORD_UNPR": Decimal("100.00")}

    digest = canonical_request_digest(**ORDER_REQUEST)

    assert digest == canonical_request_digest(
        method="post",
        tr_id="TTTT1002U",
        path=ORDER_REQUEST["path"],
        body=reordered,
    )
    assert len(digest) == 64
    assert ORDER_BODY["CANO"] not in digest
    assert digest != canonical_request_digest(
        **{**ORDER_REQUEST, "body": {**ORDER_BODY, "ORD_QTY": "2"}}
    )
    with pytest.raises(ValueError):
        canonical_request_digest(**{**ORDER_REQUEST, "body": decimal_body})


def test_strict_body_serializer_accepts_only_json_types_and_preserves_number_type():
    string_body = {**ORDER_BODY, "OVRS_ORD_UNPR": "100.0"}
    number_body = {**ORDER_BODY, "OVRS_ORD_UNPR": 100.0}

    with pytest.raises(ValueError):
        serialize_request_body_bytes(
            {**ORDER_BODY, "OVRS_ORD_UNPR": Decimal("100.00")}
        )
    with pytest.raises(ValueError):
        serialize_request_body_bytes({**ORDER_BODY, "extra": ("not", "json")})

    assert serialize_request_body_bytes(string_body) != serialize_request_body_bytes(
        number_body
    )
    assert canonical_request_digest(
        **{**ORDER_REQUEST, "body": string_body}
    ) != canonical_request_digest(**{**ORDER_REQUEST, "body": number_body})


def test_gate_mints_frozen_request_bound_authorization_only_for_live_decision(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json")
    gate = RuntimeSafetyGate(state_path)

    decision, authorization = issue(gate)

    assert decision.code == "LIVE_AUTHORIZED"
    assert isinstance(authorization, OrderAuthorization)
    assert authorization.revision == decision.revision
    assert authorization.request_digest == canonical_request_digest(**ORDER_REQUEST)
    assert authorization.nonce and authorization.seal
    assert not hasattr(authorization, "gate_id")
    with pytest.raises(FrozenInstanceError):
        authorization.revision = 2

    write_state(state_path, operator_halt=True, revision=2)
    denied, missing = issue(gate)
    assert denied.code == "OPERATOR_HALT"
    assert missing is None
    assert not hasattr(gate, "issue_authorization")
    assert not hasattr(gate, "_mint_authorization")


def test_manual_other_gate_tampered_wrong_digest_expired_and_reused_fail_closed(tmp_path):
    now = datetime(2026, 8, 11, tzinfo=timezone.utc)
    state_path = write_state(tmp_path / "runtime_safety.json")
    gate = RuntimeSafetyGate(state_path, clock=lambda: now)
    other_gate = RuntimeSafetyGate(state_path, clock=lambda: now)
    _, authorization = issue(gate)

    manual = OrderAuthorization(
        nonce="manual",
        revision=1,
        request_digest=authorization.request_digest,
        expires_at=authorization.expires_at,
        seal="0" * 64,
    )
    with pytest.raises(AuthorizationError) as manual_error:
        with gate.authorized_post(manual, **ORDER_REQUEST):
            pass
    assert manual_error.value.code == "ORDER_AUTHORIZATION_INVALID"

    with pytest.raises(AuthorizationError) as other_error:
        with other_gate.authorized_post(authorization, **ORDER_REQUEST):
            pass
    assert other_error.value.code == "ORDER_AUTHORIZATION_INVALID"

    with pytest.raises(AuthorizationError) as tamper_error:
        with gate.authorized_post(replace(authorization, revision=2), **ORDER_REQUEST):
            pass
    assert tamper_error.value.code == "ORDER_AUTHORIZATION_INVALID"

    with pytest.raises(AuthorizationError) as digest_error:
        with gate.authorized_post(
            authorization,
            **{**ORDER_REQUEST, "body": {**ORDER_BODY, "ORD_QTY": "2"}},
        ):
            pass
    assert digest_error.value.code == "ORDER_AUTHORIZATION_REQUEST_MISMATCH"

    _, expiring = issue(gate)
    now += timedelta(seconds=6)
    with pytest.raises(AuthorizationError) as expiry_error:
        with gate.authorized_post(expiring, **ORDER_REQUEST):
            pass
    assert expiry_error.value.code == "ORDER_AUTHORIZATION_EXPIRED"

    now = datetime(2026, 8, 11, tzinfo=timezone.utc)
    _, fresh = issue(gate)
    with gate.authorized_post(fresh, **ORDER_REQUEST):
        pass
    with pytest.raises(AuthorizationError) as reused_error:
        with gate.authorized_post(fresh, **ORDER_REQUEST):
            pass
    assert reused_error.value.code == "ORDER_AUTHORIZATION_INVALID"


def test_malformed_authorization_fields_fail_closed_with_structured_error(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json")
    gate = RuntimeSafetyGate(state_path)

    for field, value in (
        ("nonce", []),
        ("revision", "1"),
        ("request_digest", "not-a-digest"),
        ("expires_at", "2026-08-11T00:00:00Z"),
        ("seal", None),
    ):
        _, authorization = issue(gate)
        malformed = replace(authorization, **{field: value})
        with pytest.raises(AuthorizationError) as error:
            with gate.authorized_post(malformed, **ORDER_REQUEST):
                pass
        assert error.value.code == "ORDER_AUTHORIZATION_INVALID"


def test_authorized_post_revalidates_halt_and_exact_revision_before_yield(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json")
    gate = RuntimeSafetyGate(state_path)
    _, halted = issue(gate)
    write_state(state_path, operator_halt=True, revision=2)

    yielded = False
    with pytest.raises(AuthorizationError) as halt_error:
        with gate.authorized_post(halted, **ORDER_REQUEST):
            yielded = True
    assert yielded is False
    assert halt_error.value.code == "OPERATOR_HALT"

    write_state(state_path, revision=3)
    _, changed = issue(gate)
    write_state(state_path, revision=4)
    with pytest.raises(AuthorizationError) as revision_error:
        with gate.authorized_post(changed, **ORDER_REQUEST):
            pass
    assert revision_error.value.code == "ORDER_AUTHORIZATION_REVISION_MISMATCH"


def test_authorized_post_holds_checkpoint_flock_through_actual_io_context(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json")
    gate = RuntimeSafetyGate(state_path)
    _, authorization = issue(gate)

    with gate.authorized_post(authorization, **ORDER_REQUEST):
        parent, child = multiprocessing.Pipe(duplex=False)
        process = multiprocessing.Process(
            target=try_checkpoint_lock,
            args=(str(gate.checkpoint_lock_path), child),
        )
        process.start()
        assert parent.recv() == "blocked"
        process.join(5)
        assert process.exitcode == 0

        observed = []
        done = threading.Event()

        def thread_probe():
            fd = os.open(gate.checkpoint_lock_path, os.O_RDONLY)
            try:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    observed.append("blocked")
                else:
                    observed.append("acquired")
                    fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)
                done.set()

        thread = threading.Thread(target=thread_probe)
        thread.start()
        assert done.wait(5)
        thread.join(5)
        assert observed == ["blocked"]

    fd = os.open(gate.checkpoint_lock_path, os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def test_authorization_is_process_local_even_after_fork(tmp_path):
    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("fork-specific process-local capability probe")
    state_path = write_state(tmp_path / "runtime_safety.json")
    gate = RuntimeSafetyGate(state_path)
    _, authorization = issue(gate)
    context = multiprocessing.get_context("fork")
    parent, child = context.Pipe(duplex=False)
    process = context.Process(
        target=try_consume_in_fork,
        args=(gate, authorization, child),
    )

    process.start()
    assert parent.recv() == "ORDER_AUTHORIZATION_INVALID"
    process.join(5)
    assert process.exitcode == 0

    with gate.authorized_post(authorization, **ORDER_REQUEST):
        pass


def test_load_state_fstats_and_reads_the_same_open_inode_during_atomic_replace(
    tmp_path, monkeypatch
):
    state_path = write_state(tmp_path / "runtime_safety.json", revision=1)
    replacement = tmp_path / "replacement.json"
    replacement_state = {
        "operator_halt": True,
        "live_armed": False,
        "shadow_only": True,
        "reason": "UNCONFIGURED",
        "revision": 2,
        "updated_at": "2026-08-11T00:00:00Z",
        "updated_by": "PYTEST",
        "allowed_tickers": ["SOXL"],
        "allowed_account_fingerprints": ["UNCONFIGURED"],
        "max_order_quantity": 1,
        "max_order_notional": "1.00",
        "market_quote_max_age_seconds": 120,
        "market_slippage_buffer_percent": "5.00",
    }
    replacement.write_text(json.dumps(replacement_state), encoding="utf-8")
    replacement.chmod(0o644)
    real_open = runtime_safety.os.open
    replaced = False

    def racing_open(path, flags, *args, **kwargs):
        nonlocal replaced
        fd = real_open(path, flags, *args, **kwargs)
        if Path(path) == state_path and not replaced:
            replaced = True
            os.replace(replacement, state_path)
        return fd

    monkeypatch.setattr(runtime_safety.os, "open", racing_open)

    state, error = RuntimeSafetyGate(state_path)._load_state()

    assert error is None
    assert state["revision"] == 1
    assert state_path.stat().st_mode & 0o777 == 0o644


def test_authorize_request_refuses_non_post_methods(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json")
    decision, authorization = RuntimeSafetyGate(state_path).authorize_request(
        **POLICY_REQUEST, **{**ORDER_REQUEST, "method": "GET"}
    )

    assert decision.code == "ORDER_REQUEST_METHOD_INVALID"
    assert authorization is None


def test_gate_parses_wire_body_as_authority_and_rejects_metadata_mismatch(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json")
    body = {
        **ORDER_BODY,
        "CANO": "99999999",
        "PDNO": "TQQQ",
        "ORD_QTY": "99",
        "OVRS_ORD_UNPR": "0",
        "ORD_DVSN": "33",
    }

    decision, authorization = RuntimeSafetyGate(state_path).authorize_request(
        "SOXL",
        "BUY",
        1,
        "100.00",
        method="POST",
        tr_id="TTTT1002U",
        path="/uapi/overseas-stock/v1/trading/order",
        body=body,
        account_fingerprint=SYNTHETIC_ACCOUNT_FINGERPRINT,
        account_fingerprint_key=SYNTHETIC_ACCOUNT_FINGERPRINT_KEY,
        order_type="LIMIT",
    )

    assert decision.code == "WIRE_REQUEST_MISMATCH"
    assert authorization is None


def test_gate_rejects_order_wire_schema_non_string_relevant_values(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json")
    body = {**ORDER_BODY, "ORD_QTY": 1, "OVRS_ORD_UNPR": 100.0}

    decision, authorization = RuntimeSafetyGate(state_path).authorize_request(
        **POLICY_REQUEST,
        method="POST",
        tr_id="TTTT1002U",
        path="/uapi/overseas-stock/v1/trading/order",
        body=body,
        account_fingerprint_key=SYNTHETIC_ACCOUNT_FINGERPRINT_KEY,
    )

    assert decision.code == "ORDER_REQUEST_INVALID"
    assert authorization is None


@pytest.mark.parametrize(
    "overrides,mode,expected_code",
    [
        ({"live_armed": False}, None, "LIVE_NOT_ARMED"),
        ({"shadow_only": True}, None, "SHADOW_ONLY"),
        ({"allowed_tickers": ["TQQQ"]}, None, "TICKER_NOT_ALLOWED"),
        ({"allowed_account_fingerprints": ["0" * 64]}, None, "ACCOUNT_NOT_ALLOWED"),
        ({"operator_halt": "true"}, None, "SAFETY_STATE_INVALID_SCHEMA"),
        ({}, 0o644, "SAFETY_STATE_INSECURE_PERMISSIONS"),
    ],
)
def test_authorized_post_revalidates_all_mutable_live_state_guards(
    tmp_path, overrides, mode, expected_code
):
    state_path = write_state(tmp_path / "runtime_safety.json")
    gate = RuntimeSafetyGate(state_path)
    _, authorization = issue(gate)
    write_state(state_path, **overrides)
    if mode is not None:
        state_path.chmod(mode)

    with pytest.raises(AuthorizationError) as error:
        with gate.authorized_post(authorization, **ORDER_REQUEST):
            pass

    assert error.value.code == expected_code


def test_authorized_post_revalidates_revision_rollback(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json", revision=2)
    gate = RuntimeSafetyGate(state_path)
    _, authorization = issue(gate)
    write_state(state_path, revision=1)

    with pytest.raises(AuthorizationError) as error:
        with gate.authorized_post(authorization, **ORDER_REQUEST):
            pass

    assert error.value.code == "REVISION_ROLLBACK"


def test_concurrent_consumers_accept_exactly_one_use(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json")
    gate = RuntimeSafetyGate(state_path)
    _, authorization = issue(gate)
    barrier = threading.Barrier(3)
    results = []

    def consume():
        barrier.wait()
        try:
            with gate.authorized_post(authorization, **ORDER_REQUEST):
                results.append("accepted")
        except AuthorizationError as exc:
            results.append(exc.code)

    threads = [threading.Thread(target=consume) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(5)

    assert sorted(results) == ["ORDER_AUTHORIZATION_INVALID", "accepted"]


def test_configured_or_live_state_still_requires_0600_but_safe_default_allows_0644(tmp_path):
    state_path = write_state(tmp_path / "runtime_safety.json")
    state_path.chmod(0o644)
    _, configured_error = RuntimeSafetyGate(state_path)._load_state()
    assert configured_error.code == "SAFETY_STATE_INSECURE_PERMISSIONS"

    write_state(
        state_path,
        operator_halt=True,
        live_armed=False,
        shadow_only=True,
        allowed_account_fingerprints=["UNCONFIGURED"],
    )
    state_path.chmod(0o644)
    state, default_error = RuntimeSafetyGate(state_path)._load_state()
    assert default_error is None
    assert state["operator_halt"] is True
