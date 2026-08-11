import asyncio
import json
import multiprocessing
import os
from pathlib import Path
import py_compile

import pytest

import runtime_safety
import shadow_intent
from kis_order_engine import KisOrderEngine
from order_executor import execute_order_list
from runtime_safety import RuntimeSafetyGate, SafetyDecision, safety_block_result
from shadow_intent import IdempotencyConflict, ShadowIntentRecorder
from test_runtime_safety import (
    SYNTHETIC_ACCOUNT_FINGERPRINT,
    SYNTHETIC_CANO,
    SYNTHETIC_PRODUCT_CODE,
    write_state,
)


def _authorize_in_process(state_path, checkpoint_path, output):
    decision = RuntimeSafetyGate(state_path, checkpoint_path=checkpoint_path).authorize(
        "SOXL",
        "BUY",
        1,
        "100",
        account_fingerprint=SYNTHETIC_ACCOUNT_FINGERPRINT,
    )
    output.put((decision.revision, decision.code))


def _record_in_process(path, occurrence):
    ShadowIntentRecorder(path).record(
        ticker="SOXL",
        side="BUY",
        quantity=1,
        price="100",
        order_type="LIMIT",
        risk_reference_price="100",
        safety_revision=3,
        idempotency_key=f"occurrence-{occurrence}",
    )


def test_revision_checkpoint_is_monotonic_across_processes(tmp_path):
    state2 = write_state(tmp_path / "state2.json", revision=2)
    state3 = write_state(tmp_path / "state3.json", revision=3)
    checkpoint = tmp_path / "runtime_safety.revision.json"
    checkpoint.write_text('{"revision":1}', encoding="utf-8")
    output = multiprocessing.Queue()

    processes = [
        multiprocessing.Process(target=_authorize_in_process, args=(state2, checkpoint, output)),
        multiprocessing.Process(target=_authorize_in_process, args=(state3, checkpoint, output)),
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(10)
        assert process.exitcode == 0

    decisions = [output.get(timeout=2), output.get(timeout=2)]
    assert json.loads(checkpoint.read_text(encoding="utf-8"))["revision"] == 3
    assert (3, "LIVE_AUTHORIZED") in decisions
    assert dict(decisions)[2] in {"LIVE_AUTHORIZED", "REVISION_ROLLBACK"}

    after = RuntimeSafetyGate(state2, checkpoint_path=checkpoint).authorize(
        "SOXL", "BUY", 1, "100", account_fingerprint=SYNTHETIC_ACCOUNT_FINGERPRINT
    )
    assert after.code == "REVISION_ROLLBACK"


def test_revision_lock_failure_fails_closed(tmp_path, monkeypatch):
    state = write_state(tmp_path / "runtime_safety.json", revision=2)

    def broken_flock(*args, **kwargs):
        raise OSError("lock unavailable")

    monkeypatch.setattr(runtime_safety.fcntl, "flock", broken_flock)
    decision = RuntimeSafetyGate(state).authorize(
        "SOXL", "BUY", 1, "100", account_fingerprint=SYNTHETIC_ACCOUNT_FINGERPRINT
    )

    assert decision.code == "REVISION_LOCK_IO_ERROR"
    assert decision.can_submit is False


def test_shadow_recorder_is_valid_jsonl_after_multiprocess_appends(tmp_path):
    path = tmp_path / "shadow.jsonl"
    processes = [
        multiprocessing.Process(target=_record_in_process, args=(path, index))
        for index in range(24)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(10)
        assert process.exitcode == 0

    raw = path.read_bytes()
    assert raw.endswith(b"\n")
    records = [json.loads(line) for line in raw.splitlines()]
    assert len(records) == 24
    assert len({record["event_id"] for record in records}) == 24
    assert len({record["intent_id"] for record in records}) == 24


def test_shadow_recorder_rolls_back_partial_short_write(tmp_path, monkeypatch):
    path = tmp_path / "shadow.jsonl"
    recorder = ShadowIntentRecorder(path)
    recorder.record(
        ticker="SOXL", side="BUY", quantity=1, price="100", order_type="LIMIT",
        risk_reference_price="100", safety_revision=2, idempotency_key="existing",
    )
    original = path.read_bytes()
    real_write = shadow_intent.os.write
    calls = 0

    def short_then_fail(fd, data):
        nonlocal calls
        calls += 1
        if calls == 1:
            return real_write(fd, data[:7])
        raise OSError("simulated disk failure")

    monkeypatch.setattr(shadow_intent.os, "write", short_then_fail)
    with pytest.raises(OSError):
        recorder.record(
            ticker="SOXL", side="SELL", quantity=2, price="101", order_type="LIMIT",
            risk_reference_price="101", safety_revision=2, idempotency_key="partial",
        )

    assert path.read_bytes() == original
    assert all(json.loads(line) for line in path.read_bytes().splitlines())


def test_shadow_append_fsyncs_file_and_directory(tmp_path, monkeypatch):
    calls = []
    real_fsync = shadow_intent.os.fsync

    def tracked_fsync(fd):
        calls.append(fd)
        return real_fsync(fd)

    monkeypatch.setattr(shadow_intent.os, "fsync", tracked_fsync)
    ShadowIntentRecorder(tmp_path / "shadow.jsonl").record(
        ticker="SOXL", side="BUY", quantity=1, price="100", order_type="LIMIT",
        risk_reference_price="100", safety_revision=2, idempotency_key="fsync",
    )

    assert len(calls) == 2


def test_shadow_recorder_rejects_corrupt_or_unterminated_tail(tmp_path):
    for raw in (b'{"broken":\n', b'{"valid":true}'):
        path = tmp_path / f"shadow-{len(raw)}.jsonl"
        path.write_bytes(raw)
        with pytest.raises(shadow_intent.ShadowJournalCorrupt):
            ShadowIntentRecorder(path).record(
                ticker="SOXL", side="BUY", quantity=1, price="100", order_type="LIMIT",
                risk_reference_price="100", safety_revision=2, idempotency_key="new",
            )
        assert path.read_bytes() == raw


def test_shadow_retry_returns_existing_and_conflict_fails_closed(tmp_path):
    path = tmp_path / "shadow.jsonl"
    recorder = ShadowIntentRecorder(path)
    common = dict(
        ticker="SOXL", side="BUY", quantity=1, price="100", order_type="LIMIT",
        risk_reference_price="100", safety_revision=2, idempotency_key="retry-key",
    )

    first = recorder.record(**common)
    retry = recorder.record(**common)
    assert retry == first
    assert len(path.read_text(encoding="utf-8").splitlines()) == 1

    with pytest.raises(IdempotencyConflict) as error:
        recorder.record(**{**common, "quantity": 2})
    assert error.value.code == "IDEMPOTENCY_CONFLICT"
    assert len(path.read_text(encoding="utf-8").splitlines()) == 1


def test_shadow_identity_separates_occurrence_and_includes_risk_reference(tmp_path):
    recorder = ShadowIntentRecorder(tmp_path / "shadow.jsonl")
    base = dict(
        ticker="SOXL", side="SELL", quantity=1, price="0", order_type="MOC",
        safety_revision=7,
    )
    first = recorder.record(**base, risk_reference_price="100", idempotency_key="occ-1")
    second = recorder.record(**base, risk_reference_price="100", idempotency_key="occ-2")
    changed_risk = recorder.record(**base, risk_reference_price="101", idempotency_key="occ-3")

    assert first["schema_version"] == 2
    assert first["idempotency_key"] == "occ-1"
    assert first["risk_reference_price"] == "100"
    assert len({first["event_id"], second["event_id"], changed_risk["event_id"]}) == 3
    assert len({first["intent_id"], second["intent_id"], changed_risk["intent_id"]}) == 3


def test_shadow_failure_result_contract_uses_decision_shadow_semantics():
    decision = RuntimeSafetyGate.denied(
        "SHADOW_INTENT_RECORD_FAILED", "failed", shadow_only=True
    )
    result = safety_block_result(decision)

    assert set(("rt_cd", "msg1", "odno", "shadow", "safety_decision")) <= set(result)
    assert result["shadow"] is True
    assert result["odno"] == ""


def _make_shadow_engine(tmp_path):
    state = write_state(tmp_path / "runtime_safety.json", shadow_only=True)
    engine = object.__new__(KisOrderEngine)
    engine.runtime_safety_gate = RuntimeSafetyGate(state)
    engine.shadow_intent_recorder = ShadowIntentRecorder(tmp_path / "kis-shadow.jsonl")
    engine.cano = SYNTHETIC_CANO
    engine.acnt_prdt_cd = SYNTHETIC_PRODUCT_CODE
    engine._safe_float = lambda value: float(value)
    engine._ceil_2 = lambda value: float(value)
    engine._get_exchange_code = lambda *args, **kwargs: "NASD"
    engine._excg_cd_cache = {}
    engine._call_api = lambda *args, **kwargs: pytest.fail("KIS API must not be called")
    return engine


def test_direct_kis_explicit_idempotency_key_retries_once_and_conflicts(tmp_path):
    engine = _make_shadow_engine(tmp_path)

    first = engine.send_order("SOXL", "BUY", 1, "100", "LIMIT", idempotency_key="caller-1")
    retry = engine.send_order("SOXL", "BUY", 1, "100", "LIMIT", idempotency_key="caller-1")
    conflict = engine.send_order("SOXL", "BUY", 2, "100", "LIMIT", idempotency_key="caller-1")

    records = [json.loads(line) for line in (tmp_path / "kis-shadow.jsonl").read_text().splitlines()]
    assert len(records) == 1
    assert first["safety_decision"]["code"] == retry["safety_decision"]["code"] == "SHADOW_ONLY"
    assert conflict["safety_decision"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert conflict["shadow"] is True


def test_direct_kis_without_caller_key_creates_unique_occurrences(tmp_path):
    engine = _make_shadow_engine(tmp_path)

    engine.send_order("SOXL", "BUY", 1, "100", "LIMIT")
    engine.send_order("SOXL", "BUY", 1, "100", "LIMIT")

    records = [json.loads(line) for line in (tmp_path / "kis-shadow.jsonl").read_text().splitlines()]
    assert len(records) == 2
    assert records[0]["event_id"] != records[1]["event_id"]
    assert records[0]["intent_id"] != records[1]["intent_id"]


def test_executor_builds_stable_occurrence_key_and_forwards_it_to_broker():
    class LiveGate:
        def authorize(self, ticker, side, quantity, price, **kwargs):
            return SafetyDecision(
                "LIVE_AUTHORIZED", "ok", True, shadow_only=False, revision=9,
                ticker=ticker, side=side,
            )

    class Broker:
        cano = SYNTHETIC_CANO
        acnt_prdt_cd = SYNTHETIC_PRODUCT_CODE

        def __init__(self):
            self.keys = []

        def send_order(self, *args, idempotency_key=None, **kwargs):
            self.keys.append(idempotency_key)
            return {"rt_cd": "0", "msg1": "OK", "odno": "1"}

    order = {"side": "BUY", "qty": 1, "price": "100", "type": "LIMIT", "desc": "first"}
    broker = Broker()
    for _ in range(2):
        success, _, _ = asyncio.run(
            execute_order_list(
                broker, "SOXL", [order], set(), True, "20260811",
                runtime_safety_gate=LiveGate(),
            )
        )
        assert success is True

    assert broker.keys[0]
    assert broker.keys[0] == broker.keys[1]


@pytest.mark.parametrize(
    "module_name",
    ["runtime_safety.py", "shadow_intent.py", "order_executor.py", "kis_order_engine.py"],
)
def test_changed_modules_pycompile(tmp_path, module_name):
    root = Path(__file__).resolve().parents[1]
    py_compile.compile(
        str(root / module_name),
        cfile=str(tmp_path / f"{module_name}c"),
        doraise=True,
    )
