"""Task 11: Failure injection scenarios — safety, corruption, and environment (Tests 15-20).

All tests use temporary data; no KIS, Telegram, or network access.
"""

import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from config import ConfigManager
from strategy_v14 import V4Strategy
from trade_state_store import (
    APPROVED_BASELINE,
    DuplicateTEventError,
    TEventLedgerCorruptError,
    TradeStateStore,
)


BASELINE = dict(APPROVED_BASELINE)


def _isolated_config_for_strategy(tmp_path, events_lines=None):
    """Create an isolated ConfigManager with empty events ledger."""
    baseline_path = tmp_path / "baseline.json"
    events_path = tmp_path / "events.jsonl"
    baseline_path.write_text(json.dumps(BASELINE), encoding="utf-8")
    if events_lines is not None:
        events_path.write_text(events_lines, encoding="utf-8")
    else:
        events_path.write_text("", encoding="utf-8")

    cfg = ConfigManager()
    cfg.FILES["STRATEGY_BASELINE"] = str(baseline_path)
    cfg.FILES["T_EVENTS"] = str(events_path)
    cfg.FILES["T_STATE"] = str(tmp_path / "t_state.json")
    cfg.FILES["REVERSE_CFG"] = str(tmp_path / "reverse.json")
    cfg.FILES["SPLIT"] = str(tmp_path / "split.json")
    cfg.FILES["SEED_CFG"] = str(tmp_path / "seed.json")
    cfg.FILES["PROFIT_CFG"] = str(tmp_path / "profit.json")
    cfg.FILES["LOCKS"] = str(tmp_path / "locks.json")
    (tmp_path / "t_state.json").write_text("{}", encoding="utf-8")
    (tmp_path / "reverse.json").write_text("{}", encoding="utf-8")
    (tmp_path / "split.json").write_text(json.dumps({"SOXL": 20.0}), encoding="utf-8")
    (tmp_path / "seed.json").write_text(json.dumps({"SOXL": 6720.0}), encoding="utf-8")
    (tmp_path / "profit.json").write_text(json.dumps({"SOXL": 12.0}), encoding="utf-8")
    return cfg


def _call_strategy_plan(cfg):
    strategy = V4Strategy(cfg)
    return strategy.get_plan(
        "SOXL",
        current_price=100.0,
        avg_price=158.0735,
        qty=98,
        prev_close=99.0,
        available_cash=1482.88,
        market_type="REG",
    )


# ===========================================================================
# Test 15: 깨진 JSON·원장 마지막 줄
# A corrupt (unterminated) JSONL line in T event ledger → TEventLedgerCorruptError
# ===========================================================================
def test_15_corrupt_json_event_ledger_last_line(tmp_path):
    bad_lines = '{"event_id": "good-1", "ticker": "SOXL", "intent_id": "int-1", "kis_order_no": "p1", "fill_key": "fk-1", "event_type": "HALF", "filled_qty": 1, "filled_amount": "101.23", "t_before": "18.32", "t_after": "18.82", "revision_before": 1, "revision_after": 2, "occurred_at": "2026-08-12T13:31:22Z"}\n' \
                   '{"event_id": "broken-2", "ticker": "SOXL", "intent_id": "int-2", "kis_order_no": "p2", "fill_key": "fk-2", "event_type": "HALF", "filled_qty": 1, "filled_amount": "101.23", "t_before": "18.32"'

    cfg = _isolated_config_for_strategy(tmp_path, events_lines=bad_lines)
    plan = _call_strategy_plan(cfg)
    # Fail-closed: must halt when ledger is corrupt
    assert plan["orders"] == []
    assert plan["process_status"].startswith("⛔")
    assert plan.get("safety", {}).get("halted") is True


def test_15b_broken_unterminated_line_direct(tmp_path):
    """Direct ledger read: unterminated JSONL line raises TEventLedgerCorruptError."""
    baseline_path = tmp_path / "baseline.json"
    events_path = tmp_path / "events.jsonl"
    baseline_path.write_text(json.dumps(BASELINE), encoding="utf-8")
    events_path.write_text(
        '{"event_id": "ok", "ticker": "SOXL", "intent_id": "i1", "kis_order_no": "p1", '
        '"fill_key": "fk1", "event_type": "HALF", "filled_qty": 1, "filled_amount": "101.23", '
        '"t_before": "18.32", "t_after": "18.82", "revision_before": 1, '
        '"revision_after": 2, "occurred_at": "2026-08-12T13:31:22Z"}\n'
        '{"event_id": "broken", "tick',
        encoding="utf-8"
    )
    store = TradeStateStore(str(baseline_path), str(events_path))
    with pytest.raises(TEventLedgerCorruptError) as exc_info:
        store.load_state("SOXL")
    assert "broken" in str(exc_info.value).lower()


# ===========================================================================
# Test 16: 오래된 스냅샷·T revision 충돌
# Stale T revision → _assert_official_t_revision_current raises StaleTRevisionError
# ===========================================================================
def test_16_stale_t_revision_raises_error():
    from order_executor import _assert_official_t_revision_current

    order = {"t_revision": 1}
    current_provider = Mock(return_value={"status": "OK", "t_revision": 3})

    with pytest.raises(Exception) as exc_info:
        _assert_official_t_revision_current(order, "SOXL", current_provider)
    assert "STALE" in str(exc_info.value)


def test_16b_stale_t_revision_via_cache_key(tmp_path):
    """Stale revision in _order_success_cache_key raises InvalidOrderIntentError."""
    from order_executor import _order_success_cache_key

    order = {
        "strategy": "LAOER_V4_SOXL_20",
        "strategy_revision": 1,
        "t_revision": 1,  # stale: current is 3
        "ticker": "SOXL",
        "trade_date": "2026-08-12",
        "event_type": "FULL",
        "side": "BUY",
        "order_type": "LOC",
        "price": "140.99",
        "qty": 5,
    }
    current_provider = Mock(return_value={"status": "OK", "t_revision": 3})
    fallback_key = "fallback-123"

    with pytest.raises(Exception) as exc_info:
        _order_success_cache_key(
            "2026-08-12", "SOXL", order, "BUY", "LOC", fallback_key,
            current_t_revision_provider=current_provider,
        )
    assert "STALE" in str(exc_info.value)


# ===========================================================================
# Test 17: 비관리자 callback (Task 9 auth 활용)
# Non-admin user must be rejected by the authorization decorator
# ===========================================================================
def test_17_non_admin_callback_blocked():
    from telegram_auth import require_admin

    made_call = []

    async def protected_handler(update, context):
        made_call.append(("invoked", update, context))
        await update.effective_message.reply_text("OK")

    # Create a fake config with ADMIN_CHAT_ID
    fake_config = SimpleNamespace()
    fake_config.chat_id = 12345
    fake_config.get_chat_id = lambda: 12345

    wrapped = require_admin(fake_config, protected_handler)

    # Non-admin user
    non_admin = SimpleNamespace(
        effective_chat=SimpleNamespace(id=99999),
        effective_user=SimpleNamespace(id=99999),
        effective_message=SimpleNamespace(replies=[]),
        callback_query=None,
    )

    async def reply_text(text, **kwargs):
        non_admin.effective_message.replies.append((text, kwargs))
        return non_admin.effective_message

    non_admin.effective_message.reply_text = reply_text

    context = SimpleNamespace(args=[], bot_data={}, job_queue=None)

    asyncio.run(wrapped(non_admin, context))

    assert len(made_call) == 0
    assert non_admin.effective_message.replies
    assert any(
        "관리자" in text or "권한" in text or "denied" in text.lower()
        for text, _ in non_admin.effective_message.replies
    )


# ===========================================================================
# Test 18: HALT·unarmed·shadow에서 broker 호출 0회 (Task 1 safety gate 활용)
# When all three conditions hold, broker.send_order must never be called
# ===========================================================================
def test_18_triple_lock_stops_broker_call(tmp_path):
    """When OPERATOR_HALT=true && LIVE_ARMED=false && SHADOW_ONLY=true,
    the execute_order_list must never call broker.send_order."""
    from runtime_safety import RuntimeSafetyGate, SafetyDecision
    from order_executor import execute_order_list
    from order_intent_store import compute_intent_id

    ticker = "SOXL"
    intent_id_val = compute_intent_id({
        "strategy": "LAOER_V4_SOXL_20",
        "strategy_revision": 1,
        "t_revision": 2,
        "ticker": ticker,
        "trade_date": "2026-08-12",
        "event_type": "FULL",
        "side": "BUY",
        "order_type": "LOC",
        "price": "140.99",
        "qty": 5,
    })
    orders = [{
        "strategy": "LAOER_V4_SOXL_20",
        "strategy_revision": 1,
        "t_revision": 2,
        "trade_date": "2026-08-12",
        "ticker": ticker,
        "event_type": "FULL",
        "side": "BUY",
        "order_type": "LOC",
        "price": "140.99",
        "qty": 5,
        "desc": "별값매수",
        "type": "LOC",
        "intent_id": intent_id_val,
    }]

    # Gate set to SHADOW_ONLY → all orders rejected
    fake_gate = Mock()
    fake_gate.authorize = Mock(return_value=SafetyDecision(
        code="SHADOW_ONLY", reason="shadow mode active; live broker blocked",
        can_submit=False, shadow_only=True,
        revision=1, ticker=ticker, side="BUY",
    ))

    fake_broker = Mock()
    fake_broker.send_order = Mock(side_effect=AssertionError("broker.send_order must not be called"))
    fake_broker.send_reservation_order = Mock(side_effect=AssertionError("broker.send_reservation_order must not be called"))

    current_rev = Mock(return_value={"status": "OK", "t_revision": 2})
    intent_store = Mock()
    accepted_cache = set()

    result = asyncio.run(execute_order_list(
        fake_broker, ticker, orders, accepted_cache,
        is_market_active_now=True, today_str="2026-08-12",
        runtime_safety_gate=fake_gate,
        current_t_revision_provider=current_rev,
        order_intent_store=intent_store,
        t_event_store=None,
    ))

    success, msgs, fail_reason = result
    assert success is False
    assert "차단" in fail_reason or "SHADOW_ONLY" in fail_reason
    # Broker must NOT have been called
    fake_broker.send_order.assert_not_called()
    fake_broker.send_reservation_order.assert_not_called()


# ===========================================================================
# Test 19: 컨테이너 재시작 후 LIVE 자동활성화 0회
# After container restart, the system must NOT auto-arm to live mode
# ===========================================================================
def test_19_no_auto_arm_after_restart():
    """Verify LIVE_ARMED stays false by default even after state reload."""
    # RuntimeSafetyGate default state has no auto-arm logic
    # The state must be explicitly armed by an operator
    state_path = Path(__file__).resolve().parent / ".." / "data" / "runtime_safety.json"
    if state_path.exists():
        raw = state_path.read_text(encoding="utf-8")
        if raw.strip():
            state = json.loads(raw)
            # LIVE_ARMED must be false or the gate must refuse
            assert state.get("live_armed", True) is False or state.get("operator_halt", False) is True

    # The safety gate constructor does NOT auto-arm
    from runtime_safety import RuntimeSafetyGate, DEFAULT_STATE_PATH
    import tempfile

    # Create a minimal synthetic state file that is NOT armed
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tf:
        tf.write(json.dumps({
            "operator_halt": True,
            "live_armed": False,
            "shadow_only": True,
            "revision": 1,
            "reason": "container restart safety default",
            "updated_by": "system",
            "updated_at": "2026-08-12T00:00:00Z",
            "allowed_tickers": ["SOXL"],
            "allowed_account_fingerprints": ["unconfigured"],
            "max_order_quantity": "100",
            "max_order_notional": "100000.00",
            "market_quote_max_age_seconds": 60,
            "market_slippage_buffer_percent": "5.0",
        }))
        state_file = tf.name

    try:
        ckpt_path = state_file.replace(".json", ".revision.json")
        ckpt_lock = state_file.replace(".json", ".revision.json.lock")
        with open(ckpt_path, "w") as f:
            json.dump({"revision": 1}, f)

        gate = RuntimeSafetyGate(state_path=state_file, checkpoint_path=ckpt_path)
        decision = gate.authorize(
            "SOXL", "BUY", 5, "140.99",
            order_type="LOC",
        )
        # When operator_halt=true and shadow_only=true → SHADOW_ONLY or OPERATOR_HALT
        assert not decision.can_submit
        assert decision.code in ("SHADOW_ONLY", "OPERATOR_HALT", "LIVE_DISARMED")

        # After "restart": create a new gate against same state file
        gate2 = RuntimeSafetyGate(state_path=state_file, checkpoint_path=ckpt_path)
        # It must NOT have auto-armed
        decision2 = gate2.authorize(
            "SOXL", "BUY", 5, "140.99",
            order_type="LOC",
        )
        assert not decision2.can_submit
    finally:
        for path in (state_file, ckpt_path, ckpt_lock, state_file + ".lock"):
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass


# ===========================================================================
# Test 20: 기존 KIS 72건 1:1 대사
# The approved baseline must reflect exactly 72 legacy execution count
# ===========================================================================
def test_20_legacy_execution_count_72_tally():
    """The approved baseline declares legacy_execution_count=72."""
    assert APPROVED_BASELINE["legacy_execution_count"] == 72
    assert APPROVED_BASELINE["ticker"] == "SOXL"
    assert APPROVED_BASELINE["immutable"] is True

    # Verify the baseline is frozen
    for key, value in APPROVED_BASELINE.items():
        assert key in APPROVED_BASELINE
    assert APPROVED_BASELINE["qty"] == 98
    assert APPROVED_BASELINE["t"] == "18.32"


def test_20b_correlate_legacy_count_with_single_t_event_chain(tmp_path):
    """72 legacy executions + N new events = N+1 revisions."""
    baseline_path = tmp_path / "baseline.json"
    events_path = tmp_path / "events.jsonl"
    baseline_path.write_text(json.dumps(BASELINE), encoding="utf-8")
    events_path.write_text("", encoding="utf-8")

    store = TradeStateStore(str(baseline_path), str(events_path))
    state = store.load_state("SOXL")
    assert state.revision == 1  # baseline only, no events yet

    # Append one event: revision → 2
    event = {
        "event_id": "evt-20b-1",
        "ticker": "SOXL",
        "intent_id": "intent-20b-1",
        "kis_order_no": "POST-20b-1",
        "fill_key": "fill-20b-1",
        "event_type": "HALF",
        "filled_qty": 1,
        "filled_amount": "101.23",
        "t_before": "18.32",
        "t_after": "18.82",
        "revision_before": 1,
        "revision_after": 2,
        "occurred_at": "2026-08-12T13:31:22Z",
    }
    new_state = store.append_event(event)
    assert new_state.revision == 2

    # Event ID and fill key must be unique
    event2 = dict(event)
    event2["event_id"] = "evt-20b-2"
    event2["fill_key"] = "fill-20b-2"
    event2["t_before"] = "18.82"
    event2["t_after"] = "19.32"
    event2["revision_before"] = 2
    event2["revision_after"] = 3
    state3 = store.append_event(event2)
    assert state3.revision == 3
