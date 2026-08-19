import json
from decimal import Decimal

from config import ConfigManager
from trade_state_store import APPROVED_BASELINE
from strategy_v14 import V4Strategy


BASELINE = dict(APPROVED_BASELINE)
BASELINE_T = float(BASELINE["t"])
BASELINE_CASH = float(BASELINE["available_cash"])
BASELINE_QTY = int(BASELINE["qty"])
BASELINE_AVG = float(BASELINE["avg_price"])
BASELINE_IMPLIED_SEED = round(BASELINE_CASH + (BASELINE_QTY * BASELINE_AVG), 2)


def _isolated_strategy_config(tmp_path, events_path):
    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text(json.dumps(BASELINE), encoding="utf-8")

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
    (tmp_path / "seed.json").write_text(json.dumps({"SOXL": BASELINE_IMPLIED_SEED}), encoding="utf-8")
    (tmp_path / "profit.json").write_text(json.dumps({"SOXL": 12.0}), encoding="utf-8")
    return cfg


def _strategy_plan(cfg):
    strategy = V4Strategy(cfg)
    strategy.load_daily_snapshot = lambda ticker: None
    return _get_strategy_plan(strategy)


def _get_strategy_plan(strategy):
    return strategy.get_plan(
        "SOXL",
        current_price=100.0,
        avg_price=BASELINE_AVG,
        qty=BASELINE_QTY,
        prev_close=99.0,
        available_cash=BASELINE_CASH,
        market_type="REG",
    )


def _cached_order_snapshot():
    cached_order = {"side": "BUY", "price": 1.23, "qty": 7, "type": "LOC", "desc": "CACHED_LEAK"}
    return {
        "orders": [cached_order.copy()],
        "core_orders": [cached_order.copy()],
        "bonus_orders": [{"side": "SELL", "price": 9.87, "qty": 3, "type": "LOC", "desc": "CACHED_LEAK"}],
        "process_status": "CACHED",
        "status": "CACHED",
        "safety": None,
    }


def _assert_t_event_halt_without_cached_orders(plan):
    assert plan["orders"] == []
    assert plan["core_orders"] == []
    assert plan["bonus_orders"] == []
    assert plan["process_status"].startswith("⛔")
    assert "T" in plan["process_status"]
    assert "HALT" in plan["process_status"]
    assert plan.get("safety", {}).get("halted") is True
    assert "ledger" in plan.get("safety", {}).get("reason", "").lower()


def test_strategy_halts_order_generation_when_event_ledger_missing(tmp_path):
    cfg = _isolated_strategy_config(tmp_path, tmp_path / "missing-events.jsonl")

    plan = _strategy_plan(cfg)

    assert plan["orders"] == []
    assert plan["core_orders"] == []
    assert plan["bonus_orders"] == []
    assert plan["process_status"].startswith("⛔")
    assert "T" in plan["process_status"]
    assert plan.get("safety", {}).get("halted") is True
    assert "ledger" in plan.get("safety", {}).get("reason", "").lower()


def test_strategy_halts_order_generation_when_event_ledger_is_corrupt(tmp_path):
    events_path = tmp_path / "events.jsonl"
    events_path.write_text('{"event_id": "broken"}', encoding="utf-8")
    cfg = _isolated_strategy_config(tmp_path, events_path)

    plan = _strategy_plan(cfg)

    assert plan["orders"] == []
    assert plan["core_orders"] == []
    assert plan["bonus_orders"] == []
    assert plan["process_status"].startswith("⛔")
    assert plan.get("safety", {}).get("halted") is True
    assert "ledger" in plan.get("safety", {}).get("reason", "").lower()


def test_strategy_missing_event_ledger_halts_before_cached_snapshot_orders_can_leak(tmp_path):
    cfg = _isolated_strategy_config(tmp_path, tmp_path / "missing-events.jsonl")
    strategy = V4Strategy(cfg)
    strategy.load_daily_snapshot = lambda ticker: _cached_order_snapshot()

    plan = _get_strategy_plan(strategy)

    _assert_t_event_halt_without_cached_orders(plan)


def test_strategy_corrupt_event_ledger_halts_before_cached_snapshot_orders_can_leak(tmp_path):
    events_path = tmp_path / "events.jsonl"
    events_path.write_text('{"event_id": "broken"}', encoding="utf-8")
    cfg = _isolated_strategy_config(tmp_path, events_path)
    strategy = V4Strategy(cfg)
    strategy.load_daily_snapshot = lambda ticker: _cached_order_snapshot()

    plan = _get_strategy_plan(strategy)

    _assert_t_event_halt_without_cached_orders(plan)


def test_strategy_empty_existing_event_ledger_uses_baseline_t_without_overblocking(tmp_path):
    events_path = tmp_path / "events.jsonl"
    events_path.write_text("", encoding="utf-8")
    cfg = _isolated_strategy_config(tmp_path, events_path)

    plan = _strategy_plan(cfg)

    assert plan["t_val"] == BASELINE_T
    assert plan["split_amount"] == float(Decimal(str(BASELINE_IMPLIED_SEED)) / Decimal("20"))
    assert plan.get("safety") is None
    assert plan["orders"]
    assert any(order.get("side") in {"BUY", "SELL"} for order in plan["orders"])


def test_config_absolute_t_reads_baseline_event_ledger_only(tmp_path):
    baseline_path = tmp_path / "baseline.json"
    events_path = tmp_path / "events.jsonl"
    baseline_path.write_text(json.dumps(BASELINE), encoding="utf-8")
    events_path.write_text("", encoding="utf-8")

    cfg = ConfigManager()
    cfg.FILES["STRATEGY_BASELINE"] = str(baseline_path)
    cfg.FILES["T_EVENTS"] = str(events_path)
    cfg.FILES["EXECUTION_LEDGER"] = str(tmp_path / "execution_ledger.jsonl")
    (tmp_path / "execution_ledger.jsonl").write_text(
        json.dumps({
            "source": "KIS_CONFIRMED_FILL",
            "ticker": "SOXL",
            "side": "SELL",
            "qty": 1,
            "price": "100.00",
            "fill_key": "sell-1",
        }) + "\n",
        encoding="utf-8",
    )

    first_t, first_portion = cfg.get_absolute_t_val("SOXL", actual_qty=BASELINE_QTY, actual_avg_price=BASELINE_AVG)
    second_t, second_portion = cfg.get_absolute_t_val("SOXL", actual_qty=999, actual_avg_price=1)

    assert first_t == second_t == BASELINE_T
    expected_portion = Decimal(str(BASELINE_CASH + 100.0)) / (Decimal("20") - Decimal(str(BASELINE_T)))
    assert first_portion == second_portion == float(expected_portion)


def test_get_split_amount_is_fixed_seed_divided_by_twenty(tmp_path):
    events_path = tmp_path / "events.jsonl"
    events_path.write_text("", encoding="utf-8")
    cfg = _isolated_strategy_config(tmp_path, events_path)
    (tmp_path / "seed.json").write_text(json.dumps({"SOXL": 17659.0}), encoding="utf-8")

    assert cfg.get_split_amount("SOXL") == Decimal("882.95")

    (tmp_path / "seed.json").write_text(json.dumps({"SOXL": 16955.4}), encoding="utf-8")
    assert cfg.get_split_amount("SOXL") == Decimal("847.77")


def test_calculate_v14_state_does_not_inverse_cost_basis(tmp_path):
    baseline_path = tmp_path / "baseline.json"
    events_path = tmp_path / "events.jsonl"
    baseline_path.write_text(json.dumps(BASELINE), encoding="utf-8")
    events_path.write_text("", encoding="utf-8")

    cfg = ConfigManager()
    cfg.FILES["STRATEGY_BASELINE"] = str(baseline_path)
    cfg.FILES["T_EVENTS"] = str(events_path)
    cfg.FILES["LEDGER"] = str(tmp_path / "ledger.json")
    (tmp_path / "ledger.json").write_text(json.dumps([
        {"id": 1, "ticker": "SOXL", "side": "BUY", "qty": 999, "price": 1},
    ]), encoding="utf-8")

    t_val, budget, rem_cash = cfg.calculate_v14_state("SOXL")

    assert t_val == BASELINE_T
    expected_budget = Decimal(str(BASELINE_CASH)) / (Decimal("20") - Decimal(str(BASELINE_T)))
    assert budget == float(expected_budget)
    assert rem_cash == BASELINE_CASH


def test_missing_event_ledger_get_absolute_t_val_returns_fail_safe_zero(tmp_path):
    baseline_path = tmp_path / "baseline.json"
    events_path = tmp_path / "events.jsonl"
    baseline_path.write_text(json.dumps(BASELINE), encoding="utf-8")

    cfg = ConfigManager()
    cfg.FILES["STRATEGY_BASELINE"] = str(baseline_path)
    cfg.FILES["T_EVENTS"] = str(events_path)

    t_val, one_portion = cfg.get_absolute_t_val("SOXL", actual_qty=999, actual_avg_price=1)

    assert t_val == 0.0
    assert one_portion == 0.0


def test_missing_event_ledger_calculate_v14_state_returns_fail_safe_zero(tmp_path):
    baseline_path = tmp_path / "baseline.json"
    events_path = tmp_path / "events.jsonl"
    baseline_path.write_text(json.dumps(BASELINE), encoding="utf-8")

    cfg = ConfigManager()
    cfg.FILES["STRATEGY_BASELINE"] = str(baseline_path)
    cfg.FILES["T_EVENTS"] = str(events_path)
    cfg.FILES["LEDGER"] = str(tmp_path / "ledger.json")
    (tmp_path / "ledger.json").write_text(json.dumps([
        {"id": 1, "ticker": "SOXL", "side": "BUY", "qty": 999, "price": 1},
    ]), encoding="utf-8")

    t_val, budget, rem_cash = cfg.calculate_v14_state("SOXL")

    assert (t_val, budget, rem_cash) == (0.0, 0.0, 0.0)
