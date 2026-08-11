import hashlib
import json
from decimal import Decimal

from config import ConfigManager
from laoer_v4_20 import NormalState, ReverseState, calculate_normal_plan, calculate_reverse_plan
from order_intent_store import STRATEGY, compute_intent_id
from strategy import InfiniteStrategy
from strategy_v14 import V4Strategy


BASELINE = {
    "schema_version": 1,
    "ticker": "SOXL",
    "as_of": "2026-08-11",
    "qty": 98,
    "avg_price": "158.0735",
    "available_cash": "1482.88",
    "t": "18.32",
    "reverse_active": False,
    "source": "CEO_APPROVED_KIS_BASELINE",
    "legacy_execution_count": 72,
    "immutable": True,
}
TRADE_DATE = "2026-08-12"
STRATEGY_REVISION = 1


def D(value):
    return Decimal(str(value))


def isolated_cfg(tmp_path, *, reverse_state=None, version=None, split=20.0):
    baseline_path = tmp_path / "baseline.json"
    events_path = tmp_path / "events.jsonl"
    baseline_path.write_text(json.dumps(BASELINE), encoding="utf-8")
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
    cfg.FILES["VERSION_CFG"] = str(tmp_path / "version.json")
    (tmp_path / "t_state.json").write_text("{}", encoding="utf-8")
    (tmp_path / "reverse.json").write_text(json.dumps({"SOXL": reverse_state} if reverse_state else {}), encoding="utf-8")
    (tmp_path / "split.json").write_text(json.dumps({"SOXL": split}), encoding="utf-8")
    (tmp_path / "seed.json").write_text(json.dumps({"SOXL": 999999.0}), encoding="utf-8")
    (tmp_path / "profit.json").write_text(json.dumps({"SOXL": 15.0}), encoding="utf-8")
    (tmp_path / "locks.json").write_text("{}", encoding="utf-8")
    (tmp_path / "version.json").write_text(json.dumps(version or {"SOXL": STRATEGY}), encoding="utf-8")
    return cfg


def make_strategy(cfg):
    strategy = V4Strategy(cfg)
    strategy._get_logical_date_str = lambda: TRADE_DATE
    return strategy


def official_normal_fixture():
    return calculate_normal_plan(
        NormalState(
            ticker="SOXL",
            split=20,
            quantity=98,
            avg_price=D("158.0735"),
            cash=D("1482.88"),
            t=D("18.32"),
            reverse=False,
        )
    )


def official_reverse_fixture(day=2):
    return calculate_reverse_plan(
        ReverseState(
            ticker="SOXL",
            split=20,
            quantity=89,
            previous_quantity=98,
            avg_price=D("158.0735"),
            cash=D("1482.88"),
            t=D("17.109"),
            day=day,
            previous_closes=["130", "132", "134", "136", "138"],
        )
    )


def assert_official_order_identity(order, *, event_type, side, order_type, price, qty, t_revision=1):
    assert order["strategy"] == STRATEGY
    assert order["strategy_revision"] == STRATEGY_REVISION
    assert order["t_revision"] == t_revision
    assert order["trade_date"] == TRADE_DATE
    assert order["ticker"] == "SOXL"
    assert order["event_type"] == event_type
    assert order["side"] == side
    assert order["order_type"] == order_type
    assert order["type"] == order_type
    assert str(order["price"]) == str(price)
    assert order["qty"] == qty
    assert order["intent_id"] == compute_intent_id(order)


def test_adapter_returns_approved_baseline_normal_plan_exactly_from_official_kernel(tmp_path):
    cfg = isolated_cfg(tmp_path)
    strategy = make_strategy(cfg)
    strategy.load_daily_snapshot = lambda ticker: None
    expected = official_normal_fixture()

    plan = strategy.get_plan(
        "SOXL",
        current_price=100.0,
        avg_price=158.0735,
        qty=98,
        prev_close=99.0,
        available_cash=1482.88,
        market_type="REG",
    )

    assert plan["strategy"] == STRATEGY
    assert plan["strategy_revision"] == STRATEGY_REVISION
    assert plan["t_revision"] == 1
    assert plan["intent_ids"] == [order["intent_id"] for order in plan["orders"]]
    assert plan["source_balance_at"] == BASELINE["as_of"]
    assert plan["t_val"] == float(expected.t)
    assert plan["one_portion"] == float(expected.one_buy_budget)
    assert plan["star_ratio"] == float(expected.star_percent / D("100"))
    assert plan["star_price"] == float(expected.star_point)
    assert plan["target_price"] == float(expected.target_sell_price)
    assert plan["bonus_orders"] == []
    assert len(plan["orders"]) == 3
    assert_official_order_identity(
        plan["orders"][0],
        event_type="FULL",
        side="BUY",
        order_type="LOC",
        price="131.76",
        qty=expected.star_buy_quantity,
    )
    assert_official_order_identity(
        plan["orders"][1],
        event_type="QUARTER",
        side="SELL",
        order_type="LOC",
        price="131.77",
        qty=expected.quarter_sell_quantity,
    )
    assert_official_order_identity(
        plan["orders"][2],
        event_type="TARGET_FULL",
        side="SELL",
        order_type="LIMIT",
        price="189.69",
        qty=expected.target_sell_quantity,
    )


def test_adapter_delegates_to_public_kernel_and_does_not_emit_bonus_or_mutate_snapshot(tmp_path, monkeypatch):
    cfg = isolated_cfg(tmp_path)
    strategy = make_strategy(cfg)
    saved_snapshots = []
    strategy.load_daily_snapshot = lambda ticker: None
    strategy.save_daily_snapshot = lambda ticker, data: saved_snapshots.append(json.loads(json.dumps(data)))

    calls = []
    import laoer_v4_20

    real_calculate = laoer_v4_20.calculate_normal_plan

    def recording_calculate(state):
        calls.append(state)
        return real_calculate(state)

    monkeypatch.setattr(laoer_v4_20, "calculate_normal_plan", recording_calculate)

    plan = strategy.get_plan(
        "SOXL",
        current_price=100.0,
        avg_price=158.0735,
        qty=98,
        prev_close=1.0,
        available_cash=999999.0,
        market_type="REG",
        is_snapshot_mode=True,
    )

    assert calls and calls[0].ticker == "SOXL" and calls[0].split == 20 and calls[0].t == D("18.32")
    assert plan["bonus_orders"] == []
    assert saved_snapshots and saved_snapshots[0]["orders"] == plan["orders"]
    assert all("bonus" not in order.get("desc", "").lower() and "줍줍" not in order.get("desc", "") for order in plan["orders"])


def test_stale_snapshot_revision_is_not_reused_for_orders(tmp_path):
    cfg = isolated_cfg(tmp_path)
    strategy = make_strategy(cfg)
    stale_order = {"side": "BUY", "price": 1.23, "qty": 7, "type": "LOC", "desc": "STALE_SNAPSHOT_LEAK"}
    strategy.load_daily_snapshot = lambda ticker: {
        "date": TRADE_DATE,
        "strategy_revision": 999,
        "t_revision": 999,
        "intent_ids": ["stale"],
        "orders": [stale_order],
        "core_orders": [stale_order],
        "bonus_orders": [],
    }

    plan = strategy.get_plan(
        "SOXL",
        current_price=100.0,
        avg_price=158.0735,
        qty=98,
        prev_close=99.0,
        available_cash=1482.88,
        market_type="REG",
    )

    assert plan["orders"]
    assert all(order.get("desc") != "STALE_SNAPSHOT_LEAK" for order in plan["orders"])
    assert plan["strategy_revision"] == STRATEGY_REVISION
    assert plan["t_revision"] == 1


def test_version_config_only_routes_official_soxl20_and_forbids_mixed_v4_labels(tmp_path):
    cfg = isolated_cfg(tmp_path, version={"SOXL": "V4", "TQQQ": "V4.0"})
    strategy = InfiniteStrategy(cfg)
    strategy.v4._get_logical_date_str = lambda: TRADE_DATE
    strategy.v4.load_daily_snapshot = lambda ticker: None

    soxl_plan = strategy.get_plan("SOXL", 100.0, 158.0735, 98, 99.0, available_cash=1482.88)
    tqqq_plan = strategy.get_plan("TQQQ", 100.0, 100.0, 10, 99.0, available_cash=1000.0)

    assert cfg.get_version("SOXL") == STRATEGY
    assert cfg.get_version("TQQQ") == STRATEGY
    assert soxl_plan["strategy"] == STRATEGY
    assert tqqq_plan["orders"] == []
    assert tqqq_plan["core_orders"] == []
    assert tqqq_plan["bonus_orders"] == []
    assert tqqq_plan["safety"]["halted"] is True
    assert "SOXL" in tqqq_plan["safety"]["reason"]


def test_non_soxl_returns_zero_orders_and_halt_reason(tmp_path):
    cfg = isolated_cfg(tmp_path)
    strategy = make_strategy(cfg)

    plan = strategy.get_plan("TQQQ", 100.0, 100.0, 10, 99.0, available_cash=1000.0)

    assert plan["orders"] == []
    assert plan["core_orders"] == []
    assert plan["bonus_orders"] == []
    assert plan["safety"]["halted"] is True
    assert "SOXL" in plan["safety"]["reason"]


def test_reverse_plan_path_uses_official_kernel_event_and_order_types(tmp_path):
    reverse_state = {
        "is_active": True,
        "day_count": 2,
        "exit_target": 0.0,
        "last_update_date": "2026-08-11",
        "last_t_update_date": "2026-08-11",
        "dynamic_t": 17.109,
        "rem_cash": 1482.88,
        "is_day_one": False,
    }
    cfg = isolated_cfg(tmp_path, reverse_state=reverse_state)
    strategy = make_strategy(cfg)
    strategy.load_daily_snapshot = lambda ticker: None
    expected = official_reverse_fixture(day=2)

    plan = strategy.get_plan(
        "SOXL",
        current_price=100.0,
        avg_price=158.0735,
        qty=89,
        prev_close=138.0,
        ma_5day=134.0,
        available_cash=1482.88,
        market_type="REG",
    )

    assert plan["is_reverse"] is True
    assert plan["t_val"] == float(expected.t)
    assert plan["star_price"] == float(expected.star_point)
    assert plan["one_portion"] == float(expected.buy_budget)
    assert len(plan["orders"]) == 2
    assert_official_order_identity(
        plan["orders"][0],
        event_type="FULL",
        side="BUY",
        order_type="LOC",
        price="133.99",
        qty=expected.buy_quantity,
    )
    assert_official_order_identity(
        plan["orders"][1],
        event_type="QUARTER",
        side="SELL",
        order_type="LOC",
        price="134.00",
        qty=expected.sell_quantity,
    )
    assert [order["intent_id"] for order in plan["orders"]] == plan["intent_ids"]


def test_repository_version_config_contains_only_official_strategy_label():
    with open("data/version_config.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    assert set(data.values()) == {STRATEGY}
    assert "V4" not in data.values()
    assert "V14" not in data.values()
    assert "V4.0" not in data.values()
