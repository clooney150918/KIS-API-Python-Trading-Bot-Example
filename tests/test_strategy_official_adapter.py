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
            target_profit_percent=D("15.0"),
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
    assert len(plan["orders"]) == 8
    assert_official_order_identity(
        plan["orders"][0],
        event_type="FULL",
        side="BUY",
        order_type="LOC",
        price="131.76",
        qty=expected.star_buy_quantity,
    )
    expected_bonus_prices = [
        f"{(expected.one_buy_budget / D(expected.star_buy_quantity + k)):.2f}"
        for k in range(1, 6)
    ]
    assert [order["price"] for order in plan["orders"][1:6]] == expected_bonus_prices
    for order in plan["orders"][1:6]:
        assert_official_order_identity(
            order,
            event_type="BONUS",
            side="BUY",
            order_type="LOC",
            price=order["price"],
            qty=1,
        )
        assert order["desc"] == "공식줍줍"
    assert_official_order_identity(
        plan["orders"][6],
        event_type="QUARTER",
        side="SELL",
        order_type="LOC",
        price="131.77",
        qty=expected.quarter_sell_quantity,
    )
    assert_official_order_identity(
        plan["orders"][7],
        event_type="TARGET_FULL",
        side="SELL",
        order_type="LIMIT",
        price="181.78",
        qty=expected.target_sell_quantity,
    )


def test_adapter_delegates_to_public_kernel_and_does_not_mutate_snapshot(tmp_path, monkeypatch):
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


def test_same_revision_snapshot_with_bonus_and_legacy_orders_is_not_reused(tmp_path):
    cfg = isolated_cfg(tmp_path)
    strategy = make_strategy(cfg)
    contaminated_order = {"side": "BUY", "price": 1.23, "qty": 7, "type": "LOC", "desc": "SAME_REVISION_CONTAMINATED_LEAK"}
    strategy.load_daily_snapshot = lambda ticker: {
        "date": TRADE_DATE,
        "strategy": STRATEGY,
        "strategy_revision": STRATEGY_REVISION,
        "t_revision": 1,
        "intent_ids": ["legacy-looking-id"],
        "orders": [contaminated_order],
        "core_orders": [contaminated_order],
        "bonus_orders": [{"side": "BUY", "price": 1.11, "qty": 1, "type": "LOC", "desc": "BONUS_LEAK"}],
    }

    plan = strategy.get_plan("SOXL", 100.0, 158.0735, 98, 99.0, available_cash=1482.88)

    assert plan["bonus_orders"] == []
    assert all(order.get("desc") != "SAME_REVISION_CONTAMINATED_LEAK" for order in plan["orders"])
    assert all(order.get("desc") != "BONUS_LEAK" for order in plan["orders"])
    assert plan["orders"]
    assert all(order.get("strategy") == STRATEGY and order.get("intent_id") == compute_intent_id(order) for order in plan["orders"])
    assert plan["intent_ids"] == [compute_intent_id(order) for order in plan["orders"]]


def test_same_revision_snapshot_with_mismatched_intent_ids_is_not_reused(tmp_path):
    cfg = isolated_cfg(tmp_path)
    strategy = make_strategy(cfg)
    official_order = {
        "strategy": STRATEGY,
        "strategy_revision": STRATEGY_REVISION,
        "t_revision": 1,
        "trade_date": TRADE_DATE,
        "ticker": "SOXL",
        "event_type": "FULL",
        "side": "BUY",
        "order_type": "LOC",
        "type": "LOC",
        "price": "1.23",
        "qty": 7,
        "desc": "MISMATCHED_INTENT_SNAPSHOT_LEAK",
    }
    official_order["intent_id"] = compute_intent_id(official_order)
    strategy.load_daily_snapshot = lambda ticker: {
        "date": TRADE_DATE,
        "strategy": STRATEGY,
        "strategy_revision": STRATEGY_REVISION,
        "t_revision": 1,
        "intent_ids": ["not-the-canonical-intent-id"],
        "orders": [official_order],
        "core_orders": [official_order],
        "bonus_orders": [],
    }

    plan = strategy.get_plan("SOXL", 100.0, 158.0735, 98, 99.0, available_cash=1482.88)

    assert all(order.get("desc") != "MISMATCHED_INTENT_SNAPSHOT_LEAK" for order in plan["orders"])
    assert plan["intent_ids"] == [compute_intent_id(order) for order in plan["orders"]]


def test_missing_strategy_baseline_cannot_emit_compatibility_reverse_order(tmp_path):
    cfg = isolated_cfg(tmp_path, reverse_state={"is_active": True, "dynamic_t": 17.109, "rem_cash": 1482.88, "day_count": 1})
    del cfg.FILES["STRATEGY_BASELINE"]
    strategy = make_strategy(cfg)
    strategy.load_daily_snapshot = lambda ticker: None

    plan = strategy.get_plan("SOXL", 100.0, 158.0735, 98, 99.0, available_cash=1482.88, market_type="REG")

    assert plan["orders"] == []
    assert plan["core_orders"] == []
    assert plan["bonus_orders"] == []
    assert plan["safety"]["halted"] is True
    assert "baseline" in plan["safety"]["reason"].lower() or "STRATEGY_BASELINE" in plan["safety"]["reason"]


def test_version_config_rejects_legacy_labels_instead_of_normalizing(tmp_path):
    cfg = isolated_cfg(tmp_path, version={"SOXL": "V4", "TQQQ": "V4.0"})
    strategy = InfiniteStrategy(cfg)
    strategy.v4._get_logical_date_str = lambda: TRADE_DATE
    strategy.v4.load_daily_snapshot = lambda ticker: None

    soxl_plan = strategy.get_plan("SOXL", 100.0, 158.0735, 98, 99.0, available_cash=1482.88)
    tqqq_plan = strategy.get_plan("TQQQ", 100.0, 100.0, 10, 99.0, available_cash=1000.0)

    try:
        cfg.get_version("SOXL")
    except ValueError as exc:
        assert "legacy" in str(exc).lower() or "unsupported" in str(exc).lower()
    else:
        raise AssertionError("legacy V4 label must not be normalized to official strategy")
    for legacy_label in ("V14", "V14.x"):
        try:
            cfg.set_version("SOXL", legacy_label)
        except ValueError as exc:
            assert "legacy" in str(exc).lower() or "unsupported" in str(exc).lower()
        else:
            raise AssertionError(f"legacy {legacy_label} label must be rejected on save")
    assert soxl_plan["orders"] == []
    assert soxl_plan["core_orders"] == []
    assert soxl_plan["bonus_orders"] == []
    assert soxl_plan["safety"]["halted"] is True
    assert "legacy" in soxl_plan["safety"]["reason"].lower() or "unsupported" in soxl_plan["safety"]["reason"].lower()
    assert tqqq_plan["orders"] == []
    assert tqqq_plan["core_orders"] == []
    assert tqqq_plan["bonus_orders"] == []
    assert tqqq_plan["safety"]["halted"] is True
    assert "legacy" in tqqq_plan["safety"]["reason"].lower() or "unsupported" in tqqq_plan["safety"]["reason"].lower()


def test_non_soxl_returns_zero_orders_and_halt_reason(tmp_path):
    cfg = isolated_cfg(tmp_path)
    strategy = make_strategy(cfg)

    plan = strategy.get_plan("TQQQ", 100.0, 100.0, 10, 99.0, available_cash=1000.0)

    assert plan["orders"] == []
    assert plan["core_orders"] == []
    assert plan["bonus_orders"] == []
    assert plan["safety"]["halted"] is True
    assert "SOXL" in plan["safety"]["reason"]


def test_corrupt_reverse_day_zero_preserves_kernel_fail_closed_semantics(tmp_path):
    direct_kernel_plan = calculate_reverse_plan(
        ReverseState(
            ticker="SOXL",
            split=20,
            quantity=89,
            previous_quantity=98,
            avg_price=D("158.0735"),
            cash=D("1482.88"),
            t=D("17.109"),
            day=0,
            previous_closes=["130", "132", "134", "136", "138"],
        )
    )
    assert direct_kernel_plan.fail_closed is True
    assert "day" in direct_kernel_plan.reason.lower()

    reverse_state = {
        "is_active": True,
        "day_count": 0,
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

    plan = strategy.get_plan(
        "SOXL",
        current_price=100.0,
        avg_price=158.0735,
        qty=89,
        prev_close=138.0,
        ma_5day=134.0,
        available_cash=1482.88,
        market_type="REG",
        previous_closes=["130", "132", "134", "136", "138"],
    )

    assert plan["orders"] == []
    assert plan["core_orders"] == []
    assert plan["bonus_orders"] == []
    assert plan["intent_ids"] == []
    assert plan["safety"]["halted"] is True
    assert "day" in plan["safety"]["reason"].lower()
    assert "HALT" in plan["process_status"]
    assert not any(order.get("side") == "SELL" and order.get("order_type") == "MOC" for order in plan["orders"])


def test_active_reverse_missing_or_nonnumeric_day_count_fails_closed_without_orders(tmp_path):
    for corrupt_day_count in (None, "abc"):
        reverse_state = {
            "is_active": True,
            "exit_target": 0.0,
            "last_update_date": "2026-08-11",
            "last_t_update_date": "2026-08-11",
            "dynamic_t": 17.109,
            "rem_cash": 1482.88,
            "is_day_one": False,
        }
        if corrupt_day_count is not None:
            reverse_state["day_count"] = corrupt_day_count
        case_dir = tmp_path / f"case_{corrupt_day_count or 'missing'}"
        case_dir.mkdir()
        cfg = isolated_cfg(case_dir, reverse_state=reverse_state)
        strategy = make_strategy(cfg)
        strategy.load_daily_snapshot = lambda ticker: None

        plan = strategy.get_plan(
            "SOXL",
            current_price=100.0,
            avg_price=158.0735,
            qty=89,
            prev_close=138.0,
            ma_5day=134.0,
            available_cash=1482.88,
            market_type="REG",
            previous_closes=["130", "132", "134", "136", "138"],
        )

        assert plan["orders"] == []
        assert plan["core_orders"] == []
        assert plan["bonus_orders"] == []
        assert plan["intent_ids"] == []
        assert plan["safety"]["halted"] is True
        assert "day" in plan["safety"]["reason"].lower()
        assert "HALT" in plan["process_status"]
        assert not any(order.get("side") == "SELL" and order.get("order_type") == "MOC" for order in plan["orders"])
        assert not any(order.get("order_type") == "LOC" for order in plan["orders"])


def test_active_reverse_precision_loss_fractional_day_count_fails_closed_without_orders(tmp_path):
    for corrupt_day_count in ("1.0000000000000001", "2.0000000000000001", "9007199254740992.5"):
        reverse_state = {
            "is_active": True,
            "day_count": corrupt_day_count,
            "exit_target": 0.0,
            "last_update_date": "2026-08-11",
            "last_t_update_date": "2026-08-11",
            "dynamic_t": 17.109,
            "rem_cash": 1482.88,
            "is_day_one": False,
        }
        case_dir = tmp_path / f"case_{corrupt_day_count.replace('.', '_')}"
        case_dir.mkdir()
        cfg = isolated_cfg(case_dir, reverse_state=reverse_state)
        strategy = make_strategy(cfg)
        strategy.load_daily_snapshot = lambda ticker: None

        plan = strategy.get_plan(
            "SOXL",
            current_price=100.0,
            avg_price=158.0735,
            qty=89,
            prev_close=138.0,
            ma_5day=134.0,
            available_cash=1482.88,
            market_type="REG",
            previous_closes=["130", "132", "134", "136", "138"],
        )

        assert plan["orders"] == []
        assert plan["core_orders"] == []
        assert plan["bonus_orders"] == []
        assert plan["intent_ids"] == []
        assert plan["safety"]["halted"] is True
        assert "day" in plan["safety"]["reason"].lower()
        assert "HALT" in plan["process_status"]
        assert not any(order.get("side") == "SELL" and order.get("order_type") == "MOC" for order in plan["orders"])
        assert not any(order.get("order_type") == "LOC" for order in plan["orders"])


def test_wrapper_forwards_reverse_context_kwargs_and_matches_direct_wait(tmp_path):
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
    direct = make_strategy(cfg)
    direct.load_daily_snapshot = lambda ticker: None
    wrapper = InfiniteStrategy(cfg)
    wrapper.v4._get_logical_date_str = lambda: TRADE_DATE
    wrapper.v4.load_daily_snapshot = lambda ticker: None

    kwargs = {
        "confirmed_close": "130.00",
        "previous_closes": ["130", "132", "134", "136", "138"],
    }
    direct_plan = direct.get_plan(
        "SOXL",
        current_price=100.0,
        avg_price=158.0735,
        qty=89,
        prev_close=138.0,
        ma_5day=134.0,
        available_cash=1482.88,
        market_type="REG",
        **kwargs,
    )
    wrapper_plan = wrapper.get_plan(
        "SOXL",
        current_price=100.0,
        avg_price=158.0735,
        qty=89,
        prev_close=138.0,
        ma_5day=134.0,
        available_cash=1482.88,
        market_type="REG",
        **kwargs,
    )

    assert direct_plan["orders"] == []
    assert direct_plan["core_orders"] == []
    assert direct_plan["bonus_orders"] == []
    assert direct_plan["process_status"] == "♻️리버스복귀대기"
    assert wrapper_plan["orders"] == direct_plan["orders"]
    assert wrapper_plan["core_orders"] == direct_plan["core_orders"]
    assert wrapper_plan["bonus_orders"] == direct_plan["bonus_orders"]
    assert wrapper_plan["process_status"] == direct_plan["process_status"]
    assert wrapper_plan["intent_ids"] == direct_plan["intent_ids"]


def test_wrapper_unsupported_route_halt_plan_includes_canonical_metadata(tmp_path):
    cfg = isolated_cfg(tmp_path, version={"SOXL": "V4"})
    strategy = InfiniteStrategy(cfg)

    plan = strategy.get_plan("SOXL", 100.0, 158.0735, 98, 99.0, available_cash=1482.88)

    assert plan["orders"] == []
    assert plan["core_orders"] == []
    assert plan["bonus_orders"] == []
    assert plan["trade_date"]
    assert "t_revision" in plan
    assert plan["total_q"] == 0
    assert plan["avg_price"] == 0.0
    assert "source_balance_at" in plan
    assert plan["safety"]["halted"] is True


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
