"""Task 11: Execution replay scenarios — kernel plan computation correctness (Tests 1-8).

These tests validate the laoer_v4_20 kernel and strategy_v14 adapter against
the official 20-split rules.  All inputs are synthetic; no KIS, Telegram, or
network calls are made.
"""

import json
from decimal import Decimal

import pytest

from laoer_v4_20 import (
    FillEvent,
    NormalState,
    NormalPlan,
    ReverseState,
    ReversePlan,
    apply_fill_event,
    calculate_normal_plan,
    calculate_reverse_plan,
)


# ---------------------------------------------------------------------------
# Test 1: Baseline 정상계획 6/24/74
# T=6 → star=20-2*6=8% below avg → FIRST_HALF → star·average half budget each
# ---------------------------------------------------------------------------
def test_01_baseline_normal_plan_t6_first_half():
    """T=6 → star=20-2*6=8% below avg → FIRST_HALF → star/average half budget each."""
    state = NormalState(
        ticker="SOXL", split=20, quantity=100,
        avg_price=Decimal("30.00"), cash=Decimal("3000.00"),
        t=Decimal("6"),
    )
    plan = calculate_normal_plan(state)
    assert plan.phase == "FIRST_HALF"
    assert plan.star_percent == Decimal("8")
    assert plan.star_point > Decimal("0")
    # In first half: star_buy_budget = one_buy_budget / 2
    # one_buy_budget = 3000 / 14 ≈ 214.29
    assert plan.one_buy_budget > Decimal("200")
    assert plan.star_buy_budget == pytest.approx(plan.one_buy_budget / Decimal("2"))
    assert plan.average_buy_budget == plan.star_buy_budget
    assert plan.star_buy_quantity >= 1
    assert plan.average_buy_quantity >= 1
    assert plan.quarter_sell_quantity == 100 // 4  # 25
    assert plan.target_sell_quantity == 100 - 25  # 75
    assert not plan.fail_closed


# ---------------------------------------------------------------------------
# Test 2: T=9.99→10 경계 (전반→후반)
# T=9.99  still FIRST_HALF; T=10.00 → SECOND_HALF
# ---------------------------------------------------------------------------
def test_02_t10_boundary_second_half_transition():
    """T=9.99 → FIRST_HALF; T=10.00 → SECOND_HALF."""
    # Just below boundary (needs enough cash to buy at least 1 share)
    state_first = NormalState(
        ticker="SOXL", split=20, quantity=100,
        avg_price=Decimal("30.00"), cash=Decimal("3000.00"),
        t=Decimal("9.99"),
    )
    plan_first = calculate_normal_plan(state_first)
    assert plan_first.phase == "FIRST_HALF"
    assert plan_first.average_buy_budget > Decimal("0")
    assert plan_first.average_buy_quantity >= 1

    # At boundary
    state_second = NormalState(
        ticker="SOXL", split=20, quantity=100,
        avg_price=Decimal("30.00"), cash=Decimal("3000.00"),
        t=Decimal("10.00"),
    )
    plan_second = calculate_normal_plan(state_second)
    assert plan_second.phase == "SECOND_HALF"
    assert plan_second.average_buy_budget == Decimal("0")
    assert plan_second.average_buy_quantity == 0
    # Second half: star_buy_budget = one_buy_budget (full)
    assert plan_second.star_buy_budget == plan_second.one_buy_budget


# ---------------------------------------------------------------------------
# Test 3: T=19→19.01 리버스 경계
# T ≤ 19 → normal mode; T > 19 → reverse entry
# ---------------------------------------------------------------------------
def test_03_t19_reverse_boundary():
    # At boundary: normal mode, reverse_entry=True
    state_normal = NormalState(
        ticker="SOXL", split=20, quantity=98,
        avg_price=Decimal("158.0735"), cash=Decimal("1482.88"),
        t=Decimal("19.00"),
    )
    plan = calculate_normal_plan(state_normal)
    assert plan.phase == "SECOND_HALF"
    assert plan.reverse_entry is False
    assert not plan.fail_closed

    # Just above: reverse entry
    state_reverse = NormalState(
        ticker="SOXL", split=20, quantity=98,
        avg_price=Decimal("158.0735"), cash=Decimal("1482.88"),
        t=Decimal("19.01"),
    )
    plan_reverse = calculate_normal_plan(state_reverse)
    assert plan_reverse.phase == "REVERSE_ENTRY"
    assert plan_reverse.reverse_entry is True
    assert plan_reverse.fail_closed
    assert "range" in plan_reverse.reason.lower()


# ---------------------------------------------------------------------------
# Test 4: 쿼터매도 후 T×0.75
# apply_fill_event(FillEvent.QUARTER, T) → T * 0.75
# ---------------------------------------------------------------------------
def test_04_quarter_sell_multiplies_t_by_075():
    assert apply_fill_event(Decimal("6"), FillEvent.QUARTER) == Decimal("4.5")
    assert apply_fill_event(Decimal("10"), FillEvent.QUARTER) == Decimal("7.5")
    assert apply_fill_event(Decimal("18.32"), FillEvent.QUARTER) == Decimal("13.74")
    assert apply_fill_event(Decimal("4"), FillEvent.QUARTER) == Decimal("3")
    assert apply_fill_event(Decimal("0"), FillEvent.QUARTER) == Decimal("0")


# ---------------------------------------------------------------------------
# Test 5: 목표매도 후 일부 LOC 매수
# TARGET_SELL_THEN_FULL_BUY  → (T * 0.25) + 1.0
# TARGET_SELL_THEN_HALF_BUY  → (T * 0.25) + 0.5
# ---------------------------------------------------------------------------
def test_05_target_sell_then_buy_t_formulas():
    assert apply_fill_event(Decimal("6"), FillEvent.TARGET_FULL) == Decimal("2.5")
    assert apply_fill_event(Decimal("6"), FillEvent.TARGET_HALF) == Decimal("2.0")
    assert apply_fill_event(Decimal("10"), FillEvent.TARGET_FULL) == Decimal("3.5")
    assert apply_fill_event(Decimal("10"), FillEvent.TARGET_HALF) == Decimal("3.0")
    assert apply_fill_event(Decimal("18"), FillEvent.TARGET_FULL) == Decimal("5.5")
    assert apply_fill_event(Decimal("18"), FillEvent.TARGET_HALF) == Decimal("5.0")


# ---------------------------------------------------------------------------
# Test 6: 리버스 첫날 Q/10 MOC
# day=1 → sell_quantity = quantity // 10, t_after_sell = t * 0.9, MOC
# ---------------------------------------------------------------------------
def test_06_reverse_day1_sell_q10_moc():
    state = ReverseState(
        ticker="SOXL", split=20, quantity=98,
        avg_price=Decimal("158.0735"), cash=Decimal("1482.88"),
        t=Decimal("20"), day=1,
        previous_quantity=None,
    )
    plan = calculate_reverse_plan(state)
    assert plan.day == 1
    assert plan.sell_quantity == 98 // 10  # 9
    assert plan.sell_order_type == "MOC"
    assert plan.buy_quantity == 0
    assert plan.t_after_sell == Decimal("18.0")   # 20 * 0.9
    assert plan.t_carry == plan.t_after_sell
    assert not plan.return_to_normal
    assert not plan.fail_closed


# ---------------------------------------------------------------------------
# Test 7: 리버스 매도·매수 T 연속 변화
# day>1 → buy_budget=cash/4, buy_qty=budget/star_buy_price, t_after_buy computed
# ---------------------------------------------------------------------------
def test_07_reverse_day2_sell_buy_t_continuous():
    previous_closes = [Decimal("140"), Decimal("141"), Decimal("139"),
                       Decimal("142"), Decimal("143")]
    state = ReverseState(
        ticker="SOXL", split=20, quantity=100,
        avg_price=Decimal("145.0"), cash=Decimal("1482.88"),
        t=Decimal("18.0"), day=2,
        previous_quantity=100,
        previous_closes=previous_closes,
    )
    plan = calculate_reverse_plan(state)
    assert plan.day == 2
    assert not plan.fail_closed
    # sell = prev_qty // 10 = 10
    assert plan.sell_quantity == 10
    assert plan.sell_order_type == "LOC"
    # star_average = 141.0 → star_point = 141.00, star_buy_price = 140.99
    assert plan.star_point == Decimal("141.00")
    assert plan.star_buy_price == Decimal("140.99")
    # buy_budget = 1482.88 / 4 = 370.72
    assert plan.buy_budget == Decimal("370.72")
    # buy_qty = floor(370.72 / 140.99) = 2
    assert plan.buy_quantity > 0
    # t_after_sell = 18 * 0.9 = 16.2
    assert plan.t_after_sell == Decimal("16.2")
    # t_after_buy > t_after_sell (since buy_quantity > 0, not MOC)
    assert plan.t_after_buy > plan.t_after_sell
    assert plan.t_carry == plan.t_after_buy


# ---------------------------------------------------------------------------
# Test 8: 리버스 종료 후 T 승계
# confirmed_close > avg_price * 0.80 → return_to_normal=True, t_carry=t
# ---------------------------------------------------------------------------
def test_08_reverse_return_to_normal_t_carry():
    state = ReverseState(
        ticker="SOXL", split=20, quantity=100,
        avg_price=Decimal("145.0"), cash=Decimal("1482.88"),
        t=Decimal("16.2"), day=3,
        confirmed_close=Decimal("130.0"),  # 130 > 145*0.80=116
    )
    plan = calculate_reverse_plan(state)
    assert plan.return_to_normal is True
    assert plan.t_carry == Decimal("16.2")
    assert plan.sell_quantity == 0
    assert plan.buy_quantity == 0
    assert not plan.fail_closed


# ---------------------------------------------------------------------------
# Fill event table exhaustiveness
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("event,legacy_name", [
    (FillEvent.FULL, "FULL_BUY"),
    (FillEvent.HALF, "HALF_BUY"),
    (FillEvent.QUARTER, "QUARTER_SELL"),
    (FillEvent.TARGET_FULL, "TARGET_SELL_THEN_FULL_BUY"),
    (FillEvent.TARGET_HALF, "TARGET_SELL_THEN_HALF_BUY"),
])
def test_fill_event_legacy_compatibility(event, legacy_name):
    assert FillEvent(legacy_name) == event
    assert FillEvent(event.value) == event
