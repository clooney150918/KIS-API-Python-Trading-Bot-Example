from decimal import Decimal

import pytest

from laoer_v4_20 import (
    FillEvent,
    NormalState,
    ReverseState,
    apply_fill_event,
    calculate_normal_plan,
    calculate_reverse_plan,
)


def D(value):
    return Decimal(str(value))


def test_normal_fixture_matches_official_soxl_20_split_second_half_plan():
    state = NormalState(
        ticker="SOXL",
        split=20,
        quantity=98,
        avg_price=D("158.0735"),
        cash=D("1482.88"),
        t=D("18.32"),
        reverse=False,
    )

    plan = calculate_normal_plan(state)

    assert plan.ticker == "SOXL"
    assert plan.split == 20
    assert plan.phase == "SECOND_HALF"
    assert plan.star_percent == D("-16.64")
    assert plan.star_raw == D("131.77006960")
    assert plan.star_point == D("131.77")
    assert plan.star_buy_price == D("131.76")
    assert plan.one_buy_budget == D("882.6666666666666666666666667")
    assert plan.star_buy_quantity == 6
    assert plan.average_buy_quantity == 0
    assert plan.quarter_sell_quantity == 24
    assert plan.target_sell_quantity == 74
    assert plan.target_sell_price == D("189.69")
    assert plan.reverse_entry is False


def test_normal_accepts_float_inputs_only_by_string_decimal_conversion():
    plan = calculate_normal_plan(
        NormalState(
            ticker="SOXL",
            split=20,
            quantity=98.0,
            avg_price=158.0735,
            cash=1482.88,
            t=18.32,
            reverse=False,
        )
    )

    assert plan.star_raw == D("131.77006960")
    assert plan.star_point == D("131.77")


@pytest.mark.parametrize(
    "t_value, expected_phase, expected_avg_qty_nonzero",
    [("9.99", "FIRST_HALF", True), ("10", "SECOND_HALF", False), ("19", "SECOND_HALF", False)],
)
def test_normal_half_boundaries_and_buy_budget_shape(t_value, expected_phase, expected_avg_qty_nonzero):
    plan = calculate_normal_plan(
        NormalState(
            ticker="SOXL",
            split=20,
            quantity=10,
            avg_price=D("100"),
            cash=D("3000"),
            t=D(t_value),
            reverse=False,
        )
    )

    assert plan.phase == expected_phase
    if expected_avg_qty_nonzero:
        assert plan.star_buy_budget == plan.one_buy_budget / D("2")
        assert plan.average_buy_budget == plan.one_buy_budget / D("2")
        assert plan.average_buy_price == D("100.00")
        assert plan.average_buy_quantity > 0
    else:
        assert plan.star_buy_budget == plan.one_buy_budget
        assert plan.average_buy_budget == D("0")
        assert plan.average_buy_quantity == 0


def test_normal_enters_reverse_when_t_exceeds_19_without_new_normal_orders():
    plan = calculate_normal_plan(
        NormalState(
            ticker="SOXL",
            split=20,
            quantity=98,
            avg_price=D("158.0735"),
            cash=D("1482.88"),
            t=D("19.01"),
            reverse=False,
        )
    )

    assert plan.reverse_entry is True
    assert plan.phase == "REVERSE_ENTRY"
    assert plan.star_buy_quantity == 0
    assert plan.average_buy_quantity == 0


@pytest.mark.parametrize(
    "state_kwargs",
    [
        {"quantity": 0, "avg_price": "100", "cash": "1000", "t": "1"},
        {"quantity": 10, "avg_price": "0", "cash": "1000", "t": "1"},
        {"quantity": 10, "avg_price": "100", "cash": "0", "t": "1"},
        {"quantity": 10, "avg_price": "100", "cash": "1000", "t": "20"},
    ],
)
def test_normal_fail_closed_zeroes_quantities_on_invalid_state(state_kwargs):
    state = NormalState(
        ticker="SOXL",
        split=20,
        quantity=state_kwargs["quantity"],
        avg_price=D(state_kwargs["avg_price"]),
        cash=D(state_kwargs["cash"]),
        t=D(state_kwargs["t"]),
        reverse=False,
    )

    plan = calculate_normal_plan(state)

    assert plan.star_buy_quantity == 0
    assert plan.average_buy_quantity == 0
    assert plan.quarter_sell_quantity == 0
    assert plan.target_sell_quantity == 0


@pytest.mark.parametrize(
    "event_name, expected",
    [
        ("FULL", "4"),
        ("HALF", "3.5"),
        ("QUARTER", "2.25"),
        ("TARGET_FULL", "1.75"),
        ("TARGET_HALF", "1.25"),
    ],
)
def test_apply_fill_event_accepts_public_spec_event_names(event_name, expected):
    public_event = FillEvent(event_name)

    assert apply_fill_event(D("3"), public_event) == D(expected)
    assert apply_fill_event(D("3"), event_name) == D(expected)


@pytest.mark.parametrize(
    "event, expected",
    [
        (FillEvent.FULL_BUY, "4"),
        (FillEvent.HALF_BUY, "3.5"),
        (FillEvent.QUARTER_SELL, "2.25"),
        (FillEvent.TARGET_SELL_THEN_FULL_BUY, "1.75"),
        (FillEvent.TARGET_SELL_THEN_HALF_BUY, "1.25"),
    ],
)
def test_apply_fill_event_updates_t_by_backward_alias_event_table(event, expected):
    assert apply_fill_event(D("3"), event) == D(expected)


def test_bonus_buy_t_unchanged():
    assert apply_fill_event(Decimal("13.74"), FillEvent.BONUS_BUY) == Decimal("13.74")


def test_bonus_buy_string_parse():
    assert FillEvent("BONUS") is FillEvent.BONUS
    assert FillEvent("BONUS_BUY") is FillEvent.BONUS


@pytest.mark.parametrize("t_value", ["1", "10"])
def test_normal_fail_closed_when_cash_cannot_buy_one_share_in_normal_phase(t_value):
    plan = calculate_normal_plan(
        NormalState(
            ticker="SOXL",
            split=20,
            quantity=10,
            avg_price=D("100"),
            cash=D("1"),
            t=D(t_value),
            reverse=False,
        )
    )

    assert plan.fail_closed is True
    assert "insufficient cash" in plan.reason.lower()
    assert "cannot buy one share" in plan.reason.lower()
    assert plan.star_buy_quantity == 0
    assert plan.average_buy_quantity == 0
    assert plan.quarter_sell_quantity == 0
    assert plan.target_sell_quantity == 0


def test_normal_negative_t_fails_closed_with_zero_quantities_and_domain_reason():
    plan = calculate_normal_plan(
        NormalState(
            ticker="SOXL",
            split=20,
            quantity=10,
            avg_price="100",
            cash="1000000",
            t="-1",
            reverse=False,
        )
    )

    assert plan.fail_closed is True
    assert "t" in plan.reason.lower()
    assert any(word in plan.reason.lower() for word in ["invalid", "domain"])
    assert plan.star_buy_quantity == 0
    assert plan.average_buy_quantity == 0
    assert plan.quarter_sell_quantity == 0
    assert plan.target_sell_quantity == 0


@pytest.mark.parametrize("day", [0, -1])
def test_reverse_day_zero_or_negative_fails_closed_with_zero_quantities_and_day_reason(day):
    plan = calculate_reverse_plan(
        ReverseState(
            ticker="SOXL",
            split=20,
            quantity=10,
            avg_price="100",
            cash="1000",
            t="19",
            day=day,
        )
    )

    assert plan.fail_closed is True
    assert "day" in plan.reason.lower()
    assert any(word in plan.reason.lower() for word in ["invalid", "domain"])
    assert plan.sell_quantity == 0
    assert plan.buy_quantity == 0
    assert plan.sell_order_type == "NONE"


@pytest.mark.parametrize("field,bad_value", [("avg_price", "NaN"), ("avg_price", "Infinity"), ("avg_price", "abc"), ("cash", "NaN"), ("cash", "Infinity"), ("cash", "abc"), ("t", "NaN"), ("t", "Infinity"), ("t", "abc")])
def test_normal_malformed_decimal_state_fails_closed_without_exception(field, bad_value):
    kwargs = {"quantity": 10, "avg_price": "100", "cash": "1000", "t": "1"}
    kwargs[field] = bad_value

    plan = calculate_normal_plan(NormalState(ticker="SOXL", split=20, reverse=False, **kwargs))

    assert plan.fail_closed is True
    assert "invalid" in plan.reason.lower()
    assert plan.star_buy_quantity == 0
    assert plan.average_buy_quantity == 0
    assert plan.quarter_sell_quantity == 0
    assert plan.target_sell_quantity == 0


@pytest.mark.parametrize("field,bad_value", [("avg_price", "NaN"), ("avg_price", "Infinity"), ("avg_price", "abc"), ("cash", "NaN"), ("cash", "Infinity"), ("cash", "abc"), ("t", "NaN"), ("t", "Infinity"), ("t", "abc")])
def test_reverse_malformed_decimal_state_fails_closed_without_exception(field, bad_value):
    kwargs = {"quantity": 10, "avg_price": "100", "cash": "1000", "t": "19"}
    kwargs[field] = bad_value

    plan = calculate_reverse_plan(ReverseState(ticker="SOXL", split=20, day=1, **kwargs))

    assert plan.fail_closed is True
    assert "invalid" in plan.reason.lower()
    assert plan.sell_quantity == 0
    assert plan.buy_quantity == 0
    assert plan.sell_order_type == "NONE"


@pytest.mark.parametrize(
    "state",
    [
        NormalState(ticker="SPY", split=20, quantity=10, avg_price="abc", cash="1000", t="1", reverse=False),
        NormalState(ticker="SOXL", split=10, quantity=10, avg_price="100", cash="abc", t="1", reverse=False),
    ],
)
def test_normal_unsupported_ticker_or_split_with_malformed_numeric_fields_fails_closed_without_exception(state):
    plan = calculate_normal_plan(state)

    assert plan.fail_closed is True
    assert "supported" in plan.reason.lower()
    assert plan.star_buy_quantity == 0
    assert plan.average_buy_quantity == 0
    assert plan.quarter_sell_quantity == 0
    assert plan.target_sell_quantity == 0


@pytest.mark.parametrize(
    "state",
    [
        ReverseState(ticker="SPY", split=20, quantity=10, avg_price="abc", cash="1000", t="19", day=1),
        ReverseState(ticker="SOXL", split=10, quantity=10, avg_price="100", cash="abc", t="19", day=1),
    ],
)
def test_reverse_unsupported_ticker_or_split_with_malformed_numeric_fields_fails_closed_without_exception(state):
    plan = calculate_reverse_plan(state)

    assert plan.fail_closed is True
    assert "supported" in plan.reason.lower()
    assert plan.sell_quantity == 0
    assert plan.buy_quantity == 0
    assert plan.sell_order_type == "NONE"



def test_reverse_first_day_sells_floor_one_tenth_moc_and_buys_nothing():
    plan = calculate_reverse_plan(
        ReverseState(
            ticker="SOXL",
            split=20,
            quantity=98,
            avg_price=D("158.0735"),
            cash=D("1482.88"),
            t=D("19.01"),
            day=1,
        )
    )

    assert plan.reverse_entry is True
    assert plan.day == 1
    assert plan.sell_quantity == 9
    assert plan.sell_order_type == "MOC"
    assert plan.buy_budget == D("0")
    assert plan.buy_quantity == 0
    assert plan.t_after_sell == D("17.109")


def test_reverse_second_day_uses_previous_five_confirmed_closes_for_star_and_cash_quarter_buy():
    plan = calculate_reverse_plan(
        ReverseState(
            ticker="SOXL",
            split=20,
            quantity=89,
            previous_quantity=98,
            avg_price=D("158.0735"),
            cash=D("1482.88"),
            t=D("17.109"),
            day=2,
            previous_closes=["130", "132", "134", "136", "138"],
        )
    )

    assert plan.reverse_entry is True
    assert plan.star_point == D("134.00")
    assert plan.star_buy_price == D("133.99")
    assert plan.sell_quantity == 9
    assert plan.sell_order_type == "LOC"
    assert plan.buy_budget == D("370.72")
    assert plan.buy_quantity == 2
    assert plan.t_after_sell == D("15.3981")
    assert plan.t_after_buy == D("15.3981") + (D("20") - D("15.3981")) * D("0.25")
    assert plan.moc_sell_required is False


def test_reverse_if_cash_cannot_buy_one_share_then_moc_sell_required():
    plan = calculate_reverse_plan(
        ReverseState(
            ticker="SOXL",
            split=20,
            quantity=89,
            previous_quantity=98,
            avg_price=D("158.0735"),
            cash=D("100"),
            t=D("17.109"),
            day=3,
            previous_closes=["130", "132", "134", "136", "138"],
        )
    )

    assert plan.buy_quantity == 0
    assert plan.moc_sell_required is True
    assert plan.sell_order_type == "MOC"


def test_reverse_returns_to_normal_when_confirmed_close_exceeds_eighty_percent_of_average():
    plan = calculate_reverse_plan(
        ReverseState(
            ticker="SOXL",
            split=20,
            quantity=89,
            previous_quantity=98,
            avg_price=D("158.0735"),
            cash=D("1482.88"),
            t=D("17.109"),
            day=4,
            previous_closes=["130", "132", "134", "136", "138"],
            confirmed_close=D("126.46"),
        )
    )

    assert plan.return_to_normal is True
    assert plan.t_carry == D("17.109")
    assert plan.sell_quantity == 0
    assert plan.buy_quantity == 0
