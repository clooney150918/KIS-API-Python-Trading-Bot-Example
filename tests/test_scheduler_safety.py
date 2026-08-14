import asyncio
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

import scheduler_core
import scheduler_regular


class RecordingSchedulerGate:
    def __init__(self, allowed=True):
        self.allowed = allowed
        self.calls = []

    def assert_scheduler_execution_allowed(self, schedule_name, boundary):
        self.calls.append((schedule_name, boundary))
        return self.allowed, "allowed" if self.allowed else "blocked by test gate"


class FakeBot:
    async def send_message(self, *args, **kwargs):
        return None


class FakeCfg:
    def __init__(self):
        self.locked = []
        self.reset_count = 0
        self.reverse_day_increments = []

    def get_active_tickers(self):
        return ["SOXL"]

    def get_version(self, ticker):
        return "V14"

    def check_lock(self, ticker, lock_name):
        return False

    def set_lock(self, ticker, lock_name):
        self.locked.append((ticker, lock_name))

    def reset_locks(self):
        self.reset_count += 1

    def get_reverse_state(self, ticker):
        return {}

    def increment_reverse_day(self, ticker):
        self.reverse_day_increments.append(ticker)


class FakeBroker:
    def get_account_balance(self):
        return 10_000.0, {"SOXL": {"avg": 100.0, "qty": 10}}

    def get_current_price(self, ticker):
        return 101.0

    def get_previous_close(self, ticker):
        return 100.0

    def get_5day_ma(self, ticker):
        return 99.0


class FakeStrategy:
    def get_plan(self, *args, **kwargs):
        return {
            "core_orders": [
                {"side": "BUY", "qty": 1, "price": "99.50", "type": "LOC", "desc": "official test order"}
            ],
            "bonus_orders": [],
        }


def _context(app_data):
    return SimpleNamespace(job=SimpleNamespace(data=app_data, chat_id=None), bot=FakeBot())


def test_official_regular_schedule_checks_gate_before_market_lookup(monkeypatch):
    gate = RecordingSchedulerGate(allowed=False)
    app_data = {"scheduler_safety_gate": gate}

    def fail_if_called():
        raise AssertionError("market calendar must not be queried after scheduler gate denial")

    monkeypatch.setattr(scheduler_regular, "is_market_open", fail_if_called)

    asyncio.run(scheduler_regular.scheduled_early_regular_trade(_context(app_data)))

    assert gate.calls == [("scheduled_early_regular_trade", "start")]


def test_official_regular_schedule_checks_gate_again_before_order_submit(monkeypatch):
    gate = RecordingSchedulerGate(allowed=True)
    executed = []

    async def fake_execute_order_list(*args, **kwargs):
        executed.append((args, kwargs))
        return True, "submitted", ""

    monkeypatch.setattr(scheduler_regular, "is_market_open", lambda: True)
    monkeypatch.setattr(scheduler_regular.random, "randint", lambda a, b: 0)
    monkeypatch.setattr(scheduler_regular, "execute_order_list", fake_execute_order_list)

    app_data = {
        "scheduler_safety_gate": gate,
        "cfg": FakeCfg(),
        "broker": FakeBroker(),
        "strategy": FakeStrategy(),
        "tx_lock": asyncio.Lock(),
    }

    asyncio.run(scheduler_regular.scheduled_early_regular_trade(_context(app_data)))

    assert ("scheduled_early_regular_trade", "start") in gate.calls
    assert ("scheduled_early_regular_trade", "before_order_submit") in gate.calls
    assert executed, "official scheduler must still reach the order executor when both gates allow"


@pytest.mark.parametrize(
    ("moment", "expected"),
    [
        (datetime(2026, 8, 15, 12, 0, tzinfo=ZoneInfo("America/New_York")), False),  # Saturday
        (datetime(2026, 12, 25, 12, 0, tzinfo=ZoneInfo("America/New_York")), False),  # Christmas holiday
        (datetime(2026, 3, 9, 12, 0, tzinfo=ZoneInfo("America/New_York")), True),  # DST boundary Monday
        (datetime(2026, 11, 2, 12, 0, tzinfo=ZoneInfo("America/New_York")), True),  # DST boundary Monday
    ],
)
def test_official_trading_day_check_handles_weekend_holiday_and_dst_boundaries(moment, expected):
    assert scheduler_core.is_official_trading_day_at(moment) is expected


def test_official_trading_day_check_fails_closed_when_calendar_is_unavailable():
    class BrokenCalendarProvider:
        @staticmethod
        def get_calendar(name):
            raise RuntimeError("calendar unavailable")

    moment = datetime(2026, 3, 9, 12, 0, tzinfo=ZoneInfo("America/New_York"))

    assert scheduler_core.is_official_trading_day_at(moment, calendar_provider=BrokenCalendarProvider) is False


def test_early_regular_trade_calendar_exception_fails_closed_before_order_executor(monkeypatch):
    gate = RecordingSchedulerGate(allowed=True)
    executed = []

    def broken_calendar():
        raise RuntimeError("calendar provider unavailable")

    async def fake_execute_order_list(*args, **kwargs):
        executed.append((args, kwargs))
        return True, "submitted", ""

    monkeypatch.setattr(scheduler_regular, "is_market_open", broken_calendar)
    monkeypatch.setattr(scheduler_regular.random, "randint", lambda a, b: 0)
    monkeypatch.setattr(scheduler_regular, "execute_order_list", fake_execute_order_list)

    app_data = {
        "scheduler_safety_gate": gate,
        "cfg": FakeCfg(),
        "broker": FakeBroker(),
        "strategy": FakeStrategy(),
        "tx_lock": asyncio.Lock(),
    }

    asyncio.run(scheduler_regular.scheduled_early_regular_trade(_context(app_data)))

    assert gate.calls == [("scheduled_early_regular_trade", "start")]
    assert executed == []


def test_force_reset_calendar_exception_fails_closed_before_reset_actions(monkeypatch):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 8, 12, 4, 0, tzinfo=tz)

    cfg = FakeCfg()

    def broken_calendar():
        raise RuntimeError("calendar provider unavailable")

    monkeypatch.setattr(scheduler_core.datetime, "datetime", FixedDateTime)
    monkeypatch.setattr(scheduler_core, "is_market_open", broken_calendar)

    app_data = {
        "cfg": cfg,
        "broker": FakeBroker(),
        "strategy": FakeStrategy(),
        "tx_lock": asyncio.Lock(),
    }

    asyncio.run(scheduler_core.scheduled_force_reset(_context(app_data)))

    assert cfg.reset_count == 0
    assert cfg.reverse_day_increments == []
