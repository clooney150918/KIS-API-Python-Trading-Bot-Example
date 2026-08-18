from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from callback_config_handler import CallbackConfigHandler
from telegram_commands import TelegramCommands


class FakeConfig:
    def __init__(self):
        self.seeds = {"SOXL": 17659.0}

    def get_active_tickers(self):
        return ["SOXL"]

    def get_seed(self, ticker):
        return self.seeds[ticker]

    def set_seed(self, ticker, value):
        self.seeds[ticker] = float(value)


class FakeBroker:
    def __init__(self, cash=23456.78, fail=False):
        self.cash = cash
        self.fail = fail

    def get_account_balance(self):
        if self.fail:
            raise RuntimeError("balance unavailable")
        return self.cash, {}


class FakeMessage:
    def __init__(self):
        self.reply_text = AsyncMock()
        self.edit_text = AsyncMock()
        self.chat = SimpleNamespace(id=12345)


class FakeQuery:
    def __init__(self, data):
        self.data = data
        self.answer = AsyncMock()
        self.edit_message_text = AsyncMock()
        self.message = FakeMessage()


class FakeUpdate:
    def __init__(self, data=None):
        self.effective_chat = SimpleNamespace(id=12345)
        self.callback_query = FakeQuery(data) if data else None
        self.effective_message = self.callback_query.message if self.callback_query else FakeMessage()


class FakeContext:
    def __init__(self):
        self.bot = SimpleNamespace(send_message=AsyncMock())
        self.bot_data = {}


def run(coro):
    import asyncio

    return asyncio.run(coro)


def make_commands(cfg=None, broker=None):
    return TelegramCommands(
        cfg or FakeConfig(),
        broker or FakeBroker(),
        strategy=Mock(),
        legacy_lot_book=Mock(),
        sync_engine=Mock(),
        view=Mock(),
        tx_lock=Mock(),
    )


def make_config_handler(cfg=None, broker=None):
    return CallbackConfigHandler(
        cfg or FakeConfig(),
        broker or FakeBroker(),
        strategy=Mock(),
        legacy_lot_book=Mock(),
        sync_engine=Mock(),
        view=Mock(),
        tx_lock=Mock(),
    )


def test_cmd_seed_renders_current_balance_and_reset_button():
    update = FakeUpdate()
    context = FakeContext()
    commands = make_commands()

    run(commands.cmd_seed(update, context))

    text = update.effective_message.reply_text.await_args.args[0]
    markup = update.effective_message.reply_text.await_args.kwargs["reply_markup"]
    callback_data = [button.callback_data for row in markup.inline_keyboard for button in row]

    assert "SOXL" in text
    assert "시드 $17,659 | 현재 잔고 $23,457" in text
    assert "SEED:BAL_REQ:SOXL" in callback_data


def test_cmd_seed_balance_failure_does_not_raise_and_marks_failure():
    update = FakeUpdate()
    context = FakeContext()
    commands = make_commands(broker=FakeBroker(fail=True))

    run(commands.cmd_seed(update, context))

    text = update.effective_message.reply_text.await_args.args[0]
    assert "현재 잔고 조회실패" in text


def test_seed_balance_reset_requires_confirmation_then_sets_seed():
    cfg = FakeConfig()
    handler = make_config_handler(cfg=cfg, broker=FakeBroker(cash=34567.89))
    context = FakeContext()
    controller = SimpleNamespace(user_states={})

    request_update = FakeUpdate("SEED:BAL_REQ:SOXL")
    run(handler.handle(request_update, context, controller, "SEED", "BAL_REQ", ["SEED", "BAL_REQ", "SOXL"]))

    confirm_text = request_update.callback_query.edit_message_text.await_args.args[0]
    confirm_markup = request_update.callback_query.edit_message_text.await_args.kwargs["reply_markup"]
    confirm_callbacks = [button.callback_data for row in confirm_markup.inline_keyboard for button in row]
    assert "시드 재설정 확인" in confirm_text
    assert "현재 잔고: <b>$34,568</b>" in confirm_text
    assert "SEED:BAL_CONFIRM:SOXL" in confirm_callbacks
    assert cfg.seeds["SOXL"] == 17659.0

    confirm_update = FakeUpdate("SEED:BAL_CONFIRM:SOXL")
    run(handler.handle(confirm_update, context, controller, "SEED", "BAL_CONFIRM", ["SEED", "BAL_CONFIRM", "SOXL"]))

    assert cfg.seeds["SOXL"] == pytest.approx(34567.89)
    done_text = confirm_update.callback_query.edit_message_text.await_args.args[0]
    assert "시드 재설정 완료" in done_text
    assert "$34,568" in done_text
