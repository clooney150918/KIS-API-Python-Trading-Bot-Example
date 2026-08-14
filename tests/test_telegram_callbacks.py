from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from telegram_callbacks import TelegramCallbacks


ADMIN_ID = 12345
OTHER_ID = 99999
UNSUPPORTED = "공식 SOXL 프로필에서는 지원하지 않습니다"
DENIED = "관리자"


def run(coro):
    import asyncio

    return asyncio.run(coro)


class FakeConfig:
    def __init__(self, chat_id=ADMIN_ID):
        self.chat_id = chat_id

    def get_chat_id(self):
        return self.chat_id


class FakeQuery:
    def __init__(self, data, message_chat_id=ADMIN_ID):
        self.data = data
        self.answer = AsyncMock()
        self.edit_message_text = AsyncMock()
        self.message = SimpleNamespace(chat=SimpleNamespace(id=message_chat_id))


class FakeUpdate:
    def __init__(self, data, chat_id=ADMIN_ID, user_id=ADMIN_ID, message_chat_id=ADMIN_ID):
        self.effective_chat = SimpleNamespace(id=chat_id)
        self.effective_user = SimpleNamespace(id=user_id)
        self.callback_query = FakeQuery(data, message_chat_id=message_chat_id)
        self.effective_message = self.callback_query.message


class FakeContext:
    def __init__(self):
        self.bot = SimpleNamespace(send_message=AsyncMock())


def _callbacks(config=None):
    cb = TelegramCallbacks(
        config or FakeConfig(),
        broker=Mock(),
        strategy=Mock(),
        queue_ledger=Mock(),
        sync_engine=Mock(),
        view=Mock(),
        tx_lock=Mock(),
    )
    cb.order_handler.handle = AsyncMock(side_effect=AssertionError("old order handler called"))
    cb.config_handler.handle = AsyncMock(side_effect=AssertionError("old config handler called"))
    return cb


@pytest.mark.parametrize(
    "kwargs",
    [
        {"chat_id": OTHER_ID, "user_id": ADMIN_ID, "message_chat_id": ADMIN_ID},
        {"chat_id": ADMIN_ID, "user_id": OTHER_ID, "message_chat_id": ADMIN_ID},
        {"chat_id": ADMIN_ID, "user_id": ADMIN_ID, "message_chat_id": OTHER_ID},
    ],
)
def test_callback_auth_cross_checks_effective_chat_user_and_message_chat(kwargs):
    callbacks = _callbacks(FakeConfig(chat_id=ADMIN_ID))
    update = FakeUpdate("VERSION:LATEST", **kwargs)
    context = FakeContext()

    run(callbacks.handle_callback(update, context, controller=Mock()))

    update.callback_query.answer.assert_awaited()
    assert DENIED in update.callback_query.answer.await_args.args[0]
    callbacks.config_handler.handle.assert_not_awaited()


@pytest.mark.parametrize("data,handler_name", [
    ("EMERGENCY_EXEC:SOXL", "order_handler"),
    ("EXEC:SOXL", "order_handler"),
    ("MANUAL_PORTION:BUY:SOXL", "order_handler"),
])
def test_blocked_callback_actions_return_unsupported_without_side_effects(data, handler_name):
    callbacks = _callbacks(FakeConfig(chat_id=ADMIN_ID))
    update = FakeUpdate(data, chat_id=ADMIN_ID, user_id=ADMIN_ID, message_chat_id=ADMIN_ID)
    context = FakeContext()

    run(callbacks.handle_callback(update, context, controller=Mock()))

    update.callback_query.answer.assert_awaited()
    assert UNSUPPORTED in update.callback_query.answer.await_args.args[0]
    getattr(callbacks, handler_name).handle.assert_not_awaited()


def test_allowed_callback_for_admin_routes_to_existing_handler():
    callbacks = _callbacks(FakeConfig(chat_id=ADMIN_ID))
    callbacks.config_handler.handle = AsyncMock()
    update = FakeUpdate("VERSION:LATEST", chat_id=ADMIN_ID, user_id=ADMIN_ID, message_chat_id=ADMIN_ID)
    context = FakeContext()

    run(callbacks.handle_callback(update, context, controller=Mock()))

    callbacks.config_handler.handle.assert_awaited_once()
