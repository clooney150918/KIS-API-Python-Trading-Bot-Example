import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from telegram_commands import TelegramCommands


def run(coro):
    return asyncio.run(coro)
from telegram_view import TelegramView


ADMIN_ID = 12345
OTHER_ID = 99999
UNSUPPORTED = "공식 SOXL 프로필에서는 지원하지 않습니다"
DENIED = "관리자"


class FakeMessage:
    def __init__(self):
        self.replies = []
        self.edits = []
        self.text = ""
        self.caption = None

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))
        return self

    async def edit_text(self, text, **kwargs):
        self.edits.append((text, kwargs))
        return self


class FakeConfig:
    def __init__(self, chat_id=ADMIN_ID):
        self.chat_id = chat_id

    def get_chat_id(self):
        return self.chat_id

    def get_full_version_history(self):
        return [{"version": "V4.0", "date": "2026-08-12", "desc": ["offline"]}]

    def get_latest_version(self):
        return "V4.0-test"


class FakeContext:
    def __init__(self, args=None):
        self.args = args or []
        self.bot_data = {}
        self.job_queue = None


class FakeUpdate:
    def __init__(self, chat_id, user_id=None, message=None):
        self.effective_chat = SimpleNamespace(id=chat_id)
        self.effective_user = SimpleNamespace(id=chat_id if user_id is None else user_id)
        self.effective_message = message or FakeMessage()
        self.callback_query = None


def _commands(chat_id=ADMIN_ID):
    return TelegramCommands(
        FakeConfig(chat_id=chat_id),
        broker=Mock(),
        strategy=Mock(),
        legacy_lot_book=Mock(),
        sync_engine=Mock(sync_locks={}),
        view=TelegramView(),
        tx_lock=asyncio.Lock(),
    )


def test_non_admin_sensitive_command_is_denied_without_calling_handler():
    from telegram_auth import require_admin

    called = False

    async def sensitive(update, context):
        nonlocal called
        called = True

    wrapped = require_admin(FakeConfig(chat_id=ADMIN_ID), sensitive)
    update = FakeUpdate(chat_id=OTHER_ID, user_id=OTHER_ID)

    run(wrapped(update, FakeContext()))

    assert called is False
    assert update.effective_message.replies
    assert DENIED in update.effective_message.replies[0][0]


def test_admin_sensitive_command_is_allowed():
    from telegram_auth import require_admin

    calls = []

    async def sensitive(update, context):
        calls.append((update, context))
        await update.effective_message.reply_text("OK")

    wrapped = require_admin(FakeConfig(chat_id=ADMIN_ID), sensitive)
    update = FakeUpdate(chat_id=ADMIN_ID, user_id=ADMIN_ID)
    context = FakeContext()

    run(wrapped(update, context))

    assert len(calls) == 1
    assert update.effective_message.replies[0][0] == "OK"


@pytest.mark.parametrize("method_name,args", [
    ("cmd_update", []),
])
def test_blocked_commands_are_compatibility_stubs_without_side_effects(method_name, args):
    commands = _commands()
    commands.broker.get_current_price.side_effect = AssertionError("old broker path called")
    commands.legacy_lot_book.get_lots.side_effect = AssertionError("old queue path called")
    commands.legacy_lot_book.overwrite_queue.side_effect = AssertionError("old queue mutation called")
    update = FakeUpdate(chat_id=ADMIN_ID, user_id=ADMIN_ID)

    run(getattr(commands, method_name)(update, FakeContext(args=args)))

    assert update.effective_message.replies
    assert UNSUPPORTED in update.effective_message.replies[0][0]


def test_official_command_menu_excludes_error_and_blocked_commands():
    from telegram_auth import build_official_bot_commands

    command_names = [cmd.command for cmd in build_official_bot_commands()]

    assert "error" not in command_names
    for blocked in ["avwap", "queue", "add_q", "clear_q", "update"]:
        assert blocked not in command_names
    for allowed in ["start", "record", "history", "sync", "settlement", "seed", "reset", "version", "log"]:
        assert allowed in command_names


def test_start_message_uses_official_menu_only():
    msg = TelegramView().get_start_message(17, "🌞", "V4.0-test")

    assert "/log" in msg
    assert "/error" not in msg
    for blocked in ["/avwap", "/queue", "/add_q", "/clear_q", "/update"]:
        assert blocked not in msg


@pytest.mark.parametrize("renderer,args", [
    ("get_reset_menu", (["SOXL"],)),
    ("get_emergency_moc_confirm_menu", ("SOXL", 1, 10.0)),
])
def test_key_telegram_views_use_official_terminology(renderer, args):
    view = TelegramView()
    result = getattr(view, renderer)(*args)
    msg = result[0] if isinstance(result, tuple) else result

    forbidden_terms = ["타격", "격발", "덫", "지층", "큐", "소각", "졸업", "유령잔고", "스나이퍼"]
    assert not any(term in msg for term in forbidden_terms)
    assert any(term in msg for term in ["주문", "실행", "주문계획", "거래·매수 내역", "삭제", "목표매도 완료", "KIS 잔고 불일치"])
