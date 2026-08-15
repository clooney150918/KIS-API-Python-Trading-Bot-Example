"""Fail-closed Telegram admin authorization and official SOXL compatibility UX."""
from __future__ import annotations

import functools
import logging
from typing import Any, Awaitable, Callable, Iterable

from telegram import BotCommand

ADMIN_DENIED_MESSAGE = "⛔ 관리자 전용 기능입니다. 대표 사용자/채팅 ID가 일치하지 않아 요청을 차단했습니다."
UNSUPPORTED_OFFICIAL_SOXL_MESSAGE = "공식 SOXL 프로필에서는 지원하지 않습니다. /sync 또는 /log를 이용해 주세요."

_ALLOWED_COMMANDS: tuple[tuple[str, str], ...] = (
    ("start", "대시보드"),
    ("record", "장부 동기화 및 조회"),
    ("history", "목표매도 완료 기록"),
    ("sync", "통합 지시서 조회"),
    ("settlement", "설정 조회"),
    ("seed", "시드 설정"),
    ("report", "일일 리포트"),
    ("reset", "안전상태/당일 계획 초기화"),
    ("version", "버전 정보"),
    ("log", "시스템 진단 로그"),
)

BLOCKED_CALLBACK_ACTIONS = frozenset(
    {
        "EMERGENCY_EXEC",
        "EXEC",
        "MANUAL_PORTION",
        "UPDATE",
    }
)


def build_official_bot_commands() -> list[BotCommand]:
    """Return the public Telegram command menu for the official SOXL profile."""
    return [BotCommand(command=command, description=description) for command, description in _ALLOWED_COMMANDS]


def _safe_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def get_admin_chat_id(config: Any) -> int | None:
    """Read the configured representative chat/admin id; missing config fails closed."""
    getter = getattr(config, "get_chat_id", None)
    if not callable(getter):
        return None
    try:
        admin_id = _safe_int(getter())
    except Exception as exc:  # pragma: no cover - defensive logging only
        logging.error("관리자 ID 조회 실패: %s", exc)
        return None
    if admin_id is None or admin_id <= 0:
        return None
    return admin_id


def is_admin_update(update: Any, config: Any, *, require_callback_message_chat: bool = False) -> bool:
    """Cross-check effective chat, effective user, and optionally callback message chat."""
    admin_id = get_admin_chat_id(config)
    if admin_id is None:
        return False

    chat_id = _safe_int(getattr(getattr(update, "effective_chat", None), "id", None))
    user_id = _safe_int(getattr(getattr(update, "effective_user", None), "id", None))
    if chat_id != admin_id or user_id != admin_id:
        return False

    if require_callback_message_chat:
        query = getattr(update, "callback_query", None)
        message = getattr(query, "message", None)
        message_chat = getattr(message, "chat", None)
        message_chat_id = _safe_int(getattr(message_chat, "id", None))
        if message_chat_id != admin_id:
            return False

    return True


async def deny_update(update: Any) -> None:
    message = getattr(update, "effective_message", None)
    if message and hasattr(message, "reply_text"):
        await message.reply_text(ADMIN_DENIED_MESSAGE)


async def deny_callback(update: Any) -> None:
    query = getattr(update, "callback_query", None)
    if query and hasattr(query, "answer"):
        await query.answer(ADMIN_DENIED_MESSAGE, show_alert=True)


async def answer_unsupported_callback(update: Any) -> None:
    query = getattr(update, "callback_query", None)
    if query and hasattr(query, "answer"):
        await query.answer(UNSUPPORTED_OFFICIAL_SOXL_MESSAGE, show_alert=True)


async def reply_unsupported(update: Any) -> None:
    message = getattr(update, "effective_message", None)
    if message and hasattr(message, "reply_text"):
        await message.reply_text(UNSUPPORTED_OFFICIAL_SOXL_MESSAGE)


def require_admin(config: Any, handler: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    """Wrap a Telegram command/message handler with fail-closed admin auth."""

    @functools.wraps(handler)
    async def wrapped(update: Any, context: Any, *args: Any, **kwargs: Any) -> Any:
        if not is_admin_update(update, config):
            await deny_update(update)
            return None
        return await handler(update, context, *args, **kwargs)

    return wrapped


def is_blocked_callback_action(action: str, sub: str = "") -> bool:
    if action in BLOCKED_CALLBACK_ACTIONS:
        return True
    if action == "MODE" and str(sub).upper().startswith(("VWAP", "VOLATILITY")):
        return True
    if action == "SET_VER_CONFIRM" and any(token in str(sub).upper() for token in ("VWAP", "VOLATILITY")):
        return True
    if action == "INPUT" and any(token in str(sub).upper() for token in ("VWAP", "VOLATILITY")):
        return True
    return False
