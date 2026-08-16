"""Deterministic official order intent ledger.

Order identity is the official plan meaning, not a UI description string.  This
module performs local locked JSONL IO only; it never contacts broker, Telegram,
Docker, or network services.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Mapping

from laoer_v4_20 import FillEvent


class InvalidOrderIntentError(ValueError):
    pass


class DuplicateOrderIntentError(InvalidOrderIntentError):
    pass


class StaleTRevisionError(InvalidOrderIntentError):
    pass


class OrderIntentLedgerCorruptError(RuntimeError):
    pass


STRATEGY = "LAOER_V4_SOXL_20"
ALLOWED_TICKERS = frozenset({"SOXL"})
ALLOWED_EVENT_TYPES = frozenset(
    set(FillEvent.__members__) | {str(member.value) for member in FillEvent} | {"REVERSE_QUARTER_BUY", "REVERSE_SELL"}
)
ALLOWED_ORDER_TYPES = frozenset({"LIMIT", "LOC", "MOC", "MOO", "LOO"})
REQUIRED_PLAN_FIELDS = (
    "strategy",
    "strategy_revision",
    "t_revision",
    "ticker",
    "trade_date",
    "event_type",
    "side",
    "order_type",
    "price",
    "qty",
)
LEDGER_FIELDS = REQUIRED_PLAN_FIELDS + ("intent_id", "status", "created_at")
ACCEPTED_ORDER_FIELDS = (
    "account_fingerprint",
    "ticker",
    "exchange",
    "trade_date",
    "order_no",
    "matching_key",
)
TERMINAL_STATUSES = frozenset({"FILLED", "CANCELLED", "REJECTED"})
ALLOWED_TRANSITIONS = {
    "PLANNED": frozenset({"SUBMITTED"}),
    "SUBMITTED": frozenset({"PARTIAL", "FILLED", "CANCELLED", "REJECTED"}),
    "PARTIAL": frozenset({"FILLED", "CANCELLED", "REJECTED"}),
    "FILLED": frozenset(),
    "CANCELLED": frozenset(),
    "REJECTED": frozenset(),
}


def _parse_positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise InvalidOrderIntentError(f"{field} must be a positive integer")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.isdigit():
        parsed = int(value)
    else:
        raise InvalidOrderIntentError(f"{field} must be a positive integer")
    if parsed <= 0:
        raise InvalidOrderIntentError(f"{field} must be a positive integer")
    return parsed


def _require_non_empty_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise InvalidOrderIntentError(f"{field} is required")
    return value.strip()


def _require_iso8601(value: Any, field: str) -> str:
    text = _require_non_empty_string(value, field)
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidOrderIntentError(f"{field} must be ISO-8601") from exc
    return text


def _normalize_price(value: Any) -> str:
    text = _require_non_empty_string(str(value) if value is not None else value, "price")
    try:
        parsed = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise InvalidOrderIntentError("price must be a finite positive decimal") from exc
    if not parsed.is_finite():
        raise InvalidOrderIntentError("price must be a finite positive decimal")
    if parsed <= 0:
        raise InvalidOrderIntentError("price must be a finite positive decimal")
    return text


def _normalize_accepted_order(accepted_order: Mapping[str, Any]) -> dict[str, str]:
    if not isinstance(accepted_order, Mapping):
        raise InvalidOrderIntentError("accepted_order must be a mapping")
    accepted = {
        "account_fingerprint": _require_non_empty_string(accepted_order.get("account_fingerprint"), "account_fingerprint"),
        "ticker": _require_non_empty_string(accepted_order.get("ticker"), "ticker").upper(),
        "exchange": _require_non_empty_string(accepted_order.get("exchange"), "exchange").upper(),
        "trade_date": _require_non_empty_string(accepted_order.get("trade_date"), "trade_date").replace("-", ""),
        "order_no": _require_non_empty_string(accepted_order.get("order_no"), "order_no"),
    }
    accepted["matching_key"] = "|".join([
        accepted["account_fingerprint"],
        accepted["ticker"],
        accepted["exchange"],
        accepted["trade_date"],
        accepted["order_no"],
    ])
    supplied_key = accepted_order.get("matching_key")
    if supplied_key is not None and str(supplied_key) != accepted["matching_key"]:
        raise InvalidOrderIntentError("accepted_order matching_key mismatch")
    return accepted


def _normalize_plan(intent: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(intent, Mapping):
        raise InvalidOrderIntentError("intent must be a mapping")
    normalized = {
        "strategy": _require_non_empty_string(intent.get("strategy"), "strategy"),
        "strategy_revision": _parse_positive_int(intent.get("strategy_revision"), "strategy_revision"),
        "t_revision": _parse_positive_int(intent.get("t_revision"), "t_revision"),
        "ticker": _require_non_empty_string(intent.get("ticker"), "ticker").upper(),
        "trade_date": _require_non_empty_string(intent.get("trade_date"), "trade_date"),
        "event_type": _require_non_empty_string(intent.get("event_type"), "event_type").upper(),
        "side": _require_non_empty_string(intent.get("side"), "side").upper(),
        "order_type": _require_non_empty_string(intent.get("order_type"), "order_type").upper(),
        "price": _normalize_price(intent.get("price")),
        "qty": _parse_positive_int(intent.get("qty"), "qty"),
    }
    if normalized["strategy"] != STRATEGY:
        raise InvalidOrderIntentError(f"strategy must be {STRATEGY}")
    if normalized["ticker"] not in ALLOWED_TICKERS:
        raise InvalidOrderIntentError(f"ticker must be one of {sorted(ALLOWED_TICKERS)}")
    if normalized["event_type"] not in ALLOWED_EVENT_TYPES:
        raise InvalidOrderIntentError("event_type is not an official FillEvent")
    if normalized["side"] not in {"BUY", "SELL"}:
        raise InvalidOrderIntentError("side must be BUY or SELL")
    if normalized["order_type"] not in ALLOWED_ORDER_TYPES:
        raise InvalidOrderIntentError(f"order_type must be one of {sorted(ALLOWED_ORDER_TYPES)}")
    return normalized


def compute_intent_id(intent: Mapping[str, Any]) -> str:
    """Return sha256(ticker|trade_date|strategy_revision|event_type|side|price|qty)."""
    normalized = _normalize_plan(intent)
    canonical = "|".join(
        [
            normalized["ticker"],
            normalized["trade_date"],
            str(normalized["strategy_revision"]),
            normalized["event_type"],
            normalized["side"],
            normalized["price"],
            str(normalized["qty"]),
        ]
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class OrderIntentStore:
    def __init__(
        self,
        ledger_path: str | os.PathLike[str],
        *,
        current_t_revision_provider: Callable[[str], int] | None,
    ):
        self.ledger_path = Path(ledger_path)
        self.lock_path = self.ledger_path.with_suffix(self.ledger_path.suffix + ".lock")
        self.current_t_revision_provider = current_t_revision_provider

    def create_planned(self, intent: Mapping[str, Any]) -> dict[str, Any]:
        normalized = _normalize_plan(intent)
        self._assert_current_t_revision(normalized["ticker"], normalized["t_revision"])
        record = dict(normalized)
        record["intent_id"] = compute_intent_id(normalized)
        record["status"] = "PLANNED"
        record["created_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with self._exclusive_lock():
            records = self._read_records_unlocked()
            if any(existing["intent_id"] == record["intent_id"] for existing in records):
                raise DuplicateOrderIntentError(f"duplicate intent_id: {record['intent_id']}")
            self._append_record_unlocked(record)
        return record

    def record_accepted_order(self, intent_id: str, accepted_order: Mapping[str, Any]) -> dict[str, Any]:
        intent_id = _require_non_empty_string(intent_id, "intent_id")
        accepted = _normalize_accepted_order(accepted_order)
        with self._exclusive_lock():
            records = self._read_records_unlocked()
            latest = self._latest_by_id(records).get(intent_id)
            if latest is None:
                raise InvalidOrderIntentError(f"unknown intent_id: {intent_id}")
            old_status = latest["status"]
            if "SUBMITTED" not in ALLOWED_TRANSITIONS[old_status]:
                raise InvalidOrderIntentError(f"invalid status transition: {old_status}->SUBMITTED")
            if accepted["ticker"] != latest["ticker"]:
                raise InvalidOrderIntentError("accepted_order ticker must match intent ticker")
            updated = dict(latest)
            updated["status"] = "SUBMITTED"
            updated["accepted_order"] = accepted
            updated["created_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            self._append_record_unlocked(updated)
        return updated

    def transition_status(self, intent_id: str, new_status: str) -> dict[str, Any]:
        intent_id = _require_non_empty_string(intent_id, "intent_id")
        new_status = _require_non_empty_string(new_status, "status").upper()
        if new_status not in ALLOWED_TRANSITIONS:
            raise InvalidOrderIntentError(f"invalid status: {new_status}")
        with self._exclusive_lock():
            records = self._read_records_unlocked()
            latest = self._latest_by_id(records).get(intent_id)
            if latest is None:
                raise InvalidOrderIntentError(f"unknown intent_id: {intent_id}")
            old_status = latest["status"]
            if new_status not in ALLOWED_TRANSITIONS[old_status]:
                raise InvalidOrderIntentError(f"invalid status transition: {old_status}->{new_status}")
            updated = dict(latest)
            updated["status"] = new_status
            updated["created_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            self._append_record_unlocked(updated)
        return updated

    def list_intents(self, ticker: str | None = None) -> list[dict[str, Any]]:
        with self._shared_lock():
            records = self._read_records_unlocked()
        if ticker is None:
            return records
        wanted = ticker.upper()
        return [record for record in records if record["ticker"] == wanted]

    def _assert_current_t_revision(self, ticker: str, t_revision: int) -> None:
        if self.current_t_revision_provider is None:
            raise OrderIntentLedgerCorruptError("current T revision provider missing")
        try:
            current = _parse_positive_int(self.current_t_revision_provider(ticker), "current_t_revision")
        except InvalidOrderIntentError:
            raise
        except Exception as exc:
            raise OrderIntentLedgerCorruptError("current T revision unavailable") from exc
        if t_revision != current:
            raise StaleTRevisionError(f"stale t_revision: expected {current}, got {t_revision}")

    def _read_records_unlocked(self) -> list[dict[str, Any]]:
        if not self.ledger_path.exists():
            raise OrderIntentLedgerCorruptError(f"order intent ledger missing: {self.ledger_path}")
        records: list[dict[str, Any]] = []
        with self.ledger_path.open("r", encoding="utf-8") as f:
            for line_number, line in enumerate(f, start=1):
                if line == "":
                    continue
                if not line.endswith("\n"):
                    raise OrderIntentLedgerCorruptError(f"broken unterminated JSONL line at {line_number}")
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    raw = json.loads(stripped)
                except json.JSONDecodeError as exc:
                    raise OrderIntentLedgerCorruptError(f"invalid JSONL line at {line_number}") from exc
                try:
                    records.append(self._validate_record(raw))
                except InvalidOrderIntentError as exc:
                    raise OrderIntentLedgerCorruptError(f"invalid order intent line {line_number}: {exc}") from exc
        self._validate_sequence(records)
        return records

    def _validate_record(self, raw: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(raw, Mapping):
            raise InvalidOrderIntentError("ledger record must be a mapping")
        raw_keys = set(raw.keys())
        expected_keys = set(LEDGER_FIELDS)
        accepted_keys = expected_keys | {"accepted_order"}
        if raw_keys != expected_keys and raw_keys != accepted_keys:
            raise InvalidOrderIntentError("ledger schema keys must exactly match LEDGER_FIELDS")
        normalized = _normalize_plan(raw)
        record = dict(normalized)
        intent_id = _require_non_empty_string(raw.get("intent_id"), "intent_id")
        expected_id = compute_intent_id(normalized)
        if intent_id != expected_id:
            raise InvalidOrderIntentError("intent_id does not match canonical order intent")
        status = _require_non_empty_string(raw.get("status"), "status").upper()
        if status not in ALLOWED_TRANSITIONS:
            raise InvalidOrderIntentError(f"invalid status: {status}")
        record["intent_id"] = intent_id
        record["status"] = status
        record["created_at"] = _require_iso8601(raw.get("created_at"), "created_at")
        if "accepted_order" in raw:
            record["accepted_order"] = _normalize_accepted_order(raw.get("accepted_order"))
        return record

    def _validate_sequence(self, records: list[dict[str, Any]]) -> None:
        seen: dict[str, str] = {}
        for record in records:
            intent_id = record["intent_id"]
            status = record["status"]
            if intent_id not in seen:
                if status != "PLANNED":
                    raise OrderIntentLedgerCorruptError("first intent record must be PLANNED")
                seen[intent_id] = status
                continue
            previous = seen[intent_id]
            if status not in ALLOWED_TRANSITIONS[previous]:
                raise OrderIntentLedgerCorruptError(f"invalid status transition in ledger: {previous}->{status}")
            seen[intent_id] = status

    @staticmethod
    def _latest_by_id(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        latest: dict[str, dict[str, Any]] = {}
        for record in records:
            latest[record["intent_id"]] = record
        return latest

    def _append_record_unlocked(self, record: Mapping[str, Any]) -> None:
        payload = json.dumps(dict(record), ensure_ascii=False, separators=(",", ":")) + "\n"
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.ledger_path), os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o644)
        try:
            os.write(fd, payload.encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)
        self._fsync_dir(self.ledger_path.parent)

    def _exclusive_lock(self):
        return _FileLock(self.lock_path, fcntl.LOCK_EX)

    def _shared_lock(self):
        return _FileLock(self.lock_path, fcntl.LOCK_SH)

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        try:
            dir_fd = os.open(str(path), os.O_DIRECTORY | os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)


class _FileLock:
    def __init__(self, path: Path, mode: int):
        self.path = path
        self.mode = mode
        self.file = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = self.path.open("a+", encoding="utf-8")
        fcntl.flock(self.file.fileno(), self.mode)
        return self.file

    def __exit__(self, exc_type, exc, tb):
        assert self.file is not None
        try:
            fcntl.flock(self.file.fileno(), fcntl.LOCK_UN)
        finally:
            self.file.close()
        return False
