"""Durable, process-safe recorder for fail-closed SHADOW order intents."""

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import fcntl
import hashlib
import json
import os
from pathlib import Path
import threading
import uuid


DEFAULT_SHADOW_INTENT_PATH = Path(__file__).resolve().parent / "data" / "shadow_intents.jsonl"


class ShadowJournalCorrupt(OSError):
    """Raised when an existing journal cannot be safely appended."""

    code = "SHADOW_JOURNAL_CORRUPT"


class IdempotencyConflict(OSError):
    """Raised when a retry key is reused for a different canonical payload."""

    code = "IDEMPOTENCY_CONFLICT"


def _decimal_text(value):
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("invalid shadow intent decimal") from exc
    if not decimal_value.is_finite():
        raise ValueError("non-finite shadow intent decimal")
    return format(decimal_value, "f")


def _fsync_directory(path):
    directory_fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


class ShadowIntentRecorder:
    """Append schema-v2 intents with process-safe durability and retry idempotency."""

    def __init__(self, path=DEFAULT_SHADOW_INTENT_PATH):
        self.path = Path(path)
        self.lock_path = self.path.with_name(f"{self.path.name}.lock")
        self._lock = threading.Lock()

    @staticmethod
    def _read_records(fd, original_offset):
        if original_offset == 0:
            return []
        os.lseek(fd, 0, os.SEEK_SET)
        raw = b""
        while len(raw) < original_offset:
            chunk = os.read(fd, min(65536, original_offset - len(raw)))
            if not chunk:
                break
            raw += chunk
        if len(raw) != original_offset or not raw.endswith(b"\n"):
            raise ShadowJournalCorrupt("shadow journal has an incomplete tail")
        records = []
        try:
            for line in raw.splitlines():
                record = json.loads(line.decode("utf-8"))
                if not isinstance(record, dict):
                    raise ValueError("record is not an object")
                records.append(record)
        except (UnicodeError, json.JSONDecodeError, ValueError) as exc:
            raise ShadowJournalCorrupt("shadow journal contains invalid JSON") from exc
        return records

    @staticmethod
    def _write_all(fd, encoded):
        position = 0
        while position < len(encoded):
            written = os.write(fd, encoded[position:])
            if written <= 0:
                raise OSError("short shadow intent append")
            position += written

    def record(
        self,
        *,
        ticker,
        side,
        quantity,
        price,
        order_type,
        safety_revision,
        risk_reference_price=None,
        idempotency_key=None,
    ):
        occurrence_key = str(idempotency_key or uuid.uuid4().hex).strip()
        if not occurrence_key:
            raise ValueError("idempotency_key must be non-empty")
        stable = {
            "schema_version": 2,
            "ticker": str(ticker or "").strip().upper(),
            "side": str(side or "").strip().upper(),
            "quantity": _decimal_text(quantity),
            "price": _decimal_text(price),
            "risk_reference_price": (
                None if risk_reference_price is None else _decimal_text(risk_reference_price)
            ),
            "order_type": str(order_type or "").strip().upper(),
            "safety_revision": safety_revision,
            "status": "SHADOW_RECORDED",
        }
        canonical = json.dumps(
            {**stable, "idempotency_key": occurrence_key},
            sort_keys=True,
            separators=(",", ":"),
        )
        intent_id = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            lock_fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
            journal_fd = None
            locked = False
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX)
                locked = True
                journal_fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
                original_offset = os.lseek(journal_fd, 0, os.SEEK_END)
                records = self._read_records(journal_fd, original_offset)
                for existing in records:
                    if existing.get("idempotency_key") != occurrence_key:
                        continue
                    if existing.get("intent_id") == intent_id:
                        return existing
                    raise IdempotencyConflict(
                        "idempotency key was already used for a different payload"
                    )

                record = {
                    **stable,
                    "idempotency_key": occurrence_key,
                    "intent_id": intent_id,
                    "event_id": uuid.uuid4().hex,
                    "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                }
                encoded = (
                    json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
                ).encode("utf-8")
                os.lseek(journal_fd, original_offset, os.SEEK_SET)
                try:
                    self._write_all(journal_fd, encoded)
                    os.fsync(journal_fd)
                    _fsync_directory(self.path.parent)
                except BaseException:
                    os.ftruncate(journal_fd, original_offset)
                    os.fsync(journal_fd)
                    raise
                return record
            finally:
                if journal_fd is not None:
                    os.close(journal_fd)
                if locked:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)
