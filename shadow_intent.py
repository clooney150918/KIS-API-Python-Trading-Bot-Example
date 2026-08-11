"""Minimal append-only recorder for fail-closed SHADOW order intents."""

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import threading


DEFAULT_SHADOW_INTENT_PATH = Path(__file__).resolve().parent / "data" / "shadow_intents.jsonl"


def _decimal_text(value):
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("invalid shadow intent decimal") from exc
    if not decimal_value.is_finite():
        raise ValueError("non-finite shadow intent decimal")
    return format(decimal_value, "f")


class ShadowIntentRecorder:
    """Append schema-v1 intents without account or credential material."""

    def __init__(self, path=DEFAULT_SHADOW_INTENT_PATH):
        self.path = Path(path)
        self._lock = threading.Lock()

    def record(self, *, ticker, side, quantity, price, order_type, safety_revision):
        stable = {
            "schema_version": 1,
            "ticker": str(ticker or "").strip().upper(),
            "side": str(side or "").strip().upper(),
            "quantity": _decimal_text(quantity),
            "price": _decimal_text(price),
            "order_type": str(order_type or "").strip().upper(),
            "safety_revision": safety_revision,
            "status": "SHADOW_RECORDED",
        }
        canonical = json.dumps(stable, sort_keys=True, separators=(",", ":"))
        record = {
            **stable,
            "intent_id": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        encoded = (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode(
            "utf-8"
        )
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            try:
                written = os.write(fd, encoded)
                if written != len(encoded):
                    raise OSError("short shadow intent append")
                os.fsync(fd)
            finally:
                os.close(fd)
        return record
