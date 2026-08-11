"""Immutable baseline + append-only T event ledger store.

This module performs only local file IO against caller-provided paths.  It does
not contact KIS, Telegram, Docker, or the network.
"""

from __future__ import annotations

import fcntl
import json
import os
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from t_event_engine import TState, apply_t_event, parse_decimal, validate_t_event


class BaselineValidationError(ValueError):
    pass


class BaselineImmutableError(BaselineValidationError):
    pass


class TEventLedgerCorruptError(RuntimeError):
    pass


class DuplicateTEventError(ValueError):
    pass


BASELINE_KEYS = (
    "schema_version",
    "ticker",
    "as_of",
    "qty",
    "avg_price",
    "available_cash",
    "t",
    "reverse_active",
    "source",
    "legacy_execution_count",
    "immutable",
)
BASELINE_KEY_SET = set(BASELINE_KEYS)


class TradeStateStore:
    def __init__(self, baseline_path: str | os.PathLike[str], events_path: str | os.PathLike[str]):
        self.baseline_path = Path(baseline_path)
        self.events_path = Path(events_path)
        self.lock_path = self.events_path.with_suffix(self.events_path.suffix + ".lock")

    def load_baseline(self, ticker: str) -> dict[str, Any]:
        try:
            with self.baseline_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError as exc:
            raise BaselineValidationError(f"baseline missing: {self.baseline_path}") from exc
        except json.JSONDecodeError as exc:
            raise BaselineValidationError(f"baseline is invalid JSON: {self.baseline_path}") from exc

        return self._validate_baseline(data, ticker)

    def _validate_baseline(self, data: Mapping[str, Any], ticker: str) -> dict[str, Any]:
        if not isinstance(data, Mapping):
            raise BaselineValidationError("baseline must be a JSON object")
        keys = set(data.keys())
        if keys != BASELINE_KEY_SET:
            missing = sorted(BASELINE_KEY_SET - keys)
            extra = sorted(keys - BASELINE_KEY_SET)
            raise BaselineValidationError(f"baseline schema mismatch missing={missing} extra={extra}")
        if data["immutable"] is not True:
            raise BaselineImmutableError("baseline immutable must be true")
        if data["schema_version"] != 1:
            raise BaselineValidationError("schema_version must be 1")
        if data["ticker"] != ticker or data["ticker"] != str(data["ticker"]).upper():
            raise BaselineValidationError("baseline ticker mismatch")
        if data["source"] != "CEO_APPROVED_KIS_BASELINE":
            raise BaselineValidationError("baseline source mismatch")
        if not isinstance(data["as_of"], str) or not data["as_of"]:
            raise BaselineValidationError("as_of must be a non-empty string")
        if not isinstance(data["qty"], int) or isinstance(data["qty"], bool) or data["qty"] < 0:
            raise BaselineValidationError("qty must be a non-negative integer")
        if not isinstance(data["legacy_execution_count"], int) or isinstance(data["legacy_execution_count"], bool):
            raise BaselineValidationError("legacy_execution_count must be an integer")
        if not isinstance(data["reverse_active"], bool):
            raise BaselineValidationError("reverse_active must be boolean")
        for field in ("avg_price", "available_cash", "t"):
            if not isinstance(data[field], str):
                raise BaselineValidationError(f"{field} must be a string Decimal")
            try:
                parse_decimal(data[field], field)
            except ValueError as exc:
                raise BaselineValidationError(str(exc)) from exc
        return dict(data)

    def load_state(self, ticker: str) -> TState:
        baseline = self.load_baseline(ticker)
        events = self._read_events_locked(ticker)
        return self._state_from_events(baseline, events)

    def append_event(self, event: Mapping[str, Any]) -> TState:
        validated = validate_t_event(event)
        ticker = validated["ticker"]
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                baseline = self.load_baseline(ticker)
                events = self._read_events_unlocked(ticker)
                if any(existing["event_id"] == validated["event_id"] for existing in events):
                    raise DuplicateTEventError(f"duplicate event_id: {validated['event_id']}")
                if any(existing["fill_key"] == validated["fill_key"] for existing in events):
                    raise DuplicateTEventError(f"duplicate fill_key: {validated['fill_key']}")
                current = self._state_from_events(baseline, events)
                new_state = apply_t_event(current.t, current.revision, validated)
                new_state = TState(
                    ticker=new_state.ticker,
                    t=new_state.t,
                    revision=new_state.revision,
                    available_cash=current.available_cash,
                    reverse_active=current.reverse_active,
                )
                payload = json.dumps(validated, ensure_ascii=False, separators=(",", ":")) + "\n"
                self._atomic_append_line(payload)
                return new_state
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def _read_events_locked(self, ticker: str) -> list[dict[str, Any]]:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_SH)
            try:
                return self._read_events_unlocked(ticker)
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def _read_events_unlocked(self, ticker: str) -> list[dict[str, Any]]:
        if not self.events_path.exists():
            return []
        events: list[dict[str, Any]] = []
        with self.events_path.open("r", encoding="utf-8") as f:
            for line_number, line in enumerate(f, start=1):
                if line == "":
                    continue
                if not line.endswith("\n"):
                    raise TEventLedgerCorruptError(f"broken unterminated JSONL line at {line_number}")
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError as exc:
                    raise TEventLedgerCorruptError(f"invalid JSONL line at {line_number}") from exc
                try:
                    validated = validate_t_event(parsed)
                except ValueError as exc:
                    raise TEventLedgerCorruptError(f"invalid T event line {line_number}: {exc}") from exc
                if validated["ticker"] == ticker:
                    events.append(validated)
        event_ids = set()
        fill_keys = set()
        for existing in events:
            if existing["event_id"] in event_ids:
                raise DuplicateTEventError(f"duplicate event_id in ledger: {existing['event_id']}")
            if existing["fill_key"] in fill_keys:
                raise DuplicateTEventError(f"duplicate fill_key in ledger: {existing['fill_key']}")
            event_ids.add(existing["event_id"])
            fill_keys.add(existing["fill_key"])
        return events

    def _state_from_events(self, baseline: Mapping[str, Any], events: list[dict[str, Any]]) -> TState:
        state = TState(
            ticker=str(baseline["ticker"]),
            t=parse_decimal(baseline["t"], "t"),
            revision=1,
            available_cash=parse_decimal(baseline["available_cash"], "available_cash"),
            reverse_active=bool(baseline["reverse_active"]),
        )
        for event in events:
            applied = apply_t_event(state.t, state.revision, event)
            state = TState(
                ticker=applied.ticker,
                t=applied.t,
                revision=applied.revision,
                available_cash=state.available_cash,
                reverse_active=state.reverse_active,
            )
        return state

    def _atomic_append_line(self, payload: str) -> None:
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        existing = b""
        if self.events_path.exists():
            with self.events_path.open("rb") as src:
                existing = src.read()
        temp_path = self.events_path.with_name(f".{self.events_path.name}.tmp.{os.getpid()}")
        fd = os.open(str(temp_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        try:
            if existing:
                os.write(fd, existing)
            os.write(fd, payload.encode("utf-8"))
            os.fsync(fd)
        except Exception:
            os.close(fd)
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass
            raise
        else:
            os.close(fd)
        try:
            os.replace(str(temp_path), str(self.events_path))
            self._fsync_dir(self.events_path.parent)
        finally:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass

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
