"""KIS fill reconciliation before applying Laoer V4 T events.

KIS ``rt_cd == 0`` means only that an order was accepted.  This module is the
separate confirmation boundary: only KIS execution rows that reconcile to one
complete official intent can append a T event.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from order_intent_store import OrderIntentLedgerCorruptError
from t_event_engine import apply_fill_event_extended


class FillReconciliationError(RuntimeError):
    pass


class ProcessedFillLedgerCorruptError(RuntimeError):
    pass


def _text(value: Any) -> str:
    return str(value or "").strip()


def _decimal(value: Any, field: str) -> Decimal:
    try:
        parsed = Decimal(str(value).replace(",", ""))
    except (InvalidOperation, ValueError) as exc:
        raise FillReconciliationError(f"{field} must be a finite decimal") from exc
    if not parsed.is_finite():
        raise FillReconciliationError(f"{field} must be a finite decimal")
    return parsed


def _positive_int(value: Any, field: str) -> int:
    parsed = _decimal(value, field)
    if parsed <= 0 or parsed != parsed.to_integral_value():
        raise FillReconciliationError(f"{field} must be a positive integer")
    return int(parsed)


def _side_from_kis(value: Any) -> str:
    text = _text(value)
    if text in {"02", "2", "BUY", "매수"}:
        return "BUY"
    if text in {"01", "1", "SELL", "매도"}:
        return "SELL"
    raise FillReconciliationError(f"unknown KIS side: {value!r}")


def normalize_kis_execution(row: Mapping[str, Any], *, account_fingerprint: str) -> dict[str, Any]:
    if not isinstance(row, Mapping):
        raise FillReconciliationError("KIS execution row must be a mapping")
    order_no = _text(row.get("odno") or row.get("ODNO"))
    if not order_no:
        raise FillReconciliationError("KIS execution row missing order number")
    trade_date = _text(row.get("ord_dt") or row.get("trade_date"))
    execution_time = _text(row.get("ord_tmd") or row.get("execution_time"))
    ticker = _text(row.get("pdno") or row.get("ovrs_pdno") or row.get("ticker")).upper()
    exchange = _text(row.get("ovrs_excg_cd") or row.get("exchange") or "AMEX").upper()
    qty = _positive_int(row.get("ft_ccld_qty") or row.get("qty"), "ft_ccld_qty")
    price = _decimal(row.get("ft_ccld_unpr3") or row.get("price"), "ft_ccld_unpr3")
    order_price = None
    for key in ("ord_unpr", "ovrs_ord_unpr", "ft_ord_unpr3", "ft_ord_unpr", "order_price"):
        raw = row.get(key)
        if raw not in (None, ""):
            order_price = _decimal(raw, key)
            break
    normalized = {
        "account_fingerprint": _text(account_fingerprint),
        "ticker": ticker,
        "exchange": exchange,
        "trade_date": trade_date,
        "order_no": order_no,
        "execution_time": execution_time,
        "side": _side_from_kis(row.get("sll_buy_dvsn_cd") or row.get("side")),
        "qty": qty,
        "price": price,
        "amount": price * Decimal(qty),
    }
    if order_price is not None:
        normalized["order_price"] = order_price
    return normalized


def _money_text(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.01")), "f")


def build_fill_key(fill: Mapping[str, Any]) -> str:
    return "|".join([
        _text(fill.get("account_fingerprint")),
        _text(fill.get("ticker")).upper(),
        _text(fill.get("exchange")).upper(),
        _text(fill.get("trade_date")),
        _text(fill.get("order_no")),
        _text(fill.get("execution_time")),
        _text(fill.get("side")).upper(),
        str(int(fill.get("qty"))),
        _money_text(fill.get("price") if isinstance(fill.get("price"), Decimal) else _decimal(fill.get("price"), "price")),
    ])


def build_matching_key(fill: Mapping[str, Any]) -> str:
    return "|".join([
        _text(fill.get("account_fingerprint")),
        _text(fill.get("ticker")).upper(),
        _text(fill.get("exchange")).upper(),
        _text(fill.get("trade_date")),
        _text(fill.get("order_no")),
    ])


class ProcessedFillStore:
    def __init__(self, ledger_path: str | os.PathLike[str]):
        self.ledger_path = Path(ledger_path)
        self.lock_path = self.ledger_path.with_suffix(self.ledger_path.suffix + ".lock")

    def list_records(self) -> list[dict[str, Any]]:
        with self._lock(fcntl.LOCK_SH):
            return self._read_unlocked()

    def append_record(self, record: Mapping[str, Any]) -> None:
        payload = json.dumps(dict(record), ensure_ascii=False, separators=(",", ":")) + "\n"
        with self._lock(fcntl.LOCK_EX):
            records = self._read_unlocked()
            same_fill_records = [r for r in records if r.get("fill_key") == record.get("fill_key")]
            latest = same_fill_records[-1] if same_fill_records else None
            if latest is not None and not self._allows_reclassification(latest, record):
                return
            self._atomic_append_line(payload)

    @staticmethod
    def _allows_reclassification(existing: Mapping[str, Any], new_record: Mapping[str, Any]) -> bool:
        existing_classification = str(existing.get("classification") or "")
        new_classification = str(new_record.get("classification") or "")
        if existing_classification == new_classification:
            return False
        if existing_classification == "FINAL" and new_classification == "FINALIZATION_FAILED":
            return True
        if existing_classification == "FINALIZATION_FAILED" and new_classification == "FINAL":
            return True
        if existing_classification in {"UNCLASSIFIED", "UNCLASSIFIED_AFTER_FILLED"} and new_classification in {"PARTIAL", "FINAL"}:
            return True
        if existing_classification == "PARTIAL" and new_classification == "FINAL":
            return True
        return False

    def has_fill_key(self, fill_key: str) -> bool:
        return any(r.get("fill_key") == fill_key for r in self.list_records())

    def latest_record_for_fill_key(self, fill_key: str) -> dict[str, Any] | None:
        latest = None
        for record in self.list_records():
            if record.get("fill_key") == fill_key:
                latest = record
        return latest

    def _read_unlocked(self) -> list[dict[str, Any]]:
        if not self.ledger_path.exists():
            raise ProcessedFillLedgerCorruptError(f"processed fill ledger missing: {self.ledger_path}")
        out: list[dict[str, Any]] = []
        with self.ledger_path.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                if not line.strip():
                    continue
                if not line.endswith("\n"):
                    raise ProcessedFillLedgerCorruptError(f"unterminated JSONL at {line_no}")
                try:
                    parsed = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ProcessedFillLedgerCorruptError(f"invalid JSONL at {line_no}") from exc
                out.append(parsed)
        return out

    def _atomic_append_line(self, payload: str) -> None:
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.ledger_path), os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o644)
        try:
            os.write(fd, payload.encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)
        _fsync_dir(self.ledger_path.parent)

    def _lock(self, mode: int):
        return _FileLock(self.lock_path, mode)


class FillReconciler:
    def __init__(
        self,
        intent_store,
        trade_state_store,
        processed_fill_store: ProcessedFillStore,
        *,
        account_fingerprint: str,
        split_amount_provider=None,
    ):
        self.intent_store = intent_store
        self.trade_state_store = trade_state_store
        self.processed_fill_store = processed_fill_store
        self.account_fingerprint = account_fingerprint
        self.split_amount_provider = split_amount_provider
        self.tx_lock_path = processed_fill_store.ledger_path.with_suffix(".reconcile.lock")

    def forbid_new_orders(self, ticker: str) -> bool:
        ticker = _text(ticker).upper()
        try:
            latest = _latest_by_intent(self.intent_store.list_intents(ticker))
            processed = self.processed_fill_store.list_records()
            t_event_fill_keys = self._t_event_fill_keys(ticker)
        except (OrderIntentLedgerCorruptError, ProcessedFillLedgerCorruptError):
            return True
        if any(record.get("status") == "PARTIAL" for record in latest.values()):
            return True
        durable_halt_classifications = {
            "UNCLASSIFIED",
            "UNCLASSIFIED_AFTER_FILLED",
            "CANCELLED_PARTIAL_REMAINS",
            "FINALIZATION_FAILED",
        }
        final_fill_keys = {
            record.get("fill_key")
            for record in processed
            if _text(record.get("ticker")).upper() == ticker
            and record.get("classification") == "FINAL"
        }
        return any(
            _text(record.get("ticker")).upper() == ticker
            and record.get("classification") in durable_halt_classifications
            and record.get("fill_key") not in t_event_fill_keys
            and record.get("fill_key") not in final_fill_keys
            for record in processed
        )

    def reconcile(self, ticker: str, kis_execution_rows: list[Mapping[str, Any]]) -> dict[str, Any]:
        ticker = _text(ticker).upper()
        result = {"new_fill_count": 0, "operator_halt": False, "partial_open": False, "codes": []}
        normalized = [normalize_kis_execution(row, account_fingerprint=self.account_fingerprint) for row in kis_execution_rows]
        with _FileLock(self.tx_lock_path, fcntl.LOCK_EX):
            for fill in normalized:
                fill_key = build_fill_key(fill)
                existing_processed = self.processed_fill_store.latest_record_for_fill_key(fill_key)
                t_event_fill_keys = self._t_event_fill_keys(ticker)
                intent = self._match_intent(ticker, fill)
                if fill_key in t_event_fill_keys:
                    if intent is not None and existing_processed is None:
                        self._record_processed(fill, intent["intent_id"], "FINAL")
                    if intent is not None and intent.get("status") in {"SUBMITTED", "PARTIAL"}:
                        self.intent_store.transition_status(intent["intent_id"], "FILLED")
                    continue
                if existing_processed is not None and intent is None:
                    continue
                if (
                    existing_processed is not None
                    and existing_processed.get("classification") in {"PARTIAL", "FINAL"}
                ):
                    continue
                if intent is None:
                    self._record_processed(fill, None, "UNCLASSIFIED")
                    result["new_fill_count"] += 1
                    result["operator_halt"] = True
                    _add_code(result, "UNCLASSIFIED_FILL")
                    continue

                status = intent["status"]
                if status == "CANCELLED":
                    self._record_processed(fill, intent["intent_id"], "CANCELLED_PARTIAL_REMAINS")
                    result["new_fill_count"] += 1
                    result["operator_halt"] = True
                    _add_code(result, "CANCELLED_PARTIAL_REMAINS")
                    continue
                if status == "FILLED":
                    # A new row after terminal FILLED cannot safely mutate T again.
                    self._record_processed(fill, intent["intent_id"], "UNCLASSIFIED_AFTER_FILLED")
                    result["new_fill_count"] += 1
                    result["operator_halt"] = True
                    _add_code(result, "UNCLASSIFIED_FILL")
                    continue

                if not self._fill_within_intent(intent, fill):
                    self._record_processed(fill, intent["intent_id"], "UNCLASSIFIED")
                    result["new_fill_count"] += 1
                    result["operator_halt"] = True
                    _add_code(result, "UNCLASSIFIED_FILL")
                    continue

                accumulated_qty, accumulated_amount = self._accumulated_for_intent(intent["intent_id"])
                accumulated_qty += int(fill["qty"])
                accumulated_amount += fill["amount"]

                if accumulated_qty < int(intent["qty"]):
                    self._record_processed(fill, intent["intent_id"], "PARTIAL")
                    result["new_fill_count"] += 1
                    if status == "SUBMITTED":
                        self.intent_store.transition_status(intent["intent_id"], "PARTIAL")
                    result["operator_halt"] = True
                    result["partial_open"] = True
                    _add_code(result, "PARTIAL_FILL_OPEN")
                    continue

                if accumulated_qty != int(intent["qty"]):
                    self._record_processed(fill, intent["intent_id"], "UNCLASSIFIED")
                    result["new_fill_count"] += 1
                    result["operator_halt"] = True
                    _add_code(result, "UNCLASSIFIED_FILL")
                    continue

                event = self._build_t_event(intent, fill, accumulated_qty, accumulated_amount)
                snapshots = self._snapshot_ledgers()
                try:
                    self.trade_state_store.append_event(event)
                    self._record_processed(fill, intent["intent_id"], "FINAL")
                    self.intent_store.transition_status(intent["intent_id"], "FILLED")
                except Exception as exc:
                    self._rollback_finalization(fill, event, snapshots)
                    raise FillReconciliationError("atomic fill finalization failed; retry required") from exc
                result["new_fill_count"] += 1
            result["partial_open"] = self.forbid_new_orders(ticker)
            if result["partial_open"]:
                result["operator_halt"] = True
                _add_code(result, "RECONCILIATION_HALT_OPEN")
        return result

    def _match_intent(self, ticker: str, fill: Mapping[str, Any]) -> dict[str, Any] | None:
        latest = list(_latest_by_intent(self.intent_store.list_intents(ticker)).values())
        fill_matching_key = build_matching_key(fill)
        candidates = []
        for intent in latest:
            if intent.get("ticker") != ticker:
                continue
            if intent.get("status") not in {"SUBMITTED", "PARTIAL", "CANCELLED", "FILLED"}:
                continue
            if intent.get("side") != fill.get("side"):
                continue
            accepted_order = intent.get("accepted_order")
            if not isinstance(accepted_order, Mapping):
                continue
            if accepted_order.get("matching_key") != fill_matching_key:
                continue
            candidates.append(intent)
        if len(candidates) == 1:
            return candidates[0]
        if candidates:
            return None

        return self._match_intent_by_order_terms(ticker, fill, latest)

    def _match_intent_by_order_terms(self, ticker: str, fill: Mapping[str, Any], latest: list[dict[str, Any]]) -> dict[str, Any] | None:
        order_price = fill.get("order_price")
        if order_price is None:
            return None
        fill_prefix = "|".join([
            _text(fill.get("account_fingerprint")),
            _text(fill.get("ticker")).upper(),
            _text(fill.get("exchange")).upper(),
            _text(fill.get("trade_date")),
        ])
        candidates = []
        for intent in latest:
            if intent.get("ticker") != ticker:
                continue
            if intent.get("status") not in {"SUBMITTED", "PARTIAL"}:
                continue
            if intent.get("side") != fill.get("side"):
                continue
            accepted_order = intent.get("accepted_order")
            if not isinstance(accepted_order, Mapping):
                continue
            accepted_prefix = "|".join([
                _text(accepted_order.get("account_fingerprint")),
                _text(accepted_order.get("ticker")).upper(),
                _text(accepted_order.get("exchange")).upper(),
                _text(accepted_order.get("trade_date")).replace("-", ""),
            ])
            if accepted_prefix != fill_prefix:
                continue
            try:
                intent_price = _decimal(intent.get("price"), "intent.price")
            except FillReconciliationError:
                continue
            if abs(intent_price - order_price) > Decimal("0.01"):
                continue
            if not self._fill_within_intent(intent, fill):
                continue
            candidates.append(intent)
        return candidates[0] if len(candidates) == 1 else None

    def _fill_within_intent(self, intent: Mapping[str, Any], fill: Mapping[str, Any]) -> bool:
        if intent.get("side") != fill.get("side"):
            return False
        if int(fill["qty"]) > int(intent["qty"]):
            return False
        intent_price = _decimal(intent.get("price"), "intent.price")
        fill_price = fill["price"]
        if intent.get("side") == "BUY" and fill_price > intent_price:
            return False
        if intent.get("side") == "SELL" and fill_price < intent_price and intent_price > 0:
            return False
        return True

    def _accumulated_for_intent(self, intent_id: str) -> tuple[int, Decimal]:
        qty = 0
        amount = Decimal("0")
        for record in self.processed_fill_store.list_records():
            if record.get("intent_id") == intent_id and record.get("classification") in {"PARTIAL", "FINAL"}:
                qty += int(record["qty"])
                amount += _decimal(record["amount"], "processed.amount")
        return qty, amount

    def _t_event_fill_keys(self, ticker: str) -> set[str]:
        events_path = Path(self.trade_state_store.events_path)
        if not events_path.exists():
            return set()
        keys: set[str] = set()
        with events_path.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if _text(event.get("ticker")).upper() == ticker and event.get("fill_key"):
                    keys.add(str(event["fill_key"]))
        return keys

    def _build_t_event(self, intent: Mapping[str, Any], fill: Mapping[str, Any], qty: int, amount: Decimal) -> dict[str, Any]:
        event_type = str(intent["event_type"])
        t_state = self.trade_state_store.load_state(intent["ticker"])
        split_amount = self._split_amount_for(intent["ticker"])
        # filled_amount is persisted rounded to 2 decimals, so T must advance
        # from that SAME rounded value.  Computing t_after from the unrounded
        # amount would leave the stored (t_after, filled_amount) pair unable to
        # reproduce each other on replay, breaking the closed-loop invariant.
        amount_text = _money_text(amount)
        rounded_amount = Decimal(amount_text)
        t_after = apply_fill_event_extended(
            t_state.t,
            event_type,
            side=fill["side"],
            filled_amount=rounded_amount,
            split_amount=split_amount,
        )
        fill_key = build_fill_key(fill)
        event_id = hashlib.sha256(
            f"{intent['intent_id']}|{fill_key}|{t_state.revision}".encode("utf-8")
        ).hexdigest()
        return {
            "event_id": event_id,
            "ticker": intent["ticker"],
            "intent_id": intent["intent_id"],
            "kis_order_no": fill["order_no"],
            "fill_key": fill_key,
            "event_type": event_type,
            "side": fill["side"],
            "filled_qty": qty,
            "filled_amount": amount_text,
            "split_amount": format(split_amount, "f"),
            "t_before": str(t_state.t),
            "t_after": str(t_after),
            "revision_before": t_state.revision,
            "revision_after": t_state.revision + 1,
            "occurred_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }

    def _split_amount_for(self, ticker: str) -> Decimal:
        if not callable(self.split_amount_provider):
            raise FillReconciliationError("split_amount_provider is required for proportional T")
        try:
            split_amount = _decimal(self.split_amount_provider(ticker), "split_amount")
        except Exception as exc:
            raise FillReconciliationError("split_amount_provider returned invalid split amount") from exc
        if split_amount <= 0:
            raise FillReconciliationError("split_amount must be positive")
        return split_amount

    def _record_processed(self, fill: Mapping[str, Any], intent_id: str | None, classification: str) -> None:
        self.processed_fill_store.append_record({
            "fill_key": build_fill_key(fill),
            "matching_key": build_matching_key(fill),
            "account_fingerprint": fill["account_fingerprint"],
            "ticker": fill["ticker"],
            "exchange": fill["exchange"],
            "trade_date": fill["trade_date"],
            "order_no": fill["order_no"],
            "execution_time": fill["execution_time"],
            "side": fill["side"],
            "qty": int(fill["qty"]),
            "price": _money_text(fill["price"]),
            **({"order_price": _money_text(fill["order_price"])} if fill.get("order_price") is not None else {}),
            "amount": _money_text(fill["amount"]),
            "intent_id": intent_id,
            "classification": classification,
            "processed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        })

    def _snapshot_ledgers(self) -> dict[str, bytes]:
        return {
            "t_events": _read_bytes(Path(self.trade_state_store.events_path)),
            "processed_fills": _read_bytes(self.processed_fill_store.ledger_path),
            "order_intents": _read_bytes(Path(self.intent_store.ledger_path)),
        }

    def _rollback_finalization(self, fill: Mapping[str, Any], event: Mapping[str, Any], snapshots: Mapping[str, bytes]) -> None:
        fill_key = build_fill_key(fill)
        _remove_jsonl_records(
            Path(self.trade_state_store.events_path),
            lambda record: record.get("event_id") == event.get("event_id") or record.get("fill_key") == fill_key,
        )
        _remove_jsonl_records(
            self.processed_fill_store.ledger_path,
            lambda record: record.get("fill_key") == fill_key and record.get("classification") == "FINAL",
        )


def _latest_by_intent(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for record in records:
        latest[record["intent_id"]] = record
    return latest


def _trade_date_compact(value: Any) -> str:
    return _text(value).replace("-", "")


def _add_code(result: dict[str, Any], code: str) -> None:
    if code not in result["codes"]:
        result["codes"].append(code)


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(str(path), os.O_DIRECTORY | os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _read_bytes(path: Path) -> bytes:
    if not path.exists():
        return b""
    return path.read_bytes()


def _write_bytes_durably(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o644)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)
    _fsync_dir(path.parent)


def _remove_jsonl_records(path: Path, should_remove) -> None:
    original = _read_bytes(path)
    if not original:
        return
    kept: list[bytes] = []
    changed = False
    for line in original.splitlines(keepends=True):
        stripped = line.strip()
        if not stripped:
            kept.append(line)
            continue
        try:
            record = json.loads(stripped.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            kept.append(line)
            continue
        if should_remove(record):
            changed = True
            continue
        kept.append(line)
    if changed:
        _write_bytes_durably(path, b"".join(kept))


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
