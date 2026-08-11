"""Legacy fact ledger migration helpers.

Task 6 separates immutable pre-approval KIS facts from the official strategy
execution/T ledgers.  Legacy rows are preserved for audit only; they are never
promoted as strategy facts such as ``is_reverse``.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

APPROVED_CUTOFF_DATE = "2026-08-11"
LEGACY_SOURCE = "LEGACY_HISTORY"
OFFICIAL_FILL_SOURCE = "KIS_CONFIRMED_FILL"
SYNTHETIC_PREFIXES = ("CALIB", "GENESIS", "INIT")


class LegacyLedgerError(RuntimeError):
    pass


def _text(value: Any) -> str:
    return str(value or "").strip()


def _decimal(value: Any, field: str) -> Decimal:
    try:
        parsed = Decimal(str(value).replace(",", ""))
    except (InvalidOperation, ValueError) as exc:
        raise LegacyLedgerError(f"{field} must be a finite decimal") from exc
    if not parsed.is_finite():
        raise LegacyLedgerError(f"{field} must be a finite decimal")
    return parsed


def _positive_int(value: Any, field: str) -> int:
    parsed = _decimal(value, field)
    if parsed <= 0 or parsed != parsed.to_integral_value():
        raise LegacyLedgerError(f"{field} must be a positive integer")
    return int(parsed)


def _date_text(value: Any) -> str:
    text = _text(value)
    if not text:
        raise LegacyLedgerError("date is required")
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return text.replace("-", "")
    return text


def _iso_date_text(value: Any) -> str:
    text = _text(value)
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text


def _order_no(row: Mapping[str, Any]) -> str:
    order_no = _text(row.get("odno") or row.get("ODNO") or row.get("kis_order_no"))
    if not order_no:
        exec_id = _text(row.get("exec_id"))
        if exec_id.startswith("KIS_"):
            order_no = exec_id[4:]
        elif exec_id:
            order_no = exec_id
    if not order_no:
        raise LegacyLedgerError("KIS order number is required")
    return order_no


def _side(row: Mapping[str, Any]) -> str:
    raw = _text(row.get("side") or row.get("sll_buy_dvsn_cd")).upper()
    if raw in {"BUY", "02", "2", "매수"}:
        return "BUY"
    if raw in {"SELL", "01", "1", "매도"}:
        return "SELL"
    raise LegacyLedgerError(f"unknown side: {raw!r}")


def _price(row: Mapping[str, Any]) -> Decimal:
    return _decimal(row.get("price") if row.get("price") is not None else row.get("ft_ccld_unpr3"), "price")


def _qty(row: Mapping[str, Any]) -> int:
    return _positive_int(row.get("qty") if row.get("qty") is not None else row.get("ft_ccld_qty"), "qty")


def _load_json_array(path: str | os.PathLike[str]) -> list[dict[str, Any]]:
    parsed = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(parsed, list):
        raise LegacyLedgerError(f"expected JSON array: {path}")
    return [row for row in parsed if isinstance(row, dict)]


def _canonical_key(row: Mapping[str, Any]) -> tuple[str, str, str, int, str]:
    return (_date_text(row.get("date") or row.get("ord_dt")), _order_no(row), _side(row), _qty(row), format(_price(row), "f"))


def build_legacy_history(source_rows: Sequence[Mapping[str, Any]], *, ticker: str = "SOXL") -> list[dict[str, Any]]:
    """Copy KIS/manual rows into audit-only LEGACY_HISTORY records.

    Any original strategy labels (notably ``is_reverse``) are retained only in
    ``legacy_metadata`` and deliberately omitted from the top-level record.
    """
    target = _text(ticker).upper()
    legacy_rows: list[dict[str, Any]] = []
    for idx, row in enumerate(source_rows, start=1):
        if not isinstance(row, Mapping):
            raise LegacyLedgerError("legacy source row must be an object")
        raw_date = row.get("date") or row.get("ord_dt")
        order_no = _order_no(row)
        side = _side(row)
        qty = _qty(row)
        price = _price(row)
        metadata = {
            "kis_order_no": order_no,
            "source_index": idx,
            "original_record": dict(row),
        }
        if "is_reverse" in row:
            metadata["strategy_is_reverse"] = bool(row.get("is_reverse"))
        legacy_rows.append({
            "id": idx,
            "source": LEGACY_SOURCE,
            "ticker": _text(row.get("ticker") or row.get("pdno") or target).upper(),
            "date": _date_text(raw_date),
            "side": side,
            "qty": qty,
            "price": float(price),
            "legacy_metadata": metadata,
        })
    return legacy_rows


def reconcile_legacy_history_to_kis(legacy_rows: Sequence[Mapping[str, Any]], kis_rows: Sequence[Mapping[str, Any]]) -> dict[str, list[Any]]:
    legacy_keys = [_canonical_key({
        "date": row.get("date"),
        "odno": (row.get("legacy_metadata") or {}).get("kis_order_no"),
        "side": row.get("side"),
        "qty": row.get("qty"),
        "price": row.get("price"),
    }) for row in legacy_rows]
    kis_keys = [_canonical_key(row) for row in kis_rows]
    missing = [key for key in kis_keys if key not in legacy_keys]
    extra = [key for key in legacy_keys if key not in kis_keys]
    mismatches: list[Any] = []
    for pos, (legacy_key, kis_key) in enumerate(zip(legacy_keys, kis_keys), start=1):
        if legacy_key != kis_key:
            mismatches.append({"position": pos, "legacy": legacy_key, "kis": kis_key})
    return {"missing": missing, "extra": extra, "mismatches": mismatches}


def calculate_net_position_remaining_weighted_avg(rows: Sequence[Mapping[str, Any]]) -> tuple[int, Decimal]:
    """Return remaining quantity and weighted-average cost after sells.

    This is deliberately *not* FIFO tax-lot accounting.  Sells remove cost at
    the then-current weighted average so the result is suitable only for the
    Task 6 migration invariant, not official cost-basis reporting.
    """
    running_qty = 0
    running_cost = Decimal("0")
    for row in rows:
        qty = _qty(row)
        price = _price(row)
        if _side(row) == "BUY":
            running_qty += qty
            running_cost += price * qty
        else:
            if running_qty <= 0:
                running_qty = 0
                running_cost = Decimal("0")
                continue
            cost_per_share = running_cost / running_qty
            used = min(qty, running_qty)
            running_cost -= cost_per_share * used
            running_qty -= used
    avg = Decimal("0.0000")
    if running_qty > 0:
        avg = (running_cost / running_qty).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    return running_qty, avg


def _sha256(path: str | os.PathLike[str]) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def migrate_legacy_history(kis_source_path: str | os.PathLike[str], legacy_output_path: str | os.PathLike[str], *, manual_ledger_path: str | os.PathLike[str] | None = None, ticker: str = "SOXL") -> dict[str, Any]:
    manual_before = _sha256(manual_ledger_path) if manual_ledger_path else ""
    kis_rows = _load_json_array(kis_source_path)
    legacy_rows = build_legacy_history(kis_rows, ticker=ticker)
    report = reconcile_legacy_history_to_kis(legacy_rows, kis_rows)
    if report != {"missing": [], "extra": [], "mismatches": []}:
        raise LegacyLedgerError(f"legacy/KIS reconciliation failed: {report}")
    output = Path(legacy_output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_name(f".{output.name}.tmp.{os.getpid()}")
    temp.write_text(json.dumps(legacy_rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(str(temp), str(output))
    manual_after = _sha256(manual_ledger_path) if manual_ledger_path else ""
    if manual_ledger_path and manual_after != manual_before:
        raise LegacyLedgerError("manual ledger hash changed during migration")
    net_qty, avg_price = calculate_net_position_remaining_weighted_avg(kis_rows)
    return {
        "row_count": len(legacy_rows),
        "reconciliation": report,
        "net_qty": net_qty,
        "avg_price": format(avg_price, "f"),
        "manual_ledger_sha256_before": manual_before,
        "manual_ledger_sha256_after": manual_after,
    }


def reject_synthetic_official_event(record: Mapping[str, Any]) -> None:
    identity_values = [
        _text(record.get(field)).upper()
        for field in ("exec_id", "event_type", "kis_order_no")
        if _text(record.get(field))
    ]
    if any(value.startswith(prefix) or f"_{prefix}" in value for value in identity_values for prefix in SYNTHETIC_PREFIXES):
        raise LegacyLedgerError("synthetic CALIB/GENESIS/INIT events are blocked from the official pipeline")
    return None


def _parse_date(value: Any) -> date:
    text = _iso_date_text(value)
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise LegacyLedgerError(f"invalid trade_date: {value!r}") from exc


class ExecutionLedger:
    """Append-only official execution ledger for post-cutoff confirmed fills."""

    def __init__(self, path: str | os.PathLike[str], *, cutoff_date: str = APPROVED_CUTOFF_DATE):
        self.path = Path(path)
        self.cutoff_date = _parse_date(cutoff_date)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")

    def append_confirmed_fill(self, fill: Mapping[str, Any]) -> dict[str, Any]:
        record = dict(fill)
        reject_synthetic_official_event(record)
        if record.get("source") == LEGACY_SOURCE:
            raise LegacyLedgerError("legacy rows cannot be appended to the official execution ledger")
        if record.get("source") != OFFICIAL_FILL_SOURCE:
            raise LegacyLedgerError("official fills must use KIS_CONFIRMED_FILL source")
        if record.get("confirmed") is not True:
            raise LegacyLedgerError("only confirmed fills may be appended")
        trade_date = _parse_date(record.get("trade_date") or record.get("date"))
        if trade_date <= self.cutoff_date:
            raise LegacyLedgerError("fill is on or before the approved cutoff date")
        for field in ("ticker", "side", "qty", "price", "kis_order_no", "fill_key"):
            if field not in record or record[field] in (None, ""):
                raise LegacyLedgerError(f"missing required execution field: {field}")
        record["trade_date"] = trade_date.isoformat()
        record["ticker"] = _text(record["ticker"]).upper()
        record["side"] = _side(record)
        record["qty"] = _qty(record)
        record["price"] = format(_price(record), "f")
        self._append_jsonl(record)
        return record

    def _stable_kis_key(self, record: Mapping[str, Any]) -> tuple[str, ...] | None:
        fields = ("account_fingerprint", "ticker", "exchange", "trade_date", "kis_order_no", "execution_time", "side", "qty", "price")
        values = [_text(record.get(field)) for field in fields]
        if not all(values):
            return None
        return tuple(values)

    def _load_existing_records(self, content: bytes) -> list[dict[str, Any]]:
        if not content:
            return []
        records: list[dict[str, Any]] = []
        for line_no, raw_line in enumerate(content.decode("utf-8").splitlines(), start=1):
            if not raw_line.strip():
                continue
            try:
                parsed = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                raise LegacyLedgerError(f"invalid execution ledger JSONL at line {line_no}") from exc
            if not isinstance(parsed, dict):
                raise LegacyLedgerError(f"invalid execution ledger JSONL object at line {line_no}")
            records.append(parsed)
        return records

    def _reject_duplicate(self, record: Mapping[str, Any], existing_records: Sequence[Mapping[str, Any]]) -> None:
        fill_key = _text(record.get("fill_key"))
        stable_key = self._stable_kis_key(record)
        for existing in existing_records:
            if fill_key and _text(existing.get("fill_key")) == fill_key:
                raise LegacyLedgerError(f"duplicate execution fill_key: {fill_key}")
            if stable_key is not None and self._stable_kis_key(existing) == stable_key:
                raise LegacyLedgerError("duplicate execution stable KIS key")

    def _fsync_parent_dir(self) -> None:
        dir_fd = os.open(str(self.path.parent), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def _append_jsonl(self, record: Mapping[str, Any]) -> None:
        payload = json.dumps(dict(record), ensure_ascii=False, separators=(",", ":")) + "\n"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+") as lock_f:
            fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
            try:
                existing = b""
                if self.path.exists():
                    existing = self.path.read_bytes()
                self._reject_duplicate(record, self._load_existing_records(existing))
                temp = self.path.with_name(f".{self.path.name}.tmp.{os.getpid()}")
                fd = os.open(str(temp), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
                try:
                    if existing:
                        os.write(fd, existing)
                    os.write(fd, payload.encode("utf-8"))
                    os.fsync(fd)
                finally:
                    os.close(fd)
                os.replace(str(temp), str(self.path))
                self._fsync_parent_dir()
            finally:
                fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)
