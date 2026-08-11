import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pytest

from ledger_migration import (
    APPROVED_CUTOFF_DATE,
    ExecutionLedger,
    LegacyLedgerError,
    build_legacy_history,
    calculate_net_position_fifo_avg,
    migrate_legacy_history,
    reconcile_legacy_history_to_kis,
    reject_synthetic_official_event,
)

ROOT = Path(__file__).resolve().parents[1]
KIS_SOURCE = ROOT / "data" / "kis_execution_history_SOXL_20260622_20260811.json"
MANUAL_LEDGER = ROOT / "data" / "manual_ledger.json"


def load_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def legacy_key(row):
    return (
        row["date"],
        row["legacy_metadata"]["kis_order_no"],
        row["side"],
        int(row["qty"]),
        Decimal(str(row["price"])),
    )


def kis_key(row):
    return (
        row["date"],
        row["odno"],
        row["side"],
        int(row["qty"]),
        Decimal(str(row["price"])),
    )


def test_legacy_history_reconciles_exactly_to_all_72_kis_facts_by_date_order_side_qty_price():
    kis_rows = load_json(KIS_SOURCE)

    legacy_rows = build_legacy_history(kis_rows, ticker="SOXL")
    report = reconcile_legacy_history_to_kis(legacy_rows, kis_rows)

    assert len(kis_rows) == 72
    assert len(legacy_rows) == 72
    assert report == {"missing": [], "extra": [], "mismatches": []}
    assert [legacy_key(row) for row in legacy_rows] == [kis_key(row) for row in kis_rows]


def test_legacy_kis_facts_have_net_98_and_kis_fifo_average_158_0735():
    kis_rows = load_json(KIS_SOURCE)

    net_qty, avg_price = calculate_net_position_fifo_avg(kis_rows)

    assert net_qty == 98
    assert avg_price == Decimal("158.0735")


def test_migration_writes_legacy_history_without_modifying_manual_ledger_hash(tmp_path):
    kis_copy = tmp_path / "kis.json"
    manual_copy = tmp_path / "manual_ledger.json"
    legacy_out = tmp_path / "legacy_history_SOXL_20260622_20260810.json"
    kis_copy.write_text(KIS_SOURCE.read_text(encoding="utf-8"), encoding="utf-8")
    manual_copy.write_bytes(MANUAL_LEDGER.read_bytes())
    before = hashlib.sha256(manual_copy.read_bytes()).hexdigest()

    result = migrate_legacy_history(kis_copy, legacy_out, manual_ledger_path=manual_copy, ticker="SOXL")

    after = hashlib.sha256(manual_copy.read_bytes()).hexdigest()
    assert after == before
    assert result["manual_ledger_sha256_before"] == before
    assert result["manual_ledger_sha256_after"] == before
    assert result["row_count"] == 72
    assert legacy_out.exists()


def test_legacy_history_marks_source_and_preserves_original_reverse_metadata_without_top_level_strategy_truth():
    manual_rows = load_json(MANUAL_LEDGER)
    source_rows = [dict(row, is_reverse=True) for row in manual_rows[:3]]

    legacy_rows = build_legacy_history(source_rows, ticker="SOXL")

    assert legacy_rows
    assert all(row["source"] == "LEGACY_HISTORY" for row in legacy_rows)
    assert all("is_reverse" not in row for row in legacy_rows)
    assert all(row["legacy_metadata"]["original_record"].get("is_reverse") is True for row in legacy_rows)
    assert all(row["legacy_metadata"].get("strategy_is_reverse") is True for row in legacy_rows)


def test_execution_ledger_rejects_legacy_or_pre_cutoff_rows_and_only_appends_confirmed_post_cutoff_fills(tmp_path):
    path = tmp_path / "execution_ledger_SOXL.jsonl"
    ledger = ExecutionLedger(path, cutoff_date=APPROVED_CUTOFF_DATE)

    with pytest.raises(LegacyLedgerError, match="legacy"):
        ledger.append_confirmed_fill({
            "source": "LEGACY_HISTORY",
            "trade_date": "2026-08-12",
            "ticker": "SOXL",
            "side": "BUY",
            "qty": 1,
            "price": "100.00",
            "kis_order_no": "LEGACY-1",
            "fill_key": "legacy",
            "confirmed": True,
        })
    with pytest.raises(LegacyLedgerError, match="cutoff"):
        ledger.append_confirmed_fill({
            "source": "KIS_CONFIRMED_FILL",
            "trade_date": "2026-08-11",
            "ticker": "SOXL",
            "side": "BUY",
            "qty": 1,
            "price": "100.00",
            "kis_order_no": "PRE-CUTOFF",
            "fill_key": "pre",
            "confirmed": True,
        })
    with pytest.raises(LegacyLedgerError, match="confirmed"):
        ledger.append_confirmed_fill({
            "source": "KIS_CONFIRMED_FILL",
            "trade_date": "2026-08-12",
            "ticker": "SOXL",
            "side": "BUY",
            "qty": 1,
            "price": "100.00",
            "kis_order_no": "UNCONFIRMED",
            "fill_key": "unconfirmed",
            "confirmed": False,
        })

    appended = ledger.append_confirmed_fill({
        "source": "KIS_CONFIRMED_FILL",
        "trade_date": "2026-08-12",
        "ticker": "SOXL",
        "side": "BUY",
        "qty": 2,
        "price": "101.23",
        "kis_order_no": "POST-1",
        "fill_key": "post-1",
        "confirmed": True,
    })

    assert appended["source"] == "KIS_CONFIRMED_FILL"
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["kis_order_no"] == "POST-1"


def test_synthetic_calib_genesis_init_events_are_blocked_from_official_pipeline():
    for exec_id in ["CALIB_20260812", "GENESIS_1", "INIT_999"]:
        with pytest.raises(LegacyLedgerError, match="synthetic"):
            reject_synthetic_official_event({"exec_id": exec_id})

    assert reject_synthetic_official_event({"exec_id": "KIS_0030163741"}) is None
