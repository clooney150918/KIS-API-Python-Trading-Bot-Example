import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pytest

import ledger_migration
from ledger_migration import (
    APPROVED_CUTOFF_DATE,
    ExecutionLedger,
    LegacyLedgerError,
    build_legacy_history,
    calculate_net_position_remaining_weighted_avg,
    migrate_legacy_history,
    reconcile_legacy_history_to_kis,
    reject_synthetic_official_event,
)

ROOT = Path(__file__).resolve().parents[1]
COMMITTED_LEGACY_HISTORY = ROOT / "data" / "legacy_history_SOXL_20260622_20260810.json"
LEGACY_HISTORY_SHA256 = "b846bc83d9543f4614b7578ab137414bbbbf60a5410753add956b30b3edd0b12"
MANUAL_LEDGER_SHA256_AT_MIGRATION = "363fea9df14e5f76104771c6040cccaa68edb0539f76c5680570803043b172e2"
IGNORED_LOCAL_KIS_SOURCE = ROOT / "data" / "kis_execution_history_SOXL_20260622_20260811.json"
IGNORED_LOCAL_MANUAL_LEDGER = ROOT / "data" / "manual_ledger.json"


def load_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def committed_legacy_rows():
    return load_json(COMMITTED_LEGACY_HISTORY)


def committed_kis_rows_from_provenance():
    return [row["legacy_metadata"]["original_record"] for row in committed_legacy_rows()]


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


@pytest.fixture
def no_ignored_local_ledger_sources(monkeypatch):
    original_exists = Path.exists

    def fake_exists(self):
        if self in {IGNORED_LOCAL_KIS_SOURCE, IGNORED_LOCAL_MANUAL_LEDGER}:
            return False
        return original_exists(self)

    monkeypatch.setattr(Path, "exists", fake_exists)


def test_legacy_history_reconciles_exactly_to_all_72_kis_facts_by_date_order_side_qty_price(no_ignored_local_ledger_sources):
    assert not IGNORED_LOCAL_KIS_SOURCE.exists()
    legacy_rows = committed_legacy_rows()
    kis_rows = committed_kis_rows_from_provenance()

    rebuilt_rows = build_legacy_history(kis_rows, ticker="SOXL")
    report = reconcile_legacy_history_to_kis(legacy_rows, kis_rows)

    assert len(kis_rows) == 72
    assert len(legacy_rows) == 72
    assert hashlib.sha256(COMMITTED_LEGACY_HISTORY.read_bytes()).hexdigest() == LEGACY_HISTORY_SHA256
    assert report == {"missing": [], "extra": [], "mismatches": []}
    assert [legacy_key(row) for row in legacy_rows] == [kis_key(row) for row in kis_rows]
    assert [legacy_key(row) for row in rebuilt_rows] == [kis_key(row) for row in kis_rows]


def test_legacy_kis_facts_have_net_98_and_remaining_weighted_average_158_0735(no_ignored_local_ledger_sources):
    kis_rows = committed_kis_rows_from_provenance()

    net_qty, avg_price = calculate_net_position_remaining_weighted_avg(kis_rows)

    assert net_qty == 98
    assert avg_price == Decimal("158.0735")


def test_migration_writes_legacy_history_without_modifying_manual_ledger_hash(tmp_path, no_ignored_local_ledger_sources):
    kis_copy = tmp_path / "kis.json"
    manual_copy = tmp_path / "manual_ledger.json"
    legacy_out = tmp_path / "legacy_history_SOXL_20260622_20260810.json"
    kis_copy.write_text(json.dumps(committed_kis_rows_from_provenance(), ensure_ascii=False), encoding="utf-8")
    manual_copy.write_bytes(bytes.fromhex(MANUAL_LEDGER_SHA256_AT_MIGRATION))
    before = hashlib.sha256(manual_copy.read_bytes()).hexdigest()

    result = migrate_legacy_history(kis_copy, legacy_out, manual_ledger_path=manual_copy, ticker="SOXL")

    after = hashlib.sha256(manual_copy.read_bytes()).hexdigest()
    assert after == before
    assert result["manual_ledger_sha256_before"] == before
    assert result["manual_ledger_sha256_after"] == before
    assert result["row_count"] == 72
    assert legacy_out.exists()


def test_legacy_history_marks_source_and_preserves_original_reverse_metadata_without_top_level_strategy_truth():
    manual_rows = committed_kis_rows_from_provenance()
    source_rows = [dict(row, is_reverse=True) for row in manual_rows[:3]]

    legacy_rows = build_legacy_history(source_rows, ticker="SOXL")

    assert legacy_rows
    assert all(row["source"] == "LEGACY_HISTORY" for row in legacy_rows)
    assert all("is_reverse" not in row for row in legacy_rows)
    assert all(row["legacy_metadata"]["original_record"].get("is_reverse") is True for row in legacy_rows)
    assert all(row["legacy_metadata"].get("strategy_is_reverse") is True for row in legacy_rows)


def official_fill(**overrides):
    fill = {
        "source": "KIS_CONFIRMED_FILL",
        "trade_date": "2026-08-12",
        "ticker": "SOXL",
        "exchange": "AMEX",
        "side": "BUY",
        "qty": 2,
        "price": "101.23",
        "kis_order_no": "POST-1",
        "execution_time": "09:31:22",
        "account_fingerprint": "acct123",
        "fill_key": "post-1",
        "confirmed": True,
    }
    fill.update(overrides)
    return fill


def read_jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def test_execution_ledger_rejects_legacy_or_pre_cutoff_rows_and_only_appends_confirmed_post_cutoff_fills(tmp_path):
    path = tmp_path / "execution_ledger_SOXL.jsonl"
    ledger = ExecutionLedger(path, cutoff_date=APPROVED_CUTOFF_DATE)

    with pytest.raises(LegacyLedgerError, match="legacy"):
        ledger.append_confirmed_fill(official_fill(source="LEGACY_HISTORY", kis_order_no="LEGACY-1", fill_key="legacy"))
    with pytest.raises(LegacyLedgerError, match="cutoff"):
        ledger.append_confirmed_fill(official_fill(trade_date="2026-08-11", kis_order_no="PRE-CUTOFF", fill_key="pre"))
    with pytest.raises(LegacyLedgerError, match="confirmed"):
        ledger.append_confirmed_fill(official_fill(confirmed=False, kis_order_no="UNCONFIRMED", fill_key="unconfirmed"))

    appended = ledger.append_confirmed_fill(official_fill())

    assert appended["source"] == "KIS_CONFIRMED_FILL"
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["kis_order_no"] == "POST-1"


def test_execution_ledger_duplicate_fill_key_is_rejected_without_second_jsonl_line(tmp_path):
    path = tmp_path / "execution_ledger_SOXL.jsonl"
    ledger = ExecutionLedger(path, cutoff_date=APPROVED_CUTOFF_DATE)
    first = official_fill(fill_key="same-fill-key", kis_order_no="POST-1")
    replay = official_fill(fill_key="same-fill-key", kis_order_no="POST-2", price="102.00")

    ledger.append_confirmed_fill(first)
    with pytest.raises(LegacyLedgerError, match="duplicate"):
        ledger.append_confirmed_fill(replay)

    assert len(read_jsonl(path)) == 1
    assert read_jsonl(path)[0]["kis_order_no"] == "POST-1"


def test_execution_ledger_duplicate_stable_kis_key_is_rejected_without_second_jsonl_line(tmp_path):
    path = tmp_path / "execution_ledger_SOXL.jsonl"
    ledger = ExecutionLedger(path, cutoff_date=APPROVED_CUTOFF_DATE)
    first = official_fill(fill_key="first-key")
    replay = official_fill(fill_key="retry-key")

    ledger.append_confirmed_fill(first)
    with pytest.raises(LegacyLedgerError, match="duplicate"):
        ledger.append_confirmed_fill(replay)

    assert len(read_jsonl(path)) == 1
    assert read_jsonl(path)[0]["fill_key"] == "first-key"


def test_synthetic_calib_genesis_init_events_are_blocked_from_official_pipeline():
    for exec_id in ["CALIB_20260812", "GENESIS_1", "INIT_999"]:
        with pytest.raises(LegacyLedgerError, match="synthetic"):
            reject_synthetic_official_event({"exec_id": exec_id})

    assert reject_synthetic_official_event({"exec_id": "KIS_0030163741"}) is None


def test_synthetic_blocker_inspects_all_official_identity_fields_not_first_truthy_only():
    mixed_records = [
        {"exec_id": "KIS_0030163741", "event_type": "CALIB", "kis_order_no": "0030163741"},
        {"exec_id": "KIS_0030163742", "event_type": "GENESIS_FILL", "kis_order_no": "0030163742"},
        {"exec_id": "KIS_0030163743", "event_type": "BUY", "kis_order_no": "INIT_0030163743"},
    ]

    for record in mixed_records:
        with pytest.raises(LegacyLedgerError, match="synthetic"):
            reject_synthetic_official_event(record)


def test_misleading_fifo_function_name_is_removed_from_public_api():
    assert not hasattr(ledger_migration, "calculate_net_position_fifo_avg")
