import json

from config import ConfigManager


BASELINE = {
    "schema_version": 1,
    "ticker": "SOXL",
    "as_of": "2026-08-11",
    "qty": 98,
    "avg_price": "158.0735",
    "available_cash": "1482.88",
    "t": "18.32",
    "reverse_active": False,
    "source": "CEO_APPROVED_KIS_BASELINE",
    "legacy_execution_count": 72,
    "immutable": True,
}


def test_config_absolute_t_reads_baseline_event_ledger_only(tmp_path):
    baseline_path = tmp_path / "baseline.json"
    events_path = tmp_path / "events.jsonl"
    baseline_path.write_text(json.dumps(BASELINE), encoding="utf-8")
    events_path.write_text("", encoding="utf-8")

    cfg = ConfigManager()
    cfg.FILES["STRATEGY_BASELINE"] = str(baseline_path)
    cfg.FILES["T_EVENTS"] = str(events_path)

    first_t, first_portion = cfg.get_absolute_t_val("SOXL", actual_qty=98, actual_avg_price=158.0735)
    second_t, second_portion = cfg.get_absolute_t_val("SOXL", actual_qty=999, actual_avg_price=1)

    assert first_t == second_t == 18.32
    assert first_portion == second_portion == 1482.88 / (20 - 18.32)


def test_calculate_v14_state_does_not_inverse_cost_basis(tmp_path):
    baseline_path = tmp_path / "baseline.json"
    events_path = tmp_path / "events.jsonl"
    baseline_path.write_text(json.dumps(BASELINE), encoding="utf-8")
    events_path.write_text("", encoding="utf-8")

    cfg = ConfigManager()
    cfg.FILES["STRATEGY_BASELINE"] = str(baseline_path)
    cfg.FILES["T_EVENTS"] = str(events_path)
    cfg.FILES["LEDGER"] = str(tmp_path / "ledger.json")
    (tmp_path / "ledger.json").write_text(json.dumps([
        {"id": 1, "ticker": "SOXL", "side": "BUY", "qty": 999, "price": 1},
    ]), encoding="utf-8")

    t_val, budget, rem_cash = cfg.calculate_v14_state("SOXL")

    assert t_val == 18.32
    assert budget == 1482.88 / (20 - 18.32)
    assert rem_cash == 1482.88
