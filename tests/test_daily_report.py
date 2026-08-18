import json

import daily_report


def write_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


class DummyConfig:
    def __init__(self, root):
        self.FILES = {
            "EXECUTION_LEDGER": str(root / "data" / "execution_ledger_SOXL.jsonl"),
            "T_EVENTS": str(root / "data" / "t_events_SOXL.jsonl"),
        }

    def get_official_t_state(self, ticker):
        return {"t": 12.055, "available_cash": 9999.99}

    def calculate_cycle_cash(self, ticker):
        return 1482.88, {"cycle_cash": "1482.88"}

    def get_official_fills(self, ticker):
        return []


class DummyBroker:
    def get_account_balance(self):
        return 0, {"SOXL": {"qty": 71, "avg": 154.4842}}

    def get_current_price(self, ticker):
        return 129.10


class DummyStrategy:
    def get_plan(self, *args, **kwargs):
        return {"orders": []}


def test_daily_report_projects_t_from_confirmed_fills_missing_t_event(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daily_report, "_last_completed_trade_date", lambda today=None: "2026-08-18")
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    snapshot = {
        "date": "2026-08-18",
        "t_val": 12.055,
        "total_q": 65,
        "one_portion": 1748.6075471698114,
        "star_price": 150.4,
        "orders": [
            {"event_type": "FULL", "side": "BUY", "price": "150.39", "qty": 11},
        ],
    }
    write_json(data_dir / "daily_snapshot_V4_2026-08-18_SOXL.json", snapshot)
    (data_dir / "execution_ledger_SOXL.jsonl").write_text(
        json.dumps(
            {
                "source": "KIS_CONFIRMED_FILL",
                "trade_date": "2026-08-18",
                "ticker": "SOXL",
                "side": "BUY",
                "qty": 5,
                "price": "129.10",
                "kis_order_no": "0030308368",
                "fill_key": "|SOXL|AMEX|20260818|0030308368|191145|BUY|5|129.10",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (data_dir / "t_events_SOXL.jsonl").write_text("", encoding="utf-8")
    (data_dir / "order_intents_SOXL.jsonl").write_text("", encoding="utf-8")

    report = daily_report.build_daily_report(DummyConfig(tmp_path), DummyBroker(), DummyStrategy())

    assert "T      12.1  →  13.1    (+1.0)" in report
    assert "잔금   $2,128 → $1,483" in report
    assert "매수금  $268  →  $214" in report


def test_daily_report_does_not_project_fill_already_in_t_events(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daily_report, "_last_completed_trade_date", lambda today=None: "2026-08-18")
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    snapshot = {
        "date": "2026-08-18",
        "t_val": 12.055,
        "total_q": 65,
        "one_portion": 1748.6075471698114,
        "star_price": 150.4,
        "orders": [
            {"event_type": "FULL", "side": "BUY", "price": "150.39", "qty": 11},
        ],
    }
    write_json(data_dir / "daily_snapshot_V4_2026-08-18_SOXL.json", snapshot)
    (data_dir / "execution_ledger_SOXL.jsonl").write_text(
        json.dumps(
            {
                "source": "KIS_CONFIRMED_FILL",
                "trade_date": "2026-08-18",
                "ticker": "SOXL",
                "side": "BUY",
                "qty": 5,
                "price": "129.10",
                "kis_order_no": "0030308368",
                "fill_key": "|SOXL|AMEX|20260818|0030308368|191145|BUY|5|129.10",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (data_dir / "t_events_SOXL.jsonl").write_text(
        json.dumps(
            {
                "ticker": "SOXL",
                "fill_key": "acct-A|SOXL|AMEX|20260818|0030308368|191145|BUY|5|129.10",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (data_dir / "order_intents_SOXL.jsonl").write_text("", encoding="utf-8")
    cfg = DummyConfig(tmp_path)
    cfg.get_official_t_state = lambda ticker: {"t": 13.055, "available_cash": 1482.88}

    report = daily_report.build_daily_report(cfg, DummyBroker(), DummyStrategy())

    assert "T      12.1  →  13.1    (+1.0)" in report
    assert "잔금   $2,128 → $1,483" in report
