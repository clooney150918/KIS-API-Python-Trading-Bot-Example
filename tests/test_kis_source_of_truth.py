import hashlib
import json
import asyncio
import datetime as dt
from types import SimpleNamespace

import pandas as pd

import telegram_sync_engine
from config import ConfigManager
from telegram_sync_engine import TelegramSyncEngine


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


def _isolated_cfg(tmp_path):
    cfg = ConfigManager()
    paths = {
        "LEDGER": tmp_path / "manual_ledger.json",
        "EXECUTION_LEDGER": tmp_path / "execution_ledger_SOXL.jsonl",
        "STRATEGY_BASELINE": tmp_path / "baseline.json",
        "T_EVENTS": tmp_path / "t_events_SOXL.jsonl",
        "T_STATE": tmp_path / "legacy_t_state.json",
        "REVERSE_CFG": tmp_path / "reverse.json",
        "LOCKS": tmp_path / "locks.json",
    }
    for key, path in paths.items():
        cfg.FILES[key] = str(path)
    paths["STRATEGY_BASELINE"].write_text(json.dumps(BASELINE), encoding="utf-8")
    paths["T_EVENTS"].write_text("", encoding="utf-8")
    paths["T_STATE"].write_text(json.dumps({"SOXL": {"t_val": 3.14, "revision": 99}}), encoding="utf-8")
    paths["REVERSE_CFG"].write_text("{}", encoding="utf-8")
    paths["LOCKS"].write_text("{}", encoding="utf-8")
    return cfg, paths


def _append_half_event(path):
    event = {
        "event_id": "evt-half-1",
        "ticker": "SOXL",
        "intent_id": "intent-half-1",
        "kis_order_no": "POST-10",
        "fill_key": "fill-half-1",
        "event_type": "HALF",
        "filled_qty": 1,
        "filled_amount": "101.23",
        "t_before": "18.32",
        "t_after": "18.82",
        "revision_before": 1,
        "revision_after": 2,
        "occurred_at": "2026-08-12T13:31:22Z",
    }
    path.write_text(json.dumps(event, ensure_ascii=False) + "\n", encoding="utf-8")


def test_config_official_t_state_reads_append_only_event_ledger_not_legacy_manual_estimate(tmp_path):
    cfg, paths = _isolated_cfg(tmp_path)
    _append_half_event(paths["T_EVENTS"])

    state = cfg.get_official_t_state("SOXL")

    assert state == {
        "ticker": "SOXL",
        "t": 18.82,
        "revision": 2,
        "available_cash": 1482.88,
        "reverse_active": False,
    }
    assert state["t"] != 3.14
    assert state["revision"] != 99


def test_sync_appends_kis_confirmed_facts_to_execution_ledger_without_overwriting_manual_ledger(tmp_path):
    cfg, paths = _isolated_cfg(tmp_path)
    manual_rows = [
        {"id": 1, "ticker": "SOXL", "side": "BUY", "qty": 7, "price": 9.87, "exec_id": "MANUAL_LEGACY"}
    ]
    paths["LEDGER"].write_text(json.dumps(manual_rows, ensure_ascii=False, indent=2), encoding="utf-8")
    paths["EXECUTION_LEDGER"].write_text("", encoding="utf-8")
    before_text = paths["LEDGER"].read_text(encoding="utf-8")
    before_hash = hashlib.sha256(before_text.encode("utf-8")).hexdigest()

    engine = TelegramSyncEngine(cfg, broker=None, strategy=None, queue_ledger=None, view=None, tx_lock=None, sync_locks={})
    appended = engine.sync_official_execution_facts(
        "SOXL",
        [
            {
                "odno": "POST-10",
                "ord_dt": "20260812",
                "ord_tmd": "093122",
                "pdno": "SOXL",
                "ovrs_excg_cd": "AMEX",
                "sll_buy_dvsn_cd": "02",
                "ft_ccld_qty": "2",
                "ft_ccld_unpr3": "101.23",
            }
        ],
        account_fingerprint="acct123",
    )

    after_text = paths["LEDGER"].read_text(encoding="utf-8")
    assert hashlib.sha256(after_text.encode("utf-8")).hexdigest() == before_hash
    assert after_text == before_text
    assert appended["appended_count"] == 1
    execution_rows = [json.loads(line) for line in paths["EXECUTION_LEDGER"].read_text(encoding="utf-8").splitlines()]
    assert len(execution_rows) == 1
    assert execution_rows[0]["source"] == "KIS_CONFIRMED_FILL"
    assert execution_rows[0]["trade_date"] == "2026-08-12"
    assert execution_rows[0]["ticker"] == "SOXL"
    assert execution_rows[0]["qty"] == 2
    assert execution_rows[0]["price"] == "101.23"
    assert execution_rows[0]["kis_order_no"] == "POST-10"
    assert execution_rows[0]["confirmed"] is True


class _FixedDateTime(dt.datetime):
    @classmethod
    def now(cls, tz=None):
        if tz and getattr(tz, "key", "") == "Asia/Seoul":
            return cls(2026, 8, 13, 1, 0, 0, tzinfo=tz)
        if tz:
            return cls(2026, 8, 12, 12, 0, 0, tzinfo=tz)
        return cls(2026, 8, 12, 12, 0, 0)


class _AppendOnlySyncCfg:
    def __init__(self):
        self.FILES = {}
        self.appended = []
        self.legacy_calls = []

    def get_last_split_date(self, ticker):
        return ""

    def get_ledger(self):
        return [{"ticker": "SOXL", "side": "BUY", "qty": 2, "price": 0.0, "date": "2026-08-12"}]

    def calculate_holdings(self, ticker, rows):
        return (2, 0.0, 0.0, 0.0)

    def get_version(self, ticker):
        return "LAOER_V4_SOXL_20"

    def append_kis_confirmed_execution_fact(self, fill):
        self.appended.append(dict(fill))
        return dict(fill)

    def _legacy_mutation(self, name):
        self.legacy_calls.append(name)
        raise AssertionError(f"legacy manual ledger mutation must not be called: {name}")

    def calibrate_ledger_prices(self, *args, **kwargs):
        return self._legacy_mutation("calibrate_ledger_prices")

    def calibrate_avg_price(self, *args, **kwargs):
        return self._legacy_mutation("calibrate_avg_price")

    def overwrite_incremental_ledger(self, *args, **kwargs):
        return self._legacy_mutation("overwrite_incremental_ledger")

    def get_account_fingerprint(self):
        return "acct-process-auto-sync"


class _AppendOnlyBroker:
    def get_recent_stock_split(self, ticker, last_split_date):
        return 0.0, ""

    def get_account_balance(self):
        return 1000.0, {"SOXL": {"qty": 2, "avg": 0.0}}

    def get_execution_fills(self, ticker, start, end):
        return [
            {
                "odno": "POST-PROCESS-1",
                "ord_dt": "20260813",
                "ord_tmd": "010000",
                "pdno": "SOXL",
                "ovrs_excg_cd": "AMEX",
                "sll_buy_dvsn_cd": "02",
                "ft_ccld_qty": "2",
                "ft_ccld_unpr3": "101.23",
            }
        ]

    def get_execution_history(self, ticker, start, end):
        raise AssertionError("process_auto_sync should prefer get_execution_fills when available")


def test_process_auto_sync_appends_official_kis_facts_and_never_calls_legacy_manual_mutators(monkeypatch):
    cfg = _AppendOnlySyncCfg()
    monkeypatch.setattr(telegram_sync_engine.datetime, "datetime", _FixedDateTime)
    monkeypatch.setattr(
        telegram_sync_engine.mcal,
        "get_calendar",
        lambda _name: SimpleNamespace(schedule=lambda **_kwargs: pd.DataFrame(index=[dt.datetime(2026, 8, 12)])),
    )
    monkeypatch.setattr(telegram_sync_engine.GlobalThrottle, "wait_api_sync", lambda: None)
    engine = TelegramSyncEngine(
        cfg,
        broker=_AppendOnlyBroker(),
        strategy=SimpleNamespace(),
        queue_ledger=None,
        view=SimpleNamespace(),
        tx_lock=asyncio.Lock(),
        sync_locks={},
    )
    context = SimpleNamespace(bot=SimpleNamespace(send_message=lambda **_kwargs: None))

    result = asyncio.run(engine.process_auto_sync("SOXL", 12345, context))

    assert result == "SUCCESS"
    assert cfg.legacy_calls == []
    assert len(cfg.appended) == 1
    assert cfg.appended[0]["source"] == "KIS_CONFIRMED_FILL"
    assert cfg.appended[0]["ticker"] == "SOXL"
    assert cfg.appended[0]["qty"] == 2
    assert cfg.appended[0]["kis_order_no"] == "POST-PROCESS-1"


