import asyncio
import hashlib
import json
from types import SimpleNamespace

import pytest

import telegram_commands
from config import ConfigManager
from order_intent_store import OrderIntentStore
from telegram_commands import TelegramCommands
from telegram_view import TelegramView


class FakeMessage:
    def __init__(self):
        self.edits = []
        self.replies = []

    async def edit_text(self, text, **kwargs):
        self.edits.append((text, kwargs))
        return self

    async def reply_text(self, text, **kwargs):
        self.replies.append((text, kwargs))
        return self


class FakeUpdate:
    def __init__(self):
        self.effective_chat = SimpleNamespace(id=12345)
        self.effective_user = SimpleNamespace(id=12345)
        self.effective_message = FakeMessage()
        self.callback_query = object()


class FakeContext:
    def __init__(self):
        self.bot_data = {}
        self.job_queue = None


class FakeBroker:
    def get_account_balance(self):
        return 1482.88, {"SOXL": {"qty": 98, "avg": 158.0735}}

    def get_current_price(self, ticker, is_market_closed=False):
        return 150.0

    def get_previous_close(self, ticker):
        return 149.0

    def get_5day_ma(self, ticker):
        return 148.0

    def get_day_high_low(self, ticker):
        return 151.0, 147.0

    def get_dynamic_sniper_target(self, ticker):
        return SimpleNamespace(base_amp=8.79, metric_val=0.0)


class EmptyHistory:
    empty = True


class FakeYFTicker:
    def __init__(self, ticker):
        self.ticker = ticker

    def history(self, *args, **kwargs):
        return EmptyHistory()


class FakeStrategy:
    def get_plan(self, *args, **kwargs):
        return {
            "process_status": "🌓공식전반전",
            "t_val": 18.82,
            "t_revision": 2,
            "star_ratio": -0.1764,
            "star_price": 130.19,
            "one_portion": 882.67,
            "orders": [],
        }


class CapturingView(TelegramView):
    def __init__(self):
        super().__init__()
        self.captured_ticker_data = None

    def create_sync_report(self, status_text, dst_text, cash, rp_amount, ticker_data, is_trade_active, p_trade_data=None, exchange_rate=None):
        self.captured_ticker_data = ticker_data
        return super().create_sync_report(status_text, dst_text, cash, rp_amount, ticker_data, is_trade_active, p_trade_data, exchange_rate)


def _isolated_cfg(tmp_path):
    cfg = ConfigManager()
    files = {
        "LEDGER": tmp_path / "manual_ledger.json",
        "STRATEGY_BASELINE": tmp_path / "baseline.json",
        "T_EVENTS": tmp_path / "t_events_SOXL.jsonl",
        "T_STATE": tmp_path / "legacy_t_state.json",
        "REVERSE_CFG": tmp_path / "reverse.json",
        "LOCKS": tmp_path / "locks.json",
        "SPLIT": tmp_path / "split.json",
        "TICKER": tmp_path / "tickers.json",
        "SEED_CFG": tmp_path / "seed.json",
        "VERSION_CFG": tmp_path / "version.json",
        "UPWARD_SNIPER": tmp_path / "sniper.json",
        "PROFIT_CFG": tmp_path / "profit.json",
        "MANUAL_VWAP_CFG": tmp_path / "manual_vwap.json",
        "AVWAP_HYBRID_CFG": tmp_path / "avwap_hybrid.json",
        "ORDER_INTENTS": tmp_path / "order_intents_SOXL.jsonl",
    }
    for key, path in files.items():
        cfg.FILES[key] = str(path)
    files["LEDGER"].write_text(
        json.dumps([{"id": 1, "ticker": "SOXL", "side": "BUY", "qty": 7, "price": 9.87}], ensure_ascii=False),
        encoding="utf-8",
    )
    files["STRATEGY_BASELINE"].write_text(
        json.dumps(
            {
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
        ),
        encoding="utf-8",
    )
    files["T_EVENTS"].write_text(
        json.dumps(
            {
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
        )
        + "\n",
        encoding="utf-8",
    )
    files["T_STATE"].write_text(json.dumps({"SOXL": {"t_val": 3.14, "revision": 99}}), encoding="utf-8")
    files["REVERSE_CFG"].write_text("{}", encoding="utf-8")
    files["LOCKS"].write_text("{}", encoding="utf-8")
    files["SPLIT"].write_text(json.dumps({"SOXL": 20}), encoding="utf-8")
    files["TICKER"].write_text(json.dumps(["SOXL"]), encoding="utf-8")
    files["SEED_CFG"].write_text(json.dumps({"SOXL": 6720.0}), encoding="utf-8")
    files["VERSION_CFG"].write_text(json.dumps({"SOXL": "LAOER_V4_SOXL_20"}), encoding="utf-8")
    files["UPWARD_SNIPER"].write_text("{}", encoding="utf-8")
    files["PROFIT_CFG"].write_text(json.dumps({"SOXL": 20.0}), encoding="utf-8")
    files["MANUAL_VWAP_CFG"].write_text("{}", encoding="utf-8")
    files["AVWAP_HYBRID_CFG"].write_text("{}", encoding="utf-8")
    files["ORDER_INTENTS"].write_text("", encoding="utf-8")
    return cfg, files


def _create_commands(cfg, view):
    return TelegramCommands(
        cfg,
        broker=FakeBroker(),
        strategy=FakeStrategy(),
        queue_ledger=None,
        sync_engine=SimpleNamespace(sync_locks={}),
        view=view,
        tx_lock=asyncio.Lock(),
    )


async def _run_cmd_sync(commands, monkeypatch):
    async def fake_market_status(self):
        return "REG", "정규장"

    monkeypatch.setattr(TelegramCommands, "_get_market_status", fake_market_status)
    monkeypatch.setattr(TelegramCommands, "_get_dst_info", lambda self: (17, "🌞"))
    monkeypatch.setattr(telegram_commands, "get_budget_allocation", lambda cash, tickers, cfg: (tickers, {"SOXL": cash}))
    monkeypatch.setattr(telegram_commands.GlobalThrottle, "wait_api_sync", lambda: None)
    await commands.cmd_sync(FakeUpdate(), FakeContext())


def _latest_text(message):
    assert message.edits
    return message.edits[-1][0]


def _run(coro):
    return asyncio.run(coro)


def test_cmd_sync_actual_data_path_populates_kis_local_discrepancy_halt_without_mutating_manual_ledger(tmp_path, monkeypatch):
    cfg, files = _isolated_cfg(tmp_path)
    before_text = files["LEDGER"].read_text(encoding="utf-8")
    before_hash = hashlib.sha256(before_text.encode("utf-8")).hexdigest()
    view = CapturingView()
    commands = _create_commands(cfg, view)

    update = FakeUpdate()

    async def fake_run():
        async def fake_market_status(self):
            return "REG", "정규장"
        monkeypatch.setattr(TelegramCommands, "_get_market_status", fake_market_status)
        monkeypatch.setattr(TelegramCommands, "_get_dst_info", lambda self: (17, "🌞"))
        monkeypatch.setattr(telegram_commands, "get_budget_allocation", lambda cash, tickers, cfg: (tickers, {"SOXL": cash}))
        monkeypatch.setattr(telegram_commands.GlobalThrottle, "wait_api_sync", lambda: None)
        monkeypatch.setattr(telegram_commands.yf, "Ticker", FakeYFTicker)
        await commands.cmd_sync(update, FakeContext())

    _run(fake_run())

    item = view.captured_ticker_data[0]
    assert item["official_balance"] == {"qty": 98, "avg": 158.0735, "orderable_cash": 1482.88}
    assert item["kis_balance"] == item["official_balance"]
    assert item["local_ledger"]["qty"] == 7
    assert item["local_ledger"]["avg"] == pytest.approx(9.87)
    assert item["discrepancy"]["halted"] is True
    assert "KIS/local mismatch" in item["discrepancy"]["reason"]

    text = _latest_text(update.effective_message)
    assert "KIS 보유수량: <b>98주</b>" in text
    assert "KIS 평단: <b>$158.07</b>" in text
    assert "KIS 주문가능 예수금: <b>$1,482.88</b>" in text
    assert "⛔ <b>HALT" in text
    assert "KIS/local mismatch" in text
    assert files["LEDGER"].read_text(encoding="utf-8") == before_text
    assert hashlib.sha256(files["LEDGER"].read_text(encoding="utf-8").encode("utf-8")).hexdigest() == before_hash


def test_cmd_sync_actual_data_path_loads_order_statuses_from_intent_ledger_for_separate_rendering(tmp_path, monkeypatch):
    cfg, files = _isolated_cfg(tmp_path)
    store = OrderIntentStore(files["ORDER_INTENTS"], current_t_revision_provider=lambda _ticker: 2)
    for status, qty in [("SUBMITTED", 1), ("PARTIAL", 2), ("FILLED", 3), ("CANCELLED", 4), ("REJECTED", 5)]:
        intent = store.create_planned(
            {
                "strategy": "LAOER_V4_SOXL_20",
                "strategy_revision": 1,
                "t_revision": 2,
                "ticker": "SOXL",
                "trade_date": "2026-08-12",
                "event_type": "FULL_BUY",
                "side": "BUY",
                "order_type": "LOC",
                "price": f"13{qty}.76",
                "qty": qty,
            }
        )
        store.transition_status(intent["intent_id"], "SUBMITTED")
        if status != "SUBMITTED":
            store.transition_status(intent["intent_id"], status)
    view = CapturingView()
    commands = _create_commands(cfg, view)
    update = FakeUpdate()

    async def fake_market_status(self):
        return "REG", "정규장"
    monkeypatch.setattr(TelegramCommands, "_get_market_status", fake_market_status)
    monkeypatch.setattr(TelegramCommands, "_get_dst_info", lambda self: (17, "🌞"))
    monkeypatch.setattr(telegram_commands, "get_budget_allocation", lambda cash, tickers, cfg: (tickers, {"SOXL": cash}))
    monkeypatch.setattr(telegram_commands.GlobalThrottle, "wait_api_sync", lambda: None)
    monkeypatch.setattr(telegram_commands.yf, "Ticker", FakeYFTicker)
    _run(commands.cmd_sync(update, FakeContext()))

    statuses = view.captured_ticker_data[0]["order_statuses"]
    assert {key: len(value) for key, value in statuses.items()} == {
        "SUBMITTED": 1,
        "PARTIAL": 1,
        "FILLED": 1,
        "CANCELLED": 1,
        "REJECTED": 1,
    }
    text = _latest_text(update.effective_message)
    assert "주문접수(SUBMITTED): <b>1건</b>" in text
    assert "부분체결(PARTIAL): <b>1건</b>" in text
    assert "체결완료(FILLED): <b>1건</b>" in text
    assert "취소(CANCELLED): <b>1건</b>" in text
    assert "거절(REJECTED): <b>1건</b>" in text


def test_broken_local_ledger_summary_fails_closed_discrepancy_instead_of_healthy_empty(tmp_path):
    cfg, _files = _isolated_cfg(tmp_path)
    commands = _create_commands(cfg, CapturingView())

    def raise_corrupt_ledger():
        raise ValueError("corrupt manual ledger unavailable")

    cfg.get_ledger = raise_corrupt_ledger

    local_ledger = commands._build_local_ledger_summary_for_sync("SOXL")
    discrepancy = commands._build_kis_local_discrepancy_for_sync(
        {"qty": 98, "avg": 158.0735, "orderable_cash": 1482.88},
        local_ledger,
    )

    assert local_ledger["unavailable"] is True
    assert "corrupt" in local_ledger["error"].lower()
    assert discrepancy["halted"] is True
    assert "ledger" in discrepancy["reason"].lower()
    assert "unavailable" in discrepancy["reason"].lower() or "corrupt" in discrepancy["reason"].lower()


def test_cmd_sync_corrupt_order_intent_ledger_surfaces_warning_instead_of_all_zero_healthy_state(tmp_path, monkeypatch):
    cfg, files = _isolated_cfg(tmp_path)
    files["ORDER_INTENTS"].write_text('{"broken": true}\n', encoding="utf-8")
    view = CapturingView()
    commands = _create_commands(cfg, view)
    update = FakeUpdate()

    async def fake_market_status(self):
        return "REG", "정규장"
    monkeypatch.setattr(TelegramCommands, "_get_market_status", fake_market_status)
    monkeypatch.setattr(TelegramCommands, "_get_dst_info", lambda self: (17, "🌞"))
    monkeypatch.setattr(telegram_commands, "get_budget_allocation", lambda cash, tickers, cfg: (tickers, {"SOXL": cash}))
    monkeypatch.setattr(telegram_commands.GlobalThrottle, "wait_api_sync", lambda: None)
    monkeypatch.setattr(telegram_commands.yf, "Ticker", FakeYFTicker)

    _run(commands.cmd_sync(update, FakeContext()))

    item = view.captured_ticker_data[0]
    warning = item["order_status_warning"]
    assert warning["halted"] is True
    assert "order intent ledger" in warning["reason"].lower()
    assert "corrupt" in warning["reason"].lower() or "invalid" in warning["reason"].lower()
    text = _latest_text(update.effective_message)
    assert "주문 상태 원장" in text
    assert "HALT" in text
