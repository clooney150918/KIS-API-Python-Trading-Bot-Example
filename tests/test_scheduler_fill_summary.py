import datetime
from zoneinfo import ZoneInfo

from scheduler_core import build_daily_fill_summary


class DummyIntentStore:
    def list_intents(self, ticker):
        return [
            {"intent_id": "intent-full", "event_type": "FULL_BUY"},
            {"intent_id": "intent-target", "event_type": "TARGET_FULL"},
        ]


class DummyReconciler:
    intent_store = DummyIntentStore()


def test_daily_fill_summary_groups_records_by_kst_yesterday_and_today_labels():
    today = datetime.datetime.now(ZoneInfo("Asia/Seoul")).date()
    yesterday = today - datetime.timedelta(days=1)

    text = build_daily_fill_summary(
        "[진호봇]",
        [
            {
                "ticker": "SOXL",
                "trade_date": yesterday.strftime("%Y%m%d"),
                "side": "BUY",
                "qty": 6,
                "price": "129.10",
                "intent_id": "intent-full",
            },
            {
                "ticker": "SOXL",
                "trade_date": today.strftime("%Y%m%d"),
                "side": "SELL",
                "qty": 2,
                "price": "131.20",
                "intent_id": "intent-target",
            },
        ],
        DummyReconciler(),
    )

    assert "🔔 [진호봇] 체결 요약 (KST, 2건)" in text
    assert f"📅 어제(미국장 {yesterday:%m-%d})" in text
    assert "🟢 매수 6주 @ $129.10 (별값매수)" in text
    assert f"📅 오늘(미국장 {today:%m-%d})" in text
    assert "🔴 매도 2주 @ $131.20 (목표익절)" in text
