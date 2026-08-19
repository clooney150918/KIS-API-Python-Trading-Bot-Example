from telegram_view import TelegramView


def _base_ticker_info(**overrides):
    data = {
        "ticker": "SOXL",
        "version": "LAOER_V4_SOXL_20",
        "seed": 6720.0,
        "qty": 7,
        "avg": 9.87,
        "cash": 1.23,
        "curr": 150.0,
        "profit_amt": 0.0,
        "profit_pct": 0.0,
        "split": 20,
        "t_val": 3.14,
        "t_revision": 99,
        "target": 20.0,
        "star_pct": 0.0,
        "star_price": 0.0,
        "one_portion": 0.0,
        "is_reverse": False,
        "is_locked": False,
        "official_balance": {"qty": 98, "avg": 158.0735, "orderable_cash": 1482.88},
        "local_ledger": {"qty": 7, "avg": 9.87, "orderable_cash": 1.23},
        "official_t_state": {"t": 18.82, "revision": 2},
        "plan": {
            "process_status": "🌓공식전반전",
            "t_val": 18.82,
            "t_revision": 2,
            "star_ratio": -0.1764,
            "star_price": 130.19,
            "one_portion": 882.67,
            "orders": [],
        },
    }
    data.update(overrides)
    return data


def _render(info):
    text, _markup = TelegramView(DummyCycleCashConfig()).create_sync_report(
        "SHADOW_ONLY",
        "2026-08-12",
        cash=999999.0,
        rp_amount=0.0,
        ticker_data=[info],
        is_trade_active=True,
    )
    return text


class DummyCycleCashConfig:
    def calculate_cycle_cash(self, ticker):
        return 1482.88, {"cycle_cash": "1482.88"}


def test_official_view_prefers_kis_qty_avg_cash_and_halts_on_local_discrepancy():
    text = _render(_base_ticker_info())

    assert "💰 보유   <b>98주 · 평단 $158.07 · 현재가 $150.00 · 잔금: KIS 1,483 / 사이클현금 1,483</b>" in text
    assert "⛔ <b>HALT" in text
    assert "KIS/local mismatch" in text


def test_official_view_displays_t_and_revision_from_official_event_state_not_legacy_estimate():
    text = _render(_base_ticker_info())

    assert "📈 진행   <b>18.8 / 20 T</b>" in text
    assert "3.1 / 20 T" not in text


def test_official_view_displays_mode_star_point_star_percent_and_one_portion_from_official_plan():
    text = _render(_base_ticker_info())

    assert "무한매수 20분할 · 전반전" in text
    assert "⭐ 별지점  <b>$130.19 (-17.6%) · 1회 매수금 $883</b>" in text


def test_official_view_never_falls_back_to_stale_t_info_one_portion_for_buy_budget():
    text = _render(
        _base_ticker_info(
            one_portion=999999.0,
            cycle_cash=1482.88,
            plan={
                "process_status": "🌓공식전반전",
                "t_val": 18.32,
                "t_revision": 2,
                "star_ratio": -0.1664,
                "star_price": 131.76,
                "official_cash": 1482.88,
                "orders": [],
            },
            official_t_state={"t": 18.32, "revision": 2},
        )
    )

    assert "1회 매수금 $883" in text
    assert "1회 매수금 $999,999" not in text


def test_official_view_displays_reverse_mode_from_official_plan():
    text = _render(_base_ticker_info(plan={"process_status": "♻️공식리버스", "orders": []}, is_reverse=True))

    assert "무한매수 20분할 · 리버스" in text


def test_official_view_renders_kst_yesterday_and_today_execution_fills():
    text = _render(
        _base_ticker_info(
            plan={
                "process_status": "🌕공식후반전",
                "orders": [],
            },
            order_statuses={
                "SUBMITTED": [],
                "PARTIAL": [],
                "FILLED": [
                    {"kis_order_no": "ODNO-1", "event_type": "FULL_BUY"},
                    {"kis_order_no": "ODNO-2", "event_type": "BONUS_BUY"},
                ],
                "CANCELLED": [],
                "REJECTED": [],
            },
            yesterday_fill_date="20260818",
            today_fill_date="20260819",
            yesterday_fills=[
                {"odno": "ODNO-1", "side": "BUY", "qty": 5, "price": "129.00"},
                {"odno": "ODNO-2", "side": "BUY", "qty": 1, "price": "129.60"},
            ],
            today_fills=[],
        )
    )

    assert "📊 <b>체결 내역 (KST)</b>" in text
    assert "📅 어제(미국장 08-18)" in text
    assert "🟢 매수 6주 @ $129.10  (별값 5주 · 보너스 1주)" in text
    assert "🔴 매도 —" in text
    assert "📅 오늘(미국장 08-19)" in text
    assert "⏳ 아직 체결 없음" in text


def test_official_view_renders_corrupt_order_intent_ledger_warning_as_halt():
    text = _render(
        _base_ticker_info(
            order_statuses={
                "SUBMITTED": [],
                "PARTIAL": [],
                "FILLED": [],
                "CANCELLED": [],
                "REJECTED": [],
            },
            order_status_warning={
                "halted": True,
                "reason": "order intent ledger corrupt: invalid JSONL line 1",
            },
        )
    )

    assert "주문 상태 원장" in text
    assert "⛔ <b>HALT" in text
    assert "order intent ledger corrupt" in text
