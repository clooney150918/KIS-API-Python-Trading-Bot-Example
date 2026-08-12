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
    text, _markup = TelegramView().create_sync_report(
        "SHADOW_ONLY",
        "2026-08-12",
        cash=999999.0,
        rp_amount=0.0,
        ticker_data=[info],
        is_trade_active=True,
    )
    return text


def test_official_view_prefers_kis_qty_avg_cash_and_halts_on_local_discrepancy():
    text = _render(_base_ticker_info())

    assert "KIS 보유수량: <b>98주</b>" in text
    assert "KIS 평단: <b>$158.07</b>" in text
    assert "KIS 주문가능 예수금: <b>$1,482.88</b>" in text
    assert "로컬 장부: 7주 @ $9.87" in text
    assert "⛔ <b>HALT" in text
    assert "KIS/local mismatch" in text
    assert "💰 현재 $150.00 / 평단 $158.07 (98주)" in text


def test_official_view_displays_t_and_revision_from_official_event_state_not_legacy_estimate():
    text = _render(_base_ticker_info())

    assert "공식 T: <b>18.82T</b> (revision <b>2</b>)" in text
    assert "3.1400T" not in text
    assert "revision <b>99</b>" not in text


def test_official_view_displays_mode_star_point_star_percent_and_one_portion_from_official_plan():
    text = _render(_base_ticker_info())

    assert "공식 모드: <b>전반</b>" in text
    assert "별%: <b>-17.64%</b>" in text
    assert "별지점: <b>$130.19</b>" in text
    assert "1회 매수금: <b>$882.67</b>" in text


def test_official_view_displays_reverse_mode_from_official_plan():
    text = _render(_base_ticker_info(plan={"process_status": "♻️공식리버스", "orders": []}, is_reverse=True))

    assert "공식 모드: <b>리버스</b>" in text


def test_official_view_renders_order_states_separately_without_collapsing_into_filled():
    text = _render(
        _base_ticker_info(
            order_statuses={
                "SUBMITTED": [{"intent_id": "i-sub", "qty": 1}],
                "PARTIAL": [{"intent_id": "i-part", "qty": 2}],
                "FILLED": [{"intent_id": "i-fill", "qty": 3}],
                "CANCELLED": [{"intent_id": "i-cancel", "qty": 4}],
                "REJECTED": [{"intent_id": "i-reject", "qty": 5}],
            }
        )
    )

    assert "주문접수(SUBMITTED): <b>1건</b>" in text
    assert "부분체결(PARTIAL): <b>1건</b>" in text
    assert "체결완료(FILLED): <b>1건</b>" in text
    assert "취소(CANCELLED): <b>1건</b>" in text
    assert "거절(REJECTED): <b>1건</b>" in text


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
