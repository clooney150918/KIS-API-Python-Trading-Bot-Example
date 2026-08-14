import datetime
import glob
import json
import logging
import math
import os
from pathlib import Path
from zoneinfo import ZoneInfo


TICKER = "SOXL"
SPLIT_COUNT = 20.0
EST = ZoneInfo("America/New_York")


def _safe_float(value, default=0.0):
    try:
        val = float(str(value or default).replace(",", ""))
        if math.isnan(val) or math.isinf(val):
            return default
        return val
    except Exception:
        return default


def _safe_int(value, default=0):
    try:
        return int(_safe_float(value, default))
    except Exception:
        return default


def _read_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if data is not None else default
    except Exception:
        return default


def _read_jsonl(path):
    rows = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    parsed = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(parsed, dict):
                    rows.append(parsed)
    except FileNotFoundError:
        pass
    return rows


def _previous_est_trade_date(today=None):
    today = today or datetime.datetime.now(EST).date()
    candidate = today - datetime.timedelta(days=1)
    try:
        import pandas_market_calendars as mcal

        nyse = mcal.get_calendar("NYSE")
        schedule = nyse.schedule(
            start_date=candidate - datetime.timedelta(days=10),
            end_date=candidate,
        )
        if not schedule.empty:
            return schedule.index[-1].strftime("%Y-%m-%d")
    except Exception:
        pass

    while candidate.weekday() >= 5:
        candidate -= datetime.timedelta(days=1)
    return candidate.strftime("%Y-%m-%d")


def _last_completed_trade_date(today=None):
    """가장 최근에 마감된 미국장 거래일. 오늘이 거래일이면 오늘(16:00 마감 이후), 주말/공휴일이면 직전 거래일."""
    today = today or datetime.datetime.now(EST).date()
    try:
        import pandas_market_calendars as mcal

        nyse = mcal.get_calendar("NYSE")
        schedule = nyse.schedule(
            start_date=today - datetime.timedelta(days=10),
            end_date=today,
        )
        if not schedule.empty:
            return schedule.index[-1].strftime("%Y-%m-%d")
    except Exception:
        pass

    if today.weekday() < 5:
        return today.strftime("%Y-%m-%d")
    return _previous_est_trade_date(today)


def _find_snapshot(trade_date, ticker):
    exact = Path("data") / f"daily_snapshot_V4_{trade_date}_{ticker}.json"
    if exact.exists():
        return _read_json(exact, {})
    candidates = []
    for path in glob.glob(f"data/daily_snapshot_V4_*_{ticker}.json"):
        data = _read_json(path, {})
        if isinstance(data, dict):
            candidates.append((str(data.get("date") or ""), path, data))
    if not candidates:
        return {}
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[-1][2]


def _load_current_balance(ticker, broker=None):
    if broker is not None and hasattr(broker, "get_account_balance"):
        try:
            result = broker.get_account_balance()
            if isinstance(result, (tuple, list)) and len(result) >= 2 and isinstance(result[1], dict):
                holding = result[1].get(ticker) or {}
                if isinstance(holding, dict):
                    qty = _safe_int(holding.get("qty"))
                    avg = _safe_float(holding.get("avg", holding.get("avg_price")))
                    cash = _safe_float(result[0])
                    if qty > 0 or avg > 0:
                        return {"qty": qty, "avg_price": avg, "cash": cash}
        except Exception as exc:
            logging.warning("daily report KIS balance fallback to local file: %s", exc)

    data = _read_json(Path("data") / "kis_balance.json", {})
    holding = data.get(ticker) if isinstance(data, dict) else {}
    if not isinstance(holding, dict):
        holding = {}
    return {
        "qty": _safe_int(holding.get("qty")),
        "avg_price": _safe_float(holding.get("avg_price", holding.get("avg"))),
        "cash": _safe_float(holding.get("cash", holding.get("orderable_cash"))),
    }


def _load_current_t(config, ticker):
    try:
        state = config.get_official_t_state(ticker)
        if isinstance(state, dict):
            return _safe_float(state.get("t")), _safe_float(state.get("available_cash"))
    except Exception as exc:
        logging.warning("daily report official T load failed: %s", exc)
    data = _read_json(Path("data") / "t_state.json", {})
    state = data.get(ticker) if isinstance(data, dict) else {}
    return _safe_float(state.get("t_val", state.get("t")) if isinstance(state, dict) else 0.0), 0.0


def _load_order_metadata(ticker):
    by_order_no = {}
    by_intent_id = {}
    path = Path("data") / f"order_intents_{ticker}.jsonl"
    for row in _read_jsonl(path):
        if str(row.get("ticker", "")).upper() != ticker:
            continue
        accepted = row.get("accepted_order") if isinstance(row.get("accepted_order"), dict) else {}
        order_no = str(accepted.get("order_no") or row.get("kis_order_no") or "").strip()
        if order_no:
            by_order_no[order_no] = row
        intent_id = str(row.get("intent_id") or "").strip()
        if intent_id:
            by_intent_id[intent_id] = row
    return by_order_no, by_intent_id


def _classify_fill(fill, snapshot, order_by_no):
    order = order_by_no.get(str(fill.get("kis_order_no") or "").strip())
    if isinstance(order, dict) and order.get("event_type"):
        return str(order.get("event_type")).upper()

    side = str(fill.get("side", "")).upper()
    qty = _safe_int(fill.get("qty"))
    price = _safe_float(fill.get("price"))
    for order in snapshot.get("orders", []) if isinstance(snapshot, dict) else []:
        if not isinstance(order, dict) or str(order.get("side", "")).upper() != side:
            continue
        if _safe_int(order.get("qty")) == qty and abs(_safe_float(order.get("price")) - price) < 1.0:
            return str(order.get("event_type") or "").upper()
    return "FULL" if side == "BUY" else "QUARTER"


def _aggregate_fills(fills, snapshot, order_by_no):
    result = {
        "FULL": {"qty": 0, "amount": 0.0, "side": "BUY"},
        "BONUS": {"qty": 0, "amount": 0.0, "side": "BUY"},
        "QUARTER": {"qty": 0, "amount": 0.0, "side": "SELL"},
        "TARGET_FULL": {"qty": 0, "amount": 0.0, "side": "SELL"},
    }
    for fill in fills:
        event_type = _classify_fill(fill, snapshot, order_by_no)
        if event_type not in result:
            continue
        qty = _safe_int(fill.get("qty"))
        price = _safe_float(fill.get("price"))
        result[event_type]["qty"] += qty
        result[event_type]["amount"] += qty * price
        result[event_type]["side"] = str(fill.get("side") or result[event_type]["side"]).upper()
    for item in result.values():
        item["price"] = item["amount"] / item["qty"] if item["qty"] > 0 else 0.0
    return result


def _planned_qty(snapshot, event_type):
    return sum(
        _safe_int(order.get("qty"))
        for order in snapshot.get("orders", []) if isinstance(order, dict)
        if str(order.get("event_type", "")).upper() == event_type
    )


def _planned_qtys(snapshot, ticker, trade_date):
    planned = {
        event_type: _planned_qty(snapshot, event_type)
        for event_type in ("FULL", "BONUS", "QUARTER", "TARGET_FULL")
    }
    from_intents = {event_type: 0 for event_type in planned}
    seen_intents = set()
    for row in _read_jsonl(Path("data") / f"order_intents_{ticker}.jsonl"):
        if str(row.get("ticker", "")).upper() != ticker:
            continue
        if str(row.get("trade_date")) != trade_date:
            continue
        if str(row.get("status", "")).upper() not in {"PLANNED", "SUBMITTED"}:
            continue
        event_type = str(row.get("event_type", "")).upper()
        if event_type not in from_intents:
            continue
        intent_id = str(row.get("intent_id") or "").strip()
        if intent_id:
            if intent_id in seen_intents:
                continue
            seen_intents.add(intent_id)
        from_intents[event_type] += _safe_int(row.get("qty"))
    for event_type, qty in from_intents.items():
        if planned[event_type] <= 0 and qty > 0:
            planned[event_type] = qty
    return planned


def _fmt_money(value, decimals=0, signed=False):
    val = _safe_float(value)
    sign = "+" if signed and val > 0 else "-" if signed and val < 0 else ""
    return f"{sign}${abs(val):,.{decimals}f}"


def _fmt_signed_number(value, decimals=1):
    val = _safe_float(value)
    return f"{val:+.{decimals}f}"


def _fmt_signed_int(value):
    val = _safe_int(value)
    return f"{val:+d}"


def _format_fill_line(label, event_type, agg, planned_qty):
    qty = _safe_int(agg[event_type]["qty"])
    planned = _safe_int(planned_qty.get(event_type)) if isinstance(planned_qty, dict) else 0
    if qty > 0:
        return f"  {label}  {qty}주 @ ${agg[event_type]['price']:.2f} ✅"
    return f"  {label}  0/{planned} (미체결)"


def _orders_by_event(plan):
    grouped = {"FULL": [], "BONUS": [], "QUARTER": [], "TARGET_FULL": []}
    for order in plan.get("orders", []) if isinstance(plan, dict) else []:
        if not isinstance(order, dict):
            continue
        event_type = str(order.get("event_type") or "").upper()
        if event_type in grouped:
            grouped[event_type].append(order)
    return grouped


def _format_plan_line(icon, side_label, name, orders, ladder=False):
    qty = sum(_safe_int(order.get("qty")) for order in orders)
    amount = sum(_safe_int(order.get("qty")) * _safe_float(order.get("price")) for order in orders)
    if qty <= 0:
        return f"  {icon} {side_label}  {name}  0주   $0"
    if ladder:
        shown = orders[:5]
        qty = sum(_safe_int(order.get("qty")) for order in shown)
        amount = sum(_safe_int(order.get("qty")) * _safe_float(order.get("price")) for order in shown)
        return f"  {icon} {side_label}  {name}  {qty}주   {_fmt_money(amount)}    (사다리 최대)"
    price = _safe_float(orders[0].get("price"))
    return f"  {icon} {side_label}  {name}  {qty}주   {_fmt_money(amount)}    (@${price:.2f})"


def _current_price(ticker, broker, snapshot):
    if broker is not None and hasattr(broker, "get_current_price"):
        try:
            price = _safe_float(broker.get_current_price(ticker))
            if price > 0:
                return price
        except Exception as exc:
            logging.warning("daily report current price fallback: %s", exc)
    for key in ("current_price", "close", "star_price"):
        price = _safe_float(snapshot.get(key) if isinstance(snapshot, dict) else 0.0)
        if price > 0:
            return price
    fills = _read_jsonl(Path("data") / f"execution_ledger_{ticker}.jsonl")
    for fill in reversed(fills):
        price = _safe_float(fill.get("price"))
        if price > 0:
            return price
    return 0.0


def _realized_pnl_from_ledger(config, ticker):
    try:
        ledger = config.get_ledger() if hasattr(config, "get_ledger") else _read_json(Path("data") / "manual_ledger.json", [])
    except Exception:
        ledger = _read_json(Path("data") / "manual_ledger.json", [])
    if not isinstance(ledger, list):
        return 0.0
    lots = []
    realized = 0.0
    for row in ledger:
        if not isinstance(row, dict) or str(row.get("ticker", "")).upper() != ticker:
            continue
        side = str(row.get("side", "")).upper()
        qty = _safe_int(row.get("qty"))
        price = _safe_float(row.get("price"))
        if qty <= 0 or price <= 0:
            continue
        if side == "BUY":
            lots.append([qty, price])
        elif side == "SELL":
            remaining = qty
            while remaining > 0 and lots:
                lot_qty, lot_price = lots[0]
                take = min(remaining, lot_qty)
                realized += (price - lot_price) * take
                lot_qty -= take
                remaining -= take
                if lot_qty <= 0:
                    lots.pop(0)
                else:
                    lots[0][0] = lot_qty
            if remaining > 0:
                realized += price * remaining
    return realized


def _cash_from_snapshot_and_fills(snapshot, fills, current_t, official_cash=0.0):
    if official_cash > 0 and not isinstance(snapshot, dict):
        return official_cash
    prev_t = _safe_float(snapshot.get("t_val") if isinstance(snapshot, dict) else 0.0)
    prev_one = _safe_float(snapshot.get("one_portion") if isinstance(snapshot, dict) else 0.0)
    if official_cash > 0 and prev_one <= 0:
        return official_cash
    cash = prev_one * max(0.0, SPLIT_COUNT - prev_t)
    for fill in fills:
        amount = _safe_int(fill.get("qty")) * _safe_float(fill.get("price"))
        side = str(fill.get("side", "")).upper()
        if side == "BUY":
            cash -= amount
        elif side == "SELL":
            cash += amount
    if cash <= 0 and current_t < SPLIT_COUNT:
        current_one = _safe_float(snapshot.get("one_portion") if isinstance(snapshot, dict) else 0.0)
        cash = current_one * max(0.0, SPLIT_COUNT - current_t)
    return max(0.0, cash)


def build_daily_report(config, broker, strategy, view=None):
    today_est = datetime.datetime.now(EST).date()
    trade_date = _last_completed_trade_date(today_est)
    snapshot = _find_snapshot(trade_date, TICKER)

    ledger_path = Path(getattr(config, "FILES", {}).get("EXECUTION_LEDGER", f"data/execution_ledger_{TICKER}.jsonl"))
    fills = [
        row for row in _read_jsonl(ledger_path)
        if str(row.get("source")) == "KIS_CONFIRMED_FILL"
        and str(row.get("ticker", "")).upper() == TICKER
        and str(row.get("trade_date")) == trade_date
    ]

    current_balance = _load_current_balance(TICKER, broker)
    current_t, official_cash = _load_current_t(config, TICKER)
    current_qty = _safe_int(current_balance.get("qty"))
    current_avg = _safe_float(current_balance.get("avg_price"))
    # 잔금은 스냅샷 기준 + 전일 체결 반영으로 산출 (KIS 예수금은 결제지연으로 스테일될 수 있음)
    current_cash = _cash_from_snapshot_and_fills(snapshot, fills, current_t, official_cash=official_cash)
    current_one = current_cash / max(1.0, SPLIT_COUNT - current_t)
    price = _current_price(TICKER, broker, snapshot)

    if not fills:
        return (
            f"📋 [ {TICKER} 일일 리포트 · {trade_date} 미국장 ]\n"
            "💤 전일 체결 없음 (관망)\n"
            f"📊 T {current_t:.1f}/20 · 보유 {current_qty}주 · 잔금 {_fmt_money(current_cash)}"
        )

    prev_t = _safe_float(snapshot.get("t_val") if isinstance(snapshot, dict) else 0.0)
    prev_qty = _safe_int(snapshot.get("total_q") if isinstance(snapshot, dict) else 0)
    prev_one = _safe_float(snapshot.get("one_portion") if isinstance(snapshot, dict) else 0.0)
    prev_cash = prev_one * max(0.0, SPLIT_COUNT - prev_t)

    prev_close = price or _safe_float(snapshot.get("star_price") if isinstance(snapshot, dict) else 0.0)
    try:
        today_plan = strategy.get_plan(
            TICKER, price, current_avg, current_qty, prev_close,
            ma_5day=0.0, market_type="REG", available_cash=current_cash,
            is_simulation=True, is_snapshot_mode=False,
        )
    except Exception as exc:
        logging.warning("daily report plan build failed: %s", exc)
        today_plan = {}
    grouped_orders = _orders_by_event(today_plan)

    order_by_no, _ = _load_order_metadata(TICKER)
    aggregated = _aggregate_fills(fills, snapshot, order_by_no)
    planned_qty = _planned_qtys(snapshot, TICKER, trade_date)
    eval_amount = (price - current_avg) * current_qty if price > 0 and current_avg > 0 and current_qty > 0 else 0.0
    eval_pct = ((price - current_avg) / current_avg * 100.0) if price > 0 and current_avg > 0 else 0.0
    realized = _realized_pnl_from_ledger(config, TICKER)

    lines = [
        f"📋 [ {TICKER} 일일 리포트 · {trade_date} 미국장 ]",
        "",
        "📌 전일 체결",
        _format_fill_line("🔴 별값매수", "FULL", aggregated, planned_qty),
        _format_fill_line("🔴 줍줍", "BONUS", aggregated, planned_qty),
        _format_fill_line("🔵 쿼터매도", "QUARTER", aggregated, planned_qty),
        _format_fill_line("🔵 목표매도", "TARGET_FULL", aggregated, planned_qty),
        "",
        "📊 변화 (전일 → 오늘)",
        f"  T      {prev_t:.1f}  →  {current_t:.1f}    ({_fmt_signed_number(current_t - prev_t)})",
        f"  보유    {prev_qty}주  →  {current_qty}주    ({_fmt_signed_int(current_qty - prev_qty)})",
        f"  잔금   {_fmt_money(prev_cash)} → {_fmt_money(current_cash)}   ({_fmt_money(current_cash - prev_cash, signed=True)})",
        f"  매수금  {_fmt_money(prev_one)}  →  {_fmt_money(current_one)}    ({_fmt_money(current_one - prev_one, signed=True)})",
        "",
        "🎯 오늘 예정 체결",
        _format_plan_line("🔴", "매수", "별값", grouped_orders["FULL"]),
        _format_plan_line("🔴", "매수", "줍줍", grouped_orders["BONUS"], ladder=True),
        _format_plan_line("🔵", "매도", "쿼터", grouped_orders["QUARTER"]),
        _format_plan_line("🔵", "매도", "목표", grouped_orders["TARGET_FULL"]),
        "",
        "💰 손익",
        f"  평가  {eval_pct:+.1f}% ({_fmt_money(eval_amount, signed=True)}) · 실현 {_fmt_money(realized, signed=True)}",
    ]
    return "\n".join(lines)


async def scheduled_daily_report(context):
    try:
        app_data = {}
        if getattr(context, "job", None) is not None and isinstance(context.job.data, dict):
            app_data = context.job.data
        elif isinstance(getattr(context, "bot_data", None), dict):
            app_data = context.bot_data.get("app_data", {}) or {}

        text = build_daily_report(
            app_data.get("cfg"),
            app_data.get("broker"),
            app_data.get("strategy"),
            view=app_data.get("view"),
        )
        chat_id = (
            getattr(getattr(context, "job", None), "chat_id", None)
            or app_data.get("admin_chat_id")
            or os.getenv("ADMIN_CHAT_ID")
        )
        if chat_id:
            await context.bot.send_message(chat_id=int(chat_id), text=text)
    except Exception:
        logging.exception("scheduled daily report failed")
