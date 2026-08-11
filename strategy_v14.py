# ==========================================================
# FILE: strategy_v14.py
# ==========================================================
# 🚨 MODIFIED: [Lost Update 궁극 방어] 스냅샷 파일 I/O 전역에 GlobalThrottle.get_file_lock()을 100% 팩트 래핑 완료.
# 🚨 MODIFIED: [TypeError 런타임 붕괴 방어] datetime.time 충돌 소각 및 정수 연산 락온 유지.
# 🚨 NEW: [0주 스냅샷 오염 팩트 자가치유(Self-Healing) 결속] 통신 지연으로 YF 종가가 훼손되어 스냅샷 타점이 0.0 등으로 오염될 경우, 로드 시 공식 종가(prev_c) 기반으로 타점($)과 수량(Q)을 원자적으로 재계산하여 덮어쓰는(Self-Healing) 방어망 100% 락온 (Case 46 방어 강화).
# 🚨 MODIFIED: [리버스 자본 격리 아키텍처 결속] 스냅샷 생성 모드(is_snapshot_mode=True)일 때만 cfg.apply_reverse_daily_settlement(ticker)를 호출하여 1일 1회 원자적 장부 정산이 실행되도록 팩트 락온.
# ==========================================================
import math
import os
import json
import tempfile
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from global_throttle import GlobalThrottle

class V4Strategy:
    def __init__(self, config):
        self.cfg = config

    def _safe_float(self, value):
        try:
            val = float(str(value or 0.0).replace(',', ''))
            if math.isnan(val) or math.isinf(val):
                return 0.0
            return val
        except Exception:
            return 0.0

    def _ceil(self, val): return math.ceil(self._safe_float(val) * 100) / 100.0
    def _floor(self, val): return math.floor(self._safe_float(val) * 100) / 100.0

    def _get_logical_date_str(self):
        now_est = datetime.now(ZoneInfo('America/New_York'))
        
        if now_est.hour < 4 or (now_est.hour == 4 and now_est.minute < 4):
            target_date = now_est - timedelta(days=1)
        elif now_est.hour >= 16: 
            target_date = now_est + timedelta(days=1)
        else:
            target_date = now_est
            
        if target_date.weekday() == 5: 
            target_date += timedelta(days=2)
        elif target_date.weekday() == 6: 
            target_date += timedelta(days=1)
            
        return target_date.strftime("%Y-%m-%d")

    def save_daily_snapshot(self, ticker, plan_data):
        today_str = self._get_logical_date_str()
        snap_file = f"data/daily_snapshot_V4_{today_str}_{ticker}.json"

        safe_plan = plan_data if isinstance(plan_data, dict) else {}
        
        data = {
            "date": today_str,
            "strategy": str(safe_plan.get('strategy', '')),
            "strategy_revision": int(self._safe_float(safe_plan.get('strategy_revision', 0))),
            "t_revision": int(self._safe_float(safe_plan.get('t_revision', 0))),
            "trade_date": str(safe_plan.get('trade_date', today_str)),
            "ticker": str(ticker or "").strip().upper(),
            "intent_ids": safe_plan.get('intent_ids') or [],
            "source_balance_at": str(safe_plan.get('source_balance_at', '')),
            "total_q": int(self._safe_float(safe_plan.get('total_q', 0))),
            "avg_price": self._safe_float(safe_plan.get('avg_price', 0.0)),
            "one_portion": self._safe_float(safe_plan.get('one_portion', 0.0)),
            "star_price": self._safe_float(safe_plan.get('star_price', 0.0)),
            "star_ratio": self._safe_float(safe_plan.get('star_ratio', 0.0)),
            "target_price": self._safe_float(safe_plan.get('target_price', 0.0)), 
            "t_val": self._safe_float(safe_plan.get('t_val', 0)),
            "is_reverse": bool(safe_plan.get('is_reverse', False)),
            "is_zero_start": bool(safe_plan.get('is_zero_start', False)),
            "initial_qty": int(self._safe_float(safe_plan.get('initial_qty', 0))),
            "orders": safe_plan.get('orders') or [],
            "core_orders": safe_plan.get('core_orders') or [],
            "bonus_orders": safe_plan.get('bonus_orders') or [],
            "process_status": str(safe_plan.get('process_status', '')),
            "safety": safe_plan.get('safety'),
        }
        
        with GlobalThrottle.get_file_lock(snap_file):
            dir_name = os.path.dirname(snap_file)
            if dir_name:
                try: os.makedirs(dir_name, exist_ok=True)
                except OSError: pass
                
            fd = None
            temp_path = None
            try:
                fd, temp_path = tempfile.mkstemp(dir=dir_name or '.', text=True)
                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                    fd = None
                    json.dump(data, f, ensure_ascii=False, indent=4)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(temp_path, snap_file)
                temp_path = None
            except Exception as e:
                logging.error(f"🚨 [{ticker}] 스냅샷 파일 저장 실패: {e}")
                if fd is not None:
                    try: os.close(fd)
                    except OSError: pass
                if temp_path:
                    try: os.remove(temp_path)
                    except OSError: pass

    def load_daily_snapshot(self, ticker):
        today_str = self._get_logical_date_str()
        snap_file = f"data/daily_snapshot_V4_{today_str}_{ticker}.json"

        with GlobalThrottle.get_file_lock(snap_file):
            try:
                with open(snap_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict) and data.get("date") == today_str:
                        return data
            except Exception:
                pass
            return None

    def _apply_wash_trade_shield(self, c_orders, b_orders):
        all_o = c_orders + b_orders
        has_sell_moc = any(isinstance(o, dict) and o.get('type') in ['MOC', 'MOO'] and o.get('side') == 'SELL' for o in all_o)
        s_prices = [self._safe_float(o.get('price')) for o in all_o if isinstance(o, dict) and o.get('side') == 'SELL' and self._safe_float(o.get('price')) > 0]
        min_s = min(s_prices) if s_prices else 0.0

        def _clean(lst):
            res = []
            for o in lst:
                if not isinstance(o, dict): continue
                new_o = o.copy()
                if new_o.get('side') == 'BUY':
                    if has_sell_moc and new_o.get('type') in ['LOC', 'MOC']: 
                        continue 
                    if min_s > 0 and self._safe_float(new_o.get('price')) >= min_s:
                        new_o['price'] = round(min_s - 0.01, 2)
                    if "🛡️" not in str(new_o.get('desc', '')): 
                            new_o['desc'] = f"🛡️교정_{str(new_o.get('desc', '')).replace('🧹', '')}"
                # KIS market-on-close/open bodies must retain their zero wire price.
                if new_o.get('type') in ['MOC', 'MOO']:
                    new_o['price'] = 0.0
                else:
                    new_o['price'] = max(0.01, self._safe_float(new_o.get('price')))
                res.append(new_o)
            return res
        return _clean(c_orders), _clean(b_orders)

    def _empty_official_plan(self, ticker, qty=0, avg_price=0.0, t_val=0.0, *, reason="", process_status="⛔OFFICIAL_HALTED"):
        return {
            "strategy": "LAOER_V4_SOXL_20",
            "strategy_revision": 1,
            "t_revision": 0,
            "trade_date": self._get_logical_date_str(),
            "ticker": str(ticker or "").strip().upper(),
            "orders": [], "core_orders": [], "bonus_orders": [],
            "total_q": int(self._safe_float(qty)), "avg_price": self._safe_float(avg_price),
            "t_val": self._safe_float(t_val), "one_portion": 0.0,
            "process_status": process_status, "is_reverse": False,
            "star_price": 0.0, "star_ratio": 0.0, "target_price": 0.0,
            "real_cash_used": 0.0, "tracking_info": {}, "initial_qty": int(self._safe_float(qty)),
            "is_zero_start": int(self._safe_float(qty)) == 0,
            "intent_ids": [], "source_balance_at": "",
            "safety": {"halted": True, "reason": reason or process_status},
        }

    def _load_official_state(self, ticker):
        from trade_state_store import TradeStateStore
        target = str(ticker or "").strip().upper()
        store = TradeStateStore(self.cfg.FILES["STRATEGY_BASELINE"], self.cfg.FILES["T_EVENTS"])
        baseline = store.load_baseline(target)
        state = store.load_state(target)
        status_setter = getattr(self.cfg, "_set_t_event_status", None)
        if callable(status_setter):
            status_setter(target, True)
        return baseline, state

    def _official_intent_id(self, order):
        from order_intent_store import compute_intent_id
        return compute_intent_id(order)

    def _official_order(self, *, ticker, trade_date, t_revision, event_type, side, order_type, price, qty, desc="", risk_reference_price=None):
        price_text = f"{self._safe_float(price):.2f}"
        order = {
            "strategy": "LAOER_V4_SOXL_20",
            "strategy_revision": 1,
            "t_revision": int(t_revision),
            "trade_date": trade_date,
            "ticker": str(ticker).upper(),
            "event_type": str(event_type).upper(),
            "side": str(side).upper(),
            "order_type": str(order_type).upper(),
            "type": str(order_type).upper(),
            "price": price_text,
            "qty": int(qty),
            "desc": desc,
        }
        if risk_reference_price is not None:
            order["risk_reference_price"] = self._safe_float(risk_reference_price)
        order["intent_id"] = self._official_intent_id(order)
        return order

    def _snapshot_matches_official_revision(self, snapshot, *, strategy_revision, t_revision):
        """Legacy compatibility probe only; cached orders are never trusted.

        Task 7 fail-closed rule: a same-revision snapshot can be contaminated with
        arbitrary orders/bonus_orders or mismatched intent_ids. The official path
        must recompute through laoer_v4_20 and may only save fresh canonical
        results, never return cached snapshot orders.
        """
        if not isinstance(snapshot, dict):
            return False
        try:
            return (
                snapshot.get("strategy") == "LAOER_V4_SOXL_20"
                and int(snapshot.get("strategy_revision")) == int(strategy_revision)
                and int(snapshot.get("t_revision")) == int(t_revision)
                and isinstance(snapshot.get("intent_ids"), list)
            )
        except Exception:
            return False

    def get_plan(self, ticker, current_price, avg_price, qty, prev_close, ma_5day=0.0, market_type="REG", available_cash=0, is_simulation=False, is_snapshot_mode=False, **kwargs):
        import laoer_v4_20

        target = str(ticker or "").strip().upper()
        current_price = self._safe_float(current_price)
        avg_price = self._safe_float(avg_price)
        qty = int(self._safe_float(qty))
        real_available_cash = max(0.0, self._safe_float(available_cash))
        trade_date = self._get_logical_date_str()
        strategy_revision = 1

        if target != "SOXL":
            return self._empty_official_plan(
                target, qty, avg_price,
                reason="Official strategy only supports SOXL 20-split; non-SOXL orders are halted",
                process_status="⛔SOXL전용HALT",
            )

        # KIS 체결 재구성에 수동검토 항목이 있으면 실제 주문만 HALT한다.
        t_states = self.cfg._load_json(self.cfg.FILES.get("T_STATE", "data/t_state.json"), {})
        t_state = t_states.get(target, {}) if isinstance(t_states, dict) else {}
        if isinstance(t_state, dict) and t_state.get("needs_manual_review", False) and not is_simulation:
            t_val = round(self._safe_float(t_state.get("t_val", 0.0)), 2)
            return self._empty_official_plan(
                target, qty, avg_price, t_val,
                reason=t_state.get("review_reason", "T값 수동검토 필요"),
                process_status="⛔T값 수동검토",
            )

        if "STRATEGY_BASELINE" not in getattr(self.cfg, "FILES", {}):
            plan_result = self._empty_official_plan(
                target, qty, avg_price,
                reason="STRATEGY_BASELINE missing; official kernel baseline is required",
                process_status="⛔공식기준원장HALT",
            )
            if is_snapshot_mode: self.save_daily_snapshot(target, plan_result)
            return plan_result

        try:
            baseline, official_state = self._load_official_state(target)
        except Exception as exc:
            status_setter = getattr(self.cfg, "_set_t_event_status", None)
            if callable(status_setter):
                status_setter(target, False, exc)
            logging.error(f"⛔ [{target}] T 이벤트 원장 로드 실패 — official adapter fail-closed: {exc}")
            plan_result = self._empty_official_plan(
                target, qty, avg_price,
                reason=f"T event ledger unavailable: {exc}",
                process_status="⛔T이벤트원장HALT",
            )
            if is_snapshot_mode: self.save_daily_snapshot(target, plan_result)
            return plan_result

        t_revision = int(official_state.revision)
        t_val_dec = official_state.t
        t_val = float(t_val_dec)
        official_cash = official_state.available_cash
        split = int(self._safe_float(self.cfg.get_split_count(target)) or 20)
        if split != 20:
            plan_result = self._empty_official_plan(
                target, qty, avg_price, t_val,
                reason="Official strategy only supports SOXL 20-split; configured split is not 20",
                process_status="⛔20분할전용HALT",
            )
            plan_result["t_revision"] = t_revision
            plan_result["source_balance_at"] = baseline.get("as_of", "")
            if is_snapshot_mode: self.save_daily_snapshot(target, plan_result)
            return plan_result

        if not is_snapshot_mode:
            # Never return cached snapshot orders on the official path. Same-revision
            # snapshots are treated as stale metadata because historical files can
            # contain legacy orders, bonus_orders, or forged intent_ids.
            self.load_daily_snapshot(target)

        rev_state = self.cfg.get_reverse_state(target)
        is_rev_active = bool(rev_state.get("is_active", False)) or bool(official_state.reverse_active)

        orders = []
        is_reverse = False
        process_status = "공식SOXL20"
        star_price = 0.0
        star_ratio = 0.0
        target_price = 0.0
        one_portion = 0.0
        kernel_fail_closed = False
        kernel_reason = ""

        if is_rev_active:
            is_reverse = True
            reverse_t = rev_state.get("dynamic_t", t_val_dec) or t_val_dec
            reverse_cash = rev_state.get("rem_cash", official_cash) or official_cash
            day = int(self._safe_float(rev_state.get("day_count", 1)))
            if day <= 0:
                day = 1
            previous_quantity = int(self._safe_float(rev_state.get("previous_quantity", baseline.get("qty", qty))))
            if day <= 1:
                previous_closes = []
            else:
                previous_closes = kwargs.get("previous_closes") or []
                if not previous_closes and ma_5day:
                    previous_closes = [ma_5day] * 5
                elif not previous_closes and prev_close:
                    previous_closes = [prev_close] * 5
            reverse_plan = laoer_v4_20.calculate_reverse_plan(
                laoer_v4_20.ReverseState(
                    ticker=target,
                    split=20,
                    quantity=qty,
                    previous_quantity=previous_quantity,
                    avg_price=laoer_v4_20.Decimal(str(avg_price)),
                    cash=laoer_v4_20.Decimal(str(reverse_cash)),
                    t=laoer_v4_20.Decimal(str(reverse_t)),
                    day=day,
                    previous_closes=previous_closes,
                    confirmed_close=kwargs.get("confirmed_close"),
                )
            )
            t_val = float(reverse_plan.t)
            star_price = float(reverse_plan.star_point)
            one_portion = float(reverse_plan.buy_budget)
            kernel_fail_closed = bool(reverse_plan.fail_closed)
            kernel_reason = reverse_plan.reason
            process_status = "♻️공식리버스"
            if reverse_plan.return_to_normal:
                process_status = "♻️리버스복귀대기"
            elif not reverse_plan.fail_closed:
                if reverse_plan.buy_quantity > 0:
                    orders.append(self._official_order(
                        ticker=target, trade_date=trade_date, t_revision=t_revision,
                        event_type="FULL", side="BUY", order_type="LOC",
                        price=reverse_plan.star_buy_price, qty=reverse_plan.buy_quantity,
                        desc="공식리버스매수",
                    ))
                if reverse_plan.sell_quantity > 0:
                    if reverse_plan.sell_order_type == "MOC" and current_price <= 0.0:
                        kernel_fail_closed = True
                        kernel_reason = "MOC risk reference price must be positive and finite"
                    else:
                        sell_price = reverse_plan.star_point if reverse_plan.sell_order_type == "LOC" else current_price
                        orders.append(self._official_order(
                            ticker=target, trade_date=trade_date, t_revision=t_revision,
                            event_type="QUARTER", side="SELL", order_type=reverse_plan.sell_order_type,
                            price=sell_price, qty=reverse_plan.sell_quantity,
                            desc="공식리버스매도",
                            risk_reference_price=(current_price if reverse_plan.sell_order_type == "MOC" else None),
                        ))
        else:
            normal_plan = laoer_v4_20.calculate_normal_plan(
                laoer_v4_20.NormalState(
                    ticker=target,
                    split=20,
                    quantity=qty,
                    avg_price=laoer_v4_20.Decimal(str(avg_price)),
                    cash=official_cash,
                    t=t_val_dec,
                    reverse=False,
                )
            )
            star_price = float(normal_plan.star_point)
            star_ratio = float(normal_plan.star_percent / laoer_v4_20.Decimal("100")) if hasattr(laoer_v4_20, "Decimal") else float(normal_plan.star_percent / __import__('decimal').Decimal("100"))
            target_price = float(normal_plan.target_sell_price)
            one_portion = float(normal_plan.one_buy_budget)
            kernel_fail_closed = bool(normal_plan.fail_closed)
            kernel_reason = normal_plan.reason
            process_status = "🌓공식전반전" if normal_plan.phase == "FIRST_HALF" else "🌕공식후반전"
            if normal_plan.reverse_entry:
                process_status = "♻️공식리버스진입대기"
            elif not normal_plan.fail_closed:
                if normal_plan.average_buy_quantity > 0:
                    orders.append(self._official_order(
                        ticker=target, trade_date=trade_date, t_revision=t_revision,
                        event_type="HALF", side="BUY", order_type="LOC",
                        price=normal_plan.average_buy_price, qty=normal_plan.average_buy_quantity,
                        desc="공식평단매수",
                    ))
                if normal_plan.star_buy_quantity > 0:
                    event_type = "HALF" if normal_plan.average_buy_quantity > 0 else "FULL"
                    orders.append(self._official_order(
                        ticker=target, trade_date=trade_date, t_revision=t_revision,
                        event_type=event_type, side="BUY", order_type="LOC",
                        price=normal_plan.star_buy_price, qty=normal_plan.star_buy_quantity,
                        desc="공식별값매수",
                    ))
                if normal_plan.quarter_sell_quantity > 0:
                    orders.append(self._official_order(
                        ticker=target, trade_date=trade_date, t_revision=t_revision,
                        event_type="QUARTER", side="SELL", order_type="LOC",
                        price=normal_plan.star_point, qty=normal_plan.quarter_sell_quantity,
                        desc="공식별값매도",
                    ))
                if normal_plan.target_sell_quantity > 0:
                    orders.append(self._official_order(
                        ticker=target, trade_date=trade_date, t_revision=t_revision,
                        event_type="TARGET_FULL", side="SELL", order_type="LIMIT",
                        price=normal_plan.target_sell_price, qty=normal_plan.target_sell_quantity,
                        desc="공식목표매도",
                    ))

        if kernel_fail_closed:
            orders = []

        plan_result = {
            "strategy": "LAOER_V4_SOXL_20",
            "strategy_revision": strategy_revision,
            "t_revision": t_revision,
            "trade_date": trade_date,
            "ticker": target,
            "orders": orders,
            "core_orders": orders,
            "bonus_orders": [],
            "total_q": qty,
            "avg_price": avg_price,
            "t_val": t_val,
            "one_portion": one_portion,
            "process_status": process_status if not kernel_fail_closed else "⛔공식커널HALT",
            "is_reverse": is_reverse,
            "star_price": star_price,
            "star_ratio": star_ratio,
            "target_price": target_price,
            "real_cash_used": real_available_cash,
            "tracking_info": {},
            "initial_qty": qty,
            "is_zero_start": qty == 0,
            "intent_ids": [o["intent_id"] for o in orders],
            "source_balance_at": baseline.get("as_of", ""),
            "safety": None if not kernel_fail_closed else {"halted": True, "reason": kernel_reason},
        }
        if is_snapshot_mode: self.save_daily_snapshot(target, plan_result)
        return plan_result
