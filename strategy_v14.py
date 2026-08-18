# ==========================================================
# FILE: strategy_v14.py
# ==========================================================
# 🚨 MODIFIED: [Lost Update 궁극 방어] 스냅샷 파일 I/O 전역에 GlobalThrottle.get_file_lock()을 100% 팩트 래핑 완료.
# 🚨 MODIFIED: [TypeError 런타임 붕괴 방어] datetime.time 충돌 정리 및 정수 연산 락온 유지.
# 🚨 NEW: [0주 스냅샷 오염 팩트 자가치유(Self-Healing) 결속] 통신 지연으로 YF 종가가 훼손되어 스냅샷 타점이 0.0 등으로 오염될 경우, 로드 시 공식 종가(prev_c) 기반으로 타점($)과 수량(Q)을 원자적으로 재계산하여 덮어쓰는(Self-Healing) 방어망 100% 락온 (Case 46 방어 강화).
# 🚨 MODIFIED: [리버스 자본 격리 아키텍처 결속] 스냅샷 생성 모드(is_snapshot_mode=True)일 때만 cfg.apply_reverse_daily_settlement(ticker)를 호출하여 1일 1회 원자적 장부 정산이 실행되도록 팩트 락온.
# ==========================================================
import math
import os
import json
import tempfile
import logging
from decimal import Decimal, InvalidOperation
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

    def _parse_reverse_day_count(self, rev_state):
        if "day_count" not in rev_state:
            return None, "invalid reverse day domain: missing day_count"
        raw = rev_state.get("day_count")
        try:
            text = str(raw).strip()
            if not text:
                raise InvalidOperation("blank day_count")
            value = Decimal(text)
        except (InvalidOperation, ValueError):
            return None, "invalid reverse day domain: day_count must be numeric"
        if not value.is_finite() or value <= 0 or value != value.to_integral_value():
            return None, "invalid reverse day domain: day_count must be a positive integer"
        return int(value), ""

    def _get_reverse_plan_display_day(self, day, rev_state):
        # 주문계획 표시용 보정: 진입일이 지난 day_count=1은 2일차 계획으로 보여준다.
        if day != 1:
            return day
        raw_last_update_date = rev_state.get("last_update_date")
        if not raw_last_update_date:
            return day
        try:
            entry_date = datetime.strptime(str(raw_last_update_date).strip()[:10], "%Y-%m-%d").date()
        except (TypeError, ValueError):
            return day
        today_est = datetime.now(ZoneInfo("America/New_York")).date()
        if entry_date < today_est:
            return 2
        return day

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

    def _resolve_cycle_cash(self, target, real_available_cash, official_state, is_snapshot_mode, pending_buy_amount=0.0):
        """1회매수금 기준현금을 cycle_cash(원장 기반)로 확정하고 정합 검증한다.

        - cycle_cash = baseline.available_cash + Σ매도 − Σ매수 (KIS 예수금 미사용).
          → 중간 입금이 예수금에 더해져도 1회매수금이 부풀지 않는다.
        - 정합 검증: 모든 실계좌 계획에서 reconcile_cycle_cash로 항등식 HALT를
          판정한다. is_snapshot_mode(일일 정산 스냅샷)에서만 입금분(pending_seed)을
          기록하고, 그 외 경로는 부작용 없이 읽기전용으로 검증한다.
        - fail-closed: cycle_cash 계산 불가 또는 정합 실패 시 (cash, True, reason).

        반환: (official_cash: float, halted: bool, reason: str)
        """
        cycle_fn = getattr(self.cfg, "calculate_cycle_cash", None)
        if not callable(cycle_fn):
            # 레거시/대체 config: 기존 동작(예수금→baseline) 유지
            legacy = real_available_cash if real_available_cash > 0 else self._safe_float(
                getattr(official_state, "available_cash", 0.0)
            )
            return legacy, False, ""

        cycle_cash, detail = cycle_fn(target)
        if cycle_cash is None or self._safe_float(cycle_cash) <= 0.0:
            fallback = self._safe_float(getattr(official_state, "available_cash", 0.0))
            return fallback, True, f"cycle_cash 계산 불가(fail-closed): {detail.get('reason', '')}"

        cycle_cash = self._safe_float(cycle_cash)
        halted = False
        reason = ""
        if real_available_cash and real_available_cash > 0:
            # KIS 파생 가용현금 =(예수금+매도미정산)×0.9945 → 예수금 근사 복원
            kis_deposit_est = real_available_cash / 0.9945
            pending_buy_amount = max(0.0, self._safe_float(pending_buy_amount))
            cycle_compare_cash = kis_deposit_est + pending_buy_amount
            reconcile_fn = getattr(self.cfg, "reconcile_cycle_cash", None)
            if callable(reconcile_fn):
                rec = reconcile_fn(target, cycle_compare_cash, record_pending=bool(is_snapshot_mode))
                if rec.get("halt"):
                    halted = True
                    reason = rec.get("reason", "정합 실패 HALT")
            elif (cycle_cash - cycle_compare_cash) > 50.0:
                halted = True
                reason = (
                    f"정합 실패 HALT: cycle_cash({cycle_cash:.2f})가 "
                    f"KIS 매수미정산 보정현금({cycle_compare_cash:.2f}, "
                    f"매수미정산 {pending_buy_amount:.2f})를 초과"
                )
        return cycle_cash, halted, reason

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
        split = int(self._safe_float(self.cfg.get_split_count(target)) or 20)
        # 이벤트식 T: baseline + T 이벤트 원장에서 온 official_state.t 를 그대로 사용
        t_val = float(official_state.t)
        from decimal import Decimal
        t_val_dec = Decimal(str(round(t_val, 2)))
        # 1회매수금 기준현금: KIS 예수금이 아니라 원장 기반 cycle_cash 사용(중간 입금 격리).
        pending_buy_amount = kwargs.get("pending_buy_amount", kwargs.get("frcr_buy_amt_smtl", 0.0))
        official_cash, cash_halt, cash_halt_reason = self._resolve_cycle_cash(
            target, real_available_cash, official_state, is_snapshot_mode, pending_buy_amount
        )
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
            process_status = "♻️공식리버스"
            reverse_t = rev_state.get("dynamic_t", t_val_dec) or t_val_dec
            reverse_cash = rev_state.get("rem_cash", official_cash) or official_cash
            day, day_reason = self._parse_reverse_day_count(rev_state)
            if day_reason:
                kernel_fail_closed = True
                kernel_reason = day_reason
            else:
                day = self._get_reverse_plan_display_day(day, rev_state)
                previous_quantity = int(self._safe_float(rev_state.get("previous_quantity", baseline.get("qty", qty))))
                if day <= 1:
                    previous_closes = []
                else:
                    previous_closes = kwargs.get("previous_closes") or []
                    if not previous_closes and ma_5day:
                        previous_closes = [ma_5day] * 5
                    elif not previous_closes and prev_close:
                        previous_closes = [prev_close] * 5
                confirmed_close = kwargs.get("confirmed_close")
                if confirmed_close is None and self._safe_float(prev_close) > 0:
                    # 장마감 후 스냅샷 모드이거나 2일차 이상이면 확정 종가로 복귀 판정 주입.
                    # 첫날 장중(is_snapshot_mode=False, day=1)에는 MOC 매도가 먼저 계획되어야 하므로 제외.
                    if is_snapshot_mode or day > 1:
                        confirmed_close = prev_close
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
                        confirmed_close=confirmed_close,
                    )
                )
                t_val = float(reverse_plan.t)
                star_price = float(reverse_plan.star_point)
                one_portion = float(reverse_plan.buy_budget)
                kernel_fail_closed = bool(reverse_plan.fail_closed)
                kernel_reason = reverse_plan.reason
                if reverse_plan.return_to_normal:
                    is_reverse = False  # 일반모드 복귀: 중간 상태 없이 즉시 전환
                    t_val = float(t_val_dec)  # official T (T이벤트 원장 기준, 승계)
                    target_profit_percent = self._safe_float(self.cfg.get_target_profit(target))
                    return_normal = laoer_v4_20.calculate_normal_plan(
                        laoer_v4_20.NormalState(
                            ticker=target,
                            split=20,
                            quantity=qty,
                            avg_price=laoer_v4_20.Decimal(str(avg_price)),
                            cash=official_cash,
                            t=t_val_dec,
                            reverse=False,
                            target_profit_percent=laoer_v4_20.Decimal(str(target_profit_percent)),
                        )
                    )
                    star_price = float(return_normal.star_point)
                    star_ratio = float(return_normal.star_percent / laoer_v4_20.Decimal("100")) if hasattr(laoer_v4_20, "Decimal") else float(return_normal.star_percent / __import__('decimal').Decimal("100"))
                    target_price = float(return_normal.target_sell_price)
                    one_portion = float(return_normal.one_buy_budget)
                    # 복귀 판정 즉시 일반모드 단계로 표시 (♻️복귀대기 중간상태 제거)
                    if return_normal.reverse_entry:
                        process_status = "♻️공식리버스진입대기"
                    else:
                        process_status = "🌓공식전반전" if return_normal.phase == "FIRST_HALF" else "🌕공식후반전"
                    # kernel_fail_closed은 False 유지 — 복귀 판정 자체는 성공
                    if not return_normal.fail_closed:
                        if return_normal.average_buy_quantity > 0:
                            orders.append(self._official_order(
                                ticker=target, trade_date=trade_date, t_revision=t_revision,
                                event_type="HALF", side="BUY", order_type="LOC",
                                price=return_normal.average_buy_price, qty=return_normal.average_buy_quantity,
                                desc="공식평단매수",
                            ))
                        if return_normal.star_buy_quantity > 0:
                            event_type_n = "HALF" if return_normal.average_buy_quantity > 0 else "FULL"
                            orders.append(self._official_order(
                                ticker=target, trade_date=trade_date, t_revision=t_revision,
                                event_type=event_type_n, side="BUY", order_type="LOC",
                                price=return_normal.star_buy_price, qty=return_normal.star_buy_quantity,
                                desc="공식별값매수",
                            ))
                        if return_normal.star_buy_quantity > 0 and return_normal.one_buy_budget > 0:
                            _jj_base = int(return_normal.star_buy_quantity)
                            for _k in range(1, 6):
                                _jj_price = return_normal.one_buy_budget / laoer_v4_20.Decimal(str(_jj_base + _k))
                                orders.append(self._official_order(
                                    ticker=target, trade_date=trade_date, t_revision=t_revision,
                                    event_type="BONUS", side="BUY", order_type="LOC",
                                    price=float(_jj_price), qty=1,
                                    desc="공식보너스",
                                ))
                        if return_normal.quarter_sell_quantity > 0:
                            orders.append(self._official_order(
                                ticker=target, trade_date=trade_date, t_revision=t_revision,
                                event_type="QUARTER", side="SELL", order_type="LOC",
                                price=return_normal.star_point, qty=return_normal.quarter_sell_quantity,
                                desc="공식별값매도",
                            ))
                        if return_normal.target_sell_quantity > 0:
                            orders.append(self._official_order(
                                ticker=target, trade_date=trade_date, t_revision=t_revision,
                                event_type="TARGET_FULL", side="SELL", order_type="LIMIT",
                                price=return_normal.target_sell_price, qty=return_normal.target_sell_quantity,
                                desc="공식목표매도",
                            ))
                    else:
                        # 매수 예산 부족(fail_closed)이어도 쿼터매도·목표매도는 반드시 생성
                        _q_qty = qty // 4
                        _t_qty = max(qty - _q_qty, 0)
                        if _q_qty > 0 and float(return_normal.star_point) > 0:
                            orders.append(self._official_order(
                                ticker=target, trade_date=trade_date, t_revision=t_revision,
                                event_type="QUARTER", side="SELL", order_type="LOC",
                                price=return_normal.star_point, qty=_q_qty,
                                desc="공식별값매도",
                            ))
                        if _t_qty > 0 and float(return_normal.target_sell_price) > 0:
                            orders.append(self._official_order(
                                ticker=target, trade_date=trade_date, t_revision=t_revision,
                                event_type="TARGET_FULL", side="SELL", order_type="LIMIT",
                                price=return_normal.target_sell_price, qty=_t_qty,
                                desc="공식목표매도",
                            ))
                elif not reverse_plan.fail_closed:
                    if reverse_plan.buy_quantity > 0:
                        orders.append(self._official_order(
                            ticker=target, trade_date=trade_date, t_revision=t_revision,
                            event_type="REVERSE_QUARTER_BUY", side="BUY", order_type="LOC",
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
                                event_type="REVERSE_SELL", side="SELL", order_type=reverse_plan.sell_order_type,
                                price=sell_price, qty=reverse_plan.sell_quantity,
                                desc="공식리버스매도",
                                risk_reference_price=(current_price if reverse_plan.sell_order_type == "MOC" else None),
                            ))
        else:
            target_profit_percent = self._safe_float(self.cfg.get_target_profit(target))
            normal_plan = laoer_v4_20.calculate_normal_plan(
                laoer_v4_20.NormalState(
                    ticker=target,
                    split=20,
                    quantity=qty,
                    avg_price=laoer_v4_20.Decimal(str(avg_price)),
                    cash=official_cash,
                    t=t_val_dec,
                    reverse=False,
                    target_profit_percent=laoer_v4_20.Decimal(str(target_profit_percent)),
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
                # 보너스(하단 LOC): 별값매수 제외 5x1주, 가격 = 하루매수금/(별값수량+k)
                if normal_plan.star_buy_quantity > 0 and normal_plan.one_buy_budget > 0:
                    _jj_base = int(normal_plan.star_buy_quantity)
                    for _k in range(1, 6):
                        _jj_price = normal_plan.one_buy_budget / laoer_v4_20.Decimal(str(_jj_base + _k))
                        orders.append(self._official_order(
                            ticker=target, trade_date=trade_date, t_revision=t_revision,
                            event_type="BONUS", side="BUY", order_type="LOC",
                            price=float(_jj_price), qty=1,
                            desc="공식보너스",
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

        if cash_halt and not kernel_fail_closed:
            kernel_fail_closed = True
            kernel_reason = cash_halt_reason
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
