# ==========================================================
# FILE: strategy_v14_vwap.py
# ==========================================================
# 🚨 MODIFIED: [Lost Update 궁극 방어] 스냅샷 및 상태 캐시 읽기/쓰기 시 GlobalThrottle.get_file_lock()을 전면 결속하여 데이터 훼손(Corruption) 원천 봉쇄.
# 🚨 NEW: [0주 스냅샷 오염 팩트 자가치유(Self-Healing) 결속] 통신 지연으로 YF 종가가 훼손되어 스냅샷 타점이 0.0 등으로 오염될 경우, 로드 시 공식 종가(prev_c) 기반으로 타점($)과 수량(Q)을 원자적으로 재계산하여 덮어쓰는(Self-Healing) 방어망 100% 락온 (Case 46 방어 강화).
# ==========================================================
import math
import logging
import os
import json
import tempfile
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from global_throttle import GlobalThrottle

class V14VwapStrategy:
    def __init__(self, config):
        self.cfg = config
        self.executed = {"BUY_BUDGET": {}, "SELL_QTY": {}}
        self.state_loaded = {}

    def _safe_float(self, value):
        try:
            val = float(str(value or 0.0).replace(',', ''))
            if math.isnan(val) or math.isinf(val):
                return 0.0
            return val
        except Exception:
            return 0.0

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

    def _get_state_file(self, ticker):
        today_str = self._get_logical_date_str()
        return f"data/vwap_state_V14_{today_str}_{ticker}.json"

    def _get_snapshot_file(self, ticker):
        today_str = self._get_logical_date_str()
        return f"data/daily_snapshot_V14VWAP_{today_str}_{ticker}.json"

    def _load_state_if_needed(self, ticker):
        today_str = self._get_logical_date_str()
        state_file = self._get_state_file(ticker)
        
        is_disk_valid = False
        with GlobalThrottle.get_file_lock(state_file):
            try:
                with open(state_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict) and data.get("date") == today_str:
                        exec_data = data.get("executed")
                        exec_dict = exec_data if isinstance(exec_data, dict) else {}
                        
                        for k in self.executed.keys():
                            sub_dict = exec_dict.get(k)
                            safe_sub_dict = sub_dict if isinstance(sub_dict, dict) else {}
                            raw_val = safe_sub_dict.get(ticker, 0)
                            self.executed[k][ticker] = int(self._safe_float(raw_val)) if k == "SELL_QTY" else self._safe_float(raw_val)
                        is_disk_valid = True
            except Exception:
                pass
                
            if not is_disk_valid:
                self.executed["BUY_BUDGET"][ticker] = 0.0
                self.executed["SELL_QTY"][ticker] = 0
                self._save_state(ticker)
            
        self.state_loaded[ticker] = today_str

    def _save_state(self, ticker):
        today_str = self._get_logical_date_str()
        state_file = self._get_state_file(ticker)
        
        buy_dict = self.executed.get("BUY_BUDGET")
        sell_dict = self.executed.get("SELL_QTY")
        buy_budget = self._safe_float((buy_dict if isinstance(buy_dict, dict) else {}).get(ticker, 0.0))
        sell_qty = int(self._safe_float((sell_dict if isinstance(sell_dict, dict) else {}).get(ticker, 0)))
        
        data = {
            "date": today_str,
            "executed": {
                "BUY_BUDGET": {ticker: buy_budget},
                "SELL_QTY": {ticker: sell_qty}
            }
        }
        
        with GlobalThrottle.get_file_lock(state_file):
            fd = None
            temp_path = None
            try:
                dir_name = os.path.dirname(state_file)
                if dir_name:
                    os.makedirs(dir_name, exist_ok=True) 
                fd, temp_path = tempfile.mkstemp(dir=dir_name or '.', text=True)
                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                    fd = None
                    json.dump(data, f, ensure_ascii=False, indent=4)
                    f.flush()
                    os.fsync(f.fileno()) 
                os.replace(temp_path, state_file)
                temp_path = None
            except Exception as e:
                if fd is not None:
                    try: os.close(fd)
                    except OSError: pass
                if temp_path:
                    try: os.remove(temp_path)
                    except OSError: pass
                logging.critical(f"🚨 [STATE SAVE FAILED] {ticker} 상태 저장 실패. 봇 기억상실 위험! 원인: {e}")

    def save_daily_snapshot(self, ticker, plan_data):
        today_str = self._get_logical_date_str()
        snap_file = self._get_snapshot_file(ticker)
        
        data = {
            "date": today_str,
            "plan": plan_data
        }
        
        with GlobalThrottle.get_file_lock(snap_file):
            fd = None
            temp_path = None
            try:
                dir_name = os.path.dirname(snap_file)
                if dir_name:
                    os.makedirs(dir_name, exist_ok=True)
                fd, temp_path = tempfile.mkstemp(dir=dir_name or '.', text=True)
                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                    fd = None
                    json.dump(data, f, ensure_ascii=False, indent=4)
                    f.flush()
                    os.fsync(f.fileno()) 
                os.replace(temp_path, snap_file)
                temp_path = None
            except Exception as e:
                if fd is not None:
                    try: os.close(fd)
                    except OSError: pass
                if temp_path:
                    try: os.remove(temp_path)
                    except OSError: pass
                logging.critical(f"🚨 [SNAPSHOT SAVE FAILED] {ticker} 스냅샷 저장 실패. 지시서 보존 불가! 원인: {e}")

    def load_daily_snapshot(self, ticker):
        snap_file = self._get_snapshot_file(ticker)
        with GlobalThrottle.get_file_lock(snap_file):
            try:
                with open(snap_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get("plan") if isinstance(data, dict) else None
            except Exception:
                pass
            return None

    def ensure_failsafe_snapshot(self, ticker, current_price, total_qty, avwap_qty, avg_price, prev_close, alloc_cash):
        current_price = self._safe_float(current_price)
        total_qty = int(self._safe_float(total_qty))
        avwap_qty = int(self._safe_float(avwap_qty))
        avg_price = self._safe_float(avg_price)
        prev_close = self._safe_float(prev_close)
        alloc_cash = self._safe_float(alloc_cash)

        snap = self.load_daily_snapshot(ticker)
        if snap is not None:
            return snap
            
        pure_qty = max(0, total_qty - avwap_qty)
        
        today_str_est = self._get_logical_date_str()
        legacy_qty = pure_qty
        legacy_avg = avg_price
        
        try:
            recs = [r for r in (self.cfg.get_ledger() or []) if isinstance(r, dict) and r.get('ticker') == ticker and not str(r.get("date", "")).startswith(today_str_est)]
            ledger_qty, ledger_avg, _, _ = self.cfg.calculate_holdings(ticker, recs)
            legacy_qty = ledger_qty
            legacy_avg = ledger_avg if ledger_qty > 0 else avg_price
        except Exception:
            pass
            
        logging.warning(f"🚨 [{ticker}] V14_VWAP 스냅샷 증발 감지! 페일세이프 긴급 복원 가동")
        
        return self.get_plan(
            ticker=ticker,
            current_price=current_price,
            avg_price=legacy_avg,
            qty=legacy_qty,
            prev_close=prev_close,
            ma_5day=0.0,
            market_type="REG",
            available_cash=alloc_cash,
            is_simulation=True,
            is_snapshot_mode=True
        )

    def _ceil(self, val): return math.ceil(self._safe_float(val) * 100) / 100.0

    def record_execution(self, ticker, side, qty, exec_price):
        self._load_state_if_needed(ticker)
        safe_qty = int(self._safe_float(qty))
        safe_price = self._safe_float(exec_price)
        
        if side == "BUY":
            spent = self._safe_float(safe_qty * safe_price)
            buy_dict = self.executed.get("BUY_BUDGET")
            buy_val = (buy_dict if isinstance(buy_dict, dict) else {}).get(ticker, 0.0)
            self.executed["BUY_BUDGET"][ticker] = self._safe_float(buy_val) + spent
        else:
            sell_dict = self.executed.get("SELL_QTY")
            sell_val = (sell_dict if isinstance(sell_dict, dict) else {}).get(ticker, 0)
            self.executed["SELL_QTY"][ticker] = int(self._safe_float(sell_val)) + safe_qty
        self._save_state(ticker)

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
                    if "🛡️" not in new_o.get('desc', ''): 
                        new_o['desc'] = f"🛡️교정_{new_o.get('desc', '').replace('🧹', '')}"
                    new_o['price'] = max(0.01, self._safe_float(new_o.get('price')))
                res.append(new_o)
            return res
        return _clean(c_orders), _clean(b_orders)

    def get_plan(self, ticker, current_price, avg_price, qty, prev_close, ma_5day=0.0, market_type="REG", available_cash=0, is_simulation=False, is_snapshot_mode=False):
        current_price = self._safe_float(current_price)
        avg_price = self._safe_float(avg_price)
        qty = int(self._safe_float(qty))
        prev_close = self._safe_float(prev_close)
        ma_5day = self._safe_float(ma_5day)
        available_cash = self._safe_float(available_cash)

        if not is_snapshot_mode:
            cached_plan = self.load_daily_snapshot(ticker)
            if cached_plan:
                # 🚨 NEW: [Case 46 자가치유 방어망] 0주 스냅샷 오염 감지 시 YF 공식 종가 기반 타점 팩트 롤오버
                is_zero_val = cached_plan.get("is_zero_start")
                is_zero_snap = bool(is_zero_val) if is_zero_val is not None else (int(self._safe_float(cached_plan.get("initial_qty", -1))) == 0)
                
                if is_zero_snap and prev_close > 0.0:
                    target_p1 = max(0.01, round(self._ceil(prev_close * 1.15) - 0.01, 2))
                    is_polluted = False
                    
                    for o in cached_plan.get("core_orders", []):
                        if str(o.get("side")) == "BUY":
                            p = self._safe_float(o.get("price"))
                            if p > 0 and abs(p - target_p1) / target_p1 >= 0.01:
                                is_polluted = True
                                break
                                
                    if is_polluted:
                        logging.warning(f"🚨 [{ticker}] 0주 스냅샷 오염 감지! YF 무결성 종가(${prev_close}) 기반으로 V14_VWAP 타점을 즉각 자가 치유(Self-Healing)합니다.")
                        one_portion_amt = self._safe_float(cached_plan.get("one_portion", 0.0))
                        
                        half_budget = one_portion_amt * 0.5
                        new_q1 = int(math.floor(half_budget / target_p1)) if target_p1 > 0 else 0
                        new_q2 = int(math.floor((one_portion_amt - half_budget) / target_p1)) if target_p1 > 0 else 0
                        
                        if new_q1 == 0 and new_q2 == 0 and target_p1 > 0 and one_portion_amt >= target_p1:
                            new_q1 = int(math.floor(one_portion_amt / target_p1))
                            
                        for o_list in [cached_plan.get("core_orders", []), cached_plan.get("orders", [])]:
                            idx = 1
                            for o in o_list:
                                if str(o.get("side")) == "BUY" and "새출발" in str(o.get("desc", "")):
                                    o["price"] = target_p1
                                    o["qty"] = new_q1 if idx == 1 else new_q2
                                    idx += 1
                                    
                        self.save_daily_snapshot(ticker, cached_plan)
                return cached_plan

        split = self._safe_float(self.cfg.get_split_count(ticker))
        target_ratio = self._safe_float(self.cfg.get_target_profit(ticker)) / 100.0
        
        t_val_raw, _ = self.cfg.get_absolute_t_val(ticker, qty, avg_price)
        t_val = self._safe_float(t_val_raw)
        
        depreciation_factor = 2.0 / split if split > 0 else 0.1
        star_ratio = target_ratio - (target_ratio * depreciation_factor * t_val)
        star_price = self._ceil(avg_price * (1 + star_ratio)) if avg_price > 0 else 0.0
        target_price = self._ceil(avg_price * (1 + target_ratio)) if avg_price > 0 else 0.0
        
        is_jackpot_reached = target_price > 0 and current_price >= target_price
        
        buy_star_price = max(0.01, round(star_price - 0.01, 2)) if star_price > 0 else 0.0

        _, dynamic_budget, _ = self.cfg.calculate_v14_state(ticker)
        dynamic_budget = self._safe_float(dynamic_budget)
        
        if qty == 0:
            base_price = prev_close
        else:
            base_price = prev_close if prev_close > 0.0 else current_price
        
        if base_price <= 0.0:
            plan_result = {
                'core_orders': [], 'bonus_orders': [], 'orders': [],
                't_val': t_val, 'one_portion': dynamic_budget, 'star_price': star_price,
                'buy_star_price': buy_star_price, 'star_ratio': star_ratio,
                'target_price': target_price, 'is_reverse': False,
                'process_status': "⛔가격오류", 'tracking_info': {},
                'initial_qty': int(qty), 'is_zero_start': False
            }
            if is_snapshot_mode:
                self.save_daily_snapshot(ticker, plan_result)
            return plan_result

        core_orders = []
        bonus_orders = []
        process_status = "예방적방어선"
        is_zero_start_fact = False
        
        est_zone = ZoneInfo('America/New_York')
        kst_zone = ZoneInfo('Asia/Seoul')
        now_est = datetime.now(est_zone)
        
        base_start_est = now_est.replace(hour=15, minute=26, second=0, microsecond=0)
        shifted_start_est = now_est + timedelta(minutes=3)
        actual_start_est = max(base_start_est, shifted_start_est)
        
        base_end_est = now_est.replace(hour=15, minute=56, second=0, microsecond=0)
        
        start_dt_kst = actual_start_est.astimezone(kst_zone)
        end_dt_kst = base_end_est.astimezone(kst_zone)
        
        start_t = start_dt_kst.strftime("%H%M%S")
        end_t = end_dt_kst.strftime("%H%M%S")

        if qty == 0:
            is_zero_start_fact = True
            
            p_buy = max(0.01, round(self._ceil(base_price * 1.15) - 0.01, 2))
            buy_star_price = p_buy 
            
            b1_budget = dynamic_budget * 0.5
            b2_budget = dynamic_budget - b1_budget
            q1 = math.floor(b1_budget / p_buy) if p_buy > 0 else 0
            q2 = math.floor(b2_budget / p_buy) if p_buy > 0 else 0
            
            if q1 == 0 and q2 == 0 and p_buy > 0 and dynamic_budget >= p_buy:
                q1 = int(math.floor(dynamic_budget / p_buy))
            
            if q1 > 0: 
                o_type = "VWAP"
                desc = f"🆕새출발1({o_type})"
                core_orders.append({"side": "BUY", "price": p_buy, "qty": q1, "type": o_type, "start_time": start_t, "end_time": end_t, "desc": desc})
            if q2 > 0:
                o_type = "VWAP"
                desc = f"🆕새출발2({o_type})"
                core_orders.append({"side": "BUY", "price": p_buy, "qty": q2, "type": o_type, "start_time": start_t, "end_time": end_t, "desc": desc})
            process_status = "✨새출발"
            
        elif is_jackpot_reached and t_val > (split - 1):
            process_status = "🎉대박익절"
            if qty > 0:
                core_orders.append({"side": "SELL", "price": target_price, "qty": int(qty), "type": "LIMIT", "desc": "🎯전량대박익절"})
                
        else:
            safe_ceiling = min(avg_price, star_price) if star_price > 0 else avg_price
            p_avg = max(0.01, round(safe_ceiling - 0.01, 2))
            
            process_status = "🌓전반전" if t_val < (split / 2) else "🌕후반전"
            
            if t_val < (split / 2):
                b1_budget = dynamic_budget * 0.5
                b2_budget = dynamic_budget - b1_budget
                q_avg = math.floor(b1_budget / p_avg) if p_avg > 0 else 0
                q_star = math.floor(b2_budget / buy_star_price) if buy_star_price > 0 else 0
                
                if q_avg == 0 and q_star == 0:
                    if p_avg > 0 and dynamic_budget >= p_avg: q_avg = math.floor(dynamic_budget / p_avg)
                    elif buy_star_price > 0 and dynamic_budget >= buy_star_price: q_star = math.floor(dynamic_budget / buy_star_price)
                elif q_avg == 0 and q_star > 0: q_star = math.floor(dynamic_budget / buy_star_price) if buy_star_price > 0 else 0
                elif q_star == 0 and q_avg > 0: q_avg = math.floor(dynamic_budget / p_avg) if p_avg > 0 else 0
                 
                if q_avg > 0: 
                    o_type = "VWAP"
                    desc = f"⚓평단매수({o_type})"
                    core_orders.append({"side": "BUY", "price": p_avg, "qty": q_avg, "type": o_type, "start_time": start_t, "end_time": end_t, "desc": desc})
                if q_star > 0: 
                    o_type = "VWAP"
                    desc = f"💫별값매수({o_type})"
                    core_orders.append({"side": "BUY", "price": buy_star_price, "qty": q_star, "type": o_type, "start_time": start_t, "end_time": end_t, "desc": desc})
            else:
                q_total = math.floor(dynamic_budget / buy_star_price) if buy_star_price > 0 else 0
                if q_total > 0: 
                    o_type = "VWAP"
                    desc = f"💫별값매수(통합:{o_type})"
                    core_orders.append({"side": "BUY", "price": buy_star_price, "qty": q_total, "type": o_type, "start_time": start_t, "end_time": end_t, "desc": desc})
            
            q_sell = math.ceil(qty / 4)
            if q_sell > 0:
                o_type = "VWAP"
                desc = f"🌟별값매도({o_type})"
                core_orders.append({"side": "SELL", "price": star_price, "qty": q_sell, "type": o_type, "start_time": start_t, "end_time": end_t, "desc": desc})
            if qty - q_sell > 0:
                core_orders.append({"side": "SELL", "price": target_price, "qty": qty - q_sell, "type": "LIMIT", "desc": "🎯목표매도(V)"})

        if is_zero_start_fact and market_type != "AFTER":
            core_orders = [o for o in core_orders if isinstance(o, dict) and o.get("side") != "SELL"]

        q_base = sum(int(self._safe_float(o.get('qty'))) for o in core_orders if isinstance(o, dict) and o.get('side') == 'BUY')
        if q_base > 0:
            bonus_orders.extend(sorted([{"side": "BUY", "price": math.floor((dynamic_budget / (q_base + n)) * 100) / 100.0, "qty": 1, "type": "LOC", "desc": f"🧲줍줍(+{n}주)"} for n in range(1, 6) if math.floor((dynamic_budget / (q_base + n)) * 100) / 100.0 > 0.01], key=lambda x: x['price'], reverse=True))

        core_orders, bonus_orders = self._apply_wash_trade_shield(core_orders, bonus_orders)
        orders = core_orders + bonus_orders

        plan_result = {
            'core_orders': core_orders, 'bonus_orders': bonus_orders, 'orders': orders,
            't_val': t_val, 'one_portion': dynamic_budget, 'star_price': star_price,
            'buy_star_price': buy_star_price, 
            'star_ratio': star_ratio,
            'target_price': target_price, 'is_reverse': False,
            'process_status': process_status,
            'tracking_info': {},
            'initial_qty': int(qty),
            'is_zero_start': is_zero_start_fact 
        }
        
        if is_snapshot_mode:
            self.save_daily_snapshot(ticker, plan_result)

        self._save_state(ticker)
        return plan_result
