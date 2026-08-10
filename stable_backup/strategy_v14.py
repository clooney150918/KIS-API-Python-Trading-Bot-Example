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

class V14Strategy:
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
        snap_file = f"data/daily_snapshot_V14_{today_str}_{ticker}.json"
        
        safe_plan = plan_data if isinstance(plan_data, dict) else {}
        
        data = {
            "date": today_str,
            "total_q": int(self._safe_float(safe_plan.get('total_q', 0))),
            "avg_price": self._safe_float(safe_plan.get('avg_price', 0.0)),
            "one_portion": self._safe_float(safe_plan.get('one_portion', 0.0)),
            "star_price": self._safe_float(safe_plan.get('star_price', 0.0)),
            "star_ratio": self._safe_float(safe_plan.get('star_ratio', 0.0)),
            "target_price": self._safe_float(safe_plan.get('target_price', 0.0)), 
            "t_val": self._safe_float(safe_plan.get('t_val', 0.0)),
            "is_reverse": bool(safe_plan.get('is_reverse', False)),
            "is_zero_start": bool(safe_plan.get('is_zero_start', False)),
            "initial_qty": int(self._safe_float(safe_plan.get('initial_qty', 0))),
            "orders": safe_plan.get('orders') or [],
            "core_orders": safe_plan.get('core_orders') or [],
            "bonus_orders": safe_plan.get('bonus_orders') or [],
            "process_status": str(safe_plan.get('process_status', ''))
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
        snap_file = f"data/daily_snapshot_V14_{today_str}_{ticker}.json"
        
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
                new_o['price'] = max(0.01, self._safe_float(new_o.get('price')))
                res.append(new_o)
            return res
        return _clean(c_orders), _clean(b_orders)

    def get_plan(self, ticker, current_price, avg_price, qty, prev_close, ma_5day=0.0, market_type="REG", available_cash=0, is_simulation=False, is_snapshot_mode=False, **kwargs):
        current_price = self._safe_float(current_price)
        avg_price = self._safe_float(avg_price)
        qty = int(self._safe_float(qty))
        prev_close = self._safe_float(prev_close)
        available_cash = self._safe_float(available_cash)
        real_available_cash = max(0.0, available_cash)
        ma_5day = self._safe_float(ma_5day)

        if not is_snapshot_mode:
            snap = self.load_daily_snapshot(ticker)
            if snap:
                is_zero_val = snap.get("is_zero_start")
                is_zero_snap = bool(is_zero_val) if is_zero_val is not None else (int(self._safe_float(snap.get("total_q", -1))) == 0)
                
                if is_zero_snap and prev_close > 0.0:
                    target_p1 = max(0.01, round(self._ceil(prev_close * 1.15) - 0.01, 2))
                    is_polluted = False
                    
                    for o in snap.get("core_orders", []):
                        if str(o.get("side")) == "BUY":
                            p = self._safe_float(o.get("price"))
                            if p > 0 and abs(p - target_p1) / target_p1 >= 0.01:
                                is_polluted = True
                                break
                                
                    if is_polluted:
                        logging.warning(f"🚨 [{ticker}] 0주 스냅샷 오염 감지! YF 무결성 종가(${prev_close}) 기반으로 V14 타점을 즉각 자가 치유(Self-Healing)합니다.")
                        one_portion_amt = self._safe_float(snap.get("one_portion", 0.0))
                        
                        half_budget = one_portion_amt * 0.5
                        new_q1 = int(math.floor(half_budget / target_p1)) if target_p1 > 0 else 0
                        new_q2 = int(math.floor((one_portion_amt - half_budget) / target_p1)) if target_p1 > 0 else 0
                        
                        if new_q1 == 0 and new_q2 == 0 and target_p1 > 0 and one_portion_amt >= target_p1:
                            new_q1 = int(math.floor(one_portion_amt / target_p1))
                            
                        for o_list in [snap.get("core_orders", []), snap.get("orders", [])]:
                            idx = 1
                            for o in o_list:
                                if str(o.get("side")) == "BUY" and "새출발" in str(o.get("desc", "")):
                                    o["price"] = target_p1
                                    o["qty"] = new_q1 if idx == 1 else new_q2
                                    idx += 1
                                    
                        self.save_daily_snapshot(ticker, snap)
                return snap

        split = self._safe_float(self.cfg.get_split_count(ticker))
        if split <= 0: split = 40.0

        rev_state = self.cfg.get_reverse_state(ticker)
        is_rev_active = rev_state.get('is_active', False)
        
        if is_rev_active:
            loss_pct = ((current_price - avg_price) / avg_price * 100.0) if avg_price > 0 else 0.0
            escape_thresh = -15.0 if ticker == "TQQQ" else -20.0
            
            if loss_pct >= escape_thresh and qty > 0:
                logging.info(f"🚀 [{ticker}] 손실률 {loss_pct:.2f}% 도달 (임계치 {escape_thresh}%). 리버스 모드 탈출 및 일반 모드 롤오버를 가동합니다.")
                dynamic_t = self._safe_float(rev_state.get('dynamic_t', 0.0))
                rem_cash = self._safe_float(rev_state.get('rem_cash', 0.0))
                
                safe_denom = max(1.0, split - dynamic_t)
                new_portion = rem_cash / safe_denom if rem_cash > 0 else 1.0
                new_seed = new_portion * split
                
                self.cfg.set_seed(ticker, new_seed)
                self.cfg.set_reverse_state(ticker, False, 0, 0.0, dynamic_t=0.0, rem_cash=0.0, is_day_one=True)
                is_rev_active = False

        seed = self._safe_float(self.cfg.get_seed(ticker))
        target_pct_val = self._safe_float(self.cfg.get_target_profit(ticker))
        target_ratio = target_pct_val / 100.0
        
        portion = seed / split if split > 0 else 1.0
        t_val = (qty * avg_price) / portion if portion > 0 else 0.0
        t_val = round(t_val, 4)

        target_price = self._ceil(avg_price * (1 + target_ratio)) if avg_price > 0 else 0.0
        
        is_jackpot_reached = target_price > 0 and current_price >= target_price
        
        one_portion_amt = portion
        
        depreciation_factor = 2.0 / split if split > 0 else 0.1
        star_ratio = target_ratio - (target_ratio * depreciation_factor * t_val)
        star_price = self._ceil(avg_price * (1 + star_ratio)) if avg_price > 0 else 0.0
          
        if qty == 0:
            base_price = prev_close
        else:
            base_price = prev_close if prev_close > 0.0 else current_price

        if base_price <= 0.0: 
            plan_result = {"orders": [], "core_orders": [], "bonus_orders": [], "total_q": qty, "avg_price": avg_price, "t_val": t_val, "one_portion": one_portion_amt, "process_status": "⛔가격오류", "is_reverse": False, "star_price": star_price, "star_ratio": star_ratio, "target_price": target_price, "real_cash_used": real_available_cash, "tracking_info": {}, "initial_qty": qty, "is_zero_start": False}
            if is_snapshot_mode: self.save_daily_snapshot(ticker, plan_result)
            return plan_result

        core_orders = []
        bonus_orders = []
        process_status = "예방적방어선"
        is_zero_start_fact = False

        if market_type == "REG":
            if is_rev_active or t_val > (split - 1):
                if not is_rev_active:
                    logging.info(f"🚨 [{ticker}] T값({t_val})이 {split-1}을 돌파하여 리버스 모드(소진 방어)로 강제 진입합니다.")
                    self.cfg.set_reverse_state(ticker, True, 0, 0.0, dynamic_t=t_val, rem_cash=real_available_cash, is_day_one=True)
                    is_rev_active = True
                else:
                    # 🚨 NEW: 리버스 장부 정산(잔액 역산 및 T값 스케일링)을 스냅샷 모드에서 원자적 1회 호출
                    if is_snapshot_mode:
                        self.cfg.apply_reverse_daily_settlement(ticker)

                # 🚨 MODIFIED: 갱신된 리버스 상태 다시 로드
                rev_state = self.cfg.get_reverse_state(ticker)
                dynamic_t = self._safe_float(rev_state.get('dynamic_t', 0.0))
                rem_cash = self._safe_float(rev_state.get('rem_cash', 0.0))
                is_day_one = rev_state.get('is_day_one', True)
                
                N_div = 10 if split <= 20 else 20
                sell_qty = math.floor(qty / N_div)
                
                if is_day_one:
                    if sell_qty > 0:
                        core_orders.append({"side": "SELL", "price": 0.0, "qty": sell_qty, "type": "MOC", "desc": "🩸리버스무조건매도(1일차)"})
                    process_status = "♻️리버스(1일차 진입)"
                else:
                    rev_star = ma_5day if ma_5day > 0 else (prev_close if prev_close > 0 else current_price)
                    buy_price = max(0.01, round(rev_star - 0.01, 2))
                    
                    buy_budget = rem_cash / 4.0
                    buy_qty = math.floor(buy_budget / buy_price) if buy_price > 0 else 0
                    
                    if buy_qty > 0:
                        core_orders.append({"side": "BUY", "price": buy_price, "qty": buy_qty, "type": "LOC", "desc": "🩸리버스쿼터매수"})
                    if sell_qty > 0:
                        core_orders.append({"side": "SELL", "price": round(rev_star, 2), "qty": sell_qty, "type": "LOC", "desc": "🩸리버스분할매도"})
        
                    process_status = "♻️리버스(진행중)"
                
                core_orders, bonus_orders = self._apply_wash_trade_shield(core_orders, bonus_orders)
                orders = core_orders + bonus_orders
                
                plan_result = {
                    "orders": orders, "core_orders": core_orders, "bonus_orders": bonus_orders,
                    "total_q": qty, "avg_price": avg_price, "t_val": dynamic_t,
                    "one_portion": rem_cash / 4.0, "process_status": process_status, "is_reverse": True,
                    "star_price": rev_star if not is_day_one else 0.0, "star_ratio": 0.0, 
                    "target_price": target_price, "real_cash_used": real_available_cash, 
                    "tracking_info": {}, "initial_qty": qty, "is_zero_start": False
                }
                if is_snapshot_mode: self.save_daily_snapshot(ticker, plan_result)
                return plan_result

            if qty == 0:
                is_zero_start_fact = True
                process_status = "✨새출발"
                buy_price = max(0.01, round(self._ceil(base_price * 1.15) - 0.01, 2))
                half_budget = one_portion_amt * 0.5
                buy_qty1 = int(math.floor(half_budget / buy_price)) if buy_price > 0 else 0
                buy_qty2 = int(math.floor((one_portion_amt - half_budget) / buy_price)) if buy_price > 0 else 0
                
                if buy_qty1 == 0 and buy_qty2 == 0 and buy_price > 0 and one_portion_amt >= buy_price:
                    buy_qty1 = int(math.floor(one_portion_amt / buy_price))
                
                if buy_qty1 > 0: core_orders.append({"side": "BUY", "price": buy_price, "qty": buy_qty1, "type": "LOC", "desc": "🆕새출발1"})
                if buy_qty2 > 0: core_orders.append({"side": "BUY", "price": buy_price, "qty": buy_qty2, "type": "LOC", "desc": "🆕새출발2"})
                
            elif is_jackpot_reached and t_val > (split - 1):
                process_status = "🎉대박익절"
                if qty > 0:
                    core_orders.append({"side": "SELL", "price": target_price, "qty": int(qty), "type": "LIMIT", "desc": "🎯전량대박익절"})
                
            else:
                safe_ceiling = min(avg_price, star_price) if star_price > 0 else avg_price
                p_avg = max(0.01, round(safe_ceiling - 0.01, 2))
                
                process_status = "🌓전반전" if t_val < (split / 2) else "🌕후반전"
                
                if t_val < (split / 2):
                    b1_budget = one_portion_amt * 0.5
                    b2_budget = one_portion_amt - b1_budget
                    p_star = max(0.01, round(star_price - 0.01, 2))
                    
                    q_avg = math.floor(b1_budget / p_avg) if p_avg > 0 else 0
                    q_star = math.floor(b2_budget / p_star) if p_star > 0 else 0
                    
                    if q_avg == 0 and q_star == 0:
                        if p_avg > 0 and one_portion_amt >= p_avg: q_avg = math.floor(one_portion_amt / p_avg)
                        elif p_star > 0 and one_portion_amt >= p_star: q_star = math.floor(one_portion_amt / p_star)
                    elif q_avg == 0 and q_star > 0: q_star = math.floor(one_portion_amt / p_star) if p_star > 0 else 0
                    elif q_star == 0 and q_avg > 0: q_avg = math.floor(one_portion_amt / p_avg) if p_avg > 0 else 0
                    
                    if q_avg > 0: core_orders.append({"side": "BUY", "price": p_avg, "qty": q_avg, "type": "LOC", "desc": "⚓평단매수"})
                    if q_star > 0: core_orders.append({"side": "BUY", "price": p_star, "qty": int(q_star), "type": "LOC", "desc": "💫별값매수"})
                else: 
                    p_star = max(0.01, round(star_price - 0.01, 2))
                    if p_star > 0:
                        q_star_total = int(math.floor(one_portion_amt / p_star))
                        if q_star_total > 0:
                            core_orders.append({"side": "BUY", "price": p_star, "qty": q_star_total, "type": "LOC", "desc": "💫별값매수(통합)"})

                q_sell = math.ceil(qty / 4)
                rem_qty = int(qty - q_sell)
                if star_price > 0 and q_sell > 0:
                    core_orders.append({"side": "SELL", "price": star_price, "qty": q_sell, "type": "LOC", "desc": "🌟별값매도(쿼터)"})
                if target_price > 0 and rem_qty > 0:
                    core_orders.append({"side": "SELL", "price": target_price, "qty": rem_qty, "type": "LIMIT", "desc": "🎯목표매도(잔여)"})

            if is_zero_start_fact and market_type != "AFTER":
                 core_orders = [o for o in core_orders if isinstance(o, dict) and o.get("side") != "SELL"]

            q_base = sum(int(self._safe_float(o.get('qty'))) for o in core_orders if isinstance(o, dict) and o.get('side') == 'BUY')
            if q_base > 0:
                bonus_orders.extend(sorted([{"side": "BUY", "price": math.floor((one_portion_amt / (q_base + n)) * 100) / 100.0, "qty": 1, "type": "LOC", "desc": f"🧲줍줍(+{n}주)"} for n in range(1, 6) if math.floor((one_portion_amt / (q_base + n)) * 100) / 100.0 > 0.01], key=lambda x: x['price'], reverse=True))

            core_orders, bonus_orders = self._apply_wash_trade_shield(core_orders, bonus_orders)        
            orders = core_orders + bonus_orders
            
            plan_result = {
                "orders": orders, "core_orders": core_orders, "bonus_orders": bonus_orders, "total_q": qty, "avg_price": avg_price,
                "t_val": t_val, "one_portion": one_portion_amt, "process_status": process_status,
                "is_reverse": False, "star_price": star_price, "star_ratio": star_ratio, "target_price": target_price,
                "real_cash_used": real_available_cash, "tracking_info": {}, "initial_qty": qty, "is_zero_start": is_zero_start_fact
            }
            if is_snapshot_mode: self.save_daily_snapshot(ticker, plan_result)
            return plan_result

        plan_result = {
            "orders": [], "core_orders": [], "bonus_orders": [], 
            "total_q": qty, "avg_price": avg_price, "t_val": t_val, 
            "one_portion": one_portion_amt, "process_status": "대기중",
            "is_reverse": False, "star_price": star_price, "star_ratio": star_ratio, 
            "target_price": target_price, "real_cash_used": real_available_cash, 
            "tracking_info": {}, "initial_qty": qty, "is_zero_start": is_zero_start_fact
        }
        if is_snapshot_mode: self.save_daily_snapshot(ticker, plan_result)
        return plan_result
