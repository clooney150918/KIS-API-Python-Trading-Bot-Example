# ==========================================================
# FILE: strategy_v14.py
# ==========================================================
# 🚨 MODIFIED: [Case 08 절대 규칙 준수] 스냅샷 무결성 파이프라인 팩트 교정 - os.path.exists 방어막 소각
# 🚨 MODIFIED: [Case 21] 후반전 별값 매수 예산 통합 100% 팩트 이식
# 🚨 MODIFIED: [Case 25] 오리지널 심해 줍줍 5단 폭포수 덫 공식 진공 압축 팩트 이식 완료
# 🚨 MODIFIED: [Case 16] 임시 파일 변수 스코프 전진 배치(Hoisting)로 UnboundLocalError 런타임 붕괴 완벽 차단
# ==========================================================
import math
import os
import json
import tempfile
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

class V14Strategy:
    def __init__(self, config):
        self.cfg = config

    def _ceil(self, val): return math.ceil(val * 100) / 100.0
    def _floor(self, val): return math.floor(val * 100) / 100.0

    def _get_logical_date_str(self):
        now_est = datetime.now(ZoneInfo('America/New_York'))
        if now_est.hour < 4 or (now_est.hour == 4 and now_est.minute < 4):
            target_date = now_est - timedelta(days=1)
        else:
            target_date = now_est
        return target_date.strftime("%Y-%m-%d")

    def save_daily_snapshot(self, ticker, plan_data):
        today_str = self._get_logical_date_str()
        snap_file = f"data/daily_snapshot_V14_{today_str}_{ticker}.json"
        
        # 🚨 MODIFIED: [Case 08] 스냅샷 멱등성 파괴 방어 (무조건 원자적 덮어쓰기)
        data = {
            "date": today_str,
            "total_q": int(plan_data.get('total_q', 0)),
            "avg_price": float(plan_data.get('avg_price', 0.0)),
            "one_portion": float(plan_data.get('one_portion', 0.0)),
            "star_price": float(plan_data.get('star_price', 0.0)),
            "star_ratio": float(plan_data.get('star_ratio', 0.0)),
            "t_val": float(plan_data.get('t_val', 0.0)),
            "is_reverse": bool(plan_data.get('is_reverse', False)),
            "orders": plan_data.get('orders', []),
            "core_orders": plan_data.get('core_orders', []),
            "bonus_orders": plan_data.get('bonus_orders', []),
            "process_status": plan_data.get('process_status', '')
        }
        
        os.makedirs(os.path.dirname(snap_file), exist_ok=True)
        # 🚨 MODIFIED: [Case 16] temp_path 및 fd 스코프 최상단 전진 배치 (UnboundLocalError 런타임 붕괴 원천 봉쇄)
        fd = None
        temp_path = None
        try:
            fd, temp_path = tempfile.mkstemp(dir=os.path.dirname(snap_file), text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                fd = None
                json.dump(data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, snap_file)
            temp_path = None
        except Exception:
            if fd is not None:
                try: os.close(fd)
                except OSError: pass
            if temp_path and os.path.exists(temp_path):
                try: os.remove(temp_path)
                except OSError: pass

    def load_daily_snapshot(self, ticker):
        today_str = self._get_logical_date_str()
        snap_file = f"data/daily_snapshot_V14_{today_str}_{ticker}.json"
        
        if os.path.exists(snap_file):
            try:
                with open(snap_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if data.get("date") == today_str:
                        return data
            except Exception:
                pass
        return None

    def _apply_wash_trade_shield(self, c_orders, b_orders):
        all_o = c_orders + b_orders
        has_sell_moc = any(o['type'] in ['MOC', 'MOO'] and o['side'] == 'SELL' for o in all_o)
        s_prices = [o['price'] for o in all_o if o['side'] == 'SELL' and o['price'] > 0]
        min_s = min(s_prices) if s_prices else 0.0

        def _clean(lst):
            res = []
            for o in lst:
                new_o = o.copy()
                if new_o['side'] == 'BUY':
                    if has_sell_moc and new_o['type'] in ['LOC', 'MOC']: 
                        continue 
                    if min_s > 0 and new_o['price'] >= min_s:
                        new_o['price'] = round(min_s - 0.01, 2)
                        if "🛡️" not in new_o['desc']: 
                            new_o['desc'] = f"🛡️교정_{new_o['desc'].replace('🧹', '')}"
                    new_o['price'] = max(0.01, new_o['price'])
                res.append(new_o)
            return res
        return _clean(c_orders), _clean(b_orders)

    def get_plan(self, ticker, current_price, avg_price, qty, prev_close, ma_5day=0.0, market_type="REG", available_cash=0, is_simulation=False, is_snapshot_mode=False, **kwargs):
        if not is_snapshot_mode:
            snap = self.load_daily_snapshot(ticker)
            if snap: return snap
                
        core_orders = []
        bonus_orders = []
        process_status = "" 
        tr_info = {}
        
        real_available_cash = max(0, available_cash)
        
        seed = self.cfg.get_seed(ticker)
        split = self.cfg.get_split_count(ticker)      
        target_pct_val = self.cfg.get_target_profit(ticker) 
        target_ratio = target_pct_val / 100.0
        
        portion = seed / split if split > 0 else 1.0
        t_val = (qty * avg_price) / portion if portion > 0 else 0.0
        t_val = round(t_val, 4)

        target_price = self._ceil(avg_price * (1 + target_ratio)) if avg_price > 0 else 0
        is_jackpot_reached = target_price > 0 and current_price >= target_price

        one_portion_amt = portion
        
        depreciation_factor = 2.0 / split if split > 0 else 0.1
        star_ratio = target_ratio - (target_ratio * depreciation_factor * t_val)
        star_price = self._ceil(avg_price * (1 + star_ratio)) if avg_price > 0 else 0
            
        base_price = current_price if current_price > 0 else prev_close
        if base_price <= 0: 
            plan_result = {"orders": [], "core_orders": [], "bonus_orders": [], "total_q": qty, "avg_price": avg_price, "t_val": t_val, "one_portion": one_portion_amt, "process_status": "⛔가격오류", "is_reverse": False, "star_price": star_price, "star_ratio": star_ratio, "real_cash_used": real_available_cash, "tracking_info": tr_info}
            if is_snapshot_mode: self.save_daily_snapshot(ticker, plan_result)
            return plan_result
            
        if market_type == "REG":
            if qty == 0:
                process_status = "✨새출발"
                buy_price = max(0.01, round(self._ceil(base_price * 1.15) - 0.01, 2))
                half_budget = one_portion_amt * 0.5
                buy_qty1 = int(math.floor(half_budget / buy_price)) if buy_price > 0 else 0
                buy_qty2 = int(math.floor((one_portion_amt - half_budget) / buy_price)) if buy_price > 0 else 0
                
                if buy_qty1 == 0 and buy_qty2 == 0 and buy_price > 0 and one_portion_amt >= buy_price:
                    buy_qty1 = int(math.floor(one_portion_amt / buy_price))
                
                if buy_qty1 > 0: core_orders.append({"side": "BUY", "price": buy_price, "qty": buy_qty1, "type": "LOC", "desc": "🆕새출발1"})
                if buy_qty2 > 0: core_orders.append({"side": "BUY", "price": buy_price, "qty": buy_qty2, "type": "LOC", "desc": "🆕새출발2"})
                
                # 🚨 MODIFIED: [Case 25] 오리지널 심해 줍줍(Jubjub) 5단 폭포수 1줄 진공 압축 및 안정 정렬 팩트 이식
                q_base = sum(o['qty'] for o in core_orders if o['side'] == 'BUY')
                if q_base > 0:
                    bonus_orders.extend(sorted([{"side": "BUY", "price": math.floor((one_portion_amt / (q_base + n)) * 100) / 100.0, "qty": 1, "type": "LOC", "desc": f"🧲줍줍(+{n}주)"} for n in range(1, 6) if math.floor((one_portion_amt / (q_base + n)) * 100) / 100.0 > 0.01], key=lambda x: x['price'], reverse=True))
            
                orders = core_orders + bonus_orders
                plan_result = {"orders": orders, "core_orders": core_orders, "bonus_orders": bonus_orders, "total_q": qty, "avg_price": avg_price, "t_val": t_val, "one_portion": one_portion_amt, "process_status": process_status, "is_reverse": False, "star_price": star_price, "star_ratio": star_ratio, "real_cash_used": real_available_cash, "tracking_info": tr_info}
                if is_snapshot_mode: self.save_daily_snapshot(ticker, plan_result)
                return plan_result

            if is_jackpot_reached and t_val > (split - 1):
                process_status = "🎉대박익절"
                if qty > 0:
                    core_orders.append({"side": "SELL", "price": target_price, "qty": int(qty), "type": "LIMIT", "desc": "🎯전량대박익절"})
                core_orders, bonus_orders = self._apply_wash_trade_shield(core_orders, bonus_orders)        
                orders = core_orders + bonus_orders
                
                plan_result = {"orders": orders, "core_orders": core_orders, "bonus_orders": bonus_orders, "total_q": qty, "avg_price": avg_price, "t_val": t_val, "one_portion": one_portion_amt, "process_status": process_status, "is_reverse": False, "star_price": star_price, "star_ratio": star_ratio, "real_cash_used": real_available_cash, "tracking_info": tr_info}
                if is_snapshot_mode: self.save_daily_snapshot(ticker, plan_result)
                return plan_result
                
            elif t_val < (split / 2): process_status = "🌓전반전"
            else: process_status = "🌕후반전"

            if t_val < (split / 2):
                p_avg = max(0.01, round(min(avg_price, star_price) - 0.01, 2))
                p_star = max(0.01, round(star_price - 0.01, 2))

                half_amt = one_portion_amt * 0.5
                q_avg = math.floor(half_amt / p_avg) if p_avg > 0 else 0
                q_star = math.floor((one_portion_amt - half_amt) / p_star) if p_star > 0 else 0
                
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

            if qty > 0:
                q_qty = int(math.ceil(qty / 4))
                rem_qty = int(qty - q_qty)
                if star_price > 0 and q_qty > 0:
                    core_orders.append({"side": "SELL", "price": star_price, "qty": q_qty, "type": "LOC", "desc": "🌟별값매도(쿼터)"})
                if target_price > 0 and rem_qty > 0:
                    core_orders.append({"side": "SELL", "price": target_price, "qty": rem_qty, "type": "LIMIT", "desc": "🎯목표매도(잔여)"})

            q_base = sum(o['qty'] for o in core_orders if o['side'] == 'BUY')
            if q_base > 0:
                bonus_orders.extend(sorted([{"side": "BUY", "price": math.floor((one_portion_amt / (q_base + n)) * 100) / 100.0, "qty": 1, "type": "LOC", "desc": f"🧲줍줍(+{n}주)"} for n in range(1, 6) if math.floor((one_portion_amt / (q_base + n)) * 100) / 100.0 > 0.01], key=lambda x: x['price'], reverse=True))

            core_orders, bonus_orders = self._apply_wash_trade_shield(core_orders, bonus_orders)        
            orders = core_orders + bonus_orders
            
            plan_result = {
                "orders": orders, "core_orders": core_orders, "bonus_orders": bonus_orders, "total_q": qty, "avg_price": avg_price,
                "t_val": t_val, "one_portion": one_portion_amt, "process_status": process_status,
                "is_reverse": False, "star_price": star_price, "star_ratio": star_ratio,
                "real_cash_used": real_available_cash,
                "tracking_info": tr_info 
            }
            if is_snapshot_mode: self.save_daily_snapshot(ticker, plan_result)
            return plan_result

    def check_sniper_condition(self, ticker, cfg, broker, chat_id):
        snap = self.load_daily_snapshot(ticker)
        if not snap:
            return {"action": "HOLD", "reason": "스냅샷 부재", "limit_price": 0.0, "qty": 0}
            
        qty = snap.get('total_q', 0)
        avg_price = snap.get('avg_price', 0.0)
        star_price = snap.get('star_price', 0.0)
        target_price = snap.get('target_price', 0.0)
        is_reverse = snap.get('is_reverse', False)
        
        if qty <= 0:
            return {"action": "HOLD", "reason": "보유량 0주", "limit_price": 0.0, "qty": 0}

        q_qty = int(math.ceil(qty / 4))
        rem_qty = int(qty - q_qty)
        
        target = target_price if target_price > 0 else 0.0
        
        return {"action": "HOLD", "reason": "V14 상방 스나이퍼 감시 중", "limit_price": target, "qty": q_qty}
