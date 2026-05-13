# ==========================================================
# FILE: strategy_v14_vwap.py
# ==========================================================
# (상단 주석 생략...)
# 🚨 MODIFIED: [V72.04 후반전 별값 매수 예산 통합 락온]
# - 후반전(T >= 분할/2) 진입 시 동일한 가격(별값)임에도 50:50으로 예산을 쪼개어
#   VWAP 최소 수량(10주) 요건을 스스로 박탈하던 기계적 분할 맹점 원천 차단.
# - 단일 가격 타격 시에는 예산을 100% 단일 버킷으로 통합하여 VWAP 격발 확률을 극대화함.
# ==========================================================
import math
import logging
import os
import json
import tempfile
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

class V14VwapStrategy:
    def __init__(self, config):
        self.cfg = config
        self.residual = {}
        self.executed = {"BUY_BUDGET": {}, "SELL_QTY": {}}
        self.state_loaded = {}

    # (...중략: _get_logical_date_str ~ record_execution 유지...)
    def _get_logical_date_str(self):
        now_est = datetime.now(ZoneInfo('America/New_York'))
        if now_est.hour < 4 or (now_est.hour == 4 and now_est.minute < 4):
            target_date = now_est - timedelta(days=1)
        else:
            target_date = now_est
        return target_date.strftime("%Y-%m-%d")

    def _get_state_file(self, ticker):
        today_str = self._get_logical_date_str()
        return f"data/vwap_state_V14_{today_str}_{ticker}.json"

    def _get_snapshot_file(self, ticker):
        today_str = self._get_logical_date_str()
        return f"data/daily_snapshot_V14VWAP_{today_str}_{ticker}.json"

    def _load_state_if_needed(self, ticker):
        today_str = self._get_logical_date_str()
        if self.state_loaded.get(ticker) == today_str:
            return 
            
        state_file = self._get_state_file(ticker)
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for k in self.executed.keys():
                        raw_val = data.get("executed", {}).get(k, 0)
                        self.executed[k][ticker] = int(raw_val) if k == "SELL_QTY" else float(raw_val)
                    self.state_loaded[ticker] = today_str
                    return
            except Exception:
                pass
                 
        self.executed["BUY_BUDGET"][ticker] = 0.0
        self.executed["SELL_QTY"][ticker] = 0
        self.state_loaded[ticker] = today_str

    def _save_state(self, ticker):
        today_str = self._get_logical_date_str()
        state_file = self._get_state_file(ticker)
        data = {
            "date": today_str,
            "residual": {},
            "executed": {
                "BUY_BUDGET": float(self.executed.get("BUY_BUDGET", {}).get(ticker, 0.0)),
                "SELL_QTY": int(self.executed.get("SELL_QTY", {}).get(ticker, 0))
            }
        }
        temp_path = None
        try:
            dir_name = os.path.dirname(state_file)
            os.makedirs(dir_name, exist_ok=True) 
            fd, temp_path = tempfile.mkstemp(dir=dir_name or '.', text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno()) 
            os.replace(temp_path, state_file)
            temp_path = None
        except Exception as e:
            logging.critical(f"🚨 [STATE SAVE FAILED] {ticker} 상태 저장 실패. 봇 기억상실 위험! 원인: {e}")
            if temp_path and os.path.exists(temp_path):
                try: os.unlink(temp_path)
                except OSError: pass

    def refund_residual(self, ticker, bucket, refund_value):
        pass

    def save_daily_snapshot(self, ticker, plan_data):
        today_str = self._get_logical_date_str()
        snap_file = self._get_snapshot_file(ticker)
        
        if os.path.exists(snap_file):
            return

        data = {
            "date": today_str,
            "plan": plan_data
        }
        temp_path = None
        try:
            dir_name = os.path.dirname(snap_file)
            os.makedirs(dir_name, exist_ok=True)
            fd, temp_path = tempfile.mkstemp(dir=dir_name or '.', text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno()) 
            os.replace(temp_path, snap_file)
            temp_path = None
        except Exception as e:
            logging.critical(f"🚨 [SNAPSHOT SAVE FAILED] {ticker} 스냅샷 저장 실패. 지시서 보존 불가! 원인: {e}")
            if temp_path and os.path.exists(temp_path):
                try: os.unlink(temp_path)
                except OSError: pass

    def load_daily_snapshot(self, ticker):
        snap_file = self._get_snapshot_file(ticker)
        if os.path.exists(snap_file):
            try:
                with open(snap_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get("plan")
            except Exception:
                pass
        return None

    def ensure_failsafe_snapshot(self, ticker, current_price, total_qty, avwap_qty, avg_price, prev_close, alloc_cash):
        snap = self.load_daily_snapshot(ticker)
        if snap is not None:
            return snap
            
        pure_qty = max(0, total_qty - avwap_qty)
        
        today_str_est = self._get_logical_date_str()
        legacy_qty = pure_qty
        legacy_avg = avg_price
        try:
            recs = [r for r in self.cfg.get_ledger() if r['ticker'] == ticker and not str(r.get("date", "")).startswith(today_str_est)]
            ledger_qty, ledger_avg, _, _ = self.cfg.calculate_holdings(ticker, recs)
            legacy_qty = ledger_qty
            legacy_avg = ledger_avg if ledger_qty > 0 else avg_price
        except Exception:
            pass
            
        logging.warning(f"🚨 [{ticker}] V14_VWAP 스냅샷 증발 감지! 페일세이프 긴급 복원 가동 (총잔고:{total_qty} - 암살자:{avwap_qty} = 본대:{pure_qty}주 | 이월 장부:{legacy_qty}주)")
        
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

    def _ceil(self, val): return math.ceil(val * 100) / 100.0
    def _floor(self, val): return math.floor(val * 100) / 100.0

    def reset_residual(self, ticker):
        pass

    def record_execution(self, ticker, side, qty, exec_price):
        self._load_state_if_needed(ticker)
        if side == "BUY":
            spent = float(qty * exec_price)
            self.executed["BUY_BUDGET"][ticker] = float(self.executed["BUY_BUDGET"].get(ticker, 0.0)) + spent
        else:
            self.executed["SELL_QTY"][ticker] = int(self.executed["SELL_QTY"].get(ticker, 0)) + int(qty)
        self._save_state(ticker)

    def get_plan(self, ticker, current_price, avg_price, qty, prev_close, ma_5day=0.0, market_type="REG", available_cash=0, is_simulation=False, is_snapshot_mode=False):
        if not is_snapshot_mode:
            cached_plan = self.load_daily_snapshot(ticker)
            if cached_plan:
                return cached_plan

        split = self.cfg.get_split_count(ticker)
        target_ratio = self.cfg.get_target_profit(ticker) / 100.0
        t_val, _ = self.cfg.get_absolute_t_val(ticker, qty, avg_price)
        
        depreciation_factor = 2.0 / split if split > 0 else 0.1
        star_ratio = target_ratio - (target_ratio * depreciation_factor * t_val)
        star_price = self._ceil(avg_price * (1 + star_ratio)) if avg_price > 0 else 0
        target_price = self._ceil(avg_price * (1 + target_ratio)) if avg_price > 0 else 0
        
        buy_star_price = round(star_price - 0.01, 2) if star_price > 0.01 else 0.0

        _, dynamic_budget, _ = self.cfg.calculate_v14_state(ticker)
        
        core_orders = []
        process_status = "예방적방어선"
        is_zero_start_fact = False
        
        est_tz_check = ZoneInfo('America/New_York')
        is_dst_active = bool(datetime.now(est_tz_check).dst())
        start_t = "042500" if is_dst_active else "052500"
        end_t = "045500" if is_dst_active else "055500"

        if qty == 0:
            is_zero_start_fact = True
            p_buy = self._ceil(prev_close * 1.15)
            buy_star_price = p_buy 
            
            b1_budget = dynamic_budget * 0.5
            b2_budget = dynamic_budget * 0.5
            q1 = math.floor(b1_budget / p_buy) if p_buy > 0 else 0
            q2 = math.floor(b2_budget / p_buy) if p_buy > 0 else 0
            
            if q1 > 0: 
                o_type = "VWAP" if q1 >= 10 else "LOC"
                desc = f"🆕새출발1({o_type})"
                core_orders.append({"side": "BUY", "price": p_buy, "qty": q1, "type": o_type, "start_time": start_t if o_type == "VWAP" else None, "end_time": end_t if o_type == "VWAP" else None, "desc": desc})
            if q2 > 0:
                o_type = "VWAP" if q2 >= 10 else "LOC"
                desc = f"🆕새출발2({o_type})"
                core_orders.append({"side": "BUY", "price": p_buy, "qty": q2, "type": o_type, "start_time": start_t if o_type == "VWAP" else None, "end_time": end_t if o_type == "VWAP" else None, "desc": desc})
            process_status = "✨새출발"
        else:
            p_avg = self._ceil(avg_price)
            if t_val < (split / 2):
                b1_budget = dynamic_budget * 0.5
                b2_budget = dynamic_budget * 0.5
                q_avg = math.floor(b1_budget / p_avg) if p_avg > 0 else 0
                q_star = math.floor(b2_budget / buy_star_price) if buy_star_price > 0 else 0
                
                if q_avg > 0: 
                    o_type = "VWAP" if q_avg >= 10 else "LOC"
                    desc = f"⚓평단매수({o_type})"
                    core_orders.append({"side": "BUY", "price": p_avg, "qty": q_avg, "type": o_type, "start_time": start_t if o_type == "VWAP" else None, "end_time": end_t if o_type == "VWAP" else None, "desc": desc})
                if q_star > 0: 
                    o_type = "VWAP" if q_star >= 10 else "LOC"
                    desc = f"💫별값매수({o_type})"
                    core_orders.append({"side": "BUY", "price": buy_star_price, "qty": q_star, "type": o_type, "start_time": start_t if o_type == "VWAP" else None, "end_time": end_t if o_type == "VWAP" else None, "desc": desc})
            else:
                # 🚨 MODIFIED: [V72.04] 후반전 별값 매수 예산 통합 락온
                q_total = math.floor(dynamic_budget / buy_star_price) if buy_star_price > 0 else 0
                if q_total > 0: 
                    o_type = "VWAP" if q_total >= 10 else "LOC"
                    desc = f"💫별값매수(통합:{o_type})"
                    core_orders.append({"side": "BUY", "price": buy_star_price, "qty": q_total, "type": o_type, "start_time": start_t if o_type == "VWAP" else None, "end_time": end_t if o_type == "VWAP" else None, "desc": desc})
            
            q_sell = math.ceil(qty / 4)
            if q_sell > 0:
                o_type = "VWAP" if q_sell >= 10 else "LOC"
                desc = f"🌟별값매도({o_type})"
                core_orders.append({"side": "SELL", "price": star_price, "qty": q_sell, "type": o_type, "start_time": start_t if o_type == "VWAP" else None, "end_time": end_t if o_type == "VWAP" else None, "desc": desc})
            if qty - q_sell > 0:
                core_orders.append({"side": "SELL", "price": target_price, "qty": qty - q_sell, "type": "LIMIT", "desc": "🎯목표매도(V)"})

        if is_zero_start_fact and market_type != "AFTER":
             core_orders = [o for o in core_orders if o.get("side") != "SELL"]

        plan_result = {
            'core_orders': core_orders, 'bonus_orders': [], 'orders': core_orders,
            't_val': t_val, 'one_portion': dynamic_budget, 'star_price': star_price,
            'buy_star_price': buy_star_price, 
            'star_ratio': star_ratio,
            'target_price': target_price, 'is_reverse': False,
            'process_status': process_status,
            'tracking_info': {},
            'initial_qty': int(qty),
            'is_zero_start': is_zero_start_fact 
        }
        
        self.save_daily_snapshot(ticker, plan_result)
        return plan_result

    def get_dynamic_plan(self, ticker, current_price, prev_close, current_weight, min_idx, alloc_cash, qty, avg_price, market_type="REG"):
        self._load_state_if_needed(ticker)
        cached_plan = self.load_daily_snapshot(ticker)
        if cached_plan:
            return cached_plan
        else:
            return self.get_plan(
                ticker=ticker,
                current_price=current_price,
                avg_price=avg_price,
                qty=qty,
                prev_close=prev_close,
                available_cash=alloc_cash,
                is_simulation=True,
                is_snapshot_mode=True,
                market_type=market_type
            )
