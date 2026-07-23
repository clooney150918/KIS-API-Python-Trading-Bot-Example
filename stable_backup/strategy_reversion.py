# ==========================================================
# FILE: strategy_reversion.py
# ==========================================================
# 🚨 MODIFIED: [정액제 기반 정량제 100% 락온] 1회분 할당 예산(15%)을 타점으로 나누어 '당일 매수 정량(Fixed-Quantity)'을 산출하고 영구 고정. 폭락 시 예산을 억지로 소진하지 않고 남은 차익을 시드 풀로 보존하여 매수 가능 횟수(Runway)를 무한 확장함.
# 🚨 MODIFIED: [Lost Update 궁극 방어] 모든 상태 및 스냅샷 파일(JSON) 읽기/쓰기 연산에 GlobalThrottle.get_file_lock()을 100% 결속하여 더티 리드(Dirty Read) 및 동시성 파괴 원천 차단.
# 🚨 MODIFIED: [제4헌법 원자적 쓰기 절대 사수] tempfile 생성 ➔ flush ➔ fsync ➔ os.replace의 100% 원자적 파이프라인 강제 주입.
# 🚨 MODIFIED: [메모리 유령화(Ghost Memory) 붕괴 궁극 수술] 디스크 파일의 존재 여부 및 유효성을 교차 검증하여, 파일이 소각되었을 경우 메모리의 당일 매도 수량(SELL_QTY)을 즉각 0으로 원자적 초기화.
# 🚨 MODIFIED: [Case 46] 0주 스냅샷 오염 감지 시 YF 공식 종가를 기반으로 타점을 정밀하게 자가 치유(Self-Healing)하는 방어막 결속 완료.
# ==========================================================
import math
import os
import json
import tempfile
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from global_throttle import GlobalThrottle # 🚨 NEW: 중앙 통제소 결속

class ReversionStrategy:
    def __init__(self, config):
        self.cfg = config
        self.executed = {"BUY_BUDGET": {}, "SELL_QTY": {}}
        self.state_loaded = {}

    def _safe_float(self, value):
        try:
            val = float(str(value or 0.0).replace(',', ''))
            if math.isnan(val) or math.isinf(val): return 0.0
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
        return f"data/vwap_state_REV_{today_str}_{ticker}.json"

    def _get_snapshot_file(self, ticker):
        today_str = self._get_logical_date_str()
        return f"data/daily_snapshot_REV_{today_str}_{ticker}.json"

    def _load_state_if_needed(self, ticker):
        today_str = self._get_logical_date_str()
        state_file = self._get_state_file(ticker)
        
        is_disk_valid = False
        # 🚨 MODIFIED: File Mutex 결속
        with GlobalThrottle.get_file_lock(state_file):
            try:
                with open(state_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if data.get("date") == today_str:
                        for k in self.executed.keys():
                            raw_val = (data.get("executed") or {}).get(k, 0)
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
        data = {
            "date": today_str,
            "executed": {
                "BUY_BUDGET": self._safe_float((self.executed.get("BUY_BUDGET") or {}).get(ticker, 0.0)),
                "SELL_QTY": int(self._safe_float((self.executed.get("SELL_QTY") or {}).get(ticker, 0)))
            }
        }
        
        # 🚨 MODIFIED: File Mutex 결속 및 제4헌법 원자적 쓰기 100% 락온
        with GlobalThrottle.get_file_lock(state_file):
            fd = None
            temp_path = None
            try:
                dir_name = os.path.dirname(state_file)
                if dir_name:
                    try: os.makedirs(dir_name, exist_ok=True)
                    except OSError: pass
                
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
                logging.error(f"🚨 [{ticker}] V-REV 상태 파일 원자적 쓰기 실패: {e}")

    def save_daily_snapshot(self, ticker, plan_data):
        snap_file = self._get_snapshot_file(ticker)
        today_str = self._get_logical_date_str()
        data = {
            "date": today_str,
            "plan": plan_data
        }
        
        # 🚨 MODIFIED: File Mutex 결속 및 제4헌법 원자적 쓰기 100% 락온
        with GlobalThrottle.get_file_lock(snap_file):
            fd = None
            temp_path = None
            try:
                dir_name = os.path.dirname(snap_file)
                if dir_name:
                    try: os.makedirs(dir_name, exist_ok=True)
                    except OSError: pass
                
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
                logging.error(f"🚨 [{ticker}] V-REV 스냅샷 파일 원자적 쓰기 실패: {e}")

    def load_daily_snapshot(self, ticker):
        snap_file = self._get_snapshot_file(ticker)
        # 🚨 MODIFIED: File Mutex 결속
        with GlobalThrottle.get_file_lock(snap_file):
            try:
                with open(snap_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get("plan")
            except Exception:
                pass
            return None

    def ensure_failsafe_snapshot(self, ticker, curr_p, prev_c, alloc_cash, q_data, total_kis_qty, avwap_qty, actual_avg=0.0):
        curr_p = self._safe_float(curr_p)
        prev_c = self._safe_float(prev_c)
        alloc_cash = self._safe_float(alloc_cash)
        actual_avg = self._safe_float(actual_avg)
        
        snap = self.load_daily_snapshot(ticker)
        if snap is not None:
            return snap
            
        today_str_est = self._get_logical_date_str()
        legacy_lots = [item for item in (q_data or []) if isinstance(item, dict) and not str(item.get("date", "")).startswith(today_str_est)]
        
        logging.warning(f"🚨 [{ticker}] V_REV 스냅샷 증발 감지! 페일세이프 긴급 복원 가동")
        
        return self.get_dynamic_plan(
            ticker=ticker,
            curr_p=curr_p,
            prev_c=prev_c,
            current_weight=0.0,
            vwap_status={},
            min_idx=-1,
            alloc_cash=alloc_cash,
            q_data=legacy_lots,
            is_snapshot_mode=True,
            market_type="REG",
            actual_qty=total_kis_qty,
            actual_avg=actual_avg
        )

    def record_execution(self, ticker, side, qty, exec_price):
        self._load_state_if_needed(ticker)
        safe_qty = int(self._safe_float(qty))
        safe_price = self._safe_float(exec_price)
        
        if side == "BUY":
            spent = safe_qty * safe_price
            self.executed["BUY_BUDGET"][ticker] = self._safe_float((self.executed.get("BUY_BUDGET") or {}).get(ticker, 0.0)) + spent
        else:
            self.executed["SELL_QTY"][ticker] = int(self._safe_float((self.executed.get("SELL_QTY") or {}).get(ticker, 0))) + safe_qty
        self._save_state(ticker)

    def get_dynamic_plan(self, ticker, curr_p, prev_c, current_weight, vwap_status, min_idx, alloc_cash, q_data, is_snapshot_mode=False, market_type="REG", actual_qty=0, actual_avg=0.0):
        curr_p = self._safe_float(curr_p)
        prev_c = self._safe_float(prev_c)
        current_weight = self._safe_float(current_weight)
        alloc_cash = self._safe_float(alloc_cash)

        self._load_state_if_needed(ticker)

        cached_plan = self.load_daily_snapshot(ticker)
        
        if cached_plan:
            is_zero_val = cached_plan.get("is_zero_start")
            if is_zero_val is None:
                tot_q_snap = int(self._safe_float(cached_plan.get("snapshot_total_q", cached_plan.get("total_q", -1))))
                is_zero_snap = (tot_q_snap == 0)
            else:
                is_zero_snap = str(is_zero_val).lower() == 'true'

            # 🚨 MODIFIED: [Case 46 자가치유 결속] 0주 스냅샷 오염 감지 시 YF 무결성 종가 기반 정밀 보정
            if is_zero_snap and prev_c > 0.0:
                orders = cached_plan.get("orders", [])
                buy_orders = [o for o in orders if isinstance(o, dict) and str(o.get("side")) == "BUY"]
                
                target_p1 = round(prev_c * 1.15, 2)
                target_p2 = round(prev_c * 0.999, 2)
                
                is_polluted = False
                for o in buy_orders:
                    p = self._safe_float(o.get("price"))
                    desc = str(o.get("desc", ""))
                    if p > 0:
                        if "Buy1" in desc or "1" in desc:
                            if abs(p - target_p1) / target_p1 >= 0.01: is_polluted = True
                        elif "Buy2" in desc or "2" in desc:
                            if abs(p - target_p2) / target_p2 >= 0.01: is_polluted = True
                
                if is_polluted:
                    logging.warning(f"🚨 [{ticker}] 0주 스냅샷 오염 감지! YF 무결성 종가(${prev_c}) 기반으로 타점을 즉각 자가 치유(Self-Healing)합니다.")
                    
                    seed_val = self._safe_float(self.cfg.get_seed(ticker))
                    daily_limit = seed_val * 0.15
                    
                    safe_alloc_cash = alloc_cash if alloc_cash > 0.0 else daily_limit
                    safe_alloc_cash = min(safe_alloc_cash, daily_limit) if daily_limit > 0 else safe_alloc_cash
                    
                    total_spent = self._safe_float((self.executed.get("BUY_BUDGET") or {}).get(ticker, 0.0))
                    rem_budget = max(0.0, safe_alloc_cash - total_spent)
                    
                    b1_budget = rem_budget * 0.5
                    b2_budget = rem_budget * 0.5
                    
                    # 🚨 MODIFIED: 정액제 기반 정량제(Fixed-Amount based Fixed-Quantity) 핵심 팩트
                    new_q1 = math.floor(b1_budget / target_p1) if target_p1 > 0 else 0
                    new_q2 = math.floor(b2_budget / target_p2) if target_p2 > 0 else 0
                    
                    if new_q1 == 0 and new_q2 == 0:
                        if target_p1 > 0 and rem_budget >= target_p1: new_q1 = math.floor(rem_budget / target_p1)
                        elif target_p2 > 0 and rem_budget >= target_p2: new_q2 = math.floor(rem_budget / target_p2)
                    elif new_q1 == 0 and new_q2 > 0:
                        new_q2 = math.floor(rem_budget / target_p2) if target_p2 > 0 else 0
                    elif new_q2 == 0 and new_q1 > 0:
                        new_q1 = math.floor(rem_budget / target_p1) if target_p1 > 0 else 0

                    for o in cached_plan.get("orders", []):
                        if str(o.get("side")) == "BUY":
                            desc = str(o.get("desc", ""))
                            if "Buy1" in desc or "1" in desc:
                                o["price"] = target_p1
                                o["qty"] = new_q1
                            elif "Buy2" in desc or "2" in desc:
                                o["price"] = target_p2
                                o["qty"] = new_q2

                    self.save_daily_snapshot(ticker, cached_plan)
        
        if not is_snapshot_mode and cached_plan:
            return cached_plan

        valid_q_data = [item for item in (q_data or []) if isinstance(item, dict) and self._safe_float(item.get('price')) > 0]
        
        total_q = sum(int(self._safe_float(item.get("qty"))) for item in valid_q_data)
        total_inv = sum(self._safe_float(item.get('qty')) * self._safe_float(item.get('price')) for item in valid_q_data)
        
        dates_in_queue = sorted(list(set(str(item.get('date', '')) for item in valid_q_data if item.get('date'))), reverse=True)
        l1_qty, l1_price = 0, 0.0
        
        if dates_in_queue:
            lots_1 = [item for item in valid_q_data if str(item.get('date', '')) == dates_in_queue[0]]
            l1_qty = sum(int(self._safe_float(item.get('qty'))) for item in lots_1)
            l1_price = sum(self._safe_float(item.get('qty')) * self._safe_float(item.get('price')) for item in lots_1) / l1_qty if l1_qty > 0 else 0.0
        
        upper_qty = total_q - l1_qty
        pure_l1_qty = l1_qty
        pure_upper_qty = upper_qty

        trigger_l1 = round(l1_price * 1.006, 2)
        
        if pure_upper_qty > 0 and len(dates_in_queue) >= 2:
            upper_inv = max(0.0, total_inv - (l1_price * l1_qty))
            upper_price = upper_inv / pure_upper_qty if pure_upper_qty > 0 else 0.0
            trigger_upper = round(upper_price * 1.010, 2)
        else:
            trigger_upper = 0.0

        if cached_plan:
            is_zero_val = cached_plan.get("is_zero_start")
            if is_zero_val is None:
                tot_q_snap = int(self._safe_float(cached_plan.get("snapshot_total_q", cached_plan.get("total_q", -1))))
                is_zero_start_session = (tot_q_snap == 0)
            else:
                is_zero_start_session = str(is_zero_val).lower() == 'true'
        else:
            is_zero_start_session = (total_q == 0)

        if is_zero_start_session and total_q > 0:
            is_zero_start_session = False
        elif not is_zero_start_session and total_q == 0:
            is_zero_start_session = True

        if is_zero_start_session and prev_c <= 0.0:
            error_plan = {
                "orders": [], "trigger_loc": False, "total_q": total_q, "is_zero_start": True, "process_status": "⛔가격오류"
            }
            if is_snapshot_mode:
                self.save_daily_snapshot(ticker, error_plan)
            return error_plan

        if is_zero_start_session:
            p1_trigger = round(prev_c * 1.15, 2)
            p2_trigger = round(prev_c * 0.999, 2)
        else:
            safe_anchor = l1_price if l1_price > 0.0 else prev_c
            p1_trigger = round(safe_anchor * 0.998, 2)
            p2_trigger = round(safe_anchor * 0.993, 2)

        rem_qty_total = max(0, int(pure_l1_qty + pure_upper_qty) - int(self._safe_float((self.executed.get("SELL_QTY") or {}).get(ticker, 0))))
        available_l1 = min(pure_l1_qty, rem_qty_total) if rem_qty_total > 0 else 0
        available_upper = min(pure_upper_qty, rem_qty_total - available_l1) if rem_qty_total > 0 else 0
        
        if rem_qty_total > 0:
            active_sells = []
            if available_l1 > 0 and trigger_l1 > 0:
                active_sells.append(trigger_l1)
            if available_upper > 0 and trigger_upper >= 0.01:
                active_sells.append(trigger_upper)
                
            # 🚨 MODIFIED: [자전거래 방어막 캡핑 결속] 매수가격이 매도가격 이상일 경우 즉각 하향 조치
            if active_sells:
                min_sell = min(active_sells)
                if p1_trigger >= min_sell:
                    p1_trigger = max(0.01, round(min_sell - 0.01, 2))
                if p2_trigger >= min_sell:
                    p2_trigger = max(0.01, round(min_sell - 0.01, 2))

        orders = []

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

        total_spent = 0.0 if is_snapshot_mode else self._safe_float((self.executed.get("BUY_BUDGET") or {}).get(ticker, 0.0))
        
        # 🚨 MODIFIED: [정액제 기반 정량제 100% 팩트 연산] 총 시드의 15%로 1회 예산(Budget) 한도 절대 고정
        seed_val = self._safe_float(self.cfg.get_seed(ticker))
        daily_limit = seed_val * 0.15
        
        if alloc_cash <= 0.0:
            alloc_cash = daily_limit
            
        safe_alloc_cash = min(float(alloc_cash), daily_limit) if daily_limit > 0 else float(alloc_cash)
        rem_budget = max(0.0, safe_alloc_cash - total_spent)
        
        if rem_budget > 0:
            b1_budget = rem_budget * 0.5
            b2_budget = rem_budget * 0.5
            
            # 🚨 MODIFIED: [목표 수량 영구 고정] 이 시점에 산출된 정량(q1, q2)은 당일 절대 변경되지 않으며, 폭락 시 차익은 그대로 시드 풀로 세이브됨
            q1 = math.floor(b1_budget / p1_trigger) if p1_trigger > 0 else 0
            q2 = math.floor(b2_budget / p2_trigger) if p2_trigger > 0 else 0
            
            if q1 == 0 and q2 == 0:
                if p1_trigger > 0 and rem_budget >= p1_trigger:
                    q1 = math.floor(rem_budget / p1_trigger)
                elif p2_trigger > 0 and rem_budget >= p2_trigger:
                    q2 = math.floor(rem_budget / p2_trigger)
            elif q1 == 0 and q2 > 0:
                q2 = math.floor(rem_budget / p2_trigger) if p2_trigger > 0 else 0
            elif q2 == 0 and q1 > 0:
                q1 = math.floor(rem_budget / p1_trigger) if p1_trigger > 0 else 0
            
            if q1 > 0:
                ord_type = "VWAP"
                desc_str = "VWAP매수(Buy1)"
                orders.append({"side": "BUY", "qty": q1, "price": p1_trigger, "type": ord_type, "start_time": start_t, "end_time": end_t, "desc": desc_str})
            if q2 > 0:
                ord_type = "VWAP"
                desc_str = "VWAP매수(Buy2)"
                orders.append({"side": "BUY", "qty": q2, "price": p2_trigger, "type": ord_type, "start_time": start_t, "end_time": end_t, "desc": desc_str})
        
        if rem_qty_total > 0:
            sell_dict = {}
            if available_l1 > 0 and trigger_l1 > 0:
                sell_dict[trigger_l1] = sell_dict.get(trigger_l1, 0) + available_l1
            if available_upper > 0 and trigger_upper >= 0.01:
                sell_dict[trigger_upper] = sell_dict.get(trigger_upper, 0) + available_upper
                
            for price in sorted(sell_dict.keys()):
                s_qty = sell_dict[price]
                ord_type = "VWAP"
                
                if price == trigger_l1 and price == trigger_upper:
                    desc_str = "통합탈출"
                elif price == trigger_l1:
                    desc_str = "1층탈출"
                elif price == trigger_upper:
                    desc_str = "상위층탈출"
                else:
                    desc_str = "잔여탈출"
                    
                orders.append({
                    "side": "SELL", "qty": s_qty, "price": price, "type": ord_type, 
                    "start_time": start_t, 
                    "end_time": end_t, 
                    "desc": desc_str
                })
        
        plan_result = {
            "orders": orders, 
            "trigger_loc": False, 
            "total_q": total_q,
            "is_zero_start": is_zero_start_session,
            "process_status": "정상연산"
        }
        
        if is_zero_start_session and market_type != "AFTER":
            plan_result["orders"] = [o for o in plan_result.get("orders", []) if o.get("side") != "SELL"]
        
        if is_snapshot_mode:
            self.save_daily_snapshot(ticker, plan_result)

        self._save_state(ticker)
        return plan_result
