# ==========================================================
# FILE: strategy_reversion.py
# ==========================================================
# (상단 주석 생략...)
# 🚨 MODIFIED: [V75.11 예산 증발 데이터 기아(Amnesia) 완벽 수술]
# - 상태 파일(_load_state_if_needed)에서 날짜(date) 비교가 누락되어 어제의 예산 지출(BUY_BUDGET)이 
#   오늘로 강제 이월(Carry-over)되는 치명적 하극상 원천 차단.
# - 날짜가 일치할 때만 잔차를 로드하고, 다르면 0.0으로 팩트 초기화하도록 멱등성 락온.
# - 스냅샷 모의 장전 시(is_snapshot_mode=True), 잔여 예산이 $0.0으로 기집행 처리되어
#   매수 덫이 통째로 렌더링에서 증발하던 사용자 1의 맹점 원천 차단.
# ==========================================================
import math
import os
import json
import tempfile
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

class ReversionStrategy:
    def __init__(self, config):
        self.cfg = config
        self.residual = {}
        self.executed = {"BUY_BUDGET": {}, "SELL_QTY": {}}
        self.state_loaded = {}

    def _get_logical_date_str(self):
        now_est = datetime.now(ZoneInfo('America/New_York'))
        if now_est.hour < 4 or (now_est.hour == 4 and now_est.minute < 4):
            target_date = now_est - timedelta(days=1)
        else:
            target_date = now_est
        return target_date.strftime("%Y-%m-%d")

    def _get_state_file(self, ticker):
        today_str = self._get_logical_date_str()
        return f"data/vwap_state_REV_{today_str}_{ticker}.json"

    def _get_snapshot_file(self, ticker):
        today_str = self._get_logical_date_str()
        return f"data/daily_snapshot_REV_{today_str}_{ticker}.json"

    # 🚨 MODIFIED: [V75.11 예산 증발 기억상실 맹점 완벽 수술]
    def _load_state_if_needed(self, ticker):
        today_str = self._get_logical_date_str()
        if self.state_loaded.get(ticker) == today_str:
            return 
            
        state_file = self._get_state_file(ticker)
        if os.path.exists(state_file):
            try:
                with open(state_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 🚨 팩트 스캔: 날짜가 같을 때만 어제의 예산 복원
                    if data.get("date") == today_str:
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
        self._save_state(ticker) # 🚨 초기화 저장 강제 락온

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
            if dir_name and not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, state_file)
            temp_path = None
        except Exception:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass

    def refund_residual(self, ticker, bucket, refund_value):
        pass

    def save_daily_snapshot(self, ticker, plan_data):
        snap_file = self._get_snapshot_file(ticker)
        if os.path.exists(snap_file):
            return
            
        today_str = self._get_logical_date_str()
        data = {
            "date": today_str,
            "plan": plan_data
        }
        temp_path = None
        try:
            dir_name = os.path.dirname(snap_file)
            if not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, snap_file)
            temp_path = None
        except Exception:
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

    def ensure_failsafe_snapshot(self, ticker, curr_p, prev_c, alloc_cash, q_data, total_kis_qty, avwap_qty):
        snap = self.load_daily_snapshot(ticker)
        if snap is not None:
            return snap
            
        pure_qty = max(0, total_kis_qty - avwap_qty)
        
        today_str_est = self._get_logical_date_str()
        legacy_lots = [item for item in q_data if not str(item.get("date", "")).startswith(today_str_est)]
        legacy_q = sum(int(item.get("qty", 0)) for item in legacy_lots if float(item.get('price', 0.0)) > 0)
        
        if pure_qty != legacy_q:
            logging.warning(f"⚠️ [{ticker}] V-REV 페일세이프 경고: KIS 순수 본대 수량({pure_qty}주)과 이월 큐 장부 수량({legacy_q}주) 불일치 감지. CALIB 비파괴 보정 또는 수동 동기화 요망.")
        
        logging.warning(f"🚨 [{ticker}] V_REV 스냅샷 증발 감지! 페일세이프 긴급 복원 가동 (KIS총잔고:{total_kis_qty} - 암살자:{avwap_qty} = 본대:{pure_qty}주 | 이월 큐 장부:{legacy_q}주)")
        
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
            market_type="REG"
        )

    def reset_residual(self, ticker):
        pass

    def record_execution(self, ticker, side, qty, exec_price):
        self._load_state_if_needed(ticker)
        safe_qty = int(float(qty or 0))
        safe_price = float(exec_price or 0.0)
        
        if side == "BUY":
            spent = safe_qty * safe_price
            self.executed["BUY_BUDGET"][ticker] = float(self.executed.get("BUY_BUDGET", {}).get(ticker, 0.0)) + spent
        else:
            self.executed["SELL_QTY"][ticker] = int(self.executed.get("SELL_QTY", {}).get(ticker, 0)) + safe_qty
        self._save_state(ticker)

    def get_dynamic_plan(self, ticker, curr_p, prev_c, current_weight, vwap_status, min_idx, alloc_cash, q_data, is_snapshot_mode=False, market_type="REG"):
        self._load_state_if_needed(ticker)

        cached_plan = self.load_daily_snapshot(ticker)
        
        # 🚨 MODIFIED: [V72.19 V-REV 덫 복원 시 스냅샷 데이터 기아 방어 전진 배치]
        if not is_snapshot_mode and cached_plan:
            return cached_plan

        # 🚨 [제20경고 팩트 검증] 큐(Queue) 장부의 순수 평단가 역산. KIS 평단가(actual_avg) 절대 참조 금지.
        valid_q_data = [item for item in q_data if float(item.get('price', 0.0)) > 0]
        total_q = sum(int(item.get("qty", 0)) for item in valid_q_data)
        total_inv = sum(float(item.get('qty', 0)) * float(item.get('price', 0.0)) for item in valid_q_data)
        avg_price = (total_inv / total_q) if total_q > 0 else 0.0
        
        dates_in_queue = sorted(list(set(item.get('date') for item in valid_q_data if item.get('date'))), reverse=True)
        l1_qty, l1_price = 0, 0.0
        
        if dates_in_queue:
            lots_1 = [item for item in valid_q_data if item.get('date') == dates_in_queue[0]]
            l1_qty = sum(int(item.get('qty', 0)) for item in lots_1)
            l1_price = sum(float(item.get('qty', 0)) * float(item.get('price', 0.0)) for item in lots_1) / l1_qty if l1_qty > 0 else 0.0
        
        upper_qty = total_q - l1_qty

        # 🚨 MODIFIED: [V72.13 V-REV 1층 독립 및 상위층 총평단가 연동 엑시트 전술 이식]
        trigger_l1 = round(l1_price * 1.006, 2)
        trigger_upper = round(avg_price * 1.010, 2) if upper_qty > 0 else 0.0

        if is_snapshot_mode:
            is_zero_start_session = (total_q == 0)
        else:
            if cached_plan:
                is_zero_start_session = cached_plan.get("is_zero_start", cached_plan.get("snapshot_total_q", cached_plan.get("total_q", -1)) == 0)
            else:
                today_str_est = self._get_logical_date_str()
                legacy_lots = [item for item in valid_q_data if not str(item.get("date", "")).startswith(today_str_est)]
                legacy_q = sum(int(item.get("qty", 0)) for item in legacy_lots)
                is_zero_start_session = (legacy_q == 0)

        if is_zero_start_session or total_q == 0:
            side = "BUY"
            p1_trigger = round(prev_c * 1.15, 2)
            p2_trigger = round(prev_c * 0.999, 2)
        else:
            side = "SELL" if curr_p > prev_c else "BUY"
            # 🚨 MODIFIED: [V75.05 제20경고 절대 헌법 준수: V-REV 매수 타점 1층 평단가 앵커 락온 및 타점 배수 팩트 교정]
            safe_anchor = l1_price if l1_price > 0.0 else prev_c
            p1_trigger = round(safe_anchor * 0.9976, 2)
            p2_trigger = round(safe_anchor * 0.9887, 2)

        # 🚨 MODIFIED: [V72.24 자전거래(Wash Sale) 락온 방어막 복구]
        rem_qty_total = max(0, int(total_q) - int(self.executed["SELL_QTY"].get(ticker, 0)))
        available_l1 = min(l1_qty, rem_qty_total) if rem_qty_total > 0 else 0
        available_upper = min(upper_qty, rem_qty_total - available_l1) if rem_qty_total > 0 else 0
        
        if rem_qty_total > 0:
            active_sells = []
            if available_l1 > 0 and trigger_l1 > 0:
                active_sells.append(trigger_l1)
            if available_upper > 0 and trigger_upper > 0:
                active_sells.append(trigger_upper)
                
            if active_sells:
                min_sell = min(active_sells)
                if p1_trigger >= min_sell:
                    p1_trigger = max(0.01, round(min_sell - 0.01, 2))
                if p2_trigger >= min_sell:
                    p2_trigger = max(0.01, round(min_sell - 0.01, 2))

        orders = []

        # 🚨 MODIFIED: [V75.11 스냅샷 매수 예산 기아(Data Starvation) 원천 차단]
        # 스냅샷 모드일 때는 total_spent를 0.0으로 팩트 무시하고 100% 매수 덫을 렌더링(장전)합니다.
        total_spent = 0.0 if is_snapshot_mode else float(self.executed["BUY_BUDGET"].get(ticker, 0.0))
        
        seed_val = float(self.cfg.get_seed(ticker) or 0.0)
        daily_limit = seed_val * 0.15
        
        safe_alloc_cash = min(float(alloc_cash), daily_limit) if daily_limit > 0 else float(alloc_cash)
        rem_budget = max(0.0, safe_alloc_cash - total_spent)
        
        if rem_budget > 0:
            b1_budget = rem_budget * 0.5
            b2_budget = rem_budget * 0.5
            
            q1 = math.floor(b1_budget / p1_trigger) if p1_trigger > 0 else 0
            q2 = math.floor(b2_budget / p2_trigger) if p2_trigger > 0 else 0
            
            # 🚨 MODIFIED: [V75.08 소형 시드 데이터 기아(Data Starvation) 맹점 영구 소각 및 영끌(Sweep) 타격 복원]
            if q1 == 0 and q2 == 0:
                if p1_trigger > 0 and rem_budget >= p1_trigger:
                    q1 = math.floor(rem_budget / p1_trigger)
                elif p2_trigger > 0 and rem_budget >= p2_trigger:
                    q2 = math.floor(rem_budget / p2_trigger)
            elif q1 == 0 and q2 > 0:
                q2 = math.floor(rem_budget / p2_trigger) if p2_trigger > 0 else 0
            elif q2 == 0 and q1 > 0:
                q1 = math.floor(rem_budget / p1_trigger) if p1_trigger > 0 else 0
            
            # 🚨 MODIFIED: [V75.04 KIS VWAP 3-Min 지터 동적 시프트(Shift) 및 KST 래핑 락온]
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
            
            if q1 > 0:
                ord_type = "VWAP" if q1 >= 10 else "LOC"
                desc_str = "VWAP매수(Buy1)" if ord_type == "VWAP" else "LOC매수(Buy1)"
                orders.append({"side": "BUY", "qty": q1, "price": p1_trigger, "type": ord_type, "start_time": start_t if ord_type == "VWAP" else None, "end_time": end_t if ord_type == "VWAP" else None, "desc": desc_str})
            if q2 > 0:
                ord_type = "VWAP" if q2 >= 10 else "LOC"
                desc_str = "VWAP매수(Buy2)" if ord_type == "VWAP" else "LOC매수(Buy2)"
                orders.append({"side": "BUY", "qty": q2, "price": p2_trigger, "type": ord_type, "start_time": start_t if ord_type == "VWAP" else None, "end_time": end_t if ord_type == "VWAP" else None, "desc": desc_str})
        
        if rem_qty_total > 0:
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
            
            sell_dict = {}
            if available_l1 > 0 and trigger_l1 > 0:
                sell_dict[trigger_l1] = sell_dict.get(trigger_l1, 0) + available_l1
            if available_upper > 0 and trigger_upper > 0:
                sell_dict[trigger_upper] = sell_dict.get(trigger_upper, 0) + available_upper
                
            for price in sorted(sell_dict.keys()):
                s_qty = sell_dict[price]
                ord_type = "VWAP" if s_qty >= 10 else "LOC"
                
                if price == trigger_l1 and price == trigger_upper:
                    desc_str = "통합탈출"
                elif price == trigger_l1:
                    desc_str = "1층탈출"
                elif price == trigger_upper:
                    desc_str = "총평단탈출"
                else:
                    desc_str = "잔여탈출"
                    
                orders.append({
                    "side": "SELL", "qty": s_qty, "price": price, "type": ord_type, 
                    "start_time": start_t if ord_type == "VWAP" else None, 
                    "end_time": end_t if ord_type == "VWAP" else None, 
                    "desc": desc_str
                })
        
        plan_result = {
            "orders": orders, 
            "trigger_loc": False, 
            "total_q": total_q,
            "is_zero_start": is_zero_start_session
        }
        
        if is_zero_start_session and market_type != "AFTER":
            plan_result["orders"] = [o for o in plan_result.get("orders", []) if o.get("side") != "SELL"]
        
        if is_snapshot_mode:
            self.save_daily_snapshot(ticker, plan_result)

        self._save_state(ticker)
        return plan_result
