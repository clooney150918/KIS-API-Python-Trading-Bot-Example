# ==========================================================
# FILE: strategy_reversion.py
# ==========================================================
# 🚨 MODIFIED: [V-REV 추세장 LOC 스위칭 침묵 버그 및 상태 증발 완벽 수술]
# MODIFIED: [V44.27 0주 스냅샷 환각 락온] 
# MODIFIED: [V44.25 예산 탈취(Stealing) 런타임 붕괴 방어막 이식] 
# MODIFIED: [V44.25 AVWAP 디커플링] 
# MODIFIED: [V44.36 큐 장부 vs 브로커 실잔고 불일치 팩트 스캔] 
# MODIFIED: [V44.48 런타임 붕괴 방어] 
# NEW: [VWAP 잔차 증발 방어 롤백 엔진] 
# NEW: [V46.01 팩트 교정] 소형 시드 1주 타격 영구 동결(Data Starvation) 방어
# 🚨 MODIFIED: [V46.02 엣지 케이스 핫픽스: 잔차 파탄 완벽 해체] 
# 🚨 MODIFIED: [V48.00 단일 바구니(Single Bucket) 롤백] 
# 🚨 MODIFIED: [V50.02 30분 압축 락온] 
# 🚨 MODIFIED: [V50.03 분할 교착 및 예산 강제 축소 버그 완벽 수술] 
# 🚨 MODIFIED: [V51.00 몰빵 로직 전면 철거] 
# 🚨 MODIFIED: [V51.01 소형 시드 1주 영끌 타격 락온] 
# 🚨 MODIFIED: [V53.00 무한 재진입 락온] 
# 🚨 NEW: [KIS VWAP 알고리즘 대통합 수술] 
# 🚨 MODIFIED: [V71.05 KIS VWAP 30분 압축 타격 타임라인 락온]
# 🚨 MODIFIED: [V71.13 런타임 붕괴 방어 및 타임라인 전진 배치 수술]
# 🚨 NEW: [V71.14 예약 덫 무조건 장전 헌법 복구 및 족쇄 철거]
# 🚨 MODIFIED: [V71.25 KST 타임라인 동적 래핑 수술]
# 🚨 MODIFIED: [V71.27 런타임 붕괴 수술]
# 🚨 MODIFIED: [V72.00 줍줍 전면 소각 및 VWAP 10주 제약 LOC 우회 락온]
# 🚨 MODIFIED: [V72.01 V-REV 1회 예산(15%) 하드 마진 캡(Cap) 락온]
# 🚨 MODIFIED: [V72.02 제20경고 준수: V-REV 매수 앵커 디커플링 및 하극상 역전 방어 락온]
# 🚨 MODIFIED: [V72.11 V-REV 지층 융합 맹점 영구 소각 및 100% 독립 LIFO 덫 장전 락온]
# 🚨 MODIFIED: [V72.13 V-REV 1층 독립 및 상위층 총평단가 연동 엑시트 전술 이식]
# - 상위층(Upper) 매도 덫 타점(`trigger_upper`) 연산 시, 낡은 상위층 단독 평단가(`upper_avg`) 의존성을 영구 소각.
# - 큐 장부 SSOT 기반 총 평단가(`avg_price`)를 절대 앵커로 삼아 `avg_price * 1.010` (+1% 수익) 팩트 교정 완료.
# - 1층은 기존대로 1층 고유 타점(`l1_price * 1.006`)을 유지하여 가벼운 현금흐름 창출 궤도 100% 락온.
# - UI 렌더링 텍스트를 '1층탈출', '총평단탈출', '통합탈출'로 직관적 팩트 렌더링 미러링 완료.
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
        # 상위층의 엑시트 앵커를 낡은 upper_avg에서 큐 장부의 진성 총 평단가(avg_price)로 교체하고 +1% 잭팟 타점 부여.
        trigger_l1 = round(l1_price * 1.006, 2)
        trigger_upper = round(avg_price * 1.010, 2) if upper_qty > 0 else 0.0

        cached_plan = self.load_daily_snapshot(ticker)
        
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
            safe_anchor = l1_price if l1_price > 0.0 else prev_c
            p1_trigger = round(safe_anchor * 0.995, 2)
            p2_trigger = round(safe_anchor * 0.9725, 2)

        orders = []

        total_spent = float(self.executed["BUY_BUDGET"].get(ticker, 0.0))
        
        seed_val = float(self.cfg.get_seed(ticker) or 0.0)
        daily_limit = seed_val * 0.15
        
        safe_alloc_cash = min(float(alloc_cash), daily_limit) if daily_limit > 0 else float(alloc_cash)
        rem_budget = max(0.0, safe_alloc_cash - total_spent)
        
        if rem_budget > 0:
            b1_budget = rem_budget * 0.5
            b2_budget = rem_budget * 0.5
            
            q1 = math.floor(b1_budget / p1_trigger) if p1_trigger > 0 else 0
            q2 = math.floor(b2_budget / p2_trigger) if p2_trigger > 0 else 0
            
            est_tz_check = ZoneInfo('America/New_York')
            is_dst_active = bool(datetime.now(est_tz_check).dst())
            start_t = "042500" if is_dst_active else "052500"
            end_t = "045500" if is_dst_active else "055500"
            
            if q1 > 0:
                ord_type = "VWAP" if q1 >= 10 else "LOC"
                desc_str = "VWAP매수(Buy1)" if ord_type == "VWAP" else "LOC매수(Buy1)"
                orders.append({"side": "BUY", "qty": q1, "price": p1_trigger, "type": ord_type, "start_time": start_t if ord_type == "VWAP" else None, "end_time": end_t if ord_type == "VWAP" else None, "desc": desc_str})
            if q2 > 0:
                ord_type = "VWAP" if q2 >= 10 else "LOC"
                desc_str = "VWAP매수(Buy2)" if ord_type == "VWAP" else "LOC매수(Buy2)"
                orders.append({"side": "BUY", "qty": q2, "price": p2_trigger, "type": ord_type, "start_time": start_t if ord_type == "VWAP" else None, "end_time": end_t if ord_type == "VWAP" else None, "desc": desc_str})
            
        rem_qty_total = max(0, int(total_q) - int(self.executed["SELL_QTY"].get(ticker, 0)))
        
        if rem_qty_total > 0:
            est_tz_check = ZoneInfo('America/New_York')
            is_dst_active = bool(datetime.now(est_tz_check).dst())
            start_t = "042500" if is_dst_active else "052500"
            end_t = "045500" if is_dst_active else "055500"
            
            available_l1 = min(l1_qty, rem_qty_total)
            available_upper = min(upper_qty, rem_qty_total - available_l1)
            
            sell_dict = {}
            if available_l1 > 0 and trigger_l1 > 0:
                sell_dict[trigger_l1] = sell_dict.get(trigger_l1, 0) + available_l1
            if available_upper > 0 and trigger_upper > 0:
                sell_dict[trigger_upper] = sell_dict.get(trigger_upper, 0) + available_upper
                
            for price in sorted(sell_dict.keys()):
                s_qty = sell_dict[price]
                ord_type = "VWAP" if s_qty >= 10 else "LOC"
                
                # 🚨 MODIFIED: [V72.13 UI 미러링 팩트 교정]
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
