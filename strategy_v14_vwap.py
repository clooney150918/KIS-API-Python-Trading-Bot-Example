# ==========================================================
# FILE: strategy_v14_vwap.py
# ==========================================================
# MODIFIED: [V44.27 0주 스냅샷 환각 락온] 서버 재시작으로 인메모리 스냅샷이 소실되었을 때, 메인 장부에서 당일 날짜(EST)의 거래를 100% 도려내고 오직 어제까지 이월된 순수 과거 물량만을 스캔하여 '0주 새출발' 상태를 완벽히 팩트 복구하는 타임머신 역산 엔진 이식 완료.
# MODIFIED: [V44.27 AVWAP 잔고 오염 방어] V14_VWAP 런타임 엔진에 KIS 총잔고 대신 암살자 물량이 배제된 pure_qty를 주입하여 동적 플랜 훼손 원천 차단
# MODIFIED: [V44.25 AVWAP 디커플링] VWAP 기상 전 스냅샷 2중 교차 검증(Fail-Safe) 및 암살자 물량(AVWAP) 100% 격리(Decoupling) 파이프라인 이식 완료.
# NEW: [VWAP 잔차 증발 방어 롤백 엔진] 주문 거절/미체결 시 삭감된 예산을 버킷 식별자 기반으로 원상 복구(Refund)하는 환불 파이프라인 개통 완료.
# 🚨 MODIFIED: [V50.02 30분 압축 락온] 타임 윈도우 스캔 범위를 range(27, 60)에서 range(27, 57)로 정밀 교정하여 15:56 타격 종료 완벽 동기화.
# 🚨 MODIFIED: [V51.01 소형 시드 1주 영끌 타격 락온] 예산이 1주 가격보다 작더라도 장막판 가불을 통해 무조건 1주 베이스캠프 확보 보장.
# 🚨 MODIFIED: [치명적 경고 3] 텔레그램 지시서 조회(min_idx < 0) 시 스냅샷 반환 디커플링 및 실시간 타격 분리 락온.
# 🚨 MODIFIED: [치명적 경고 19] UnboundLocalError 원천 봉쇄를 위한 변수 스코프 전진 배치 수술 완료.
# 🚨 NEW: [KIS VWAP 알고리즘 대통합 수술] 1분 단위 타임 슬라이싱 연산 및 잔차 버킷 파편화 궤적을 100% 영구 소각하고, 할당된 1회분 총 예산을 단일 KIS VWAP 덫 예약 주문 플랜으로 통짜 스냅샷 산출하여 반환하도록 아키텍처 대수술 완료.
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
        # MODIFIED: [잔차 버킷 파편화 궤적 영구 소각] 1분 단위 슬라이싱용 잔차 버킷 철거 완료
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
            "residual": {}, # MODIFIED: 잔차 버킷 소각에 따른 빈 딕셔너리 락온
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
        # MODIFIED: 잔차 버킷 소각에 따라 환불 로직 바이패스 락온
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
        # MODIFIED: 잔차 버킷 소각에 따라 리셋 로직 바이패스 락온
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
        
        # MODIFIED: [KIS VWAP 알고리즘 대통합 수술] 1분 단위 타임 슬라이싱 연산을 소각하고 단일 KIS VWAP 덫 예약 주문(type: "VWAP") 플랜으로 통짜 반환
        if qty == 0:
            is_zero_start_fact = True
            p_buy = self._ceil(prev_close * 1.15)
            buy_star_price = p_buy 
            
            q_buy = math.floor(dynamic_budget / p_buy) if p_buy > 0 else 0
            if q_buy > 0: core_orders.append({"side": "BUY", "price": p_buy, "qty": q_buy, "type": "VWAP", "desc": "🆕새출발(VWAP)"})
            process_status = "✨새출발"
        else:
            p_avg = self._ceil(avg_price)
            if t_val < (split / 2):
                q_avg = math.floor((dynamic_budget * 0.5) / p_avg) if p_avg > 0 else 0
                q_star = math.floor((dynamic_budget * 0.5) / buy_star_price) if buy_star_price > 0 else 0
                if q_avg > 0: core_orders.append({"side": "BUY", "price": p_avg, "qty": q_avg, "type": "VWAP", "desc": "⚓평단매수(VWAP)"})
                if q_star > 0: core_orders.append({"side": "BUY", "price": buy_star_price, "qty": q_star, "type": "VWAP", "desc": "💫별값매수(VWAP)"})
            else:
                q_star = math.floor(dynamic_budget / buy_star_price) if buy_star_price > 0 else 0
                if q_star > 0: core_orders.append({"side": "BUY", "price": buy_star_price, "qty": q_star, "type": "VWAP", "desc": "💫별값매수(VWAP)"})
            
            q_sell = math.ceil(qty / 4)
            if q_sell > 0:
                core_orders.append({"side": "SELL", "price": star_price, "qty": q_sell, "type": "VWAP", "desc": "🌟별값매도(VWAP)"})
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
        
        # NEW: [치명적 경고 19] UnboundLocalError 원천 봉쇄를 위한 변수 스코프 전진 배치
        alloc_qty = 0
        spent_b = 0.0
        exact_s_qty = 0.0
        alloc_s_qty = 0
        b_budget_slice = 0.0
        slice_budget = 0.0
        b_bucket = 0.0
        
        cached_plan = self.load_daily_snapshot(ticker)
        
        # MODIFIED: [치명적 경고 3] 텔레그램 지시서 조회(min_idx < 0) 시 스냅샷 반환 디커플링 및 실시간 타격 분리 락온
        # MODIFIED: [KIS VWAP 알고리즘 대통합 수술] 1분 단위 타임 슬라이싱 연산을 소각하고 단일 KIS VWAP 덫 예약 주문 플랜으로 통짜 반환
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
