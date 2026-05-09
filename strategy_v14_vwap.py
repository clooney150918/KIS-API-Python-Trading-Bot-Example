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
        self.residual = {"BUY_AVG": {}, "BUY_STAR": {}, "SELL_STAR": {}, "SELL_TARGET": {}}
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
                    for k in self.residual.keys():
                        self.residual[k][ticker] = float(data.get("residual", {}).get(k, 0.0))
                    for k in self.executed.keys():
                        raw_val = data.get("executed", {}).get(k, 0)
                        self.executed[k][ticker] = int(raw_val) if k == "SELL_QTY" else float(raw_val)
                    self.state_loaded[ticker] = today_str
                    return
            except Exception:
                 pass
                
        for k in self.residual.keys():
            self.residual[k][ticker] = 0.0
        self.executed["BUY_BUDGET"][ticker] = 0.0
        self.executed["SELL_QTY"][ticker] = 0
        self.state_loaded[ticker] = today_str

    def _save_state(self, ticker):
        today_str = self._get_logical_date_str()
        state_file = self._get_state_file(ticker)
        data = {
            "date": today_str,
            "residual": {k: float(self.residual[k].get(ticker, 0.0)) for k in self.residual.keys()},
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
        self._load_state_if_needed(ticker)
        if bucket in self.residual:
            self.residual[bucket][ticker] = float(self.residual[bucket].get(ticker, 0.0)) + float(refund_value)
            self._save_state(ticker)

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
        self._load_state_if_needed(ticker)
        for k in self.residual: self.residual[k][ticker] = 0.0
        self.executed["BUY_BUDGET"][ticker] = 0.0
        self.executed["SELL_QTY"][ticker] = 0
        self._save_state(ticker)

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
        
        if qty == 0:
            is_zero_start_fact = True
            p_buy = self._ceil(prev_close * 1.15)
            buy_star_price = p_buy 
            
            q_buy = math.floor(dynamic_budget / p_buy) if p_buy > 0 else 0
            if q_buy > 0: core_orders.append({"side": "BUY", "price": p_buy, "qty": q_buy, "type": "LOC", "desc": "🆕새출발(VWAP대기)"})
            process_status = "✨새출발"
        else:
            p_avg = self._ceil(avg_price)
            if t_val < (split / 2):
                q_avg = math.floor((dynamic_budget * 0.5) / p_avg) if p_avg > 0 else 0
                q_star = math.floor((dynamic_budget * 0.5) / buy_star_price) if buy_star_price > 0 else 0
                if q_avg > 0: core_orders.append({"side": "BUY", "price": p_avg, "qty": q_avg, "type": "LOC", "desc": "⚓평단매수(V)"})
                if q_star > 0: core_orders.append({"side": "BUY", "price": buy_star_price, "qty": q_star, "type": "LOC", "desc": "💫별값매수(V)"})
            else:
                q_star = math.floor(dynamic_budget / buy_star_price) if buy_star_price > 0 else 0
                if q_star > 0: core_orders.append({"side": "BUY", "price": buy_star_price, "qty": q_star, "type": "LOC", "desc": "💫별값매수(V)"})
            
            q_sell = math.ceil(qty / 4)
            if q_sell > 0:
                core_orders.append({"side": "SELL", "price": star_price, "qty": q_sell, "type": "LOC", "desc": "🌟별값매도(V)"})
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
        
        plan_static = self.get_plan(
            ticker=ticker,
            current_price=current_price,
            avg_price=avg_price,
            qty=qty,
            prev_close=prev_close,
            available_cash=alloc_cash,
            is_simulation=True,
            is_snapshot_mode=False,
            market_type=market_type
        )
        star_price = float(plan_static['star_price'])
        buy_star_price = float(plan_static.get('buy_star_price', round(star_price - 0.01, 2) if star_price > 0.01 else 0.0))
        target_price = float(plan_static['target_price'])
        total_budget = float(plan_static['one_portion'])
        
        initial_qty = int(plan_static.get('initial_qty', qty))
        
        cached_plan = self.load_daily_snapshot(ticker)
        
        # MODIFIED: [치명적 경고 3] 텔레그램 지시서 조회(min_idx < 0) 시 스냅샷 반환 디커플링 및 실시간 타격 분리 락온
        if min_idx < 0 and cached_plan:
            return cached_plan
            
        if cached_plan:
            is_zero_start_session = cached_plan.get('is_zero_start', initial_qty == 0)
        else:
            today_str_est = self._get_logical_date_str()
            try:
                recs = [r for r in self.cfg.get_ledger() if r['ticker'] == ticker and not str(r.get("date", "")).startswith(today_str_est)]
                ledger_qty, _, _, _ = self.cfg.calculate_holdings(ticker, recs)
                is_zero_start_session = (ledger_qty == 0)
            except Exception:
                is_zero_start_session = (qty == 0)
        
        try:
            # MODIFIED: [치명적 경고 9] vwap_data.py 의존성 완전 소각 및 config.py 통합 프로파일 다이렉트 수혈 락온
            profile = self.cfg.get_vwap_profile(ticker) if hasattr(self.cfg, 'get_vwap_profile') else {}
        except Exception as e:
            logging.error(f"🚨 [{ticker}] VWAP 프로파일 로드 실패: {e}")
            profile = {}
            
        target_keys = [f"15:{str(m).zfill(2)}" for m in range(27, 57)]
        total_target_vol = sum(profile.get(k, 0.0) for k in target_keys)
        
        now_est = datetime.now(ZoneInfo('America/New_York'))
        time_str = now_est.strftime('%H:%M')
        
        rem_weight = 0.0
        if time_str in target_keys:
            start_idx = target_keys.index(time_str)
            for k in target_keys[start_idx:]:
                rem_weight += profile.get(k, 0.0)
                
            raw_weight = profile.get(time_str, 0.0)
            slice_ratio = (raw_weight / rem_weight) if rem_weight > 0 else 1.0
            
            current_weight = (raw_weight / total_target_vol) if total_target_vol > 0 else (1.0 / len(target_keys))
        else:
            slice_ratio = 0.0
            current_weight = 0.0
        
        orders = []
        
        total_spent = float(self.executed["BUY_BUDGET"].get(ticker, 0.0))
        rem_budget_global = max(0.0, total_budget - total_spent)
        
        if rem_budget_global > 0 and current_weight > 0:
            slice_budget = total_budget * current_weight
            b_bucket = float(self.residual["BUY_STAR"].get(ticker, 0.0)) + slice_budget
            b_budget_slice = min(b_bucket, rem_budget_global)

            if current_price > 0:
                # 🚨 MODIFIED: [V51.01 소형 시드 1주 영끌 타격 락온]
                if min_idx >= 28 and total_spent == 0.0 and b_budget_slice < current_price:
                    b_budget_slice = current_price

                if buy_star_price > 0 and (is_zero_start_session or current_price <= buy_star_price):
                    alloc_qty = int(math.floor(b_budget_slice / current_price))
                    if alloc_qty > 0:
                         spent_b = alloc_qty * current_price
                         self.residual["BUY_STAR"][ticker] = max(0.0, b_bucket - spent_b)
                         orders.append({"side": "BUY", "qty": alloc_qty, "price": buy_star_price if not is_zero_start_session else current_price, "desc": "VWAP분할매수", "bucket": "BUY_STAR"})
                    else:
                         self.residual["BUY_STAR"][ticker] = b_bucket
                else:
                    self.residual["BUY_STAR"][ticker] = b_bucket

        rem_sell_qty = int(math.ceil(initial_qty / 4)) - int(self.executed["SELL_QTY"].get(ticker, 0))
        if rem_sell_qty > 0 and star_price > 0 and slice_ratio > 0:
            if current_price >= star_price:
                 exact_s_qty = float(rem_sell_qty * slice_ratio) + float(self.residual["SELL_STAR"].get(ticker, 0.0))
                 alloc_s_qty = int(min(math.floor(exact_s_qty), rem_sell_qty))
                 self.residual["SELL_STAR"][ticker] = float(exact_s_qty - alloc_s_qty)
                 if alloc_s_qty > 0:
                     orders.append({"side": "SELL", "qty": alloc_s_qty, "price": star_price, "desc": "VWAP분할익절", "bucket": "SELL_STAR"})
            else:
                 self.residual["SELL_STAR"][ticker] = float(self.residual["SELL_STAR"].get(ticker, 0.0)) + float(rem_sell_qty * slice_ratio)

        if is_zero_start_session and market_type != "AFTER":
            orders = [o for o in orders if o.get("side") != "SELL"]

        self._save_state(ticker)
        return {"orders": orders, "trigger_loc": False}
