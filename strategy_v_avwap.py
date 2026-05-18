# ==========================================================
# FILE: strategy_v_avwap.py
# ==========================================================
# 🚨 MODIFIED: [V59.00 AVWAP 암살자 예산 100% 수혈 및 15:25 전량 덤핑 팩트 교정]
# 🚨 MODIFIED: [V60.00 옴니 매트릭스 진입 차단망 전면 폐기 및 데드코드 소각]
# 🚨 MODIFIED: [V61.00 숏(SOXS) 전면 소각 작전 지시서 적용]
# 🚨 NEW: [V65.00 AVWAP 동적 하드스탑 락온]
# 🚨 NEW: [V66.00 AVWAP 암살자 덤핑 지터(Jitter) 분산 락온]
# 🚨 NEW: [V75.04 상태 캐시 기억상실(Amnesia) 완벽 수술]
# 🚨 MODIFIED: [V76.01 ATR5 동적 하드스탑 영구 소각 및 투트랙 엑시트 절대 락온]
# 🚨 MODIFIED: [V76.02 타점 역전 패러독스 하드 마진 락온 (매니저 제안 수혈)]
# 🚨 MODIFIED: [V76.03 암살자 덤핑 지터(Jitter) 코어 연산 디커플링 해체 및 동적 타임라인 락온]
# 🚨 NEW: [V77.00 V7.1 백테스트 절대 동기화 롤백 (Animal Spirit 야성 회복)]
# 🚨 MODIFIED: [V77.01 데이터 기아 방어 및 런타임 무결성 팩트 수술] 
# - 이벤트 루프 교착의 원흉이었던 동기 함수 _get_exec_1m_data() 100% 영구 소각
# - get_decision 시그니처에 df_1min_exec 주입 및 time_est 기반 데이터 슬라이싱 락온
# - 캔들 파서의 대문자 변수(High, Low, Open)를 소문자로 전면 교정하여 KeyError 런타임 붕괴 원천 차단
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import math
import random
import yfinance as yf
import pandas as pd
import json
import os
import tempfile

class VAvwapHybridPlugin:
    def __init__(self):
        # NEW: [V77.00 플러그인 닉네임 교체 - 야성 회복]
        self.plugin_name = "AVWAP_V7.1_ANIMAL_SPIRIT"
        self.leverage = 3.0       

    def _get_logical_date_str(self, now_est):
        if now_est.hour < 4 or (now_est.hour == 4 and now_est.minute < 4):
            target_date = now_est - datetime.timedelta(days=1)
        else:
            target_date = now_est
        return target_date.strftime('%Y%m%d')

    def _get_state_file(self, ticker, now_est):
        return f"data/avwap_state_persistent_{ticker}.json"

    def load_state(self, ticker, now_est):
        file_path = self._get_state_file(ticker, now_est)
        today_str = self._get_logical_date_str(now_est)

        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                if data.get('date') != today_str:
                    qty = data.get('qty', 0)
                    if qty > 0:
                        data['bought'] = True
                        data['shutdown'] = False
                    else:
                        data['qty'] = 0
                        data['avg_price'] = 0.0
                        data['shutdown'] = False
                        data['strikes'] = 0
                        data['bought'] = False
                        data['daily_bought_qty'] = 0
                        data['daily_sold_qty'] = 0

                    # NEW: [V77.00 상태 변수 초기화] V7.1 팩트 인젝션
                    data['PM_H'] = 0.0
                    data['PM_L'] = 0.0
                    data['T_H'] = 0.0
                    data['T_L'] = 0.0
                    data['offset'] = 0.0
                    data['dump_jitter_sec'] = random.randint(0, 180)

                    data['date'] = today_str
                    self.save_state(ticker, now_est, data)
                
                # 안전 형변환 보장
                data['PM_H'] = float(data.get('PM_H', 0.0))
                data['PM_L'] = float(data.get('PM_L', 0.0))
                data['T_H'] = float(data.get('T_H', 0.0))
                data['T_L'] = float(data.get('T_L', 0.0))
                data['offset'] = float(data.get('offset', 0.0))

                return data
            except Exception:
                pass

        # NEW: [V77.00 초기 상태값 구성] 과잉 방어 플래그 소각
        return {
            "executed_buy": False, "shutdown": False, "strikes": 0, "qty": 0, 
            "avg_price": 0.0, "daily_bought_qty": 0, "daily_sold_qty": 0, 
            "dump_jitter_sec": random.randint(0, 180),
            "PM_H": 0.0, "PM_L": 0.0, "T_H": 0.0, "T_L": 0.0, "offset": 0.0
        }

    def save_state(self, ticker, now_est, state_data):
        file_path = self._get_state_file(ticker, now_est)
        today_str = self._get_logical_date_str(now_est)

        merged_data = {}
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    merged_data = json.load(f)
            except Exception:
                pass

        if merged_data.get('date') != today_str:
            merged_data = {}

        merged_data.update(state_data)
        merged_data['date'] = today_str

        try:
            dir_name = os.path.dirname(file_path)
            if dir_name and not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)

            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(merged_data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, file_path)
        except Exception as e:
            logging.error(f"🚨 [V_AVWAP] 상태 저장 실패 (원자적 쓰기 에러): {e}")

    def fetch_macro_context(self, base_ticker):
        try:
            tkr = yf.Ticker(base_ticker)
            df_1m = tkr.history(period="5d", interval="1m", prepost=False, timeout=5)

            prev_vwap = 0.0
            prev_close = 0.0

            est = ZoneInfo('America/New_York')
            now_est = datetime.datetime.now(est)

            if now_est.hour < 4 or (now_est.hour == 4 and now_est.minute < 5):
                today_est = (now_est - datetime.timedelta(days=1)).date()
            else:
                today_est = now_est.date()

            if not df_1m.empty:
                if df_1m.index.tz is None:
                    df_1m.index = df_1m.index.tz_localize('UTC').tz_convert(est)
                else:
                    df_1m.index = df_1m.index.tz_convert(est)

                df_past_1m = df_1m[df_1m.index.date < today_est].copy()

                if not df_past_1m.empty:
                    last_date = df_past_1m.index.date[-1]
                    df_prev_day = df_past_1m[df_past_1m.index.date == last_date].copy()
                    df_prev_day = df_prev_day.between_time('09:30', '15:59')

                    if not df_prev_day.empty:
                        prev_close = float(df_prev_day['Close'].iloc[-1])
                        df_prev_day['tp'] = (df_prev_day['High'].astype(float) + df_prev_day['Low'].astype(float) + df_prev_day['Close'].astype(float)) / 3.0
                        df_prev_day['vol'] = df_prev_day['Volume'].astype(float)
                        df_prev_day['vol_tp'] = df_prev_day['tp'] * df_prev_day['vol']

                        cum_vol = df_prev_day['vol'].sum()
                        if cum_vol > 0:
                            prev_vwap = df_prev_day['vol_tp'].sum() / cum_vol
                        else:
                            prev_vwap = prev_close

            df_30m = tkr.history(period="60d", interval="30m", timeout=5)
            avg_vol_20 = 0.0

            if not df_30m.empty:
                if df_30m.index.tz is None:
                    df_30m.index = df_30m.index.tz_localize('UTC').tz_convert(est)
                else:
                    df_30m.index = df_30m.index.tz_convert(est)

                first_30m = df_30m[df_30m.index.time == datetime.time(9, 30)]
                past_first_30m = first_30m[first_30m.index.date < today_est]

                if len(past_first_30m) >= 20:
                    avg_vol_20 = float(past_first_30m['Volume'].tail(20).mean())
                elif len(past_first_30m) > 0:
                    avg_vol_20 = float(past_first_30m['Volume'].mean())

            if prev_vwap == 0.0:
                prev_vwap = prev_close

            return {
                "prev_close": prev_close,
                "prev_vwap": prev_vwap,
                "avg_vol_20": avg_vol_20
            }

        except Exception as e:
            logging.error(f"🚨 [V_AVWAP] YF 기초자산 매크로 컨텍스트 추출 실패 ({base_ticker}): {e}")
            return None

    # MODIFIED: [V77.01 데이터 기아 방어 및 런타임 무결성 팩트 수술] df_1min_exec 수혈 락온
    def get_decision(self, base_ticker=None, exec_ticker=None, base_curr_p=0.0, exec_curr_p=0.0, base_day_open=0.0, avwap_avg_price=0.0, avwap_qty=0, avwap_alloc_cash=0.0, context_data=None, df_1min_base=None, df_1min_exec=None, now_est=None, avwap_state=None, regime_data=None, is_simulation=False, **kwargs):
        # NEW: [V77.00 스코프 상단 선언] 
        avwap_qty = avwap_qty if avwap_qty != 0 else kwargs.get('current_qty', 0)
        exec_curr_p = exec_curr_p if exec_curr_p > 0 else kwargs.get('exec_curr_p', 0.0)
        avwap_avg_price = avwap_avg_price if avwap_avg_price > 0 else kwargs.get('avwap_avg_price', kwargs.get('avg_price', 0.0))
        avwap_alloc_cash = avwap_alloc_cash if avwap_alloc_cash > 0 else kwargs.get('alloc_cash', kwargs.get('avwap_alloc_cash', 0.0))
        amp5 = float(kwargs.get('amp5', 0.0))
        prev_c = float(kwargs.get('prev_close', 0.0))
        
        now_est = now_est or datetime.datetime.now(ZoneInfo('America/New_York'))
        curr_time = now_est.time()
        
        time_0925 = datetime.time(9, 25)
        time_0930 = datetime.time(9, 30)

        persistent_state = self.load_state(exec_ticker, now_est)
        is_shutdown = persistent_state.get('shutdown', False)
        
        dump_jitter_sec = persistent_state.get('dump_jitter_sec', 0)
        base_dump_dt = datetime.datetime.combine(now_est.date(), datetime.time(15, 20)).replace(tzinfo=ZoneInfo('America/New_York'))
        dynamic_dump_dt = base_dump_dt - datetime.timedelta(seconds=dump_jitter_sec)
        time_dynamic_dump = dynamic_dump_dt.time()
        
        pm_h = persistent_state.get('PM_H', 0.0)
        pm_l = persistent_state.get('PM_L', 0.0)
        t_h = persistent_state.get('T_H', 0.0)
        t_l = persistent_state.get('T_L', 0.0)
        offset = persistent_state.get('offset', 0.0)

        def _build_res(action, reason, qty=0, target_price=0.0):
            return {
                'action': action,
                'reason': reason,
                'qty': qty,
                'target_price': target_price,
                'vwap': 0.0,
                'base_curr_p': base_curr_p,
                'prev_vwap': context_data.get('prev_vwap', 0.0) if context_data else 0.0
            }

        # ---------------------------------------------------------
        # 1. 매도 (보유 중일 때) 로직 - V7.1 백테스트 투트랙 자동 청산
        # ---------------------------------------------------------
        if avwap_qty > 0:
            safe_avg = avwap_avg_price if avwap_avg_price > 0 else exec_curr_p

            if safe_avg <= 0:
                return _build_res('SELL', 'CORRUPT_PRICE_EMERGENCY_DUMP', qty=avwap_qty, target_price=exec_curr_p)

            if curr_time >= time_dynamic_dump:
                persistent_state["shutdown"] = True
                if not is_simulation:
                    self.save_state(exec_ticker, now_est, persistent_state)
                return _build_res('SELL', '동적_덤핑_타임라인_도달_전량_시장가_덤핑', qty=avwap_qty, target_price=exec_curr_p)

            exit_target_price = round(safe_avg * 1.02, 2)
            if exec_curr_p >= exit_target_price:
                return _build_res('SELL', '목표가(+2.0%)_도달_순수모멘텀_익절_격발', qty=avwap_qty, target_price=exit_target_price)

            return _build_res('HOLD', '보유중_순수익절(+2.0%)_및_동적덤핑_감시중')

        # ---------------------------------------------------------
        # 2. 매수 (포지션 0주 일 때) 로직 - V7.1 암살자 스캔 및 격발
        # ---------------------------------------------------------
        if is_shutdown:
            return _build_res('WAIT', '당일영구동결_상태(신규진입금지)')

        if curr_time >= time_dynamic_dump:
            persistent_state["shutdown"] = True
            if not is_simulation:
                self.save_state(exec_ticker, now_est, persistent_state)
            return _build_res('SHUTDOWN', '동적_덤핑_타임라인_도달_신규진입_영구동결')

        if prev_c <= 0 or amp5 <= 0:
            return _build_res('WAIT', '진입_평가용_필수데이터_결측_대기')

        # NEW: [V77.01 데이터 기아 방어 및 런타임 무결성 팩트 수술] df_1min_exec 릴레이 배선 및 time_est 슬라이싱 적용
        if curr_time >= time_0925 and pm_h == 0.0:
            df_1m = df_1min_exec
            if df_1m is not None and not df_1m.empty and 'time_est' in df_1m.columns:
                df_pre = df_1m[(df_1m['time_est'] >= '040000') & (df_1m['time_est'] <= '092959')]
                if not df_pre.empty:
                    pm_h = float(df_pre['high'].max())
                    pm_l = float(df_pre['low'].min())
                    
                    offset = prev_c * amp5 * 0.40
                    t_h = pm_h - offset
                    t_l = pm_l + offset
                    
                    if t_l >= t_h:
                        t_l = max(0.01, t_h - 0.01)

                    persistent_state['PM_H'] = pm_h
                    persistent_state['PM_L'] = pm_l
                    persistent_state['T_H'] = t_h
                    persistent_state['T_L'] = t_l
                    persistent_state['offset'] = offset

                    if not is_simulation:
                        self.save_state(exec_ticker, now_est, persistent_state)
                    logging.info(f"🎯 [V7.1 백테스트 락온] {exec_ticker} PM_H: {pm_h:.2f}, PM_L: {pm_l:.2f}, 순수 진폭 Offset: {offset:.2f} | T_H: {t_h:.2f}, T_L: {t_l:.2f}")
                else:
                    return _build_res('WAIT', '프리마켓_데이터_결측_대기중')

        if pm_h == 0.0 or t_h == 0.0:
            return _build_res('WAIT', '프리마켓_타겟_연산_대기중')

        if curr_time < time_0930:
            return _build_res('WAIT', '정규장_개장_대기중')

        # NEW: [V77.01 데이터 기아 방어 및 런타임 무결성 팩트 수술] time_est 슬라이싱 적용
        df_1m = df_1min_exec
        if df_1m is None or df_1m.empty or 'time_est' not in df_1m.columns:
            return _build_res('WAIT', '정규장_실시간_1분봉_결측')

        df_reg = df_1m[(df_1m['time_est'] >= '093000') & (df_1m['time_est'] <= '152000')]
        if df_reg.empty:
            return _build_res('WAIT', '정규장_캔들_형성대기')

        # 🚨 MODIFIED: [V77.01 데이터 기아 방어 및 런타임 무결성 팩트 수술] 소문자 컬럼 매핑으로 KeyError 소각
        curr_candle = df_reg.iloc[-1]
        curr_h = float(curr_candle['high'])
        curr_l = float(curr_candle['low'])
        curr_o = float(curr_candle['open'])
        
        hit_h = curr_h >= t_h
        hit_l = curr_l <= t_l
        
        if hit_h and hit_l:
            if abs(curr_o - t_h) < abs(curr_o - t_l):
                hit_l = False
            else:
                hit_h = False
                
        if hit_l:
            persistent_state["shutdown"] = True
            if not is_simulation:
                self.save_state(exec_ticker, now_est, persistent_state)
            logging.info(f"🛑 [V7.1 하락 락온] 1분봉 저가({curr_l:.2f})가 T_L({t_l:.2f}) 하향 돌파. 당일 매매 셧다운!")
            return _build_res('SHUTDOWN', '일반하락장_T_L하향돌파_매매종료')

        if hit_h:
            safe_budget = avwap_alloc_cash * 0.95
            buy_qty = int(math.floor(safe_budget / exec_curr_p)) if exec_curr_p > 0 else 0
            if buy_qty > 0:
                logging.info(f"🚀 [V7.1 상승 격발] 1분봉 고가({curr_h:.2f})가 T_H({t_h:.2f}) 상향 돌파. 야성 매수 진입!")
                return _build_res('BUY', '일반상승장_T_H상향돌파_순수모멘텀_격발', qty=buy_qty, target_price=exec_curr_p)

        return _build_res('WAIT', '순수_타격선_도달_감시중')
