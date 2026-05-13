# ==========================================================
# FILE: strategy_v_avwap.py
# ==========================================================
# 🚨 MODIFIED: [V59.00 AVWAP 암살자 예산 100% 수혈 및 15:25 전량 덤핑 팩트 교정]
# 🚨 MODIFIED: [V59.02 잔재 데드코드 영구 소각]
# 🚨 MODIFIED: [V59.04 프리마켓 락다운 쉴드 이식]
# 🚨 MODIFIED: [V60.00 옴니 매트릭스 진입 차단망 전면 폐기 및 데드코드 소각]
# 🚨 MODIFIED: [V61.00 숏(SOXS) 전면 소각 작전 지시서 적용]
# 🚨 NEW: [상대적 체력 연산 30.0% 셧다운 락온]
# 🚨 NEW: [V65.00 AVWAP 동적 하드스탑 락온]
# 🚨 NEW: [V66.00 AVWAP 암살자 덤핑 지터(Jitter) 분산 락온]
# 🚨 MODIFIED: [V66.05 Split-Brain 시각적 디커플링 해결]
# 🚨 NEW: [V71.01 시계열 체력 예외 허용 엔진(V-Turn Intercept) 이식]
# 🚨 MODIFIED: [V71.08 AVWAP 암살자 덤핑 타임라인 전진 배치 팩트 교정]
# 🚨 NEW: [V72.09 3-Stage Apex Intercept (정점 요격) 전술 탑재]
# 🚨 MODIFIED: [V72.10 3단계 격발 차원 붕괴(버그) 완벽 수술] 
# 🚨 NEW: [V72.16 AVWAP 정점요격 스위치 탑재 및 IndentationError 팩트 수술]
# - get_decision 내부 3단계 정점요격 연산 블록 최상단에 바이패스 쉴드 이식 완료.
# - 스위치 OFF 시 정점 갱신 및 투매 판별 연산을 전면 무시하고 상태를 초기화.
# - 런타임 붕괴를 유발하던 들여쓰기(Indentation) 오차 3곳 100% 팩트 교정 완료.
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
        self.plugin_name = "AVWAP_HYBRID_DUAL"
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

                    data['HA_LATCHED_BULL'] = False
                    data['dump_jitter_sec'] = random.randint(0, 180)
                    
                    data['APEX_STAGE_1'] = False
                    data['APEX_STAGE_2'] = False
                    data['APEX_PEAK_PRICE'] = 0.0

                    data['date'] = today_str
                    self.save_state(ticker, now_est, data)
                     
                data['APEX_STAGE_1'] = data.get('APEX_STAGE_1', False)
                data['APEX_STAGE_2'] = data.get('APEX_STAGE_2', False)
                data['APEX_PEAK_PRICE'] = data.get('APEX_PEAK_PRICE', 0.0)

                return data
            except Exception:
                pass

        return {
            "executed_buy": False, "shutdown": False, "strikes": 0, "qty": 0, 
            "avg_price": 0.0, "daily_bought_qty": 0, "daily_sold_qty": 0, 
            "HA_LATCHED_BULL": False, "dump_jitter_sec": random.randint(0, 180),
            "APEX_STAGE_1": False, "APEX_STAGE_2": False, "APEX_PEAK_PRICE": 0.0
        }

    def save_state(self, ticker, now_est, state_data):
        file_path = self._get_state_file(ticker, now_est)
        state_data['date'] = self._get_logical_date_str(now_est)

        try:
            dir_name = os.path.dirname(file_path)
            if dir_name and not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)

            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, file_path)
        except Exception as e:
            logging.error(f"🚨 [V_AVWAP] 상태 저장 실패: {e}")

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

    def get_decision(self, base_ticker=None, exec_ticker=None, base_curr_p=0.0, exec_curr_p=0.0, base_day_open=0.0, avwap_avg_price=0.0, avwap_qty=0, avwap_alloc_cash=0.0, context_data=None, df_1min_base=None, now_est=None, avwap_state=None, is_apex_on=True, **kwargs):
        df_1min_base = df_1min_base if df_1min_base is not None else kwargs.get('base_df')
        avwap_qty = avwap_qty if avwap_qty != 0 else kwargs.get('current_qty', 0)

        base_curr_p = base_curr_p if base_curr_p > 0 else kwargs.get('base_curr_p', 0.0)
        exec_curr_p = exec_curr_p if exec_curr_p > 0 else kwargs.get('exec_curr_p', 0.0)
        base_day_open = base_day_open if base_day_open > 0 else kwargs.get('base_day_open', 0.0)
        avwap_avg_price = avwap_avg_price if avwap_avg_price > 0 else kwargs.get('avwap_avg_price', kwargs.get('avg_price', 0.0))
        avwap_alloc_cash = avwap_alloc_cash if avwap_alloc_cash > 0 else kwargs.get('alloc_cash', kwargs.get('avwap_alloc_cash', 0.0))

        atr5 = kwargs.get('atr5', 0.0)
        day_high = kwargs.get('day_high', 0.0)
        day_low = kwargs.get('day_low', 0.0)
        prev_c = kwargs.get('prev_close', 0.0)

        if now_est is None:
            now_est = datetime.datetime.now(ZoneInfo('America/New_York'))

        if base_curr_p <= 0.0 and df_1min_base is not None and not df_1min_base.empty:
            try: base_curr_p = float(df_1min_base['close'].iloc[-1])
            except Exception: pass

        avwap_state = avwap_state or {}
        curr_time = now_est.time()

        time_0410 = datetime.time(4, 10)
        time_0930 = datetime.time(9, 30)
        
        dump_jitter_sec = avwap_state.get('dump_jitter_sec', 0)
        base_dump_dt = datetime.datetime.combine(now_est.date(), datetime.time(15, 20)).replace(tzinfo=ZoneInfo('America/New_York'))
        dynamic_dump_dt = base_dump_dt - datetime.timedelta(seconds=dump_jitter_sec)
        time_dynamic_dump = dynamic_dump_dt.time()

        is_regular_session = curr_time >= time_0930

        base_vwap = base_curr_p
        vwap_success = False 

        ha_2_bullish_no_lower = False
        trend_sequence = "PENDING"
        
        is_pure_5m_2_bearish = False
        current_5m_is_bearish = False

        if df_1min_base is not None and not df_1min_base.empty:
            try:
                df = df_1min_base.copy()

                if 'time_est' in df.columns:
                    if is_regular_session:
                        df = df[(df['time_est'] >= '093000') & (df['time_est'] <= '155900')]
                    else:
                        df = df[(df['time_est'] >= '040000') & (df['time_est'] <= '092959')]

                if not df.empty:
                    df['tp'] = (df['high'].astype(float) + df['low'].astype(float) + df['close'].astype(float)) / 3.0
                    df['vol'] = df['volume'].astype(float)
                    df['vol_tp'] = df['tp'] * df['vol']

                    cum_vol = df['vol'].sum()
                    if cum_vol > 0:
                        base_vwap = df['vol_tp'].sum() / cum_vol
                        vwap_success = True

                    t_high_idx = df['high'].astype(float).idxmax()
                    t_low_idx = df['low'].astype(float).idxmin()
                    if t_high_idx < t_low_idx:
                        trend_sequence = "BEAR"
                    elif t_low_idx < t_high_idx:
                        trend_sequence = "BULL"

                    if is_regular_session and curr_time < datetime.time(9, 35):
                        ha_2_bullish_no_lower = False
                    else:
                        df['datetime'] = pd.to_datetime(df.index)
                        df.set_index('datetime', inplace=True)
                        df_5m = df.resample('5min', label='left', closed='left').agg({
                            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
                        }).dropna()

                        if not df_5m.empty:
                            df_5m['is_pure_bearish'] = df_5m['close'].astype(float) < df_5m['open'].astype(float)
                            if len(df_5m) >= 3:
                                last_2_pure = df_5m.iloc[-3:-1]
                                is_pure_5m_2_bearish = bool(last_2_pure['is_pure_bearish'].all())
                            
                            current_5m_open = float(df_5m['open'].iloc[-1])
                            current_5m_is_bearish = (base_curr_p < current_5m_open)

                            df_5m['HA_Close'] = (df_5m['open'].astype(float) + df_5m['high'].astype(float) + df_5m['low'].astype(float) + df_5m['close'].astype(float)) / 4.0
                            ha_open = []
                            for i in range(len(df_5m)):
                                if i == 0:
                                    ha_open.append((float(df_5m['open'].iloc[i]) + float(df_5m['close'].iloc[i])) / 2.0)
                                else:
                                    ha_open.append((ha_open[i-1] + float(df_5m['HA_Close'].iloc[i-1])) / 2.0)

                            df_5m['HA_Open'] = pd.Series(ha_open, index=df_5m.index)
                            df_5m['HA_High'] = df_5m[['high', 'HA_Open', 'HA_Close']].max(axis=1)
                            df_5m['HA_Low'] = df_5m[['low', 'HA_Open', 'HA_Close']].min(axis=1)

                            df_5m['No_Lower_Wick'] = (df_5m['HA_Open'] - df_5m['HA_Low']) <= 0.01
                            df_5m['Is_Bullish'] = df_5m['HA_Close'] >= df_5m['HA_Open']

                            if len(df_5m) >= 2:
                                last_2 = df_5m.tail(2)
                                ha_2_bullish_no_lower = last_2['Is_Bullish'].all() and last_2['No_Lower_Wick'].all()

            except Exception as e:
                logging.error(f"🚨 [V_AVWAP] 기초자산 HA 및 5분봉 연산 실패: {e}")

        def _build_res(action, reason, qty=0, target_price=0.0):
            return {
                'action': action,
                'reason': reason,
                'qty': qty,
                'target_price': target_price,
                'vwap': base_vwap,
                'base_curr_p': base_curr_p,
                'prev_vwap': context_data.get('prev_vwap', 0.0) if context_data else 0.0
            }

        if not vwap_success and avwap_qty == 0:
            return _build_res('WAIT', 'VWAP_데이터_결측_동결')

        safe_qty = int(math.floor(float(avwap_qty)))

        # ---------------------------------------------------------
        # [3-Stage Apex Intercept (정점 요격) 전술 상태 업데이트]
        # ---------------------------------------------------------
        persistent_state = self.load_state(exec_ticker, now_est)
        apex_stage_1 = persistent_state.get('APEX_STAGE_1', False)
        apex_stage_2 = persistent_state.get('APEX_STAGE_2', False)
        apex_peak_price = persistent_state.get('APEX_PEAK_PRICE', 0.0)
        apex_changed = False

        actual_gap_dollar_apex = day_high - day_low
        actual_gap_pct_apex = (actual_gap_dollar_apex / prev_c) * 100.0 if prev_c > 0 else 0.0

        if is_apex_on: # 🚨 NEW: 정점요격 스위치 바이패스 쉴드 이식
            if day_high > apex_peak_price:
                apex_peak_price = day_high
                apex_changed = True
                if apex_stage_1 and apex_stage_2:
                    apex_stage_2 = False
                    
            if atr5 > 0 and actual_gap_pct_apex >= atr5:
                if not apex_stage_1:
                    apex_stage_1 = True
                    apex_peak_price = day_high
                    apex_changed = True

            if apex_stage_1 and not apex_stage_2:
                if is_pure_5m_2_bearish:
                    apex_stage_2 = True
                    apex_changed = True
        else:
            # 스위치 OFF 시 상태 초기화 및 바이패스
            if apex_stage_1 or apex_stage_2 or apex_peak_price > 0.0:
                apex_stage_1 = False
                apex_stage_2 = False
                apex_peak_price = 0.0
                apex_changed = True

        if apex_changed:
            persistent_state['APEX_STAGE_1'] = apex_stage_1
            persistent_state['APEX_STAGE_2'] = apex_stage_2
            persistent_state['APEX_PEAK_PRICE'] = apex_peak_price
            self.save_state(exec_ticker, now_est, persistent_state)

        # ---------------------------------------------------------
        # 1. 매도 (보유 중일 때) 로직 - 동적 지터(15:17~15:20) 무조건 덤핑 락온
        # ---------------------------------------------------------
        if safe_qty > 0:
            safe_avg = avwap_avg_price if avwap_avg_price > 0 else exec_curr_p

            if safe_avg <= 0:
                return _build_res('SELL', 'CORRUPT_PRICE_EMERGENCY_DUMP', qty=safe_qty, target_price=exec_curr_p)

            # [3-Stage Apex Intercept (정점 요격) 3단계 격발]
            if is_apex_on and apex_stage_2 and current_5m_is_bearish: # 🚨 NEW: 정점요격 스위치 결속 락온
                persistent_state['shutdown'] = True
                self.save_state(exec_ticker, now_est, persistent_state)
                return _build_res('SELL', '🎯 정점 요격(Apex Intercept) 팩트 타격 완료', qty=safe_qty, target_price=exec_curr_p)

            if curr_time >= time_dynamic_dump:
                persistent_state["shutdown"] = True
                self.save_state(exec_ticker, now_est, persistent_state)
                reason_str = f'{time_dynamic_dump.strftime("%H:%M:%S")}_도달_당일교전종료_무조건덤핑'
                return _build_res('SELL', reason_str, qty=safe_qty, target_price=exec_curr_p)

            if atr5 > 0 and exec_curr_p > 0 and safe_avg > 0:
                loss_pct = ((safe_avg - exec_curr_p) / safe_avg) * 100.0
                if loss_pct >= atr5:
                    persistent_state["shutdown"] = True
                    self.save_state(exec_ticker, now_est, persistent_state)
                    return _build_res('SELL', f'ATR5_동적_하드스탑_피격(-{loss_pct:.2f}%)_당일영구동결', qty=safe_qty, target_price=exec_curr_p)

            return _build_res('HOLD', '보유중_관망(동적_지터_덤핑_대기)')

        # ---------------------------------------------------------
        # 2. 매수 (포지션 0주 일 때) 로직 - 배타적 갭 필터 및 모멘텀 스캔
        # ---------------------------------------------------------
        if not context_data:
            return _build_res('WAIT', '매크로_데이터_수집대기')

        if avwap_state.get('shutdown', False) or persistent_state.get('shutdown', False):
             return _build_res('WAIT', '당일영구동결_상태(신규진입금지)')

        if curr_time < time_0410:
            return _build_res('WAIT', '04:10_이전_오프닝_휩소_방어(10분_안전마진_대기)')

        if not is_regular_session:
            return _build_res('WAIT', '프리마켓_노이즈_원천차단_정규장_개장_대기')

        if curr_time >= time_dynamic_dump:
            persistent_state["shutdown"] = True
            self.save_state(exec_ticker, now_est, persistent_state)
            reason_str = f'{time_dynamic_dump.strftime("%H:%M:%S")}_도달_신규진입_영구동결'
            return _build_res('SHUTDOWN', reason_str)

        base_prev_c = float(context_data.get('prev_close', 0.0))
        prev_vwap = float(context_data.get('prev_vwap', 0.0))

        if prev_c <= 0 or atr5 <= 0 or day_high <= 0 or day_low <= 0 or exec_curr_p <= 0 or base_vwap <= 0 or prev_vwap <= 0:
            return _build_res('WAIT', '진입_평가용_필수데이터_결측_대기')
            
        actual_gap_dollar = day_high - day_low
        actual_gap_pct = (actual_gap_dollar / prev_c) * 100.0 if prev_c > 0 else 0.0
        
        rem_relative_pct = ((atr5 - actual_gap_pct) / atr5 * 100.0) if atr5 > 0 else 0.0

        if rem_relative_pct < 30.0:
            persistent_state["shutdown"] = True
            self.save_state(exec_ticker, now_est, persistent_state)
            return _build_res('SHUTDOWN', 'ATR5_상대체력_30%미만_고갈_당일신규진입_영구동결')

        base_day_high = float(kwargs.get('base_day_high', 0.0))
        base_day_low = float(kwargs.get('base_day_low', 0.0))
        
        is_neg_gap_state = False
        if base_day_high > 0 and base_day_low > 0 and base_prev_c > 0:
            is_neg_gap_state = (base_day_high < base_prev_c) and (base_day_low < base_prev_c)

        cond1_met = not is_neg_gap_state

        ha_latched_bull = persistent_state.get('HA_LATCHED_BULL', False)
        latch_changed = False

        if ha_2_bullish_no_lower:
            if not ha_latched_bull:
                ha_latched_bull = True
                latch_changed = True
                
        if trend_sequence == "BEAR" or rem_relative_pct < 30.0:
            if ha_latched_bull:
                ha_latched_bull = False
                latch_changed = True

        if latch_changed:
            persistent_state['HA_LATCHED_BULL'] = ha_latched_bull
            self.save_state(exec_ticker, now_est, persistent_state)

        cond2_met = (base_curr_p > base_vwap) and ha_latched_bull
        cond3_met = True

        cond_seq = True
        if trend_sequence == "BEAR":
            cond_seq = False
            mid_point = 0.0
            if day_high > 0 and day_low > 0:
                mid_point = (day_high + day_low) / 2.0
            if atr5 > 0 and actual_gap_pct >= (atr5 * 0.5) and exec_curr_p >= mid_point:
                cond_seq = True

        if cond1_met and cond2_met and cond3_met and cond_seq:
            if avwap_alloc_cash > 0:
                safe_budget = avwap_alloc_cash * 0.95
                buy_qty = int(math.floor(safe_budget / exec_curr_p))
                if buy_qty > 0:
                    return _build_res('BUY', 'V47_하이킨아시_배타적갭필터_통과_타격개시', qty=buy_qty, target_price=exec_curr_p)
            return _build_res('WAIT', '가용예산부족_대기')
        else:
            fail_reasons = []
            if not cond1_met: fail_reasons.append("원웨이/배타적갭필터미달")
            if not cond2_met: fail_reasons.append("HA모멘텀미달")
            if not cond3_met: fail_reasons.append("체력미달")
            if not cond_seq: fail_reasons.append("시계열체력하락세")
            return _build_res('WAIT', f'진입조건대기({",".join(fail_reasons)})')
