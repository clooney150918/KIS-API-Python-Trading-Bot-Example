# ==========================================================
# FILE: strategy_v_avwap.py
# ==========================================================
# [strategy_v_avwap.py] - 🌟 V47.00 앱솔루트 팩트 교정 🌟
# 💡 V-REV 하이브리드 전용 차세대 AVWAP 스나이퍼 플러그인 (Dual-Referencing)
# 🚨 MODIFIED: [V47.00 하이킨아시 듀얼 모멘텀 추세 시스템 락온]
# - 04:00 EST 프리마켓 1분봉 파서 스캔 확장 및 데이터 기아 해체 (open 컬럼)
# - 하이킨아시 5min 리샘플링 기반 3대 진입 조건(원웨이, 모멘텀, 체력) 락온
# - 15:00 EST 오버나이트 존버(Hold) 모드 이식 및 투트랙 엑시트 전면 개조
# - 10:00 EST 단판 승부 및 조기퇴근(단일 출장) 셧다운 로직 영구 소각 (무한 스캔 개방)
# 🚨 MODIFIED: [1일 한정 실전 테스트] 제1조건(고저가 원웨이) 강제 바이패스 락온
# 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
# 제1헌법: 동기 I/O 100% 비동기 격리.
# 제3헌법: 타임존 단일 소스 락온 (EST 100%).
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import math
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

                        data['date'] = today_str
                        self.save_state(ticker, now_est, data)

                    return data
            except Exception:
                pass
        return {"executed_buy": False, "shutdown": False, "strikes": 0, "qty": 0, "avg_price": 0.0, "daily_bought_qty": 0, "daily_sold_qty": 0}

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

    def get_decision(self, base_ticker=None, exec_ticker=None, base_curr_p=0.0, exec_curr_p=0.0, base_day_open=0.0, avwap_avg_price=0.0, avwap_qty=0, avwap_alloc_cash=0.0, context_data=None, df_1min_base=None, now_est=None, avwap_state=None, **kwargs):

        df_1min_base = df_1min_base if df_1min_base is not None else kwargs.get('base_df')
        avwap_qty = avwap_qty if avwap_qty != 0 else kwargs.get('current_qty', 0)

        base_curr_p = base_curr_p if base_curr_p > 0 else kwargs.get('base_curr_p', 0.0)
        exec_curr_p = exec_curr_p if exec_curr_p > 0 else kwargs.get('exec_curr_p', 0.0)
        base_day_open = base_day_open if base_day_open > 0 else kwargs.get('base_day_open', 0.0)
        avwap_avg_price = avwap_avg_price if avwap_avg_price > 0 else kwargs.get('avwap_avg_price', kwargs.get('avg_price', 0.0))
        avwap_alloc_cash = avwap_alloc_cash if avwap_alloc_cash > 0 else kwargs.get('alloc_cash', kwargs.get('avwap_alloc_cash', 0.0))

        user_target_pct = kwargs.get('target_profit', 4.0)
        target_mode = kwargs.get('target_mode', 'AUTO')

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

        time_0400 = datetime.time(4, 0)
        time_1500 = datetime.time(15, 0)

        base_vwap = base_curr_p
        vwap_success = False 

        base_reg_high = base_curr_p
        base_reg_low = base_curr_p

        is_inverse = exec_ticker.upper() in ["SOXS", "SQQQ", "SPXU"]

        ha_2_bullish_no_lower = False
        ha_2_bearish_no_upper = False

        if df_1min_base is not None and not df_1min_base.empty:
            try:
                df = df_1min_base.copy()

                # 🚨 [V47.00] 04:00~15:59 EST 구간 강제 확장 및 프리마켓 데이터 기아 방어
                if 'time_est' in df.columns:
                    df = df[(df['time_est'] >= '040000') & (df['time_est'] <= '155900')]

                if not df.empty:
                    df['tp'] = (df['high'].astype(float) + df['low'].astype(float) + df['close'].astype(float)) / 3.0
                    df['vol'] = df['volume'].astype(float)
                    df['vol_tp'] = df['tp'] * df['vol']

                    cum_vol = df['vol'].sum()
                    if cum_vol > 0:
                        base_vwap = df['vol_tp'].sum() / cum_vol
                        vwap_success = True

                    # 하이킨아시 5min 리샘플링 구현 (오차 허용 0.01$ 락온)
                    df['datetime'] = pd.to_datetime(df.index)
                    df.set_index('datetime', inplace=True)
                    df_5m = df.resample('5min', label='left', closed='left').agg({
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last',
                        'volume': 'sum'
                    }).dropna()

                    if not df_5m.empty:
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

                        # 0.01$ 갭 필터링
                        df_5m['No_Lower_Wick'] = (df_5m['HA_Open'] - df_5m['HA_Low']) <= 0.01
                        df_5m['No_Upper_Wick'] = (df_5m['HA_High'] - df_5m['HA_Open']) <= 0.01
                        df_5m['Is_Bullish'] = df_5m['HA_Close'] >= df_5m['HA_Open']
                        df_5m['Is_Bearish'] = df_5m['HA_Close'] < df_5m['HA_Open']

                        if len(df_5m) >= 2:
                            last_2 = df_5m.tail(2)
                            ha_2_bullish_no_lower = last_2['Is_Bullish'].all() and last_2['No_Lower_Wick'].all()
                            ha_2_bearish_no_upper = last_2['Is_Bearish'].all() and last_2['No_Upper_Wick'].all()

            except Exception as e:
                logging.error(f"🚨 [V_AVWAP] 기초자산 HA 연산 실패: {e}")

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
        # 1. 매도 (보유 중일 때) 로직 - 투트랙 엑시트 & 15:00 존버
        # ---------------------------------------------------------
        if safe_qty > 0:
            safe_avg = avwap_avg_price if avwap_avg_price > 0 else exec_curr_p

            if safe_avg <= 0:
                return _build_res('SELL', 'CORRUPT_PRICE_EMERGENCY_DUMP', qty=safe_qty, target_price=exec_curr_p)

            # 🚨 [V47.00] 15:00 EST 타임스탑 존버 모드 투트랙
            if curr_time >= time_1500:
                if exec_curr_p > safe_avg:
                    return _build_res('SELL', '15:00_도달_수익중_전량팩트덤핑', qty=safe_qty, target_price=exec_curr_p)
                else:
                    avwap_state["shutdown"] = True
                    self.save_state(exec_ticker, now_est, avwap_state)
                    return _build_res('HOLD', '15:00_도달_손실중_오버나이트_존버(절대손절금지)')

            exec_return = (exec_curr_p - safe_avg) / safe_avg

            # AUTO 모드 하이킨아시 2연속 역추세 청산
            if target_mode == "AUTO":
                if not is_inverse and ha_2_bearish_no_upper:
                    return _build_res('SELL', 'AUTO_하이킨아시_역추세(음봉2연속)_즉각덤핑', qty=safe_qty, target_price=exec_curr_p)
                elif is_inverse and ha_2_bullish_no_lower:
                    return _build_res('SELL', 'AUTO_하이킨아시_역추세(양봉2연속)_즉각덤핑', qty=safe_qty, target_price=exec_curr_p)
            else:
                # MANUAL 모드 사용자 설정 목표 청산
                if exec_return >= (user_target_pct / 100.0):
                    return _build_res('SELL', f'MANUAL_목표달성(+{user_target_pct:.1f}%)_지정가익절', qty=safe_qty, target_price=exec_curr_p)

            return _build_res('HOLD', '보유중_관망')

        # ---------------------------------------------------------
        # 2. 매수 (포지션 0주 일 때) 로직 - 하이킨아시 3대 조건 팩트 스캔
        # ---------------------------------------------------------
        if not context_data:
            return _build_res('WAIT', '매크로_데이터_수집대기')

        if avwap_state.get('shutdown', False):
            return _build_res('WAIT', '당일영구동결_상태(신규진입금지)')

        # 🚨 [V45.00 동적 킬 스위치] 정규장(09:30 EST~) 횡보장 스캔 락온
        if df_1min_base is not None and not df_1min_base.empty:
            df_reg = df_1min_base[df_1min_base['time_est'] >= '093000']
            if not df_reg.empty:
                base_reg_high = float(df_reg['high'].max())
                base_reg_low = float(df_reg['low'].min())
                base_prev_c_for_kill = float(context_data.get('prev_close', 0.0))
                if base_prev_c_for_kill > 0 and base_reg_high > base_prev_c_for_kill and base_reg_low < base_prev_c_for_kill:
                    avwap_state["shutdown"] = True
                    self.save_state(exec_ticker, now_est, avwap_state)
                    return _build_res('SHUTDOWN', '정규장_횡보장_감지(Zero-Line_관통)_신규진입_영구동결')

        if curr_time < time_0400:
            return _build_res('WAIT', '04:00_이전_프리마켓_대기')

        if curr_time >= time_1500:
            avwap_state["shutdown"] = True
            self.save_state(exec_ticker, now_est, avwap_state)
            return _build_res('SHUTDOWN', '15:00_도달_신규진입_영구동결')

        # 필수 데이터 결측 검증
        base_prev_c = float(context_data.get('prev_close', 0.0))
        prev_vwap = float(context_data.get('prev_vwap', 0.0))
        base_day_high = kwargs.get('base_day_high', 0.0)
        base_day_low = kwargs.get('base_day_low', 0.0)

        if prev_c <= 0 or atr5 <= 0 or day_high <= 0 or day_low <= 0 or exec_curr_p <= 0 or base_vwap <= 0 or prev_vwap <= 0:
            return _build_res('WAIT', '진입_평가용_필수데이터_결측_대기')

        # 1. 고저가 부호 일치 (원웨이 방향 판별)
        cond1_met = False
        if base_day_high > 0 and base_day_low > 0 and base_prev_c > 0:
            if not is_inverse:
                cond1_met = (base_day_high > base_prev_c) and (base_day_low > base_prev_c)
            else:
                cond1_met = (base_day_high < base_prev_c) and (base_day_low < base_prev_c)

        # 🚨 MODIFIED: [1일 한정 실전 테스트] 제1조건(고저가 원웨이) 강제 바이패스 락온
        cond1_met = True 

        # 2. 하이킨아시 모멘텀
        cond2_met = False
        if not is_inverse:
            cond2_met = (base_vwap > prev_vwap) and ha_2_bullish_no_lower
        else:
            cond2_met = (base_vwap < prev_vwap) and ha_2_bearish_no_upper

        # 3. 잔여 체력 1% 이상
        cond3_met = False
        actual_gap_dollar = day_high - day_low
        actual_gap_pct = (actual_gap_dollar / prev_c) * 100.0
        rem_5_pct = atr5 - actual_gap_pct
        cond3_met = (rem_5_pct >= 1.0)

        if cond1_met and cond2_met and cond3_met:
            if avwap_alloc_cash > 0:
                # 🚨 [V47.00] 암살자 현금 50% 락온 상태에서 거절 방어용 95% 마진 체결
                safe_budget = avwap_alloc_cash * 0.95
                buy_qty = int(math.floor(safe_budget / exec_curr_p))
                if buy_qty > 0:
                    return _build_res('BUY', '하이킨아시_조건충족_1일테스트_타격개시', qty=buy_qty, target_price=exec_curr_p)
            return _build_res('WAIT', '가용예산부족_대기')
        else:
            fail_reasons = []
            if not cond1_met: fail_reasons.append("원웨이미달")
            if not cond2_met: fail_reasons.append("HA모멘텀미달")
            if not cond3_met: fail_reasons.append(f"체력({rem_5_pct:.1f}%)미달")
            return _build_res('WAIT', f'진입조건대기({",".join(fail_reasons)})')

