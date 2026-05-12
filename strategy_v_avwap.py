# ==========================================================
# FILE: strategy_v_avwap.py
# ==========================================================
# 🚨 MODIFIED: [V59.00 AVWAP 암살자 예산 100% 수혈 및 15:25 전량 덤핑 팩트 교정]
# 🚨 [치명적 맹점 수술]: 장중 HA 2연속 음봉 및 체력고갈에 의한 조기 익절(Premature Exit) 로직 전면 영구 소각.
# 암살자는 오직 진입(BUY) 조건만 판별하며, 진입 후에는 15:25 EST까지 100% 무조건 홀딩(HOLD) 후 
# 수익/손실 불문 전량 덤핑(SELL)하여 본진 예산을 복구하도록 아키텍처 대수술 완료.
# 🚨 MODIFIED: [V59.02 잔재 데드코드 영구 소각] SELL 사유 텍스트 내부에 남아있는 레거시 키워드 '(조기퇴근)' 100% 영구 소각 완료.
# 🚨 MODIFIED: [V59.04 프리마켓 락다운 쉴드 이식] 09:30 이전 매수 타격 원천 차단으로 제13헌법 6조 완벽 준수.
# 🚨 MODIFIED: [V60.00 옴니 매트릭스 진입 차단망 전면 폐기 및 데드코드 소각]
# 1) 상위 라우터(strategy.py)에서 중앙 통제가 이루어지므로, 플러그인 내부에 잘못 복사된 
#    self.apply_omni_matrix_filter 호출 및 미정의 qty 참조 블록을 100% 영구 소각 (제2헌법 준수).
# 2) 횡보장 락다운(allow_buy=False)에 의한 타격 중단을 원천 차단하고 오직 팩트 데이터로만 진입 판별.
# 🚨 MODIFIED: [V61.00 숏(SOXS) 전면 소각 작전 지시서 적용]
# 1) is_inverse 인버스 판별 변수 및 SOXS 티커 조건 분기 100% 전면 철거.
# 2) 하이킨아시 음봉 판별 팩트(ha_latched_bear 등) 상태 메모리 및 연산 영구 소각.
# 3) 롱(SOXL) 진입 전용 단일 팩트(cond1, cond2, cond_seq)로 아키텍처 진공 압축 완료.
# 🚨 NEW: [상대적 체력 연산 30.0% 셧다운 락온]
# 기존 절대 진폭(1.0%) 차감 방식을 전면 소각하고, ATR5 대비 잔여 체력 비율(%)을 연산하여 30.0% 미만 시 신규 진입을 영구 동결하는 기관급 하드 마진 방어막 탑재.
# 🚨 NEW: [V65.00 AVWAP 동적 하드스탑 락온]
# 진입 평단가 대비 현재가의 손실률(%)이 당일 ATR5(%) 수치 이상으로 하락하는 찰나,
# 즉시 셧다운(shutdown=True) 플래그를 활성화하고 전량 덤핑(SELL)을 격발하는 절대 방어막 탑재.
# 🚨 NEW: [V66.00 AVWAP 암살자 덤핑 지터(Jitter) 분산 락온]
# 서버 병목 방어를 위해 15:25 하드코딩 덤핑을 소각하고 0~180초 난수를 차감한 동적 타임스탬프로 분산 타격 이식 완료.
# 🚨 MODIFIED: [V66.05 Split-Brain 시각적 디커플링 해결]
# 관제탑 환각 방지를 위해 HA_LATCHED_BULL 상태 변경 시 즉각 파일(JSON)에 원자적(Atomic)으로 각인(save_state)하는 무결성 검증 완료. 제4헌법 완벽 준수.
# 🚨 NEW: [V71.01 시계열 체력 예외 허용 엔진(V-Turn Intercept) 이식]
# 하락 추세(Time_High < Time_Low) 판독 시에도, 진폭이 ATR5의 50% 이상 도달 및 현재가가 당일 미드포인트 이상 회복 시 V자 반등으로 판별하여 예외적으로 롱(SOXL) 진입을 허용하는 디커플링 아키텍처 대수술 완료.
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
                    # 🚨 MODIFIED: [V61.00 숏(SOXS) 전면 소각] HA_LATCHED_BEAR 영구 적출 완료

                    # NEW: [V66.00 AVWAP 덤핑 지터 분산 타격 락온] 매일 0~180초 사이의 지터 난수 발급 및 영속화
                    data['dump_jitter_sec'] = random.randint(0, 180)

                    data['date'] = today_str
                    self.save_state(ticker, now_est, data)

                return data
            except Exception:
                pass
        
        # NEW: [V66.00 AVWAP 덤핑 지터 분산 타격 락온] 초기 생성 시 지터 난수 주입
        return {"executed_buy": False, "shutdown": False, "strikes": 0, "qty": 0, "avg_price": 0.0, "daily_bought_qty": 0, "daily_sold_qty": 0, "HA_LATCHED_BULL": False, "dump_jitter_sec": random.randint(0, 180)}

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
        # MODIFIED: [V60.00] 옴니 매트릭스 중복 필터 블록 전면 소각
        # 제2헌법 및 제13헌법 5조에 의거, 하단에 존재하던 self.apply_omni_matrix_filter 호출 및 
        # allow_buy 분기 블록을 흔적도 없이 적출함. 암살자는 이제 오직 팩트 지표로만 판단함.

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

        # 🚨 [Time-Split Radar] 듀얼 세션 스위치 및 타임 쉴드
        time_0410 = datetime.time(4, 10)
        time_0930 = datetime.time(9, 30)
        
        # NEW: [V66.00 AVWAP 덤핑 지터 분산 타격 락온] 동적 타임스탬프 연산
        dump_jitter_sec = avwap_state.get('dump_jitter_sec', 0)
        base_dump_dt = datetime.datetime.combine(now_est.date(), datetime.time(15, 25)).replace(tzinfo=ZoneInfo('America/New_York'))
        dynamic_dump_dt = base_dump_dt - datetime.timedelta(seconds=dump_jitter_sec)
        time_dynamic_dump = dynamic_dump_dt.time()

        is_regular_session = curr_time >= time_0930

        base_vwap = base_curr_p
        vwap_success = False 

        # 🚨 MODIFIED: [V61.00 숏(SOXS) 전면 소각] is_inverse 변수 영구 철거
        ha_2_bullish_no_lower = False
        trend_sequence = "PENDING"

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
        # 1. 매도 (보유 중일 때) 로직 - 동적 지터(15:22~15:25) 무조건 덤핑 락온
        # ---------------------------------------------------------
        if safe_qty > 0:
            safe_avg = avwap_avg_price if avwap_avg_price > 0 else exec_curr_p

            if safe_avg <= 0:
                # MODIFIED: [V59.02 잔재 데드코드 영구 소각] 레거시 키워드 (조기퇴근) 100% 삭제
                return _build_res('SELL', 'CORRUPT_PRICE_EMERGENCY_DUMP', qty=safe_qty, target_price=exec_curr_p)

            # 🚨 MODIFIED: [V66.00 AVWAP 동적 지터 덤핑 락온] 동적 타임스탬프 도달 시 무조건 전량 팩트 덤핑
            if curr_time >= time_dynamic_dump:
                avwap_state["shutdown"] = True
                self.save_state(exec_ticker, now_est, avwap_state)
                reason_str = f'{time_dynamic_dump.strftime("%H:%M:%S")}_도달_당일교전종료_무조건덤핑'
                return _build_res('SELL', reason_str, qty=safe_qty, target_price=exec_curr_p)

            # NEW: [V65.00 AVWAP 동적 하드스탑 락온] ATR5 기반 실시간 손절 방어막 전진 배치
            if atr5 > 0 and exec_curr_p > 0 and safe_avg > 0:
                loss_pct = ((safe_avg - exec_curr_p) / safe_avg) * 100.0
                if loss_pct >= atr5:
                    avwap_state["shutdown"] = True
                    self.save_state(exec_ticker, now_est, avwap_state)
                    return _build_res('SELL', f'ATR5_동적_하드스탑_피격(-{loss_pct:.2f}%)_당일영구동결', qty=safe_qty, target_price=exec_curr_p)

            # 🚨 [AI 에이전트 절대 주의 - 환각 방어막]
            # 장중 휩소 및 조기 익절(HA 역추세, 수익률 도달) 로직 전면 영구 소각 완료.
            # 암살자는 진입 후 덤핑 시간까지 어떠한 흔들림 없이 100% 홀딩(HOLD)합니다.

            return _build_res('HOLD', '보유중_관망(동적_지터_덤핑_대기)')

        # ---------------------------------------------------------
        # 2. 매수 (포지션 0주 일 때) 로직 - 배타적 갭 필터 및 모멘텀 스캔
        # ---------------------------------------------------------
        if not context_data:
            return _build_res('WAIT', '매크로_데이터_수집대기')

        if avwap_state.get('shutdown', False):
            return _build_res('WAIT', '당일영구동결_상태(신규진입금지)')

        if curr_time < time_0410:
            return _build_res('WAIT', '04:10_이전_오프닝_휩소_방어(10분_안전마진_대기)')

        # 🚨 MODIFIED: [V59.04 프리마켓 락다운 쉴드 이식] 제13헌법 6조 완벽 준수
        if not is_regular_session:
            return _build_res('WAIT', '프리마켓_노이즈_원천차단_정규장_개장_대기')

        # 🚨 MODIFIED: [V66.00 AVWAP 동적 지터 덤핑 락온] 신규 진입 영구동결 시간 교정
        if curr_time >= time_dynamic_dump:
            avwap_state["shutdown"] = True
            self.save_state(exec_ticker, now_est, avwap_state)
            reason_str = f'{time_dynamic_dump.strftime("%H:%M:%S")}_도달_신규진입_영구동결'
            return _build_res('SHUTDOWN', reason_str)

        base_prev_c = float(context_data.get('prev_close', 0.0))
        prev_vwap = float(context_data.get('prev_vwap', 0.0))

        if prev_c <= 0 or atr5 <= 0 or day_high <= 0 or day_low <= 0 or exec_curr_p <= 0 or base_vwap <= 0 or prev_vwap <= 0:
            return _build_res('WAIT', '진입_평가용_필수데이터_결측_대기')
            
        actual_gap_dollar = day_high - day_low
        actual_gap_pct = (actual_gap_dollar / prev_c) * 100.0 if prev_c > 0 else 0.0
        
        # NEW: [상대적 체력 연산 30.0% 셧다운 락온] 절대 진폭 차감이 아닌 ATR5 대비 잔여 체력 비율(%) 연산 및 락온
        rem_relative_pct = ((atr5 - actual_gap_pct) / atr5 * 100.0) if atr5 > 0 else 0.0

        if rem_relative_pct < 30.0:
            avwap_state["shutdown"] = True
            self.save_state(exec_ticker, now_est, avwap_state)
            return _build_res('SHUTDOWN', 'ATR5_상대체력_30%미만_고갈_당일신규진입_영구동결')

        # 기초지수(SOXX) 고저가 방향 팩트 스캔
        base_day_high = float(kwargs.get('base_day_high', 0.0))
        base_day_low = float(kwargs.get('base_day_low', 0.0))
        
        is_neg_gap_state = False
        if base_day_high > 0 and base_day_low > 0 and base_prev_c > 0:
            is_neg_gap_state = (base_day_high < base_prev_c) and (base_day_low < base_prev_c)

        # 🚨 MODIFIED: [V61.00 숏(SOXS) 전면 소각] cond1 롱 단일 팩트 락온
        cond1_met = not is_neg_gap_state

        # 🚨 MODIFIED: [V66.05 Split-Brain 시각적 디커플링 해결]
        # 상태 리셋(Latching Release) 로직 무결성 검증 및 SSOT 배선 강화
        persistent_state = self.load_state(exec_ticker, now_est)
        ha_latched_bull = persistent_state.get('HA_LATCHED_BULL', False)
        latch_changed = False

        if ha_2_bullish_no_lower:
            if not ha_latched_bull:
                ha_latched_bull = True
                latch_changed = True
                
        # 🚨 MODIFIED: [Latching 릴리스 팩트 교정] 상대 체력 30% 미만 시 상태기억 해제
        if trend_sequence == "BEAR" or rem_relative_pct < 30.0:
            if ha_latched_bull:
                ha_latched_bull = False
                latch_changed = True

        if latch_changed:
            persistent_state['HA_LATCHED_BULL'] = ha_latched_bull
            self.save_state(exec_ticker, now_est, persistent_state)

        # 🚨 MODIFIED: [V61.00 숏(SOXS) 전면 소각] cond2_met 롱 단일 팩트 락온
        cond2_met = (base_curr_p > base_vwap) and ha_latched_bull

        cond3_met = True

        # 🚨 MODIFIED: [V61.00 숏(SOXS) 전면 소각] cond_seq 롱 단일 팩트 락온
        # 🚨 NEW: [V71.01 시계열 체력 예외 허용 엔진(V-Turn Intercept) 이식]
        # 하락 추세(Time_High < Time_Low)라도 진폭이 5일 평균 진폭(ATR5)의 50% 이상이고, 
        # 현재가가 당일 고점/저점의 중간값(Mid-point) 이상 회복 시 V자 반등으로 판별하여 롱 진입 강제 허가.
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
            if not cond_seq: 
                fail_reasons.append("시계열체력하락세")
            return _build_res('WAIT', f'진입조건대기({",".join(fail_reasons)})')

