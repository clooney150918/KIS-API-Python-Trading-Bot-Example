# ==========================================================
# FILE: telegram_avwap_console.py
# ==========================================================
# 🚨 MODIFIED: [V53.11 시계열 체력 듀얼 대칭 락온] 
# 🚨 MODIFIED: [V53.09 관제탑 UI 횡보장 킬 스위치 시각적 렌더링 강제 바이패스]
# 🚨 MODIFIED: [V61.00 숏(SOXS) 전면 소각 작전 지시서 적용]
# 🚨 NEW: [상대적 체력 연산 30.0% 셧다운 락온 및 UI 디커플링 수술]
# 🚨 NEW: [V65.00 AVWAP 동적 하드스탑 락온]
# 🚨 NEW: [V66.00 AVWAP 암살자 덤핑 지터(Jitter) 분산 락온]
# 🚨 MODIFIED: [V66.05 Split-Brain 시각적 디커플링 해결]
# 🚨 NEW: [3-Stage Apex Intercept (정점 요격) 전술 상태 렌더링 이식]
# 🚨 NEW: [V72.16 AVWAP 정점요격 스위치 UI 연동]
# 🚨 NEW: [V74.00 Operation HA V-Turn Intercept UI 디커플링 해체]
# 🚨 MODIFIED: [V74.02 HA V-Turn 1양봉 격발 및 꼬리 스캔 팩트 동기화]
# 🚨 NEW: [V74.04 심해 고도 필터(Deep-Sea Altitude Filter) UI 락온 및 스코프 전진 배치]
# 🚨 MODIFIED: [V74.05 찐바닥 체력 30% 락다운 바이패스 소각 및 UI 원상 복구]
# - 5년 장기 백테스트 결과에 따라 체력 30% 미만 시 셧다운을 절대 준수하도록 교정됨.
# - 관제탑 UI 렌더링에서도 체력 30% 미만 시 V-Turn 포착 여부와 무관하게 
#   무조건 🔴(미달)로 표출되도록 V65.00의 절대 헌법 UI로 100% 원상 복구 완료.
# - V-Turn 시그널은 시계열 하락세 및 HA 모멘텀 부재만 바이패스(Bypass)합니다.
# 🚨 MODIFIED: [V75.02 관제탑 런타임 붕괴 및 시각적 환각 완벽 수술]
# - get_decision 비동기 래핑 및 is_simulation=True 주입 (제1헌법 및 관찰자 효과 차단)
# - 낡은 10시/15시 텍스트 소각 및 09:30~09:34 캔들 대기 / 지터 덤핑 타임라인 팩트 교정
# 🚨 MODIFIED: [V75.05 텍스트 다이어트 팩트 교정] 프리장/정규장 텍스트 전면 소각
# - 모바일 가독성 최적화를 위해 고가, 저가, 당일 VWAP 앞의 '프리장', '정규장' 꼬리표를 영구 소각하여 1줄 렌더링 락온.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import math
import asyncio
import pandas as pd
import json
import os
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

class AvwapConsolePlugin:
    def __init__(self, config, broker, strategy, tx_lock):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.tx_lock = tx_lock

    async def get_console_message(self, app_data):
        est = ZoneInfo('America/New_York')
        now_est = datetime.datetime.now(est)
        curr_time = now_est.time()
        
        time_0930 = datetime.time(9, 30)
        is_regular_session = curr_time >= time_0930
        
        # MODIFIED: [V75.05 텍스트 다이어트 팩트 교정] 프리장/정규장 꼬리표 (hl_label) 변수 소각
        if not is_regular_session:
            header_status = "🌅 <b>[ 프리마켓 관측 모드 (정규장 대기 중) ]</b>"
        else:
            header_status = "🔥 <b>[ 정규장 관측 모드 (프리장 노이즈 소각 완료) ]</b>"
        
        active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
        avwap_tickers = [t for t in active_tickers if t == "SOXL"]
            
        if not avwap_tickers:
            return "⚠️ <b>[AVWAP 암살자 오프라인]</b>\n▫️ AVWAP 지원 종목이 없습니다.", None
           
        active_avwap = avwap_tickers
        tracking_cache = app_data.get('sniper_tracking', {})
        
        base_tkr = "SOXX"
        base_prev_vwap, base_curr_vwap = 0.0, 0.0
        avg_vwap_5m = 0.0
        base_day_high, base_day_low, base_prev_c = 0.0, 0.0, 0.0
        base_reg_high, base_reg_low = 0.0, 0.0
        base_curr_p = 0.0
      
        ha_status_text = "데이터 부족"
        ha_2_bullish_no_lower = False
        ha_v_turn_candidate = False
        trend_sequence = "PENDING"
        
        df_1m = None
        try:
            try:
                base_prev_c_val = await asyncio.wait_for(asyncio.to_thread(self.broker.get_previous_close, base_tkr), timeout=2.0)
                base_prev_c = float(base_prev_c_val) if base_prev_c_val else 0.0
                
                base_curr_p_val = await asyncio.wait_for(asyncio.to_thread(self.broker.get_current_price, base_tkr), timeout=2.0)
                base_curr_p = float(base_curr_p_val) if base_curr_p_val else 0.0
            except Exception as e:
                logging.debug(f"🚨 기초자산 H/L/PrevC/CurrP 스캔 에러: {e}")

            avwap_ctx = None
            if hasattr(self.strategy, 'v_avwap_plugin'):
                avwap_ctx = await asyncio.wait_for(
                    asyncio.to_thread(self.strategy.v_avwap_plugin.fetch_macro_context, base_tkr), timeout=4.0
                )
             
            if avwap_ctx:
                base_prev_vwap = float(avwap_ctx.get('prev_vwap', 0.0))
            
            df_1m = await asyncio.wait_for(
                asyncio.to_thread(self.broker.get_1min_candles_df, base_tkr), timeout=4.0
            )
             
            if df_1m is not None and not df_1m.empty:
                df = df_1m.copy()
                 
                if 'time_est' in df.columns:
                    if is_regular_session:
                        df = df[(df['time_est'] >= '093000') & (df['time_est'] <= '155900')]
                    else:
                        df = df[(df['time_est'] >= '040000') & (df['time_est'] <= '092959')]
                 
                if not df.empty:
                    base_day_high = float(df['high'].astype(float).max())
                    base_day_low = float(df['low'].astype(float).min())
                    base_reg_high = base_day_high
                    base_reg_low = base_day_low
                
                    df['tp'] = (df['high'].astype(float) + df['low'].astype(float) + df['close'].astype(float)) / 3.0
                    df['vol'] = df['volume'].astype(float)
                    df['vol_tp'] = df['tp'] * df['vol']
             
                    cum_vol = df['vol'].sum()
                    if cum_vol > 0:
                        base_curr_vwap = df['vol_tp'].sum() / cum_vol
                    else:
                        base_curr_vwap = float(df['close'].iloc[-1])
          
                    if base_curr_p == 0.0:
                        base_curr_p = float(df['close'].iloc[-1])
        
                    recent_5 = df.tail(5)
                    sum_vol_5 = recent_5['vol'].sum()
                    if sum_vol_5 > 0:
                        avg_vwap_5m = recent_5['vol_tp'].sum() / sum_vol_5
                    else:
                        avg_vwap_5m = base_curr_vwap

                    t_high_idx = df['high'].astype(float).idxmax()
                    t_low_idx = df['low'].astype(float).idxmin()
                    if t_high_idx < t_low_idx:
                        trend_sequence = "BEAR"
                    elif t_low_idx < t_high_idx:
                        trend_sequence = "BULL"

                    try:
                        if is_regular_session and curr_time < datetime.time(9, 35):
                            ha_status_text = "⏳ 캔들 형성 대기 중"
                            ha_2_bullish_no_lower = False
                            ha_v_turn_candidate = False
                        else:
                            df_ha = df.copy()
                            df_ha['datetime'] = pd.to_datetime(df_ha.index)
                            df_ha.set_index('datetime', inplace=True)
                            df_5m = df_ha.resample('5min', label='left', closed='left').agg({
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

                                df_5m['No_Lower_Wick'] = (df_5m[['HA_Open', 'HA_Close']].min(axis=1) - df_5m['HA_Low']) <= 0.01
                                df_5m['No_Upper_Wick'] = (df_5m['HA_High'] - df_5m[['HA_Open', 'HA_Close']].max(axis=1)) <= 0.01
                                df_5m['Has_Lower_Wick'] = (df_5m[['HA_Open', 'HA_Close']].min(axis=1) - df_5m['HA_Low']) > 0.01
                                df_5m['Has_Upper_Wick'] = (df_5m['HA_High'] - df_5m[['HA_Open', 'HA_Close']].max(axis=1)) > 0.01

                                df_5m['Is_Bullish'] = df_5m['HA_Close'] >= df_5m['HA_Open']
                                df_5m['Is_Bearish'] = df_5m['HA_Close'] < df_5m['HA_Open']

                                if len(df_5m) >= 2:
                                    last_2 = df_5m.tail(2)
                                    ha_2_bullish_no_lower = last_2['Is_Bullish'].all() and last_2['No_Lower_Wick'].all()

                                if len(df_5m) >= 3:
                                    last_idx = len(df_5m) - 1
                                    bull_cond = df_5m['Is_Bullish'].iloc[last_idx] and df_5m['No_Lower_Wick'].iloc[last_idx] and df_5m['Has_Upper_Wick'].iloc[last_idx]
                                    if bull_cond:
                                        bear_count = 0
                                        for i in range(last_idx - 1, -1, -1):
                                            if df_5m['Is_Bearish'].iloc[i] and df_5m['No_Upper_Wick'].iloc[i] and df_5m['Has_Lower_Wick'].iloc[i]:
                                                bear_count += 1
                                            else:
                                                break
                                        if bear_count >= 2:
                                            ha_v_turn_candidate = True

                                last_ha = df_5m.iloc[-1]
                                if last_ha['Is_Bullish']:
                                    ha_wick = "아래 꼬리 없음" if last_ha['No_Lower_Wick'] else "아래 꼬리 존재"
                                    ha_status_text = f"양봉 ({ha_wick})"
                                else:
                                    ha_wick = "윗 꼬리 없음" if last_ha['No_Upper_Wick'] else "윗 꼬리 존재"
                                    ha_status_text = f"음봉 ({ha_wick})"
                    except Exception as e:
                        logging.error(f"관제탑 HA 연산 실패: {e}")

                else:
                    base_curr_p = float(df_1m['close'].iloc[-1]) if base_curr_p == 0.0 else base_curr_p
                    base_curr_vwap = base_curr_p
                    avg_vwap_5m = base_curr_vwap

        except asyncio.TimeoutError:
            logging.error(f"🚨 AVWAP 관제탑 기초자산({base_tkr}) 스캔 타임아웃 발생")
        except Exception as e:
            logging.error(f"🚨 AVWAP 관제탑 기초자산 스캔 에러: {e}")

        msg = f"🔫 <b>[ 차세대 AVWAP 듀얼 모멘텀 관제탑 ]</b>\n{header_status}\n\n"
        msg += f"🏛️ <b>[ 기초자산 ({base_tkr}) 모멘텀 스캔 ]</b>\n"
        
        if base_prev_c > 0 and base_day_high > 0 and base_day_low > 0:
            b_high_pct = ((base_day_high - base_prev_c) / base_prev_c) * 100
            b_low_pct = ((base_day_low - base_prev_c) / base_prev_c) * 100
            # MODIFIED: [V75.05 텍스트 다이어트 팩트 교정] 프리장/정규장 텍스트 소각
            msg += f"▫️ 고가: <b>${base_day_high:.2f}</b> ({b_high_pct:+.2f}%)\n"
            msg += f"▫️ 저가: <b>${base_day_low:.2f}</b> ({b_low_pct:+.2f}%)\n"
            msg += f"▫️ 현재가(1T 종가): <b>${base_curr_p:.2f}</b>\n"
            
        if base_prev_vwap > 0:
            msg += f"▫️ 전일 VWAP: <b>${base_prev_vwap:,.2f}</b>\n"
            rt_gap = ((base_curr_vwap - base_prev_vwap) / base_prev_vwap) * 100
            # MODIFIED: [V75.05 텍스트 다이어트 팩트 교정] 프리장/정규장 텍스트 소각
            msg += f"▫️ 당일 VWAP: <b>${base_curr_vwap:,.2f}</b> ({rt_gap:+.2f}%)\n"
            if avg_vwap_5m > 0 and base_curr_vwap > 0:
                avg_5m_gap = ((avg_vwap_5m - base_curr_vwap) / base_curr_vwap) * 100
                msg += f"▫️ 5분 평균 VWAP: <b>${avg_vwap_5m:,.2f}</b> ({avg_5m_gap:+.2f}%)\n"
        else:
            # MODIFIED: [V75.05 텍스트 다이어트 팩트 교정] 프리장/정규장 텍스트 소각
            msg += f"▫️ 당일 VWAP: <b>${base_curr_vwap:,.2f}</b>\n"
            if avg_vwap_5m > 0:
                msg += f"▫️ 5분 평균 VWAP: <b>${avg_vwap_5m:,.2f}</b>\n"

        keyboard = []

        for t in active_avwap:
            if not tracking_cache.get(f"AVWAP_INIT_{t}"):
                try:
                    saved_state = await asyncio.to_thread(self.strategy.v_avwap_plugin.load_state, t, now_est)
                    if saved_state:
                        tracking_cache[f"AVWAP_BOUGHT_{t}"] = saved_state.get('bought', False)
                        tracking_cache[f"AVWAP_SHUTDOWN_{t}"] = saved_state.get('shutdown', False)
                        tracking_cache[f"AVWAP_QTY_{t}"] = saved_state.get('qty', 0)
                        tracking_cache[f"AVWAP_AVG_{t}"] = saved_state.get('avg_price', 0.0)
                        tracking_cache[f"AVWAP_STRIKES_{t}"] = saved_state.get('strikes', 0)
                        tracking_cache[f"HA_LATCHED_BULL_{t}"] = saved_state.get('HA_LATCHED_BULL', False)
                        tracking_cache[f"AVWAP_DUMP_JITTER_{t}"] = saved_state.get('dump_jitter_sec', 0)
                        
                        tracking_cache[f"APEX_STAGE_1_{t}"] = saved_state.get('APEX_STAGE_1', False)
                        tracking_cache[f"APEX_STAGE_2_{t}"] = saved_state.get('APEX_STAGE_2', False)
                        tracking_cache[f"APEX_PEAK_PRICE_{t}"] = saved_state.get('APEX_PEAK_PRICE', 0.0)
                    
                        tracking_cache[f"AVWAP_INIT_{t}"] = True
                except Exception as e:
                    logging.error(f"🚨 AVWAP 관제탑 상태 자가 복구 실패 ({t}): {e}")

            is_avwap_active = await asyncio.to_thread(getattr(self.cfg, 'get_avwap_hybrid_mode', lambda x: False), t)
            active_str = "🟢 가동 중" if is_avwap_active else "⚪ 대기 중 (OFF)"
            
            is_apex_on = await asyncio.to_thread(getattr(self.cfg, 'get_avwap_apex_mode', lambda x: True), t)
            
            curr_p, day_high, day_low = 0.0, 0.0, 0.0
            try:
                prev_c = await asyncio.wait_for(asyncio.to_thread(self.broker.get_previous_close, t), timeout=2.0)
            except Exception: prev_c = 0.0
             
            try:
                df_t = await asyncio.wait_for(asyncio.to_thread(self.broker.get_1min_candles_df, t), timeout=3.0)
                if df_t is not None and not df_t.empty:
                    if 'time_est' in df_t.columns:
                        if is_regular_session:
                            df_t = df_t[(df_t['time_est'] >= '093000') & (df_t['time_est'] <= '155900')]
                        else:
                            df_t = df_t[(df_t['time_est'] >= '040000') & (df_t['time_est'] <= '092959')]
                    if not df_t.empty:
                        day_high = float(df_t['high'].astype(float).max())
                        day_low = float(df_t['low'].astype(float).min())
                        curr_p = float(df_t['close'].iloc[-1])
            except Exception: pass
            
            try:
                atr5, _ = await asyncio.wait_for(asyncio.to_thread(self.broker.get_atr_data, t), timeout=3.0)
            except Exception: atr5 = 0.0
             
            curr_p = float(curr_p) if curr_p else 0.0
            prev_c = float(prev_c) if prev_c else 0.0
            day_high = float(day_high) if day_high else curr_p
            day_low = float(day_low) if day_low else curr_p
            
            day_amplitude = 0.0
            deep_sea_threshold = 0.0
            is_deep_sea = False
            ha_v_turn_detected = False

            if day_high > 0 and day_low > 0:
                day_amplitude = day_high - day_low
                if day_amplitude > 0:
                    deep_sea_threshold = day_low + (day_amplitude * 0.3)
                    if curr_p <= deep_sea_threshold:
                        is_deep_sea = True

            if ha_v_turn_candidate and is_deep_sea:
                ha_v_turn_detected = True
            
            avwap_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
            avwap_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)
            strikes = tracking_cache.get(f"AVWAP_STRIKES_{t}", 0)
            is_shutdown = tracking_cache.get(f"AVWAP_SHUTDOWN_{t}", False)
            
            label = "롱"
            msg += f"\n🎯 <b>[ {t} ({label}) 작전반 - {active_str} ]</b>\n"

            momentum_met = False
            trend_str = "🔴 <b>조건 미달 (실시간 추세 돌파 감시)</b>"
            
            cond1_met, cond2_met, cond3_met = False, False, False
            cond_seq = True
            
            rem_relative_pct = 0.0
            actual_gap_pct = 0.0

            if base_prev_c > 0 and base_day_high > 0 and base_day_low > 0:
                is_neg_gap_state = (base_day_high < base_prev_c) and (base_day_low < base_prev_c)
                cond1_met = not is_neg_gap_state
                
            if prev_c > 0 and day_high > 0 and day_low > 0:
                actual_gap_dollar = day_high - day_low
                actual_gap_pct = (actual_gap_dollar / prev_c) * 100.0
                if atr5 > 0:
                    rem_relative_pct = ((atr5 - actual_gap_pct) / atr5 * 100.0) if atr5 > 0 else 0.0
                    
            try:
                _saved_state = await asyncio.to_thread(self.strategy.v_avwap_plugin.load_state, t, now_est)
                ha_latched_bull = _saved_state.get('HA_LATCHED_BULL', False)
                tracking_cache[f"HA_LATCHED_BULL_{t}"] = ha_latched_bull
                
                tracking_cache[f"APEX_STAGE_1_{t}"] = _saved_state.get('APEX_STAGE_1', False)
                tracking_cache[f"APEX_STAGE_2_{t}"] = _saved_state.get('APEX_STAGE_2', False)
                tracking_cache[f"APEX_PEAK_PRICE_{t}"] = _saved_state.get('APEX_PEAK_PRICE', 0.0)
            except Exception as e:
                logging.error(f"🚨 관제탑 상태 로드 에러: {e}")
                ha_latched_bull = tracking_cache.get(f"HA_LATCHED_BULL_{t}", False)

            if base_curr_p > 0 and base_curr_vwap > 0:
                cond2_met = ((base_curr_p > base_curr_vwap) and ha_latched_bull) or ha_v_turn_detected
                if cond2_met and not (ha_2_bullish_no_lower or ha_v_turn_detected):
                     ha_status_text = f"{ha_status_text}이지만 시계열 락온 유지"

            seq_text = "상승/대기"
            if trend_sequence == "BEAR":
                cond_seq = False
                if ha_v_turn_detected:
                    cond_seq = True
                    seq_text = "V자 반등(HA 찐바닥 포착)"
                else:
                    seq_text = "하락세(Time_High&lt;Time_Low)"
        
            # 🚨 MODIFIED: [V74.05 찐바닥 체력 30% 락다운 바이패스 소각] 
            c1_str = "🟢" if cond1_met else "🔴"
            c2_str = "🟢" if cond2_met else "🔴"
            c3_str = "🟢" if rem_relative_pct >= 30.0 else "🔴"
            c_seq_str = "🟢" if cond_seq else "🔴"

            criteria = "H/L방향(+) &amp; 시계열상승 &amp; HA모멘텀(현재가&gt;VWAP) &amp; 상대체력(&gt;=30%)"

            if base_curr_p > 0 and base_curr_vwap > 0 and prev_c > 0 and atr5 > 0:
                if cond1_met and cond2_met and (rem_relative_pct >= 30.0) and cond_seq:
                    momentum_met = True
                    trend_str = "🟢 <b>조건 충족 (타격 개시 대기)</b>"
                else:
                    trend_str = "🔴 <b>조건 미달 (실시간 추세 돌파 감시)</b>"
            else:
                trend_str = "⚠️ 데이터 수집 대기 중"

            # 🚨 MODIFIED: [V74.05 체력 30% 바이패스 소각 원상 복구]
            c3_text = f"상대 잔여 체력 30% 이상 (현재: {rem_relative_pct:.1f}%)"

            msg += f"▫️ 판별 기준: <code>{criteria}</code>\n"
            msg += f"▫️ <b>[ 하이킨아시 듀얼 모멘텀 조건 ]</b>\n"
            msg += f"   {c1_str} 고저가 방향 원웨이 일치\n"
            msg += f"   {c_seq_str} 시계열 체력 통과 ({seq_text})\n"
            msg += f"   {c2_str} HA 모멘텀 일치 (현재 5T: {ha_status_text})\n"
            msg += f"   {c3_str} {c3_text}\n"
            msg += f"▫️ 타격 상태: {trend_str}\n"

            if not is_apex_on:
                apex_status_txt = "⚪ 비활성 (수동 OFF 및 지터 덤핑 전용)"
            else:
                apex_s1 = tracking_cache.get(f"APEX_STAGE_1_{t}", False)
                apex_s2 = tracking_cache.get(f"APEX_STAGE_2_{t}", False)
                
                apex_status_txt = "⚪ 비활성 (조건 미달)"
                if apex_s2: apex_status_txt = "🎯 [2단계] 투매 감지 (최종 격발 대기)"
                elif apex_s1: apex_status_txt = "🎯 [1단계] 고점 돌파 (방아쇠 장전 중)"
                
            msg += f"▫️ 정점 요격(Apex Intercept): <b>{apex_status_txt}</b>\n"

            dump_jitter_sec = tracking_cache.get(f"AVWAP_DUMP_JITTER_{t}", 0)
            base_dump_dt = datetime.datetime.combine(now_est.date(), datetime.time(15, 20)).replace(tzinfo=ZoneInfo('America/New_York'))
            dynamic_dump_dt = base_dump_dt - datetime.timedelta(seconds=dump_jitter_sec)
            dynamic_dump_str = dynamic_dump_dt.strftime("%H:%M:%S")

            strike_icon_txt = f"당일 단판 승부 ({dynamic_dump_str} 덤핑 & ATR5 하드스탑 락온)"
            msg += f"▫️ 작전: <b>{strike_icon_txt}</b>\n"
            msg += f"▫️ 독립 물량: {avwap_qty}주\n"

            exh_5 = 0.0
            if atr5 > 0 and prev_c > 0 and day_low > 0:
                high_pct = ((day_high - prev_c) / prev_c) * 100 if prev_c > 0 else 0.0
                low_pct = ((day_low - prev_c) / prev_c) * 100 if prev_c > 0 else 0.0
                
                curr_pct = ((curr_p - prev_c) / prev_c) * 100 if prev_c > 0 else 0.0
                curr_rebound_gap = curr_p - day_low if curr_p >= day_low else 0.0
                curr_rebound_pct = (curr_rebound_gap / prev_c) * 100 if prev_c > 0 else 0.0
                
                high_rebound_gap = day_high - day_low if day_high >= day_low else 0.0
                high_rebound_pct = (high_rebound_gap / prev_c) * 100 if prev_c > 0 else 0.0
            
                exh_5 = (high_rebound_pct / atr5 * 100) if atr5 > 0 else 0
                
                rem_relative_battery = 100.0 - exh_5
                rem_relative_str = f"상대 체력 {rem_relative_battery:.1f}% 잔여" if rem_relative_battery >= 0 else "체력 완전 고갈 (오버슈팅)"

                def make_bar(exh):
                    pos = min(5, max(0, math.ceil(exh / 20.0)))
                    return "━" * pos + "🎯" + "━" * (5 - pos)

                # MODIFIED: [V75.05 텍스트 다이어트 팩트 교정] 프리장/정규장 텍스트 소각
                msg += f"\n📊 <b>[ {t} 당일 체력 정밀 분석 ]</b>\n"
                msg += f"▫️ 전일 종가: <b>${prev_c:.2f}</b> (베이스라인)\n"
                msg += f"▫️ 고가: <b>${day_high:.2f}</b> ({high_pct:+.2f}%/<b>+{high_rebound_pct:.2f}%</b>)\n"
                msg += f"▫️ 저가: <b>${day_low:.2f}</b> ({low_pct:+.2f}%/<b>베이스</b>)\n"
                msg += f"▫️ 현재가: <b>${curr_p:.2f}</b> ({curr_pct:+.2f}%/<b>+{curr_rebound_pct:.2f}%</b>)\n"
                
                if avwap_qty > 0 and avwap_avg > 0:
                    avg_pct = ((avwap_avg - prev_c) / prev_c) * 100 if prev_c > 0 else 0.0
                    avg_rebound_gap = avwap_avg - day_low if avwap_avg >= day_low else 0.0
                    avg_rebound_pct = (avg_rebound_gap / prev_c) * 100 if prev_c > 0 else 0.0
                    msg += f"▫️ 매수평단: <b>${avwap_avg:.2f}</b> ({avg_pct:+.2f}%/<b>+{avg_rebound_pct:.2f}%</b>)\n"
                msg += "\n"
                  
                msg += f"🔋 <b>단기 체력 (ATR5 예상진폭: {atr5:.2f}%)</b>\n"
                msg += f"▫️ 잔여 체력: <b>{rem_relative_str}</b>\n"
                msg += f"   [0%] {make_bar(exh_5)} [100%]\n"
                msg += f"               <b>({exh_5:.0f}% 소진 / 고가 기준)</b>\n\n"
                
                # 🚨 NEW: [V74.04 심해 고도 필터 렌더링 이식]
                msg += f"🌊 <b>심해 고도 (Bottom 30% 찐바닥 락온)</b>\n"
                msg += f"▫️ 심해 임계선: <b>${deep_sea_threshold:.2f}</b> (당일 진폭 ${day_amplitude:.2f})\n"
                if is_deep_sea:
                    msg += f"▫️ 현재가 위치: <b>🟢 심해 통과 (조건 충족)</b>\n"
                else:
                    msg += f"▫️ 현재가 위치: <b>🔴 고도 초과 (노이즈 바이패스)</b>\n"

            # MODIFIED: [V75.02 낡은 10:00 / 15:00 하드코딩 텍스트 영구 소각 및 팩트 교정]
            if not tracking_cache.get(f"AVWAP_BOUGHT_{t}") and not tracking_cache.get(f"AVWAP_SHUTDOWN_{t}"):
                curr_time = now_est.time()
                time_0930 = datetime.time(9, 30)
                time_0934 = datetime.time(9, 34, 59)
                time_dynamic_dump = dynamic_dump_dt.time()
                
                if curr_time < time_0930:
                    avwap_status_txt = "⏳ 프리장 관측 중 (정규장 대기)"
                elif time_0930 <= curr_time <= time_0934:
                    avwap_status_txt = "⏳ 캔들 형성 대기 중"
                elif curr_time >= time_dynamic_dump:
                    avwap_status_txt = "⛔ 금일 감시 종료"

            status_txt = "👀 타점 스캔중"
            if not is_avwap_active:
                status_txt = "⚪ 모드 비활성 (레이더 관측 중)"
            elif is_shutdown: 
                if avwap_qty > 0:
                    status_txt = "🌙 미체결 잔량 오버나이트 롤오버"
                else:
                    status_txt = "🛑 당일 영구동결 (SHUTDOWN)"
            elif avwap_qty > 0: 
                status_txt = f"🎯 딥매수 완료 ({dynamic_dump_str} 덤핑 & ATR5 하드스탑 감시 중)"
            else:
                try:
                    avwap_state_dict = {"strikes": strikes}
                    # MODIFIED: [V75.02 제1헌법 준수 및 관찰자 효과 차단] 비동기 래핑 및 is_simulation=True 강제 주입
                    decision = await asyncio.wait_for(
                        asyncio.to_thread(
                            self.strategy.v_avwap_plugin.get_decision,
                            base_ticker=base_tkr,
                            exec_ticker=t,
                            base_curr_p=base_curr_p,
                            exec_curr_p=curr_p,
                            base_day_open=0.0,
                            avwap_avg_price=avwap_avg,
                            avwap_qty=avwap_qty,
                            avwap_alloc_cash=999999.0,
                            context_data=avwap_ctx,
                            df_1min_base=df_1m,
                            now_est=now_est,
                            avwap_state=avwap_state_dict,
                            regime_data=None,
                            prev_close=prev_c,
                            day_high=day_high,
                            day_low=day_low,
                            atr5=atr5,
                            base_day_high=base_day_high,
                            base_day_low=base_day_low,
                            is_apex_on=is_apex_on,
                            is_simulation=True
                        ),
                        timeout=10.0
                    )

                    action = decision.get('action')
                    reason = decision.get('reason', '')
               
                    if action in ['BUY', 'SELL']:
                        status_txt = f"🔥 타격 조건 100% 충족 ({reason})"
                    elif action == 'SHUTDOWN':
                        status_txt = f"🛑 셧다운 격발 ({reason})"
                    elif reason:
                        status_txt = f"⏳ 대기 ({reason})"
                except Exception as e:
                    logging.debug(f"AVWAP 상태 텍스트 추출 에러: {e}")

            msg += f"\n▫️ 상태: <b>{status_txt}</b>\n"
            
            if avwap_qty > 0:
                keyboard.append([InlineKeyboardButton(f"🧯 {t} 암살자 수동 청산 (0주 락온)", callback_data=f"AVWAP_SET:SYNC_ZERO:{t}")])

        keyboard.append([
            InlineKeyboardButton("🔄 관제탑 새로고침", callback_data="AVWAP_SET:REFRESH:NONE"),
            InlineKeyboardButton("🔙 닫기", callback_data="RESET:CANCEL")
        ])

        msg += f"\n\n⏱️ <i>마지막 스캔: {now_est.strftime('%Y-%m-%d %H:%M:%S')} (EST)</i>\n"

        return msg, InlineKeyboardMarkup(keyboard)
