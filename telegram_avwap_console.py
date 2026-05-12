# ==========================================================
# FILE: telegram_avwap_console.py
# ==========================================================
# 🚨 MODIFIED: [V53.11 시계열 체력 듀얼 대칭 락온] 
# 숏(SOXS) 진입 시 상승 체력 차단 필터 UI 팩트 교정 및 판별 기준 텍스트 대칭화
# 🚨 MODIFIED: [V53.09 관제탑 UI 횡보장 킬 스위치 시각적 렌더링 강제 바이패스]
# MODIFIED: [V47.00 하이킨아시 듀얼 모멘텀 추세 시스템 락온]
# - 04:00 EST 프리마켓 1분봉 파서 스캔 확장 및 데이터 기아 해체
# - 하이킨아시 5min 리샘플링 기반 3대 진입 조건(원웨이, 모멘텀, 체력) 락온
# - 15:00 EST 오버나이트 존버(Hold) 모드 이식 및 투트랙 엑시트 렌더링
# - 10:00 EST 단판 승부 및 조기퇴근(단일 출장) 셧다운 로직 영구 소각 (무한 스캔 개방)
# 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
# NEW: [V47] 시계열 체력 필터 및 현재가 vs 실시간 VWAP 갭 조건 관제탑 렌더링 100% 통합
# 🚨 MODIFIED: [관제탑 듀얼 세션 디커플링 (Time-Split Radar)] 
# 프리마켓(04:00~09:29)과 정규장(09:30~16:00) 데이터를 100% 팩트 분리하여, 정규장 개장 시 프리장 노이즈를 완벽히 소각하고 0점 세팅 락온.
# 🚨 MODIFIED: [V56.00 상태 기억형(Stateful Latching) 모멘텀 락온 엔진 UI 디커플링 수술]
# 현재 캔들이 조건에 맞지더라도 영구 락온 상태라면 모멘텀 🟢 점등 유지 및 "음봉이지만 시계열 락온 유지" 직관적 텍스트 렌더링 동기화 완료.
# 🚨 MODIFIED: [V59.00 AVWAP 암살자 예산 100% 수혈 및 15:25 전량 덤핑 팩트 교정]
# 관제탑 렌더링 분기점을 15:00에서 15:25로 이동하여 시각적 디커플링 원천 차단
# 🚨 MODIFIED: [V59.01 AVWAP 관제탑 '목표 익절' 텍스트 영구 소각]
# 암살자 청산 로직이 15:25 EST 무조건 덤핑으로 대수술됨에 따라, 의미를 상실한 목표 수익률 연산 및 '목표 익절' 렌더링 블록을 시스템에서 100% 적출(소각) 완료.
# 🚨 MODIFIED: [V59.02 잔재 데드코드 영구 소각] 
# 15:25 전량 덤핑 헌법에 따라 무의미해진 '목표가 익절 대기' 환각 텍스트를 '미체결 잔량 오버나이트 롤오버'로 팩트 교정 완료.
# 🚨 MODIFIED: [V59.05 잔재 데드코드 영구 소각] 
# 15:25 단판 승부 헌법에 따라 무의미해진 다중 출장(N회차 교전 완료) 및 무한 출장 렌더링 텍스트를 100% 영구 소각 완료.
# 🚨 MODIFIED: [V61.00 숏(SOXS) 전면 소각 작전 지시서 적용]
# 1) SOXS 종목 강제 주입 로직 영구 소각.
# 2) 인버스 판별, 하락세, 음봉(Bearish) 전용 텍스트 및 상태 메모리 전면 철거.
# 3) 오직 롱(SOXL) 단일 방향 팩트 시각화 및 조건 판별문 진공 압축 완료.
# 🚨 NEW: [상대적 체력 연산 30.0% 셧다운 락온 및 UI 디커플링 수술]
# 기존 절대 진폭 차감을 소각하고, 상대적 잔여 체력 비율(%)을 연산하여 UI 및 Latching 로직에 100% 팩트 동기화.
# 🚨 NEW: [V65.00 AVWAP 동적 하드스탑 락온]
# 암살자 상태 및 작전 텍스트에 ATR5 동적 하드스탑 감시 팩트를 다이내믹하게 인젝션하여 시각적 디커플링 해체 완료.
# 🚨 NEW: [V66.00 AVWAP 암살자 덤핑 지터(Jitter) 분산 락온]
# 관제탑 렌더링 시 하드코딩된 15:25 덤핑 텍스트를 소각하고, 캐시에 저장된 지터 초를 반영한 동적 타임스탬프로 시각적 팩트 교정 완료.
# NEW: [AVWAP 수동 개입 엣지 케이스 방어] 수동 매도 후 유령 물량을 0주로 강제 동기화하는 관제탑 전용 뷰포트 신설
# 🚨 MODIFIED: [V66.05 Split-Brain 시각적 디커플링 해결]
# 텔레그램 관제탑이 자체 캐시로 하이킨아시 락온을 연산하던 레거시 로직 영구 소각.
# 코어 엔진의 상태 파일(JSON)을 SSOT로 삼아 100% 팩트 미러링하도록 아키텍처 수술 완료.
# 🚨 MODIFIED: [V71.02 관제탑 V-Turn 팩트 동기화(UI 디커플링 해체) 수술]
# 코어 엔진에 이식된 V-Turn Intercept(찐바닥 예외 허용) 조건을 관제탑에도 100% 팩트 이식.
# 진폭 50% 돌파 및 당일 미드포인트 회복 감지 시 '하락세' 텍스트를 'V자 반등(찐바닥 포착)'으로 동적 오버라이드.
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
        
        # 🚨 [Time-Split Radar] 세션 분리 스위치 락온
        time_0930 = datetime.time(9, 30)
        is_regular_session = curr_time >= time_0930
        
        if not is_regular_session:
            header_status = "🌅 <b>[ 프리마켓 관측 모드 (정규장 대기 중) ]</b>"
            hl_label = "프리장"
        else:
            header_status = "🔥 <b>[ 정규장 관측 모드 (프리장 노이즈 소각 완료) ]</b>"
            hl_label = "정규장"
        
        active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
        
        # 🚨 MODIFIED: [V61.00 숏(SOXS) 전면 소각] SOXS 강제 주입 로직 영구 철거
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
                 
                # 🚨 [Time-Split Radar] 세션에 따른 데이터 슬라이싱 (노이즈 소각)
                if 'time_est' in df.columns:
                    if is_regular_session:
                        df = df[(df['time_est'] >= '093000') & (df['time_est'] <= '155900')]
                    else:
                        df = df[(df['time_est'] >= '040000') & (df['time_est'] <= '092959')]
                 
                if not df.empty:
                    # 세션별 순수 고/저가 스캔
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

                    # 🚨 [Time-Split Radar] 세션별 시계열 에너지 방향성 즉석 판독
                    t_high_idx = df['high'].astype(float).idxmax()
                    t_low_idx = df['low'].astype(float).idxmin()
                    if t_high_idx < t_low_idx:
                        trend_sequence = "BEAR"
                    elif t_low_idx < t_high_idx:
                        trend_sequence = "BULL"

                    # 🚨 [Time-Split Radar] 하이킨아시 5min 리샘플링 및 예외 락온
                    try:
                        if is_regular_session and curr_time < datetime.time(9, 35):
                            ha_status_text = "⏳ 캔들 형성 대기 중"
                            ha_2_bullish_no_lower = False
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
                                
                                # 0.01$ 갭 필터링
                                df_5m['No_Lower_Wick'] = (df_5m['HA_Open'] - df_5m['HA_Low']) <= 0.01
                                df_5m['Is_Bullish'] = df_5m['HA_Close'] >= df_5m['HA_Open']

                                if len(df_5m) >= 2:
                                    last_2 = df_5m.tail(2)
                                    ha_2_bullish_no_lower = last_2['Is_Bullish'].all() and last_2['No_Lower_Wick'].all()

                                last_ha = df_5m.iloc[-1]
                                if last_ha['Is_Bullish']:
                                    ha_wick = "아래 꼬리 없음" if last_ha['No_Lower_Wick'] else "아래 꼬리 존재"
                                    ha_status_text = f"양봉 ({ha_wick})"
                                else:
                                    ha_status_text = "음봉"
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
            msg += f"▫️ {hl_label} 고가: <b>${base_day_high:.2f}</b> ({b_high_pct:+.2f}%)\n"
            msg += f"▫️ {hl_label} 저가: <b>${base_day_low:.2f}</b> ({b_low_pct:+.2f}%)\n"
            msg += f"▫️ 현재가(1T 종가): <b>${base_curr_p:.2f}</b>\n"
            
        if base_prev_vwap > 0:
            msg += f"▫️ 전일 VWAP: <b>${base_prev_vwap:,.2f}</b>\n"
            rt_gap = ((base_curr_vwap - base_prev_vwap) / base_prev_vwap) * 100
            msg += f"▫️ 당일 {hl_label} VWAP: <b>${base_curr_vwap:,.2f}</b> ({rt_gap:+.2f}%)\n"
            if avg_vwap_5m > 0 and base_curr_vwap > 0:
                avg_5m_gap = ((avg_vwap_5m - base_curr_vwap) / base_curr_vwap) * 100
                msg += f"▫️ 5분 평균 VWAP: <b>${avg_vwap_5m:,.2f}</b> ({avg_5m_gap:+.2f}%)\n"
        else:
            msg += f"▫️ 당일 {hl_label} VWAP: <b>${base_curr_vwap:,.2f}</b>\n"
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
                        # NEW: [V66.00 AVWAP 덤핑 지터 분산 타격 락온] 지터 캐시 로드
                        tracking_cache[f"AVWAP_DUMP_JITTER_{t}"] = saved_state.get('dump_jitter_sec', 0)
                        tracking_cache[f"AVWAP_INIT_{t}"] = True
                except Exception as e:
                    logging.error(f"🚨 AVWAP 관제탑 상태 자가 복구 실패 ({t}): {e}")

            is_avwap_active = await asyncio.to_thread(getattr(self.cfg, 'get_avwap_hybrid_mode', lambda x: False), t)
            active_str = "🟢 가동 중" if is_avwap_active else "⚪ 대기 중 (OFF)"
            
            # 🚨 [Time-Split Radar] 타겟 티커의 세션별 순수 고/저가 스캔 락온
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
            
            avwap_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
            avwap_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)
            strikes = tracking_cache.get(f"AVWAP_STRIKES_{t}", 0)
            is_shutdown = tracking_cache.get(f"AVWAP_SHUTDOWN_{t}", False)
            
            # 🚨 MODIFIED: [V61.00 숏(SOXS) 전면 소각] 롱 하드코딩 및 라벨 압축
            label = "롱"
            msg += f"\n🎯 <b>[ {t} ({label}) 작전반 - {active_str} ]</b>\n"

            momentum_met = False
            trend_str = "🔴 <b>조건 미달 (실시간 추세 돌파 감시)</b>"
            
            cond1_met, cond2_met, cond3_met = False, False, False
            cond_seq = True
            
            # 🚨 NEW: [상대적 체력 연산 30.0% 셧다운 락온 및 UI 디커플링 수술]
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
                    cond3_met = (rem_relative_pct >= 30.0)
                    
            # 🚨 MODIFIED: [V66.05 Split-Brain 시각적 디커플링 해결] 
            # 독자적 모멘텀 판독 기능 영구 소각, JSON 파일의 HA_LATCHED_BULL 팩트만을 참조 (SSOT 단일화)
            try:
                _saved_state = await asyncio.to_thread(self.strategy.v_avwap_plugin.load_state, t, now_est)
                ha_latched_bull = _saved_state.get('HA_LATCHED_BULL', False)
                tracking_cache[f"HA_LATCHED_BULL_{t}"] = ha_latched_bull
            except Exception as e:
                logging.error(f"🚨 관제탑 HA_LATCHED_BULL 팩트 로드 에러: {e}")
                ha_latched_bull = tracking_cache.get(f"HA_LATCHED_BULL_{t}", False)

            if base_curr_p > 0 and base_curr_vwap > 0:
                cond2_met = (base_curr_p > base_curr_vwap) and ha_latched_bull
                if cond2_met and not ha_2_bullish_no_lower:
                     # 🚨 MODIFIED: [V66.05 Split-Brain 시각적 디커플링 해결] JSON 상태와 100% 일치 렌더링
                     ha_status_text = f"{ha_status_text}이지만 시계열 락온 유지"

            # 🚨 MODIFIED: [V71.02 관제탑 V-Turn 팩트 동기화(UI 디커플링 해체) 수술]
            seq_text = "상승/대기"
            if trend_sequence == "BEAR":
                cond_seq = False
                mid_point = 0.0
                if day_high > 0 and day_low > 0:
                    mid_point = (day_high + day_low) / 2.0
                if atr5 > 0 and actual_gap_pct >= (atr5 * 0.5) and curr_p >= mid_point:
                    cond_seq = True
                    seq_text = "V자 반등(찐바닥 포착)"
                else:
                    seq_text = "하락세(Time_High&lt;Time_Low)"
             
            c1_str = "🟢" if cond1_met else "🔴"
            c2_str = "🟢" if cond2_met else "🔴"
            c3_str = "🟢" if cond3_met else "🔴"
            c_seq_str = "🟢" if cond_seq else "🔴"

            # 🚨 MODIFIED: [상대적 체력 연산 30.0% 셧다운 락온] 판별 기준 텍스트 압축 완료
            criteria = "H/L방향(+) &amp; 시계열상승 &amp; HA모멘텀(현재가&gt;VWAP) &amp; 상대체력(&gt;=30%)"

            if base_curr_p > 0 and base_curr_vwap > 0 and prev_c > 0 and atr5 > 0:
                if cond1_met and cond2_met and cond3_met and cond_seq:
                    momentum_met = True
                    trend_str = "🟢 <b>조건 충족 (타격 개시 대기)</b>"
                else:
                    trend_str = "🔴 <b>조건 미달 (실시간 추세 돌파 감시)</b>"
            else:
                trend_str = "⚠️ 데이터 수집 대기 중"

            msg += f"▫️ 판별 기준: <code>{criteria}</code>\n"
            msg += f"▫️ <b>[ 하이킨아시 듀얼 모멘텀 조건 ]</b>\n"
            msg += f"   {c1_str} 고저가 방향 원웨이 일치\n"
            
            # 🚨 MODIFIED: [V71.02 관제탑 V-Turn 팩트 동기화(UI 디커플링 해체) 수술]
            msg += f"   {c_seq_str} 시계열 체력 통과 ({seq_text})\n"
            
            msg += f"   {c2_str} HA 모멘텀 일치 (현재 5T: {ha_status_text})\n"
            # 🚨 MODIFIED: [상대적 체력 연산 30.0% 셧다운 락온] 잔여 체력 브리핑 텍스트 팩트 수술
            msg += f"   {c3_str} 상대 잔여 체력 30% 이상 (현재: {rem_relative_pct:.1f}%)\n"
            msg += f"▫️ 타격 상태: {trend_str}\n"

            # 🚨 MODIFIED: [V66.00 AVWAP 지터 분산 타격 락온] 동적 덤핑 시간 연산
            dump_jitter_sec = tracking_cache.get(f"AVWAP_DUMP_JITTER_{t}", 0)
            base_dump_dt = datetime.datetime.combine(now_est.date(), datetime.time(15, 25)).replace(tzinfo=ZoneInfo('America/New_York'))
            dynamic_dump_dt = base_dump_dt - datetime.timedelta(seconds=dump_jitter_sec)
            dynamic_dump_str = dynamic_dump_dt.strftime("%H:%M:%S")

            # NEW: [V66.00 AVWAP 동적 하드스탑 락온 및 지터 분산 타격] 작전 브리핑 텍스트 팩트 교정
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
                 
                # 🚨 MODIFIED: [상대적 체력 연산 30.0% 셧다운 락온] 배터리 UI 텍스트 팩트 수술
                rem_relative_battery = 100.0 - exh_5
                rem_relative_str = f"상대 체력 {rem_relative_battery:.1f}% 잔여" if rem_relative_battery >= 0 else "체력 완전 고갈 (오버슈팅)"

                def make_bar(exh):
                    pos = min(5, max(0, math.ceil(exh / 20.0)))
                    return "━" * pos + "🎯" + "━" * (5 - pos)

                msg += f"\n📊 <b>[ {t} 당일 체력 정밀 분석 ]</b>\n"
                msg += f"▫️ 전일 종가: <b>${prev_c:.2f}</b> (베이스라인)\n"
                msg += f"▫️ {hl_label} 고가: <b>${day_high:.2f}</b> ({high_pct:+.2f}%/<b>+{high_rebound_pct:.2f}%</b>)\n"
                msg += f"▫️ {hl_label} 저가: <b>${day_low:.2f}</b> ({low_pct:+.2f}%/<b>베이스</b>)\n"
                msg += f"▫️ 현재가: <b>${curr_p:.2f}</b> ({curr_pct:+.2f}%/<b>+{curr_rebound_pct:.2f}%</b>)\n"
                
                if avwap_qty > 0 and avwap_avg > 0:
                    avg_pct = ((avwap_avg - prev_c) / prev_c) * 100 if prev_c > 0 else 0.0
                    avg_rebound_gap = avwap_avg - day_low if avwap_avg >= day_low else 0.0
                    avg_rebound_pct = (avg_rebound_gap / prev_c) * 100 if prev_c > 0 else 0.0
                    msg += f"▫️ 매수평단: <b>${avwap_avg:.2f}</b> ({avg_pct:+.2f}%/<b>+{avg_rebound_pct:.2f}%</b>)\n"
                msg += "\n"
                  
                msg += f"🔋 <b>단기 체력 (ATR5 예상진폭: {atr5:.2f}%)</b>\n"
                msg += f"▫️ 잔여 체력: <b>{rem_relative_str}</b>\n"
                # 🚨 MODIFIED: [상대적 체력 연산 30.0% 셧다운 락온] 배터리 바 [100%] 팩트 수술
                msg += f"   [0%] {make_bar(exh_5)} [100%]\n"
                msg += f"               <b>({exh_5:.0f}% 소진 / 고가 기준)</b>\n"

            curr_time = now_est.time()
            # 🚨 MODIFIED: [V66.00 AVWAP 동적 지터 분산 타격 락온] 타임 쉴드 전진 배치 변수 소각 및 동적 덤핑 타임 적용
            time_dynamic_dump = dynamic_dump_dt.time()
            
            status_txt = "👀 타점 스캔중"
            if not is_avwap_active:
                status_txt = "⚪ 모드 비활성 (레이더 관측 중)"
            elif is_shutdown: 
                # 🚨 MODIFIED: [V59.02 잔재 데드코드 영구 소각] 15:25 전량 덤핑 후 물리적 잔량 발생 시 팩트 기반 렌더링으로 진공 압축
                if avwap_qty > 0:
                     status_txt = "🌙 미체결 잔량 오버나이트 롤오버"
                else:
                    status_txt = "🛑 당일 영구동결 (SHUTDOWN)"
            elif avwap_qty > 0: 
                # NEW: [V66.00 AVWAP 동적 하드스탑 및 지터 분산 락온] 상태 텍스트 팩트 교정
                status_txt = f"🎯 딥매수 완료 ({dynamic_dump_str} 덤핑 & ATR5 하드스탑 감시 중)"
            else:
                try:
                    avwap_state_dict = {"strikes": strikes}
                    
                    decision = self.strategy.v_avwap_plugin.get_decision(
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
                        base_day_low=base_day_low
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

            msg += f"▫️ 상태: <b>{status_txt}</b>\n"
            
            # NEW: [AVWAP 수동 개입 엣지 케이스 방어] 수동 매도 후 유령 물량을 0주로 강제 동기화하는 관제탑 전용 뷰포트 신설
            if avwap_qty > 0:
                keyboard.append([InlineKeyboardButton(f"🧯 {t} 암살자 수동 청산 (0주 락온)", callback_data=f"AVWAP_SET:SYNC_ZERO:{t}")])

        keyboard.append([
            InlineKeyboardButton("🔄 관제탑 새로고침", callback_data="AVWAP_SET:REFRESH:NONE"),
            InlineKeyboardButton("🔙 닫기", callback_data="RESET:CANCEL")
        ])

        msg += f"\n\n⏱️ <i>마지막 스캔: {now_est.strftime('%Y-%m-%d %H:%M:%S')} (EST)</i>\n"

        return msg, InlineKeyboardMarkup(keyboard)
