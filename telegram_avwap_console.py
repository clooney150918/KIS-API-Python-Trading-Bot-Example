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
        
        # 🚨 MODIFIED: 파일 I/O 비동기 래핑
        active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
        avwap_tickers = [t for t in active_tickers if t == "SOXL"]
        if "SOXL" in avwap_tickers:
            avwap_tickers.append("SOXS")
            
        if not avwap_tickers:
            return "⚠️ <b>[AVWAP 암살자 오프라인]</b>\n▫️ AVWAP 지원 종목이 없습니다.", None
        
        active_avwap = avwap_tickers

        tracking_cache = app_data.get('sniper_tracking', {})
        
        # 1. 기초자산(SOXX) 모멘텀 스캔 (타임아웃 족쇄 4초)
        base_tkr = "SOXX"
        base_prev_vwap, base_curr_vwap = 0.0, 0.0
        avg_vwap_5m = 0.0
        base_day_high, base_day_low, base_prev_c = 0.0, 0.0, 0.0
        base_reg_high, base_reg_low = 0.0, 0.0
        base_curr_p = 0.0
        
        ha_status_text = "데이터 부족"
        ha_2_bullish_no_lower = False
        ha_2_bearish_no_upper = False
        
        df_1m = None
        try:
            try:
                base_prev_c_val = await asyncio.wait_for(asyncio.to_thread(self.broker.get_previous_close, base_tkr), timeout=2.0)
                base_prev_c = float(base_prev_c_val) if base_prev_c_val else 0.0
                
                base_hl = await asyncio.wait_for(asyncio.to_thread(self.broker.get_day_high_low, base_tkr), timeout=2.0)
                base_day_high = float(base_hl[0]) if base_hl else 0.0
                base_day_low = float(base_hl[1]) if base_hl else 0.0
                
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
                
                # 🚨 MODIFIED: [V47.00] 04:00~15:59 EST 구간 강제 확장
                if 'time_est' in df.columns:
                    df = df[(df['time_est'] >= '040000') & (df['time_est'] <= '155900')]
                
                if not df.empty:
                    # 정규장 전용 순수 고가/저가 스캔 락온
                    df_reg = df[df['time_est'] >= '093000']
                    if not df_reg.empty:
                        base_reg_high = float(df_reg['high'].astype(float).max())
                        base_reg_low = float(df_reg['low'].astype(float).min())
                    else:
                        base_reg_high = float(df['high'].astype(float).max())
                        base_reg_low = float(df['low'].astype(float).min())
                    
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

                    # 🚨 [V47.00] 하이킨아시 5min 리샘플링 구현 및 관제탑 실시간 렌더링 락온
                    try:
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
                            df_5m['No_Upper_Wick'] = (df_5m['HA_High'] - df_5m['HA_Open']) <= 0.01
                            df_5m['Is_Bullish'] = df_5m['HA_Close'] >= df_5m['HA_Open']
                            df_5m['Is_Bearish'] = df_5m['HA_Close'] < df_5m['HA_Open']

                            if len(df_5m) >= 2:
                                last_2 = df_5m.tail(2)
                                ha_2_bullish_no_lower = last_2['Is_Bullish'].all() and last_2['No_Lower_Wick'].all()
                                ha_2_bearish_no_upper = last_2['Is_Bearish'].all() and last_2['No_Upper_Wick'].all()

                            last_ha = df_5m.iloc[-1]
                            ha_dir = "양봉" if last_ha['Is_Bullish'] else "음봉"
                            if last_ha['Is_Bullish']:
                                ha_wick = "아래 꼬리 없음" if last_ha['No_Lower_Wick'] else "아래 꼬리 존재"
                            else:
                                ha_wick = "위 꼬리 없음" if last_ha['No_Upper_Wick'] else "위 꼬리 존재"
                            ha_status_text = f"{ha_dir} ({ha_wick})"
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

        msg = f"🔫 <b>[ 차세대 AVWAP 듀얼 모멘텀 관제탑 ]</b>\n\n"
        msg += f"🏛️ <b>[ 기초자산 ({base_tkr}) 모멘텀 스캔 ]</b>\n"
        
        if base_prev_c > 0 and base_day_high > 0 and base_day_low > 0:
            b_high_pct = ((base_day_high - base_prev_c) / base_prev_c) * 100
            b_low_pct = ((base_day_low - base_prev_c) / base_prev_c) * 100
            msg += f"▫️ 당일 고가(프리포함): <b>${base_day_high:.2f}</b> ({b_high_pct:+.2f}%)\n"
            msg += f"▫️ 당일 저가(프리포함): <b>${base_day_low:.2f}</b> ({b_low_pct:+.2f}%)\n"
            msg += f"▫️ 현재가(1T 종가): <b>${base_curr_p:.2f}</b>\n"
            
        # 🚨 MODIFIED: [V53.09 관제탑 UI 횡보장 킬 스위치 시각적 렌더링 강제 바이패스]
        if base_prev_vwap > 0:
            msg += f"▫️ 전일 VWAP: <b>${base_prev_vwap:,.2f}</b>\n"
            rt_gap = ((base_curr_vwap - base_prev_vwap) / base_prev_vwap) * 100
            msg += f"▫️ 당일 VWAP: <b>${base_curr_vwap:,.2f}</b> ({rt_gap:+.2f}%)\n"
            if avg_vwap_5m > 0 and base_curr_vwap > 0:
                avg_5m_gap = ((avg_vwap_5m - base_curr_vwap) / base_curr_vwap) * 100
                msg += f"▫️ 5분 평균 VWAP: <b>${avg_vwap_5m:,.2f}</b> ({avg_5m_gap:+.2f}%)\n"
        else:
            msg += f"▫️ 당일 VWAP: <b>${base_curr_vwap:,.2f}</b>\n"
            if avg_vwap_5m > 0:
                msg += f"▫️ 5분 평균 VWAP: <b>${avg_vwap_5m:,.2f}</b>\n"

        keyboard = []

        # NEW: [V47] 시계열 체력 스캔 팩트 로드 (비동기 래핑)
        trend_sequence = "PENDING"
        try:
            def _read_avwap_cache():
                cache_file = "data/avwap_cache.json"
                if os.path.exists(cache_file):
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        return json.load(f)
                return {}
            avwap_cache_data = await asyncio.to_thread(_read_avwap_cache)
            base_cache_info = avwap_cache_data.get(base_tkr, {})
            t_high = base_cache_info.get('time_high', "")
            t_low = base_cache_info.get('time_low', "")
            if t_high and t_low:
                # 🚨 [V53.11 듀얼 대칭 락온] 시계열 비교 팩트 정밀 교정
                if t_high < t_low:
                    trend_sequence = "BEAR"
                elif t_low < t_high:
                    trend_sequence = "BULL"
                else:
                    trend_sequence = "PENDING"
        except Exception as e:
            logging.debug(f"시계열 체력 스캔 렌더링 에러: {e}")

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
                    tracking_cache[f"AVWAP_INIT_{t}"] = True
                except Exception as e:
                    logging.error(f"🚨 AVWAP 관제탑 상태 자가 복구 실패 ({t}): {e}")

            is_avwap_active = await asyncio.to_thread(getattr(self.cfg, 'get_avwap_hybrid_mode', lambda x: False), "SOXL" if t == "SOXS" else t)
            active_str = "🟢 가동 중" if is_avwap_active else "⚪ 대기 중 (OFF)"
            
            try:
                curr_p = await asyncio.wait_for(asyncio.to_thread(self.broker.get_current_price, t), timeout=2.0)
            except Exception: curr_p = 0.0
            
            try:
                prev_c = await asyncio.wait_for(asyncio.to_thread(self.broker.get_previous_close, t), timeout=2.0)
            except Exception: prev_c = 0.0
            
            try:
                day_high, day_low = await asyncio.wait_for(asyncio.to_thread(self.broker.get_day_high_low, t), timeout=2.0)
            except Exception: day_high, day_low = 0.0, 0.0
            
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
            
            user_target_pct = await asyncio.to_thread(getattr(self.cfg, 'get_avwap_target_profit', lambda x: 4.0), t)
            target_mode = tracking_cache.get(f"AVWAP_TARGET_MODE_{t}", "AUTO") 
            
            label = "롱" if t == "SOXL" else "숏"
            msg += f"\n🎯 <b>[ {t} ({label}) 작전반 - {active_str} ]</b>\n"

            # 🚨 [V47.00] 하이킨아시 3대 조건 연산 및 렌더링 락온
            momentum_met = False
            trend_str = "🔴 <b>조건 미달 (실시간 추세 돌파 감시)</b>"
            
            cond1_met, cond2_met, cond3_met = False, False, False
            cond_seq = True
            rem_5_pct_console = 0.0

            # 🚨 [V53.11] 듀얼 체력 대칭 락온 (롱/숏 거울 구조 시각화)
            if t == "SOXL" and trend_sequence == "BEAR":
                cond_seq = False
            elif t == "SOXS" and trend_sequence == "BULL":
                cond_seq = False

            if base_prev_c > 0 and base_day_high > 0 and base_day_low > 0:
                is_neg_gap_state = (base_day_high < base_prev_c) and (base_day_low < base_prev_c)
                if t == "SOXS":
                    cond1_met = is_neg_gap_state
                else:
                    cond1_met = not is_neg_gap_state
                    
            if prev_c > 0 and day_high > 0 and day_low > 0:
                actual_gap_dollar = day_high - day_low
                actual_gap_pct = (actual_gap_dollar / prev_c) * 100.0
                if atr5 > 0:
                    rem_5_pct_console = atr5 - actual_gap_pct
                    cond3_met = (rem_5_pct_console >= 1.0)
                    
            if base_curr_p > 0 and base_curr_vwap > 0:
                if t == "SOXS":
                    cond2_met = (base_curr_p < base_curr_vwap) and ha_2_bearish_no_upper
                else:
                    cond2_met = (base_curr_p > base_curr_vwap) and ha_2_bullish_no_lower
            
            c1_str = "🟢" if cond1_met else "🔴"
            c2_str = "🟢" if cond2_met else "🔴"
            c3_str = "🟢" if cond3_met else "🔴"
            c_seq_str = "🟢" if cond_seq else "🔴"

            # HTML Entity escape 적용 (런타임 에러 방어)
            if t == "SOXS":
                criteria = "H/L방향(-) &amp; 시계열하락 &amp; HA모멘텀(현재가&lt;VWAP) &amp; 체력(&gt;=1%)"
            else:
                criteria = "H/L방향(+) &amp; 시계열상승 &amp; HA모멘텀(현재가&gt;VWAP) &amp; 체력(&gt;=1%)"

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
            
            # 🚨 [V53.11] 롱/숏 대칭 시계열 텍스트 분기
            if t == "SOXL":
                seq_text = "상승/대기" if cond_seq else "하락세(Time_High&lt;Time_Low)"
            else:
                seq_text = "하락/대기" if cond_seq else "상승세(Time_Low&lt;Time_High)"
            msg += f"   {c_seq_str} 시계열 체력 통과 ({seq_text})\n"
                
            msg += f"   {c2_str} HA 모멘텀 일치 (현재 5T: {ha_status_text})\n"
            msg += f"   {c3_str} 잔여 체력 1% 이상 (현재: {rem_5_pct_console:.1f}%)\n"
            msg += f"▫️ 타격 상태: {trend_str}\n"

            strike_icon_txt = "무한 출장 (실시간 추세 돌파 락온)"
            if strikes > 0:
                msg += f"▫️ 모드: <b>{strike_icon_txt} ({strikes}회차 교전 완료)</b>\n"
            else:
                msg += f"▫️ 모드: <b>{strike_icon_txt} 세팅됨</b>\n"

            msg += f"▫️ 독립 물량: {avwap_qty}주\n"

            exh_5 = 0.0
            rem_5_pct = 0.0

            if atr5 > 0 and prev_c > 0 and day_low > 0:
                high_pct = ((day_high - prev_c) / prev_c) * 100 if prev_c > 0 else 0.0
                low_pct = ((day_low - prev_c) / prev_c) * 100 if prev_c > 0 else 0.0
                
                curr_pct = ((curr_p - prev_c) / prev_c) * 100 if prev_c > 0 else 0.0
                curr_rebound_gap = curr_p - day_low if curr_p >= day_low else 0.0
                curr_rebound_pct = (curr_rebound_gap / prev_c) * 100 if prev_c > 0 else 0.0
                
                high_rebound_gap = day_high - day_low if day_high >= day_low else 0.0
                high_rebound_pct = (high_rebound_gap / prev_c) * 100 if prev_c > 0 else 0.0
                
                exh_5 = (high_rebound_pct / atr5 * 100) if atr5 > 0 else 0
                rem_5_pct = atr5 - high_rebound_pct
                
                rem_5_str = f"+{rem_5_pct:.2f}% 추가 상승 여력" if rem_5_pct >= 0 else "체력 완전 고갈 (오버슈팅)"

                def make_bar(exh):
                    pos = min(5, max(0, math.ceil(exh / 20.0)))
                    return "━" * pos + "🎯" + "━" * (5 - pos)
                
                msg += f"\n📊 <b>[ {t} 당일 체력 정밀 분석 ]</b>\n"
                msg += f"▫️ 전일 종가: <b>${prev_c:.2f}</b> (베이스라인)\n"
                msg += f"▫️ 당일 고가: <b>${day_high:.2f}</b> ({high_pct:+.2f}%/<b>+{high_rebound_pct:.2f}%</b>)\n"
                msg += f"▫️ 당일 저가: <b>${day_low:.2f}</b> ({low_pct:+.2f}%/<b>베이스</b>)\n"
                msg += f"▫️ 현재가: <b>${curr_p:.2f}</b> ({curr_pct:+.2f}%/<b>+{curr_rebound_pct:.2f}%</b>)\n"
                
                if avwap_qty > 0 and avwap_avg > 0:
                    avg_pct = ((avwap_avg - prev_c) / prev_c) * 100 if prev_c > 0 else 0.0
                    avg_rebound_gap = avwap_avg - day_low if avwap_avg >= day_low else 0.0
                    avg_rebound_pct = (avg_rebound_gap / prev_c) * 100 if prev_c > 0 else 0.0
                    msg += f"▫️ 매수평단: <b>${avwap_avg:.2f}</b> ({avg_pct:+.2f}%/<b>+{avg_rebound_pct:.2f}%</b>)\n"
                msg += "\n"
                
                msg += f"🔋 <b>단기 체력 (ATR5 예상진폭: {atr5:.2f}%)</b>\n"
                msg += f"▫️ 잔여 체력: <b>{rem_5_str}</b>\n"
                msg += f"   [0%] {make_bar(exh_5)} [+{atr5:.2f}%]\n"
                msg += f"               <b>({exh_5:.0f}% 소진 / 고가 기준)</b>\n"

            if target_mode == "AUTO":
                if exh_5 >= 90: base_target = 1.0
                elif exh_5 >= 80: base_target = 2.0
                elif exh_5 >= 70: base_target = 3.0
                else: base_target = 4.0
                
                if rem_5_pct > 0:
                    rem_cap = math.floor(rem_5_pct * 10) / 10.0
                    dynamic_target = min(base_target, rem_cap)
                    dynamic_target = max(1.0, dynamic_target)
                else:
                    dynamic_target = 1.0
                
                applied_pct = dynamic_target
                target_display = f"🤖자율주행 (+{applied_pct:.1f}%)"
            else:
                applied_pct = user_target_pct
                target_display = f"🖐️수동고정 (+{applied_pct:.1f}%)"

            if avwap_qty > 0 and avwap_avg > 0:
                locked_pct = tracking_cache.get(f"AVWAP_LOCKED_TARGET_PCT_{t}", applied_pct)
                target_price = avwap_avg * (1 + locked_pct / 100.0)
                if target_mode == "AUTO":
                    target_display = f"🤖자율주행 (+{locked_pct:.1f}%)"
                msg += f"▫️ 목표 익절: <b>${target_price:.2f}</b> ({target_display})\n"
            else:
                msg += f"▫️ 목표 익절: <b>{target_display}</b>\n"

            curr_time = now_est.time()
            time_1500 = datetime.time(15, 0)
            
            status_txt = "👀 타점 스캔중"
            if not is_avwap_active:
                status_txt = "⚪ 모드 비활성 (레이더 관측 중)"
            elif is_shutdown: 
                # 🚨 [V47.00] 15:00 EST 이후 존버 홀딩 모드 명시적 표출 락온
                if avwap_qty > 0:
                    if curr_time >= time_1500 and curr_p <= avwap_avg:
                        status_txt = "🌙 오버나이트 존버 모드 (절대손절금지)"
                    else:
                        status_txt = "🌙 오버나이트 홀딩 중 (목표가 익절 대기)"
                else:
                    status_txt = "🛑 당일 영구동결 (SHUTDOWN)"
            elif avwap_qty > 0: 
                status_txt = "🎯 딥매수 완료 (익절 감시중)"
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

        keyboard.append([
            InlineKeyboardButton("🔄 관제탑 새로고침", callback_data="AVWAP_SET:REFRESH:NONE"),
            InlineKeyboardButton("🔙 닫기", callback_data="RESET:CANCEL")
        ])

        msg += f"\n\n⏱️ <i>마지막 스캔: {now_est.strftime('%Y-%m-%d %H:%M:%S')} (EST)</i>\n"
        msg += f"💡 <i>설정 제어는 /settlement (전술설정) 메뉴에서 가능합니다.</i>"

        return msg, InlineKeyboardMarkup(keyboard)
