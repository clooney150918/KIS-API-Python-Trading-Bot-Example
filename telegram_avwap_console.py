# ==========================================================
# FILE: telegram_avwap_console.py
# MODIFIED: [V44.30] AVWAP 관제탑 순수 모니터링화 (설정 제어 버튼 소각)
# MODIFIED: [V44.31] 체력 분석 기준 팩트 교정 - 현재가가 아닌 '당일 고가(High)' 기준으로 방전율 및 잔여 체력 계산 락온 완료
# NEW: [1단계 타임라인 수술] 10:00 EST 타임쉴드 버그를 10:20 EST로 절대 락온 및 UI 텍스트 팩트 교정.
# 🚨 MODIFIED: [V44.50 이벤트 루프 교착 방어] 관제탑 렌더링 시 발생하는 모든 JSON 설정 파일 스캔 및 속성 조회를 비동기 래핑 완료.
# 🚨 MODIFIED: [V44.61 팩트 교정] 관제탑 실시간 VWAP 연산 시 프리마켓 노이즈 전면 소각 및 정규장 100% 락온
# 🚨 MODIFIED: [V44.62 인덴테이션 붕괴 수술] PEP8 규격 강제 및 IndentationError(런타임 즉사) 맹점 영구 소각 완료.
# MODIFIED: [V44.63 자율주행 수익률 하향 스위칭] AUTO 모드 수익률 스펙트럼 1.0%~4.0% 절대 락온 완료
# 🚨 MODIFIED: [V44.72 팩트 교정] AVWAP 관제탑 day_high 파라미터 누수 배선 연결
# 🚨 MODIFIED: [V44.73 팩트 교정] AVWAP 관제탑 가상 예산(0.0) 누수 및 타격 조건 충족 렌더링 락온
# 🚨 MODIFIED: [V44.74 팩트 교정] AVWAP 관제탑 딥매수 완료 후 현재가 증발 맹점 완벽 수술
# 🚨 MODIFIED: [V44.75 팩트 교정] 봇 재가동(업데이트) 시 메모리 증발로 인한 AVWAP 정보 유실(0주 표출) 시각적 맹점 원천 차단. 관제탑 자체 Self-Healing 로드 엔진 이식
# NEW: [V45.00 동적 킬 스위치 상태 렌더링] 기초자산 정규장 순수 진폭(High/Low)을 추출하여 Zero-Line 관통 여부(횡보 감시) 및 SHUTDOWN 상태를 직관적으로 렌더링.
# NEW: [V46 단판 승부 락온] 단판 승부 3대 조건 검증 및 관제탑 UI 렌더링 팩트 교정 완료.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import math
import asyncio
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
        
        # 🚨 [V44.30 수술] 모드 활성화 여부 상관없이 무조건 렌더링하도록 락다운 해제
        active_avwap = avwap_tickers

        tracking_cache = app_data.get('sniper_tracking', {})
        
        # 1. 기초자산(SOXX) 모멘텀 스캔 (타임아웃 족쇄 4초)
        base_tkr = "SOXX"
        base_prev_vwap, base_curr_vwap = 0.0, 0.0
        avg_vwap_5m = 0.0
        base_day_high, base_day_low, base_prev_c = 0.0, 0.0, 0.0
        # NEW: [V45.00 동적 킬 스위치] 정규장 스캔용 변수
        base_reg_high, base_reg_low = 0.0, 0.0
        
        df_1m = None
        try:
            # 기초자산 당일 고/저/전일종가 스캔
            try:
                base_prev_c_val = await asyncio.wait_for(asyncio.to_thread(self.broker.get_previous_close, base_tkr), timeout=2.0)
                base_prev_c = float(base_prev_c_val) if base_prev_c_val else 0.0
                
                base_hl = await asyncio.wait_for(asyncio.to_thread(self.broker.get_day_high_low, base_tkr), timeout=2.0)
                base_day_high = float(base_hl[0]) if base_hl else 0.0
                base_day_low = float(base_hl[1]) if base_hl else 0.0
            except Exception as e:
                logging.debug(f"🚨 기초자산 H/L/PrevC 스캔 에러: {e}")

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
                
                # 🚨 MODIFIED: [V44.61 팩트 수술] 관제탑 실시간 VWAP 연산 시 프리마켓 노이즈 원천 차단
                if 'time_est' in df.columns:
                    df = df[(df['time_est'] >= '093000') & (df['time_est'] <= '155900')]
                
                if not df.empty:
                    # NEW: [V45.00 동적 킬 스위치] 정규장 전용 순수 고가/저가 스캔 락온
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
                        
                    recent_5 = df.tail(5)
                    sum_vol_5 = recent_5['vol'].sum()
                    if sum_vol_5 > 0:
                        avg_vwap_5m = recent_5['vol_tp'].sum() / sum_vol_5
                    else:
                        avg_vwap_5m = base_curr_vwap
                else:
                    base_curr_vwap = float(df_1m['close'].iloc[-1])
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
            
        # NEW: [V45.00 동적 킬 스위치 상태 렌더링]
        if base_prev_c > 0 and base_reg_high > 0 and base_reg_low > 0:
            if base_reg_high > base_prev_c and base_reg_low < base_prev_c:
                zero_line_status = "🔴 관통 (추세 붕괴 / 횡보장 셧다운)"
            else:
                zero_line_status = "🟢 방어 (추세 유지 / 원웨이)"
            msg += f"▫️ 횡보 감시: <b>{zero_line_status}</b>\n"
        
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

        for t in active_avwap:
            # 🚨 MODIFIED: [V44.75 팩트 수술] 관제탑 호출 시 메모리 증발(업데이트/재부팅) 상태라면 디스크에서 직접 자가 복구(Self-Healing) 실행
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

            # 🚨 MODIFIED: 파일 I/O 속성 조회 비동기 래핑
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
            
            # 🚨 MODIFIED: 파일 I/O 속성 조회 비동기 래핑
            is_multi = await asyncio.to_thread(getattr(self.cfg, 'get_avwap_multi_strike_mode', lambda x: False), t)
            user_target_pct = await asyncio.to_thread(getattr(self.cfg, 'get_avwap_target_profit', lambda x: 4.0), t)
            target_mode = tracking_cache.get(f"AVWAP_TARGET_MODE_{t}", "AUTO") 
            
            label = "롱" if t == "SOXL" else "숏"
            msg += f"\n🎯 <b>[ {t} ({label}) 작전반 - {active_str} ]</b>\n"

            # NEW: [V46 단판 승부 락온]
            momentum_met = False
            trend_str = "🔴 <b>조건 미달 (단판 승부 대기)</b>"
            
            # 잔여 체력 선제 연산
            cond1_met, cond2_met, cond3_met = False, False, False
            rem_5_pct_console = 0.0

            if prev_c > 0 and day_high > 0 and day_low > 0:
                if t == "SOXS":
                    cond1_met = (day_high < prev_c) and (day_low < prev_c)
                else:
                    cond1_met = (day_high > prev_c) and (day_low > prev_c)
                    
                actual_gap_dollar = day_high - day_low
                actual_gap_pct = (actual_gap_dollar / prev_c) * 100.0
                if atr5 > 0:
                    rem_5_pct_console = atr5 - actual_gap_pct
                    cond3_met = (rem_5_pct_console >= 1.0)
            
            if base_prev_vwap > 0 and base_curr_vwap > 0 and avg_vwap_5m > 0:
                gap1 = base_curr_vwap - base_prev_vwap
                gap2 = avg_vwap_5m - base_curr_vwap
                if t == "SOXS":
                    cond2_met = (gap1 < 0) and (gap2 < 0)
                else:
                    cond2_met = (gap1 > 0) and (gap2 > 0)
            
            c1_str = "🟢" if cond1_met else "🔴"
            c2_str = "🟢" if cond2_met else "🔴"
            c3_str = "🟢" if cond3_met else "🔴"

            if t == "SOXS":
                criteria = "H/L방향(-) &amp; VWAP(-) &amp; 체력(&gt;=1%)"
            else:
                criteria = "H/L방향(+) &amp; VWAP(+) &amp; 체력(&gt;=1%)"

            if base_prev_vwap > 0 and base_curr_vwap > 0 and avg_vwap_5m > 0 and prev_c > 0 and atr5 > 0:
                if cond1_met and cond2_met and cond3_met:
                    momentum_met = True
                    trend_str = "🟢 <b>조건 충족 (10:00 단판 격발 대기)</b>"
                else:
                    trend_str = "🔴 <b>조건 미달 (조건 부적합)</b>"
            else:
                trend_str = "⚠️ 데이터 수집 대기 중"

            msg += f"▫️ 판별 기준: <code>{criteria}</code>\n"
            msg += f"▫️ <b>[10:00 EST 단판 승부 조건]</b>\n"
            msg += f"   {c1_str} 고저가 방향 원웨이 일치\n"
            msg += f"   {c2_str} VWAP 갭 모멘텀 일치\n"
            msg += f"   {c3_str} 잔여 체력 1% 이상 (현재: {rem_5_pct_console:.1f}%)\n"
            msg += f"▫️ 타격 상태: {trend_str}\n"

            strike_icon_txt = "단판 승부 (1회 조기퇴근 락온)"
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
                
                # 🚨 MODIFIED: [V44.31 수술] 현재가가 아닌 당일 고가 기준으로 방전율 및 잔여 체력 계산
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
                
                # 🚨 MODIFIED: [V44.74 팩트 교정] 매수평단과 현재가 동시 표출 락온
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
                # MODIFIED: [V44.63 자율주행 수익률 하향 스위칭] UI 표출용 스펙트럼 1.0%~4.0% 절대 락온 완료
                if exh_5 >= 90: base_target = 1.0
                elif exh_5 >= 80: base_target = 2.0
                elif exh_5 >= 70: base_target = 3.0
                else: base_target = 4.0
                
                if rem_5_pct > 0:
                    rem_cap = math.floor(rem_5_pct * 10) / 10.0
                    dynamic_target = min(base_target, rem_cap)
                    # MODIFIED: [V44.63 자율주행 수익률 하향 스위칭] 최소 1.0% 보장 하드 클램핑 락온
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
                hardstop_price = avwap_avg * (1 - 8.0 / 100.0)
                if target_mode == "AUTO":
                    target_display = f"🤖자율주행 (+{locked_pct:.1f}%)"
                msg += f"▫️ 목표 익절: <b>${target_price:.2f}</b> ({target_display}) | 하드스탑: <b>${hardstop_price:.2f}</b> (-8.0%)\n"
            else:
                msg += f"▫️ 목표 익절: <b>{target_display}</b> | 하드스탑: <b>-8.0%</b>\n"

            status_txt = "👀 타점 스캔중"
            if not is_avwap_active:
                status_txt = "⚪ 모드 비활성 (레이더 관측 중)"
            elif is_shutdown: 
                status_txt = "🛑 당일 영구동결 (SHUTDOWN)"
            elif avwap_qty > 0: 
                status_txt = "🎯 딥매수 완료 (익절 감시중)"
            else:
                try:
                    base_curr_p = float(df_1m['close'].iloc[-1]) if df_1m is not None and not df_1m.empty else 0.0
                    avwap_state_dict = {"strikes": strikes}
                    
                    decision = self.strategy.v_avwap_plugin.get_decision(
                        base_ticker=base_tkr,
                        exec_ticker=t,
                        base_curr_p=base_curr_p,
                        exec_curr_p=curr_p,
                        base_day_open=0.0,
                        avwap_avg_price=avwap_avg,
                        avwap_qty=avwap_qty,
                        avwap_alloc_cash=999999.0, # 🚨 MODIFIED: [V44.73] 텔레그램 관제탑 가상 예산(0.0) 누수 방어를 위해 넉넉한 가상 예산 주입
                        context_data=avwap_ctx,
                        df_1min_base=df_1m,
                        now_est=now_est,
                        avwap_state=avwap_state_dict,
                        regime_data=None,
                        prev_close=prev_c,
                        day_high=day_high,
                        day_low=day_low,
                        atr5=atr5
                    )

                    action = decision.get('action')
                    reason = decision.get('reason', '')
                    
                    # 🚨 MODIFIED: [V45.00] 타격 조건 충족 및 셧다운 격발 직관적 렌더링 락온
                    if action in ['BUY', 'SELL']:
                        status_txt = f"🔥 타격 조건 100% 충족 ({reason})"
                    elif action == 'SHUTDOWN':
                        status_txt = f"🛑 셧다운 격발 ({reason})"
                    elif reason:
                        status_txt = f"⏳ 대기 ({reason})"
                except Exception as e:
                    logging.debug(f"AVWAP 상태 텍스트 추출 에러: {e}")

            msg += f"▫️ 상태: <b>{status_txt}</b>\n"

        # 🚨 MODIFIED: [V44.30] 설정 모드 스위칭 버튼 영구 소각 (순수 모니터링 기능만 유지)
        keyboard.append([
            InlineKeyboardButton("🔄 관제탑 새로고침", callback_data="AVWAP_SET:REFRESH:NONE"),
            InlineKeyboardButton("🔙 닫기", callback_data="RESET:CANCEL")
        ])

        msg += f"\n\n⏱️ <i>마지막 스캔: {now_est.strftime('%Y-%m-%d %H:%M:%S')} (EST)</i>\n"
        msg += f"💡 <i>설정 제어는 /settlement (전술설정) 메뉴에서 가능합니다.</i>"

        return msg, InlineKeyboardMarkup(keyboard)
