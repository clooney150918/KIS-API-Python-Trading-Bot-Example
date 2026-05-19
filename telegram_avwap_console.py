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
# 🚨 NEW: [V72.16 AVWAP 정점요격 스위치 UI 연동]
# 🚨 MODIFIED: [V75.02 관제탑 런타임 붕괴 및 시각적 환각 완벽 수술]
# 🚨 MODIFIED: [V75.05 텍스트 다이어트 팩트 교정] 프리장/정규장 텍스트 전면 소각
# 🚨 NEW: [V7.4 Assassin Lock-on] 관제탑 UI 렌더링 디커플링 해체
# - 무의미해진 낡은 렌더링 텍스트(Apex 단계, 심해 통과, 하이킨아시 형상) 전면 적출 및 시각적 환각 해체.
# - 당일 타겟 팩트 데이터(PM_H, PM_L, T_H, T_L, Offset) 및 실시간 현재가 스프레드를 진공 압축으로 렌더링.
# - 암살자의 실시간 감시 상태를 대기, 셧다운, 갭상승 휩소 방어, 익절 2.0% 청산 대기 등으로 직관적 표출하도록 뷰포트 교정.
# - 0주 강제 동기화 락온 뷰포트는 기존 절대 헌법에 따라 유지 보존.
# 🚨 MODIFIED: [V76.01 ATR5 동적 하드스탑 렌더링 영구 소각 및 투트랙 엑시트 UI 동기화]
# - V7.4 암살자 투트랙 엑시트 룰에 정면 위배되는 ATR5 하드스탑(손절) 관련 UI 잔재 텍스트 스캔 및 클리닝 완료.
# 🚨 MODIFIED: [순수익 2.0% 절대 보장 타점 공식]
# - 관제탑 레이더 UI에 렌더링되는 익절목표 가격인 trap_price 연산식에 수수료 공식을 동기화
# - 섀도우 스캔을 위한 get_decision 호출 시 fee_rate 파라미터 결속하여 시각적 환각 소각
# 🚨 MODIFIED: [V77.01 데이터 기아 방어 및 런타임 무결성 팩트 수술]
# - 수수료 팩트 스캔 및 is_apex_on 낡은 파라미터 연산을 전면 영구 소각
# - trap_price 익절 목표가 렌더링을 순수 1.02 곱연산으로 팩트 롤백하여 UI 디커플링 해체
# - get_decision 섀도우 연산 호출 시 df_1min_exec=None 주입 락온
# 🚨 NEW: [V77.02 프리마켓 관제탑 데이터 기아 및 런타임 붕괴 완벽 수술]
# - 런타임 붕괴 뇌관 적출: 섀도우 연산 호출 시 존재하지 않는 낡은 변수(avwap_ctx) 참조로 인한 NameError 100% 영구 소각.
# - 팩트 수혈 파이프라인 개통: amp5, prev_close, df_1min_exec 파라미터를 비동기 병렬 스캔하여 get_decision 엔진에 다이렉트 수혈 락온.
# - 시각적 환각 소각 및 상태 오버라이드: pm_locked 팩트를 추출하여 09:25 이전 "👀 프리장 스캔 중", 이후 "🔒 09:25 타겟 락온 완료" 렌더링 동기화.
# - 렌더링 디커플링 해체: get_decision 호출을 문자열 구성 전단으로 시프트(Shift)하여 동적으로 연산된 타겟을 즉시 표출.
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
        
        time_0925 = datetime.time(9, 25)
        time_0930 = datetime.time(9, 30)
        is_regular_session = curr_time >= time_0930
        
        if not is_regular_session:
            header_status = "🌅 <b>[ 프리마켓 관측 모드 (정규장 대기 중) ]</b>"
        else:
            header_status = "🔥 <b>[ 정규장 관측 모드 (V7.1 야성 암살자 가동) ]</b>"
        
        active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
        avwap_tickers = [t for t in active_tickers if t == "SOXL"]
        
        if not avwap_tickers:
            return "⚠️ <b>[AVWAP 암살자 오프라인]</b>\n▫️ AVWAP 지원 종목이 없습니다.", None
           
        active_avwap = avwap_tickers
        tracking_cache = app_data.get('sniper_tracking', {})
        
        msg = f"🔫 <b>[ 차세대 AVWAP V7.1 암살자 관제탑 ]</b>\n{header_status}\n\n"
        keyboard = []

        for t in active_avwap:
            # 1. State Load & Self-Healing
            if not tracking_cache.get(f"AVWAP_INIT_{t}"):
                try:
                    saved_state = await asyncio.to_thread(self.strategy.v_avwap_plugin.load_state, t, now_est)
                    if saved_state:
                        tracking_cache[f"AVWAP_BOUGHT_{t}"] = saved_state.get('bought', False)
                        tracking_cache[f"AVWAP_SHUTDOWN_{t}"] = saved_state.get('shutdown', False)
                        tracking_cache[f"AVWAP_QTY_{t}"] = saved_state.get('qty', 0)
                        tracking_cache[f"AVWAP_AVG_{t}"] = saved_state.get('avg_price', 0.0)
                        tracking_cache[f"AVWAP_STRIKES_{t}"] = saved_state.get('strikes', 0)
                        tracking_cache[f"AVWAP_DUMP_JITTER_{t}"] = saved_state.get('dump_jitter_sec', 0)
                        tracking_cache[f"AVWAP_TRAP_ODNO_{t}"] = saved_state.get('trap_odno', "")
                        
                        # V7.1 Target Fact Injection
                        tracking_cache[f"AVWAP_PM_H_{t}"] = saved_state.get('PM_H', 0.0)
                        tracking_cache[f"AVWAP_PM_L_{t}"] = saved_state.get('PM_L', 0.0)
                        tracking_cache[f"AVWAP_T_H_{t}"] = saved_state.get('T_H', 0.0)
                        tracking_cache[f"AVWAP_T_L_{t}"] = saved_state.get('T_L', 0.0)
                        tracking_cache[f"AVWAP_OFFSET_{t}"] = saved_state.get('offset', 0.0)
                        tracking_cache[f"AVWAP_PM_LOCKED_{t}"] = saved_state.get('pm_locked', False)
                        
                        tracking_cache[f"AVWAP_INIT_{t}"] = True
                except Exception as e:
                    logging.error(f"🚨 AVWAP 관제탑 상태 로드 에러 ({t}): {e}")

            is_avwap_active = await asyncio.to_thread(getattr(self.cfg, 'get_avwap_hybrid_mode', lambda x: False), t)
            active_str = "🟢 가동 중" if is_avwap_active else "⚪ 대기 중 (OFF)"
            
            # 2. Fetch Current Data & Missing Params (Timeout Fallback Shield & Fact Pipelining)
            amp5 = 0.0
            df_1m = None
            try:
                # 🚨 NEW: [V77.02 팩트 수혈 파이프라인 개통] 비동기 병렬 스캔
                curr_p_task = asyncio.to_thread(self.broker.get_current_price, t)
                prev_c_task = asyncio.to_thread(self.broker.get_previous_close, t)
                amp5_task = asyncio.to_thread(self.broker.get_amp_5d_data, t)
                df_task = asyncio.to_thread(self.broker.get_1min_candles_df, t)

                res_batch = await asyncio.wait_for(
                    asyncio.gather(curr_p_task, prev_c_task, amp5_task, df_task, return_exceptions=True),
                    timeout=5.0
                )
                
                curr_p = float(res_batch[0]) if not isinstance(res_batch[0], Exception) and res_batch[0] else 0.0
                prev_c = float(res_batch[1]) if not isinstance(res_batch[1], Exception) and res_batch[1] else 0.0
                amp5 = float(res_batch[2]) if not isinstance(res_batch[2], Exception) and res_batch[2] else 0.0
                df_1m = res_batch[3] if not isinstance(res_batch[3], Exception) else None
                
            except Exception as e:
                logging.debug(f"🚨 데이터 팩트 수혈 에러: {e}")
                curr_p, prev_c, amp5, df_1m = 0.0, 0.0, 0.0, None

            avwap_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
            avwap_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)
            is_shutdown = tracking_cache.get(f"AVWAP_SHUTDOWN_{t}", False)
            trap_odno = tracking_cache.get(f"AVWAP_TRAP_ODNO_{t}", "")
            
            # V7.1 Target Extraction Default
            pm_h = tracking_cache.get(f"AVWAP_PM_H_{t}", 0.0)
            pm_l = tracking_cache.get(f"AVWAP_PM_L_{t}", 0.0)
            t_h = tracking_cache.get(f"AVWAP_T_H_{t}", 0.0)
            t_l = tracking_cache.get(f"AVWAP_T_L_{t}", 0.0)
            offset = tracking_cache.get(f"AVWAP_OFFSET_{t}", 0.0)
            pm_locked = tracking_cache.get(f"AVWAP_PM_LOCKED_{t}", False)
            
            # 3. Action Scan via get_decision simulation (스코프 전진 배치 락온)
            status_txt = "👀 타점 스캔중"
            if not is_avwap_active:
                status_txt = "⚪ 모드 비활성 (레이더 관측 중)"
            elif is_shutdown: 
                status_txt = "🛑 당일 영구동결 (SHUTDOWN)"
            elif avwap_qty > 0:
                if trap_odno:
                    status_txt = "🎯 딥매수 완료 (+2.0% 익절 덫 장전 중)"
                else:
                    status_txt = "🎯 딥매수 완료 (15:20 전량 덤핑 대기 중)"
            else:
                try:
                    avwap_state_dict = {
                        "strikes": tracking_cache.get(f"AVWAP_STRIKES_{t}", 0),
                        "shutdown": tracking_cache.get(f"AVWAP_SHUTDOWN_{t}", False),
                        "qty": tracking_cache.get(f"AVWAP_QTY_{t}", 0),
                        "avg_price": tracking_cache.get(f"AVWAP_AVG_{t}", 0.0),
                        "bought": tracking_cache.get(f"AVWAP_BOUGHT_{t}", False),
                        "daily_bought_qty": tracking_cache.get(f"AVWAP_DAILY_BOUGHT_{t}", 0),
                        "daily_sold_qty": tracking_cache.get(f"AVWAP_DAILY_SOLD_{t}", 0),
                        "trap_odno": tracking_cache.get(f"AVWAP_TRAP_ODNO_{t}", ""),
                        "PM_H": pm_h,
                        "PM_L": pm_l,
                        "T_H": t_h,
                        "T_L": t_l,
                        "offset": offset,
                        "pm_locked": pm_locked,
                        "dump_jitter_sec": tracking_cache.get(f"AVWAP_DUMP_JITTER_{t}", 0)
                    }
                    
                    # 🚨 NEW: [V77.02 런타임 붕괴 뇌관 적출 및 다이렉트 수혈]
                    # 존재하지 않는 avwap_ctx 변수를 소각하고, amp5 및 df_1min_exec 팩트 직접 주입
                    decision = await asyncio.wait_for(
                        asyncio.to_thread(
                            self.strategy.v_avwap_plugin.get_decision,
                            base_ticker=t, exec_ticker=t,
                            base_curr_p=curr_p, exec_curr_p=curr_p,
                            df_1min_base=None, df_1min_exec=df_1m, avwap_qty=avwap_qty,
                            now_est=now_est, avwap_state=avwap_state_dict,
                            context_data=None, # 환각 변수 제거
                            is_simulation=True,
                            amp5=amp5,
                            prev_close=prev_c
                        ),
                        timeout=5.0
                    )
                    
                    if decision:
                        action = decision.get('action')
                        reason = decision.get('reason', '')
                        
                        # 🚨 NEW: [V77.02 다이렉트 패스 페이로드 오버라이드] 
                        # 동적으로 연산된 타겟을 즉시 렌더링에 반영하기 위한 팩트 덮어쓰기
                        pm_h = decision.get('PM_H', pm_h)
                        pm_l = decision.get('PM_L', pm_l)
                        t_h = decision.get('T_H', t_h)
                        t_l = decision.get('T_L', t_l)
                        offset = decision.get('offset', offset)
                        pm_locked = decision.get('pm_locked', pm_locked)
                        
                        tracking_cache[f"AVWAP_PM_LOCKED_{t}"] = pm_locked
                        tracking_cache[f"AVWAP_PM_H_{t}"] = pm_h
                        tracking_cache[f"AVWAP_PM_L_{t}"] = pm_l
                        tracking_cache[f"AVWAP_T_H_{t}"] = t_h
                        tracking_cache[f"AVWAP_T_L_{t}"] = t_l
                        tracking_cache[f"AVWAP_OFFSET_{t}"] = offset
                        
                        # 🚨 NEW: [V77.02 시각적 환각 소각 및 상태 렌더링]
                        if not pm_locked and curr_time < time_0925:
                            status_txt = "👀 프리장 실시간 타겟 스캔 중"
                        elif pm_locked and curr_time < time_0930:
                            status_txt = "🔒 09:25 타겟 락온 완료 (정규장 대기 중)"
                        else:
                            if action in ['BUY', 'SELL']:
                                status_txt = f"🔥 타격 조건 충족 ({reason})"
                            elif action == 'SHUTDOWN':
                                status_txt = f"🛑 셧다운 격발 ({reason})"
                            elif reason:
                                status_txt = f"⏳ 대기 ({reason})"
                                
                except Exception as e:
                    logging.debug(f"AVWAP 상태 텍스트 추출 에러: {e}")

            # 4. Message Assembly (using updated, dynamic targets)
            msg += f"🎯 <b>[ {t} (롱) 작전반 - {active_str} ]</b>\n"
            msg += f"▫️ 프리장 최고 (PM_H): <b>${pm_h:.2f}</b>\n"
            msg += f"▫️ 프리장 최저 (PM_L): <b>${pm_l:.2f}</b>\n"
            msg += f"▫️ Amp5 오프셋: <b>${offset:.2f}</b>\n"
            msg += f"▫️ 상승 돌파 목표 (T_H): <b>${t_h:.2f}</b>\n"
            msg += f"▫️ 하락 락온 기준 (T_L): <b>${t_l:.2f}</b>\n\n"

            msg += f"📊 <b>[ 실시간 현재가 스프레드 ]</b>\n"
            msg += f"▫️ 전일종가: <b>${prev_c:.2f}</b> (Amp5: {amp5*100:.2f}%)\n"
            msg += f"▫️ 현재가: <b>${curr_p:.2f}</b>\n"

            # 🚨 MODIFIED: [V77.01 데이터 기아 방어 및 런타임 무결성 팩트 수술] 순수 1.02 곱연산 롤백
            if avwap_qty > 0:
                trap_price = round(avwap_avg * 1.02, 2)
                msg += f"▫️ 매수평단: <b>${avwap_avg:.2f}</b> ({avwap_qty}주)\n"
                msg += f"▫️ 익절목표(+2.0%): <b>${trap_price:.2f}</b>\n"

            msg += f"\n🚨 <b>[ 작전 상태 ]</b>\n"
            msg += f"▫️ 상태: <b>{status_txt}</b>\n"

            # 5. 0주 강제 동기화 락온 뷰포트 보존 (절대 헌법)
            if avwap_qty > 0:
                keyboard.append([InlineKeyboardButton(f"🧯 {t} 암살자 수동 청산 (0주 락온)", callback_data=f"AVWAP_SET:SYNC_ZERO:{t}")])

        keyboard.append([
            InlineKeyboardButton("🔄 관제탑 새로고침", callback_data="AVWAP_SET:REFRESH:NONE"),
            InlineKeyboardButton("🔙 닫기", callback_data="RESET:CANCEL")
        ])

        msg += f"\n\n⏱️ <i>마지막 스캔: {now_est.strftime('%Y-%m-%d %H:%M:%S')} (EST)</i>\n"

        return msg, InlineKeyboardMarkup(keyboard)
