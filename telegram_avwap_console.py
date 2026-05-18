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
        
        if not is_regular_session:
            header_status = "🌅 <b>[ 프리마켓 관측 모드 (정규장 대기 중) ]</b>"
        else:
            header_status = "🔥 <b>[ 정규장 관측 모드 (V7.4 암살자 가동) ]</b>"
        
        active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
        avwap_tickers = [t for t in active_tickers if t == "SOXL"]
        
        if not avwap_tickers:
            return "⚠️ <b>[AVWAP 암살자 오프라인]</b>\n▫️ AVWAP 지원 종목이 없습니다.", None
           
        active_avwap = avwap_tickers
        tracking_cache = app_data.get('sniper_tracking', {})
        
        msg = f"🔫 <b>[ 차세대 AVWAP V7.4 암살자 관제탑 ]</b>\n{header_status}\n\n"
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
                        
                        # V7.4 State Fact Injection
                        tracking_cache[f"AVWAP_PM_H_{t}"] = saved_state.get('PM_H', 0.0)
                        tracking_cache[f"AVWAP_PM_L_{t}"] = saved_state.get('PM_L', 0.0)
                        tracking_cache[f"AVWAP_T_H_{t}"] = saved_state.get('T_H', 0.0)
                        tracking_cache[f"AVWAP_T_L_{t}"] = saved_state.get('T_L', 0.0)
                        tracking_cache[f"AVWAP_OFFSET_{t}"] = saved_state.get('offset', 0.0)
                        tracking_cache[f"AVWAP_WHIPSAW_MODE_{t}"] = saved_state.get('whipsaw_mode', False)
                        tracking_cache[f"AVWAP_WHIPSAW_ARMED_{t}"] = saved_state.get('whipsaw_armed', False)
                        tracking_cache[f"AVWAP_WHIPSAW_CHECKED_{t}"] = saved_state.get('whipsaw_checked', False)
                        
                        tracking_cache[f"AVWAP_INIT_{t}"] = True
                except Exception as e:
                    logging.error(f"🚨 AVWAP 관제탑 상태 로드 에러 ({t}): {e}")

            is_avwap_active = await asyncio.to_thread(getattr(self.cfg, 'get_avwap_hybrid_mode', lambda x: False), t)
            active_str = "🟢 가동 중" if is_avwap_active else "⚪ 대기 중 (OFF)"
            
            # 2. Fetch Current Data (Timeout Fallback Shield)
            try:
                curr_p_val = await asyncio.wait_for(asyncio.to_thread(self.broker.get_current_price, t), timeout=2.0)
                curr_p = float(curr_p_val) if curr_p_val else 0.0
                
                prev_c_val = await asyncio.wait_for(asyncio.to_thread(self.broker.get_previous_close, t), timeout=2.0)
                prev_c = float(prev_c_val) if prev_c_val else 0.0
                
                atr5_val, _ = await asyncio.wait_for(asyncio.to_thread(self.broker.get_atr_data, t), timeout=2.0)
                atr5 = float(atr5_val) if atr5_val else 0.0
            except Exception:
                curr_p, prev_c, atr5 = 0.0, 0.0, 0.0

            avwap_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
            avwap_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)
            is_shutdown = tracking_cache.get(f"AVWAP_SHUTDOWN_{t}", False)
            trap_odno = tracking_cache.get(f"AVWAP_TRAP_ODNO_{t}", "")
            
            # V7.4 Target Extraction
            pm_h = tracking_cache.get(f"AVWAP_PM_H_{t}", 0.0)
            pm_l = tracking_cache.get(f"AVWAP_PM_L_{t}", 0.0)
            t_h = tracking_cache.get(f"AVWAP_T_H_{t}", 0.0)
            t_l = tracking_cache.get(f"AVWAP_T_L_{t}", 0.0)
            offset = tracking_cache.get(f"AVWAP_OFFSET_{t}", 0.0)
            whipsaw_mode = tracking_cache.get(f"AVWAP_WHIPSAW_MODE_{t}", False)
            whipsaw_armed = tracking_cache.get(f"AVWAP_WHIPSAW_ARMED_{t}", False)

            msg += f"🎯 <b>[ {t} (롱) 작전반 - {active_str} ]</b>\n"
            msg += f"▫️ 프리장 최고 (PM_H): <b>${pm_h:.2f}</b>\n"
            msg += f"▫️ 프리장 최저 (PM_L): <b>${pm_l:.2f}</b>\n"
            msg += f"▫️ ATR5 오프셋: <b>${offset:.2f}</b>\n"
            msg += f"▫️ 상승 돌파 목표 (T_H): <b>${t_h:.2f}</b>\n"
            msg += f"▫️ 하락 락온 기준 (T_L): <b>${t_l:.2f}</b>\n\n"

            msg += f"📊 <b>[ 실시간 현재가 스프레드 ]</b>\n"
            msg += f"▫️ 전일종가: <b>${prev_c:.2f}</b> (ATR5: {atr5:.2f}%)\n"
            msg += f"▫️ 현재가: <b>${curr_p:.2f}</b>\n"

            # 🚨 MODIFIED: [V77.01 데이터 기아 방어 및 런타임 무결성 팩트 수술] 수수료 팩트 스캔 소각 및 순수 1.02 곱연산 롤백
            if avwap_qty > 0:
                trap_price = round(avwap_avg * 1.02, 2)
                msg += f"▫️ 매수평단: <b>${avwap_avg:.2f}</b> ({avwap_qty}주)\n"
                msg += f"▫️ 익절목표(+2.0%): <b>${trap_price:.2f}</b>\n"

            # 3. Action Scan via get_decision simulation
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
                        "PM_H": tracking_cache.get(f"AVWAP_PM_H_{t}", 0.0),
                        "PM_L": tracking_cache.get(f"AVWAP_PM_L_{t}", 0.0),
                        "T_H": tracking_cache.get(f"AVWAP_T_H_{t}", 0.0),
                        "T_L": tracking_cache.get(f"AVWAP_T_L_{t}", 0.0),
                        "offset": tracking_cache.get(f"AVWAP_OFFSET_{t}", 0.0),
                        "whipsaw_mode": tracking_cache.get(f"AVWAP_WHIPSAW_MODE_{t}", False),
                        "whipsaw_armed": tracking_cache.get(f"AVWAP_WHIPSAW_ARMED_{t}", False),
                        "whipsaw_checked": tracking_cache.get(f"AVWAP_WHIPSAW_CHECKED_{t}", False),
                        "dump_jitter_sec": tracking_cache.get(f"AVWAP_DUMP_JITTER_{t}", 0)
                    }
                    
                    # 🚨 MODIFIED: [V77.01 데이터 기아 방어 및 런타임 무결성 팩트 수술] is_apex_on, fee_rate 파라미터 소각 및 df_1min_exec=None 릴레이 배선
                    decision = await asyncio.wait_for(
                        asyncio.to_thread(
                            self.strategy.v_avwap_plugin.get_decision,
                            base_ticker=t, exec_ticker=t,
                            base_curr_p=curr_p, exec_curr_p=curr_p,
                            df_1min_base=None, df_1min_exec=None, avwap_qty=avwap_qty,
                            now_est=now_est, avwap_state=avwap_state_dict,
                            context_data=avwap_ctx,
                            is_simulation=True
                        ),
                        timeout=10.0
                    )
                    action = decision.get('action')
                    reason = decision.get('reason', '')
                    if action in ['BUY', 'SELL']:
                        status_txt = f"🔥 타격 조건 충족 ({reason})"
                    elif action == 'SHUTDOWN':
                        status_txt = f"🛑 셧다운 격발 ({reason})"
                    elif whipsaw_mode and not whipsaw_armed:
                         status_txt = f"🛡️ 갭상승 휩소 모드 (T_H 하회 대기)"
                    elif whipsaw_mode and whipsaw_armed:
                        status_txt = f"🛡️ 갭상승 휩소 방어 중 (T_H 재돌파 감시)"
                    elif reason:
                         status_txt = f"⏳ 대기 ({reason})"
                except Exception as e:
                    logging.debug(f"AVWAP 상태 텍스트 추출 에러: {e}")

            msg += f"\n🚨 <b>[ 작전 상태 ]</b>\n"
            msg += f"▫️ 상태: <b>{status_txt}</b>\n"

            # 4. 0주 강제 동기화 락온 뷰포트 보존 (절대 헌법)
            if avwap_qty > 0:
                keyboard.append([InlineKeyboardButton(f"🧯 {t} 암살자 수동 청산 (0주 락온)", callback_data=f"AVWAP_SET:SYNC_ZERO:{t}")])

        keyboard.append([
            InlineKeyboardButton("🔄 관제탑 새로고침", callback_data="AVWAP_SET:REFRESH:NONE"),
            InlineKeyboardButton("🔙 닫기", callback_data="RESET:CANCEL")
        ])

        msg += f"\n\n⏱️ <i>마지막 스캔: {now_est.strftime('%Y-%m-%d %H:%M:%S')} (EST)</i>\n"

        return msg, InlineKeyboardMarkup(keyboard)
