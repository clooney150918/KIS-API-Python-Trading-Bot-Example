# ==========================================================
# FILE: telegram_avwap_console.py
# ==========================================================
# 🚨 MODIFIED: [V77.31] 팻핑거 방어 - PRE, REG 시장 상태일 때만 수동 요격 버튼 활성화 락온
# 🚨 NEW: [Case 11] 다중 출격(Multi-Sortie) 모드 관제탑 헤더 상태 렌더링 동기화
# 🚨 MODIFIED: [Case 14 절대 헌법 준수] 달력 API(mcal) 호출 시 10.0초 타임아웃 락온으로 이벤트 루프 교착 완벽 차단
# 🚨 MODIFIED: [Case 28 준수] 팻핑거 방어를 위한 수동 요격 UI 디커플링 팩트 교정 (타점 이탈 시 버튼 비활성화)
# 🚨 MODIFIED: [0.0달러 환각 방어] 통신 장애 시 0.0달러 폴백 값이 수동 요격 버튼을 강제 활성화시키는 맹점 원천 차단
# 🚨 NEW: [Case 31] AVWAP 1분봉 시차 패러독스(Time-Shield Decoupling) 락온 캐싱 팩트 동기화 완료.
# 🚨 MODIFIED: [V78.00 팩트 교정] AVWAP 오프셋 연산 50% -> 45% 하향 락온 및 관제탑 렌더링 100% 동기화 완료.
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
        
        time_0400 = datetime.time(4, 0)
        time_0930 = datetime.time(9, 30)
     
        import pandas_market_calendars as mcal
        try:
            def _fetch_schedule():
                nyse = mcal.get_calendar('NYSE')
                return nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
            schedule = await asyncio.wait_for(asyncio.to_thread(_fetch_schedule), timeout=10.0)
            if schedule.empty:
                status_code = "CLOSE"
            else:
                market_open = schedule.iloc[0]['market_open'].astimezone(est)
                market_close = schedule.iloc[0]['market_close'].astimezone(est)
                pre_start = market_open.replace(hour=4, minute=0, second=0, microsecond=0)
                after_end = market_close.replace(hour=20, minute=0, second=0, microsecond=0)

                if pre_start <= now_est < market_open:
                    status_code = "PRE"
                elif market_open <= now_est < market_close:
                    status_code = "REG"
                elif market_close <= now_est < after_end:
                    status_code = "AFTER"
                else:
                    status_code = "CLOSE"
        except asyncio.TimeoutError:
            logging.error("🚨 달력 API 호출 타임아웃 (10초). Fail-Open 평일 개장으로 강제 폴백합니다.")
            if now_est.weekday() < 5:
                status_code = "REG"
            else:
                status_code = "CLOSE"
        except Exception:
            if now_est.weekday() < 5:
                status_code = "REG"
            else:
                status_code = "CLOSE"

        if status_code in ["AFTER", "CLOSE"]:
            header_status = "🌙 <b>[ 애프터마켓 / 감시 종료 ]</b>"
        elif status_code == "PRE":
            header_status = "🌅 <b>[ 프리장 선제 타격 모드 (04:00~09:29 스캔 중) ]</b>"
        else:
            header_status = "🔥 <b>[ 정규장 실시간 추격 모드 (V77.08 지정가 덫 요격) ]</b>"
        
        active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
        avwap_tickers = [t for t in active_tickers if t == "SOXL"]
       
        if not avwap_tickers:
            return "⚠️ <b>[AVWAP 암살자 오프라인]</b>\n▫️ AVWAP 지원 종목이 없습니다.", None
           
        active_avwap = avwap_tickers
        tracking_cache = app_data.get('sniper_tracking', {})
        
        try:
            cash_val, _ = await asyncio.wait_for(asyncio.to_thread(self.broker.get_account_balance), timeout=5.0)
            available_cash = float(cash_val or 0.0)
        except asyncio.TimeoutError:
            available_cash = 0.0
        except Exception as e:
            available_cash = 0.0
        
        msg = f"🔫 <b>[ 차세대 AVWAP V78.20 관제탑 ]</b>\n{header_status}\n\n"
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
                        tracking_cache[f"AVWAP_DUMP_JITTER_{t}"] = saved_state.get('dump_jitter_sec', 0)
                        tracking_cache[f"AVWAP_TRAP_ODNO_{t}"] = saved_state.get('trap_odno', "")
                        
                        tracking_cache[f"AVWAP_LIMIT_ORDER_PLACED_{t}"] = saved_state.get('limit_order_placed', False)
                        tracking_cache[f"AVWAP_PLACED_TARGET_TH_{t}"] = saved_state.get('placed_target_th', 0.0)
                        tracking_cache[f"AVWAP_TRAP_PLACED_TIME_{t}"] = saved_state.get('trap_placed_time', "")
           
                        tracking_cache[f"AVWAP_PM_H_{t}"] = saved_state.get('PM_H', 0.0)
                        tracking_cache[f"AVWAP_PM_L_{t}"] = saved_state.get('PM_L', 0.0)
                        tracking_cache[f"AVWAP_T_H_{t}"] = saved_state.get('T_H', 0.0)
                        tracking_cache[f"AVWAP_T_L_{t}"] = saved_state.get('T_L', 0.0)
                        tracking_cache[f"AVWAP_OFFSET_{t}"] = saved_state.get('offset', 0.0)
                        
                    tracking_cache[f"AVWAP_INIT_{t}"] = True
                except Exception as e:
                    pass

            is_avwap_active = await asyncio.to_thread(getattr(self.cfg, 'get_avwap_hybrid_mode', lambda x: False), t)
            
            sortie_mode = await asyncio.to_thread(getattr(self.cfg, 'get_avwap_sortie_mode', lambda x: "SINGLE"), t)
            sortie_str = "단일 타격(1회)" if sortie_mode == "SINGLE" else "다중 출격(무한)"
            active_str = f"🟢 암살 가동 ({sortie_str})" if is_avwap_active else "⚪ 대기 (OFF)"
            
            amp5 = 0.0
            df_1m = None
            try:
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
                curr_p, prev_c, amp5, df_1m = 0.0, 0.0, 0.0, None

            if df_1m is not None and not df_1m.empty and 'time_est' in df_1m.columns:
                df_reg = df_1m[(df_1m['time_est'] >= '093000') & (df_1m['time_est'] <= '155959')]
                if not df_reg.empty:
                    tracking_cache[f"AVWAP_REG_H_{t}"] = float(df_reg['high'].astype(float).max())
                    tracking_cache[f"AVWAP_REG_L_{t}"] = float(df_reg['low'].astype(float).min())

            avwap_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
            avwap_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)
            is_shutdown = tracking_cache.get(f"AVWAP_SHUTDOWN_{t}", False)
            trap_odno = tracking_cache.get(f"AVWAP_TRAP_ODNO_{t}", "")
            
            limit_order_placed = tracking_cache.get(f"AVWAP_LIMIT_ORDER_PLACED_{t}", False)
            placed_target_th = tracking_cache.get(f"AVWAP_PLACED_TARGET_TH_{t}", 0.0)
            trap_placed_time = tracking_cache.get(f"AVWAP_TRAP_PLACED_TIME_{t}", "")
            
            pm_h = tracking_cache.get(f"AVWAP_PM_H_{t}", 0.0)
            pm_l = tracking_cache.get(f"AVWAP_PM_L_{t}", 0.0)
            t_h = tracking_cache.get(f"AVWAP_T_H_{t}", 0.0)
            t_l = tracking_cache.get(f"AVWAP_T_L_{t}", 0.0)
            offset = tracking_cache.get(f"AVWAP_OFFSET_{t}", 0.0)
            
            status_txt = "⚡ T_H 선제 지정가 덫 장전 대기 중"
            
            try:
                avwap_state_dict = {
                    "strikes": tracking_cache.get(f"AVWAP_STRIKES_{t}", 0),
                    "shutdown": is_shutdown,
                    "qty": avwap_qty,
                    "avg_price": avwap_avg,
                    "bought": tracking_cache.get(f"AVWAP_BOUGHT_{t}", False),
                    "daily_bought_qty": tracking_cache.get(f"AVWAP_DAILY_BOUGHT_{t}", 0),
                    "daily_sold_qty": tracking_cache.get(f"AVWAP_DAILY_SOLD_{t}", 0),
                    "trap_odno": trap_odno,
                    "PM_H": pm_h,
                    "PM_L": pm_l,
                    "T_H": t_h,
                    "T_L": t_l,
                    "offset": offset,
                    "limit_order_placed": limit_order_placed,
                    "placed_target_th": placed_target_th,
                    "dump_jitter_sec": tracking_cache.get(f"AVWAP_DUMP_JITTER_{t}", 0),
                    "trap_placed_time": tracking_cache.get(f"AVWAP_TRAP_PLACED_TIME_{t}", "")
                }
                
                decision = await asyncio.wait_for(
                    asyncio.to_thread(
                        self.strategy.v_avwap_plugin.get_decision,
                        base_ticker=t, exec_ticker=t,
                        base_curr_p=curr_p, exec_curr_p=curr_p,
                        df_1min_base=None, df_1min_exec=df_1m, avwap_qty=avwap_qty,
                        avwap_alloc_cash=available_cash, 
                        now_est=now_est, avwap_state=avwap_state_dict,
                        context_data=None,
                        is_simulation=True,
                        amp5=amp5,
                        prev_close=prev_c,
                        sortie_mode=sortie_mode
                    ),
                    timeout=5.0
                )
                
                if decision:
                    action = decision.get('action')
                    reason = decision.get('reason', '')
                    
                    pm_h = decision.get('PM_H', pm_h)
                    pm_l = decision.get('PM_L', pm_l)
                    t_h = decision.get('T_H', t_h)
                    t_l = decision.get('T_L', t_l)
                    offset = decision.get('offset', offset)
                    
                    tracking_cache[f"AVWAP_PM_H_{t}"] = pm_h
                    tracking_cache[f"AVWAP_PM_L_{t}"] = pm_l
                    tracking_cache[f"AVWAP_T_H_{t}"] = t_h
                    tracking_cache[f"AVWAP_T_L_{t}"] = t_l
                    tracking_cache[f"AVWAP_OFFSET_{t}"] = offset
        
                    if is_shutdown: 
                        status_txt = f"🛑 셧다운 격발 ({reason})" if reason and action == 'SHUTDOWN' else "🛑 당일 영구동결 (SHUTDOWN 퇴근)"
                    elif avwap_qty > 0:
                        if trap_odno:
                            # 🚨 MODIFIED: [V78.00] 익절 덫 안내 문구 2.0% 통일
                            status_txt = "🎯 체결 완료 ➡️ [2.0% 지정가 익절 덫] 가동 중"
                        else:
                            status_txt = "🎯 체결 완료 ➡️ (15:20 청산 지터 대기 중)"
                    elif limit_order_placed and placed_target_th > 0:
                        status_txt = f"⚡ 요격 조건 100% 충족 ➡️ [지정가 매수 덫 장전 집행: ${placed_target_th:.2f}]"
                    else:
                        if action == "PLACE_TRAP":
                            status_txt = f"⚡ 요격 조건 100% 충족 ➡️ [지정가 매수 덫 장전 집행]"
                        elif action == "VERIFY_TRAP_FILL":
                            status_txt = f"🔥 덫 하향 관통 ➡️ [실체결 무결성 검증 격발]"
                        elif action == "TRAP_WAIT":
                            status_txt = f"⏳ 지정가 덫 장전 완료 ➡️ [지정가 매수 체결 대기]"
                        elif action == 'SHUTDOWN':
                            status_txt = f"🛑 셧다운 격발 ({reason})"
                        elif reason:
                            if "동적_순수타격선_도달_감시중" in reason or "스캔" in status_txt:
                                status_txt = "⚡ T_H 선제 지정가 덫 장전 대기 중"
                            else:
                                status_txt = f"⏳ 대기 ({reason})"
                            
            except Exception as e:
                pass

            reg_h = tracking_cache.get(f"AVWAP_REG_H_{t}", 0.0)
            reg_l = tracking_cache.get(f"AVWAP_REG_L_{t}", 0.0)
            
            msg += f"🎯 <b>[ {t} (롱) 작전반 - {active_str} ]</b>\n"
            msg += f"▫️ 프리장 최고 (PM_H): <b>${pm_h:.2f}</b>\n"
            msg += f"▫️ 프리장 최저 (PM_L): <b>${pm_l:.2f}</b>\n"
            msg += f"▫️ 정규장 최고 (REG_H): <b>${reg_h:.2f}</b>\n"
            msg += f"▫️ 정규장 최저 (REG_L): <b>${reg_l:.2f}</b>\n"
            # 🚨 MODIFIED: [V78.00 팩트 교정] 오프셋 45% 렌더링 락온
            msg += f"▫️ Amp5 오프셋 (45%): <b>${offset:.2f}</b>\n"
            msg += f"▫️ 상승 돌파 목표 (T_H): <b>${t_h:.2f}</b>\n      (지정가 덫 장전선)\n"
            msg += f"▫️ 하락 지지 기준 (T_L): <b>${t_l:.2f}</b>\n      (단순 참조용)\n\n"

            msg += f"📊 <b>[ 실시간 현재가 스프레드 ]</b>\n"
            msg += f"▫️ 전일종가: <b>${prev_c:.2f}</b>\n      (Amp5 진폭: {amp5*100:.2f}%)\n"
            msg += f"▫️ 현재가격: <b>${curr_p:.2f}</b>\n"

            if avwap_qty > 0:
                # 🚨 MODIFIED: [V78.00] 투트랙 익절 목표가 2.0% 락온
                trap_price = round(avwap_avg * 1.02, 2)
                msg += f"▫️ 매수평단: <b>${avwap_avg:.2f}</b> ({avwap_qty}주)\n"
                msg += f"▫️ 익절목표(+2.0%): <b>${trap_price:.2f}</b>\n"

            msg += f"\n🚨 <b>[ 작전 수행 현황 ]</b>\n"
            msg += f"▫️ 현재상태: <b>{status_txt}</b>\n"

            if status_code in ["PRE", "REG"]:
                if avwap_qty > 0:
                    keyboard.append([InlineKeyboardButton(f"🧯 {t} 암살자 수동 청산 (0주 락온)", callback_data=f"AVWAP_SET:SYNC_ZERO:{t}")])
                else:
                    if t_h > 0.0 and curr_p > 0.0 and curr_p < t_h:
                        keyboard.append([InlineKeyboardButton(f"🔫 [{t}] 수동 강제 요격 (Manual Fire)", callback_data=f"AVWAP_SET:MANUAL_FIRE_REQ:{t}")])
                    elif t_h > 0.0 and curr_p >= t_h:
                        keyboard.append([InlineKeyboardButton(f"❌ [{t}] 수동 요격 불가 (타점 이탈)", callback_data="AVWAP_SET:REFRESH:NONE")])
                    else:
                        keyboard.append([InlineKeyboardButton(f"❌ [{t}] 수동 요격 불가 (T_H 대기 또는 현재가 스캔 실패)", callback_data="AVWAP_SET:REFRESH:NONE")])
            else:
                keyboard.append([InlineKeyboardButton(f"⛔ [{t}] 장마감 (수동 제어 불가)", callback_data="AVWAP_SET:REFRESH:NONE")])

            toggle_target = "MULTI" if sortie_mode == "SINGLE" else "SINGLE"
            toggle_text = "🔄 무한 출장 모드로 변경" if sortie_mode == "SINGLE" else "🎯 단일 타격 모드로 변경"
            keyboard.append([InlineKeyboardButton(toggle_text, callback_data=f"MODE:AVWAP_SORTIE:{t}:{toggle_target}")])

        keyboard.append([
            InlineKeyboardButton("🔄 관제탑 새로고침", callback_data="AVWAP_SET:REFRESH:NONE"),
            InlineKeyboardButton("🔙 닫기", callback_data="RESET:CANCEL")
        ])

        msg += f"\n\n⏱️ <i>마지막 레이더 스캔: {now_est.strftime('%Y-%m-%d %H:%M:%S')} (EST)</i>\n"

        return msg, InlineKeyboardMarkup(keyboard)
