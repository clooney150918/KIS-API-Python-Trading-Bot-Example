# ==========================================================
# FILE: scheduler_vwap.py
# ==========================================================
# 🚨 MODIFIED: [V-REV 추세장 LOC 스위칭 침묵 버그 및 상태 증발 완벽 수술]
# 🚨 MODIFIED: [V53.06 전투 사령부 외부 통신 10초 타임아웃 및 폴백 방어막 이식]
# 🚨 MODIFIED: [V53.08 들여쓰기(Indentation) 붕괴 런타임 즉사 버그 완벽 수술]
# 🚨 MODIFIED: [Case 30 & Case 26 절대 위반 교정] 갭 하이재킹 실패 시 텔레그램 에러 타전망 팩트 이식 완료
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import asyncio
import traceback
import math
import os
import time
import json
import pandas_market_calendars as mcal
import tempfile
import html  # 🚨 NEW: [Case 26] 이스케이프 쉴드 주입

from scheduler_core import is_market_open, get_budget_allocation

async def scheduled_vwap_init_and_cancel(context):
    if context.job.data.get('tx_lock') is None:
        logging.warning("⚠️ [vwap_init_and_cancel] tx_lock 미초기화. 이번 사이클 스킵.")
        return

    try:
        is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=10.0)
    except asyncio.TimeoutError:
        logging.error("⚠️ 달력 API 타임아웃. 스케줄 증발 방어를 위해 평일 강제 개장(Fail-Open) 처리합니다.")
        est = ZoneInfo('America/New_York')
        is_open = datetime.datetime.now(est).weekday() < 5

    if not is_open:
        return
    
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    
    def _get_market_close():
        nyse = mcal.get_calendar('NYSE')
        return nyse.schedule(start_date=now_est.date(), end_date=now_est.date())

    try:
        schedule = await asyncio.wait_for(asyncio.to_thread(_get_market_close), timeout=10.0)
        if schedule.empty:
            logging.info("💤 [vwap_init] 달력 API 빈 데이터 반환. 금일은 미국 증시 휴장일입니다.")
            return
        else:
            market_close = schedule.iloc[0]['market_close'].astimezone(est)
    except asyncio.TimeoutError:
        logging.error("⚠️ 장마감시간 달력 API 타임아웃. 평일 강제 마감시간(16:00 EST) 세팅.")
        if now_est.weekday() < 5:
            market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        else:
            return
    except Exception as e:
        logging.error(f"⚠️ 장마감시간 달력 API 에러({e}). 평일 강제 마감시간(16:00 EST) 세팅.")
        if now_est.weekday() < 5:
            market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        else:
            return
        
    vwap_start_time = market_close - datetime.timedelta(minutes=34, seconds=0)
    vwap_end_time = market_close 
    
    if not (vwap_start_time <= now_est <= vwap_end_time):
        return
    
    app_data = context.job.data
    cfg, broker, tx_lock = app_data['cfg'], app_data['broker'], app_data['tx_lock']
    chat_id = context.job.chat_id
    
    vwap_cache = app_data.setdefault('vwap_cache', {})
    
    today_str = now_est.strftime('%Y%m%d')
    if vwap_cache.get('date') != today_str:
        vwap_cache.clear()
        vwap_cache['date'] = today_str
        
    async def _do_init():
        async with tx_lock:
            active_tickers = await asyncio.to_thread(cfg.get_active_tickers)
            for t in active_tickers:
                version = await asyncio.to_thread(cfg.get_version, t)
                is_manual_vwap = await asyncio.to_thread(getattr(cfg, 'get_manual_vwap_mode', lambda x: False), t)
                
                if version == "V_REV" or (version == "V14" and is_manual_vwap):
                    if not vwap_cache.get(f"REV_{t}_nuked"):
                        try:
                            msg = f"🌅 <b>[{t}] KIS VWAP/LOC 예약 덫 관측 및 섀도우 오버라이드망 기상</b>\n"
                            msg += f"▫️ 장 마감 34분 전 진입을 확인하여 KIS 서버의 예약 덫 체결을 관망합니다.\n"
                            msg += f"▫️ 기초자산 갭 이탈 감지 시 즉각 개입(Gap Hijack)하는 섀도우 모드로 전환합니다. ⚔️"
            
                            vwap_cache[f"REV_{t}_nuked"] = True
                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', disable_notification=True)
                            await asyncio.sleep(1.0)
                        except Exception as e:
                            logging.error(f"🚨 관측 모드 전환 알림 실패: {e}", exc_info=True)
                            vwap_cache[f"REV_{t}_nuked"] = False 
            
    try:
        await asyncio.wait_for(_do_init(), timeout=45.0)
    except Exception as e:
        logging.error(f"🚨 Fail-Safe 타임아웃 에러: {e}", exc_info=True)


async def scheduled_vwap_trade(context):
    try:
        is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=10.0)
    except asyncio.TimeoutError:
        logging.error("⚠️ 달력 API 타임아웃. 스케줄 증발 방어를 위해 평일 강제 개장(Fail-Open) 처리합니다.")
        est = ZoneInfo('America/New_York')
        is_open = datetime.datetime.now(est).weekday() < 5

    if not is_open:
        return
    
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    
    if context.job.data.get('tx_lock') is None:
        logging.warning("⚠️ [vwap_trade] tx_lock 미초기화. 이번 사이클 스킵.")
        return
        
    def _get_market_close():
        nyse = mcal.get_calendar('NYSE')
        return nyse.schedule(start_date=now_est.date(), end_date=now_est.date())

    try:
        schedule = await asyncio.wait_for(asyncio.to_thread(_get_market_close), timeout=10.0)
        if schedule.empty:
            logging.info("💤 [vwap_trade] 달력 API 빈 데이터 반환. 금일은 미국 증시 휴장일입니다.")
            return
        else:
            market_close = schedule.iloc[0]['market_close'].astimezone(est)
    except asyncio.TimeoutError:
        logging.error("⚠️ 장마감시간 달력 API 타임아웃. 평일 강제 마감시간(16:00 EST) 세팅.")
        if now_est.weekday() < 5:
            market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        else:
            return
    except Exception as e:
        logging.error(f"⚠️ 장마감시간 달력 API 에러({e}). 평일 강제 마감시간(16:00 EST) 세팅.")
        if now_est.weekday() < 5:
            market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        else:
            return
         
    vwap_start_time = market_close - datetime.timedelta(minutes=34, seconds=0)
    vwap_end_time = market_close 
    
    if not (vwap_start_time <= now_est <= vwap_end_time):
        return

    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    queue_ledger = app_data.get('queue_ledger')
    chat_id = context.job.chat_id
    base_map = app_data.get('base_map', {'SOXL': 'SOXX', 'TQQQ': 'QQQ'})
    
    vwap_cache = app_data.setdefault('vwap_cache', {})
    today_str = now_est.strftime('%Y%m%d')
    
    if vwap_cache.get('date') != today_str:
        vwap_cache.clear()
        vwap_cache['date'] = today_str

    async def _do_vwap():
        async with tx_lock:
            cash, holdings = await asyncio.to_thread(broker.get_account_balance)
            if holdings is None: return
            
            active_tickers = await asyncio.to_thread(cfg.get_active_tickers)
            _, allocated_cash = await asyncio.to_thread(get_budget_allocation, cash, active_tickers, cfg)
            
            base_curr_p = 0.0
            ask_price = 0.0
            exec_price = 0.0
            buy_qty = 0
            nuked_count = 0
            
            for t in active_tickers:
                version = await asyncio.to_thread(cfg.get_version, t)
                is_manual_vwap = await asyncio.to_thread(getattr(cfg, 'get_manual_vwap_mode', lambda x: False), t)

                if version == "V_REV" or (version == "V14" and is_manual_vwap):
                    if version == "V_REV":
                        if vwap_cache.get(f"REV_{t}_gap_hijack_fired"):
                            continue
                          
                        base_tkr = base_map.get(t, 'SOXX')
                    
                        try:
                            base_curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, base_tkr), timeout=10.0)
                            base_curr_p = float(base_curr_p_val or 0.0)
                        except Exception:
                            base_curr_p = 0.0
                              
                        try:
                            df_1min_base = await asyncio.wait_for(asyncio.to_thread(broker.get_1min_candles_df, base_tkr), timeout=10.0)
                            if df_1min_base is not None and not df_1min_base.empty:
                                df_b = df_1min_base.copy()
                                if 'time_est' in df_b.columns:
                                    df_b = df_b[(df_b['time_est'] >= '093000') & (df_b['time_est'] <= '155900')]
                                 
                                if not df_b.empty:
                                    df_b['tp'] = (df_b['high'].astype(float) + df_b['low'].astype(float) + df_b['close'].astype(float)) / 3.0
                                    df_b['vol'] = df_b['volume'].astype(float)
                                    df_b['vol_tp'] = df_b['tp'] * df_b['vol']
                                   
                                    c_vol = df_b['vol'].sum()
                                    base_vwap = df_b['vol_tp'].sum() / c_vol if c_vol > 0 else base_curr_p
            
                                    gap_pct = ((base_curr_p - base_vwap) / base_vwap * 100.0) if base_vwap > 0 else 0.0
                                    gap_thresh = await asyncio.to_thread(getattr(cfg, 'get_vrev_gap_threshold', lambda x: -0.67), t)
                                     
                                    if gap_pct <= gap_thresh:
                                        logging.info(f"⚡ [{t}] Gap Hijack Triggered! gap: {gap_pct:.2f}%, thresh: {gap_thresh}%")
                                        
                                        nuked_count = 0
                                        try:
                                            est_now = datetime.datetime.now(ZoneInfo('America/New_York'))
                                            d_str = est_now.strftime('%Y%m%d')
                                            resv_orders = await asyncio.to_thread(broker.get_reservation_orders, t, d_str, d_str)
                                            for req in resv_orders:
                                                odno = req.get('ovrs_rsvn_odno') or req.get('odno')
                                                ord_dt = req.get('rsvn_ord_rcit_dt') or req.get('ord_dt', d_str)
                                                if odno:
                                                    try:
                                                        await asyncio.to_thread(broker.cancel_reservation_order, ord_dt, odno)
                                                        nuked_count += 1
                                                    except Exception as e:
                                                        logging.error(f"🚨 [{t}] 예약 덫 취소 실패: {e}")
                                             
                                            unfilled = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                            if isinstance(unfilled, list):
                                                for uo in unfilled:
                                                    dvsn = str(uo.get('ord_dvsn_cd') or uo.get('ord_dvsn') or '').strip().zfill(2)
                                                    if dvsn in ['36', '00']:
                                                        u_odno = uo.get('odno')
                                                        if u_odno:
                                                            try:
                                                                await asyncio.to_thread(broker.cancel_order, t, u_odno)
                                                                nuked_count += 1
                                                            except Exception as e:
                                                                logging.error(f"🚨 [{t}] 일반 덫(VWAP/LOC) 취소 실패: {e}")
                                            
                                            logging.info(f"⚡ [{t}] KIS 실원장 스캔: 예약 및 일반 덫 {nuked_count}건 팩트 파기 완료.")
                                        except Exception as e:
                                            logging.error(f"🚨 [{t}] KIS 실원장 덫 스캔 에러: {e}")
                                        
                                        await asyncio.sleep(2.0)
                                        
                                        seed = await asyncio.to_thread(cfg.get_seed, t)
                                        daily_limit = float(seed or 0.0) * 0.15
                                        
                                        alloc_cash = allocated_cash.get(t, 0.0)
                                        safe_alloc_cash = min(float(alloc_cash), daily_limit) if daily_limit > 0 else float(alloc_cash)
                                     
                                        total_spent = 0.0
                                        if hasattr(strategy, 'v_rev_plugin'):
                                            total_spent = float(strategy.v_rev_plugin.executed.get("BUY_BUDGET", {}).get(t, 0.0))
                                      
                                        rem_budget = max(0.0, safe_alloc_cash - total_spent)
                                         
                                        try:
                                            ask_price_val = await asyncio.wait_for(asyncio.to_thread(broker.get_ask_price, t), timeout=10.0)
                                            ask_price = float(ask_price_val or 0.0)
                                        except Exception:
                                            ask_price = 0.0
                                            
                                        try:
                                            curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, t), timeout=10.0)
                                            curr_p = float(curr_p_val or 0.0)
                                        except Exception:
                                            curr_p = 0.0
                                            
                                        exec_price = ask_price if ask_price > 0 else curr_p
                                        buy_qty = int(math.floor(rem_budget / exec_price)) if exec_price > 0 else 0
                                        
                                        if buy_qty > 0:
                                            res = await asyncio.to_thread(broker.send_order, t, "BUY", buy_qty, exec_price, "LIMIT")
                                            odno = res.get('odno', '') if isinstance(res, dict) else ''

                                            if res and res.get('rt_cd') == '0' and odno:
                                                vwap_cache[f"REV_{t}_gap_hijack_fired"] = True
                                                msg = f"⚡ <b>[{t}] 🤖 모멘텀 자율주행 (Gap Hijack) 섀도우 오버라이드 격발!</b>\n"
                                                msg += f"▫️ 기초자산({base_tkr}) VWAP 이탈률(<b>{gap_pct:+.2f}%</b>)이 임계치(<b>{gap_thresh}%</b>)를 하향 돌파했습니다.\n"
                                                msg += f"▫️ KIS 예약 덫({nuked_count}건)을 즉각 파기(Nuke)하고, 잔여 예산 100%를 매도 1호가로 일괄 스윕(Sweep) 타격했습니다!\n"
                                                msg += f"▫️ 스윕 수량: <b>{buy_qty}주</b> (단가: ${exec_price:.2f})"
                                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                                
                                                if hasattr(strategy, 'v_rev_plugin'):
                                                    await asyncio.to_thread(strategy.v_rev_plugin.record_execution, t, "BUY", buy_qty, exec_price)
                                                if queue_ledger:
                                                    await asyncio.to_thread(queue_ledger.add_lot, t, buy_qty, exec_price, "GAP_HIJACK_BUY")
                                            else:
                                                # 🚨 MODIFIED: [Case 30 & Case 26 절대 위반 교정] 갭 하이재킹 실패 시 텔레그램 에러 타전망 팩트 이식 완료
                                                err_msg = html.escape(res.get('msg1', '응답 없음') if isinstance(res, dict) else '통신 장애')
                                                logging.error(f"🚨 [{t}] V-REV 갭 하이재킹 KIS 서버 거절: {err_msg}")
                                                reject_msg = (
                                                    f"🚨 <b>[{t}] V-REV 갭 하이재킹 스윕(Sweep) 서버 거절 (Reject)!</b>\n"
                                                    f"▫️ 사유: <code>{err_msg}</code>\n"
                                                    f"▫️ 조치: 다음 스캔 시 재시도합니다."
                                                )
                                                await context.bot.send_message(chat_id=chat_id, text=reject_msg, parse_mode='HTML')
                                      
                        except Exception as e:
                            logging.error(f"🚨 갭 스위칭 스캔 에러: {e}")

        try:
            await asyncio.wait_for(_do_vwap(), timeout=90.0)
        except Exception as e:
            logging.error(f"🚨 VWAP 섀도우 오버라이드 스케줄러 에러: {e}", exc_info=True)
