# ==========================================================
# FILE: scheduler_sniper.py
# ==========================================================
# 🚨 MODIFIED: [Case 24] 관제탑 렌더링 무결성 사수 (is_regular_session 종속 제거 및 정규장 팩트 분리 필터링)
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import asyncio
import traceback
import math
import os
import json
import glob
import tempfile
import html  
import yfinance as yf
import pandas_market_calendars as mcal
import time 

from scheduler_core import is_market_open

async def scheduled_sniper_monitor(context):
    is_open = False
    for attempt in range(3):
        try:
            is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=10.0)
            break
        except asyncio.TimeoutError:
            if attempt == 2:
                logging.error("⚠️ 달력 API 타임아웃. 스케줄 증발 방어를 위해 평일 강제 개장(Fail-Open) 처리합니다.")
                est = ZoneInfo('America/New_York')
                is_open = datetime.datetime.now(est).weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))
        except Exception:
            if attempt == 2:
                est = ZoneInfo('America/New_York')
                is_open = datetime.datetime.now(est).weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))

    if not is_open: return
    
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)

    if context.job.data.get('tx_lock') is None:
        logging.warning("⚠️ [sniper_monitor] tx_lock 미초기화. 이번 사이클 스킵.")
        return
    
    def _get_market_hours():
        time.sleep(0.06)
        nyse = mcal.get_calendar('NYSE')
        return nyse.schedule(start_date=now_est.date(), end_date=now_est.date())

    schedule = None
    for attempt in range(3):
        try:
            schedule = await asyncio.wait_for(asyncio.to_thread(_get_market_hours), timeout=10.0)
            break
        except asyncio.TimeoutError:
            if attempt == 2: logging.error("⚠️ 장운영시간 달력 API 타임아웃. 평일 강제 시간 세팅.")
            else: await asyncio.sleep(1.0 * (2 ** attempt))
        except Exception:
            if attempt == 2: pass
            else: await asyncio.sleep(1.0 * (2 ** attempt))
            
    if schedule is not None and not schedule.empty:
        market_open = schedule.iloc[0]['market_open'].astimezone(est)
        market_close = schedule.iloc[0]['market_close'].astimezone(est)
    elif schedule is not None and schedule.empty:
        logging.info("💤 [sniper_monitor] 달력 API 빈 데이터 반환. 금일은 미국 증시 휴장일입니다.")
        return
    else:
        if now_est.weekday() < 5:
            market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        else: return
   
    pre_start = market_open - datetime.timedelta(hours=5, minutes=30)
    start_monitor = pre_start 
    end_monitor = market_close - datetime.timedelta(minutes=1)
    
    if not (start_monitor <= now_est <= end_monitor): return

    is_regular_session = market_open <= now_est <= market_close
    
    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    chat_id = context.job.chat_id
    base_map = app_data.get('base_map', {'SOXL': 'SOXX', 'TQQQ': 'QQQ'})
    
    tracking_cache = app_data.setdefault('sniper_tracking', {})
    today_est_str = now_est.strftime('%Y%m%d')
    if tracking_cache.get('date') != today_est_str:
        tracking_cache.clear()
        tracking_cache['date'] = today_est_str
        def _clean_sniper_caches():
            try:
                for _f in glob.glob("data/sniper_cache_*.json"):
                    try: os.remove(_f)
                    except OSError: pass
            except Exception: pass
        await asyncio.to_thread(_clean_sniper_caches)
               
    async def _do_sniper():
        async with tx_lock:
            cash, holdings = 0.0, None
            for attempt in range(3):
                try:
                    cash, holdings = await asyncio.wait_for(asyncio.to_thread(broker.get_account_balance), timeout=15.0)
                    break
                except asyncio.TimeoutError:
                    if attempt == 2: logging.warning("⚠️ 잔고 조회 타임아웃 (15초). 폴백 적용.")
                    else: await asyncio.sleep(1.0 * (2 ** attempt))
                except Exception:
                    if attempt == 2: pass
                    else: await asyncio.sleep(1.0 * (2 ** attempt))
            
            if holdings is None: return
            
            safe_holdings = holdings if isinstance(holdings, dict) else {}
            avwap_free_cash = max(0.0, float(cash))
            
            active_tickers = await asyncio.to_thread(cfg.get_active_tickers)
            for t in active_tickers:
                await asyncio.sleep(0.06) 
                
                version = await asyncio.to_thread(cfg.get_version, t)

                if version == "V_REV":
                    h = safe_holdings.get(t) or {}
                    actual_qty = int(float(h.get('qty', 0)))
                    q_ledger = app_data.get('queue_ledger')
                    if q_ledger:
                        q_data = await asyncio.to_thread(q_ledger.get_queue, t)
                        total_q = sum(item.get("qty", 0) for item in q_data)

                        if actual_qty == 0 and total_q > 0:
                            _vwap_cache_ref = app_data.get('vwap_cache', {})
                            if _vwap_cache_ref.get(f"REV_{t}_sweep_msg_sent"): continue
                            
                            dump_jitter_sec = tracking_cache.get(f"AVWAP_DUMP_JITTER_{t}", 0)
                            base_dump_dt = datetime.datetime.combine(now_est.date(), datetime.time(15, 20)).replace(tzinfo=ZoneInfo('America/New_York'))
                            dynamic_dump_dt = base_dump_dt - datetime.timedelta(seconds=dump_jitter_sec)
                            if dynamic_dump_dt.time() <= now_est.time() <= datetime.time(16, 0): continue

                            if not tracking_cache.get(f"REV_{t}_panic_sell_warn"):
                                tracking_cache[f"REV_{t}_panic_sell_warn"] = True
                                await context.bot.send_message(
                                    chat_id=chat_id,
                                    text=f"🚨 <b>[비상] [{t}] 수동매매로 인한 잔고 증발이 감지되었습니다.</b>\n▫️ 봇의 매매가 일시 정지됩니다.\n▫️ 시드 오염을 막기 위해 즉시 <code>/reset</code> 커맨드를 실행하여 장부를 소각하십시오.",
                                    parse_mode='HTML'
                                 )
                    continue
                
                if version == "V_REV" and await asyncio.to_thread(getattr(cfg, 'get_avwap_hybrid_mode', lambda x: False), t):
                    if not tracking_cache.get(f"AVWAP_INIT_{t}"):
                        try:
                            saved_state = await asyncio.to_thread(strategy.v_avwap_plugin.load_state, t, now_est)
                            if saved_state:
                                tracking_cache[f"AVWAP_BOUGHT_{t}"] = saved_state.get('bought', False)
                                tracking_cache[f"AVWAP_SHUTDOWN_{t}"] = saved_state.get('shutdown', False)
                                tracking_cache[f"AVWAP_EXECUTED_BUY_{t}"] = saved_state.get('executed_buy', False)
                                tracking_cache[f"AVWAP_QTY_{t}"] = saved_state.get('qty', 0)
                                tracking_cache[f"AVWAP_AVG_{t}"] = saved_state.get('avg_price', 0.0)
                                tracking_cache[f"AVWAP_STRIKES_{t}"] = saved_state.get('strikes', 0)
                                tracking_cache[f"AVWAP_DAILY_BOUGHT_{t}"] = saved_state.get('daily_bought_qty', 0)
                                tracking_cache[f"AVWAP_DAILY_SOLD_{t}"] = saved_state.get('daily_sold_qty', 0)
                                tracking_cache[f"AVWAP_TRAP_ODNO_{t}"] = saved_state.get('trap_odno', "")
                                tracking_cache[f"AVWAP_PM_H_{t}"] = saved_state.get('PM_H', 0.0)
                                tracking_cache[f"AVWAP_PM_L_{t}"] = saved_state.get('PM_L', 0.0)
                                tracking_cache[f"AVWAP_T_H_{t}"] = saved_state.get('T_H', 0.0)
                                tracking_cache[f"AVWAP_T_L_{t}"] = saved_state.get('T_L', 0.0)
                                tracking_cache[f"AVWAP_OFFSET_{t}"] = saved_state.get('offset', 0.0)
                                tracking_cache[f"AVWAP_DUMP_JITTER_{t}"] = saved_state.get('dump_jitter_sec', 0)
                                tracking_cache[f"AVWAP_LIMIT_ORDER_PLACED_{t}"] = saved_state.get('limit_order_placed', False)
                                tracking_cache[f"AVWAP_PLACED_TARGET_TH_{t}"] = saved_state.get('placed_target_th', 0.0)
                                tracking_cache[f"AVWAP_BUY_ODNO_{t}"] = saved_state.get('buy_odno', "")
                                tracking_cache[f"AVWAP_TRAP_PLACED_TIME_{t}"] = saved_state.get('trap_placed_time', "")
                        except Exception: pass
                        tracking_cache[f"AVWAP_INIT_{t}"] = True
                    
                    if tracking_cache.get(f"AVWAP_SHUTDOWN_{t}"): continue
             
                    target_base = base_map.get(t, t) 
                    ctx_data = tracking_cache.get(f"AVWAP_CTX_{t}")
                    if not ctx_data:
                        for attempt in range(3):
                            try:
                                ctx_data = await asyncio.wait_for(asyncio.to_thread(strategy.v_avwap_plugin.fetch_macro_context, target_base), timeout=15.0)
                                if ctx_data: 
                                    tracking_cache[f"AVWAP_CTX_{t}"] = ctx_data
                                    break
                            except Exception:
                                if attempt == 2: pass
                                else: await asyncio.sleep(1.0 * (2 ** attempt))
                         
                    if not ctx_data: continue 
     
                    avwap_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
                    avwap_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)
        
                    exec_curr_p, base_curr_p = 0.0, 0.0
                    for attempt in range(3):
                        try:
                            exec_curr_p = float(await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, t), timeout=15.0) or 0.0)
                            base_curr_p = float(await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, target_base), timeout=15.0) or 0.0)
                            break
                        except Exception:
                            if attempt == 2: pass
                            else: await asyncio.sleep(1.0 * (2 ** attempt))
                          
                    if exec_curr_p <= 0 or base_curr_p <= 0: continue
                    
                    if not tracking_cache.get(f"AVWAP_DAY_OPEN_{target_base}"):
                        def _fetch_open(tkr):
                            try:
                                time.sleep(0.06)
                                st = yf.Ticker(tkr)
                                h = st.history(period="1d", interval="1m", prepost=False, timeout=5)
                                if not h.empty: return float(h['Open'].dropna().iloc[0])
                            except: pass
                            return 0.0
             
                        for attempt in range(3):
                            try:
                                fetched_open = float(await asyncio.wait_for(asyncio.to_thread(_fetch_open, target_base), timeout=15.0) or 0.0)
                                if fetched_open > 0: 
                                    tracking_cache[f"AVWAP_DAY_OPEN_{target_base}"] = fetched_open
                                    break
                            except Exception: 
                                if attempt == 2: pass
                                else: await asyncio.sleep(1.0 * (2 ** attempt))
  
                    base_day_open = tracking_cache.get(f"AVWAP_DAY_OPEN_{target_base}", 0.0)
                    prev_c, day_high, day_low, amp5, base_day_high, base_day_low, ma_5day = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
                    df_1min_t, df_1min_base = None, None
               
                    for attempt in range(3):
                        try:
                            res_batch = await asyncio.wait_for(
                                asyncio.gather(
                                    asyncio.to_thread(broker.get_previous_close, t),
                                    asyncio.to_thread(broker.get_amp_5d_data, t),
                                    asyncio.to_thread(broker.get_1min_candles_df, t),
                                    asyncio.to_thread(broker.get_1min_candles_df, target_base), 
                                    asyncio.to_thread(broker.get_5day_ma, t),
                                    return_exceptions=True
                                ), timeout=15.0
                            )
                            prev_c = float(res_batch[0]) if not isinstance(res_batch[0], Exception) and res_batch[0] else 0.0
                            amp5 = float(res_batch[1]) if not isinstance(res_batch[1], Exception) and res_batch[1] else 0.0
                            df_1min_t = res_batch[2] if not isinstance(res_batch[2], Exception) else None
                            df_1min_base = res_batch[3] if not isinstance(res_batch[3], Exception) else None
                            ma_5day = float(res_batch[4]) if len(res_batch) > 4 and not isinstance(res_batch[4], Exception) and res_batch[4] else 0.0
                  
                            # 🚨 MODIFIED: [Case 24] 관제탑 렌더링 무결성 사수 (is_regular_session 종속성 제거 및 순수 타임라인 필터링 락온)
                            if df_1min_t is not None and not df_1min_t.empty:
                                df_t_copy = df_1min_t.copy()
                                if 'time_est' in df_t_copy.columns:
                                    df_t_reg = df_t_copy[(df_t_copy['time_est'] >= '093000') & (df_t_copy['time_est'] <= '155959')]
                                    if not df_t_reg.empty:
                                        day_high = float(df_t_reg['high'].astype(float).max())
                                        day_low = float(df_t_reg['low'].astype(float).min())
                                        tracking_cache[f"AVWAP_REG_H_{t}"] = day_high
                                        tracking_cache[f"AVWAP_REG_L_{t}"] = day_low
                
                            if df_1min_base is not None and not df_1min_base.empty:
                                df_b_copy = df_1min_base.copy()
                                if 'time_est' in df_b_copy.columns:
                                    df_b_reg = df_b_copy[(df_b_copy['time_est'] >= '093000') & (df_b_copy['time_est'] <= '155959')]
                                    if not df_b_reg.empty:
                                         base_day_high = float(df_b_reg['high'].astype(float).max())
                                         base_day_low = float(df_b_reg['low'].astype(float).min())
                            break
                        except Exception: 
                            if attempt == 2: pass
                            else: await asyncio.sleep(1.0 * (2 ** attempt))
     
                    avwap_state_dict = {
                        "strikes": tracking_cache.get(f"AVWAP_STRIKES_{t}", 0),
                        "shutdown": tracking_cache.get(f"AVWAP_SHUTDOWN_{t}", False),
                        "executed_buy": tracking_cache.get(f"AVWAP_EXECUTED_BUY_{t}", False),
                        "qty": tracking_cache.get(f"AVWAP_QTY_{t}", 0),
                        "avg_price": tracking_cache.get(f"AVWAP_AVG_{t}", 0.0),
                        "bought": tracking_cache.get(f"AVWAP_BOUGHT_{t}", False),
                        "daily_bought_qty": tracking_cache.get(f"AVWAP_DAILY_BOUGHT_{t}", 0),
                        "daily_sold_qty": tracking_cache.get(f"AVWAP_DAILY_SOLD_{t}", 0),
                        "trap_odno": tracking_cache.get(f"AVWAP_TRAP_ODNO_{t}", ""),
                        "buy_odno": tracking_cache.get(f"AVWAP_BUY_ODNO_{t}", ""),
                        "PM_H": tracking_cache.get(f"AVWAP_PM_H_{t}", 0.0),
                        "PM_L": tracking_cache.get(f"AVWAP_PM_L_{t}", 0.0),
                        "T_H": tracking_cache.get(f"AVWAP_T_H_{t}", 0.0),
                        "T_L": tracking_cache.get(f"AVWAP_T_L_{t}", 0.0),
                        "offset": tracking_cache.get(f"AVWAP_OFFSET_{t}", 0.0),
                        "limit_order_placed": tracking_cache.get(f"AVWAP_LIMIT_ORDER_PLACED_{t}", False),
                        "placed_target_th": tracking_cache.get(f"AVWAP_PLACED_TARGET_TH_{t}", 0.0),
                        "dump_jitter_sec": tracking_cache.get(f"AVWAP_DUMP_JITTER_{t}", 0),
                        "trap_placed_time": tracking_cache.get(f"AVWAP_TRAP_PLACED_TIME_{t}", "")
                    }
                    
                    sortie_mode = await asyncio.to_thread(getattr(cfg, 'get_avwap_sortie_mode', lambda x: "SINGLE"), t)
             
                    decision = await asyncio.to_thread(
                        strategy.get_avwap_decision,
                        base_ticker=target_base, exec_ticker=t, base_curr_p=base_curr_p,
                        exec_curr_p=exec_curr_p, base_day_open=base_day_open, avg_price=avwap_avg,
                        qty=avwap_qty, alloc_cash=avwap_free_cash, context_data=ctx_data,
                        df_1min_base=df_1min_base, df_1min_exec=df_1min_t, now_est=now_est, avwap_state=avwap_state_dict,
                        regime_data=None, prev_close=prev_c, ma_5day=ma_5day, day_high=day_high, day_low=day_low, amp5=amp5,
                        base_day_high=base_day_high, base_day_low=base_day_low,
                        is_simulation=False,
                        sortie_mode=sortie_mode
                    )
         
                    action = decision.get("action")
                    reason = decision.get("reason", "")
                    
                    tracking_cache[f"AVWAP_PM_H_{t}"] = decision.get("PM_H", tracking_cache.get(f"AVWAP_PM_H_{t}", 0.0))
                    tracking_cache[f"AVWAP_PM_L_{t}"] = decision.get("PM_L", tracking_cache.get(f"AVWAP_PM_L_{t}", 0.0))
                    tracking_cache[f"AVWAP_T_H_{t}"] = decision.get("T_H", tracking_cache.get(f"AVWAP_T_H_{t}", 0.0))
                    tracking_cache[f"AVWAP_T_L_{t}"] = decision.get("T_L", tracking_cache.get(f"AVWAP_T_L_{t}", 0.0))
                    tracking_cache[f"AVWAP_OFFSET_{t}"] = decision.get("offset", tracking_cache.get(f"AVWAP_OFFSET_{t}", 0.0))
                    
                    if decision.get("limit_order_placed") is not None:
                        tracking_cache[f"AVWAP_LIMIT_ORDER_PLACED_{t}"] = decision.get("limit_order_placed")
                    if decision.get("placed_target_th") is not None:
                         tracking_cache[f"AVWAP_PLACED_TARGET_TH_{t}"] = decision.get("placed_target_th")
                    if decision.get("trap_placed_time") is not None:
                         tracking_cache[f"AVWAP_TRAP_PLACED_TIME_{t}"] = decision.get("trap_placed_time")

                    if action == "PLACE_TRAP":
                        price = float(decision.get("target_price", 0.0))
                        qty = int(decision.get("qty", 0))
       
                        if qty > 0 and price > 0:
                            exec_price = price
                            res = await asyncio.to_thread(broker.send_order, t, "BUY", qty, exec_price, "LIMIT")
                            odno = res.get('odno', '') if isinstance(res, dict) else ''
      
                            if res and res.get('rt_cd') == '0' and odno:
                                tracking_cache[f"AVWAP_BUY_ODNO_{t}"] = odno
                                msg = f"🎯 <b>[AVWAP] Dawn Sniper 순수 지정가 덫 선제 장전 절대 락온!</b>\n▫️ 타겟: {t}\n▫️ 고정 덫 단가: ${exec_price:.2f}\n▫️ 목표 수량: {qty}주\n▫️ 사유: {reason}"
                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                
                                state_data = avwap_state_dict.copy()
                                state_data['limit_order_placed'] = True
                                state_data['placed_target_th'] = exec_price
                                state_data['buy_odno'] = odno
                                state_data['trap_placed_time'] = decision.get("trap_placed_time", "")
                                await asyncio.to_thread(strategy.v_avwap_plugin.save_state, t, now_est, state_data)
                            else:
                                err_msg = html.escape(res.get('msg1', '응답 없음') if isinstance(res, dict) else '통신 장애')
                                logging.error(f"🚨 [{t}] AVWAP 덫 장전 KIS 서버 거절: {err_msg}")
                                reject_msg = (
                                    f"🚨 <b>[{t}] Dawn Sniper 덫 장전 서버 거절 (Reject)!</b>\n"
                                    f"▫️ 사유: <code>{err_msg}</code>\n"
                                    f"▫️ 조치: 다음 1분 사이클에서 재장전을 시도합니다."
                                )
                                tracking_cache[f"AVWAP_LIMIT_ORDER_PLACED_{t}"] = False
                                tracking_cache[f"AVWAP_PLACED_TARGET_TH_{t}"] = 0.0
                                tracking_cache[f"AVWAP_TRAP_PLACED_TIME_{t}"] = ""
                                await context.bot.send_message(chat_id=chat_id, text=reject_msg, parse_mode='HTML')

                    elif action == "VERIFY_TRAP_FILL":
                        price = float(decision.get("target_price", 0.0))
                        buy_odno = tracking_cache.get(f"AVWAP_BUY_ODNO_{t}", "")
                        
                        if buy_odno:
                            ccld_qty = 0
                            qty = int(math.floor((avwap_free_cash * 0.95) / price)) if price > 0 else 0
                            
                            for _ in range(4):
                                await asyncio.sleep(2.0)
                                unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []
                                
                                my_order = next((ox for ox in safe_unfilled if ox.get('odno') == buy_odno), None)
                                if my_order:
                                    ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                    if ccld_qty >= qty: break
                                else:
                                    ccld_qty = qty
                                    break
                                    
                            if ccld_qty > 0:
                                if ccld_qty < qty:
                                    try:
                                        await asyncio.to_thread(broker.cancel_order, t, buy_odno)
                                        await asyncio.sleep(0.5)
                                    except: pass
                                    
                                avwap_free_cash -= (ccld_qty * price)
                                old_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
                                old_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)
            
                                new_qty = old_qty + ccld_qty
                                new_avg = ((old_qty * old_avg) + (ccld_qty * price)) / new_qty if new_qty > 0 else 0.0
                            
                                msg = f"⚔️ <b>[AVWAP] Dawn Sniper 덫 체결 명중!</b>\n▫️ 타겟: {t}\n▫️ 타점: ${price:.2f}\n▫️ 팩트 체결수량: {ccld_qty}주 (목표 {qty}주)\n▫️ 사유: {reason}"
                                if ccld_qty < qty: msg += f"\n▫️ 미체결 {qty - ccld_qty}주는 안전을 위해 즉각 취소(Nuke)되었습니다."
                                
                                tracking_cache[f"AVWAP_BOUGHT_{t}"] = True
                                tracking_cache[f"AVWAP_SHUTDOWN_{t}"] = False
                                tracking_cache[f"AVWAP_EXECUTED_BUY_{t}"] = True
                                tracking_cache[f"AVWAP_QTY_{t}"] = new_qty
                                tracking_cache[f"AVWAP_AVG_{t}"] = round(new_avg, 4)
                                tracking_cache[f"AVWAP_TRAP_PLACED_TIME_{t}"] = "" 
                        
                                daily_b = tracking_cache.get(f"AVWAP_DAILY_BOUGHT_{t}", 0) + ccld_qty
                                tracking_cache[f"AVWAP_DAILY_BOUGHT_{t}"] = daily_b
                     
                                trap_price = round(new_avg * 1.02, 2)
                                
                                await asyncio.sleep(1.0)
                                
                                trap_res = await asyncio.to_thread(broker.send_order, t, "SELL", ccld_qty, trap_price, "LIMIT")
                                trap_odno = trap_res.get('odno', '') if isinstance(trap_res, dict) else ''
                                
                                if trap_res and trap_res.get('rt_cd') == '0' and trap_odno:
                                    tracking_cache[f"AVWAP_TRAP_ODNO_{t}"] = trap_odno
                                    msg += f"\n\n🎯 <b>[투트랙 엑시트 장전]</b>\n▫️ +2.0% 수익 타점(<b>${trap_price:.2f}</b>)에 익절 덫을 즉시 자동 장전했습니다."
                                else:
                                    trap_err = html.escape(trap_res.get('msg1', '오류') if isinstance(trap_res, dict) else '통신 장애')
                                    msg += f"\n\n⚠️ <b>[익절 덫 장전 실패]</b> KIS 서버 거절: {trap_err}"
                                
                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                 
                                state_data = avwap_state_dict.copy()
                                state_data.update({
                                    "bought": True, "shutdown": False, "executed_buy": True,
                                    "qty": new_qty, "avg_price": round(new_avg, 4), "daily_bought_qty": daily_b,
                                    "trap_odno": trap_odno,
                                    "trap_placed_time": ""
                                })
                                await asyncio.to_thread(strategy.v_avwap_plugin.save_state, t, now_est, state_data)

                    elif action == "TRAP_WAIT":
                        pass
                        
                    elif action == "SELL":
                        price = float(decision.get("target_price", decision.get("price", 0.0)))
                        qty = int(decision.get("qty", 0))
                        
                        if qty > 0:
                            trap_filled_qty = 0
                            ccld_qty = 0
                            exec_price = 0.0
                            total_sold = 0
                            
                            trap_odno = tracking_cache.get(f"AVWAP_TRAP_ODNO_{t}", "")
                            if trap_odno:
                                try:
                                    await asyncio.to_thread(broker.cancel_order, t, trap_odno)
                                    await asyncio.sleep(1.0)
                                except Exception as e_cancel: pass
                                
                            today_est_str = now_est.strftime('%Y%m%d')
                            exec_hist = await asyncio.to_thread(broker.get_execution_history, t, today_est_str, today_est_str)
                            
                            if trap_odno and isinstance(exec_hist, list):
                                trap_filled_qty = sum(int(float(ex.get('ft_ccld_qty', '0'))) for ex in exec_hist if ex.get('odno') == trap_odno)
                                
                            remaining_qty = max(0, qty - trap_filled_qty)

                            if remaining_qty > 0:
                                bid_price = 0.0
                                for attempt in range(3):
                                    try:
                                        bid_price_val = await asyncio.wait_for(asyncio.to_thread(broker.get_bid_price, t), timeout=10.0)
                                        bid_price = float(bid_price_val or 0.0)
                                        break
                                    except Exception: 
                                        if attempt == 2: bid_price = 0.0
                                        else: await asyncio.sleep(1.0 * (2**attempt))
                
                                fallback_price = price if price > 0.0 else exec_curr_p
                                exec_price = bid_price if bid_price > 0 else fallback_price
    
                                res = await asyncio.to_thread(broker.send_order, t, "SELL", remaining_qty, exec_price, "LIMIT")
                                odno = res.get('odno', '') if isinstance(res, dict) else ''
            
                                if res and res.get('rt_cd') == '0' and odno:
                                    for _ in range(4):
                                        await asyncio.sleep(2.0)
                                        unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                        safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []
                                        
                                        my_order = next((ox for ox in safe_unfilled if ox.get('odno') == odno), None)
                                        if my_order:
                                            ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                            if ccld_qty >= remaining_qty: break
                                        else:
                                            ccld_qty = remaining_qty
                                            break
       
                                    if ccld_qty < remaining_qty:
                                        try:
                                            await asyncio.to_thread(broker.cancel_order, t, odno)
                                            await asyncio.sleep(0.5)
                                        except Exception: pass
                                else:
                                    err_msg = html.escape(res.get('msg1', '응답 없음') if isinstance(res, dict) else '통신 장애')
                                    logging.error(f"🚨 [{t}] AVWAP 암살자 덤핑 KIS 서버 거절: {err_msg}")
                                    reject_msg = (
                                        f"🚨 <b>[{t}] AVWAP 암살자 덤핑 서버 거절 (Reject)!</b>\n"
                                        f"▫️ 엔진이 매도를 격발했으나 KIS 서버에서 주문을 거부했습니다.\n"
                                        f"▫️ 사유: <code>{err_msg}</code>\n"
                                        f"▫️ 조치: 다음 1분 사이클에서 재타격을 시도합니다."
                                    )
                                    await context.bot.send_message(chat_id=chat_id, text=reject_msg, parse_mode='HTML')

                            total_sold = trap_filled_qty + ccld_qty
                            
                            if total_sold > 0:
                                msg = f"⚔️ <b>[AVWAP] Dawn Sniper 엑시트 청산 성공!</b>\n▫️ 타겟: {t}\n▫️ 팩트 체결수량: {total_sold}주 (목표 {qty}주)\n▫️ 사유: {reason}"
                                if ccld_qty < remaining_qty: msg += f"\n⚠️ 잔량 {remaining_qty - ccld_qty}주 미체결 강제 취소됨."
                                
                                old_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
                                new_qty = max(0, old_qty - total_sold)
                                shutdown_flag = tracking_cache.get(f"AVWAP_SHUTDOWN_{t}", False)
                                
                                if new_qty == 0:
                                    strikes = tracking_cache.get(f"AVWAP_STRIKES_{t}", 0) + 1
                                    tracking_cache[f"AVWAP_STRIKES_{t}"] = strikes
    
                                    dump_jitter_sec = tracking_cache.get(f"AVWAP_DUMP_JITTER_{t}", 0)
                                    base_dump_dt = datetime.datetime.combine(now_est.date(), datetime.time(15, 20)).replace(tzinfo=ZoneInfo('America/New_York'))
                                    dynamic_dump_dt = base_dump_dt - datetime.timedelta(seconds=dump_jitter_sec)
                                    dynamic_dump_str = dynamic_dump_dt.strftime("%H:%M:%S")
     
                                    if "덤핑" in reason or "EMERGENCY" in reason:
                                        msg += f"\n🛡️ <b>{dynamic_dump_str} (Jitter 적용) 타임스탑 도달 전량 덤핑 완료.</b> 암살자 작전을 <b>영구 동결(Shutdown)</b>합니다."
                                        shutdown_flag = True
                                    else:
                                        if sortie_mode == "MULTI":
                                            msg += f"\n🔄 익절 청산 완료. <b>[다중 출격(Multi-Sortie)]</b> 모드가 활성화되어 재장전 궤도로 즉시 리셋됩니다."
                                            shutdown_flag = False
                                            tracking_cache[f"AVWAP_LIMIT_ORDER_PLACED_{t}"] = False
                                            tracking_cache[f"AVWAP_PLACED_TARGET_TH_{t}"] = 0.0
                                            tracking_cache[f"AVWAP_TRAP_PLACED_TIME_{t}"] = ""
                                            tracking_cache[f"AVWAP_BUY_ODNO_{t}"] = ""
                                            tracking_cache[f"AVWAP_TRAP_ODNO_{t}"] = ""
                                        else:
                                            msg += f"\n🛡️ 단일 타격 익절 청산 완료. 암살자 작전을 <b>영구 동결(Shutdown)</b>합니다."
                                            shutdown_flag = True
                                            
                                    new_avg = 0.0
                                    avwap_free_cash += (total_sold * exec_price)
                                else:
                                    msg += f"\n⚠️ 다음 1분봉 루프에서 잔량 재시도"
                                    shutdown_flag = True
                                    new_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)

                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                   
                                daily_s = tracking_cache.get(f"AVWAP_DAILY_SOLD_{t}", 0) + total_sold
                                tracking_cache[f"AVWAP_DAILY_SOLD_{t}"] = daily_s
                                tracking_cache[f"AVWAP_BOUGHT_{t}"] = (new_qty > 0)
                                tracking_cache[f"AVWAP_SHUTDOWN_{t}"] = shutdown_flag
                                tracking_cache[f"AVWAP_QTY_{t}"] = new_qty
                                tracking_cache[f"AVWAP_AVG_{t}"] = new_avg
       
                                state_data = avwap_state_dict.copy()
                                state_data.update({
                                    'bought': tracking_cache[f"AVWAP_BOUGHT_{t}"],
                                    'shutdown': shutdown_flag,
                                    'qty': new_qty,
                                    'avg_price': new_avg,
                                    "daily_sold_qty": daily_s,
                                    "trap_odno": tracking_cache.get(f"AVWAP_TRAP_ODNO_{t}", ""),
                                    "limit_order_placed": tracking_cache.get(f"AVWAP_LIMIT_ORDER_PLACED_{t}", False),
                                    "placed_target_th": tracking_cache.get(f"AVWAP_PLACED_TARGET_TH_{t}", 0.0),
                                    "trap_placed_time": tracking_cache.get(f"AVWAP_TRAP_PLACED_TIME_{t}", ""),
                                    "buy_odno": tracking_cache.get(f"AVWAP_BUY_ODNO_{t}", "")
                                })
                                await asyncio.to_thread(strategy.v_avwap_plugin.save_state, t, now_est, state_data)

                    elif action == "SHUTDOWN":
                        if not tracking_cache.get(f"AVWAP_SHUTDOWN_{t}"):
                            tracking_cache[f"AVWAP_SHUTDOWN_{t}"] = True
                            
                            buy_odno = tracking_cache.get(f"AVWAP_BUY_ODNO_{t}", "")
                            if buy_odno and tracking_cache.get(f"AVWAP_LIMIT_ORDER_PLACED_{t}"):
                                try:
                                    await asyncio.to_thread(broker.cancel_order, t, buy_odno)
                                    await asyncio.sleep(0.5)
                                    msg_trap = "\n▫️ (장전된 매수 덫 전면 파기 완료)"
                                except: msg_trap = "\n▫️ (장전된 덫 파기 시도 중 에러)"
                            else: msg_trap = ""
                                
                            tracking_cache[f"AVWAP_LIMIT_ORDER_PLACED_{t}"] = False
                            tracking_cache[f"AVWAP_PLACED_TARGET_TH_{t}"] = 0.0
                            tracking_cache[f"AVWAP_TRAP_PLACED_TIME_{t}"] = ""
                            
                            state_data = avwap_state_dict.copy()
                            state_data.update({
                                "shutdown": True,
                                "limit_order_placed": False,
                                "placed_target_th": 0.0,
                                "trap_placed_time": ""
                            })
                            
                            await asyncio.to_thread(strategy.v_avwap_plugin.save_state, t, now_est, state_data)
                            msg = f"🛡️ <b>[AVWAP] 암살자 작전 당일 영구 동결(SHUTDOWN)</b>\n▫️ 타겟: {t}\n▫️ 사유: {reason}{msg_trap}"
                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

                master_switch = await asyncio.to_thread(getattr(cfg, 'get_master_switch', lambda x: "ALL"), t)
                sniper_buy_locked = await asyncio.to_thread(getattr(cfg, 'get_sniper_buy_locked', lambda x: False), t)
                sniper_sell_locked = await asyncio.to_thread(getattr(cfg, 'get_sniper_sell_locked', lambda x: False), t)

                curr_p = 0.0
                for attempt in range(3):
                    try:
                        curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, t), timeout=15.0)
                        curr_p = float(curr_p_val or 0.0)
                        break
                    except asyncio.TimeoutError:
                        if attempt == 2: logging.warning(f"⚠️ [{t}] 현재가 스캔 타임아웃. 0.0 폴백.")
                        else: await asyncio.sleep(1.0 * (2**attempt))
                    except Exception:
                        if attempt == 2: curr_p = 0.0
                        else: await asyncio.sleep(1.0 * (2**attempt))
                    
                if curr_p <= 0: continue

                sniper_func = getattr(strategy, 'check_sniper_condition', None)
                if sniper_func: res = await asyncio.to_thread(sniper_func, t, cfg, broker, chat_id)
                else: res = {"action": "HOLD", "reason": "스나이퍼 모듈 누락(Bypass)", "limit_price": 0.0}
         
                action = res.get("action")
                reason = res.get("reason", "")
                limit_p = res.get("limit_price", 0.0)

                is_rev = (await asyncio.to_thread(cfg.get_version, t) == "V_REV")

                if action == "BUY" and not is_rev and not sniper_buy_locked and master_switch != "UP_ONLY":
                    qty = res.get("qty", 0)
                    if qty > 0:
                        await asyncio.to_thread(broker.cancel_targeted_orders, t, "02", "03")
                        await asyncio.sleep(1.0)
                        
                        has_unfilled = False
                        for _ in range(4):
                            unfilled = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                            if isinstance(unfilled, list) and any(
                                o.get('sll_buy_dvsn_cd') == '02' and str(o.get('ord_dvsn_cd') or o.get('ord_dvsn') or '').strip().zfill(2) == '00' 
                                for o in unfilled
                            ):
                                has_unfilled = True
                                break
                            await asyncio.sleep(2.0)
                        
                        if has_unfilled: continue
                        
                        ask_price = 0.0
                        for attempt in range(3):
                            try:
                                bid_price_val = await asyncio.wait_for(asyncio.to_thread(broker.get_ask_price, t), timeout=10.0)
                                ask_price = float(bid_price_val or 0.0)
                                break
                            except Exception: 
                                if attempt == 2: ask_price = 0.0
                                else: await asyncio.sleep(1.0 * (2**attempt))
                        exec_price = ask_price if ask_price > 0 else limit_p

                        order_res = await asyncio.to_thread(broker.send_order, t, "BUY", qty, exec_price, "LIMIT")
                        odno = order_res.get('odno', '') if isinstance(order_res, dict) else ''
                        
                        if order_res and order_res.get('rt_cd') == '0' and odno:
                            ccld_qty = 0
                            for _ in range(4):
                                 await asyncio.sleep(2.0)
                                 unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                 safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []
                                 my_order = next((ox for ox in safe_unfilled if ox.get('odno') == odno), None)
                                 if my_order:
                                     ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                     if ccld_qty >= qty: break
                                 else:
                                     ccld_qty = qty
                                     break

                            if ccld_qty < qty:
                                try:
                                    await asyncio.to_thread(broker.cancel_order, t, odno)
                                    await asyncio.sleep(1.0)
                                except: pass

                            if ccld_qty > 0:
                                if hasattr(cfg, 'set_sniper_buy_locked'):
                                    await asyncio.to_thread(cfg.set_sniper_buy_locked, t, True)
                                   
                                exec_history = await asyncio.to_thread(broker.get_execution_history, t, today_est_str, today_est_str)
                                
                                actual_exec_price = next((float(ex.get('ft_ccld_unpr3', '0')) for ex in exec_history if ex.get('sll_buy_dvsn_cd') == '02' and ex.get('odno') == odno and float(ex.get('ft_ccld_unpr3', '0')) > 0), next((float(ex.get('ft_ccld_unpr3', '0')) for ex in exec_history if ex.get('sll_buy_dvsn_cd') == '02' and float(ex.get('ft_ccld_unpr3', '0')) > 0), limit_p))
                                display_price = actual_exec_price if actual_exec_price > 0 else limit_p
            
                                msg = f"🚨 <b>[{t}] 스나이퍼 딥-매수(Intercept) 명중!</b>\n▫️ 타겟가: ${limit_p:.2f}\n▫️ 팩트 단가: ${display_price:.2f}\n▫️ 체결수량: {ccld_qty}주 (요청: {qty}주)\n▫️ 사유: {reason}\n▫️ 하방 방어망이 잠깁니다 (상방 독립 유지)."
                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                        else:
                            err_msg = html.escape(order_res.get('msg1', '응답 없음') if isinstance(order_res, dict) else '통신 장애')
                            logging.error(f"🚨 [{t}] 스나이퍼 매수 KIS 서버 거절: {err_msg}")
                            reject_msg = (
                                f"🚨 <b>[{t}] 스나이퍼 딥매수 서버 거절 (Reject)!</b>\n"
                                f"▫️ 사유: <code>{err_msg}</code>\n"
                                f"▫️ 조치: 다음 스캔 시 재시도합니다."
                            )
                            await context.bot.send_message(chat_id=chat_id, text=reject_msg, parse_mode='HTML')
                
                is_zero_start_session = False
                try:
                    snap = None
                    if is_rev and hasattr(strategy, 'v_rev_plugin'): snap = await asyncio.to_thread(strategy.v_rev_plugin.load_daily_snapshot, t)
                    elif version == "V14":
                        is_manual_vwap = await asyncio.to_thread(getattr(cfg, 'get_manual_vwap_mode', lambda x: False), t)
                        if is_manual_vwap and hasattr(strategy, 'v14_vwap_plugin'): snap = await asyncio.to_thread(strategy.v14_vwap_plugin.load_daily_snapshot, t)
                        elif hasattr(strategy, 'v14_plugin') and hasattr(strategy.v14_plugin, 'load_daily_snapshot'): snap = await asyncio.to_thread(strategy.v14_plugin.load_daily_snapshot, t)
                    if snap: is_zero_start_session = snap.get("is_zero_start", snap.get("total_q", snap.get("initial_qty", -1)) == 0)
                except Exception: pass

                upward_mode = await asyncio.to_thread(getattr(cfg, 'get_upward_sniper_mode', lambda x: False), t)
                is_upward_active = upward_mode and not is_rev and not sniper_sell_locked and master_switch != "DOWN_ONLY"
            
                if is_zero_start_session: is_upward_active = False

                if is_upward_active and action in ["SELL_QUARTER", "SELL_JACKPOT"]:
                    qty = res.get("qty", 0)
                    if qty > 0:
                        await asyncio.to_thread(broker.cancel_targeted_orders, t, "01", "03")
                        await asyncio.sleep(1.0)
                    
                        has_unfilled = False
                        for _ in range(4):
                            unfilled = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                            if isinstance(unfilled, list) and any(
                                o.get('sll_buy_dvsn_cd') == '01' and str(o.get('ord_dvsn_cd') or o.get('ord_dvsn') or '').strip().zfill(2) == '00' 
                                for o in unfilled
                            ):
                                has_unfilled = True
                                break
                            await asyncio.sleep(2.0)
                
                        if has_unfilled: continue
      
                        bid_price = 0.0
                        for attempt in range(3):
                            try:
                                bid_price_val = await asyncio.wait_for(asyncio.to_thread(broker.get_bid_price, t), timeout=10.0)
                                bid_price = float(bid_price_val or 0.0)
                                break
                            except Exception:
                                if attempt == 2: bid_price = 0.0
                                else: await asyncio.sleep(1.0 * (2**attempt))
                        exec_price = bid_price if bid_price > 0 else limit_p
                
                        order_res = await asyncio.to_thread(broker.send_order, t, "SELL", qty, exec_price, "LIMIT")
                        odno = order_res.get('odno', '') if isinstance(order_res, dict) else ''
                        
                        if order_res and order_res.get('rt_cd') == '0' and odno:
                            ccld_qty = 0
                            for _ in range(4):
                                await asyncio.sleep(2.0)
                                unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []
                                
                                my_order = next((ox for ox in safe_unfilled if ox.get('odno') == odno), None)
                                if my_order:
                                    ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                    if ccld_qty >= qty: break
                                else:
                                    ccld_qty = qty
                                    break
                
                            if ccld_qty < qty:
                                 try:
                                     await asyncio.to_thread(broker.cancel_order, t, odno)
                                     await asyncio.sleep(1.0)
                                 except: pass

                            if ccld_qty > 0:
                                if hasattr(cfg, 'set_sniper_sell_locked'): await asyncio.to_thread(cfg.set_sniper_sell_locked, t, True)
                                      
                                exec_history = await asyncio.to_thread(broker.get_execution_history, t, today_est_str, today_est_str)
                                   
                                actual_exec_price = next((float(ex.get('ft_ccld_unpr3', '0')) for ex in exec_history if ex.get('sll_buy_dvsn_cd') == '01' and ex.get('odno') == odno and float(ex.get('ft_ccld_unpr3', '0')) > 0), next((float(ex.get('ft_ccld_unpr3', '0')) for ex in exec_history if ex.get('sll_buy_dvsn_cd') == '01' and float(ex.get('ft_ccld_unpr3', '0')) > 0), limit_p))
                                display_price = actual_exec_price if actual_exec_price > 0 else limit_p
                 
                                msg = f"🦇 <b>[{t}] 스나이퍼 상방 기습({action}) 명중!</b>\n▫️ 타겟가: ${limit_p:.2f}\n▫️ 팩트 단가: ${display_price:.2f}\n▫️ 체결수량: {ccld_qty}주 (요청: {qty}주)\n▫️ 사유: {reason}\n▫️ 상방 감시망이 잠깁니다 (하방 독립 유지)."
                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                        else:
                            err_msg = html.escape(order_res.get('msg1', '응답 없음') if isinstance(order_res, dict) else '통신 장애')
                            logging.error(f"🚨 [{t}] 스나이퍼 상방 기습 서버 거절: {err_msg}")
                            reject_msg = (
                                f"🚨 <b>[{t}] 스나이퍼 상방 기습 서버 거절 (Reject)!</b>\n"
                                f"▫️ 사유: <code>{err_msg}</code>\n"
                                f"▫️ 조치: 다음 스캔 시 재시도합니다."
                            )
                            await context.bot.send_message(chat_id=chat_id, text=reject_msg, parse_mode='HTML')

    try:
        await asyncio.wait_for(_do_sniper(), timeout=120.0)
    except Exception as e:
        logging.error(f"🚨 스나이퍼 타임아웃 에러: {e}", exc_info=True)
