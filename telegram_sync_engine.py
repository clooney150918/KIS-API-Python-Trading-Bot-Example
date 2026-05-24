# ==========================================================
# FILE: telegram_sync_engine.py
# ==========================================================
# 🚨 MODIFIED: [V-REV 및 AVWAP 디커플링 누수 차단] 액면분할 감지 시 모든 장부 소급 보정
# 🚨 MODIFIED: [제1헌법 준수] 비동기 함수 내 QueueLedger 인스턴스화 격리
# 🚨 MODIFIED: [제1헌법 준수] os.path.exists 및 open() 동기 파일 I/O 뇌관 비동기 래핑 100% 완료
# 🚨 MODIFIED: [네임스페이스 결측 붕괴 교정] pandas 모듈 전진 배치(import pandas as pd)로 NameError 원천 봉쇄
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import time
import os
import asyncio
import json
import tempfile
import traceback
import yfinance as yf
import pandas as pd # 🚨 NEW: [네임스페이스 누락 팩트 교정]
import pandas_market_calendars as mcal
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

class TelegramSyncEngine:
    def __init__(self, config, broker, strategy, queue_ledger, view, tx_lock, sync_locks):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.queue_ledger = queue_ledger
        self.view = view
        self.tx_lock = tx_lock
        self.sync_locks = sync_locks

    async def process_auto_sync(self, ticker, chat_id, context, silent_ledger=False):
        if ticker not in self.sync_locks:
            self.sync_locks[ticker] = asyncio.Lock()
            
        if self.sync_locks[ticker].locked(): return "LOCKED"
            
        async with self.sync_locks[ticker]:
            async with self.tx_lock:
                last_split_date = await asyncio.to_thread(self.cfg.get_last_split_date, ticker)
                split_ratio, split_date = 0.0, ""
                for attempt in range(3):
                    try:
                        await asyncio.sleep(0.06)
                        split_ratio, split_date = await asyncio.wait_for(
                            asyncio.to_thread(self.broker.get_recent_stock_split, ticker, last_split_date), timeout=15.0
                        )
                        break
                    except Exception:
                        if attempt == 2:
                            split_ratio, split_date = 0.0, ""
                            logging.warning(f"⚠️ [{ticker}] 야후 파이낸스 액면분할 조회 타임아웃, 이번 싱크에서 스킵")
                        else: await asyncio.sleep(1.0 * (2 ** attempt))
                
                if split_ratio > 0.0 and split_date != "":
                    est = ZoneInfo('America/New_York')
                    now_est = datetime.datetime.now(est)
                    await asyncio.to_thread(self.cfg.apply_stock_split, ticker, split_ratio)
                    
                    if not getattr(self, 'queue_ledger', None):
                        from queue_ledger import QueueLedger
                        self.queue_ledger = await asyncio.to_thread(QueueLedger)
                        
                    if getattr(self, 'queue_ledger', None):
                        await asyncio.to_thread(self.queue_ledger.apply_stock_split, ticker, split_ratio)
                        
                    if hasattr(self.strategy, 'v_avwap_plugin'):
                        await asyncio.to_thread(self.strategy.v_avwap_plugin.apply_stock_split, ticker, split_ratio, now_est)
                        
                    await asyncio.to_thread(self.cfg.set_last_split_date, ticker, split_date)
                    split_type = "액면분할" if split_ratio > 1.0 else "액면병합(역분할)"
                    await context.bot.send_message(chat_id, f"✂️ <b>[{ticker}] 야후 파이낸스 {split_type} 자동 감지!</b>\n▫️ 감지된 비율: <b>{split_ratio}배</b> (발생일: {split_date})\n▫️ 봇이 기존 V14 장부, V-REV 큐 장부, AVWAP 상태 캐시의 수량과 평단가를 100% 무인 자동 소급 조정 완료했습니다.", parse_mode='HTML')
                 
                kst = ZoneInfo('Asia/Seoul')
                now_kst = datetime.datetime.now(kst)
                est = ZoneInfo('America/New_York')
                now_est = datetime.datetime.now(est)
                
                def _get_last_trade_date():
                    time.sleep(0.06)
                    nyse = mcal.get_calendar('NYSE')
                    schedule_data = nyse.schedule(start_date=(now_est - datetime.timedelta(days=10)).date(), end_date=now_est.date())
                    return schedule_data

                schedule = pd.DataFrame()
                for attempt in range(3):
                    try:
                        schedule = await asyncio.wait_for(asyncio.to_thread(_get_last_trade_date), timeout=15.0)
                        break
                    except Exception:
                        if attempt == 2: pass
                        else: await asyncio.sleep(1.0 * (2**attempt))
                        
                if not schedule.empty:
                    last_trade_date = schedule.index[-1]
                    target_ledger_str = last_trade_date.strftime('%Y-%m-%d')
                else: target_ledger_str = now_est.strftime('%Y-%m-%d')

                holdings = None
                for attempt in range(3):
                    try:
                        _, holdings = await asyncio.wait_for(asyncio.to_thread(self.broker.get_account_balance), timeout=15.0)
                        break
                    except Exception:
                        if attempt == 2: holdings = None
                        else: await asyncio.sleep(1.0 * (2**attempt))
                        
                if holdings is None:
                    await context.bot.send_message(chat_id, f"❌ <b>[{ticker}] API 오류</b>\n잔고를 불러오지 못했습니다.", parse_mode='HTML')
                    return "ERROR"

                actual_qty = int(float(holdings.get(ticker, {'qty': 0}).get('qty') or 0))
                actual_avg = float(holdings.get(ticker, {'avg': 0}).get('avg') or 0.0)

                full_ledger = await asyncio.to_thread(self.cfg.get_ledger)
                recs_for_check = [r for r in full_ledger if r['ticker'] == ticker]
                ledger_qty_for_check, _, _, _ = await asyncio.to_thread(self.cfg.calculate_holdings, ticker, recs_for_check)
                
                vrev_ledger_qty_for_check = 0
                is_rev = (await asyncio.to_thread(self.cfg.get_version, ticker) == "V_REV")
                
                if is_rev:
                    if not getattr(self, 'queue_ledger', None):
                        from queue_ledger import QueueLedger
                        self.queue_ledger = await asyncio.to_thread(QueueLedger)
                    
                    q_data_check = await asyncio.to_thread(self.queue_ledger.get_queue, ticker)
                    vrev_ledger_qty_for_check = sum(int(float(item.get("qty") or 0)) for item in q_data_check)
                
                max_check_qty = max(ledger_qty_for_check, vrev_ledger_qty_for_check)

                kis_search_start = (now_kst - datetime.timedelta(days=4)).strftime('%Y%m%d')
                query_end_dt = now_kst.strftime('%Y%m%d')

                def filter_to_est(execs_raw):
                    filtered = []
                    if not execs_raw: return filtered
                    for ex in execs_raw:
                        ord_dt = ex.get('ord_dt') or ex.get('ord_strt_dt')
                        if not ord_dt: continue
                        ord_tmd = ex.get('ord_tmd')
                        if not ord_tmd or len(str(ord_tmd)) != 6: ord_tmd = '000000'
                        try:
                            k_dt = datetime.datetime.strptime(f"{ord_dt}{ord_tmd}", "%Y%m%d%H%M%S").replace(tzinfo=kst)
                            e_dt = k_dt.astimezone(est)
                            if e_dt.strftime('%Y-%m-%d') == target_ledger_str:
                               filtered.append(ex)
                        except Exception: pass
                    return filtered

                raw_execs = []
                target_execs = []
                
                if actual_qty == 0 and max_check_qty > 0:
                    max_retries = 6
                    prev_sold_today = -1
                    stable_cnt = 0
                    for attempt in range(max_retries):
                        raw_execs = None
                        for inner_attempt in range(3):
                            try:
                                raw_execs = await asyncio.wait_for(asyncio.to_thread(self.broker.get_execution_history, ticker, kis_search_start, query_end_dt), timeout=15.0)
                                break
                            except Exception:
                                if inner_attempt == 2: raw_execs = []
                                else: await asyncio.sleep(1.0 * (2**inner_attempt))
                                
                        target_execs = filter_to_est(raw_execs)
                        sold_today = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01")
                        
                        if sold_today >= max_check_qty:
                            if sold_today == prev_sold_today:
                                stable_cnt += 1
                                if stable_cnt >= 1: break
                            else: stable_cnt = 0
                            prev_sold_today = sold_today
                        
                        if attempt < max_retries - 1:
                            logging.info(f"⏳ [{ticker}] 체결 원장 지연(Lag) 감지. 데이터 안정화 및 EST 매핑 검증 중... ({attempt+1}/{max_retries})")
                            await asyncio.sleep(2.0)
                else:
                    for inner_attempt in range(3):
                        try:
                            raw_execs = await asyncio.wait_for(asyncio.to_thread(self.broker.get_execution_history, ticker, kis_search_start, query_end_dt), timeout=15.0)
                            break
                        except Exception:
                            if inner_attempt == 2: raw_execs = []
                            else: await asyncio.sleep(1.0 * (2**inner_attempt))
                    target_execs = filter_to_est(raw_execs)

                if target_execs:
                    calibrated_count = await asyncio.to_thread(self.cfg.calibrate_ledger_prices, ticker, target_ledger_str, target_execs)
                    if calibrated_count > 0:
                        logging.info(f"🔧 [{ticker}] LOC/MOC 주문 {calibrated_count}건에 대해 실제 체결 단가 소급 업데이트를 완료했습니다.")

                full_ledger = await asyncio.to_thread(self.cfg.get_ledger)
                recs = [r for r in full_ledger if r['ticker'] == ticker]
                ledger_qty, avg_price, _, _ = await asyncio.to_thread(self.cfg.calculate_holdings, ticker, recs)
                
                diff = actual_qty - ledger_qty
                price_diff = abs(actual_avg - avg_price)

                today_recs = [r for r in recs if r['date'] == target_ledger_str and 'INIT' not in str(r.get('exec_id', '')) and 'CALIB' not in str(r.get('exec_id', ''))]
                ledger_today_buy = sum(r['qty'] for r in today_recs if r['side'] == 'BUY')
                ledger_today_sell = sum(r['qty'] for r in today_recs if r['side'] == 'SELL')
                
                exec_today_buy = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "02")
                exec_today_sell = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01")
                
                avwap_daily_buy = 0
                avwap_daily_sell = 0
                try:
                    if hasattr(self.strategy, 'v_avwap_plugin'):
                        avwap_state_sync = await asyncio.to_thread(self.strategy.v_avwap_plugin.load_state, ticker, now_est)
                        avwap_daily_buy = int(avwap_state_sync.get('daily_bought_qty', 0))
                        avwap_daily_sell = int(avwap_state_sync.get('daily_sold_qty', 0))
                except Exception: pass
                
                exec_today_buy = max(0, exec_today_buy - avwap_daily_buy)
                exec_today_sell = max(0, exec_today_sell - avwap_daily_sell)
                
                needs_reconstruction = (diff != 0) or (ledger_today_buy != exec_today_buy) or (ledger_today_sell != exec_today_sell)

                if not needs_reconstruction and price_diff < 0.01: pass 
                elif not needs_reconstruction and price_diff >= 0.01:
                    await asyncio.to_thread(self.cfg.calibrate_avg_price, ticker, actual_avg)
                    await context.bot.send_message(chat_id, f"🔧 <b>[{ticker}] 장부 평단가 미세 오차({price_diff:.4f}) 교정 완료!</b>", parse_mode='HTML')
                elif needs_reconstruction:
                    temp_recs = [r for r in recs if r['date'] != target_ledger_str or 'INIT' in str(r.get('exec_id', ''))]
                    temp_qty, temp_avg, _, _ = await asyncio.to_thread(self.cfg.calculate_holdings, ticker, temp_recs)
                    
                    temp_sim_qty = temp_qty
                    temp_sim_avg = temp_avg
                    new_target_records = []
                    
                    if target_execs:
                        target_execs.sort(key=lambda x: str(x.get('ord_dt', '00000000')) + str(x.get('ord_tmd', '000000'))) 
                        for ex in target_execs:
                            side_cd = ex.get('sll_buy_dvsn_cd')
                            exec_qty = int(float(ex.get('ft_ccld_qty') or '0'))
                            exec_price = float(ex.get('ft_ccld_unpr3') or '0')
                            
                            if side_cd == "02": 
                                new_avg = ((temp_sim_qty * temp_sim_avg) + (exec_qty * exec_price)) / (temp_sim_qty + exec_qty) if (temp_sim_qty + exec_qty) > 0 else exec_price
                                temp_sim_qty += exec_qty
                                temp_sim_avg = new_avg
                            else: temp_sim_qty -= exec_qty
                            
                            rec_item = {'date': target_ledger_str, 'side': "BUY" if side_cd == "02" else "SELL", 'qty': exec_qty, 'price': exec_price, 'avg_price': temp_sim_avg}
                            if is_rev: rec_item['is_reverse'] = True
                            new_target_records.append(rec_item)
                            
                    gap_qty = actual_qty - temp_sim_qty
                    if gap_qty != 0:
                        calib_side = "BUY" if gap_qty > 0 else "SELL"
                        calib_price = actual_avg
                        if calib_side == "SELL" and actual_avg <= 0.0:
                            actual_clear_price_calib = 0.0
                            if target_execs:
                                sell_execs_calib = [ex for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01"]
                                if sell_execs_calib:
                                    tot_amt_calib = sum(int(float(ex.get('ft_ccld_qty') or '0')) * float(ex.get('ft_ccld_unpr3') or '0') for ex in sell_execs_calib)
                                    tot_q_calib = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in sell_execs_calib)
                                    if tot_q_calib > 0: actual_clear_price_calib = round(tot_amt_calib / tot_q_calib, 4)
                            
                            if actual_clear_price_calib == 0.0 and raw_execs:
                                recent_sells = [ex for ex in raw_execs if ex.get('sll_buy_dvsn_cd') == "01"]
                                if recent_sells:
                                    recent_sells.sort(key=lambda x: f"{x.get('ord_dt', '')}{x.get('ord_tmd', '')}", reverse=True)
                                    last_sell_dt = recent_sells[0].get('ord_dt')
                                    same_day_sells = [ex for ex in recent_sells if ex.get('ord_dt') == last_sell_dt]
                                    tot_amt = sum(int(float(ex.get('ft_ccld_qty') or '0')) * float(ex.get('ft_ccld_unpr3') or '0') for ex in same_day_sells)
                                    tot_q = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in same_day_sells)
                                    if tot_q > 0: actual_clear_price_calib = round(tot_amt / tot_q, 4)
                            
                            if actual_clear_price_calib > 0.0: calib_price = actual_clear_price_calib
                            else: calib_price = temp_sim_avg if temp_sim_avg > 0 else (temp_avg if temp_avg > 0 else 0.01)
                            calib_avg = temp_sim_avg
                        elif calib_side == "BUY" and actual_avg <= 0.0:
                            calib_price = temp_sim_avg if temp_sim_avg > 0 else (temp_avg if temp_avg > 0 else 0.01)
                            calib_avg = temp_sim_avg
                        else:
                            calib_price = actual_avg if actual_avg > 0 else temp_sim_avg
                            calib_avg = actual_avg if actual_avg > 0 else temp_sim_avg
                        
                        calib_item = {'date': target_ledger_str, 'side': calib_side, 'qty': abs(gap_qty), 'price': calib_price, 'avg_price': calib_avg, 'exec_id': f"CALIB_{int(time.time())}", 'desc': "비파괴 보정"}
                        if is_rev: calib_item['is_reverse'] = True
                        new_target_records.append(calib_item)
                        
                        if new_target_records:
                            if actual_qty > 0:
                                for r in new_target_records: r['avg_price'] = actual_avg
                            elif temp_recs: 
                                if actual_qty > 0: temp_recs[-1]['avg_price'] = actual_avg
                
                    await asyncio.to_thread(self.cfg.overwrite_incremental_ledger, ticker, temp_recs, new_target_records)
                    if gap_qty != 0: await context.bot.send_message(chat_id, f"🔧 <b>[{ticker}] 통합 메인 장부(MAIN LEDGER) 비파괴 보정 완료!</b>\n▫️ KIS 실잔고 오차 수량({gap_qty}주)을 역사 보존 상태로 안전하게 교정했습니다.", parse_mode='HTML')

                if is_rev:
                    q_data_before = await asyncio.to_thread(self.queue_ledger.get_queue, ticker)
                    vrev_ledger_qty = sum(int(float(item.get("qty") or 0)) for item in q_data_before)
                    
                    sold_today_vrev = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01") if target_execs else 0
                    sold_today_vrev = max(0, sold_today_vrev - avwap_daily_sell)
                    
                    avwap_qty_global = 0
                    tracking_cache_global = None
                    try:
                        app_data = context.bot_data.get('app_data', {})
                        tracking_cache_global = app_data.get('sniper_tracking', {})
                        avwap_qty_global = tracking_cache_global.get(f"AVWAP_QTY_{ticker}", 0)

                        if avwap_qty_global == 0:
                            if hasattr(self.strategy, 'v_avwap_plugin'):
                                avwap_state = await asyncio.to_thread(self.strategy.v_avwap_plugin.load_state, ticker, now_est)
                                avwap_qty_global = int(avwap_state.get('qty', 0))
                    except Exception: pass
                    
                    if actual_qty == vrev_ledger_qty and avwap_qty_global > 0:
                        try:
                            if tracking_cache_global is not None:
                                tracking_cache_global[f"AVWAP_QTY_{ticker}"] = 0
                                tracking_cache_global[f"AVWAP_AVG_{ticker}"] = 0.0
                                tracking_cache_global[f"AVWAP_BOUGHT_{ticker}"] = False
                                tracking_cache_global[f"AVWAP_SHUTDOWN_{ticker}"] = True

                            if hasattr(self.strategy, 'v_avwap_plugin'):
                                state_data = {
                                    'bought': False, 'shutdown': True, 'qty': 0, 'avg_price': 0.0,
                                    'strikes': tracking_cache_global.get(f"AVWAP_STRIKES_{ticker}", 0) if tracking_cache_global is not None else 0,
                                    'daily_bought_qty': tracking_cache_global.get(f"AVWAP_DAILY_BOUGHT_{ticker}", 0) if tracking_cache_global is not None else 0,
                                    'daily_sold_qty': tracking_cache_global.get(f"AVWAP_DAILY_SOLD_{ticker}", 0) if tracking_cache_global is not None else 0,
                                    'dump_jitter_sec': tracking_cache_global.get(f"AVWAP_DUMP_JITTER_{ticker}", 0) if tracking_cache_global is not None else 0
                                }
                                await asyncio.to_thread(self.strategy.v_avwap_plugin.save_state, ticker, now_est, state_data)
                            avwap_qty_global = 0
                        except Exception: pass

                    adjusted_actual_qty = max(0, actual_qty - avwap_qty_global)
                    
                    if adjusted_actual_qty == 0 and (vrev_ledger_qty > 0 or sold_today_vrev > 0):
                        if actual_qty == 0 and avwap_qty_global > 0:
                            try:
                                if tracking_cache_global is not None:
                                    tracking_cache_global[f"AVWAP_QTY_{ticker}"] = 0
                                    tracking_cache_global[f"AVWAP_AVG_{ticker}"] = 0.0
                                    tracking_cache_global[f"AVWAP_BOUGHT_{ticker}"] = False
                                    tracking_cache_global[f"AVWAP_SHUTDOWN_{ticker}"] = True

                                if hasattr(self.strategy, 'v_avwap_plugin'):
                                    state_data = {
                                        'bought': False, 'shutdown': True, 'qty': 0, 'avg_price': 0.0,
                                        'strikes': tracking_cache_global.get(f"AVWAP_STRIKES_{ticker}", 0) if tracking_cache_global is not None else 0,
                                        'daily_bought_qty': 0, 'daily_sold_qty': 0, 'first_scan_done': False, 'first_scan_passed': False,
                                        'dump_jitter_sec': tracking_cache_global.get(f"AVWAP_DUMP_JITTER_{ticker}", 0) if tracking_cache_global is not None else 0
                                    }
                                    await asyncio.to_thread(self.strategy.v_avwap_plugin.save_state, ticker, now_est, state_data)
                            except Exception: pass

                        added_seed = 0.0
                        _vrev_snap_ok = False
                        snapshot = None
                        try:
                            actual_clear_price = 0.0
                            tot_q = 0
                            
                            if target_execs:
                                sell_execs = [ex for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01"]
                                if sell_execs:
                                    tot_amt = sum(int(float(ex.get('ft_ccld_qty') or '0')) * float(ex.get('ft_ccld_unpr3') or '0') for ex in sell_execs)
                                    tot_q = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in sell_execs)
                                    if tot_q > 0: actual_clear_price = round(tot_amt / tot_q, 4)
                            
                            last_sell_dt = "당일"

                            if actual_clear_price == 0.0:
                                if raw_execs:
                                    recent_sells = [ex for ex in raw_execs if ex.get('sll_buy_dvsn_cd') == "01"]
                                    if recent_sells:
                                        recent_sells.sort(key=lambda x: f"{x.get('ord_dt', '')}{x.get('ord_tmd', '')}", reverse=True)
                                        last_sell_dt = recent_sells[0].get('ord_dt')
                                        same_day_sells = [ex for ex in recent_sells if ex.get('ord_dt') == last_sell_dt]
                                        tot_amt = sum(int(float(ex.get('ft_ccld_qty') or '0')) * float(ex.get('ft_ccld_unpr3') or '0') for ex in same_day_sells)
                                        tot_q = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in same_day_sells)
                                        if tot_q > 0: actual_clear_price = round(tot_amt / tot_q, 4)

                            if tot_q > vrev_ledger_qty:
                                missing_qty = tot_q - vrev_ledger_qty
                                buy_execs = [ex for ex in (target_execs or []) if ex.get('sll_buy_dvsn_cd') == "02"]
                                temp_invested = sum(float(item.get("qty", 0)) * float(item.get("price", 0)) for item in q_data_before)
                                temp_avg = temp_invested / vrev_ledger_qty if vrev_ledger_qty > 0 else 0.0
                                missing_price = temp_avg
                             
                                if buy_execs:
                                    b_tot_amt = sum(int(float(ex.get('ft_ccld_qty') or '0')) * float(ex.get('ft_ccld_unpr3') or '0') for ex in buy_execs)
                                    b_tot_q = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in buy_execs)
                                    if b_tot_q > 0:
                                        q_today_amt = 0.0
                                        q_today_qty = 0
                                        for item in q_data_before:
                                            if str(item.get("date", "")).startswith(target_ledger_str):
                                                iq = int(float(item.get("qty", 0)))
                                                q_today_qty += iq
                                                q_today_amt += iq * float(item.get("price", 0))
                                                
                                        pure_manual_q = b_tot_q - q_today_qty
                                        pure_manual_amt = b_tot_amt - q_today_amt
                                        if pure_manual_q >= missing_qty and pure_manual_q > 0 and pure_manual_amt > 0:
                                            derived_price = pure_manual_amt / pure_manual_q
                                            missing_price = round(derived_price, 4)
                                        else: missing_price = round(b_tot_amt / b_tot_q, 4)
                                            
                                q_data_before.append({"date": now_est.strftime('%Y-%m-%d %H:%M:%S'), "qty": missing_qty, "price": missing_price, "exec_id": "MANUAL_SYNC"})
                                vrev_ledger_qty = tot_q
                                await asyncio.to_thread(self.queue_ledger.overwrite_queue, ticker, q_data_before)

                            total_invested = sum(float(item.get("qty", 0)) * float(item.get("price", 0)) for item in q_data_before)
                            q_avg_price = total_invested / vrev_ledger_qty if vrev_ledger_qty > 0 else 0.0

                            curr_p = 0.0
                            for attempt in range(3):
                                try:
                                    curr_p_val = await asyncio.wait_for(asyncio.to_thread(self.broker.get_current_price, ticker), timeout=15.0)
                                    curr_p = float(curr_p_val or 0.0)
                                    break
                                except Exception:
                                    if attempt == 2: curr_p = 0.0
                                    else: await asyncio.sleep(1.0 * (2**attempt))
                            
                            clear_price = actual_clear_price if actual_clear_price > 0.0 else (curr_p if curr_p and curr_p > 0 else q_avg_price * 1.006)
                            snapshot = await asyncio.to_thread(self.strategy.capture_vrev_snapshot, ticker, clear_price, q_avg_price, vrev_ledger_qty)
                            
                            if snapshot:
                                realized_pnl = snapshot['realized_pnl']
                                yield_pct = snapshot['realized_pnl_pct']
                                compound_rate = float(await asyncio.to_thread(self.cfg.get_compound_rate, ticker)) / 100.0
                                if realized_pnl > 0 and compound_rate > 0:
                                    added_seed = realized_pnl * compound_rate
                                    current_seed = await asyncio.to_thread(self.cfg.get_seed, ticker)
                                    await asyncio.to_thread(self.cfg.set_seed, ticker, current_seed + added_seed)
                                   
                                cap_dt = snapshot['captured_at']
                                cap_dt_str = cap_dt if isinstance(cap_dt, str) else cap_dt.strftime('%Y-%m-%d')
                                start_dt_str = q_data_before[0]['date'][:10] if q_data_before else cap_dt_str[:10]
                                
                                hist_data = await asyncio.to_thread(self.cfg._load_json, self.cfg.FILES["HISTORY"], [])
                                new_hist = {
                                    "id": int(time.time()), "ticker": ticker, "start_date": start_dt_str, "end_date": cap_dt_str[:10],
                                    "invested": total_invested, "revenue": total_invested + realized_pnl,
                                    "profit": realized_pnl, "yield": yield_pct, "trades": q_data_before 
                                }
                                hist_data.append(new_hist)
                                await asyncio.to_thread(self.cfg._save_json, self.cfg.FILES["HISTORY"], hist_data)
                                _vrev_snap_ok = True
                                
                        except Exception as e:
                            logging.error(f"🚨 스냅샷 캡처 및 복리 정산 중 치명적 오류 감지: {e}\n{traceback.format_exc()}")
                            snapshot = None
                            
                        await asyncio.to_thread(self.queue_ledger.sync_with_broker, ticker, 0)
                        
                        if _vrev_snap_ok:
                            msg = f"🎉 <b>[{ticker} V-REV 잭팟 스윕(전량 익절) 감지!]</b>\n▫️ 잔고가 0주가 되어 LIFO 큐 지층을 100% 소각(초기화)했습니다."
                            if added_seed > 0: msg += f"\n💸 <b>자동 복리 +${added_seed:,.0f}</b> 이 다음 운용 시드에 완벽하게 추가되었습니다!"
                            await context.bot.send_message(chat_id, msg, parse_mode='HTML')
                            if snapshot:
                                try:
                                    img_path = await asyncio.to_thread(
                                        self.view.create_profit_image, ticker=ticker, profit=snapshot['realized_pnl'], 
                                        yield_pct=snapshot['realized_pnl_pct'], invested=snapshot['avg_price'] * snapshot['cleared_qty'], 
                                        revenue=snapshot['clear_price'] * snapshot['cleared_qty'], end_date=cap_dt_str[:10]
                                    )
                                    # 🚨 MODIFIED: [제1헌법] os.path.exists 비동기 격리 락온
                                    is_img_exist = await asyncio.to_thread(os.path.exists, img_path) if img_path else False
                                    if img_path and is_img_exist:
                                        def _read_img2(p):
                                            with open(p, 'rb') as f_in: return f_in.read()
                                        img_bytes2 = await asyncio.to_thread(_read_img2, img_path)
                                        if img_path.lower().endswith('.gif'): await context.bot.send_animation(chat_id=chat_id, animation=img_bytes2)
                                        else: await context.bot.send_photo(chat_id=chat_id, photo=img_bytes2)
                                except Exception: pass
                        else:
                            await context.bot.send_message(chat_id, f"⚠️ <b>[{ticker} V-REV 0주 강제 정산 완료]</b>\n▫️ 0주를 확인하여 큐를 안전하게 비웠으나 통신 지연으로 졸업 카드는 생략되었습니다.", parse_mode='HTML')
                            
                        return "SUCCESS"
                     
                    if adjusted_actual_qty == vrev_ledger_qty: pass
                    else:
                        if adjusted_actual_qty > 0 and adjusted_actual_qty < vrev_ledger_qty:
                            gap_qty = vrev_ledger_qty - adjusted_actual_qty
                            vwap_state_file = f"data/vwap_state_REV_{ticker}.json"
                            
                            # 🚨 MODIFIED: [제1헌법] os.path.exists 비동기 격리 락온
                            v_state_exists = await asyncio.to_thread(os.path.exists, vwap_state_file)
                            if v_state_exists:
                                try:
                                    def _read_v_state(f_path):
                                        with open(f_path, 'r', encoding='utf-8') as vf: return json.load(vf)
                                    v_state = await asyncio.to_thread(_read_v_state, vwap_state_file)
                                    if "executed" in v_state and "SELL_QTY" in v_state["executed"]:
                                        old_sell_qty = v_state["executed"]["SELL_QTY"]
                                        v_state["executed"]["SELL_QTY"] = max(0, old_sell_qty - gap_qty)
                                    def _write_v_state(state_dict, f_path):
                                        fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(f_path) or '.')
                                        with os.fdopen(fd, 'w', encoding='utf-8') as _vf_out:
                                            json.dump(state_dict, _vf_out, ensure_ascii=False, indent=4)
                                            _vf_out.flush()
                                            os.fsync(_vf_out.fileno())
                                        os.replace(tmp_path, f_path)
                                    await asyncio.to_thread(_write_v_state, v_state, vwap_state_file)
                                except Exception: pass

                            calibrated = await asyncio.to_thread(self.queue_ledger.sync_with_broker, ticker, adjusted_actual_qty, actual_avg)
                            if calibrated: await context.bot.send_message(chat_id, f"🔧 <b>[{ticker}] V-REV 큐(Queue) 비파괴 보정 완료!</b>\n▫️ 수동 매도 물량(<b>{gap_qty}주</b>)을 LIFO 큐에서 안전하게 차감했습니다.", parse_mode='HTML')
                            
                        elif adjusted_actual_qty > 0 and adjusted_actual_qty > vrev_ledger_qty:
                            gap_qty = adjusted_actual_qty - vrev_ledger_qty
                            real_buy_price = actual_avg
                            try:
                                buy_execs = [ex for ex in (target_execs or []) if ex.get('sll_buy_dvsn_cd') == "02"]
                                if buy_execs:
                                    b_tot_amt = sum(int(float(ex.get('ft_ccld_qty') or '0')) * float(ex.get('ft_ccld_unpr3') or '0') for ex in buy_execs)
                                    b_tot_q = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in buy_execs)
                                    if b_tot_q > 0: real_buy_price = round(b_tot_amt / b_tot_q, 4)
                                        
                                    if real_buy_price == actual_avg:
                                        search_start_dt = (now_kst - datetime.timedelta(days=4)).strftime('%Y%m%d')
                                        past_raw = await asyncio.to_thread(self.broker.get_execution_history, ticker, search_start_dt, query_end_dt)
                                        past_execs = filter_to_est(past_raw)
                                        if past_execs:
                                            p_buy_execs = [ex for ex in past_execs if ex.get('sll_buy_dvsn_cd') == "02"]
                                            if p_buy_execs:
                                                b_tot_amt = sum(int(float(ex.get('ft_ccld_qty') or '0')) * float(ex.get('ft_ccld_unpr3') or '0') for ex in p_buy_execs)
                                                b_tot_q = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in p_buy_execs)
                                                if b_tot_q > 0: real_buy_price = round(b_tot_amt / b_tot_q, 4)
                            except Exception: pass

                            if real_buy_price == actual_avg:
                                old_invested = sum(float(item.get("qty", 0)) * float(item.get("price", 0)) for item in q_data_before)
                                new_invested = adjusted_actual_qty * actual_avg
                                if new_invested > old_invested:
                                    derived_price = (new_invested - old_invested) / gap_qty
                                    real_buy_price = round(derived_price, 4) if derived_price > 0 else actual_avg
                
                            q_data = await asyncio.to_thread(self.queue_ledger.get_queue, ticker)
                            q_data.append({"date": now_est.strftime('%Y-%m-%d %H:%M:%S'), "qty": gap_qty, "price": real_buy_price, "exec_id": f"MANUAL_BUY_{int(time.time())}"})
                            try:
                                await asyncio.to_thread(self.queue_ledger.overwrite_queue, ticker, q_data)
                                await context.bot.send_message(chat_id, f"🔧 <b>[{ticker}] V-REV 큐(Queue) 수동 매수 편입 완료!</b>\n▫️ KIS 실잔고에 맞춰 신규 지층(<b>{gap_qty}주</b>, 추정단가 ${real_buy_price})을 정밀 추가했습니다.", parse_mode='HTML')
                            except Exception: pass
                
                    return "SUCCESS"

                if not is_rev:
                    sold_today_v14 = sum(int(float(ex.get('ft_ccld_qty') or '0')) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01") if target_execs else 0
                    sold_today_v14 = max(0, sold_today_v14 - avwap_daily_sell)
                    
                    if actual_qty == 0 and (ledger_qty > 0 or sold_today_v14 > 0):
                        today_est_str = now_est.strftime('%Y-%m-%d')
                        prev_c = 0.0
                        for attempt in range(3):
                            try:
                                prev_c_val = await asyncio.wait_for(asyncio.to_thread(self.broker.get_previous_close, ticker), timeout=15.0)
                                prev_c = float(prev_c_val or 0.0)
                                break
                            except Exception:
                                if attempt == 2: prev_c = 0.0
                                else: await asyncio.sleep(1.0 * (2**attempt))
                
                        try:
                            new_hist, added_seed = await asyncio.to_thread(self.cfg.archive_graduation, ticker, today_est_str, prev_c)

                            if new_hist:
                                msg = f"🎉 <b>[{ticker} 졸업 확인!]</b>\n장부를 명예의 전당에 저장하고 새 사이클을 준비합니다."
                                if added_seed > 0: msg += f"\n💸 <b>자동 복리 +${added_seed:,.0f}</b> 이 다음 운용 시드에 완벽하게 추가되었습니다!"
                                await context.bot.send_message(chat_id, msg, parse_mode='HTML')
                               
                                try:
                                    img_path = await asyncio.to_thread(
                                        self.view.create_profit_image,
                                        ticker=ticker, profit=new_hist['profit'], yield_pct=new_hist['yield'],
                                        invested=new_hist['invested'], revenue=new_hist['revenue'], end_date=new_hist['end_date']
                                    )
                                    # 🚨 MODIFIED: [제1헌법] os.path.exists 비동기 격리 락온
                                    is_img_exist = await asyncio.to_thread(os.path.exists, img_path) if img_path else False
                                    if img_path and is_img_exist:
                                        def _read_img3(p):
                                            with open(p, 'rb') as f_in: return f_in.read()
                                        img_bytes3 = await asyncio.to_thread(_read_img3, img_path)
                                        if img_path.lower().endswith('.gif'): await context.bot.send_animation(chat_id=chat_id, animation=img_bytes3)
                                        else: await context.bot.send_photo(chat_id=chat_id, photo=img_bytes3)
                                except Exception: pass
                            else:
                                full_ledger2 = await asyncio.to_thread(self.cfg.get_ledger)
                                all_recs = [r for r in full_ledger2 if r['ticker'] != ticker]
                                await asyncio.to_thread(self.cfg._save_json, self.cfg.FILES["LEDGER"], all_recs)
                                await context.bot.send_message(chat_id, f"⚠️ <b>[{ticker} 강제 정산 완료]</b>\n잔고가 0주이나 마이너스 수익 상태이므로 명예의 전당 박제 없이 장부를 비우고 새출발 타점을 장전합니다.", parse_mode='HTML')
                        except Exception: pass

                    return "SUCCESS"

                return "SUCCESS"

    async def _display_ledger(self, ticker, chat_id, context, query=None, message_obj=None, pre_fetched_holdings=None):
        full_ledger = await asyncio.to_thread(self.cfg.get_ledger)
        recs = [r for r in full_ledger if r['ticker'] == ticker]
        
        if not recs:
            msg = f"📭 <b>[{ticker}]</b> 현재 진행 중인 사이클이 없습니다 (보유량 0주)."
        else:
            from collections import OrderedDict
            agg_dict = OrderedDict()
            total_buy = 0.0
            total_sell = 0.0
            
            for rec in recs:
                parts = rec['date'].split('-')
                if len(parts) == 3: date_short = f"{parts[1]}.{parts[2]}"
                else: date_short = rec['date']
                    
                side_str = "🔴매수" if rec['side'] == 'BUY' else "🔵매도"
                key = (date_short, side_str)
                
                if key not in agg_dict: agg_dict[key] = {'qty': 0, 'amt': 0.0}
                    
                agg_dict[key]['qty'] += rec['qty']
                agg_dict[key]['amt'] += (rec['qty'] * rec['price'])
                
                if rec['side'] == 'BUY': total_buy += (rec['qty'] * rec['price'])
                elif rec['side'] == 'SELL': total_sell += (rec['qty'] * rec['price'])
            
            report = f"📜 <b>[ {ticker} 일자별 매매 (통합 변동분) (총 {len(agg_dict)}일) ]</b>\n\n<code>No. 일자   구분  평균단가  수량\n"
            report += "-"*30 + "\n"
            
            idx = 1
            for (date, side), data in agg_dict.items():
                tot_qty = data['qty']
                avg_prc = data['amt'] / tot_qty if tot_qty > 0 else 0.0
                report += f"{idx:<3} {date} {side} ${avg_prc:<6.2f} {tot_qty}주\n"
                idx += 1
                
            report += "-"*30 + "</code>\n"
            
            actual_qty = int(float(pre_fetched_holdings.get(ticker, {'qty': 0})['qty'] or 0)) if pre_fetched_holdings else 0
            actual_avg = float(pre_fetched_holdings.get(ticker, {'avg': 0})['avg'] or 0.0) if pre_fetched_holdings else 0.0
            
            split = await asyncio.to_thread(self.cfg.get_split_count, ticker)
            t_val, _ = await asyncio.to_thread(self.cfg.get_absolute_t_val, ticker, actual_qty, actual_avg)
            
            report += "📊 <b>[ 현재 진행 상황 요약 ]</b>\n"
            report += f"▪️ 현재 T값 : {t_val:.4f} T ({int(split)}분할)\n"
            report += f"▪️ 보유 수량 : {actual_qty} 주 (평단 ${actual_avg:,.2f})\n"
            report += f"▪️ 총 매수액 : ${total_buy:,.2f}\n"
            report += f"▪️ 총 매도액 : ${total_sell:,.2f}"
            
            msg = report

        active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
        keyboard = []
        
        v_mode = await asyncio.to_thread(self.cfg.get_version, ticker)
        if v_mode == "V_REV":
            keyboard.append([InlineKeyboardButton(f"🗄️ {ticker} V-REV 큐(Queue) 정밀 관리", callback_data=f"QUEUE:VIEW:{ticker}")])
            
        row = [InlineKeyboardButton(f"🔄 {t} 장부 업데이트", callback_data=f"REC:SYNC:{t}") for t in active_tickers]
        keyboard.append(row)
        markup = InlineKeyboardMarkup(keyboard)

        if query:
             await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
        elif message_obj:
            await message_obj.edit_text(msg, reply_markup=markup, parse_mode='HTML')
        else:
            await context.bot.send_message(chat_id, msg, reply_markup=markup, parse_mode='HTML')
