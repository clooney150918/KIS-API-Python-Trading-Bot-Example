# ==========================================================
# FILE: telegram_sync_engine.py
# ==========================================================
# 🚨 MODIFIED: [제1헌법 철저 준수] _get_last_trade_date 내부 달력 API(mcal) 스캔 시 GlobalThrottle.wait_api_sync()를 강제 주입하여 썬더링 허드 완벽 차단.
# 🚨 MODIFIED: [Lost Update 궁극 방어] process_auto_sync 내부에서 상태 캐시(vwap_state_REV)를 갱신할 때, 읽기와 쓰기를 단일 동기 스레드 함수(_update_v_state)로 묶은 뒤 GlobalThrottle.get_file_lock()을 100% 팩트 래핑하여 원자적(Atomic) 무결성 사수 완료.
# 🚨 MODIFIED: [유령 잔고 방어 팩트 수술] actual_qty < a_qty_for_check 로 인해 마이너스 값이 산출될 경우 0주 졸업 오인 방어망 결속
# 🚨 NEW: 본진 전략 타점 오염을 원천 차단하기 위해 KIS 실서버 원본 계좌 잔고 및 평단가를 시각적 세션으로 완전 격리 표출
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
import math 
import html 
import functools
import yfinance as yf
import pandas as pd 
import pandas_market_calendars as mcal
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from global_throttle import GlobalThrottle # 🚨 NEW: 중앙 통제소 결속

class TelegramSyncEngine:
    def __init__(self, config, broker, strategy, queue_ledger, view, tx_lock, sync_locks):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.queue_ledger = queue_ledger
        self.view = view
        self.tx_lock = tx_lock
        self.sync_locks = sync_locks

    def _safe_float(self, value):
        try:
            f_val = float(str(value or 0.0).replace(',', ''))
            if math.isnan(f_val) or math.isinf(f_val): return 0.0
            return f_val
        except Exception: return 0.0

    async def _retry_api(self, func, *args, timeout=15.0, default=None, **kwargs):
        for attempt in range(3):
            try:
                if asyncio.iscoroutinefunction(func):
                    return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
                else:
                    p_func = functools.partial(func, *args, **kwargs)
                    return await asyncio.wait_for(asyncio.to_thread(p_func), timeout=timeout)
            except Exception as e:
                if attempt == 2:
                    func_name = getattr(func, '__name__', 'unknown_func')
                    logging.debug(f"🚨 API 래퍼 최종 실패 ({func_name}): {e}")
                    return default
                await asyncio.sleep(1.0 * (2 ** attempt))
        return default

    async def _safe_send(self, context, chat_id, text, timeout=15.0, **kwargs):
        if not chat_id: return None
        try:
            return await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=text, **kwargs), timeout=timeout)
        except Exception as e:
            logging.error(f"🚨 텔레그램 전송 실패: {e}")
            return None

    async def process_auto_sync(self, ticker, chat_id, context, silent_ledger=False):
        if ticker not in self.sync_locks:
            self.sync_locks[ticker] = asyncio.Lock()
            
        if self.sync_locks[ticker].locked(): return "LOCKED"
            
        async with self.sync_locks[ticker]:
            async with self.tx_lock:
                est = ZoneInfo('America/New_York')
                now_est = datetime.datetime.now(est)
                kst = ZoneInfo('Asia/Seoul')
                now_kst = datetime.datetime.now(kst)

                last_split_date = await self._retry_api(self.cfg.get_last_split_date, ticker, default="")
                split_ratio, split_date = await self._retry_api(self.broker.get_recent_stock_split, ticker, last_split_date, default=(0.0, ""))
                
                if split_ratio > 0.0 and split_date != "":
                    await self._retry_api(self.cfg.apply_stock_split, ticker, split_ratio, timeout=10.0)
                    if getattr(self, 'queue_ledger', None):
                        await self._retry_api(self.queue_ledger.apply_stock_split, ticker, split_ratio, timeout=10.0)
                    if hasattr(self.strategy, 'v_avwap_plugin'):
                        await self._retry_api(self.strategy.v_avwap_plugin.apply_stock_split, ticker, split_ratio, now_est, timeout=10.0)
                    
                    try:
                        from assassin_ledger import AssassinLedger
                        a_ledger = await asyncio.wait_for(asyncio.to_thread(AssassinLedger), timeout=5.0)
                        await self._retry_api(a_ledger.apply_stock_split, ticker, split_ratio, timeout=10.0)
                    except Exception as e:
                        logging.error(f"🚨 암살자 장부 액면분할 팩트 적용 실패: {e}")
                    
                    await self._retry_api(self.cfg.set_last_split_date, ticker, split_date, timeout=5.0)
                    
                    split_type = "액면분할" if split_ratio > 1.0 else "액면병합(역분할)"
                    await self._safe_send(context, chat_id, f"✂️ <b>[{html.escape(str(ticker))}] 야후 파이낸스 {split_type} 자동 감지!</b>\n▫️ 감지된 비율: <b>{split_ratio}배</b> (발생일: {html.escape(str(split_date))})\n▫️ 봇이 기존 V14 장부, V-REV 큐 장부, 암살자 장부, AVWAP 상태 캐시의 수량과 평단가를 100% 무인 자동 소급 조정 완료했습니다.", parse_mode='HTML')
             
                def _get_last_trade_date(target_est):
                    GlobalThrottle.wait_api_sync() # 🚨 MODIFIED: 중앙 통제소 락온
                    nyse = mcal.get_calendar('NYSE')
                    return nyse.schedule(start_date=(target_est - datetime.timedelta(days=10)).date(), end_date=target_est.date())

                schedule = await self._retry_api(_get_last_trade_date, now_est, timeout=10.0, default=pd.DataFrame())
                if not schedule.empty:
                    last_trade_date = schedule.index[-1]
                    target_ledger_str = last_trade_date.strftime('%Y-%m-%d')
                else:
                    target_ledger_str = now_est.strftime('%Y-%m-%d')

                res_bal = await self._retry_api(self.broker.get_account_balance, timeout=15.0, default=None)
                if not res_bal:
                    await self._safe_send(context, chat_id, f"❌ <b>[{html.escape(str(ticker))}] API 오류</b>\n잔고를 불러오지 못했습니다.", parse_mode='HTML')
                    return "ERROR"
                    
                holdings = res_bal[1] if isinstance(res_bal, (list, tuple)) and len(res_bal) > 1 else {}
                safe_holdings = holdings if isinstance(holdings, dict) else {}
                safe_ticker_info = safe_holdings.get(ticker) or {'qty': 0, 'avg': 0.0}
                
                actual_qty = int(self._safe_float(safe_ticker_info.get('qty')))
                actual_avg = self._safe_float(safe_ticker_info.get('avg'))

                full_ledger = await self._retry_api(self.cfg.get_ledger, default=[])
                recs_for_check = [r for r in (full_ledger or []) if isinstance(r, dict) and r.get('ticker') == ticker]
                hold_res = await self._retry_api(self.cfg.calculate_holdings, ticker, recs_for_check, default=(0, 0.0, 0.0, 0.0))
                ledger_qty_for_check = hold_res[0] if isinstance(hold_res, tuple) and len(hold_res) > 0 else 0
                
                vrev_ledger_qty_for_check = 0
                is_rev = (await self._retry_api(self.cfg.get_version, ticker, default="V14") == "V_REV")
                
                if is_rev and getattr(self, 'queue_ledger', None):
                    q_data_check = await self._retry_api(self.queue_ledger.get_queue, ticker, default=[])
                    vrev_ledger_qty_for_check = sum(int(self._safe_float(item.get("qty"))) for item in q_data_check if isinstance(item, dict))
                    vrev_total_invested = sum(int(self._safe_float(item.get("qty"))) * self._safe_float(item.get("price")) for item in q_data_check if isinstance(item, dict))
                    
                    if vrev_ledger_qty_for_check > 0: 
                        actual_avg = round(vrev_total_invested / vrev_ledger_qty_for_check, 4)
                 
                max_check_qty = max(ledger_qty_for_check, vrev_ledger_qty_for_check)

                a_qty_for_check = 0
                try:
                    from assassin_ledger import AssassinLedger
                    a_ledger = await asyncio.wait_for(asyncio.to_thread(AssassinLedger), timeout=5.0)
                    a_data_check = await self._retry_api(a_ledger.get_ledger, ticker, default=[])
                    a_qty_for_check = sum(int(self._safe_float(item.get("qty"))) for item in (a_data_check or []))
                except Exception as e:
                    logging.error(f"🚨 암살자 장부 수량 스캔 에러: {e}")
                
                max_check_qty = max(max_check_qty, a_qty_for_check)

                kis_search_start = (now_kst - datetime.timedelta(days=4)).strftime('%Y%m%d')
                query_end_dt = now_kst.strftime('%Y%m%d')

                def filter_to_est(execs_raw):
                    filtered = []
                    if not execs_raw: return filtered
                    for ex in execs_raw:
                        if not isinstance(ex, dict): continue
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
                        raw_execs = await self._retry_api(self.broker.get_execution_history, ticker, kis_search_start, query_end_dt, timeout=15.0, default=[])
                        target_execs = filter_to_est(raw_execs)
                        sold_today = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01")
                        
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
                    raw_execs = await self._retry_api(self.broker.get_execution_history, ticker, kis_search_start, query_end_dt, timeout=15.0, default=[])
                    target_execs = filter_to_est(raw_execs)

                if target_execs:
                    calibrated_count = await self._retry_api(self.cfg.calibrate_ledger_prices, ticker, target_ledger_str, target_execs, timeout=10.0, default=0)
                    if calibrated_count > 0:
                        logging.info(f"🔧 [{ticker}] LOC/MOC 주문 {calibrated_count}건에 대해 실제 체결 단가 소급 업데이트를 완료했습니다.")

                    full_ledger = await self._retry_api(self.cfg.get_ledger, default=[])
                    recs = [r for r in (full_ledger or []) if isinstance(r, dict) and r.get('ticker') == ticker]
                    
                    hold_res2 = await self._retry_api(self.cfg.calculate_holdings, ticker, recs, default=(0,0.0,0.0,0.0))
                    ledger_qty = hold_res2[0] if isinstance(hold_res2, tuple) and len(hold_res2) > 0 else 0
                    avg_price = hold_res2[1] if isinstance(hold_res2, tuple) and len(hold_res2) > 1 else 0.0
                    
                    raw_actual_qty_for_vrev = actual_qty - a_qty_for_check
                    if raw_actual_qty_for_vrev < 0:
                        logging.warning(f"🚨 [{ticker}] KIS 잔고 불일치 감지: KIS({actual_qty}) - 암살자({a_qty_for_check}) = {raw_actual_qty_for_vrev}주. 유령 잔고(Ghost Balance) 방어를 위해 본진 수량을 0주가 아닌 큐 장부 수량으로 강제 보정합니다.")
                        safe_actual_qty_for_vrev = vrev_ledger_qty_for_check if is_rev else ledger_qty
                    else:
                        safe_actual_qty_for_vrev = raw_actual_qty_for_vrev

                    diff = safe_actual_qty_for_vrev - ledger_qty
                    price_diff = abs(actual_avg - avg_price)

                    today_recs = [r for r in recs if r.get('date') == target_ledger_str and 'INIT' not in str(r.get('exec_id', '')) and 'CALIB' not in str(r.get('exec_id', ''))]
                    ledger_today_buy = sum(r.get('qty', 0) for r in today_recs if r.get('side') == 'BUY')
                    ledger_today_sell = sum(r.get('qty', 0) for r in today_recs if r.get('side') == 'SELL')
                    
                    exec_today_buy = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "02")
                    exec_today_sell = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01")
                    
                    needs_reconstruction = (diff != 0)

                    if not needs_reconstruction and price_diff >= 0.01:
                        await self._retry_api(self.cfg.calibrate_avg_price, ticker, actual_avg, timeout=10.0)
                        await self._safe_send(context, chat_id, f"🔧 <b>[{html.escape(str(ticker))}] 장부 평단가 미세 오차({price_diff:.4f}) 교정 완료!</b>", parse_mode='HTML')
                    elif needs_reconstruction:
                        temp_recs = [r for r in recs if r.get('date') != target_ledger_str or 'INIT' in str(r.get('exec_id', ''))]
                        
                        temp_res = await self._retry_api(self.cfg.calculate_holdings, ticker, temp_recs, default=(0,0.0,0.0,0.0))
                        temp_sim_qty = temp_res[0] if isinstance(temp_res, tuple) and len(temp_res) > 0 else 0
                        temp_sim_avg = temp_res[1] if isinstance(temp_res, tuple) and len(temp_res) > 1 else 0.0
                        temp_avg = temp_sim_avg
                        
                        new_target_records = []
                        
                        if target_execs:
                            target_execs.sort(key=lambda x: str(x.get('ord_dt') or '00000000') + str(x.get('ord_tmd') or '000000')) 
                            for ex in target_execs:
                                side_cd = ex.get('sll_buy_dvsn_cd')
                                exec_qty = int(self._safe_float(ex.get('ft_ccld_qty')))
                                exec_price = self._safe_float(ex.get('ft_ccld_unpr3'))
                                
                                if side_cd == "02": 
                                    new_avg = ((temp_sim_qty * temp_sim_avg) + (exec_qty * exec_price)) / (temp_sim_qty + exec_qty) if (temp_sim_qty + exec_qty) != 0 else exec_price
                                    temp_sim_qty += exec_qty
                                    temp_sim_avg = new_avg
                                else: temp_sim_qty -= exec_qty
                                
                                rec_item = {'date': target_ledger_str, 'side': "BUY" if side_cd == "02" else "SELL", 'qty': exec_qty, 'price': exec_price, 'avg_price': temp_sim_avg}
                                if is_rev: rec_item['is_reverse'] = True
                                new_target_records.append(rec_item)
                                
                        gap_qty = safe_actual_qty_for_vrev - temp_sim_qty
                        if gap_qty != 0:
                            calib_side = "BUY" if gap_qty > 0 else "SELL"
                            calib_price = actual_avg
                            actual_clear_price_calib = 0.0

                            if target_execs:
                                sell_execs_calib = [ex for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01"]
                                if sell_execs_calib:
                                    tot_amt_calib = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) * self._safe_float(ex.get('ft_ccld_unpr3')) for ex in sell_execs_calib)
                                    tot_q_calib = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for sell_execs_calib in sell_execs_calib)
                                    if tot_q_calib > 0: actual_clear_price_calib = round(tot_amt_calib / tot_q_calib, 4)
                            
                            if calib_side == "SELL" and actual_avg <= 0.0:
                                if actual_clear_price_calib > 0.0: calib_price = actual_clear_price_calib
                                else: calib_price = temp_sim_avg if temp_sim_avg > 0 else (temp_avg if temp_avg > 0 else 0.01)
                                calib_avg = temp_sim_avg
                            elif calib_side == "BUY" and actual_avg <= 0.0:
                                if actual_clear_price_calib > 0.0:
                                    calib_price = actual_clear_price_calib
                                    calib_avg = actual_clear_price_calib
                                else:
                                    calib_price = temp_sim_avg if temp_sim_avg > 0 else (temp_avg if temp_avg > 0 else 0.01)
                                    calib_avg = temp_sim_avg
                            else:
                                calib_price = actual_avg if actual_avg > 0 else temp_sim_avg
                                calib_avg = actual_avg if actual_avg > 0 else temp_sim_avg
                                
                            calib_item = {'date': target_ledger_str, 'side': calib_side, 'qty': abs(gap_qty), 'price': calib_price, 'avg_price': calib_avg, 'exec_id': f"CALIB_{int(time.time())}", 'desc': "비파괴 보정"}
                            if is_rev: calib_item['is_reverse'] = True
                            new_target_records.append(calib_item)
                            
                        if new_target_records:
                            if safe_actual_qty_for_vrev > 0:
                                for r in new_target_records: r['avg_price'] = actual_avg
                    
                        await self._retry_api(self.cfg.overwrite_incremental_ledger, ticker, temp_recs, new_target_records, timeout=10.0)
                        if gap_qty != 0: await self._safe_send(context, chat_id, f"🔧 <b>[{html.escape(str(ticker))}] 통합 메인 장부(MAIN LEDGER) 비파괴 보정 완료!</b>\n▫️ KIS 실잔고 오차 수량({gap_qty}주)을 역사 보존 상태로 안전하게 교정했습니다.", parse_mode='HTML')

                    if is_rev:
                        q_data_before = await self._retry_api(self.queue_ledger.get_queue, ticker, default=[])
                        vrev_ledger_qty = sum(int(self._safe_float(item.get("qty"))) for item in q_data_before if isinstance(item, dict))
                        sold_today_vrev = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01") if target_execs else 0
                        
                        if safe_actual_qty_for_vrev == 0 and (vrev_ledger_qty > 0 or sold_today_vrev > 0):
                            if sold_today_vrev == 0 and vrev_ledger_qty > 0:
                                await self._safe_send(context, chat_id, f"🚨 <b>[{html.escape(str(ticker))} 유령 잔고 방어 가동]</b>\nKIS 실잔고가 0주로 조회되었으나, 당일 매도 체결 내역이 0건입니다. 통신 오류(Ghost Balance)일 가능성이 매우 높아 장부 강제 소각(자동 졸업)을 차단합니다.\n▫️ HTS 등을 통해 수동으로 100% 전량 매도한 상태라면 <code>/reset</code> 명령어를 사용하여 봇을 초기화하십시오.", parse_mode='HTML')
                                return "GHOST_BALANCE_BLOCKED"

                            added_seed = 0.0
                            _vrev_snap_ok = False
                            snapshot = None
                            actual_clear_price = 0.0
                            tot_q = 0
                
                            if target_execs:
                                sell_execs = [ex for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01"]
                                if sell_execs:
                                    tot_amt = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) * self._safe_float(ex.get('ft_ccld_unpr3')) for ex in sell_execs)
                                    tot_q = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in sell_execs)
                                    if tot_q > 0: actual_clear_price = round(tot_amt / tot_q, 4)

                            if tot_q > vrev_ledger_qty:
                                missing_qty = tot_q - vrev_ledger_qty
                                buy_execs = [ex for ex in (target_execs or []) if ex.get('sll_buy_dvsn_cd') == "02"]
                                temp_invested = sum(self._safe_float(item.get("qty")) * self._safe_float(item.get("price")) for item in q_data_before if isinstance(item, dict))
                                temp_avg = temp_invested / vrev_ledger_qty if vrev_ledger_qty > 0 else 0.0
                                missing_price = temp_avg
                
                                if buy_execs:
                                    b_tot_amt = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) * self._safe_float(ex.get('ft_ccld_unpr3')) for ex in buy_execs)
                                    b_tot_q = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in buy_execs)
                                    if b_tot_q > 0:
                                        q_today_amt = 0.0
                                        q_today_qty = 0
                                        for item in q_data_before:
                                            if isinstance(item, dict) and str(item.get("date", "")).startswith(target_ledger_str):
                                                iq = int(self._safe_float(item.get("qty")))
                                                q_today_qty += iq
                                                q_today_amt += iq * self._safe_float(item.get("price"))
                                        
                                        pure_manual_q = b_tot_q - q_today_qty
                                        pure_manual_amt = b_tot_amt - q_today_amt
                                   
                                        if pure_manual_q >= missing_qty and pure_manual_q > 0 and pure_manual_amt > 0:
                                            derived_price = pure_manual_amt / pure_manual_q
                                            missing_price = round(derived_price, 4)
                                        else: missing_price = round(b_tot_amt / b_tot_q, 4)

                                q_data_before.append({"date": now_est.strftime('%Y-%m-%d %H:%M:%S'), "qty": missing_qty, "price": missing_price, "exec_id": "MANUAL_SYNC"})
                                vrev_ledger_qty = tot_q
                                await self._retry_api(self.queue_ledger.overwrite_queue, ticker, q_data_before, timeout=10.0)

                            total_invested = sum(self._safe_float(item.get("qty")) * self._safe_float(item.get("price")) for item in q_data_before if isinstance(item, dict))
                            q_avg_price = total_invested / vrev_ledger_qty if vrev_ledger_qty > 0 else 0.0

                            curr_p = await self._retry_api(self.broker.get_current_price, ticker, timeout=15.0, default=0.0)
                            clear_price = actual_clear_price if actual_clear_price > 0.0 else (curr_p if curr_p and curr_p > 0 else q_avg_price * 1.006)
                            snapshot = await self._retry_api(self.strategy.capture_vrev_snapshot, ticker, clear_price, q_avg_price, vrev_ledger_qty, timeout=10.0, default={})
                             
                            if snapshot and isinstance(snapshot, dict):
                                realized_pnl = self._safe_float(snapshot.get('realized_pnl', 0.0))
                                yield_pct = self._safe_float(snapshot.get('realized_pnl_pct', 0.0))
                                compound_rate = self._safe_float(await self._retry_api(self.cfg.get_compound_rate, ticker, default=70.0)) / 100.0
                                 
                                if realized_pnl > 0 and compound_rate > 0:
                                    added_seed = realized_pnl * compound_rate
                                    current_seed = self._safe_float(await self._retry_api(self.cfg.get_seed, ticker, default=6720.0))
                                    await self._retry_api(self.cfg.set_seed, ticker, current_seed + added_seed, timeout=10.0)
                                 
                                cap_dt = snapshot.get('captured_at', now_est)
                                cap_dt_str = cap_dt if isinstance(cap_dt, str) else cap_dt.strftime('%Y-%m-%d')
                     
                                start_dt_str = str(q_data_before[0].get('date', ''))[:10] if q_data_before and isinstance(q_data_before[0], dict) else cap_dt_str[:10]
                       
                                hist_data = await self._retry_api(self.cfg._load_json, self.cfg.FILES["HISTORY"], [], default=[])
                                 
                                new_hist = {
                                    "id": int(time.time()), "ticker": ticker, "start_date": start_dt_str, "end_date": cap_dt_str[:10],
                                    "invested": self._safe_float(total_invested), "revenue": self._safe_float(total_invested + realized_pnl),
                                    "profit": realized_pnl, "yield": yield_pct, "trades": q_data_before 
                                }
                
                                hist_data.append(new_hist)
                                await self._retry_api(self.cfg._save_json, self.cfg.FILES["HISTORY"], hist_data, timeout=10.0)
                                _vrev_snap_ok = True
                                    
                            if getattr(self, 'queue_ledger', None):
                                await self._retry_api(self.queue_ledger.sync_with_broker, ticker, 0, timeout=10.0)
                        
                            if _vrev_snap_ok:
                                msg = f"🎉 <b>[{html.escape(str(ticker))}] V-REV 잭팟 스윕(전량 익절) 감지!]</b>\n▫️ 잔고가 0주가 되어 LIFO 큐 지층을 100% 소각(초기화)했습니다."
                                if added_seed > 0: msg += f"\n💸 <b>자동 복리 +${added_seed:,.0f}</b> 이 다음 운용 시드에 완벽하게 추가되었습니다!"
                                await self._safe_send(context, chat_id, msg, parse_mode='HTML')
                                 
                                if snapshot and isinstance(snapshot, dict):
                                   
                                    img_path = await self._retry_api(
                                        self.view.create_profit_image, 
                                        ticker=ticker, 
                                        profit=self._safe_float(snapshot.get('realized_pnl', 0.0)), 
                                        yield_pct=self._safe_float(snapshot.get('realized_pnl_pct', 0.0)), 
                                        invested=self._safe_float(snapshot.get('avg_price', 0.0)) * self._safe_float(snapshot.get('cleared_qty', 0)), 
                                        revenue=self._safe_float(snapshot.get('clear_price', 0.0)) * self._safe_float(snapshot.get('cleared_qty', 0)), 
                                        end_date=cap_dt_str[:10],
                                        timeout=15.0, default=None
                                    )
                                    if img_path:
                                        def _read_img3(p):
                                            with open(p, 'rb') as f_in: return f_in.read()
                                        try:
                                            img_bytes3 = await self._retry_api(_read_img3, img_path)
                                            if str(img_path).lower().endswith('.gif'): await context.bot.send_animation(chat_id=chat_id, animation=img_bytes3)
                                            else: await context.bot.send_photo(chat_id=chat_id, photo=img_bytes3)
                                        except OSError: pass
                            else:
                                await self._safe_send(context, chat_id, f"⚠️ <b>[{html.escape(str(ticker))}] V-REV 0주 강제 정산 완료]</b>\n▫️ 0주를 확인하여 큐를 안전하게 비웠으나 통신 지연으로 졸업 카드는 생략되었습니다.", parse_mode='HTML')
                     
                            pass 
                       
                        if safe_actual_qty_for_vrev == vrev_ledger_qty:
                            pass
                        elif safe_actual_qty_for_vrev > 0 and safe_actual_qty_for_vrev < vrev_ledger_qty:
                            gap_qty = vrev_ledger_qty - safe_actual_qty_for_vrev
                            
                            # 🚨 MODIFIED: [Lost Update 궁극 방어] 읽기와 쓰기를 단일 동기 스레드 함수 내에서 파일 뮤텍스로 100% 팩트 래핑
                            def _update_v_state(tkr, g_qty):
                                f_path = f"data/vwap_state_REV_{tkr}.json"
                                with GlobalThrottle.get_file_lock(f_path):
                                    try:
                                        with open(f_path, 'r', encoding='utf-8') as vf: v_state = json.load(vf)
                                    except Exception:
                                        v_state = {}
                                        
                                    if isinstance(v_state, dict) and "executed" in v_state and isinstance(v_state["executed"], dict) and "SELL_QTY" in v_state["executed"]:
                                        old_sell_qty = v_state["executed"]["SELL_QTY"]
                                        v_state["executed"]["SELL_QTY"] = max(0, old_sell_qty - g_qty)
                                        
                                    fd = None
                                    tmp_path = None
                                    try:
                                        fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(f_path) or '.')
                                        with os.fdopen(fd, 'w', encoding='utf-8') as _vf_out:
                                            fd = None
                                            json.dump(v_state, _vf_out, ensure_ascii=False, indent=4)
                                            _vf_out.flush()
                                            os.fsync(_vf_out.fileno())
                                        os.replace(tmp_path, f_path)
                                        tmp_path = None
                                    except Exception as write_err:
                                        if fd is not None:
                                            try: os.close(fd)
                                            except OSError: pass
                                        if tmp_path:
                                            try: os.remove(tmp_path)
                                            except OSError: pass
                                        raise write_err

                            await self._retry_api(_update_v_state, ticker, gap_qty, timeout=10.0)

                            actual_clear_price_for_sync = 0.0
                            if target_execs:
                                sell_execs_sync = [ex for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01"]
                                if sell_execs_sync:
                                    t_amt = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) * self._safe_float(ex.get('ft_ccld_unpr3')) for ex in sell_execs_sync)
                                    t_q = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in sell_execs_sync)
                                    if t_q > 0: actual_clear_price_for_sync = round(t_amt / t_q, 4)
                            
                            calibrated = False
                            if getattr(self, 'queue_ledger', None):
                                calibrated = await self._retry_api(self.queue_ledger.sync_with_broker, ticker, safe_actual_qty_for_vrev, 0.0, actual_clear_price_for_sync, timeout=10.0, default=False)
                             
                            if calibrated: await self._safe_send(context, chat_id, f"🔧 <b>[{html.escape(str(ticker))}] V-REV 큐(Queue) 비파괴 보정 및 리앵커링 완료!</b>\n▫️ 수동 매도 물량(<b>{gap_qty}주</b>)을 LIFO 큐에서 안전하게 차감하고, 수익금만큼 잔여 지층의 평단가를 일괄 차감했습니다.", parse_mode='HTML')
                             
                        elif safe_actual_qty_for_vrev > 0 and safe_actual_qty_for_vrev > vrev_ledger_qty:
                            gap_qty = safe_actual_qty_for_vrev - vrev_ledger_qty
                            
                            logging.info(f"🛡️ [{ticker}] V-REV 큐 장부 절대주의 가동: KIS 실잔고 초과분({gap_qty}주)을 개인 물량으로 간주하여 편입 차단.")
                            await self._safe_send(context, chat_id, f"🛡️ <b>[{html.escape(str(ticker))}] 개인 장기 물량 격리 보호 (Ghost Balance 차단)</b>\n▫️ KIS 실잔고가 로컬 큐 장부보다 <b>{gap_qty}주</b> 많습니다.\n▫️ 봇 관리를 벗어난 개인 장기 투자 물량으로 간주하여 V-REV 장부에 강제 편입(MANUAL_BUY)하지 않고 안전하게 100% 격리했습니다.\n▫️ 만약 봇 운용을 위해 수동 매수한 물량이라면 <code>/add_q</code> 명령어로 직접 지층을 주입하십시오.", parse_mode='HTML')
                
                if not is_rev:
                    sold_today_v14 = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01") if target_execs else 0
                    
                    if actual_qty == 0 and (ledger_qty > 0 or sold_today_v14 > 0):
                        if sold_today_v14 == 0 and ledger_qty > 0:
                            await self._safe_send(context, chat_id, f"🚨 <b>[{html.escape(str(ticker))} 유령 잔고 방어 가동]</b>\nKIS 실잔고가 0주로 조회되었으나, 당일 매도 체결 내역이 0건입니다. 통신 오류(Ghost Balance)일 가능성이 매우 높아 장부 강제 소각(자동 졸업)을 차단합니다.\n▫️ HTS 등을 통해 수동으로 100% 전량 매도한 상태라면 <code>/reset</code> 명령어를 사용하여 봇을 초기화하십시오.", parse_mode='HTML')
                            return "GHOST_BALANCE_BLOCKED"

                        today_est_str = now_est.strftime('%Y-%m-%d')
                        prev_c = await self._retry_api(self.broker.get_previous_close, ticker, default=0.0)
                        
                        grad_res = await self._retry_api(self.cfg.archive_graduation, ticker, today_est_str, prev_c, timeout=10.0, default=(None, 0.0))
                        new_hist, added_seed = grad_res if isinstance(grad_res, tuple) and len(grad_res) >= 2 else (None, 0.0)

                        if new_hist and isinstance(new_hist, dict):
                            msg = f"🎉 <b>[{html.escape(str(ticker))} 졸업 확인!]</b>\n장부를 명예의 전당에 저장하고 새 사이클을 준비합니다."
                            if added_seed > 0: msg += f"\n💸 <b>자동 복리 +${added_seed:,.0f}</b> 이 다음 운용 시드에 완벽하게 추가되었습니다!"
                            await self._safe_send(context, chat_id, msg, parse_mode='HTML')
                            
                            img_path = await self._retry_api(
                                self.view.create_profit_image,
                                ticker=ticker, 
                                profit=self._safe_float(new_hist.get('profit', 0.0)), 
                                yield_pct=self._safe_float(new_hist.get('yield', 0.0)),
                                invested=self._safe_float(new_hist.get('invested', 0.0)), 
                                revenue=self._safe_float(new_hist.get('revenue', 0.0)), 
                                end_date=new_hist.get('end_date', target_ledger_str),
                                timeout=15.0, default=None
                            )
                            if img_path:
                                def _read_img4(p):
                                    with open(p, 'rb') as f_in: return f_in.read()
                                try:
                                    img_bytes4 = await self._retry_api(_read_img4, img_path)
                                    if str(img_path).lower().endswith('.gif'): await context.bot.send_animation(chat_id=chat_id, animation=img_bytes4)
                                    else: await context.bot.send_photo(chat_id=chat_id, photo=img_bytes4)
                                except OSError: pass
                        else:
                            full_ledger2 = await self._retry_api(self.cfg.get_ledger, default=[])
                            all_recs = [r for r in full_ledger2 if isinstance(r, dict) and r.get('ticker') != ticker]
                            await self._retry_api(self.cfg._save_json, self.cfg.FILES["LEDGER"], all_recs, timeout=10.0)
                            await self._safe_send(context, chat_id, f"⚠️ <b>[{html.escape(str(ticker))} 강제 정산 완료]</b>\n잔고가 0주이나 마이너스 수익 상태이므로 명예의 전당 박제 없이 장부를 비우고 새출발 타점을 장전합니다.", parse_mode='HTML')

                if now_est.time() >= datetime.time(16, 0):
                    try:
                        curr_p_val = await self._retry_api(self.broker.get_current_price, ticker, timeout=10.0)
                        curr_p = self._safe_float(curr_p_val)
                    
                        prev_c_val = await self._retry_api(self.broker.get_previous_close, ticker, timeout=10.0)
                        prev_c = self._safe_float(prev_c_val)
                    
                        ma_5day_val = await self._retry_api(self.broker.get_5day_ma, ticker, timeout=10.0)
                        ma_5day = self._safe_float(ma_5day_val)
                        
                        bal_res = await self._retry_api(self.broker.get_account_balance, timeout=10.0)
                        cash_for_snap = self._safe_float(bal_res[0]) if bal_res else 0.0
                        
                        from scheduler_core import get_budget_allocation
                        active_tickers_list = await asyncio.wait_for(asyncio.to_thread(self.cfg.get_active_tickers), timeout=10.0) or []
                        _, alloc_cash_dict = await asyncio.wait_for(asyncio.to_thread(get_budget_allocation, cash_for_snap, active_tickers_list, self.cfg), timeout=10.0)
                        avail_cash = self._safe_float((alloc_cash_dict or {}).get(ticker, 0.0))
             
                        full_ledger_final = await self._retry_api(self.cfg.get_ledger, timeout=10.0)
                        recs_final = [r for r in (full_ledger_final or []) if isinstance(r, dict) and r.get('ticker') == ticker]
                        hold_res_final = await self._retry_api(self.cfg.calculate_holdings, ticker, recs_final, timeout=10.0)
                  
                        final_qty = hold_res_final[0] if isinstance(hold_res_final, tuple) and len(hold_res_final) > 0 else 0
                        final_avg = hold_res_final[1] if isinstance(hold_res_final, tuple) and len(hold_res_final) > 1 else 0.0
                    
                        if final_qty == 0:
                            curr_p = 0.0
                         
                        await asyncio.wait_for(asyncio.to_thread(
                            self.strategy.get_plan, ticker, curr_p, final_avg, final_qty, prev_c, ma_5day=ma_5day,
                            market_type="REG", available_cash=avail_cash, is_simulation=True, is_snapshot_mode=True
                        ), timeout=15.0)
                        logging.info(f"📸 [{ticker}] 16:05 EST 확정 정산 완료 후 명일(D+1) 대비 스냅샷 박제(Forward-Lock) 성공.")
                    except Exception as e:
                        logging.error(f"🚨 [{ticker}] 16:05 EST 스냅샷 팩트 박제 실패: {e}")

                return "SUCCESS"

    async def _display_ledger(self, ticker, chat_id, context, query=None, message_obj=None, pre_fetched_holdings=None):
        full_ledger = await self._retry_api(self.cfg.get_ledger, default=[])
        recs = [r for r in full_ledger if isinstance(r, dict) and r.get('ticker') == ticker]
        
        report = ""
        
        if not recs:
            report += f"📭 <b>[{html.escape(str(ticker))}]</b> 현재 진행 중인 본진 사이클이 없습니다 (보유량 0주).\n\n"
        else:
            from collections import OrderedDict
            agg_dict = OrderedDict()
            total_buy = 0.0
            total_sell = 0.0
            
            for rec in recs:
                raw_date = str(rec.get('date') or '').split(' ')[0]
                parts = raw_date.split('-')
                if len(parts) == 3: date_short = f"{parts[1]}.{parts[2]}"
                else: date_short = raw_date
                     
                side_str = "🔴매수" if rec.get('side') == 'BUY' else "🔵매도"
                key = (date_short, side_str)
            
                if key not in agg_dict: agg_dict[key] = {'qty': 0, 'amt': 0.0}

                agg_dict[key]['qty'] += int(self._safe_float(rec.get('qty')))
                agg_dict[key]['amt'] += (int(self._safe_float(rec.get('qty'))) * self._safe_float(rec.get('price')))
            
                if rec.get('side') == 'BUY': total_buy += (int(self._safe_float(rec.get('qty'))) * self._safe_float(rec.get('price')))
                elif rec.get('side') == 'SELL': total_sell += (int(self._safe_float(rec.get('qty'))) * self._safe_float(rec.get('price')))
            
            report += f"📜 <b>[ {html.escape(str(ticker))} 일자별 매매 (통합 변동분) (총 {len(agg_dict)}일) ]</b>\n\n<code>No. 일자   구분  평균단가  수량\n"
            report += "-"*30 + "\n"
            
            idx = 1
            for (date, side), data in agg_dict.items():
                tot_qty = data['qty']
                avg_prc = data['amt'] / tot_qty if tot_qty != 0 else 0.0
                report += f"{idx:<3} {date} {side} ${avg_prc:<6.2f} {tot_qty}주\n"
                idx += 1
                 
            report += "-"*30 + "</code>\n\n"
        
        safe_holdings = pre_fetched_holdings if isinstance(pre_fetched_holdings, dict) else {}
        actual_qty = int(self._safe_float((safe_holdings.get(ticker) or {'qty': 0}).get('qty')))
        actual_avg = self._safe_float((safe_holdings.get(ticker) or {'avg': 0}).get('avg'))
        
        kis_raw_qty = actual_qty
        kis_raw_avg = actual_avg

        v_mode = await self._retry_api(self.cfg.get_version, ticker, default="V14")
        
        if v_mode == "V_REV":
            if not getattr(self, 'queue_ledger', None):
                from queue_ledger import QueueLedger
                self.queue_ledger = await asyncio.to_thread(QueueLedger)
            
            if getattr(self, 'queue_ledger', None):
                q_data_ui = await self._retry_api(self.queue_ledger.get_queue, ticker, default=[])
                vrev_inv_ui = sum(int(self._safe_float(item.get('qty'))) * self._safe_float(item.get('price')) for item in q_data_ui if isinstance(item, dict))
                vrev_q_ui = sum(int(self._safe_float(item.get('qty'))) for item in q_data_ui if isinstance(item, dict))
                
                actual_qty = vrev_q_ui
                
                if vrev_q_ui > 0: actual_avg = round(vrev_inv_ui / vrev_q_ui, 4)
                else: actual_avg = 0.0

        split = await self._retry_api(self.cfg.get_split_count, ticker, default=40.0)
        t_val_res = await self._retry_api(self.cfg.get_absolute_t_val, ticker, actual_qty, actual_avg, default=(0.0, 0.0))
        t_val = t_val_res[0] if isinstance(t_val_res, tuple) and len(t_val_res) > 0 else 0.0
         
        t_val_safe = self._safe_float(t_val)
        split_safe = int(self._safe_float(split))

        report += "📊 <b>[ 현재 본진 진행 상황 요약 ]</b>\n"
        report += f"▪️ 현재 T값 : {t_val_safe:.4f} T ({split_safe}분할)\n"
        report += f"▪️ 보유 수량 : {actual_qty} 주 (평단 ${actual_avg:,.2f})\n"
        
        if recs:
            report += f"▪️ 총 매수액 : ${total_buy:,.2f}\n"
            report += f"▪️ 총 매도액 : ${total_sell:,.2f}\n"

        try:
            from assassin_ledger import AssassinLedger
            a_ledger = await asyncio.wait_for(asyncio.to_thread(AssassinLedger), timeout=5.0)
            a_data = await self._retry_api(a_ledger.get_ledger, ticker, default=[])
            
            report += f"\n🔫 <b>[ 암살자 (aVWAP) 독립 장부 상태 ]</b>\n"
            
            if a_data and isinstance(a_data, list) and len(a_data) > 0:
                a_qty = sum(int(self._safe_float(r.get('qty'))) for r in a_data)
                if a_qty > 0:
                    a_inv = sum(int(self._safe_float(r.get('qty'))) * self._safe_float(r.get('price')) for r in a_data)
                    a_avg = a_inv / a_qty if a_qty > 0 else 0.0
                    report += f"▪️ 진행 상태 : <b>ON (교전/오버나이트 중)</b>\n"
                    report += f"▪️ 락온 수량 : <b>{a_qty} 주</b>\n"
                    report += f"▪️ 진입 평단가: <b>${a_avg:,.2f}</b>\n"
                else:
                    report += f"▪️ 진행 상태 : <b>STANDBY (타점 감시 중 / 0주)</b>\n"
            else:
                report += f"▪️ 진행 상태 : <b>STANDBY (타점 감시 중 / 0주)</b>\n"
                
        except Exception as e:
            logging.error(f"🚨 암살자 장부 UI 렌더링 실패: {e}")

        report += f"\n🏛️ <b>[ KIS 실서버 종합 원장 계좌 정보 ]</b>\n"
        report += f"▪️ KIS 총 수량 : <b>{kis_raw_qty} 주</b> (본진 {actual_qty}주 + 암살자 {kis_raw_qty - actual_qty if kis_raw_qty > actual_qty else 0}주)\n"
        report += f"▪️ KIS 실평단가 : <b>${kis_raw_avg:,.2f}</b> (증권사 앱 표출 팩트 단가)\n"

        msg = report
        
        if len(msg) > 4000:
            msg = msg[:3900] + "\n\n... (장부 내역이 너무 길어 하단이 생략되었습니다) ✂️"

        active_tickers = await self._retry_api(self.cfg.get_active_tickers, default=[])
        keyboard = []
         
        if v_mode == "V_REV":
            keyboard.append([InlineKeyboardButton(f"🗄️ {html.escape(str(ticker))} V-REV 큐(Queue) 정밀 관리", callback_data=f"QUEUE:VIEW:{ticker}")])
             
        row = [InlineKeyboardButton(f"🔄 {html.escape(str(t))} 장부 업데이트", callback_data=f"REC:SYNC:{t}") for t in active_tickers if isinstance(t, str)]
        if row: keyboard.append(row)
        markup = InlineKeyboardMarkup(keyboard)

        if query:
            try: await asyncio.wait_for(query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML'), timeout=15.0)
            except Exception: pass
        elif message_obj:
            try: await asyncio.wait_for(message_obj.edit_text(msg, reply_markup=markup, parse_mode='HTML'), timeout=15.0)
            except Exception: pass
        else:
            await self._safe_send(context, chat_id, msg, reply_markup=markup, parse_mode='HTML')
