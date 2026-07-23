# ==========================================================
# FILE: scheduler_core.py
# ==========================================================
# 🚨 MODIFIED: [제1헌법 철저 준수] is_market_open 내부 달력 API(mcal) 스캔 전 파편화된 time.sleep(0.06)을 영구 소각하고, GlobalThrottle.wait_api_sync() 중앙 통제소 락온 완료.
# 🚨 MODIFIED: [Lost Update 궁극 방어] scheduled_auto_sync 내부 _check_and_set_lock에서 sync_lock.json 생성 및 갱신 시 GlobalThrottle.get_file_lock() 100% 팩트 래핑 완료.
# 🚨 MODIFIED: [본진 졸업 마비 패러독스 수술] 암살자가 오버나이트를 수행하여 계좌에 물량이 남아있더라도, `process_realtime_graduation`에서 KIS 잔고에서 암살자 장부(`AssassinLedger`) 수량을 차감하여 본진 물량만을 정확히 추출, 0주 새출발 졸업망이 정상 가동되도록 팩트 락온. 단, 음수가 발생할 경우 큐 장부를 교차 검증하여 조기 졸업을 안전하게 차단(Bypass).
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import asyncio
import math
import os
import time
import json
import tempfile
import glob
import random
import pandas_market_calendars as mcal
import html
from global_throttle import GlobalThrottle # 🚨 NEW: 중앙 통제소 결속

def _safe_float(val):
    try:
        f_val = float(str(val or 0.0).replace(',', ''))
        if math.isnan(f_val) or math.isinf(f_val): return 0.0
        return f_val
    except Exception:
        return 0.0

def _read_json_sync(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f: return json.load(f)
    except OSError: pass
    except json.JSONDecodeError: pass
    return {}

def _atomic_write_json_sync(filepath, data):
    dir_name = os.path.dirname(filepath) or '.'
    try: os.makedirs(dir_name, exist_ok=True)
    except OSError: pass
    
    fd = None
    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(dir=dir_name, text=True)
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            fd = None
            json.dump(data, f, ensure_ascii=False, indent=4)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, filepath)
        tmp_path = None
    except Exception as e:
        if fd is not None:
            try: os.close(fd)
            except OSError: pass
        if tmp_path:
            try: os.remove(tmp_path)
            except OSError: pass
        raise e

async def async_retry(func, *args, default=None, timeout=10.0, **kwargs):
    for attempt in range(3):
        try:
            return await asyncio.wait_for(asyncio.to_thread(func, *args, **kwargs), timeout=timeout)
        except asyncio.TimeoutError:
            if attempt < 2: await asyncio.sleep(1.0 * (2 ** attempt))
            else: return default
        except Exception as e:
            if attempt < 2: await asyncio.sleep(1.0 * (2 ** attempt))
            else: return default

def is_market_open():
    for attempt in range(3):
        try:
            GlobalThrottle.wait_api_sync() # 🚨 MODIFIED: 파편화된 sleep 소각 및 중앙 통제소 락온
            est = ZoneInfo('America/New_York')
            today = datetime.datetime.now(est)
            if today.weekday() >= 5: return False
           
            nyse = mcal.get_calendar('NYSE')
            schedule = nyse.schedule(start_date=today.date(), end_date=today.date())
            if not schedule.empty: return True
            else: return False
        except Exception as e:
            if attempt == 2:
                est = ZoneInfo('America/New_York')
                return datetime.datetime.now(est).weekday() < 5
            time.sleep(1.0 * (2 ** attempt))

def get_budget_allocation(cash, tickers, cfg):
    sorted_tickers = sorted(tickers or [], key=lambda x: 0 if x == "SOXL" else (1 if x == "TQQQ" else 2))
    allocated = {}
    free_cash = _safe_float(cash)
    
    base_portions = {}
    for tx in sorted_tickers:
        version = getattr(cfg, 'get_version', lambda x: "V14")(tx)
        if version == "V_REV":
            base_portions[tx] = _safe_float(getattr(cfg, 'get_seed', lambda x: 0.0)(tx)) * 0.15
        else:
            split = int(_safe_float(getattr(cfg, 'get_split_count', lambda x: 40)(tx)))
            if split <= 0: split = 40
            seed = _safe_float(getattr(cfg, 'get_seed', lambda x: 0.0)(tx))
            base_portions[tx] = seed / split

    for tx in sorted_tickers:
        req = base_portions[tx]
        version = getattr(cfg, 'get_version', lambda x: "V14")(tx)
        if version == "V_REV":
            allocated[tx] = req
            free_cash = max(0.0, free_cash - req) 
        else:
            if free_cash >= req:
                allocated[tx] = req
                free_cash = max(0.0, free_cash - req) 
            else:
                allocated[tx] = 0.0

    v14_active = [tx for tx in sorted_tickers if getattr(cfg, 'get_version', lambda x: "V14")(tx) != "V_REV" and allocated.get(tx, 0.0) > 0]
    if v14_active and free_cash > 0:
        surplus = free_cash / len(v14_active)
        for tx in v14_active:
            allocated[tx] += surplus

    return sorted_tickers, allocated

def perform_self_cleaning():
    try:
        now = time.time()
        seven_days = 7 * 24 * 3600
        one_day = 24 * 3600
        target_patterns = [
            ("logs/bot_app_*.log", seven_days),          
            ("logs/bot_app.log.*", seven_days),          
            ("data/daily_snapshot_*.json", seven_days),  
            ("data/vwap_state_*.json", seven_days),      
            ("data/profit_*.png", seven_days),           
            ("data/profit_*.gif", seven_days),           
            ("data/*.bak_*", seven_days),       
            ("data/tmp*", one_day),  
            ("logs/tmp*", one_day)
        ]
        for pattern, max_age in target_patterns:
            for f in glob.glob(pattern):
                try:
                    if os.stat(f).st_mtime < now - max_age: os.remove(f)
                except OSError: pass
    except Exception as e:
        logging.error(f"🧹 자정(Self-Cleaning) 작업 중 시스템 오류 발생: {e}")

async def scheduled_self_cleaning(context):
    try:
        await asyncio.wait_for(asyncio.to_thread(perform_self_cleaning), timeout=60.0)
        logging.info("🧹 [시스템 자정 작업 완료] 7일 초과 낡은 로그/스냅샷 및 임시 파일 GC(소각) 완료")
    except Exception as e:
        logging.error(f"🚨 [Self-Cleaning] 가비지 컬렉션(GC) 에러: {e}")

async def scheduled_token_check(context):
    job = getattr(context, 'job', None)
    app_data = getattr(job, 'data', {}) if job else {}
    if not isinstance(app_data, dict): app_data = {}
   
    broker = app_data.get('broker')
    if not broker: return
    
    jitter_seconds = random.randint(0, 180)
    await asyncio.sleep(jitter_seconds)
    await async_retry(broker._get_access_token, force=True)
    logging.info("🔑 [API 토큰 갱신] 토큰 갱신이 안전하게 완료되었습니다.")

async def scheduled_force_reset(context):
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    if not (3 <= now_est.hour <= 5): return

    async def _do_force_reset():
        is_open = False
        for attempt in range(3):
            try:
                is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=10.0)
                break
            except Exception:
                if attempt == 2: is_open = now_est.weekday() < 5
                else: await asyncio.sleep(1.0 * (2 ** attempt))

        job = getattr(context, 'job', None)
        app_data = getattr(job, 'data', {}) if job else {}
        if not isinstance(app_data, dict): app_data = {}
        
        cfg = app_data.get('cfg')
        broker = app_data.get('broker')
        tx_lock = app_data.get('tx_lock')
        strategy = app_data.get('strategy')
        chat_id = getattr(job, 'chat_id', None)

        if not is_open:
            if chat_id:
                try: 
                    await asyncio.wait_for(
                        context.bot.send_message(chat_id=chat_id, text="⛔ <b>오늘은 휴장일입니다. 초기화를 스킵합니다.</b>", parse_mode='HTML'),
                        timeout=15.0
                    )
                except Exception: pass
            return
        
        if not cfg or not broker or not tx_lock: return
        
        try:
            await asyncio.wait_for(asyncio.to_thread(cfg.reset_locks), timeout=10.0)
        except Exception as e:
            logging.error(f"🚨 일일 초기화 락 해제 타임아웃: {e}")
        
        res = None
        holdings = {}
        cash_val = 0.0
        async with tx_lock:
            for attempt in range(3):
                try:
                    await asyncio.sleep(0.06)
                    res = await asyncio.wait_for(asyncio.to_thread(broker.get_account_balance), timeout=10.0)
                    cash_val = _safe_float(res[0]) if isinstance(res, (list, tuple)) and len(res) > 0 else 0.0
                    raw_h = res[1] if isinstance(res, (list, tuple)) and len(res) > 1 else {}
                    holdings = raw_h if isinstance(raw_h, dict) else {}
                    break
                except Exception:
                    if attempt == 2: holdings = {}
                    else: await asyncio.sleep(1.0 * (2 ** attempt))
                 
        msg_addons = ""
        
        try:
            active_tickers = await asyncio.wait_for(asyncio.to_thread(cfg.get_active_tickers), timeout=10.0)
        except Exception:
            active_tickers = []
        if not isinstance(active_tickers, list): active_tickers = []
   
        alloc_cash_dict = {}
        try:
            alloc_res = await asyncio.wait_for(asyncio.to_thread(get_budget_allocation, cash_val, active_tickers, cfg), timeout=10.0)
            alloc_cash_dict = alloc_res[1] if isinstance(alloc_res, (list, tuple)) and len(alloc_res) > 1 else {}
        except Exception as e:
            logging.error(f"🚨 일일 초기화 예산 할당 에러: {e}")

        for t in active_tickers:
            try:
                await asyncio.sleep(0.06)
                 
                version = "V14"
                try: version = await asyncio.wait_for(asyncio.to_thread(cfg.get_version, t), timeout=5.0)
                except Exception: pass
                
                rev_state = {}
                try: 
                    rev_state_raw = await asyncio.wait_for(asyncio.to_thread(cfg.get_reverse_state, t), timeout=5.0)
                    rev_state = rev_state_raw if isinstance(rev_state_raw, dict) else {}
                except Exception: pass
                
                safe_h_data = holdings.get(t) if isinstance(holdings.get(t), dict) else {}
                actual_avg = _safe_float(safe_h_data.get('avg', 0.0))
                actual_qty = int(_safe_float(safe_h_data.get('qty', 0)))
                
                curr_p, prev_c = 0.0, 0.0
                for attempt in range(3):
                    try:
                        await asyncio.sleep(0.06)
                        curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, t), timeout=10.0)
                        curr_p = _safe_float(curr_p_val)
                        await asyncio.sleep(0.06)
                        prev_c_val = await asyncio.wait_for(asyncio.to_thread(broker.get_previous_close, t), timeout=10.0)
                        prev_c = _safe_float(prev_c_val)
                        break
                    except Exception:
                        if attempt == 2: pass
                        else: await asyncio.sleep(1.0 * (2 ** attempt))

                ma_5day = 0.0
                for attempt in range(3):
                    try:
                        await asyncio.sleep(0.06)
                        ma_5day_val = await asyncio.wait_for(asyncio.to_thread(broker.get_5day_ma, t), timeout=10.0)
                        ma_5day = _safe_float(ma_5day_val)
                        break
                    except Exception:
                        if attempt == 2: ma_5day = 0.0
                        else: await asyncio.sleep(1.0 * (2 ** attempt))

                if version == "V_REV":
                    if curr_p > 0 and actual_avg > 0:
                        curr_ret = (curr_p - actual_avg) / actual_avg * 100.0
                        exit_target = _safe_float(rev_state.get("exit_target", 0.0))
                        
                        if curr_ret >= exit_target:
                            await asyncio.wait_for(asyncio.to_thread(cfg.set_reverse_state, t, False, 0, 0.0), timeout=5.0)
                            
                            ledger_data = []
                            try: ledger_data = await asyncio.wait_for(asyncio.to_thread(cfg.get_ledger), timeout=10.0)
                            except Exception: pass
                            
                            changed = False
                            if isinstance(ledger_data, list):
                                for lr in ledger_data:
                                    if isinstance(lr, dict) and lr.get('ticker') == t and lr.get('is_reverse', False):
                                        lr['is_reverse'] = False
                                        changed = True
                                if changed:
                                    await asyncio.wait_for(asyncio.to_thread(cfg._save_json, cfg.FILES["LEDGER"], ledger_data), timeout=10.0)
                            safe_t = html.escape(str(t))
                            msg_addons += f"\n🌤️ <b>[{safe_t}] 리버스 목표 달성({curr_ret:.2f}%)!</b> 격리 병동 졸업 및 일반 모드 복귀 완료!"
                        else:
                            await asyncio.wait_for(asyncio.to_thread(cfg.increment_reverse_day, t), timeout=5.0)
                else:
                    await asyncio.wait_for(asyncio.to_thread(cfg.increment_reverse_day, t), timeout=5.0)

                logging.info(f"📸 [{t}] 04:00 AM 기상 완료. (스냅샷은 전일 16:05 EST에 사전 박제(Forward-Lock)되었으므로 생성을 바이패스합니다.)")

            except Exception as e:
                logging.error(f"🚨 [{t}] 일일 초기화 단일 종목 에러 (Cascade 방어): {e}")

        final_msg = f"🔓 <b>[04:00 EST] 시스템 일일 초기화 완료 (매매 잠금 해제 & 고점 관측 센서 가동)</b>" + msg_addons
        if chat_id:
            try: 
                await asyncio.wait_for(
                    context.bot.send_message(chat_id=chat_id, text=final_msg, parse_mode='HTML'),
                    timeout=15.0
                )
            except Exception: pass
    
        
    try:
        await asyncio.wait_for(_do_force_reset(), timeout=180.0)
    except Exception as e:
        logging.error(f"🚨 [force_reset] 전역 타임아웃: {e}")

# ==============================================================
# 1. 🎓 실시간 조기 졸업 정산 (Scenario 2)
# ==============================================================
async def process_realtime_graduation(ticker, cfg, broker, queue_ledger, chat_id, context, tx_lock):
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    if now_est.time() >= datetime.time(15, 15): return
        
    async with tx_lock:
        res = None
        holdings = {}
        for attempt in range(3):
            try:
                await asyncio.sleep(0.06)
                res = await asyncio.wait_for(asyncio.to_thread(broker.get_account_balance), timeout=10.0)
                if isinstance(res, (list, tuple)) and len(res) > 1:
                    holdings = res[1] if isinstance(res[1], dict) else {}
                break
            except Exception:
                if attempt == 2: return
                await asyncio.sleep(1.0 * (2 ** attempt))
                 
        safe_holdings_t = holdings.get(ticker) if isinstance(holdings.get(ticker), dict) else {}
        kis_qty = int(_safe_float(safe_holdings_t.get('qty', 0)))
        
        a_qty = 0
        try:
            from assassin_ledger import AssassinLedger
            a_ledger = await asyncio.wait_for(asyncio.to_thread(AssassinLedger), timeout=5.0)
            a_data = await async_retry(a_ledger.get_ledger, ticker, default=[])
            a_qty = sum(int(_safe_float(l.get('qty'))) for l in (a_data or []))
        except Exception as e:
            logging.error(f"🚨 [{ticker}] 조기 졸업 스캔 중 암살자 장부 로드 에러: {e}")
            
        pure_vrev_qty = kis_qty - a_qty
        
        # 🚨 MODIFIED: [본진 0주 오인 방어막] 음수 산출 시 큐 장부를 교차 검증하여 조기 졸업을 강제 차단(Bypass)
        if pure_vrev_qty <= 0:
            if pure_vrev_qty < 0:
                q_ledger_data = await async_retry(queue_ledger.get_queue, ticker, default=[])
                q_qty = sum(int(_safe_float(item.get("qty"))) for item in (q_ledger_data or []) if isinstance(item, dict))
                if q_qty > 0:
                    logging.warning(f"🚨 [{ticker}] 조기 졸업 스캔 우회: KIS 잔고({kis_qty}주) - 암살자({a_qty}주) 연산 시 음수({pure_vrev_qty}주) 발생. 큐 장부({q_qty}주)가 존재하므로 조기 졸업(0주 잭팟) 판정을 강제 차단합니다.")
                    return
            pure_vrev_qty = 0
        
        if pure_vrev_qty == 0:
            try:
                ledger = []
                try: ledger = await asyncio.wait_for(asyncio.to_thread(cfg.get_ledger), timeout=10.0)
                except Exception: pass
                
                target_recs = [r for r in (ledger or []) if isinstance(r, dict) and r.get('ticker') == ticker]
                
                ledger_qty, avg_price, invested, sold = 0, 0.0, 0.0, 0.0
                try:
                    ledger_qty, avg_price, invested, sold = await asyncio.wait_for(asyncio.to_thread(cfg.calculate_holdings, ticker, target_recs), timeout=10.0)
                except Exception: pass
                
                if ledger_qty > 0:
                    logging.info(f"🎓 [{ticker}] 실시간 조기 졸업 조건 충족 (15:15 이전 전량 익절 팩트).")
                    today_str = now_est.strftime('%Y-%m-%d')
                    
                    hist, added_seed = None, 0.0
                    try:
                        grad_res = await asyncio.wait_for(asyncio.to_thread(cfg.archive_graduation, ticker, today_str, 0.0), timeout=15.0)
                        if isinstance(grad_res, tuple) and len(grad_res) >= 2: hist, added_seed = grad_res
                    except Exception as e: logging.error(f"🚨 조기졸업 기록 타임아웃: {e}")
                    
                    if hist:
                        if queue_ledger:
                            try: await asyncio.wait_for(asyncio.to_thread(queue_ledger.clear_queue, ticker), timeout=5.0)
                            except Exception: pass
                            
                        msg = f"🎓 <b>[{html.escape(str(ticker))}] 실시간 조기 졸업 (Scenario 2) 완료!</b>\n"
                        msg += f"▫️ 15:15 EST 이전 전량 익절이 감지되었습니다.\n"
                        msg += f"▫️ 수익금: <b>${_safe_float(hist.get('profit', 0.0)):.2f}</b>\n▫️ 장부와 큐(Queue)가 즉시 100% 소각되었습니다."
                        try: 
                            await asyncio.wait_for(
                                context.bot.send_message(chat_id, msg, parse_mode='HTML'),
                                timeout=15.0
                            )
                        except Exception: pass

                        try:
                            job = getattr(context, 'job', None)
                            app_data = {}
                            if job and getattr(job, 'data', None) and isinstance(job.data, dict):
                                app_data = job.data
                            else:
                                bot_data = getattr(context, 'bot_data', {})
                                app_data = bot_data.get('app_data', {}) if isinstance(bot_data.get('app_data'), dict) else {}
                                
                            strategy = app_data.get('strategy')
                            
                            if strategy:
                                cash_val = _safe_float(res[0]) if isinstance(res, (list, tuple)) and len(res) > 0 else 0.0
                                alloc_res = await asyncio.wait_for(asyncio.to_thread(get_budget_allocation, cash_val, [ticker], cfg), timeout=10.0)
                                alloc_cash_dict = alloc_res[1] if isinstance(alloc_res, (list, tuple)) and len(alloc_res) > 1 else {}
                                available_cash = _safe_float(alloc_cash_dict.get(ticker, 0.0))
                                
                                curr_p = 0.0
                                for attempt in range(3):
                                    try:
                                        await asyncio.sleep(0.06)
                                        curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, ticker), timeout=10.0)
                                        curr_p = _safe_float(curr_p_val)
                                        break
                                    except Exception:
                                        if attempt == 2: curr_p = 0.0
                                        else: await asyncio.sleep(1.0 * (2 ** attempt))

                                prev_c = 0.0
                                for attempt in range(3):
                                    try:
                                        await asyncio.sleep(0.06)
                                        prev_c_val = await asyncio.wait_for(asyncio.to_thread(broker.get_previous_close, ticker), timeout=10.0)
                                        prev_c = _safe_float(prev_c_val)
                                        break
                                    except Exception:
                                        if attempt == 2: prev_c = 0.0
                                        else: await asyncio.sleep(1.0 * (2 ** attempt))

                                if prev_c <= 0.0:
                                    prev_c = curr_p

                                plan = {}
                                try:
                                    plan = await asyncio.wait_for(
                                        asyncio.to_thread(
                                            strategy.get_plan, ticker, curr_p, 0.0, 0, prev_c, ma_5day=0.0,
                                            market_type="REG", available_cash=available_cash,
                                            is_simulation=True, is_snapshot_mode=True
                                        ),
                                        timeout=15.0
                                    )
                                except Exception as e: logging.error(f"🚨 조기졸업 새출발 연산 타임아웃: {e}")
                                
                                if isinstance(plan, dict) and plan.get('core_orders'):
                                    sent_orders = 0
                                    for o in plan['core_orders']:
                                        if not isinstance(o, dict) or str(o.get('side')) != 'BUY': continue
                                        
                                        o_qty = int(_safe_float(o.get('qty')))
                                        o_price = _safe_float(o.get('price'))
                                        o_type = str(o.get('type', 'LOC'))
                                        
                                        for s_attempt in range(3):
                                            try:
                                                await asyncio.sleep(0.06)
                                                if o_type == 'VWAP':
                                                    o_type = 'LOC'  
                                                    
                                                ord_res = await asyncio.wait_for(
                                                    asyncio.to_thread(broker.send_order, ticker, 'BUY', o_qty, o_price, o_type), 
                                                    timeout=15.0
                                                )
                                                if isinstance(ord_res, dict) and ord_res.get('rt_cd') == '0':
                                                    sent_orders += 1
                                                break
                                            except Exception as e:
                                                if s_attempt == 2: logging.error(f"🚨 [{ticker}] 재진입 덫 장전 에러: {e}")
                                                else: await asyncio.sleep(1.0 * (2 ** s_attempt))
                                                
                                    if sent_orders > 0:
                                        reentry_msg = f"🔄 <b>[{html.escape(str(ticker))}] 당일 조기 졸업 ➔ 새 사이클(15% 고정 예산) 재진입 완료!</b>\n▫️ 0주 새출발 타점을 산출하여 LOC 매수 덫을 성공적으로 전송했습니다."
                                        try: 
                                            await asyncio.wait_for(
                                                context.bot.send_message(chat_id, reentry_msg, parse_mode='HTML'),
                                                timeout=15.0
                                            )
                                        except Exception: pass
                        except Exception as re_e:
                            logging.error(f"🚨 [{ticker}] 조기 졸업 후 강제 재진입 파이프라인 에러: {re_e}")

            except Exception as e:
                logging.error(f"🚨 [{ticker}] 실시간 조기 졸업 처리 에러: {e}")

# ==============================================================
# 2. 🏛️ 16:05 EST 정규 정산 (Scenario 1, 3 & Bypass)
# ==============================================================
async def scheduled_auto_sync(context):
    logging.info("✅ [확정 정산] 16:05 EST 팩트 기반 확정 정산 엔진 다이렉트 가동")
    job = getattr(context, 'job', None)
    app_data = getattr(job, 'data', {}) if job else {}
    if not isinstance(app_data, dict): app_data = {}
    
    tx_lock = app_data.get('tx_lock')
    cfg = app_data.get('cfg')
    broker = app_data.get('broker')
    bot = app_data.get('bot')
    chat_id = getattr(job, 'chat_id', None)

    if not tx_lock or not cfg or not broker or not bot or not chat_id: return
    
    def _check_and_set_lock():
        est_tz = ZoneInfo('America/New_York')
        today_est = datetime.datetime.now(est_tz).strftime("%Y-%m-%d")
        lock_file = "data/sync_lock.json"
        
        # 🚨 MODIFIED: [Lost Update 궁극 방어] 파일 뮤텍스 100% 팩트 래핑
        with GlobalThrottle.get_file_lock(lock_file):
            try: os.makedirs("data", exist_ok=True)
            except OSError: pass
            try:
                with open(lock_file, "r", encoding="utf-8") as f:
                    lock_data = json.load(f)
                    if isinstance(lock_data, dict) and lock_data.get("last_sync") == today_est: return False, today_est
            except Exception: pass
    
            fd = None; tmp_path = None
            try:
                fd, tmp_path = tempfile.mkstemp(dir="data", text=True)
                with os.fdopen(fd, 'w', encoding="utf-8") as f:
                    fd = None
                    json.dump({"last_sync": today_est}, f)
                    f.flush(); os.fsync(f.fileno())
                os.replace(tmp_path, lock_file)
                tmp_path = None
            except Exception:
                if fd is not None:
                    try: os.close(fd)
                    except OSError: pass
                if tmp_path:
                    try: os.remove(tmp_path)
                    except OSError: pass
            return True, today_est

    can_run, today_est = await async_retry(_check_and_set_lock, default=(False, ""))
    if not can_run: return

    status_msg = None
    try: 
        status_msg = await asyncio.wait_for(
            context.bot.send_message(chat_id=chat_id, text=f"📝 <b>[16:05 EST] 장부 자동 동기화(무결성 검증)를 시작합니다.</b>", parse_mode='HTML'),
            timeout=15.0
        )
    except Exception: pass
    
    success_tickers = []
    try:
        active_tickers = await asyncio.wait_for(asyncio.to_thread(cfg.get_active_tickers), timeout=10.0)
    except Exception:
        active_tickers = []
    if not isinstance(active_tickers, list): active_tickers = []
    
    for t in active_tickers:
        try:
            await asyncio.sleep(0.06)
            res = await bot.sync_engine.process_auto_sync(t, chat_id, context, silent_ledger=True)
            if res == "SUCCESS": success_tickers.append(t)
        except Exception as e:
            logging.error(f"🚨 [{t}] 확정 정산 단일 종목 에러 (Cascade 방어): {e}")
            
    if success_tickers:
        res = None
        holdings = {}
        async with tx_lock:
            for attempt in range(3):
                try:
                    await asyncio.sleep(0.06)
                    res = await asyncio.wait_for(asyncio.to_thread(broker.get_account_balance), timeout=10.0)
                    raw_h = res[1] if isinstance(res, (list, tuple)) and len(res) > 1 else {}
                    holdings = raw_h if isinstance(raw_h, dict) else {}
                    break
                except Exception:
                    if attempt == 2: holdings = {}
                    else: await asyncio.sleep(1.0 * (2 ** attempt))
                    
        await bot.sync_engine._display_ledger(success_tickers[0], chat_id, context, message_obj=status_msg, pre_fetched_holdings=holdings)
    else:
        if status_msg:
            try: 
                await asyncio.wait_for(
                    status_msg.edit_text(f"📝 <b>[16:05 EST] 장부 동기화 완료</b> (표시할 진행 중인 장부가 없습니다)", parse_mode='HTML'),
                    timeout=15.0
                )
            except Exception: pass
