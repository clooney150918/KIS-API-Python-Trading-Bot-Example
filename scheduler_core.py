# ==========================================================
# FILE: scheduler_core.py
# ==========================================================
# 🚨 MODIFIED: [제1헌법 철저 준수] is_market_open 내부 달력 API(mcal) 스캔 전 파편화된 time.sleep(0.06)을 영구 정리하고, GlobalThrottle.wait_api_sync() 중앙 통제소 락온 완료.
# 🚨 MODIFIED: [Lost Update 궁극 방어] scheduled_auto_sync 내부 _check_and_set_lock에서 sync_lock.json 생성 및 갱신 시 GlobalThrottle.get_file_lock() 100% 팩트 래핑 완료.
# 🚨 MODIFIED: [본진 졸업 마비 패러독스 수술] 보조전략가 오버나이트를 수행하여 계좌에 물량이 남아있더라도, `process_realtime_graduation`에서 KIS 잔고에서 보조전략 장부(`AssassinLedger`) 수량을 차감하여 본진 물량만을 정확히 추출, 0주 새출발 졸업망이 정상 가동되도록 팩트 락온. 단, 음수가 발생할 경우 큐 장부를 교차 검증하여 조기 졸업을 안전하게 차단(Bypass).
# 🚨 NEW: [Thundering Herd 영구 정리] 스케줄 동시 기상 시 달력 API(mcal) 무한 호출로 인한 60초 병목(Skipped) 붕괴를 원천 차단하기 위해 `_MCAL_CACHE` 전역 인메모리 캐싱 파이프라인 100% 결속.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import asyncio
import inspect
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

# 🚨 NEW: [Thundering Herd 방어용 전역 캐시]
_MCAL_CACHE = {}

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

def is_official_trading_day_at(moment=None, calendar_provider=mcal):
    """Return whether NYSE is officially open for the given New York date.

    Calendar uncertainty is fail-closed: unofficial schedule execution must not
    continue merely because the date looks like a weekday.
    """
    est = ZoneInfo('America/New_York')
    current = moment or datetime.datetime.now(est)
    if current.tzinfo is None:
        current = current.replace(tzinfo=est)
    current_est = current.astimezone(est)
    if current_est.weekday() >= 5:
        return False
    try:
        nyse = calendar_provider.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=current_est.date(), end_date=current_est.date())
        return not schedule.empty
    except Exception as e:
        logging.error(f"⚠️ official trading calendar unavailable; fail-closed: {e}")
        return False


def is_market_open():
    """Official trading-day check with per-day cache and fail-closed uncertainty."""
    est = ZoneInfo('America/New_York')
    today = datetime.datetime.now(est)
    date_str = today.strftime('%Y-%m-%d')
    if date_str in _MCAL_CACHE:
        return _MCAL_CACHE[date_str]

    for attempt in range(3):
        try:
            GlobalThrottle.wait_api_sync() # 🚨 MODIFIED: 중앙 통제소 락온
            is_open = is_official_trading_day_at(today)
            _MCAL_CACHE[date_str] = is_open
            return is_open
        except Exception as e:
            logging.error(f"⚠️ official trading-day check failed; retrying fail-closed: {e}")
            if attempt == 2:
                _MCAL_CACHE[date_str] = False
                return False
            time.sleep(1.0 * (2 ** attempt))


def _resolve_scheduler_gate(app_data):
    if not isinstance(app_data, dict):
        return None
    gate = app_data.get('scheduler_safety_gate') or app_data.get('runtime_safety_gate')
    if gate is not None:
        return gate
    broker = app_data.get('broker')
    return getattr(broker, 'runtime_safety_gate', None)


def _coerce_gate_result(result):
    if isinstance(result, tuple):
        allowed = bool(result[0]) if result else False
        reason = str(result[1]) if len(result) > 1 else "scheduler gate blocked"
        return allowed, reason
    if isinstance(result, dict):
        if 'allowed' in result:
            return bool(result.get('allowed')), str(result.get('reason', 'scheduler gate blocked'))
        if 'can_submit' in result:
            return bool(result.get('can_submit')), str(result.get('reason', 'scheduler gate blocked'))
    if hasattr(result, 'can_submit'):
        return bool(result.can_submit), str(getattr(result, 'reason', 'scheduler gate blocked'))
    return bool(result), "allowed" if result else "scheduler gate blocked"


def _runtime_state_allows_scheduler(gate, schedule_name, boundary):
    if gate is None:
        return False, "scheduler runtime safety gate is not configured"
    if hasattr(gate, 'assert_scheduler_execution_allowed'):
        return _coerce_gate_result(gate.assert_scheduler_execution_allowed(schedule_name, boundary))
    if hasattr(gate, '_load_state'):
        state, decision = gate._load_state()
        if decision is not None:
            return False, getattr(decision, 'reason', 'runtime safety state rejected scheduler')
        if not isinstance(state, dict):
            return False, "runtime safety state is invalid"
        if state.get('operator_halt') is True:
            return False, str(state.get('reason') or 'operator halt is active')
        if boundary == 'before_order_submit' and not (state.get('live_armed') is True or state.get('shadow_only') is True):
            return False, "live trading is not armed and shadow mode is not enabled"
        return True, "allowed"
    return False, "scheduler runtime safety gate does not expose a supported preflight"


async def ensure_scheduler_runtime_allowed(app_data, schedule_name, boundary):
    gate = _resolve_scheduler_gate(app_data)
    result = _runtime_state_allows_scheduler(gate, schedule_name, boundary)
    if inspect.isawaitable(result):
        result = await result
    allowed, reason = _coerce_gate_result(result)
    if not allowed:
        logging.warning(f"🛑 [{schedule_name}] scheduler safety gate blocked at {boundary}: {reason}")
    return allowed

def get_budget_allocation(cash, tickers, cfg):
    sorted_tickers = sorted(tickers or [], key=lambda x: 0 if x == "SOXL" else (1 if x == "TQQQ" else 2))
    allocated = {}
    free_cash = _safe_float(cash)
    
    base_portions = {}
    for tx in sorted_tickers:
        split = int(_safe_float(getattr(cfg, 'get_split_count', lambda x: 40)(tx)))
        if split <= 0: split = 40
        seed = _safe_float(getattr(cfg, 'get_seed', lambda x: 0.0)(tx))
        base_portions[tx] = seed / split

    for tx in sorted_tickers:
        req = base_portions[tx]
        if free_cash >= req:
            allocated[tx] = req
            free_cash = max(0.0, free_cash - req) 
        else:
            allocated[tx] = 0.0

    v14_active = [tx for tx in sorted_tickers if allocated.get(tx, 0.0) > 0]
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
            ("data/slice_state_*.json", seven_days),      
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
        logging.info("🧹 [시스템 자정 작업 완료] 7일 초과 낡은 로그/스냅샷 및 임시 파일 GC(정리) 완료")
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
            except Exception as e:
                if attempt == 2:
                    logging.error(f"⚠️ is_market_open 달력 API 오류/타임아웃. 달력 불확실성으로 강제 초기화를 fail-closed 스킵합니다: {e}")
                    is_open = False
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

                if bool(rev_state.get("is_active", False)):
                    if prev_c > 0 and actual_avg > 0:
                        close_ret = (prev_c - actual_avg) / actual_avg * 100.0
                        exit_target = _safe_float(rev_state.get("exit_target", 0.0))
                        
                        if close_ret >= exit_target:
                            carry_dynamic_t = _safe_float(rev_state.get("dynamic_t", 0.0))
                            carry_rem_cash = _safe_float(rev_state.get("rem_cash", 0.0))
                            await asyncio.wait_for(asyncio.to_thread(cfg.set_reverse_state, t, False, 0, 0.0, dynamic_t=carry_dynamic_t, rem_cash=carry_rem_cash), timeout=5.0)
                            safe_t = html.escape(str(t))
                            msg_addons += f"\n🌤️ <b>[{safe_t}] 리버스 목표 달성({close_ret:.2f}%)!</b> 격리 병동 졸업 및 일반 모드 복귀 완료!"
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
async def process_realtime_graduation(ticker, cfg, broker, legacy_lot_book, chat_id, context, tx_lock):
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
            logging.error(f"🚨 [{ticker}] 조기 졸업 스캔 중 보조전략 장부 로드 에러: {e}")
            
        main_qty = max(0, kis_qty - a_qty)
        
        if main_qty == 0:
            try:
                ledger_qty, avg_price, invested, sold = 0, 0.0, 0.0, 0.0
                try:
                    ledger_qty, avg_price, invested, sold = await asyncio.wait_for(asyncio.to_thread(cfg.calculate_holdings_from_official_ledger, ticker), timeout=10.0)
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
                        msg = f"🎓 <b>[{html.escape(str(ticker))}] 실시간 조기 졸업 (Scenario 2) 완료!</b>\n"
                        msg += f"▫️ 15:15 EST 이전 전량 익절이 감지되었습니다.\n"
                        msg += f"▫️ 수익금: <b>${_safe_float(hist.get('profit', 0.0)):.2f}</b>\n▫️ 장부가 즉시 정리되었습니다."
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
                                            pending_buy_amount=getattr(broker, "last_pending_buy_amount", 0.0),
                                            is_simulation=True, is_snapshot_mode=True
                                        ),
                                        timeout=15.0
                                    )
                                except Exception as e: logging.error(f"🚨 조기졸업 새출발 연산 타임아웃: {e}")
                                
                                if isinstance(plan, dict) and plan.get('core_orders'):
                                    logging.warning(
                                        f"🛑 [{ticker}] 조기 졸업 후 직접 재진입 주문 경로 차단: "
                                        "공식 V4 주문 스케줄러만 주문 제출 가능"
                                    )
                                    try:
                                        await asyncio.wait_for(
                                            context.bot.send_message(
                                                chat_id,
                                                f"🛑 <b>[{html.escape(str(ticker))}] 직접 재진입 주문 차단</b>\n"
                                                "▫️ 공식 V4 주문 스케줄러 외 주문 제출은 안전정책상 중단되었습니다.",
                                                parse_mode='HTML'
                                            ),
                                            timeout=15.0
                                        )
                                    except Exception:
                                        pass
                        except Exception as re_e:
                            logging.error(f"🚨 [{ticker}] 조기 졸업 후 강제 재진입 파이프라인 에러: {re_e}")

            except Exception as e:
                logging.error(f"🚨 [{ticker}] 실시간 조기 졸업 처리 에러: {e}")

# ==============================================================
# 2. 🏛️ 16:05 EST 정규 정산 (Scenario 1, 3 & Bypass)
# ==============================================================
# ── [16:05 정산 체결 요약] 봇 구분 / 매수·매도 / 주문구분 라벨 (확정 포맷) ──
_BOT_LABELS = {
    "jinho_bot": "[진호봇]",
    "eunkyung_bot": "[은경봇]",
}

_EVENT_TYPE_LABELS = {
    "FULL": "별값매수",
    "FULL_BUY": "별값매수",
    "HALF": "분할매수",
    "HALF_BUY": "분할매수",
    "BONUS": "보너스매수",
    "BONUS_BUY": "보너스매수",
    "QUARTER": "쿼터매도",
    "QUARTER_SELL": "쿼터매도",
    "TARGET_FULL": "목표익절",
    "TARGET_SELL_THEN_FULL_BUY": "목표익절",
    "TARGET_HALF": "목표익절(반)",
    "TARGET_SELL_THEN_HALF_BUY": "목표익절(반)",
}


def _resolve_bot_label() -> str:
    """런타임(load_dotenv 이후)에 daemon_name을 읽어 봇 구분 라벨을 결정한다."""
    name = (os.getenv("daemon_name") or "").strip().lower()
    return _BOT_LABELS.get(name) or (f"[{name}]" if name else "[봇]")


def _fill_side_icon(side) -> str:
    side = str(side or "")
    if side == "BUY":
        return "🟢"
    if side == "SELL":
        return "🔴"
    return "⚪"


def _fill_side_label(side) -> str:
    side = str(side or "")
    if side == "BUY":
        return "매수"
    if side == "SELL":
        return "매도"
    return side


def _fill_gubun_label(event_type) -> str:
    return _EVENT_TYPE_LABELS.get(str(event_type).upper(), "")


def _format_fill_price(price) -> str:
    if price is None or price == "":
        return "-"
    try:
        return f"{float(str(price).replace(',', '')):.2f}"
    except Exception:
        return str(price)


def _format_fill_date(record) -> str:
    td = str(record.get("trade_date") or "").replace("-", "").strip()
    if len(td) == 8:
        return f"{td[4:6]}-{td[6:8]}"
    return td or "--"


def _event_type_for_intent_id(reconciler, intent_id, ticker) -> str:
    """체결이 매칭된 intent의 event_type(주문구분)을 읽기 전용으로 조회."""
    if not intent_id or reconciler is None:
        return ""
    try:
        for intent in reconciler.intent_store.list_intents(str(ticker).upper()):
            if intent.get("intent_id") == intent_id:
                return str(intent.get("event_type") or "")
    except Exception:
        pass
    return ""


def build_daily_fill_summary(bot_label: str, records: list, reconciler=None) -> str:
    """16:05 정산 스캔의 하루 체결 요약 알림 텍스트(확정 포맷).

        🔔 [진호봇] 오늘 체결 요약 (N건)
        🟢 매수 6주 @ $142.35 (별값매수)
        🔴 매도 19주 @ $142.36 (쿼터매도)
        · 08-14
    """
    if not records:
        return ""
    lines = [f"🔔 {bot_label} 오늘 체결 요약 ({len(records)}건)"]
    date_text = ""
    for record in records:
        if not isinstance(record, dict):
            continue
        side = _fill_side_label(record.get("side"))
        icon = _fill_side_icon(record.get("side"))
        try:
            qty_text = f"{int(record.get('qty'))}주"
        except Exception:
            qty_text = "-주"
        price_text = _format_fill_price(record.get("price"))
        gubun = _fill_gubun_label(_event_type_for_intent_id(reconciler, record.get("intent_id"), record.get("ticker")))
        gubun_part = f" ({gubun})" if gubun else ""
        lines.append(f"{icon} {side} {qty_text} @ ${price_text}{gubun_part}")
        if not date_text:
            date_text = _format_fill_date(record)
    if date_text:
        lines.append(f"· {date_text}")
    return "\n".join(lines)


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
    
    # ── [체결 요약] reconcile 전 processed_fills 스냅샷 (전후 diff로 당일 신규 체결 추출) ──
    fill_reconciler = app_data.get('fill_reconciliation_guard')
    before_fill_keys = set()
    if fill_reconciler is not None:
        try:
            before_fill_keys = {r.get("fill_key") for r in fill_reconciler.processed_fill_store.list_records() if r.get("fill_key")}
        except Exception as e:
            logging.warning(f"⚠️ [체결 요약] processed_fills 스냅샷 실패(요약 알림 생략): {e}")
            before_fill_keys = None

    for t in active_tickers:
        try:
            await asyncio.sleep(0.06)
            res = await bot.sync_engine.process_auto_sync(t, chat_id, context, silent_ledger=True)
            if res == "SUCCESS": success_tickers.append(t)
        except Exception as e:
            logging.error(f"🚨 [{t}] 확정 정산 단일 종목 에러 (Cascade 방어): {e}")

    # ── [체결 요약] reconcile 후 신규 체결 취합 → 하루 매수·매도 요약 DM 발송 ──
    if fill_reconciler is not None and before_fill_keys is not None:
        try:
            new_fill_records = [
                r for r in fill_reconciler.processed_fill_store.list_records()
                if r.get("fill_key") and r.get("fill_key") not in before_fill_keys
            ]
        except Exception as e:
            logging.error(f"⛔ [체결 요약] processed_fills 조회 실패: {e}")
            new_fill_records = []
        if new_fill_records:
            msg = build_daily_fill_summary(_resolve_bot_label(), new_fill_records, fill_reconciler)
            if msg:
                try:
                    await asyncio.wait_for(
                        context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML"),
                        timeout=15.0,
                    )
                except Exception as exc:
                    logging.error(f"⛔ [{_resolve_bot_label()}] 체결 요약 알림 발송 실패: {exc}")

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
