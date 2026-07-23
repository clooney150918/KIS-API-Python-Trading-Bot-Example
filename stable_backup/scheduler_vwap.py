# ==========================================================
# FILE: scheduler_vwap.py
# ==========================================================
# 🚨 MODIFIED: [Thundering Herd 영구 소각] 달력 API(mcal) 스캔 전 부여되던 파편화된 time.sleep(0.06)을 전면 소각.
# 🚨 MODIFIED: [중앙 통제소 락온] GlobalThrottle.wait_api_sync()를 주입하여 Thread-Safe 한 100% 중앙 집중형 TPS 방어망 결속 완료.
# 🚨 MODIFIED: [Buy High, Sell Low 패러독스 궁극 수술] 매수 체결 직후 거시적 VWAP 이탈률만 보고 손실 덤핑을 유발하던 맹독성 '상방 매도 하이재킹(Upward Sell Hijack)' 로직 및 연계 플래그를 시스템 전역에서 100% 영구 소각 완료.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import asyncio
import pandas_market_calendars as mcal

from scheduler_core import is_market_open
from vwap_core_engine import execute_vwap_init, execute_vwap_trade
from vwap_aftermarket_engine import execute_aftermarket_trade
from global_throttle import GlobalThrottle # 🚨 NEW: 전역 통제소 결속

def _fetch_market_schedule_sync(now_est):
    """ 🚨 [제2헌법 준수] 달력 API(mcal) 스캔 로직 단일화 (GlobalThrottle 중앙 통제) """
    GlobalThrottle.wait_api_sync() # 🚨 MODIFIED: 파편화된 sleep 소각 및 중앙 통제소 락온
    nyse = mcal.get_calendar('NYSE')
    return nyse.schedule(start_date=now_est.date(), end_date=now_est.date())

async def _get_market_close_time(now_est):
    """ 🚨 [DRY 원칙 및 제5헌법 결속] 달력 스캔 중복 소각 및 3단 백오프 기반 Fail-Open 폴백 """
    schedule = None
    for attempt in range(3):
        try:
            schedule = await asyncio.wait_for(asyncio.to_thread(_fetch_market_schedule_sync, now_est), timeout=10.0)
            break
        except asyncio.TimeoutError:
            if attempt == 2: logging.error("⚠️ 장마감시간 달력 API 타임아웃. 평일 강제 마감시간(16:00 EST) 세팅.")
            else: await asyncio.sleep(1.0 * (2 ** attempt))
        except Exception as e:
            if attempt == 2: logging.error(f"⚠️ 장마감시간 달력 API 에러({e}). 평일 강제 마감시간(16:00 EST) 세팅.")
            else: await asyncio.sleep(1.0 * (2 ** attempt))

    if schedule is not None and not schedule.empty:
        return schedule.iloc[0]['market_close'].astimezone(now_est.tzinfo)
    elif schedule is not None and schedule.empty:
        return None 
    else:
        if now_est.weekday() < 5:
            return now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        else:
            return None

async def scheduled_vwap_init_and_cancel(context):
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    if now_est.time() < datetime.time(12, 0):
        return

    job = getattr(context, 'job', None)
    raw_job_data = getattr(job, 'data', None) if job else None
    job_data = raw_job_data if isinstance(raw_job_data, dict) else {}
    
    tx_lock = job_data.get('tx_lock')
    cfg = job_data.get('cfg')
    broker = job_data.get('broker')
    chat_id = getattr(job, 'chat_id', None)
   
    if not tx_lock or not cfg or not broker:
        logging.warning("⚠️ [vwap_init_and_cancel] 필수 컨텍스트 미초기화. 이번 사이클 스킵.")
        return

    is_open = False
    for attempt in range(3):
        try:
            is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=10.0)
            break
        except asyncio.TimeoutError:
            if attempt == 2:
                is_open = now_est.weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))
        except Exception:
            if attempt == 2:
                is_open = now_est.weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))

    if not is_open:
        return
    
    market_close = await _get_market_close_time(now_est)
    if not market_close: 
        logging.info("💤 [vwap_init] 달력 API 휴장일 판별 완료.")
        return
        
    vwap_start_time = market_close - datetime.timedelta(minutes=34, seconds=0)
    
    if not (vwap_start_time <= now_est <= market_close):
        return
    
    vwap_cache = job_data.setdefault('vwap_cache', {})
    today_str = now_est.strftime('%Y%m%d')
    
    if vwap_cache.get('date') != today_str:
        vwap_cache.clear()
        vwap_cache['date'] = today_str
            
    try:
        await asyncio.wait_for(
            execute_vwap_init(tx_lock, cfg, broker, chat_id, context, vwap_cache), 
            timeout=45.0
        )
    except Exception as e:
        logging.error(f"🚨 Fail-Safe 타임아웃 에러 (Init 단계): {e}", exc_info=True)


async def scheduled_vwap_trade(context):
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    if now_est.time() < datetime.time(12, 0):
        return

    job = getattr(context, 'job', None)
    raw_job_data = getattr(job, 'data', None) if job else None
    job_data = raw_job_data if isinstance(raw_job_data, dict) else {}
    
    tx_lock = job_data.get('tx_lock')
    cfg = job_data.get('cfg')
    broker = job_data.get('broker')
    strategy = job_data.get('strategy')
    queue_ledger = job_data.get('queue_ledger')
    chat_id = getattr(job, 'chat_id', None)
    
    if not tx_lock or not cfg or not broker or not strategy:
        logging.warning("⚠️ [vwap_trade] 필수 컨텍스트 미초기화. 이번 사이클 스킵.")
        return

    is_open = False
    for attempt in range(3):
        try:
            is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=10.0)
            break
        except asyncio.TimeoutError:
            if attempt == 2:
                is_open = now_est.weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))
        except Exception:
            if attempt == 2:
                is_open = now_est.weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))

    if not is_open:
        return
    
    market_close = await _get_market_close_time(now_est)
    if not market_close: 
        logging.info("💤 [vwap_trade] 달력 API 휴장일 판별 완료.")
        return
         
    vwap_start_time = market_close - datetime.timedelta(minutes=33, seconds=0) # 15:27 EST
    
    if not (vwap_start_time <= now_est <= market_close):
        return

    base_map = job_data.get('base_map')
    if not isinstance(base_map, dict): base_map = {'SOXL': 'SOXX', 'TQQQ': 'QQQ'}
    
    vwap_cache = job_data.setdefault('vwap_cache', {})
    today_str = now_est.strftime('%Y%m%d')
    
    if vwap_cache.get('date') != today_str:
        vwap_cache.clear()
        vwap_cache['date'] = today_str

    try:
        await asyncio.wait_for(
            execute_vwap_trade(tx_lock, cfg, broker, strategy, queue_ledger, chat_id, context, base_map, vwap_cache), 
            timeout=120.0
        )
    except Exception as e:
        logging.error(f"🚨 VWAP 섀도우 오버라이드 스케줄러 엔진 타임아웃/에러: {e}", exc_info=True)


async def scheduled_aftermarket_vrev_trade(context):
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    if now_est.time() < datetime.time(15, 0):
        return

    job = getattr(context, 'job', None)
    raw_job_data = getattr(job, 'data', None) if job else None
    job_data = raw_job_data if isinstance(raw_job_data, dict) else {}
    
    tx_lock = job_data.get('tx_lock')
    cfg = job_data.get('cfg')
    broker = job_data.get('broker')
    strategy = job_data.get('strategy')
    queue_ledger = job_data.get('queue_ledger')
    chat_id = getattr(job, 'chat_id', None)
    
    if not tx_lock or not cfg or not broker or not strategy:
        return

    is_open = False
    for attempt in range(3):
        try:
            is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=10.0)
            break
        except asyncio.TimeoutError:
            if attempt == 2:
                is_open = now_est.weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))
        except Exception:
            if attempt == 2:
                is_open = now_est.weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))

    if not is_open:
        return

    try:
        await asyncio.wait_for(
            execute_aftermarket_trade(tx_lock, cfg, broker, strategy, queue_ledger, chat_id, context), 
            timeout=240.0
        )
    except Exception as e:
        logging.error(f"🚨 애프터장 스케줄러 엔진 타임아웃/에러: {e}", exc_info=True)
