# ==========================================================
# FILE: scheduler_core.py
# ==========================================================
# 🚨 MODIFIED: [V72.01 V-REV 예산 뻥튀기(Double Spending) 맹점 100% 소각]
# 🚨 MODIFIED: [V73.10 확정 정산 16:05 EST 전진 배치 및 시각적 디커플링 해체]
# 🚨 MODIFIED: [Case 27 절대 위반 교정] 에스크로(Escrow) 로직 전면 소각 및 예산 분배망 진공 압축 완료
# ==========================================================
import os
import logging
import datetime
import time
import math
import asyncio
import glob
import random
import pandas_market_calendars as mcal
import json
import tempfile
from zoneinfo import ZoneInfo

def is_market_open():
    try:
        est = ZoneInfo('America/New_York')
        today = datetime.datetime.now(est)
        if today.weekday() >= 5: 
            return False
            
        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=today.date(), end_date=today.date())
        
        if not schedule.empty:
            return True
        else:
            logging.info("💤 [is_market_open] 달력 API 빈 데이터 반환. 금일은 미국 증시 휴장일입니다.")
            return False
    except Exception as e:
        logging.error(f"⚠️ 달력 라이브러리 에러 발생. 스케줄 증발 방어를 위해 평일 강제 개장(Fail-Open) 처리합니다: {e}")
        est = ZoneInfo('America/New_York')
        return datetime.datetime.now(est).weekday() < 5

# MODIFIED: [Case 27] 에스크로 연관 연산 전면 적출 및 V-REV 1일 고정 예산 분배망 진공 압축 완료
def get_budget_allocation(cash, tickers, cfg):
    sorted_tickers = sorted(tickers, key=lambda x: 0 if x == "SOXL" else (1 if x == "TQQQ" else 2))
    allocated = {}
    
    safe_cash = float(cash) if cash is not None else 0.0
    
    # 🚨 MODIFIED: [Case 27 절대 위반 수술] 에스크로(vrev_virtual_escrow) 삭감 로직 100% 영구 소각
    # 타임라인이 완벽히 분리되었으므로 V14의 가용 예산을 100% 개방함
    free_cash = safe_cash
    
    for tx in sorted_tickers:
        version = getattr(cfg, 'get_version', lambda x: "V14")(tx)
        
        if version == "V_REV":
            rev_daily_budget = float(getattr(cfg, 'get_seed', lambda x: 0.0)(tx) or 0.0) * 0.15
            allocated[tx] = rev_daily_budget
        else:
            split = int(getattr(cfg, 'get_split_count', lambda x: 40)(tx) or 40)
            seed = float(getattr(cfg, 'get_seed', lambda x: 0.0)(tx) or 0.0)
            portion = seed / split if split > 0 else 0.0
            
            if free_cash >= portion:
                allocated[tx] = free_cash
                free_cash -= portion
            else: 
                allocated[tx] = 0.0
        
    return sorted_tickers, allocated

def perform_self_cleaning():
    try:
        now = time.time()
        seven_days = 7 * 24 * 3600
        one_day = 24 * 3600
        
        for f in glob.glob("logs/*.log"):
            if os.path.isfile(f) and os.stat(f).st_mtime < now - seven_days:
                try: os.remove(f)
                except: pass
                
        for f in glob.glob("data/*.bak_*"):
            if os.path.isfile(f) and os.stat(f).st_mtime < now - seven_days:
                try: os.remove(f)
                except: pass
   
        for prefix in ["daily_snapshot_*", "vwap_state_*"]:
            for f in glob.glob(f"data/{prefix}.json"):
                if os.path.isfile(f) and os.stat(f).st_mtime < now - seven_days:
                    try: os.remove(f)
                    except: pass
   
        for directory in ["data", "logs"]:
            for f in glob.glob(f"{directory}/tmp*"):
                if os.path.isfile(f) and os.stat(f).st_mtime < now - one_day:
                    try: os.remove(f)
                    except: pass
    except Exception as e:
        logging.error(f"🧹 자정(Self-Cleaning) 작업 중 오류 발생: {e}")

async def scheduled_self_cleaning(context):
    await asyncio.to_thread(perform_self_cleaning)
    logging.info("🧹 [시스템 자정 작업 완료] 7일 초과 로그/백업 및 24시간 초과 임시 파일 소각 완료")

async def scheduled_token_check(context):
    jitter_seconds = random.randint(0, 180)
    logging.info(f"🔑 [API 토큰 갱신] 서버 동시 접속 부하 방지를 위해 {jitter_seconds}초 대기 후 발급을 시작합니다.")
    await asyncio.sleep(jitter_seconds)
    
    await asyncio.to_thread(context.job.data['broker']._get_access_token, force=True)
    logging.info("🔑 [API 토큰 갱신] 토큰 갱신이 안전하게 완료되었습니다.")

async def scheduled_force_reset(context):
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    
    if not (3 <= now_est.hour <= 5):
        return

    async def _do_force_reset():
        try:
            is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=10.0)
        except asyncio.TimeoutError:
            logging.error("⚠️ [force_reset] is_market_open 달력 API 타임아웃. 평일 강제 개장 처리합니다.")
            is_open = now_est.weekday() < 5

        if not is_open:
            await context.bot.send_message(chat_id=context.job.chat_id, text="⛔ <b>오늘은 미국 증시 휴장일입니다. 금일 시스템 매매 잠금 해제 및 정규장 주문 스케줄을 모두 건너뜁니다.</b>", parse_mode='HTML')
            return
        
        try:
            app_data = context.job.data
            cfg = app_data['cfg']
            
            await asyncio.to_thread(cfg.reset_locks)
            
            broker = app_data['broker']
            tx_lock = app_data['tx_lock']
            chat_id = context.job.chat_id
             
            if tx_lock is None:
                logging.warning("⚠️ [force_reset] tx_lock 미초기화. 이번 사이클 스킵.")
                try:
                    await context.bot.send_message(chat_id=chat_id, text="⚠️ <b>[시스템 경고]</b> tx_lock 미초기화로 초기화 스케줄을 1회 스킵합니다.", parse_mode='HTML')
                except Exception:
                    pass
                return
             
            async with tx_lock:
                _, holdings = await asyncio.to_thread(broker.get_account_balance)
                
            if holdings is None:
                holdings = {}
                
            msg_addons = ""
            
            active_tickers = await asyncio.to_thread(cfg.get_active_tickers)
            for t in active_tickers:
                version = await asyncio.to_thread(cfg.get_version, t)
                rev_state = await asyncio.to_thread(cfg.get_reverse_state, t)
                
                if version == "V_REV":
                    actual_avg = float(holdings.get(t, {'avg': 0})['avg'])
                    try:
                        curr_p_val = await asyncio.wait_for(
                             asyncio.to_thread(broker.get_current_price, t),
                             timeout=10.0
                        )
                        curr_p = float(curr_p_val or 0.0)
                    except asyncio.TimeoutError:
                        logging.error(f"⚠️ [{t}] 현재가 조회 타임아웃 (10초). 0.0으로 폴백합니다.")
                        curr_p = 0.0
                    except Exception as e:
                        logging.error(f"⚠️ [{t}] 현재가 조회 실패: {e}")
                        curr_p = 0.0
                    
                    if curr_p > 0 and actual_avg > 0:
                        curr_ret = (curr_p - actual_avg) / actual_avg * 100.0
                        exit_target = rev_state.get("exit_target", 0.0)
                        
                        if curr_ret >= exit_target:
                            await asyncio.to_thread(cfg.set_reverse_state, t, True, 0, 0.0)
                            
                            ledger_data = await asyncio.to_thread(cfg.get_ledger)
                            changed = False
                            for lr in ledger_data:
                                if lr.get('ticker') == t and lr.get('is_reverse', False):
                                    lr['is_reverse'] = False
                                    changed = True
                            if changed:
                                await asyncio.to_thread(cfg._save_json, cfg.FILES["LEDGER"], ledger_data)
                             
                            msg_addons += f"\n🌤️ <b>[{t}] 리버스 목표 달성({curr_ret:.2f}%)!</b> 격리 병동 졸업 완료!"
                        else:
                            await asyncio.to_thread(cfg.increment_reverse_day, t)
                else:
                    await asyncio.to_thread(cfg.increment_reverse_day, t)
                    
            final_msg = f"🔓 <b>[04:00 EST] 시스템 일일 초기화 완료 (매매 잠금 해제 & 고점 관측 센서 가동)</b>" + msg_addons
            await context.bot.send_message(chat_id=chat_id, text=final_msg, parse_mode='HTML')
            
        except Exception as e:
            await context.bot.send_message(chat_id=context.job.chat_id, text=f"🚨 <b>시스템 초기화 중 에러 발생:</b> {e}", parse_mode='HTML')

    try:
        await asyncio.wait_for(_do_force_reset(), timeout=180.0)
    except Exception as e:
         logging.error(f"🚨 [force_reset] 전역 타임아웃(180초) 또는 런타임 붕괴 발생: {e}")

async def scheduled_auto_sync(context):
    logging.info("✅ [확정 정산] 16:05 EST 팩트 기반 확정 정산 엔진 다이렉트 가동")

    if context.job.data.get('tx_lock') is None:
        logging.warning("⚠️ [auto_sync] tx_lock 미초기화. 장부 표시 스킵.")
        return
    
    def _check_and_set_lock():
        est_tz = ZoneInfo('America/New_York')
        today_est = datetime.datetime.now(est_tz).strftime("%Y-%m-%d")
        lock_file = "data/sync_lock.json"
        os.makedirs("data", exist_ok=True)

        try:
            if os.path.exists(lock_file):
                with open(lock_file, "r") as f:
                    lock_data = json.load(f)
                    if lock_data.get("last_sync") == today_est:
                        return False, today_est
        except Exception:
            pass

        try:
            fd, tmp_path = tempfile.mkstemp(dir="data", text=True)
            with os.fdopen(fd, 'w') as f:
                json.dump({"last_sync": today_est}, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, lock_file)
        except Exception as e:
            logging.error(f"🚨 동기화 락온 파일 저장 실패: {e}")

        return True, today_est

    can_run, today_est = await asyncio.to_thread(_check_and_set_lock)
    
    if not can_run:
        logging.info(f"⏳ [정산 멱등성 락온] 오늘({today_est} EST)의 16:05 확정 정산이 이미 완료되었습니다. 중복 실행 및 다중 렌더링을 100% 차단합니다.")
        return

    chat_id = context.job.chat_id
    bot = context.job.data['bot']
    status_msg = await context.bot.send_message(chat_id=chat_id, text=f"📝 <b>[16:05 EST] 장부 자동 동기화(무결성 검증)를 시작합니다.</b>", parse_mode='HTML')
    
    success_tickers = []
    active_tickers = await asyncio.to_thread(context.job.data['cfg'].get_active_tickers)
    for t in active_tickers:
        res = await bot.sync_engine.process_auto_sync(t, chat_id, context, silent_ledger=True)
        if res == "SUCCESS":
            success_tickers.append(t)
            
    if success_tickers:
        async with context.job.data['tx_lock']:
            _, holdings = await asyncio.to_thread(context.job.data['broker'].get_account_balance)
        await bot.sync_engine._display_ledger(success_tickers[0], chat_id, context, message_obj=status_msg, pre_fetched_holdings=holdings)
    else:
        await status_msg.edit_text(f"📝 <b>[16:05 EST] 장부 동기화 완료</b> (표시할 진행 중인 장부가 없습니다)", parse_mode='HTML')
