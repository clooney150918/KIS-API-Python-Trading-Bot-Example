# ==========================================================
# FILE: scheduler_core.py
# ==========================================================
# 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
# 제1헌법: 파일 I/O 및 달력 API(mcal)는 무조건 asyncio.to_thread로 래핑하여 이벤트 루프 교착(Deadlock)을 원천 차단함.
# 제3헌법: KST 타임존 및 is_dst 기반의 동적 스케줄링 전면 소각. 
# MODIFIED: [V44.47 KST 타임 패러독스 영구 소각] 서머타임 분기 함수 통합 및 EST 절대 시간 기반으로 100% 디커플링 락온 완료.
# NEW: [콜드 스타트 런타임 붕괴 방어] scheduled_auto_sync 내부 tx_lock None 가드 이식.
# NEW: [전역 타임아웃 이식] scheduled_force_reset 이벤트 루프 교착 방어 타임아웃 래퍼 적용.
# 🚨 NEW: [달력 API 결측 연쇄 기절 방어] is_market_open 평일 강제 개장(Fail-Open) 락온 이식 완료.
# 🚨 MODIFIED: [V-REV SSOT 팩트 교정] scheduled_force_reset 내 낡은 is_active 의존성 영구 소각 및 version 락온.
# 🚨 MODIFIED: [V72.01 V-REV 예산 뻥튀기(Double Spending) 맹점 100% 소각]
# - 기존 spent를 파일 I/O로 파싱하여 이중 차감하고 free_cash를 더하던 데드코드를 영구 적출.
# - 코어 엔진 내부에서 자체 잔차를 연산하므로, 오직 순수 1회 예산(15%)만 주입하여
#   0.5회분씩 2건의 지정가 VWAP 주문이 정확히 분할 타격되도록 락온.
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

# 🚨 [AI 에이전트 절대 주의]
# 이 함수는 동기(Synchronous) 블로킹 함수입니다. 비동기 루프 내에서 직접 호출하면 전체 스케줄러가 교착(Deadlock)되어 증발합니다. 
# 반드시 호출부에서 await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=10.0) 패턴으로 래핑하세요.
def is_market_open():
    try:
        est = ZoneInfo('America/New_York')
        today = datetime.datetime.now(est)
        if today.weekday() >= 5: 
            return False
            
        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=today.date(), end_date=today.date())
        
        # 🚨 MODIFIED: [달력 API 결측 연쇄 기절 방어] schedule.empty == True 여도 평일이면 무조건 Fail-Open(강제 개장) 반환 락온
        if not schedule.empty:
            return True
        else:
            logging.warning("⚠️ [is_market_open] 달력 API가 빈 값을 반환했으나 평일(월~금)이므로 Fail-Open(강제 개장) 방어막을 가동합니다.")
            return True
    except Exception as e:
        logging.error(f"⚠️ 달력 라이브러리 에러 발생. 스케줄 증발 방어를 위해 평일 강제 개장(Fail-Open) 처리합니다: {e}")
        est = ZoneInfo('America/New_York')
        return datetime.datetime.now(est).weekday() < 5

def get_budget_allocation(cash, tickers, cfg):
    sorted_tickers = sorted(tickers, key=lambda x: 0 if x == "SOXL" else (1 if x == "TQQQ" else 2))
    allocated = {}
    
    safe_cash = float(cash) if cash is not None else 0.0
    
    dynamic_total_locked = 0.0
    vrev_virtual_escrow = 0.0 
    
    for tx in tickers:
        rev_state = cfg.get_reverse_state(tx)
        if rev_state.get("is_active", False):
            is_locked = getattr(cfg, 'get_order_locked', lambda x: False)(tx)
            if not is_locked:
                dynamic_total_locked += float(cfg.get_escrow_cash(tx) or 0.0)
        
        if cfg.get_version(tx) == "V_REV":
            vrev_virtual_escrow += float(cfg.get_seed(tx) or 0.0) * 0.15

    free_cash = max(0.0, safe_cash - dynamic_total_locked - vrev_virtual_escrow)
    
    for tx in sorted_tickers:
        version = getattr(cfg, 'get_version', lambda x: "V14")(tx)
        rev_state = cfg.get_reverse_state(tx)
        is_rev = rev_state.get("is_active", False)
        
        if version == "V_REV":
            rev_daily_budget = float(cfg.get_seed(tx) or 0.0) * 0.15
            # 🚨 MODIFIED: [V72.01 V-REV 예산 뻥튀기(Double Spending) 맹점 100% 소각]
            # 기존 spent를 파일 I/O로 파싱하여 이중 차감하고 free_cash를 더하던 데드코드를 영구 적출.
            # 코어 엔진 내부에서 자체 잔차를 연산하므로, 오직 순수 1회 예산(15%)만 주입하여
            # 0.5회분씩 2건의 지정가 VWAP 주문이 정확히 분할 타격되도록 락온.
            allocated[tx] = rev_daily_budget
        else:
            other_locked = dynamic_total_locked
            if is_rev:
                is_locked = getattr(cfg, 'get_order_locked', lambda x: False)(tx)
                if not is_locked:
                    other_locked -= float(cfg.get_escrow_cash(tx) or 0.0)
            
            if is_rev:
                my_escrow = float(cfg.get_escrow_cash(tx) or 0.0)
                allocated[tx] = my_escrow + other_locked
            else:
                split = int(cfg.get_split_count(tx) or 0)
                seed = float(cfg.get_seed(tx) or 0.0)
                portion = seed / split if split > 0 else 0.0
                
                if free_cash >= portion:
                    allocated[tx] = free_cash
                    free_cash -= portion
                else: 
                    allocated[tx] = 0.0
        
    return sorted_tickers, allocated

def get_actual_execution_price(execs, target_qty, side_cd):
    if not execs or target_qty <= 0: return 0.0
    
    execs.sort(key=lambda x: str(x.get('ord_tmd') or '000000'), reverse=True)
    matched_qty = 0
    total_amt = 0.0
    for ex in execs:
        if ex.get('sll_buy_dvsn_cd') == side_cd: 
            eqty = int(float(ex.get('ft_ccld_qty') or 0))
            eprice = float(ex.get('ft_ccld_unpr3') or 0.0)
            if matched_qty + eqty <= target_qty:
                total_amt += eqty * eprice
                matched_qty += eqty
            elif matched_qty < target_qty:
                rem = target_qty - matched_qty
                total_amt += rem * eprice
                matched_qty += rem
            
            if matched_qty >= target_qty:
                break
    
    if matched_qty > 0:
        return round(total_amt / matched_qty, 2)
    return 0.0

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
    # 🚨 [EST 절대 시간 락온] 타임 패러독스 방어
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    
    # 04:00 EST 실행 시간 이탈 여부 검증 (Jitter 방어)
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
            
            # MODIFIED: [초기화 누수 및 락온 해제] tx_lock 미초기화 시 스킵되더라도 매매 잠금은 무조건 해제되도록 전진 배치하여 멱등성 확보
            await asyncio.to_thread(cfg.reset_locks)
            
            broker = app_data['broker']
            tx_lock = app_data['tx_lock']
            chat_id = context.job.chat_id
             
            # 🚨 MODIFIED: [V44.65 KST 17:00 런타임 붕괴 및 이벤트 루프 교착 방어]
            # 🚨 [AI 에이전트 절대 주의 - 환각(Hallucination) 방어막]
            if tx_lock is None:
                logging.warning("⚠️ [force_reset] tx_lock 미초기화. 이번 사이클 스킵.")
                try:
                    await context.bot.send_message(chat_id=chat_id, text="⚠️ <b>[시스템 경고]</b> tx_lock 미초기화로 초기화 스케줄을 1회 스킵합니다.", parse_mode='HTML')
                except Exception:
                    pass
                return
             
            # MODIFIED: [버그 2 수술] force_reset tx_lock 점유율 압축 및 병목 해체
            # 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막] 이곳의 tx_lock 점유 구간은 이미 V44.66에서 극한 압축되었다. 락 대기 병목을 핑계로 로직을 확장하는 오버 엔지니어링을 절대 금지한다.
            async with tx_lock:
                _, holdings = await asyncio.to_thread(broker.get_account_balance)
                
            if holdings is None:
                holdings = {}
                
            msg_addons = ""
            
            active_tickers = await asyncio.to_thread(cfg.get_active_tickers)
            for t in active_tickers:
                # 🚨 MODIFIED: [V-REV SSOT 팩트 교정] 낡은 is_active 의존성 전면 소각 및 version 락온
                version = await asyncio.to_thread(cfg.get_version, t)
                rev_state = await asyncio.to_thread(cfg.get_reverse_state, t)
                
                if version == "V_REV":
                    actual_avg = float(holdings.get(t, {'avg': 0})['avg'])
                    
                    # 🚨 MODIFIED: [V44.65 KST 17:00 런타임 붕괴 및 이벤트 루프 교착 방어]
                    # 🚨 [AI 에이전트 절대 주의 - 환각(Hallucination) 방어막]
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
                            # 🚨 MODIFIED: [V54.04] V_REV 모드라면 is_active를 False로 끄지 않고 True로 보존하여 '0주 새출발' 상태 팩트 락온 (SSOT 무결성 유지)
                            await asyncio.to_thread(cfg.set_reverse_state, t, True, 0, 0.0)
                            await asyncio.to_thread(cfg.clear_escrow_cash, t)
                            
                            ledger_data = await asyncio.to_thread(cfg.get_ledger)
                            changed = False
                            for lr in ledger_data:
                                if lr.get('ticker') == t and lr.get('is_reverse', False):
                                    lr['is_reverse'] = False
                                    changed = True
                            if changed:
                                await asyncio.to_thread(cfg._save_json, cfg.FILES["LEDGER"], ledger_data)
                             
                            msg_addons += f"\n🌤️ <b>[{t}] 리버스 목표 달성({curr_ret:.2f}%)!</b> 격리 병동 졸업 및 Escrow 해제 완료!"
                        else:
                            await asyncio.to_thread(cfg.increment_reverse_day, t)
                    else:
                        await asyncio.to_thread(cfg.increment_reverse_day, t)
                else:
                    await asyncio.to_thread(cfg.increment_reverse_day, t)
                    
            final_msg = f"🔓 <b>[04:00 EST] 시스템 일일 초기화 완료 (매매 잠금 해제 & 고점 관측 센서 가동)</b>" + msg_addons
            await context.bot.send_message(chat_id=chat_id, text=final_msg, parse_mode='HTML')
            
        except Exception as e:
            await context.bot.send_message(chat_id=context.job.chat_id, text=f"🚨 <b>시스템 초기화 중 에러 발생:</b> {e}", parse_mode='HTML')

    # NEW: [전역 타임아웃 이식] 메인 로직 전체 180초 타임아웃 족쇄 체결
    try:
        await asyncio.wait_for(_do_force_reset(), timeout=180.0)
    except Exception as e:
         logging.error(f"🚨 [force_reset] 전역 타임아웃(180초) 또는 런타임 붕괴 발생: {e}")

# 🚨 [KST 분기 함수 통합] 21:00 EST 스케줄 단일화
async def scheduled_auto_sync(context):
    logging.info("✅ [확정 정산] 21:00 EST 팩트 기반 확정 정산 엔진 다이렉트 가동")

    # MODIFIED: [V44.68 콜드 스타트 방어막 전진 배치 및 팩트 교정]
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
                # MODIFIED: [제4헌법 원자적 쓰기 무결성 락온] flush 및 fsync 추가
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, lock_file)
        except Exception as e:
            logging.error(f"🚨 동기화 락온 파일 저장 실패: {e}")

        return True, today_est

    # 🚨 [비동기 래핑] 파일 I/O 락 점유 원천 차단
    can_run, today_est = await asyncio.to_thread(_check_and_set_lock)
    
    if not can_run:
        logging.info(f"⏳ [정산 멱등성 락온] 오늘({today_est} EST)의 21:00 확정 정산이 이미 완료되었습니다. 중복 실행 및 다중 렌더링을 100% 차단합니다.")
        return

    chat_id = context.job.chat_id
    bot = context.job.data['bot']
    status_msg = await context.bot.send_message(chat_id=chat_id, text=f"📝 <b>[21:00 EST] 장부 자동 동기화(무결성 검증)를 시작합니다.</b>", parse_mode='HTML')
    
    success_tickers = []
    active_tickers = await asyncio.to_thread(context.job.data['cfg'].get_active_tickers)
    for t in active_tickers:
         # MODIFIED: [제2헌법 라우팅 누수 런타임 붕괴 방어] sync_engine 호출로 팩트 교정
        res = await bot.sync_engine.process_auto_sync(t, chat_id, context, silent_ledger=True)
        if res == "SUCCESS":
            success_tickers.append(t)
            
    if success_tickers:
        async with context.job.data['tx_lock']:
            _, holdings = await asyncio.to_thread(context.job.data['broker'].get_account_balance)
        # MODIFIED: [제2헌법 라우팅 누수 런타임 붕괴 방어] sync_engine 호출로 팩트 교정
        await bot.sync_engine._display_ledger(success_tickers[0], chat_id, context, message_obj=status_msg, pre_fetched_holdings=holdings)
    else:
        await status_msg.edit_text(f"📝 <b>[21:00 EST] 장부 동기화 완료</b> (표시할 진행 중인 장부가 없습니다)", parse_mode='HTML')
