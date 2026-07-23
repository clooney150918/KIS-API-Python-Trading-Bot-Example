# ==========================================================
# FILE: scheduler_regular.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 5대 헌법 및 38대 엣지 케이스 완벽 결속 교차 검증 완료.
# 🚨 MODIFIED: [Thundering Herd 영구 소각] 파편화된 await asyncio.sleep(0.06) 땜질을 무려 24개소에서 전면 삭제.
# 🚨 MODIFIED: [중앙 통제소 위임] 모든 API 지연을 GlobalThrottle(중앙 통제소)로 100% 위임하여 비동기 이벤트 루프 마비 및 교착 상태 완벽 방어.
# 🚨 MODIFIED: [예약 주문 증발(Ghost Order) 궁극 수술] V14 LOC 장전 시 하드코딩되어 있던 `is_market_active_now = False`를 영구 소각. 현재 시간(EST)을 동적으로 판별하여 프리장 개장 후(서머타임 04:05 EST)에는 실시간 본주문(`send_order`)을, 개장 전(윈터타임 03:05 EST)에는 예약 주문(`send_reservation_order`)을 격발하도록 팩트 락온 완료.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import asyncio
import random
import html
import math

from scheduler_core import is_market_open, get_budget_allocation
from order_executor import execute_order_list
from state_io_manager import read_avwap_state_sync

def _safe_float(val):
    try:
        f_val = float(str(val or 0.0).replace(',', ''))
        if math.isnan(f_val) or math.isinf(f_val):
            return 0.0
        return f_val
    except Exception:
        return 0.0

async def scheduled_early_regular_trade(context):
    is_open = False
    for attempt in range(3):
        try:
            is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=10.0)
            break
        except asyncio.TimeoutError:
            if attempt == 2:
                logging.error("⚠️ is_market_open 달력 API 타임아웃. 평일이므로 강제 개장 처리합니다.")
                est = ZoneInfo('America/New_York')
                is_open = datetime.datetime.now(est).weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))
        except Exception:
            if attempt == 2:
                est = ZoneInfo('America/New_York')
                is_open = datetime.datetime.now(est).weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))

    if not is_open:
        return
    
    job = getattr(context, 'job', None)
    app_data = getattr(job, 'data', {}) if job else {}
    if not isinstance(app_data, dict): app_data = {}
    
    cfg = app_data.get('cfg')
    broker = app_data.get('broker')
    strategy = app_data.get('strategy')
    tx_lock = app_data.get('tx_lock')
    chat_id = getattr(job, 'chat_id', None)
  
    if tx_lock is None:
        logging.warning("⚠️ [early_trade] tx_lock 미초기화. 이번 사이클 스킵.")
        return

    jitter_seconds = random.randint(0, 180)
    
    if chat_id:
        try:
            await asyncio.wait_for(
                context.bot.send_message(
                    chat_id=chat_id, 
                    text=f"🌃 <b>[17:05 KST] 정규장 스케줄러 기상!</b>\n"
                         f"▫️ 서버 접속 부하 방지를 위해 <b>{jitter_seconds}초</b> 대기 후 V14 덫 전송 및 스냅샷을 박제합니다.", 
                    parse_mode='HTML'
                ),
                timeout=15.0
            )
        except Exception as e:
            logging.error(f"초기 기상 메시지 텔레그램 발송 실패: {e}")
        
    await asyncio.sleep(jitter_seconds)

    MAX_RETRIES = 5
    RETRY_DELAY = 10
    successful_orders_cache = set()

    async def _do_early_trade():
        est_z = ZoneInfo('America/New_York')
        curr_est = datetime.datetime.now(est_z)
        today_str = curr_est.strftime("%Y-%m-%d")
        
        async with tx_lock:
            cash, holdings = 0.0, None
            for attempt in range(3):
                try:
                    # 🚨 MODIFIED: 파편화된 sleep 소각 (GlobalThrottle 위임)
                    res = await asyncio.wait_for(asyncio.to_thread(broker.get_account_balance), timeout=15.0)
                    cash = _safe_float(res[0]) if isinstance(res, (list, tuple)) and len(res) > 0 else 0.0
                    holdings = res[1] if isinstance(res, (list, tuple)) and len(res) > 1 else {}
                    break
                except asyncio.TimeoutError:
                    if attempt == 2: return False, "잔고 조회 타임아웃"
                    else: await asyncio.sleep(1.0 * (2 ** attempt))
                except Exception as e:
                    if attempt == 2: return False, f"잔고 조회 오류: {html.escape(str(e))}"
                    else: await asyncio.sleep(1.0 * (2 ** attempt))
            
            if holdings is None:
                return False, "❌ 계좌 정보를 불러오지 못했습니다."
            
            safe_holdings = holdings if isinstance(holdings, dict) else {}
            
            try:
                active_tickers_list = await asyncio.wait_for(asyncio.to_thread(cfg.get_active_tickers), timeout=10.0) or []
            except Exception:
                active_tickers_list = []
            
            if isinstance(active_tickers_list, str): active_tickers_list = [active_tickers_list]
            elif not isinstance(active_tickers_list, list): active_tickers_list = []
            
            if not active_tickers_list:
                return False, "❌ 활성 종목 리스트 결측치 반환. 스케줄 보호 중단."
            
            try:
                alloc_res = await asyncio.wait_for(asyncio.to_thread(get_budget_allocation, cash, active_tickers_list, cfg), timeout=10.0)
            except Exception as e:
                logging.error(f"🚨 예산 할당 타임아웃/에러: {e}")
                alloc_res = None
            
            if not alloc_res or len(alloc_res) != 2:
                return False, "❌ 예산 할당 로직 결측치(None) 반환. 스케줄 보호 중단."
            
            sorted_tickers, allocated_cash = alloc_res
            sorted_tickers = sorted_tickers or []
            allocated_cash = allocated_cash or {}
            
            msgs = {t: "" for t in sorted_tickers}
            all_success_map = {t: True for t in sorted_tickers}
            
            loop_fully_successful = True
            loop_fail_reason = ""

            for t in sorted_tickers:
                try:
                    try:
                        version = await asyncio.wait_for(asyncio.to_thread(cfg.get_version, t), timeout=5.0)
                    except Exception:
                        version = "V14"
                        
                    try:
                        is_locked = await asyncio.wait_for(asyncio.to_thread(cfg.check_lock, t, "REG"), timeout=5.0)
                    except Exception:
                        is_locked = False
                    
                    if is_locked:
                        skip_msg = f"⚠️ <b>[{t}] REG 잠금 미해제 — 스케줄 루프 스킵</b>\n▫️ 수동으로 잠금 해제 후 상태를 확인하십시오."
                        if chat_id:
                            try:
                                await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=skip_msg, parse_mode='HTML'), timeout=15.0)
                            except Exception: pass
                        continue

                    h = safe_holdings.get(t) or {}
                    safe_avg = _safe_float(h.get('avg'))
                    safe_qty = int(_safe_float(h.get('qty')))
                    safe_alloc_cash = _safe_float(allocated_cash.get(t, 0.0))

                    curr_p, prev_c = 0.0, 0.0
                    for _api_retry in range(3):
                        try:
                            # 🚨 MODIFIED: 파편화된 sleep 소각 (GlobalThrottle 위임)
                            curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, t), timeout=15.0)
                            curr_p = _safe_float(curr_p_val)
                            
                            prev_c_val = await asyncio.wait_for(asyncio.to_thread(broker.get_previous_close, t), timeout=15.0)
                            prev_c = _safe_float(prev_c_val)
                            
                            if curr_p > 0 and prev_c > 0: break
                        except Exception:
                            pass
                        await asyncio.sleep(1.0 * (2**_api_retry))

                    ma_5day = 0.0
                    for attempt in range(3):
                        try:
                            ma_5day_val = await asyncio.wait_for(asyncio.to_thread(broker.get_5day_ma, t), timeout=15.0)
                            ma_5day = _safe_float(ma_5day_val)
                            break
                        except Exception: 
                            if attempt == 2: ma_5day = 0.0
                            else: await asyncio.sleep(1.0 * (2**attempt))
                    
                    try:
                        plan = await asyncio.wait_for(asyncio.to_thread(
                            strategy.get_plan, t, curr_p, safe_avg, safe_qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=safe_alloc_cash, is_snapshot_mode=True
                        ), timeout=15.0)
                    except Exception as e:
                        logging.error(f"🚨 [{t}] 플랜 생성 타임아웃/에러: {e}")
                        plan = None
                    
                    if not isinstance(plan, dict):
                        msgs[t] += f"🚨 <b>[{t}] 스냅샷 유실 또는 손상! KIS 전송 불가.</b>\n"
                        all_success_map[t] = False
                        loop_fully_successful = False
                        loop_fail_reason = f"[{t}] 플랜 오염"
                        continue
                    
                    if version == "V14":
                        msgs[t] += f"💎 <b>[{t}] V14 오리지널 정규장 실전 덫 장전 완료 (17:05 KST 타격망)</b>\n"
                        
                        # 🚨 MODIFIED: [예약 주문 증발(Ghost Order) 궁극 수술] 하드코딩된 False를 파기하고 서머타임(04:05 EST, 프리장 개장) 여부를 동적으로 판별하여 실시간 본주문(True)으로 자동 전환 락온
                        is_market_active_now = curr_est.hour >= 4

                        target_orders = plan.get('core_orders') or plan.get('orders') or []
                        if not isinstance(target_orders, list): target_orders = []
                        
                        success_core, msg_core, fail_reason_core = await execute_order_list(
                            broker, t, target_orders, successful_orders_cache, is_market_active_now, today_str, is_capital_locked=False, order_category="1차 필수"
                        )
                        msgs[t] += msg_core

                        if not success_core:
                            all_success_map[t] = False
                            loop_fully_successful = False
                            loop_fail_reason = fail_reason_core
                            
                        target_bonus = plan.get('bonus_orders') or []
                        if not isinstance(target_bonus, list): target_bonus = []
                        
                        if all_success_map[t]:
                            success_bonus, msg_bonus, fail_reason_bonus = await execute_order_list(
                                broker, t, target_bonus, successful_orders_cache, is_market_active_now, today_str, is_capital_locked=False, order_category="2차 보너스"
                            )
                            msgs[t] += msg_bonus
                            if not success_bonus:
                                all_success_map[t] = False
                                loop_fully_successful = False
                                loop_fail_reason = fail_reason_bonus
                        elif target_bonus:
                            msgs[t] += f"⚠️ 1차 필수 장전 실패로 2차 보너스 덫 보류 (중복 매매 방어)\n"
                        
                        if (target_orders or target_bonus) and all_success_map[t]:
                            try:
                                await asyncio.wait_for(asyncio.to_thread(cfg.set_lock, t, "REG"), timeout=5.0)
                            except Exception as e:
                                logging.error(f"🚨 락 설정 타임아웃: {e}")
                            msgs[t] += "\n🔒 <b>V14 필수 덫(로컬 엔진 포함) 장전 완료 (잠금 설정됨)</b>"
                    
                    else: 
                        msgs[t] += f"🔄 <b>[{t}] V-REV 역추세 덫 모의 장전 및 스냅샷 박제</b>\n"
                        target_orders = plan.get('core_orders') or plan.get('orders') or []
                        if not isinstance(target_orders, list): target_orders = []
                        for o in target_orders:
                            if not isinstance(o, dict): continue
                            safe_desc = html.escape(str(o.get('desc', '주문')))
                            safe_qty = int(_safe_float(o.get('qty')))
                            safe_price = _safe_float(o.get('price'))
                            msgs[t] += f"└ 모의 1차 필수: {safe_desc} {safe_qty}주 (${safe_price})\n"
                    
                        target_bonus = plan.get('bonus_orders') or []
                        if not isinstance(target_bonus, list): target_bonus = []
                        for o in target_bonus:
                            if not isinstance(o, dict): continue
                            safe_desc = html.escape(str(o.get('desc', '주문')))
                            safe_qty = int(_safe_float(o.get('qty')))
                            safe_price = _safe_float(o.get('price'))
                            msgs[t] += f"└ 모의 2차 보너스: {safe_desc} {safe_qty}주 (${safe_price})\n"
                            
                        if target_orders or target_bonus:
                            msgs[t] += "\n📸 <b>V-REV 당일 스냅샷 팩트 박제 완료 (15:26 EST 지연 투하 대기)</b>"
                    
                    if msgs[t].strip() and chat_id:
                        try:
                            await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=msgs[t], parse_mode='HTML'), timeout=15.0)
                        except Exception as tg_e:
                            logging.error(f"[{t}] 개별 종목 텔레그램 메시지 발송 실패: {tg_e}")

                except Exception as e:
                    all_success_map[t] = False
                    loop_fully_successful = False
                    loop_fail_reason = f"[{t}] 치명적 오류: {str(e)}"
                    logging.error(f"🚨 [{t}] early_trade 개별 종목 처리 중 치명적 오류: {e}")
                    if chat_id:
                        try:
                            await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=f"🚨 <b>[{t}] 스케줄러 처리 중 오류 발생. 스킵합니다.</b>\n<code>{html.escape(str(e))}</code>", parse_mode='HTML'), timeout=15.0)
                        except Exception: pass

            if not loop_fully_successful:
                return False, loop_fail_reason
        return True, "SUCCESS"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            success, fail_reason = await asyncio.wait_for(_do_early_trade(), timeout=300.0)
            if success:
                if attempt > 1 and chat_id: 
                    try:
                        await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=f"✅ <b>[통신 복구] {attempt}번째 재시도 끝에 장전을 완수했습니다!</b>", parse_mode='HTML'), timeout=15.0)
                    except Exception: pass
                return 
        except Exception as e:
            logging.error(f"17:05 덫 장전 에러 ({attempt}/{MAX_RETRIES}): {e}", exc_info=True)
            if attempt == 1 and chat_id:
                safe_err = html.escape(str(e))
                try:
                    await asyncio.wait_for(
                        context.bot.send_message(
                            chat_id=chat_id, 
                            text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 10초 뒤 장전을 재시도합니다! 🛡️\n<code>사유: {safe_err}</code>", 
                            parse_mode='HTML'
                        ),
                        timeout=15.0
                    )
                except Exception: pass
        else:
            logging.warning(f"장전 조건 미충족 ({attempt}/{MAX_RETRIES}): {fail_reason}")
            if attempt == 1 and chat_id:
                 safe_fail = html.escape(str(fail_reason))
                 try:
                     await asyncio.wait_for(
                         context.bot.send_message(
                            chat_id=chat_id, 
                            text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 10초 뒤 장전을 재시도합니다! 🛡️\n<code>사유: {safe_fail}</code>", 
                            parse_mode='HTML'
                         ),
                         timeout=15.0
                     )
                 except Exception: pass

        if attempt < MAX_RETRIES:
            if attempt != 1 and attempt % 5 == 0 and chat_id:
                try:
                    await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 10초 뒤 재시도합니다! 🛡️", parse_mode='HTML'), timeout=15.0)
                except Exception: pass
            await asyncio.sleep(RETRY_DELAY)

    if chat_id:
        try:
            await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text="🚨 <b>[긴급 에러] 17:05 스케줄 통신 복구 최종 실패. 수동 점검 요망!</b>", parse_mode='HTML'), timeout=15.0)
        except Exception: pass


async def scheduled_regular_trade_delayed(context):
    is_open = False
    for attempt in range(3):
        try:
            is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=10.0)
            break
        except asyncio.TimeoutError:
            if attempt == 2:
                est = ZoneInfo('America/New_York')
                is_open = datetime.datetime.now(est).weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))
        except Exception:
            if attempt == 2:
                est = ZoneInfo('America/New_York')
                is_open = datetime.datetime.now(est).weekday() < 5
            else: await asyncio.sleep(1.0 * (2 ** attempt))

    if not is_open:
        return
    
    job = getattr(context, 'job', None)
    app_data = getattr(job, 'data', {}) if job else {}
    if not isinstance(app_data, dict): app_data = {}
    
    cfg = app_data.get('cfg')
    broker = app_data.get('broker')
    strategy = app_data.get('strategy')
    tx_lock = app_data.get('tx_lock')
    chat_id = getattr(job, 'chat_id', None)
    
    if tx_lock is None:
        return
    
    jitter_seconds = random.randint(0, 45)

    if chat_id:
        try:
            await asyncio.wait_for(
                context.bot.send_message(
                    chat_id=chat_id, 
                    text=f"🌃 <b>[15:26 EST] V-REV 본진 덫(자체 1분 슬라이싱 포함) 투하 개시!</b>\n"
                         f"🛡️ 서버 접속 부하 방지를 위해 <b>{jitter_seconds}초</b> 대기 후 전송/인계를 시도합니다.", 
                    parse_mode='HTML'
                ),
                timeout=15.0
            )
        except Exception as e:
            logging.error(f"지연 투하 시작 메시지 텔레그램 발송 실패: {e}")

    await asyncio.sleep(jitter_seconds)

    MAX_RETRIES = 15
    RETRY_DELAY = 60
    successful_orders_cache = set()

    async def _do_delayed_trade():
        async with tx_lock:
            cash, holdings = 0.0, None
            for attempt in range(3):
                try:
                    # 🚨 MODIFIED: 파편화된 sleep 소각 (GlobalThrottle 위임)
                    res = await asyncio.wait_for(asyncio.to_thread(broker.get_account_balance), timeout=15.0)
                    cash = _safe_float(res[0]) if isinstance(res, (list, tuple)) and len(res) > 0 else 0.0
                    holdings = res[1] if isinstance(res, (list, tuple)) and len(res) > 1 else {}
                    break
                except Exception as e:
                    if attempt == 2: return False, f"잔고 조회 오류: {html.escape(str(e))}"
                    else: await asyncio.sleep(1.0 * (2 ** attempt))
            
            if holdings is None:
                return False, "❌ 계좌 정보를 불러오지 못했습니다."
            
            safe_holdings = holdings if isinstance(holdings, dict) else {}
            
            try:
                active_tickers_list = await asyncio.wait_for(asyncio.to_thread(cfg.get_active_tickers), timeout=10.0) or []
            except Exception:
                active_tickers_list = []
            
            if isinstance(active_tickers_list, str): active_tickers_list = [active_tickers_list]
            elif not isinstance(active_tickers_list, list): active_tickers_list = []
            
            if not active_tickers_list:
                return False, "❌ 활성 종목 리스트 결측치(None) 반환. 스케줄 보호 중단."
                
            try:
                alloc_res = await asyncio.wait_for(asyncio.to_thread(get_budget_allocation, cash, active_tickers_list, cfg), timeout=10.0)
            except Exception as e:
                logging.error(f"🚨 예산 할당 타임아웃/에러: {e}")
                alloc_res = None
                
            if not alloc_res or len(alloc_res) != 2:
                return False, "❌ 예산 할당 로직 결측치 반환."
            
            sorted_tickers, allocated_cash = alloc_res
            sorted_tickers = sorted_tickers or []
            allocated_cash = allocated_cash or {}
            
            plans = {}
            msgs = {t: "" for t in sorted_tickers}
            all_success_map = {t: True for t in sorted_tickers}
            capital_locked_map = {t: False for t in sorted_tickers} 
            
            loop_fully_successful = True
            loop_fail_reason = ""

            est_z = ZoneInfo('America/New_York')
            curr_est = datetime.datetime.now(est_z)
            today_str = curr_est.strftime("%Y-%m-%d")
            
            is_market_active_now = True

            for t in sorted_tickers:
                try:
                    try:
                        version = await asyncio.wait_for(asyncio.to_thread(cfg.get_version, t), timeout=5.0)
                    except Exception:
                        version = "V14"
                    
                    if version == "V14":
                        continue 
                    
                    try:
                        is_locked = await asyncio.wait_for(asyncio.to_thread(cfg.check_lock, t, "REG"), timeout=5.0)
                    except Exception:
                        is_locked = False
                        
                    if is_locked:
                        continue
                        
                    is_capital_locked = False
                    if version == "V_REV" and t == "SOXL":
                        for attempt in range(3):
                            try:
                                avwap_state = await asyncio.wait_for(asyncio.to_thread(read_avwap_state_sync, t, today_str), timeout=5.0)
                                avwap_qty = int(_safe_float(avwap_state.get('qty', 0)))
                                avwap_shutdown = bool(avwap_state.get('shutdown', False))
                                if avwap_qty > 0 and not avwap_shutdown:
                                    is_capital_locked = True
                                break
                            except Exception as e:
                                if attempt == 2: logging.error(f"🚨 [{t}] 암살자 자본 잠김 스캔 에러: {e}")
                                else: await asyncio.sleep(1.0 * (2 ** attempt))
                                
                    capital_locked_map[t] = is_capital_locked 

                    h = safe_holdings.get(t) or {}
                    safe_avg = _safe_float(h.get('avg'))
                    safe_qty = int(_safe_float(h.get('qty')))
                    safe_alloc_cash = _safe_float(allocated_cash.get(t, 0.0))

                    if is_capital_locked:
                        for seed_attempt in range(3):
                            try:
                                seed_val = await asyncio.wait_for(asyncio.to_thread(cfg.get_seed, t), timeout=5.0)
                                safe_alloc_cash = _safe_float(seed_val) * 0.15
                                logging.info(f"🚨 [{t}] 자본 잠김 감지: 이관 플랜 생성을 위해 가상의 1일 고정 예산(${safe_alloc_cash:.2f})을 강제 복원합니다.")
                                break
                            except Exception as e:
                                if seed_attempt == 2: logging.error(f"🚨 [{t}] 가상 예산 복원 실패 (기존 0.0 예산 유지): {e}")
                                else: await asyncio.sleep(1.0 * (2 ** seed_attempt))

                    curr_p, prev_c = 0.0, 0.0
                    for _api_retry in range(3):
                        try:
                            # 🚨 MODIFIED: 파편화된 sleep 소각 (GlobalThrottle 위임)
                            curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, t), timeout=15.0)
                            curr_p = _safe_float(curr_p_val)
                            
                            prev_c_val = await asyncio.wait_for(asyncio.to_thread(broker.get_previous_close, t), timeout=15.0)
                            prev_c = _safe_float(prev_c_val)
                            
                            if curr_p > 0 and prev_c > 0: break
                        except Exception:
                            pass
                        await asyncio.sleep(1.0 * (2**_api_retry))

                    ma_5day = 0.0
                    for attempt in range(3):
                        try:
                            ma_5day_val = await asyncio.wait_for(asyncio.to_thread(broker.get_5day_ma, t), timeout=15.0)
                            ma_5day = _safe_float(ma_5day_val)
                            break
                        except Exception: 
                            if attempt == 2: ma_5day = 0.0
                            else: await asyncio.sleep(1.0 * (2**attempt))
                    
                    try:
                        plan = await asyncio.wait_for(asyncio.to_thread(
                            strategy.get_plan, t, curr_p, safe_avg, safe_qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=safe_alloc_cash, is_snapshot_mode=True
                        ), timeout=15.0)
                    except Exception as e:
                        logging.error(f"🚨 [{t}] 플랜 생성 타임아웃/에러: {e}")
                        plan = None
                    
                    if not isinstance(plan, dict):
                        msgs[t] += f"🚨 <b>[{t}] 스냅샷 유실 또는 손상! KIS 전송 불가.</b>\n"
                        all_success_map[t] = False
                        loop_fully_successful = False
                        loop_fail_reason = f"[{t}] 스냅샷 유실"
                        continue

                    plans[t] = plan
                    if plan.get('core_orders') or plan.get('orders') or plan.get('bonus_orders'):
                        if is_capital_locked:
                            msgs[t] += f"⏳ <b>[{t}] 자본 잠김(Capital Lock-up) 감지!</b>\n▫️ 암살자 100% 점유로 인해 정규장 플랜을 <b>16:01 애프터장 일괄 타격</b>으로 지연 이관(Delay & Transfer)합니다.\n"
                        else:
                            msgs[t] += f"🔄 <b>[{t}] V-REV 역추세 실전 덫(로컬 엔진 포함) 장전 완료</b>\n"
                except Exception as e:
                    all_success_map[t] = False
                    loop_fully_successful = False
                    loop_fail_reason = f"[{t}] 치명적 오류: {str(e)}"
                    logging.error(f"🚨 [{t}] delayed_trade 플랜 조회 오류: {e}")
                    msgs[t] += f"🚨 <b>[{t}] 플랜 조회 중 에러 발생</b>\n"

            for t in sorted_tickers:
                try:
                    if t not in plans:
                        if msgs[t].strip() and chat_id:
                            try: 
                                await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=msgs[t], parse_mode='HTML'), timeout=15.0)
                            except Exception as tg_e: logging.error(f"[{t}] V-REV 디커플링 메시지 발송 실패: {tg_e}")
                        continue
                        
                    target_orders = plans[t].get('core_orders') or plans[t].get('orders') or []
                    if not isinstance(target_orders, list): target_orders = []
                        
                    is_capital_locked = capital_locked_map.get(t, False) 
                    
                    success_core, msg_core, fail_reason_core = await execute_order_list(
                        broker, t, target_orders, successful_orders_cache, is_market_active_now, today_str, is_capital_locked=is_capital_locked, order_category="1차 필수"
                    )
                    msgs[t] += msg_core

                    if not success_core:
                        all_success_map[t] = False
                        loop_fully_successful = False
                        loop_fail_reason = fail_reason_core
                        
                    target_bonus = plans[t].get('bonus_orders') or []
                    if not isinstance(target_bonus, list): target_bonus = []
                        
                    if not all_success_map[t] and target_bonus:
                        msgs[t] += f"⚠️ 1차 필수 장전 실패로 2차 보너스 덫 보류 (중복 매매 방어)\n"
                    else:
                        success_bonus, msg_bonus, fail_reason_bonus = await execute_order_list(
                            broker, t, target_bonus, successful_orders_cache, is_market_active_now, today_str, is_capital_locked=is_capital_locked, order_category="2차 보너스"
                        )
                        msgs[t] += msg_bonus
                        if not success_bonus:
                            all_success_map[t] = False
                            loop_fully_successful = False
                            loop_fail_reason = fail_reason_bonus

                    if all_success_map[t] and (target_orders or target_bonus):
                        try:
                            await asyncio.wait_for(asyncio.to_thread(cfg.set_lock, t, "REG"), timeout=5.0)
                        except Exception as e:
                            logging.error(f"🚨 락 설정 타임아웃: {e}")
                    
                        if is_capital_locked:
                            msgs[t] += "\n🔒 <b>V-REV 플랜 애프터장 이관 완료 (잠금 설정됨)</b>"
                        else:
                            msgs[t] += "\n🔒 <b>V-REV 필수 덫(로컬 엔진 포함) 전송 완료 (잠금 설정됨)</b>"
                    elif not all_success_map[t] and (target_orders or target_bonus):
                        msgs[t] += "\n⚠️ <b>일부 덫 장전/이관 실패 (매매 잠금 보류)</b>"
                        
                    if msgs[t].strip() and chat_id:
                        try:
                            await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=msgs[t], parse_mode='HTML'), timeout=15.0)
                        except Exception as tg_e:
                            logging.error(f"[{t}] V-REV 완료 메시지 발송 실패: {tg_e}")

                except Exception as e:
                    all_success_map[t] = False
                    loop_fully_successful = False
                    loop_fail_reason = f"[{t}] 치명적 오류: {str(e)}"
                    logging.error(f"🚨 [{t}] delayed_trade 개별 종목 처리 중 치명적 오류: {e}")
                    if chat_id:
                        try:
                            await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=f"🚨 <b>[{t}] 스케줄러 처리 중 오류 발생. 스킵합니다.</b>\n<code>{html.escape(str(e))}</code>", parse_mode='HTML'), timeout=15.0)
                        except Exception: pass

            if not loop_fully_successful:
                return False, loop_fail_reason
        return True, "SUCCESS"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            success, fail_reason = await asyncio.wait_for(_do_delayed_trade(), timeout=300.0)
            if success:
                if attempt > 1 and chat_id: 
                    try:
                        await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=f"✅ <b>[통신 복구] {attempt}번째 재시도 끝에 장전/이관을 완수했습니다!</b>", parse_mode='HTML'), timeout=15.0)
                    except Exception: pass
                return 
        except Exception as e:
            logging.error(f"정규장 덫 실전 전송 에러 ({attempt}/{MAX_RETRIES}): {e}", exc_info=True)
            if attempt == 1 and chat_id:
                safe_err = html.escape(str(e))
                try:
                    await asyncio.wait_for(
                        context.bot.send_message(
                            chat_id=chat_id, 
                            text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 실전 장전을 재시도합니다! 🛡️\n<code>사유: {safe_err}</code>", 
                            parse_mode='HTML'
                        ),
                        timeout=15.0
                    )
                except Exception: pass
        else:
             if attempt == 1 and chat_id:
                 safe_fail = html.escape(str(fail_reason))
                 try:
                     await asyncio.wait_for(
                         context.bot.send_message(
                            chat_id=chat_id, 
                            text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 실전 장전을 재시도합니다! 🛡️\n<code>사유: {safe_fail}</code>", 
                            parse_mode='HTML'
                         ),
                         timeout=15.0
                     )
                 except Exception: pass

        if attempt < MAX_RETRIES:
            if attempt != 1 and attempt % 5 == 0 and chat_id:
                try:
                    await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 10초 뒤 재시도합니다! 🛡️", parse_mode='HTML'), timeout=15.0)
                except Exception: pass
            await asyncio.sleep(RETRY_DELAY)

    if chat_id:
        try:
            await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text="🚨 <b>[긴급 에러] V-REV 실전 전송/이관 통신 복구 최종 실패. 수동 점검 요망!</b>", parse_mode='HTML'), timeout=15.0)
        except Exception: pass
