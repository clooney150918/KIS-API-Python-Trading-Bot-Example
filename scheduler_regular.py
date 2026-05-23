# ==========================================================
# FILE: scheduler_regular.py
# ==========================================================
# 🚨 MODIFIED: [V73.15 타임라인 디커플링 대통합] 17:05 KST V14 선제 타격 및 V-REV 스냅샷 분리 락온
# 🚨 MODIFIED: [Case 20 준수] b_start = max(b_start, s_start) 연산 100% 동기화 적용
# 🚨 NEW: [Case 32 & 33 절대 규칙] 3단 지수 백오프 및 스케줄러 루프 TPS 캡핑 이식 완료
# 🚨 MODIFIED: [Case 14 절대 헌법 준수] is_market_open 비동기 호출 타임아웃 10.0초 하드코딩 완료
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import asyncio
import random
import html

from scheduler_core import is_market_open, get_budget_allocation

async def scheduled_early_regular_trade(context):
    is_open = False
    for attempt in range(3):
        try:
            # MODIFIED: [Case 14] 달력 API 타임아웃 10초 하드코딩 락온
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
    
    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    
    if tx_lock is None:
        logging.warning("⚠️ [early_trade] tx_lock 미초기화. 이번 사이클 스킵.")
        return

    jitter_seconds = random.randint(0, 180)
    await context.bot.send_message(
        chat_id=context.job.chat_id, 
        text=f"🌃 <b>[17:05 KST] 정규장 스케줄러 기상!</b>\n"
             f"▫️ 서버 접속 부하 방지를 위해 <b>{jitter_seconds}초</b> 대기 후 V14 덫 전송 및 스냅샷을 박제합니다.", 
        parse_mode='HTML'
    )
    await asyncio.sleep(jitter_seconds)

    MAX_RETRIES = 5
    RETRY_DELAY = 10

    async def _do_early_trade():
        est_z = ZoneInfo('America/New_York')
        kst_z = ZoneInfo('Asia/Seoul')
        curr_est = datetime.datetime.now(est_z)
        
        async with tx_lock:
            cash, holdings = 0.0, None
            for attempt in range(3):
                try:
                    cash, holdings = await asyncio.wait_for(asyncio.to_thread(broker.get_account_balance), timeout=15.0)
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
            active_tickers_list = await asyncio.to_thread(cfg.get_active_tickers)
            sorted_tickers, allocated_cash = await asyncio.to_thread(get_budget_allocation, cash, active_tickers_list, cfg)
            
            msgs = {t: "" for t in sorted_tickers}
            all_success_map = {t: True for t in sorted_tickers}

            for t in sorted_tickers:
                await asyncio.sleep(0.06) # 🚨 NEW: [Case 32]
                
                version = await asyncio.to_thread(cfg.get_version, t)
                is_locked = await asyncio.to_thread(cfg.check_lock, t, "REG")
                if is_locked:
                    skip_msg = f"⚠️ <b>[{t}] REG 잠금 미해제 — 스케줄 루프 스킵</b>\n▫️ 수동으로 잠금 해제 후 상태를 확인하십시오."
                    await context.bot.send_message(context.job.chat_id, skip_msg, parse_mode='HTML')
                    continue
                
                h = safe_holdings.get(t) or {}
                safe_avg = float(h.get('avg') or 0.0)
                safe_qty = int(float(h.get('qty') or 0))

                curr_p, prev_c = 0.0, 0.0
                for _api_retry in range(3):
                    try:
                        curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, t), timeout=15.0)
                        curr_p = float(curr_p_val or 0.0)
                        prev_c_val = await asyncio.wait_for(asyncio.to_thread(broker.get_previous_close, t), timeout=15.0)
                        prev_c = float(prev_c_val or 0.0)
                        if curr_p > 0 and prev_c > 0: break
                    except Exception:
                        pass
                    await asyncio.sleep(1.0 * (2**_api_retry))

                ma_5day = 0.0
                for attempt in range(3):
                    try:
                        ma_5day_val = await asyncio.wait_for(asyncio.to_thread(broker.get_5day_ma, t), timeout=15.0)
                        ma_5day = float(ma_5day_val or 0.0)
                        break
                    except Exception: 
                        if attempt == 2: ma_5day = 0.0
                        else: await asyncio.sleep(1.0 * (2**attempt))
                
                plan = await asyncio.to_thread(
                    strategy.get_plan, t, curr_p, safe_avg, safe_qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash.get(t, 0.0), is_snapshot_mode=True
                )
                
                if version == "V14":
                    msgs[t] += f"💎 <b>[{t}] V14 오리지널 정규장 실전 덫 장전 완료 (17:05 KST 타격망)</b>\n"
                    
                    b_start = curr_est.replace(hour=15, minute=26, second=0, microsecond=0)
                    s_start = curr_est + datetime.timedelta(minutes=3)
                    b_start = max(b_start, s_start)
                    b_end = curr_est.replace(hour=15, minute=56, second=0, microsecond=0)
                    
                    dyn_start_t = b_start.astimezone(kst_z).strftime("%H%M%S")
                    dyn_end_t = b_end.astimezone(kst_z).strftime("%H%M%S")

                    target_orders = plan.get('core_orders', plan.get('orders', []))
                    for o in target_orders:
                        if o['type'] == 'VWAP':
                            res = await asyncio.to_thread(broker.send_order, t, o['side'], o['qty'], o['price'], o['type'], start_time=dyn_start_t, end_time=dyn_end_t)
                        else:
                            res = await asyncio.to_thread(broker.send_reservation_order, t, o['side'], o['qty'], o['price'], o['type'])
                        
                        is_success = res.get('rt_cd') == '0'
                        if not is_success: all_success_map[t] = False

                        err_msg = html.escape(res.get('msg1', '오류'))
                        status_icon = '✅' if is_success else f'❌({err_msg})'
                        msgs[t] += f"└ 1차 필수: {o['desc']} {o['qty']}주 (${o['price']}): {status_icon}\n"
                        await asyncio.sleep(0.2)
                        
                    target_bonus = plan.get('bonus_orders', [])
                    for o in target_bonus:
                        if o['type'] == 'VWAP':
                            res = await asyncio.to_thread(broker.send_order, t, o['side'], o['qty'], o['price'], o['type'], start_time=dyn_start_t, end_time=dyn_end_t)
                        else:
                            res = await asyncio.to_thread(broker.send_reservation_order, t, o['side'], o['qty'], o['price'], o['type'])
                        
                        is_success = res.get('rt_cd') == '0'
                        err_msg = html.escape(res.get('msg1', '잔금패스'))
                        status_icon = '✅' if is_success else f'❌({err_msg})'
                        msgs[t] += f"└ 2차 보너스: {o['desc']} {o['qty']}주 (${o['price']}): {status_icon}\n"
                        await asyncio.sleep(0.2)
                    
                    if (target_orders or target_bonus) and all_success_map[t]:
                        await asyncio.to_thread(cfg.set_lock, t, "REG")
                        msgs[t] += "\n🔒 <b>V14 필수 덫 KIS 실원장 전송 완료 (잠금 설정됨)</b>"
                
                else: 
                    msgs[t] += f"🔄 <b>[{t}] V-REV 역추세 덫 모의 장전 및 스냅샷 박제</b>\n"
                    target_orders = plan.get('core_orders', plan.get('orders', []))
                    for o in target_orders:
                        msgs[t] += f"└ 모의 1차 필수: {o['desc']} {o['qty']}주 (${o['price']})\n"
                    target_bonus = plan.get('bonus_orders', [])
                    for o in target_bonus:
                        msgs[t] += f"└ 모의 2차 보너스: {o['desc']} {o['qty']}주 (${o['price']})\n"
                    if target_orders or target_bonus:
                        msgs[t] += "\n📸 <b>V-REV 당일 스냅샷 팩트 박제 완료 (15:26 EST 지연 투하 대기)</b>"
                
                if msgs[t]:
                    await context.bot.send_message(chat_id=context.job.chat_id, text=msgs[t], parse_mode='HTML')

        return True, "SUCCESS"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            success, fail_reason = await asyncio.wait_for(_do_early_trade(), timeout=300.0)
            if success:
                if attempt > 1: await context.bot.send_message(chat_id=context.job.chat_id, text=f"✅ <b>[통신 복구] {attempt}번째 재시도 끝에 장전을 완수했습니다!</b>", parse_mode='HTML')
                return 
        except Exception as e:
            logging.error(f"17:05 덫 장전 에러 ({attempt}/{MAX_RETRIES}): {e}", exc_info=True)
            if attempt == 1:
                safe_err = html.escape(str(e))
                await context.bot.send_message(
                    chat_id=context.job.chat_id, 
                    text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 장전을 재시도합니다! 🛡️\n<code>사유: {safe_err}</code>", 
                    parse_mode='HTML'
                )
        else:
            logging.warning(f"장전 조건 미충족 ({attempt}/{MAX_RETRIES}): {fail_reason}")
            if attempt == 1:
                 safe_fail = html.escape(str(fail_reason))
                 await context.bot.send_message(
                    chat_id=context.job.chat_id, 
                    text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 장전을 재시도합니다! 🛡️\n<code>사유: {safe_fail}</code>", 
                    parse_mode='HTML'
                 )

        if attempt < MAX_RETRIES:
            if attempt != 1 and attempt % 5 == 0:
                await context.bot.send_message(chat_id=context.job.chat_id, text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 재시도합니다! 🛡️", parse_mode='HTML')
            await asyncio.sleep(RETRY_DELAY)

    await context.bot.send_message(chat_id=context.job.chat_id, text="🚨 <b>[긴급 에러] 17:05 스케줄 통신 복구 최종 실패. 수동 점검 요망!</b>", parse_mode='HTML')


async def scheduled_regular_trade_delayed(context):
    is_open = False
    for attempt in range(3):
        try:
            # MODIFIED: [Case 14] 달력 API 타임아웃 10초 하드코딩 락온
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
    
    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    
    if tx_lock is None:
        return
    
    jitter_seconds = random.randint(0, 180)

    await context.bot.send_message(
        chat_id=context.job.chat_id, 
        text=f"🌃 <b>[15:26 EST] V-REV 본진 덫 KIS 실원장 투하 개시!</b>\n"
             f"🛡️ 서버 접속 부하 방지를 위해 <b>{jitter_seconds}초</b> 대기 후 V-REV 주문 전송을 시도합니다.", 
        parse_mode='HTML'
    )

    await asyncio.sleep(jitter_seconds)

    MAX_RETRIES = 15
    RETRY_DELAY = 60

    async def _do_delayed_trade():
        async with tx_lock:
            active_tickers_list = await asyncio.to_thread(cfg.get_active_tickers)
            
            plans = {}
            msgs = {t: "" for t in active_tickers_list}
            all_success_map = {t: True for t in active_tickers_list}

            est_z = ZoneInfo('America/New_York')
            kst_z = ZoneInfo('Asia/Seoul')
            curr_est = datetime.datetime.now(est_z)
            
            b_start = curr_est.replace(hour=15, minute=26, second=0, microsecond=0)
            s_start = curr_est + datetime.timedelta(minutes=3)
            a_start = max(b_start, s_start)
            b_end = curr_est.replace(hour=15, minute=56, second=0, microsecond=0)
            
            dyn_start_t = a_start.astimezone(kst_z).strftime("%H%M%S")
            dyn_end_t = b_end.astimezone(kst_z).strftime("%H%M%S")

            for t in active_tickers_list:
                await asyncio.sleep(0.06) # 🚨 NEW: [Case 32]
                
                version = await asyncio.to_thread(cfg.get_version, t)
                if version == "V14":
                    continue 
                    
                is_locked = await asyncio.to_thread(cfg.check_lock, t, "REG")
                if is_locked:
                    continue
                
                plan = await asyncio.to_thread(
                    strategy.get_plan, t, 0.0, 0.0, 0, 0.0, ma_5day=0.0, market_type="REG", available_cash=0.0, is_snapshot_mode=False
                )
                
                if not plan:
                    msgs[t] += f"🚨 <b>[{t}] 스냅샷 유실! KIS 전송 불가.</b>\n"
                    all_success_map[t] = False
                    continue

                plans[t] = plan
                if plan.get('core_orders', []) or plan.get('orders', []):
                    msgs[t] += f"🔄 <b>[{t}] V-REV 역추세 실전 덫 장전 완료</b>\n"

            for t in active_tickers_list:
                if t not in plans: continue
                target_orders = plans[t].get('core_orders', plans[t].get('orders', []))
                for o in target_orders:
                    res = await asyncio.to_thread(
                        broker.send_order, 
                        t, o['side'], o['qty'], o['price'], o['type'],
                        start_time=dyn_start_t if o['type'] == 'VWAP' else None, 
                        end_time=dyn_end_t if o['type'] == 'VWAP' else None
                    )
                    
                    is_success = res.get('rt_cd') == '0'
                    if not is_success: all_success_map[t] = False

                    err_msg = html.escape(res.get('msg1', '오류'))
                    status_icon = '✅' if is_success else f'❌({err_msg})'
                    msgs[t] += f"└ 1차 필수: {o['desc']} {o['qty']}주 (${o['price']}): {status_icon}\n"
                    await asyncio.sleep(0.2)
                    
            for t in active_tickers_list:
                if t not in plans: continue
                target_bonus = plans[t].get('bonus_orders', [])
                for o in target_bonus:
                    res = await asyncio.to_thread(
                        broker.send_order, 
                        t, o['side'], o['qty'], o['price'], o['type'],
                        start_time=dyn_start_t if o['type'] == 'VWAP' else None, 
                        end_time=dyn_end_t if o['type'] == 'VWAP' else None
                    )
                    
                    is_success = res.get('rt_cd') == '0'
                    err_msg = html.escape(res.get('msg1', '잔금패스'))
                    status_icon = '✅' if is_success else f'❌({err_msg})'
                    msgs[t] += f"└ 2차 보너스: {o['desc']} {o['qty']}주 (${o['price']}): {status_icon}\n"
                    await asyncio.sleep(0.2) 

            for t in active_tickers_list:
                if t not in plans: continue
                target_orders = plans[t].get('core_orders', plans[t].get('orders', []))
                target_bonus = plans[t].get('bonus_orders', [])
                if not target_orders and not target_bonus: continue
                
                if all_success_map[t] and len(target_orders) > 0:
                    await asyncio.to_thread(cfg.set_lock, t, "REG")
                    msgs[t] += "\n🔒 <b>V-REV 필수 덫 실원장 전송 완료 (잠금 설정됨)</b>"
                elif not all_success_map[t] and len(target_orders) > 0:
                    msgs[t] += "\n⚠️ <b>일부 덫 장전 실패 (매매 잠금 보류)</b>"
                    
                await context.bot.send_message(chat_id=context.job.chat_id, text=msgs[t], parse_mode='HTML')

        return True, "SUCCESS"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            success, fail_reason = await asyncio.wait_for(_do_delayed_trade(), timeout=300.0)
            if success:
                return 
        except Exception as e:
            logging.error(f"정규장 덫 실전 전송 에러 ({attempt}/{MAX_RETRIES}): {e}", exc_info=True)
            if attempt == 1:
                safe_err = html.escape(str(e))
                await context.bot.send_message(
                    chat_id=context.job.chat_id, 
                    text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 실전 장전을 재시도합니다! 🛡️\n<code>사유: {safe_err}</code>", 
                    parse_mode='HTML'
                )
        else:
            if attempt == 1:
                 safe_fail = html.escape(str(fail_reason))
                 await context.bot.send_message(
                    chat_id=context.job.chat_id, 
                    text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 실전 장전을 재시도합니다! 🛡️\n<code>사유: {safe_fail}</code>", 
                    parse_mode='HTML'
                 )

        if attempt < MAX_RETRIES:
            await asyncio.sleep(RETRY_DELAY)

    await context.bot.send_message(chat_id=context.job.chat_id, text="🚨 <b>[긴급 에러] V-REV 실전 전송 통신 복구 최종 실패. 수동 점검 요망!</b>", parse_mode='HTML')
