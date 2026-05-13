# ==========================================================
# FILE: scheduler_regular.py
# ==========================================================
# MODIFIED: [V53.06 전투 사령부 외부 통신 10초 타임아웃 및 폴백 방어막 이식]
# 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
# 제1헌법: queue_ledger.get_queue 등 모든 파일 I/O 및 락 점유 메서드는 무조건 asyncio.to_thread로 래핑하여 이벤트 루프 교착(Deadlock)을 원천 차단함.
# MODIFIED: [V44.47 이벤트 루프 데드락 영구 소각] 동기식 블로킹 호출 전면 비동기 래핑.
# 🚨 MODIFIED: [V54.02 깡통 스냅샷 붕괴 방어] V-REV 예방 덫 소각 시 생성되는 더미 스냅샷에 prev_c 및 is_zero_start 팩트 다이렉트 주입 락온
# 🚨 MODIFIED: [V-REV 데이터 기아 방어] 통신 장애로 0.0 폴백 시 루프 조기 탈출(continue) 무시 및 깡통 스냅샷 팩트 박제 락온.
# 🚨 NEW: [KIS VWAP 알고리즘 권한 위임 수술] V-REV 수동 덫 장전 경고문 및 바이패스 분기를 전면 소각. 17:05 KST 기상 시 산출된 단일 지시서를 KIS VWAP 예약 주문 및 LOC 예약 주문으로 즉각 전송하고 예약 주문 번호(ODNO)를 로컬 캐시에 영속화하는 라우팅 배선 100% 개통 완료.
# 🚨 MODIFIED: [V71.05 정규장 스케줄러 라이브 주문 런타임 붕괴 수술 및 시간 인젝션]
# 🚨 MODIFIED: [V71.12 로컬 캐시 의존성 영구 소각 및 코어 압축]
# 🚨 MODIFIED: [V71.14 지정가 VWAP 일반주문(Regular Order) 100% 팩트 락온]
# - KIS 명세에 따라 VWAP(36)은 일반주문 API(TTTT1002U)로만 전송되어야 하므로, 
#   예약주문 API로 쏘아 Reject 당하던 하극상 맹점을 원천 차단하고 분기 라우팅을 100% 역배선 완료.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import asyncio
import random

from scheduler_core import is_market_open, get_budget_allocation

async def scheduled_regular_trade(context):
    try:
        is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=10.0)
    except asyncio.TimeoutError:
        logging.error("⚠️ is_market_open 달력 API 타임아웃. 평일이므로 강제 개장 처리합니다.")
        est = ZoneInfo('America/New_York')
        is_open = datetime.datetime.now(est).weekday() < 5

    if not is_open:
        return
    
    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    
    if tx_lock is None:
        logging.warning("⚠️ [regular_trade] tx_lock 미초기화. 이번 사이클 스킵.")
        await context.bot.send_message(chat_id=context.job.chat_id, text="⚠️ <b>[시스템 경고]</b> tx_lock 미초기화로 정규장 주문을 1회 스킵합니다.", parse_mode='HTML')
        return
    
    jitter_seconds = random.randint(0, 180)

    await context.bot.send_message(
        chat_id=context.job.chat_id, 
        text=f"🌃 <b>[04:05 EST] 통합 주문 장전 및 스냅샷 박제!</b>\n"
             f"🛡️ 서버 접속 부하 방지를 위해 <b>{jitter_seconds}초</b> 대기 후 안전하게 주문 전송을 시도합니다.", 
        parse_mode='HTML'
    )

    await asyncio.sleep(jitter_seconds)

    MAX_RETRIES = 15
    RETRY_DELAY = 60

    async def _do_regular_trade():
        est = ZoneInfo('America/New_York')
        _now_est = datetime.datetime.now(est)
        today_str_est = _now_est.strftime("%Y-%m-%d")
        
        async with tx_lock:
            try:
                cash, holdings = await asyncio.wait_for(asyncio.to_thread(broker.get_account_balance), timeout=10.0)
            except asyncio.TimeoutError:
                logging.warning("⚠️ [regular_trade] 잔고 조회 타임아웃 (10초).")
                return False, "잔고 조회 타임아웃"
            except Exception as e:
                return False, f"잔고 조회 오류: {e}"
            
            if holdings is None:
                return False, "❌ 계좌 정보를 불러오지 못했습니다."
            
            safe_holdings = holdings if isinstance(holdings, dict) else {}

            active_tickers_list = await asyncio.to_thread(cfg.get_active_tickers)
            sorted_tickers, allocated_cash = await asyncio.to_thread(get_budget_allocation, cash, active_tickers_list, cfg)
            
            plans = {}
            msgs = {t: "" for t in sorted_tickers}
            all_success_map = {t: True for t in sorted_tickers}

            for t in sorted_tickers:
                is_locked = await asyncio.to_thread(cfg.check_lock, t, "REG")
                if is_locked:
                    skip_msg = (
                        f"⚠️ <b>[{t}] REG 잠금 미해제 — 주문 스킵</b>\n"
                        f"▫️ 전날 REG 잠금이 자정 초기화 시 해제되지 않아 오늘 04:05 EST 주문 루프에서 제외되었습니다.\n"
                        f"▫️ 수동으로 잠금 해제 후 상태를 확인하십시오."
                    )
                    await context.bot.send_message(context.job.chat_id, skip_msg, parse_mode='HTML')
                    continue
                
                h = safe_holdings.get(t) or {}
                safe_avg = float(h.get('avg') or 0.0)
                safe_qty = int(float(h.get('qty') or 0))

                curr_p = 0.0
                prev_c = 0.0
                for _api_retry in range(3):
                    try:
                        curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, t), timeout=10.0)
                        curr_p = float(curr_p_val or 0.0)
                    except Exception:
                        curr_p = 0.0
                        
                    try:
                        prev_c_val = await asyncio.wait_for(asyncio.to_thread(broker.get_previous_close, t), timeout=10.0)
                        prev_c = float(prev_c_val or 0.0)
                    except Exception:
                        prev_c = 0.0
                        
                    if curr_p > 0 and prev_c > 0:
                        break
                    await asyncio.sleep(2.0)

                if curr_p <= 0 or prev_c <= 0:
                    msgs[t] += (
                        f"🚨 <b>[{t}] 전일 종가/현재가 API 3회 결측 감지!</b>\n"
                        f"▫️ 0.0 폴백 상태이나 깡통 스냅샷(0.0) 박제를 위해 런타임을 강제 진행합니다.\n"
                    )

                try:
                    ma_5day_val = await asyncio.wait_for(asyncio.to_thread(broker.get_5day_ma, t), timeout=10.0)
                    ma_5day = float(ma_5day_val or 0.0)
                except Exception:
                    ma_5day = 0.0
                
                # 🚨 MODIFIED: [KIS VWAP 알고리즘 대통합] V-REV 및 VWAP 수동/우회 처리 전면 소각 및 단일 통짜 플랜 로드 락온
                plan = await asyncio.to_thread(
                    strategy.get_plan,
                    t, curr_p, safe_avg, safe_qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash.get(t, 0.0), is_snapshot_mode=True
                )
                
                plans[t] = plan
                if plan.get('core_orders', []) or plan.get('orders', []):
                    is_rev = plan.get('is_reverse', False)
                    msgs[t] += f"🔄 <b>[{t}] 리버스(VWAP) 예약 덫 장전 완료</b>\n" if is_rev else f"💎 <b>[{t}] 정규장(LOC/VWAP) 덫 장전 완료</b>\n"

            # 🚨 MODIFIED: [V71.14 일반주문 배선 복구] VWAP은 예약이 아닌 일반주문으로 직결
            for t in sorted_tickers:
                if t not in plans: continue
                target_orders = plans[t].get('core_orders', plans[t].get('orders', []))
                for o in target_orders:
                    if o['type'] == "VWAP":
                        res = await asyncio.to_thread(
                            broker.send_order, 
                            t, o['side'], o['qty'], o['price'], o['type'],
                            start_time=o.get('start_time'), end_time=o.get('end_time')
                        )
                    else:
                        res = await asyncio.to_thread(
                            broker.send_reservation_order, 
                            t, o['side'], o['qty'], o['price'], o['type']
                        )
                    
                    is_success = res.get('rt_cd') == '0'
                    if not is_success: all_success_map[t] = False

                    # 🚨 [V71.12 로컬 캐시 의존성 영구 소각] _save_resv_odno_sync 호출부 전면 적출 완료.
                    err_msg = res.get('msg1', '오류')
                    status_icon = '✅' if is_success else f'❌({err_msg})'
                    msgs[t] += f"└ 1차 필수: {o['desc']} {o['qty']}주 (${o['price']}): {status_icon}\n"
                    await asyncio.sleep(0.2)
                    
            for t in sorted_tickers:
                if t not in plans: continue
                target_bonus = plans[t].get('bonus_orders', [])
                for o in target_bonus:
                    # 🚨 MODIFIED: [V71.14 일반주문 배선 복구] VWAP은 예약이 아닌 일반주문으로 직결
                    if o['type'] == "VWAP":
                        res = await asyncio.to_thread(
                            broker.send_order, 
                            t, o['side'], o['qty'], o['price'], o['type'],
                            start_time=o.get('start_time'), end_time=o.get('end_time')
                        )
                    else:
                        res = await asyncio.to_thread(
                            broker.send_reservation_order, 
                            t, o['side'], o['qty'], o['price'], o['type']
                        )
                    
                    is_success = res.get('rt_cd') == '0'

                    # 🚨 [V71.12 로컬 캐시 의존성 영구 소각] _save_resv_odno_sync 호출부 전면 적출 완료.
                    err_msg = res.get('msg1', '잔금패스')
                    status_icon = '✅' if is_success else f'❌({err_msg})'
                    msgs[t] += f"└ 2차 보너스: {o['desc']} {o['qty']}주 (${o['price']}): {status_icon}\n"
                    await asyncio.sleep(0.2) 

            for t in sorted_tickers:
                if t not in plans: continue
                target_orders = plans[t].get('core_orders', plans[t].get('orders', []))
                target_bonus = plans[t].get('bonus_orders', [])
                if not target_orders and not target_bonus: continue
                
                if all_success_map[t] and len(target_orders) > 0:
                    await asyncio.to_thread(cfg.set_lock, t, "REG")
                    msgs[t] += "\n🔒 <b>필수 덫 정상 전송 완료 (잠금 설정됨)</b>"
                elif not all_success_map[t] and len(target_orders) > 0:
                    msgs[t] += "\n⚠️ <b>일부 덫 장전 실패 (매매 잠금 보류)</b>"
                elif len(target_bonus) > 0:
                    await asyncio.to_thread(cfg.set_lock, t, "REG")
                    msgs[t] += "\n🔒 <b>보너스 덫만 전송 완료 (잠금 설정됨)</b>"
                    
                await context.bot.send_message(chat_id=context.job.chat_id, text=msgs[t], parse_mode='HTML')

            return True, "SUCCESS"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            success, fail_reason = await asyncio.wait_for(_do_regular_trade(), timeout=300.0)
            if success:
                if attempt > 1:
                    await context.bot.send_message(chat_id=context.job.chat_id, text=f"✅ <b>[통신 복구] {attempt}번째 재시도 끝에 덫 장전을 완수했습니다!</b>", parse_mode='HTML')
                return 
        except Exception as e:
            logging.error(f"정규장 덫 전송 에러 ({attempt}/{MAX_RETRIES}): {e}", exc_info=True)
            if attempt == 1:
                await context.bot.send_message(
                    chat_id=context.job.chat_id, 
                    text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 재시도합니다! 🛡️\n<code>사유: {type(e).__name__}: {e}</code>", 
                    parse_mode='HTML'
                )
        else:
            logging.warning(f"정규장 덫 조건 미충족 ({attempt}/{MAX_RETRIES}): {fail_reason}")
            if attempt == 1:
                 await context.bot.send_message(
                    chat_id=context.job.chat_id, 
                    text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 재시도합니다! 🛡️\n<code>사유: {fail_reason}</code>", 
                    parse_mode='HTML'
                 )

        if attempt < MAX_RETRIES:
            if attempt != 1 and attempt % 5 == 0:
                await context.bot.send_message(chat_id=context.job.chat_id, text=f"⚠️ <b>[API 통신 지연 감지]</b>\n한투 서버 불안정. 1분 뒤 재시도합니다! 🛡️", parse_mode='HTML')
            await asyncio.sleep(RETRY_DELAY)

    await context.bot.send_message(chat_id=context.job.chat_id, text="🚨 <b>[긴급 에러] 통신 복구 최종 실패. 수동 점검 요망!</b>", parse_mode='HTML')
