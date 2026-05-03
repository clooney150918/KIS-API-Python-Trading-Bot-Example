# ==========================================================
# FILE: scheduler_aftermarket.py
# ==========================================================
# MODIFIED: [V44.08 평단가 팩트 디커플링] AVWAP 암살자 매수로 인해 한투 실잔고 평단가가 희석되는 맹점을 원천 차단.
# V-REV 모드일 경우 KIS 실잔고 평단가를 전면 무시하고, 오직 V-REV 큐 장부의 진성 평단가를 역산하여 3% 로터리 덫 타점에 반영하도록 락온 완료.
# NEW: [V44.09 AVWAP 물량 물귀신 덤핑 원천 차단 및 디커플링 팩트 수술] AVWAP 암살자가 장중 딥매수한 물량이 장 마감 직후 애프터마켓 덫에 묶여 동반 투매(물귀신)되는 치명적 맹점을 완벽 수술. V-REV 큐에 해당하는 수량만을 수학적으로 핀셋 차감하여 로터리 덫으로 전송하도록 물량 디커플링 락온 완료.
# MODIFIED: [V44.44 이벤트 루프 교착 방어] 달력 API(pandas_market_calendars) 동기 블로킹 비동기 래핑 및 타임아웃 Fail-Open 족쇄 체결 완료.
# 🚨 MODIFIED: [V44.47 이벤트 루프 데드락 영구 소각] JSON 및 장부 데이터를 스캔하는 모든 동기 호출을 예외 없이 비동기 래핑 완료.
# 🚨 MODIFIED: [V44.51 V-REV 락온 및 LIFO 큐 팩트 수술] 애프터마켓 로터리 덫이 V14 종목(TQQQ 등)을 무지성 타격하던 치명적 맹점(Fall-through) 원천 차단. 오직 V-REV 모드에서만, 그리고 LIFO 큐 장부의 팩트 데이터(수량/평단가) 기반으로만 덫이 장전되도록 절대 헌법 적용 완료.
# 🚨 MODIFIED: [V44.52 애프터마켓 모드 검증 락온 최상단 전진 배치] V14 종목(TQQQ 등)의 정규장 매도 방어선 무지성 철거 방어 완결.
# ==========================================================
import logging
import asyncio
import math
import datetime
from zoneinfo import ZoneInfo

async def scheduled_after_market_lottery(context):
    """
    16:05 EST 기상. 정규장 종료 후 체결되지 않고 남은 물량이 있다면,
    전체 평단가 대비 +3%의 수익권에 장후 지정가(AFTER_LIMIT) 덫을 놓습니다.
    """
    app_data = context.job.data
    cfg = app_data['cfg']
    broker = app_data['broker']
    tx_lock = app_data['tx_lock']
    chat_id = context.job.chat_id

    # 1. 미국 동부 시간 기준 오늘 장이 열렸는지 팩트 체크
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    
    # 🚨 MODIFIED: [V44.44 이벤트 루프 교착 방어] 동기 연산을 비동기 스레드로 격리
    def _check_market_schedule():
        import pandas_market_calendars as mcal
        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
        return not schedule.empty

    try:
        # 달력 라이브러리 연산을 별도 스레드로 밀어내어 이벤트 루프 보호 및 10초 타임아웃 설정
        is_trading_day = await asyncio.wait_for(asyncio.to_thread(_check_market_schedule), timeout=10.0)
    except asyncio.TimeoutError:
        logging.error("⚠️ [애프터마켓] 달력 라이브러리 타임아웃. 평일 강제 개장 처리.")
        is_trading_day = now_est.weekday() < 5
    except Exception as e:
        logging.error(f"⚠️ [애프터마켓] 달력 라이브러리 에러. 평일 강제 개장 처리: {e}")
        is_trading_day = now_est.weekday() < 5

    if not is_trading_day:
        logging.info("🌙 [애프터마켓] 금일 휴장일로 로터리 덫 스케줄러를 패스합니다.")
        return

    # 🚨 [비동기 래핑] 파일 I/O 동기 블로킹 방어
    active_tickers = await asyncio.to_thread(cfg.get_active_tickers)

    for t in active_tickers:
        # 🚨 MODIFIED: [V44.52 애프터마켓 모드 검증 락온 최상단 전진 배치]
        # V14 종목(TQQQ 등)의 정규장 방어선(SELL 주문)이 무지성 취소되는 치명적 맹점을 막기 위해 루프 최상단에서 우선 필터링.
        ver = await asyncio.to_thread(cfg.get_version, t)
        if ver != "V_REV":
            logging.info(f"🛡️ [{t}] {ver} 모드는 애프터마켓 로터리 덫 엑시트 대상이 아니므로 안전하게 바이패스합니다.")
            continue

        # 🚨 [V40.03 팩트 수술 1단계] 호가창에 묶인 미체결 매도 주문 강제 취소 (주식 해방)
        try:
            await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
            # KIS 서버가 취소를 승인하고 가용 수량을 갱신할 때까지 물리적 버퍼 타임 부여
            await asyncio.sleep(2.0) 
        except Exception as e:
            logging.error(f"🚨 [{t}] 애프터마켓 진입 전 미체결 취소 에러: {e}")

        async with tx_lock:
            _, holdings = await asyncio.to_thread(broker.get_account_balance)
            
        if holdings is None:
            logging.error(f"🚨 [{t}] 애프터마켓 잔고 조회 실패.")
            continue

        h_data = holdings.get(t, {})
        # 🚨 [V40.03 팩트 수술 2단계] 총 보유수량(qty) 대신 주문 가능 수량(ord_psbl_qty) 절대 락온
        ord_psbl_qty = int(h_data.get('ord_psbl_qty', 0))
        
        target_avg = 0.0
        target_qty = 0
        avwap_qty = 0
        vrev_qty = 0
        
        # 1) 평단가 및 수량 팩트 100% 디커플링 (오직 V-REV LIFO 큐 기준)
        queue_ledger = app_data.get('queue_ledger')
        if queue_ledger:
            # 🚨 [비동기 래핑] 파일 I/O 동기 블로킹 방어
            q_data = await asyncio.to_thread(queue_ledger.get_queue, t)
            vrev_qty = sum(int(float(item.get('qty', 0))) for item in q_data)
            
            if vrev_qty > 0:
                vrev_inv = sum(int(float(item.get('qty', 0))) * float(item.get('price', 0.0)) for item in q_data)
                target_avg = vrev_inv / vrev_qty  # KIS 평단가 무시, 오직 큐 장부 진성 평단가 락온
                target_qty = vrev_qty             # 수량 역시 큐 지층 수량으로 100% 락온
                logging.info(f"🛡️ [{t}] 애프터마켓 덫 타점 디커플링: V-REV 큐 진성 평단가(${target_avg:.4f}) 및 수량({target_qty}주)으로 팩트 오버라이드 완료.")
            else:
                logging.warning(f"⚠️ [{t}] V-REV 모드이나 큐(Queue)에 유효한 지층이 없어 덫 장전을 스킵합니다.")
                continue
        else:
            logging.error(f"🚨 [{t}] V-REV 큐 장부 객체를 찾을 수 없어 덫 장전을 안전하게 중단합니다.")
            continue
            
        # 2) 물량 팩트 디커플링 및 API Reject 방어
        try:
            jobs = context.job_queue.jobs() if context.job_queue else []
            job_data = jobs[0].data if jobs and jobs[0].data is not None else {}
            tracking_cache = job_data.get('sniper_tracking', {})
            avwap_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
            
            if avwap_qty == 0:
                strategy = app_data.get('strategy')
                if strategy and hasattr(strategy, 'v_avwap_plugin'):
                    # 🚨 [비동기 래핑] 파일 I/O 동기 블로킹 방어
                    avwap_state = await asyncio.to_thread(strategy.v_avwap_plugin.load_state, t, now_est)
                    avwap_qty = int(avwap_state.get('qty', 0))
        except Exception as e:
            logging.error(f"🚨 [{t}] AVWAP 상태 로드 에러 (물량 디커플링 우회): {e}")

        # 안전장치: KIS 증권사의 실제 가용 수량(ord_psbl_qty)을 초과해서 주문을 넣을 수 없도록 방어
        pure_sellable = max(0, ord_psbl_qty - avwap_qty)
        target_qty = min(target_qty, pure_sellable)
        
        if target_qty <= 0:
             logging.warning(f"⚠️ [{t}] 덫을 놓을 유효 수량이 0주이거나 AVWAP 물량과 충돌하여 스킵합니다.")
             continue

        # 0주 새출발 당일인지 판독 (당일 0주 매수분은 애프터마켓 덫에서 엑시트 허용)
        # 스냅샷 디커플링 유지
        is_zero_start_fact = False
        try:
            from strategy_reversion import ReversionStrategy
            
            cached_snap = None
            rev_plugin = ReversionStrategy(cfg)
            # 🚨 [비동기 래핑] 파일 I/O 동기 블로킹 방어
            cached_snap = await asyncio.to_thread(rev_plugin.load_daily_snapshot, t)
             
            if cached_snap:
                is_zero_start_fact = cached_snap.get("is_zero_start", False)
        except Exception as e:
            logging.error(f"🚨 [{t}] 애프터마켓 스냅샷 로드 에러: {e}")

        if target_qty > 0 and target_avg > 0:
            target_price = math.ceil(target_avg * 1.03 * 100) / 100.0
             
            try:
                # 🚨 AFTER_LIMIT (장후 지정가) 코드로 전송
                res = await asyncio.to_thread(broker.send_order, t, "SELL", target_qty, target_price, "AFTER_LIMIT")
                
                if res.get('rt_cd') == '0':
                    msg = f"🌙 <b>[{t}] 애프터마켓 3% 로터리 덫(Lottery Trap) 장전 완료</b>\n"
                    msg += f"▫️ 대상 물량: <b>{target_qty}주</b>\n"
                    if avwap_qty > 0:
                        msg += f"▫️ (AVWAP 독립 물량 {avwap_qty}주는 차감 보존됨)\n"
                    msg += f"▫️ 기준 평단: <b>${target_avg:.2f}</b>\n"
                    msg += f"▫️ 타겟 가격: <b>${target_price:.2f}</b>\n"
                    if is_zero_start_fact:
                        msg += "💡 (0주 새출발 당일 확보 물량 애프터마켓 엑시트 가동)"
                     
                    await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                else:
                    err_msg = res.get('msg1', '알 수 없는 에러')
                    fail_msg = f"❌ <b>[{t}] 애프터마켓 덫 장전 실패:</b> {err_msg}"
                    await context.bot.send_message(chat_id=chat_id, text=fail_msg, parse_mode='HTML')
            except Exception as e:
                logging.error(f"🚨 [{t}] 애프터마켓 주문 전송 중 에러: {e}")
                
        elif int(h_data.get('qty', 0)) > 0 and target_qty == 0:
            if ord_psbl_qty > 0 and avwap_qty > 0 and ord_psbl_qty <= avwap_qty:
                logging.info(f"🛡️ [{t}] 보유 물량 전량이 AVWAP 암살자 소유이므로 V-REV 애프터마켓 덫 장전을 바이패스합니다.")
            else:
                logging.warning(f"⚠️ [{t}] 잔고는 있으나 주문가능수량(ord_psbl_qty)이 0주입니다. 미체결 취소 딜레이로 인해 덫 장전을 스킵합니다.")
