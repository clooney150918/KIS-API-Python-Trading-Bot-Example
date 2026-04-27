# ==========================================================
# [scheduler_aftermarket.py] - 🌟 100% 통합 무결점 완성본 🌟
# ⚠️ 16:05 EST 애프터마켓 잔여 물량 3% 로터리 덫 전송 전담 스케줄러
# 🚨 MODIFIED: [V30.09 핫픽스] pytz 소각 및 ZoneInfo('America/New_York') 이식을 통한 타임존 오차 차단.
# 🚨 MODIFIED: [V40.03 핫픽스] 애프터마켓 덫 장전 실패(ord_psbl_qty 부족) 맹점 완벽 수술.
# 장 마감 직후 묶여있는 미체결 매도 주문을 전면 강제 취소(Nuke)하여 주식을 해방시킨 뒤, 
# 총 보유수량(qty)이 아닌 '순수 주문 가능 수량(ord_psbl_qty)'만을 팩트로 스캔하여 덫을 장전하도록 아키텍처 개조.
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
    
    import pandas_market_calendars as mcal
    try:
        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
        is_trading_day = not schedule.empty
    except Exception as e:
        logging.error(f"⚠️ [애프터마켓] 달력 라이브러리 에러. 평일 강제 개장 처리: {e}")
        is_trading_day = now_est.weekday() < 5

    if not is_trading_day:
        logging.info("🌙 [애프터마켓] 금일 휴장일로 로터리 덫 스케줄러를 패스합니다.")
        return

    active_tickers = cfg.get_active_tickers()

    for t in active_tickers:
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
        actual_avg = float(h_data.get('avg') or 0.0)

        # 0주 새출발 당일인지 판독 (당일 0주 매수분은 애프터마켓 덫에서 엑시트 허용)
        # 스냅샷 디커플링 유지
        is_zero_start_fact = False
        try:
            from strategy_reversion import ReversionStrategy
            from strategy_v14_vwap import V14VwapStrategy
            
            ver = cfg.get_version(t)
            is_manual_vwap = getattr(cfg, 'get_manual_vwap_mode', lambda x: False)(t)
            
            cached_snap = None
            if ver == "V_REV":
                rev_plugin = ReversionStrategy()
                cached_snap = rev_plugin.load_daily_snapshot(t)
            elif ver == "V14" and is_manual_vwap:
                v14_vwap_plugin = V14VwapStrategy(cfg)
                cached_snap = v14_vwap_plugin.load_daily_snapshot(t)
                
            if cached_snap:
                is_zero_start_fact = cached_snap.get("is_zero_start", False)
        except Exception as e:
            logging.error(f"🚨 [{t}] 애프터마켓 스냅샷 로드 에러: {e}")

        if ord_psbl_qty > 0 and actual_avg > 0:
            target_price = math.ceil(actual_avg * 1.03 * 100) / 100.0
            
            try:
                # 🚨 AFTER_LIMIT (장후 지정가) 코드로 전송
                res = await asyncio.to_thread(broker.send_order, t, "SELL", ord_psbl_qty, target_price, "AFTER_LIMIT")
                
                if res.get('rt_cd') == '0':
                    msg = f"🌙 <b>[{t}] 애프터마켓 3% 로터리 덫(Lottery Trap) 장전 완료</b>\n"
                    msg += f"▫️ 대상 물량: <b>{ord_psbl_qty}주</b>\n"
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
                
        elif int(h_data.get('qty', 0)) > 0 and ord_psbl_qty == 0:
            # 주식은 있으나 락다운(주문가능수량 0) 상태인 엣지 케이스 보고
            logging.warning(f"⚠️ [{t}] 잔고는 있으나 주문가능수량(ord_psbl_qty)이 0주입니다. 미체결 취소 딜레이로 인해 덫 장전을 스킵합니다.")
