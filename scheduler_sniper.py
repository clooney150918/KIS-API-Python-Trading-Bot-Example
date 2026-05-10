# ==========================================================
# FILE: scheduler_sniper.py
# ==========================================================
# MODIFIED: [V53.06 전투 사령부 외부 통신 10초 타임아웃 및 폴백 방어막 이식]
# 🚨 MODIFIED: [V53.08 들여쓰기(Indentation) 붕괴 런타임 즉사 버그 완벽 수술]
# [scheduler_sniper.py] - 🌟 100% 분할 캡슐화 완성본 (V47.00) 🌟
# 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
# 제1헌법: queue_ledger.get_queue 등 모든 파일 I/O 및 락 점유 메서드는 무조건 asyncio.to_thread로 래핑하여 이벤트 루프 교착(Deadlock)을 원천 차단함.
# MODIFIED: [V44.47 이벤트 루프 데드락 영구 소각] 동기식 블로킹 호출 전면 비동기 래핑 완료.
# MODIFIED: [V44.48 런타임 즉사 방어] SHUTDOWN 분기문 들여쓰기(Indentation) 붕괴 교정 및 SyntaxError 영구 소각.
# NEW: [V44.51 파일 I/O 스레드 블로킹 철거] tracking_cache 날짜 변경 시 격발되는 파일 삭제 로직 비동기 래핑 및 백신 주석 하드코딩.
# MODIFIED: [V44.69 타임 드리프트 및 콜드스타트 엣지 케이스 방어막 이식]
# 🚨 MODIFIED: [V46.03 예산 침범 패러독스 방어] KIS 증거금 룰에 의해 AVWAP이 본대 예산을 침범하는 것을 막기 위해 1.05배 하드 마진 락온 이식
# 🚨 MODIFIED: [V46.04 AVWAP 증거금 침식 방어] 15:27 해제 조건 소각 및 마진 1.20배 상향 락온
# 🚨 MODIFIED: [V46.05 YF API 무한 호출 병목 소각 및 타임아웃 연장] Lock Starvation 방어
# 🚨 MODIFIED: [V46.06 기초자산 고/저가 스캔 배선 팩트 개통] 단판 승부 파라미터 누수 수술
# 🚨 MODIFIED: [V47.00 AVWAP 오버나이트 홀딩 락온] 일일 누적 매수/매도량 팩트 수혈 파이프라인 이식 (디커플링 대비)
# 🚨 MODIFIED: [V47.00 하이킨아시 듀얼 모멘텀] 본대 예산 보호막 무력화 0.0 및 암살자 예산 50% 강제 락온
# 🚨 MODIFIED: [V47.00 하이킨아시 듀얼 모멘텀] 옴니 매트릭스 락다운 블록 바이패스 처리(04:00 EST 개방)
# 🚨 NEW: [달력 API 결측 연쇄 기절 방어] 장운영시간 빈 값 반환 시 평일 09:30~16:00 EST 강제 폴백 락온 이식 완료.
# 🚨 MODIFIED: [V59.00 AVWAP 암살자 예산 100% 수혈 및 15:25 전량 덤핑 팩트 교정]
# 🚨 MODIFIED: [V59.02 잔재 데드코드 영구 소각] 매도 사유 내 잔재하는 낡은 익절(조기퇴근 등) 분기 100% 적출 및 15:25 덤핑 셧다운 단일화 락온
# 🚨 MODIFIED: [V59.05 잔재 데드코드 영구 소각] AVWAP 다중 출장(N회차) 및 조기 익절/손절 잔재 텍스트 100% 영구 소각 완료.
# 🚨 MODIFIED: [V60.00 옴니 매트릭스 락다운 데드코드 전면 폐기] 
# 스나이퍼 격발 전 매수 방아쇠를 잠그기 위해 잔존하던 옴니 매트릭스 필터 데드코드를 전면 소각하여 런타임 뇌관 해체.
# 🚨 MODIFIED: [V61.00 숏(SOXS) 전면 소각 작전 지시서 적용]
# 1) 암살자 출격 감시 루프 내 avwap_targets 배열에 SOXS를 강제 주입하여 이중 타격을 유발하던 디커플링 로직을 100% 영구 철거 완료.
# 2) 다중 티커 루프를 걷어내고 롱(SOXL) 단일 방향으로 진공 압축 및 들여쓰기 교정 완료.
# 🚨 MODIFIED: [V61.02 가상 에스크로 연산 데드코드 영구 소각]
# V59 절대 헌법(AVWAP 예산 100% 수혈)에 따라 무의미해진 V46 시절의 
# 파일 I/O 기반 virtual_locked_budget 연산 블록 30여 줄을 100% 영구 적출하여 런타임 병목 해체 완료.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import asyncio
import traceback
import math
import os
import json
import glob
import yfinance as yf
import pandas_market_calendars as mcal

from scheduler_core import is_market_open

async def scheduled_sniper_monitor(context):
    try:
        is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=10.0)
    except asyncio.TimeoutError:
        logging.error("⚠️ 달력 API 타임아웃. 스케줄 증발 방어를 위해 평일 강제 개장(Fail-Open) 처리합니다.")
        est = ZoneInfo('America/New_York')
        is_open = datetime.datetime.now(est).weekday() < 5

    if not is_open:
        return
    
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    
    if context.job.data.get('tx_lock') is None:
        logging.warning("⚠️ [sniper_monitor] tx_lock 미초기화. 이번 사이클 스킵.")
        return
    
    # 🚨 [비동기 래핑] 달력 API 교착 원천 소각
    def _get_market_hours():
        nyse = mcal.get_calendar('NYSE')
        return nyse.schedule(start_date=now_est.date(), end_date=now_est.date())

    try:
        schedule = await asyncio.wait_for(asyncio.to_thread(_get_market_hours), timeout=10.0)
        # 🚨 MODIFIED: [달력 API 결측 연쇄 기절 방어] schedule.empty == True 여도 평일이면 무조건 Fail-Open(강제 운영시간 세팅) 락온
        if schedule.empty:
            logging.warning("⚠️ [sniper_monitor] 달력 API 빈 값 반환. 평일 강제 운영시간(09:30~16:00 EST) 폴백 가동.")
            if now_est.weekday() < 5:
                market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
                market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
            else:
                return
        else:
            market_open = schedule.iloc[0]['market_open'].astimezone(est)
            market_close = schedule.iloc[0]['market_close'].astimezone(est)
    except asyncio.TimeoutError:
        logging.error("⚠️ 장운영시간 달력 API 타임아웃. 평일 강제 시간 세팅.")
        if now_est.weekday() < 5:
            market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        else: return
    except Exception:
        if now_est.weekday() < 5:
            market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        else: return
    
    pre_start = market_open - datetime.timedelta(hours=5, minutes=30)
    start_monitor = pre_start + datetime.timedelta(minutes=1)
    end_monitor = market_close - datetime.timedelta(minutes=1)
    
    if not (start_monitor <= now_est <= end_monitor):
        return

    is_regular_session = market_open <= now_est <= market_close
    
    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    
    # 🚨 MODIFIED: [V60.00] 더 이상 사용되지 않는 regime_data 로드 코드 영구 소각
    
    base_map = app_data.get('base_map', {'SOXL': 'SOXX', 'TQQQ': 'QQQ'})
    chat_id = context.job.chat_id
    
    tracking_cache = app_data.setdefault('sniper_tracking', {})
    
    today_est_str = now_est.strftime('%Y%m%d')
    if tracking_cache.get('date') != today_est_str:
        tracking_cache.clear()
        tracking_cache['date'] = today_est_str
        
        # MODIFIED: [V44.51 이벤트 루프 교착 방어] 파일 삭제 동기 블로킹 비동기 래핑
        # 🚨 [AI 에이전트 절대 주의 - 환각(Hallucination) 방어막]
        # glob 및 os.remove를 통한 파일 삭제 등 동기식 디스크 I/O 작업이 메인 이벤트 루프를 블로킹하여
        # 스나이퍼 및 전체 스케줄러가 교착(Deadlock) 상태에 빠지는 것을 원천 차단하기 위해
        # 내부 동기 함수로 캡슐화 후 비동기(asyncio.to_thread) 래핑을 강제합니다.
        def _clean_sniper_caches():
            try:
                for _f in glob.glob("data/sniper_cache_*.json"):
                    try:
                        os.remove(_f)
                    except OSError:
                        pass
            except Exception as e:
                logging.debug(f"스나이퍼 캐시 청소 중 에러: {e}")
                
        await asyncio.to_thread(_clean_sniper_caches)
              
    async def _do_sniper():
        async with tx_lock:
            # 🚨 [비동기 래핑] 파일/API I/O 데드락 방어 10초 타임아웃
            try:
                cash, holdings = await asyncio.wait_for(asyncio.to_thread(broker.get_account_balance), timeout=10.0)
            except asyncio.TimeoutError:
                logging.warning("⚠️ 잔고 조회 타임아웃 (10초). 폴백 적용.")
                cash, holdings = 0.0, None
            except Exception:
                cash, holdings = 0.0, None
                
            if holdings is None: return
            
            safe_holdings = holdings if isinstance(holdings, dict) else {}
             
            # 🚨 NEW: [V61.02 가상 에스크로 연산 데드코드 영구 소각] V46 시절의 가상 에스크로 차감 연산 블록 30여줄 전면 철거
            # 🚨 MODIFIED: [V59.00] 본대 예산 보호막 무력화 및 가용 현금 100% 수혈 락온 (AVWAP 95% 타격)
            virtual_locked_budget = 0.0
            avwap_free_cash = max(0.0, float(cash))
            
            for t in await asyncio.to_thread(cfg.get_active_tickers):
                version = await asyncio.to_thread(cfg.get_version, t)
                
                if version == "V_REV":
                    h = safe_holdings.get(t) or {}
                    actual_qty = int(float(h.get('qty', 0)))
                    q_ledger = app_data.get('queue_ledger')
                    if q_ledger:
                        # 🚨 [비동기 래핑] 파일 I/O 데드락 방어
                        q_data = await asyncio.to_thread(q_ledger.get_queue, t)
                        total_q = sum(item.get("qty", 0) for item in q_data)

                        if actual_qty == 0 and total_q > 0:
                            _vwap_cache_ref = app_data.get('vwap_cache', {})
                            if _vwap_cache_ref.get(f"REV_{t}_sweep_msg_sent"):
                                continue
                             
                            if not tracking_cache.get(f"REV_{t}_panic_sell_warn"):
                                tracking_cache[f"REV_{t}_panic_sell_warn"] = True
                                await context.bot.send_message(
                                    chat_id=chat_id,
                                    text=f"🚨 <b>[비상] [{t}] 수동매매로 인한 잔고 증발이 감지되었습니다.</b>\n"
                                         f"▫️ 봇의 매매가 일시 정지됩니다.\n"
                                         f"▫️ 시드 오염을 막기 위해 즉시 <code>/reset</code> 커맨드를 실행하여 장부를 소각하십시오.",
                                    parse_mode='HTML'
                                )
                            continue
                 
                if version == "V_REV" and await asyncio.to_thread(getattr(cfg, 'get_avwap_hybrid_mode', lambda x: False), t):
                    # 🚨 MODIFIED: [V61.00 숏(SOXS) 전면 소각] SOXS 강제 주입 및 다중 루프 전면 철거
                    # 오직 롱(t) 단일 방향으로만 스캔 및 타격 집행.
                    
                    if not tracking_cache.get(f"AVWAP_INIT_{t}"):
                        try:
                            saved_state = await asyncio.to_thread(strategy.v_avwap_plugin.load_state, t, now_est)
                            if saved_state:
                                tracking_cache[f"AVWAP_BOUGHT_{t}"] = saved_state.get('bought', False)
                                tracking_cache[f"AVWAP_SHUTDOWN_{t}"] = saved_state.get('shutdown', False)
                                tracking_cache[f"AVWAP_QTY_{t}"] = saved_state.get('qty', 0)
                                tracking_cache[f"AVWAP_AVG_{t}"] = saved_state.get('avg_price', 0.0)
                                tracking_cache[f"AVWAP_STRIKES_{t}"] = saved_state.get('strikes', 0)
                                # 🚨 MODIFIED: [V47.00 AVWAP 오버나이트 홀딩 락온] 일일 누적 매수/매도량 및 스캔 팩트 수혈
                                tracking_cache[f"AVWAP_DAILY_BOUGHT_{t}"] = saved_state.get('daily_bought_qty', 0)
                                tracking_cache[f"AVWAP_DAILY_SOLD_{t}"] = saved_state.get('daily_sold_qty', 0)
                                tracking_cache[f"AVWAP_FIRST_SCAN_DONE_{t}"] = saved_state.get('first_scan_done', False)
                                tracking_cache[f"AVWAP_FIRST_SCAN_PASSED_{t}"] = saved_state.get('first_scan_passed', False)
                        except Exception as e:
                            logging.error(f"AVWAP 상태 복구 실패: {e}")
                        tracking_cache[f"AVWAP_INIT_{t}"] = True
         
                    if tracking_cache.get(f"AVWAP_SHUTDOWN_{t}"): continue
            
                    target_base = base_map.get(t, t) 
                    
                    ctx_data = tracking_cache.get(f"AVWAP_CTX_{t}")
                    if not ctx_data:
                        try:
                            ctx_data = await asyncio.wait_for(asyncio.to_thread(strategy.v_avwap_plugin.fetch_macro_context, target_base), timeout=10.0)
                            if ctx_data:
                                tracking_cache[f"AVWAP_CTX_{t}"] = ctx_data
                        except Exception: pass
                     
                    if not ctx_data:
                        continue 
    
                    avwap_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
                    avwap_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)
                    
                    try:
                        exec_curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, t), timeout=10.0)
                        exec_curr_p = float(exec_curr_p_val or 0.0)
                    except asyncio.TimeoutError:
                        logging.warning(f"⚠️ [{t}] 현재가 스캔 타임아웃. 0.0 폴백.")
                        exec_curr_p = 0.0
                    except Exception:
                        exec_curr_p = 0.0
                        
                    if exec_curr_p <= 0: continue
                    
                    try:
                        base_curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, target_base), timeout=10.0)
                        base_curr_p = float(base_curr_p_val or 0.0)
                    except asyncio.TimeoutError:
                        base_curr_p = 0.0
                    except Exception:
                        base_curr_p = 0.0
                        
                    if base_curr_p <= 0: continue
                    
                    # 🚨 MODIFIED: [V46.05 YF API 무한 호출 병목 소각] 시가(Open) 당일 1회 스캔 및 메모리 락온
                    if not tracking_cache.get(f"AVWAP_DAY_OPEN_{target_base}"):
                        def _fetch_open(tkr):
                            try:
                                st = yf.Ticker(tkr)
                                h = st.history(period="1d", interval="1m", prepost=False, timeout=5)
                                if not h.empty: return float(h['Open'].dropna().iloc[0])
                            except: pass
                            return 0.0
                            
                        try:
                            fetched_open_val = await asyncio.wait_for(asyncio.to_thread(_fetch_open, target_base), timeout=10.0)
                            fetched_open = float(fetched_open_val or 0.0)
                        except asyncio.TimeoutError:
                            fetched_open = 0.0
                        except Exception:
                            fetched_open = 0.0
                            
                        if fetched_open > 0:
                            tracking_cache[f"AVWAP_DAY_OPEN_{target_base}"] = fetched_open
                            
                    base_day_open = tracking_cache.get(f"AVWAP_DAY_OPEN_{target_base}", 0.0)
                    
                    df_1min_base = None
                    try: 
                        df_1min_base = await asyncio.wait_for(asyncio.to_thread(broker.get_1min_candles_df, target_base), timeout=10.0)
                    except asyncio.TimeoutError:
                        pass
                    except Exception: 
                        pass
                    
                    # 🚨 MODIFIED: [V46.06 기초자산 고/저가 스캔 배선 팩트 개통]
                    prev_c, day_high, day_low, atr5, base_day_high, base_day_low = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
                    try:
                        prev_c_task = asyncio.to_thread(broker.get_previous_close, t)
                        high_low_task = asyncio.to_thread(broker.get_day_high_low, t)
                        atr_task = asyncio.to_thread(broker.get_atr_data, t)
                        base_hl_task = asyncio.to_thread(broker.get_day_high_low, target_base)
                        
                        res_prev, res_hl, res_atr, res_base_hl = await asyncio.wait_for(
                            asyncio.gather(prev_c_task, high_low_task, atr_task, base_hl_task, return_exceptions=True),
                            timeout=10.0
                        )
                        
                        prev_c = float(res_prev) if not isinstance(res_prev, Exception) and res_prev else 0.0
                        
                        # 🚨 [AI 에이전트 절대 주의 - 환각 방어막] 당일 고가(day_high) 팩트 스캔 및 주입
                        day_high = float(res_hl[0]) if not isinstance(res_hl, Exception) and res_hl else 0.0
                        day_low = float(res_hl[1]) if not isinstance(res_hl, Exception) and res_hl else 0.0
                        
                        atr5 = float(res_atr[0]) if not isinstance(res_atr, Exception) and res_atr else 0.0
                        
                        base_day_high = float(res_base_hl[0]) if not isinstance(res_base_hl, Exception) and res_base_hl else 0.0
                        base_day_low = float(res_base_hl[1]) if not isinstance(res_base_hl, Exception) and res_base_hl else 0.0
                    except asyncio.TimeoutError:
                        logging.warning("⚠️ AVWAP 파라미터 병렬 스캔 타임아웃. 0.0 폴백.")
                    except Exception as e:
                        logging.debug(f"AVWAP 파라미터 병렬 스캔 실패: {e}")
                        
                    # 🚨 MODIFIED: [V47.00 AVWAP 오버나이트 홀딩 락온] 스캔/체결 팩트 동기화
                    avwap_state_dict = {
                        "strikes": tracking_cache.get(f"AVWAP_STRIKES_{t}", 0),
                        "shutdown": tracking_cache.get(f"AVWAP_SHUTDOWN_{t}", False),
                        "qty": tracking_cache.get(f"AVWAP_QTY_{t}", 0),
                        "avg_price": tracking_cache.get(f"AVWAP_AVG_{t}", 0.0),
                        "bought": tracking_cache.get(f"AVWAP_BOUGHT_{t}", False),
                        "daily_bought_qty": tracking_cache.get(f"AVWAP_DAILY_BOUGHT_{t}", 0),
                        "daily_sold_qty": tracking_cache.get(f"AVWAP_DAILY_SOLD_{t}", 0),
                        "first_scan_done": tracking_cache.get(f"AVWAP_FIRST_SCAN_DONE_{t}", False),
                        "first_scan_passed": tracking_cache.get(f"AVWAP_FIRST_SCAN_PASSED_{t}", False)
                    }
                    
                    # 🚨 [비동기 래핑] 파일 I/O 의존성 방어 및 [AI 에이전트 절대 주의 - 환각 방어막] day_high 주입
                    decision = await asyncio.to_thread(
                        strategy.get_avwap_decision,
                        base_ticker=target_base, exec_ticker=t, base_curr_p=base_curr_p,
                        exec_curr_p=exec_curr_p, base_day_open=base_day_open, avg_price=avwap_avg,
                        qty=avwap_qty, alloc_cash=avwap_free_cash, context_data=ctx_data,
                        df_1min_base=df_1min_base, now_est=now_est, avwap_state=avwap_state_dict,
                        regime_data=None, prev_close=prev_c, day_high=day_high, day_low=day_low, atr5=atr5,
                        base_day_high=base_day_high, base_day_low=base_day_low # 🚨 MODIFIED: V46.06 추가
                    )
                    
                    action = decision.get("action")
                    reason = decision.get("reason", "")
                    
                    # 🚨 MODIFIED: [V47.00 AVWAP 오버나이트 홀딩 락온] 돌연변이 상태(first_scan_done 등) 동기화
                    if 'first_scan_done' in avwap_state_dict:
                        tracking_cache[f"AVWAP_FIRST_SCAN_DONE_{t}"] = avwap_state_dict['first_scan_done']
                    if 'first_scan_passed' in avwap_state_dict:
                        tracking_cache[f"AVWAP_FIRST_SCAN_PASSED_{t}"] = avwap_state_dict['first_scan_passed']
                    if 'shutdown' in avwap_state_dict:
                        tracking_cache[f"AVWAP_SHUTDOWN_{t}"] = avwap_state_dict['shutdown']
                        
                    if action == "BUY":
                        price = float(decision.get("target_price", decision.get("price", 0.0)))
                        qty = int(decision.get("qty", 0))
                            
                        if qty > 0 and price > 0:
                            has_unfilled = False
                            for _ in range(4):
                                unfilled = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                if isinstance(unfilled, list) and any(
                                    o.get('sll_buy_dvsn_cd') == '02' and str(o.get('ord_dvsn_cd') or o.get('ord_dvsn') or '').strip().zfill(2) == '00' 
                                    for o in unfilled
                                ):
                                    has_unfilled = True
                                    break
                                await asyncio.sleep(2.0)
                
                            if has_unfilled:
                                await asyncio.to_thread(broker.cancel_targeted_orders, t, "02", "00")
                                await asyncio.sleep(1.0)
                                continue
                        
                            res = await asyncio.to_thread(broker.send_order, t, "BUY", qty, price, "LIMIT")
                            odno = res.get('odno', '') if isinstance(res, dict) else ''
                        
                            if res and res.get('rt_cd') == '0' and odno:
                                ccld_qty = 0
                                for _ in range(4):
                                    await asyncio.sleep(2.0)
                                    unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                    safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []
                                    
                                    my_order = next((ox for ox in safe_unfilled if ox.get('odno') == odno), None)
                                    if my_order:
                                        ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                    else:
                                        ccld_qty = qty
                                        break
                                        
                                if ccld_qty < qty:
                                    try:
                                        await asyncio.to_thread(broker.cancel_order, t, odno)
                                        await asyncio.sleep(0.5)
                                    except Exception as e_cancel:
                                        logging.warning(f"⚠️ [{t}] AVWAP 매수 잔여 취소 실패: {e_cancel}")
                
                                if ccld_qty > 0:
                                    avwap_free_cash -= (ccld_qty * price)
                                    
                                    # MODIFIED: [V59.05 잔재 데드코드 영구 소각] 다중 출장 텍스트 적출
                                    msg = f"⚔️ <b>[AVWAP] 단타 암살자 딥매수 타격 성공!</b>\n▫️ 타겟: {t}\n▫️ 타점: ${price}\n▫️ 팩트 체결수량: {ccld_qty}주 (목표 {qty}주)\n▫️ 사유: {reason}"
                                    if ccld_qty < qty:
                                        msg += f"\n▫️ 미체결 {qty - ccld_qty}주는 안전을 위해 즉각 취소(Nuke)되었습니다."
                                    await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                    
                                    old_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
                                    old_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)
                                    new_qty = old_qty + ccld_qty
                                    new_avg = ((old_qty * old_avg) + (ccld_qty * price)) / new_qty if new_qty > 0 else 0.0

                                    # 🚨 MODIFIED: [V47.00 AVWAP 오버나이트 홀딩 락온] 당일 체결량 합산 및 보존
                                    daily_b = tracking_cache.get(f"AVWAP_DAILY_BOUGHT_{t}", 0) + ccld_qty
                                    tracking_cache[f"AVWAP_DAILY_BOUGHT_{t}"] = daily_b
                                    
                                    tracking_cache[f"AVWAP_BOUGHT_{t}"] = True
                                    tracking_cache[f"AVWAP_SHUTDOWN_{t}"] = False
                                    tracking_cache[f"AVWAP_QTY_{t}"] = new_qty
                                    tracking_cache[f"AVWAP_AVG_{t}"] = round(new_avg, 4)
                                    
                                    state_data = {
                                        "bought": True,
                                        "shutdown": False,
                                        "qty": new_qty,
                                        "avg_price": round(new_avg, 4),
                                        "strikes": tracking_cache.get(f"AVWAP_STRIKES_{t}", 0),
                                        "daily_bought_qty": daily_b,
                                        "daily_sold_qty": tracking_cache.get(f"AVWAP_DAILY_SOLD_{t}", 0),
                                        "first_scan_done": tracking_cache.get(f"AVWAP_FIRST_SCAN_DONE_{t}", False),
                                        "first_scan_passed": tracking_cache.get(f"AVWAP_FIRST_SCAN_PASSED_{t}", False)
                                    }
                                    await asyncio.to_thread(strategy.v_avwap_plugin.save_state, t, now_est, state_data)
                    
                    elif action == "SELL":
                        price = float(decision.get("target_price", decision.get("price", 0.0)))
                        qty = int(decision.get("qty", 0))
                        
                        if qty > 0:
                            exec_price = price
                            if exec_price <= 0.0:
                                try:
                                    bid_price_val = await asyncio.wait_for(asyncio.to_thread(broker.get_bid_price, t), timeout=10.0)
                                    bid_price = float(bid_price_val or 0.0)
                                except asyncio.TimeoutError:
                                    bid_price = 0.0
                                except Exception:
                                    bid_price = 0.0
                                exec_price = bid_price if bid_price > 0 else exec_curr_p
                                
                            has_unfilled = False
                            for _ in range(4):
                                unfilled = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                if isinstance(unfilled, list) and any(
                                    o.get('sll_buy_dvsn_cd') == '01' and str(o.get('ord_dvsn_cd') or o.get('ord_dvsn') or '').strip().zfill(2) == '00' 
                                    for o in unfilled
                                ):
                                    has_unfilled = True
                                    break
                                await asyncio.sleep(2.0)
                            
                            if has_unfilled:
                                await asyncio.to_thread(broker.cancel_targeted_orders, t, "01", "00")
                                await asyncio.sleep(1.0)
                                continue

                            res = await asyncio.to_thread(broker.send_order, t, "SELL", qty, exec_price, "LIMIT")
                            odno = res.get('odno', '') if isinstance(res, dict) else ''
                            
                            if res and res.get('rt_cd') == '0' and odno:
                                ccld_qty = 0
                                for _ in range(4):
                                    await asyncio.sleep(2.0)
                                    unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                    safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []
                                    
                                    my_order = next((ox for ox in safe_unfilled if ox.get('odno') == odno), None)
                                    if my_order:
                                        ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                    else:
                                        ccld_qty = qty
                                        break
                                        
                                if ccld_qty < qty:
                                    try:
                                        await asyncio.to_thread(broker.cancel_order, t, odno)
                                        await asyncio.sleep(0.5)
                                    except Exception as e_cancel:
                                        logging.warning(f"⚠️ [{t}] AVWAP 매도 잔여 취소 실패: {e_cancel}")
                                
                                if ccld_qty > 0:
                                    msg = f"⚔️ <b>[AVWAP] 암살자 덤핑 타격!</b>\n▫️ 타겟: {t}\n▫️ 타점: ${exec_price}\n▫️ 팩트 체결수량: {ccld_qty}주 (목표 {qty}주)\n▫️ 사유: {reason}"
                                    
                                    old_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
                                    new_qty = max(0, old_qty - ccld_qty)
                                    
                                    shutdown_flag = tracking_cache.get(f"AVWAP_SHUTDOWN_{t}", False)
                                    
                                    if new_qty == 0:
                                        strikes = tracking_cache.get(f"AVWAP_STRIKES_{t}", 0) + 1
                                        tracking_cache[f"AVWAP_STRIKES_{t}"] = strikes
                                        
                                        # 🚨 MODIFIED: [V59.05 잔재 데드코드 영구 소각] 다중 출장 텍스트 적출 및 15:25 단판 승부 락온
                                        msg += "\n🛡️ 금일 해당 종목의 15:25 타임스탑 청산 완료. 암살자 작전을 <b>영구 동결(Shutdown)</b>합니다."
                                        shutdown_flag = True
                                        
                                        new_avg = 0.0
                                        avwap_free_cash += (ccld_qty * exec_price)
                                    else:
                                        msg += f"\n⚠️ 잔량 {new_qty}주 발생 (미체결 강제 취소됨, 다음 1분봉 루프에서 재시도)"
                                        shutdown_flag = True
                                        new_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)

                                    await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                    
                                    # 🚨 MODIFIED: [V47.00 AVWAP 오버나이트 홀딩 락온] 당일 체결량 합산 및 보존
                                    daily_s = tracking_cache.get(f"AVWAP_DAILY_SOLD_{t}", 0) + ccld_qty
                                    tracking_cache[f"AVWAP_DAILY_SOLD_{t}"] = daily_s

                                    tracking_cache[f"AVWAP_BOUGHT_{t}"] = (new_qty > 0)
                                    tracking_cache[f"AVWAP_SHUTDOWN_{t}"] = shutdown_flag
                                    tracking_cache[f"AVWAP_QTY_{t}"] = new_qty
                                    tracking_cache[f"AVWAP_AVG_{t}"] = new_avg
                                    
                                    state_data = {
                                        'bought': tracking_cache[f"AVWAP_BOUGHT_{t}"],
                                        'shutdown': shutdown_flag,
                                        'strikes': tracking_cache.get(f"AVWAP_STRIKES_{t}", 0),
                                        'qty': new_qty,
                                        'avg_price': new_avg,
                                        "daily_bought_qty": tracking_cache.get(f"AVWAP_DAILY_BOUGHT_{t}", 0),
                                        "daily_sold_qty": daily_s,
                                        "first_scan_done": tracking_cache.get(f"AVWAP_FIRST_SCAN_DONE_{t}", False),
                                        "first_scan_passed": tracking_cache.get(f"AVWAP_FIRST_SCAN_PASSED_{t}", False)
                                    }
                                    await asyncio.to_thread(strategy.v_avwap_plugin.save_state, t, now_est, state_data)

                    elif action == "SHUTDOWN":
                        if not tracking_cache.get(f"AVWAP_SHUTDOWN_{t}"):
                            tracking_cache[f"AVWAP_SHUTDOWN_{t}"] = True
                            state_data = {
                                "bought": tracking_cache.get(f"AVWAP_BOUGHT_{t}", False),
                                "shutdown": True,
                                "qty": tracking_cache.get(f"AVWAP_QTY_{t}", 0),
                                "avg_price": tracking_cache.get(f"AVWAP_AVG_{t}", 0.0),
                                "strikes": tracking_cache.get(f"AVWAP_STRIKES_{t}", 0),
                                # 🚨 MODIFIED: [V47.00 AVWAP 오버나이트 홀딩 락온]
                                "daily_bought_qty": tracking_cache.get(f"AVWAP_DAILY_BOUGHT_{t}", 0),
                                "daily_sold_qty": tracking_cache.get(f"AVWAP_DAILY_SOLD_{t}", 0),
                                "first_scan_done": tracking_cache.get(f"AVWAP_FIRST_SCAN_DONE_{t}", False),
                                "first_scan_passed": tracking_cache.get(f"AVWAP_FIRST_SCAN_PASSED_{t}", False)
                            }
                            await asyncio.to_thread(strategy.v_avwap_plugin.save_state, t, now_est, state_data)
                            msg = f"🛡️ <b>[AVWAP] 암살자 작전 영구 셧다운(동결)</b>\n▫️ 타겟: {t}\n▫️ 사유: {reason}"
                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

                master_switch = await asyncio.to_thread(getattr(cfg, 'get_master_switch', lambda x: "ALL"), t)
                sniper_buy_locked = await asyncio.to_thread(getattr(cfg, 'get_sniper_buy_locked', lambda x: False), t)
                sniper_sell_locked = await asyncio.to_thread(getattr(cfg, 'get_sniper_sell_locked', lambda x: False), t)

                try:
                    curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, t), timeout=10.0)
                    curr_p = float(curr_p_val or 0.0)
                except asyncio.TimeoutError:
                    logging.warning(f"⚠️ [{t}] 현재가 스캔 타임아웃. 0.0 폴백.")
                    curr_p = 0.0
                except Exception:
                    curr_p = 0.0
                    
                if curr_p <= 0:
                    continue

                sniper_func = getattr(strategy, 'check_sniper_condition', None)
                if sniper_func:
                    res = await asyncio.to_thread(sniper_func, t, cfg, broker, chat_id)
                else:
                    res = {"action": "HOLD", "reason": "스나이퍼 모듈 누락(Bypass)", "limit_price": 0.0}
                    
                action = res.get("action")
                reason = res.get("reason", "")
                limit_p = res.get("limit_price", 0.0)

                is_rev = (await asyncio.to_thread(cfg.get_version, t) == "V_REV")

                # MODIFIED: [V60.00] 옴니 매트릭스 락다운 데드코드 전면 폐기 완료.

                if action == "BUY" and not is_rev and not sniper_buy_locked and master_switch != "UP_ONLY":
                    qty = res.get("qty", 0)
                    if qty > 0:
                        cancelled = await asyncio.to_thread(broker.cancel_targeted_orders, t, "02", "03")
                        await asyncio.sleep(1.0)
                        
                        has_unfilled = False
                        for _ in range(4):
                            unfilled = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                            if isinstance(unfilled, list) and any(
                                o.get('sll_buy_dvsn_cd') == '02' and str(o.get('ord_dvsn_cd') or o.get('ord_dvsn') or '').strip().zfill(2) == '00' 
                                for o in unfilled
                            ):
                                has_unfilled = True
                                break
                            await asyncio.sleep(2.0)
                        
                        if has_unfilled:
                            continue
                             
                        order_res = await asyncio.to_thread(broker.send_order, t, "BUY", qty, limit_p, "LIMIT")
                        odno = order_res.get('odno', '') if isinstance(order_res, dict) else ''
                        
                        if order_res and order_res.get('rt_cd') == '0' and odno:
                            ccld_qty = 0
                            for _ in range(4):
                                await asyncio.sleep(2.0)
                                unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []

                                my_order = next((ox for ox in safe_unfilled if ox.get('odno') == odno), None)
                                if my_order:
                                    ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                else:
                                    ccld_qty = qty
                                    break

                            if ccld_qty < qty:
                                try:
                                    await asyncio.to_thread(broker.cancel_order, t, odno)
                                    await asyncio.sleep(1.0)
                                except: pass

                            if ccld_qty > 0:
                                if hasattr(cfg, 'set_sniper_buy_locked'):
                                    await asyncio.to_thread(cfg.set_sniper_buy_locked, t, True)
                                    
                                exec_history = await asyncio.to_thread(broker.get_execution_history, t, today_est_str, today_est_str)
                                 
                                def get_actual_execution_price(history, side_code, target_odno):
                                    if not history: return 0.0
                                    for ex in history:
                                        if ex.get('sll_buy_dvsn_cd') == side_code and ex.get('odno') == target_odno:
                                            p = float(ex.get('ft_ccld_unpr3', '0'))
                                            if p > 0: return p
                                            
                                    target_recs = [ex for ex in history if ex.get('sll_buy_dvsn_cd') == side_code]
                                    for ex in target_recs:
                                        p = float(ex.get('ft_ccld_unpr3', '0'))
                                        if p > 0: return p
                                    return 0.0
                                   
                                actual_exec_price = get_actual_execution_price(exec_history, "02", odno)
                                display_price = actual_exec_price if actual_exec_price > 0 else limit_p
                                   
                                msg = f"🚨 <b>[{t}] 스나이퍼 딥-매수(Intercept) 명중!</b>\n▫️ 타겟가: ${limit_p}\n▫️ 팩트 단가: ${display_price}\n▫️ 체결수량: {ccld_qty}주 (요청: {qty}주)\n▫️ 사유: {reason}\n▫️ 하방 방어망이 잠깁니다 (상방 독립 유지)."
                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
              
                is_zero_start_session = False
                try:
                    snap = None
                    if is_rev and hasattr(strategy, 'v_rev_plugin'):
                        snap = await asyncio.to_thread(strategy.v_rev_plugin.load_daily_snapshot, t)
                    elif version == "V14":
                        is_manual_vwap = await asyncio.to_thread(getattr(cfg, 'get_manual_vwap_mode', lambda x: False), t)
                        if is_manual_vwap and hasattr(strategy, 'v14_vwap_plugin'):
                            snap = await asyncio.to_thread(strategy.v14_vwap_plugin.load_daily_snapshot, t)
                        elif hasattr(strategy, 'v14_plugin') and hasattr(strategy.v14_plugin, 'load_daily_snapshot'):
                            snap = await asyncio.to_thread(strategy.v14_plugin.load_daily_snapshot, t)
                    if snap:
                        is_zero_start_session = snap.get("is_zero_start", snap.get("total_q", snap.get("initial_qty", -1)) == 0)
                except Exception:
                    pass

                upward_mode = await asyncio.to_thread(getattr(cfg, 'get_upward_sniper_mode', lambda x: False), t)
                is_upward_active = upward_mode and not is_rev and not sniper_sell_locked and master_switch != "DOWN_ONLY"
                    
                if is_zero_start_session:
                    is_upward_active = False

                if is_upward_active and action in ["SELL_QUARTER", "SELL_JACKPOT"]:
                    qty = res.get("qty", 0)
                    if qty > 0:
                        cancelled = await asyncio.to_thread(broker.cancel_targeted_orders, t, "01", "03")
                        await asyncio.sleep(1.0)
                        
                        has_unfilled = False
                        for _ in range(4):
                            unfilled = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                            if isinstance(unfilled, list) and any(
                                o.get('sll_buy_dvsn_cd') == '01' and str(o.get('ord_dvsn_cd') or o.get('ord_dvsn') or '').strip().zfill(2) == '00' 
                                for o in unfilled
                            ):
                                has_unfilled = True
                                break
                            await asyncio.sleep(2.0)
                        
                        if has_unfilled:
                            continue
                             
                        order_res = await asyncio.to_thread(broker.send_order, t, "SELL", qty, limit_p, "LIMIT")
                        odno = order_res.get('odno', '') if isinstance(order_res, dict) else ''
                        
                        if order_res and order_res.get('rt_cd') == '0' and odno:
                            ccld_qty = 0
                            for _ in range(4):
                                await asyncio.sleep(2.0)
                                unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []
                                
                                my_order = next((ox for ox in safe_unfilled if ox.get('odno') == odno), None)
                                if my_order:
                                    ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                else:
                                    ccld_qty = qty
                                    break
                    
                            if ccld_qty < qty:
                                try:
                                    await asyncio.to_thread(broker.cancel_order, t, odno)
                                    await asyncio.sleep(1.0)
                                except: pass

                            if ccld_qty > 0:
                                if hasattr(cfg, 'set_sniper_sell_locked'):
                                    await asyncio.to_thread(cfg.set_sniper_sell_locked, t, True)
                                    
                                exec_history = await asyncio.to_thread(broker.get_execution_history, t, today_est_str, today_est_str)
                                
                                def get_actual_execution_price(history, side_code, target_odno):
                                    if not history: return 0.0
                                    for ex in history:
                                        if ex.get('sll_buy_dvsn_cd') == side_code and ex.get('odno') == target_odno:
                                            p = float(ex.get('ft_ccld_unpr3', '0'))
                                            if p > 0: return p
                                            
                                    target_recs = [ex for ex in history if ex.get('sll_buy_dvsn_cd') == side_code]
                                    for ex in target_recs:
                                        p = float(ex.get('ft_ccld_unpr3', '0'))
                                        if p > 0: return p
                                    return 0.0
                              
                                actual_exec_price = get_actual_execution_price(exec_history, "01", odno)
                                display_price = actual_exec_price if actual_exec_price > 0 else limit_p
                                            
                                msg = f"🦇 <b>[{t}] 스나이퍼 상방 기습({action}) 명중!</b>\n▫️ 타겟가: ${limit_p}\n▫️ 팩트 단가: ${display_price}\n▫️ 체결수량: {ccld_qty}주 (요청: {qty}주)\n▫️ 사유: {reason}\n▫️ 상방 감시망이 잠깁니다 (하방 독립 유지)."
                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

    try:
        # 🚨 MODIFIED: [V46.05 이벤트 루프 교착 방어] Lock Starvation 대비 호흡 연장
        await asyncio.wait_for(_do_sniper(), timeout=90.0)
    except Exception as e:
        logging.error(f"🚨 스나이퍼 타임아웃 에러: {e}", exc_info=True)
