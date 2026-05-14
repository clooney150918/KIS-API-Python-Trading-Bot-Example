# ==========================================================
# FILE: scheduler_vwap.py
# ==========================================================
# 🚨 MODIFIED: [V-REV 추세장 LOC 스위칭 침묵 버그 및 상태 증발 완벽 수술]
# - 60% 거래량 지배력 감지 후 LOC 전환 시 텔레그램 무음(disable_notification=True) 파라미터 영구 소각
# - 텔레그램 발송 직후 로깅망을 통해 팩트 박제 추가
# - LOC 주문 전송 시 KIS 서버 거절(Reject) 사유를 타전하도록 에러 로깅망 완벽 이식
# MODIFIED: [V53.06 전투 사령부 외부 통신 10초 타임아웃 및 폴백 방어막 이식]
# 🚨 MODIFIED: [V53.08 들여쓰기(Indentation) 붕괴 런타임 즉사 버그 완벽 수술]
# 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
# 제1헌법: queue_ledger.get_queue 등 모든 파일 I/O 및 락 점유 메서드는 무조건 asyncio.to_thread로 래핑하여 이벤트 루프 교착(Deadlock)을 원천 차단함.
# 제9헌법: U_CURVE_WEIGHTS 하드코딩 배열 영구 소각. vwap_data.py에서 동적 로드하여 팩트 기반 재정규화 필수.
# MODIFIED: [V44.47 이벤트 루프 데드락 영구 소각 및 동적 U-Curve 팩트 락온] 동기식 블로킹 호출 전면 비동기 래핑 및 하드코딩 배열 철거 완료.
# MODIFIED: [맹점 3 수술] 루프 내부 cfg 접근(파일 I/O) 메서 전면 비동기(asyncio.to_thread) 래핑 완료.
# NEW: [콜드 스타트 런타임 붕괴 방어] scheduled_vwap_init_and_cancel 진입부 tx_lock None 가드 이식 완료.
# NEW: [VWAP 잔차 증발 방어 롤백 엔진 이식] 타격 스킵(호가 이탈) 및 주문 거절/미체결 발생 시 삭감된 예산을 코어 엔진(버킷)으로 100% 환불(Refund)하는 파이프라인 개통 완료.
# MODIFIED: [V44.79 팩트 교정] 잔차 환불 인플레이션 맹점 및 미체결 늪 원천 차단
# 🚨 MODIFIED: [V46.04 KIS 리젝 텔레메트리 이식] 거절 사유 로깅 추가
# 🚨 MODIFIED: [V46.05 이벤트 루프 교착 방어] Lock Starvation 대비 호흡 연장
# 🚨 MODIFIED: [V47.02 런타임 붕괴 방어] target_sweep_qty UnboundLocalError 스코프 전진 배치로 영구 소각 완료
# 🚨 MODIFIED: [V50.02 30분 압축 락온] 타임 윈도우 스캔 범위를 range(27, 60)에서 range(27, 57)로 정밀 교정하여 15:56 타격 종료 완벽 동기화.
# 🚨 MODIFIED: [V52.00 V14 VWAP 예산 누수 영구 소각] get_dynamic_plan 호출 시 6번째 인자에 0.0이 하드코딩되어 당일 예산이 0원으로 강제 주입되던 치명적 맹점 원천 차단. v14_alloc_cash 스코프 전진 배치 및 팩트 예산 주입 파이프라인 100% 개통 완료.
# 🚨 MODIFIED: [V53.00 무한 재진입 락온] 0주 매수 금지(Daily Buy-Lock) 족쇄 전면 폐기. 전량 익절 후에도 당일 타점 도달 시 100% 재매수 강제 가동.
# 🚨 MODIFIED: [V44.48 런타임 붕괴 방어] 들여쓰기 붕괴(IndentationError) 완벽 교정 및 팩트 종속 완료.
# 🚨 MODIFIED: [V54.01 VWAP 데이터 통합 롤백] vwap_data.py 외부 파일 임포트 소각 및 ConfigManager 수혈 락온
# 🚨 MODIFIED: [V54.02 깡통 스냅샷 붕괴 방어] prev_c 다이렉트 추출 파이프라인 이식으로 데이터 기아(Data Starvation) 원천 차단
# 🚨 NEW: [달력 API 결측 연쇄 기절 방어] 장마감시간 빈 값 반환 시 평일 16:00 EST 강제 폴백 락온 이식 완료.
# 🚨 MODIFIED: [V60.00 옴니 매트릭스 락다운 데드코드 전면 폐기] 
# 스나이퍼 격발 전 매수 방아쇠를 잠그기 위해 잔존하던 옴니 매트릭스 필터 데드코드를 전면 소각하여 런타임 뇌관 해체.
# 🚨 MODIFIED: [V66.09 V-REV VWAP 런타임 엑스레이 이식]
# 🚨 NEW: [KIS VWAP 알고리즘 권한 위임 수술] 1분마다 매수/매도 주문을 쏘던 자체 타임 슬라이싱 타격망 100% 영구 소각. KIS 예약 덫 체결 관망 및 갭 하이재킹(Gap Hijack) 섀도우 오버라이드망으로 롤 완벽 격상.
# 🚨 NEW: [V71.10 섀도우 오버라이드 덫 파기 팩트 스캔] 갭 하이재킹 격발 시 로컬 캐시 조회 파기 로직 전면 소각 및 KIS 실원장(get_reservation_orders) 다이렉트 연동 아키텍처 이식.
# 🚨 MODIFIED: [V71.14 지정가 VWAP 일반주문 역배선 팩트 락온 및 갭 하이재킹 기절 버그 수술]
# 🚨 MODIFIED: [V72.08 갭 하이재킹 예산 연산 공식 디커플링 해체 및 SSOT 락온]
# - 정규장 V-REV 매수 예산 산출 공식(V72.01 하드 마진 캡)과 갭 하이재킹의 예산 연산 로직이 서로 다르게 작동하던 치명적 수학적 디커플링 원천 차단.
# - get_budget_allocation 파이프라인을 섀도우 오버라이드망에 다이렉트 결속시켜, 하이재킹 타격 시에도 "1일 할당량(15%) 초과 금지" 룰이 100% 팩트 연동되도록 아키텍처 대수술 완료.
# 🚨 NEW: [V72.18 갭 하이재킹 예약 원장 맵핑 누수 및 일반 미체결 이중 방화벽 락온]
# 🚨 MODIFIED: [V72.21 휴장일 맹독성 페일 오픈(Fail-Open) 팩트 교정]
# - 달력 API 정상 빈 데이터 반환 시 휴장일로 명확히 간주하고 안전 종료(Return)하도록 수술.
# 🚨 NEW: [V73.00 섀도우 오버라이드망 타임 윈도우 시프트 및 디커플링 락온]
# - 본진 덫 장전 시각(15:26 EST)에 맞춰 갭 하이재킹 모니터링 타임 윈도우를 장 마감 36분 전에서 34분 전으로 정밀 동기화.
# - 렌더링 텍스트를 '장 마감 34분 전'으로 팩트 교정하여 시각적 디커플링 해체 완료.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import asyncio
import traceback
import math
import os
import time
import json
import pandas_market_calendars as mcal
import tempfile

from scheduler_core import is_market_open, get_budget_allocation

async def scheduled_vwap_init_and_cancel(context):
    if context.job.data.get('tx_lock') is None:
        logging.warning("⚠️ [vwap_init_and_cancel] tx_lock 미초기화. 이번 사이클 스킵.")
        return

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
    
    def _get_market_close():
        nyse = mcal.get_calendar('NYSE')
        return nyse.schedule(start_date=now_est.date(), end_date=now_est.date())

    try:
        schedule = await asyncio.wait_for(asyncio.to_thread(_get_market_close), timeout=10.0)
        if schedule.empty:
            # 🚨 MODIFIED: [V72.21 휴장일 맹독성 페일 오픈 팩트 교정]
            logging.info("💤 [vwap_init] 달력 API 빈 데이터 반환. 금일은 미국 증시 휴장일입니다.")
            return
        else:
            market_close = schedule.iloc[0]['market_close'].astimezone(est)
    except asyncio.TimeoutError:
        logging.error("⚠️ 장마감시간 달력 API 타임아웃. 평일 강제 마감시간(16:00 EST) 세팅.")
        if now_est.weekday() < 5:
            market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        else:
            return
    except Exception as e:
        logging.error(f"⚠️ 장마감시간 달력 API 에러({e}). 평일 강제 마감시간(16:00 EST) 세팅.")
        if now_est.weekday() < 5:
            market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        else:
            return
        
    # 🚨 MODIFIED: [V73.00 섀도우 오버라이드망 기상 시간 디커플링 수술]
    # 본진 덫 장전(15:26 EST)에 맞춰 타임 윈도우를 장 마감 34분 전으로 정밀 동기화
    vwap_start_time = market_close - datetime.timedelta(minutes=34, seconds=0)
    vwap_end_time = market_close 
    
    if not (vwap_start_time <= now_est <= vwap_end_time):
        return
    
    app_data = context.job.data
    cfg, broker, tx_lock = app_data['cfg'], app_data['broker'], app_data['tx_lock']
    chat_id = context.job.chat_id
    
    vwap_cache = app_data.setdefault('vwap_cache', {})
    
    today_str = now_est.strftime('%Y%m%d')
    if vwap_cache.get('date') != today_str:
        vwap_cache.clear()
        vwap_cache['date'] = today_str
        
    async def _do_init():
        async with tx_lock:
            active_tickers = await asyncio.to_thread(cfg.get_active_tickers)
            for t in active_tickers:
                version = await asyncio.to_thread(cfg.get_version, t)
                is_manual_vwap = await asyncio.to_thread(getattr(cfg, 'get_manual_vwap_mode', lambda x: False), t)
                
                if version == "V_REV" or (version == "V14" and is_manual_vwap):
                    if not vwap_cache.get(f"REV_{t}_nuked"):
                        try:
                            msg = f"🌅 <b>[{t}] KIS VWAP/LOC 예약 덫 관측 및 섀도우 오버라이드망 기상</b>\n"
                            msg += f"▫️ 장 마감 34분 전 진입을 확인하여 KIS 서버의 예약 덫 체결을 관망합니다.\n"
                            msg += f"▫️ 기초자산 갭 이탈 감지 시 즉각 개입(Gap Hijack)하는 섀도우 모드로 전환합니다. ⚔️"
            
                            vwap_cache[f"REV_{t}_nuked"] = True
                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', disable_notification=True)
                            await asyncio.sleep(1.0)
                        except Exception as e:
                            logging.error(f"🚨 관측 모드 전환 알림 실패: {e}", exc_info=True)
                            vwap_cache[f"REV_{t}_nuked"] = False 
            
    try:
        await asyncio.wait_for(_do_init(), timeout=45.0)
    except Exception as e:
        logging.error(f"🚨 Fail-Safe 타임아웃 에러: {e}", exc_info=True)


async def scheduled_vwap_trade(context):
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
        logging.warning("⚠️ [vwap_trade] tx_lock 미초기화. 이번 사이클 스킵.")
        return
        
    def _get_market_close():
        nyse = mcal.get_calendar('NYSE')
        return nyse.schedule(start_date=now_est.date(), end_date=now_est.date())

    try:
        schedule = await asyncio.wait_for(asyncio.to_thread(_get_market_close), timeout=10.0)
        if schedule.empty:
            # 🚨 MODIFIED: [V72.21 휴장일 맹독성 페일 오픈 팩트 교정]
            logging.info("💤 [vwap_trade] 달력 API 빈 데이터 반환. 금일은 미국 증시 휴장일입니다.")
            return
        else:
            market_close = schedule.iloc[0]['market_close'].astimezone(est)
    except asyncio.TimeoutError:
        logging.error("⚠️ 장마감시간 달력 API 타임아웃. 평일 강제 마감시간(16:00 EST) 세팅.")
        if now_est.weekday() < 5:
            market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        else:
            return
    except Exception as e:
        logging.error(f"⚠️ 장마감시간 달력 API 에러({e}). 평일 강제 마감시간(16:00 EST) 세팅.")
        if now_est.weekday() < 5:
            market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        else:
            return
         
    # 🚨 MODIFIED: [V73.00 섀도우 오버라이드망 기상 시간 디커플링 수술]
    # 본진 덫 장전(15:26 EST)에 맞춰 타임 윈도우를 장 마감 34분 전으로 정밀 동기화
    vwap_start_time = market_close - datetime.timedelta(minutes=34, seconds=0)
    vwap_end_time = market_close 
    
    if not (vwap_start_time <= now_est <= vwap_end_time):
        return

    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    queue_ledger = app_data.get('queue_ledger')
    chat_id = context.job.chat_id
    base_map = app_data.get('base_map', {'SOXL': 'SOXX', 'TQQQ': 'QQQ'})
    
    vwap_cache = app_data.setdefault('vwap_cache', {})
    today_str = now_est.strftime('%Y%m%d')
    
    if vwap_cache.get('date') != today_str:
        vwap_cache.clear()
        vwap_cache['date'] = today_str

    async def _do_vwap():
        async with tx_lock:
            cash, holdings = await asyncio.to_thread(broker.get_account_balance)
            if holdings is None: return
            
            active_tickers = await asyncio.to_thread(cfg.get_active_tickers)
            _, allocated_cash = await asyncio.to_thread(get_budget_allocation, cash, active_tickers, cfg)
            
            base_curr_p = 0.0
            ask_price = 0.0
            exec_price = 0.0
            buy_qty = 0
            nuked_count = 0
            
            for t in active_tickers:
                version = await asyncio.to_thread(cfg.get_version, t)
                is_manual_vwap = await asyncio.to_thread(getattr(cfg, 'get_manual_vwap_mode', lambda x: False), t)

                if version == "V_REV" or (version == "V14" and is_manual_vwap):
                    if version == "V_REV":
                        if vwap_cache.get(f"REV_{t}_gap_hijack_fired"):
                            continue
                          
                        base_tkr = base_map.get(t, 'SOXX')
                    
                        try:
                            base_curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, base_tkr), timeout=10.0)
                            base_curr_p = float(base_curr_p_val or 0.0)
                        except Exception:
                            base_curr_p = 0.0
                              
                        try:
                            df_1min_base = await asyncio.wait_for(asyncio.to_thread(broker.get_1min_candles_df, base_tkr), timeout=10.0)
                            if df_1min_base is not None and not df_1min_base.empty:
                                df_b = df_1min_base.copy()
                                if 'time_est' in df_b.columns:
                                    df_b = df_b[(df_b['time_est'] >= '093000') & (df_b['time_est'] <= '155900')]
                                 
                                if not df_b.empty:
                                    df_b['tp'] = (df_b['high'].astype(float) + df_b['low'].astype(float) + df_b['close'].astype(float)) / 3.0
                                    df_b['vol'] = df_b['volume'].astype(float)
                                    df_b['vol_tp'] = df_b['tp'] * df_b['vol']
                                   
                                    c_vol = df_b['vol'].sum()
                                    base_vwap = df_b['vol_tp'].sum() / c_vol if c_vol > 0 else base_curr_p
            
                                    gap_pct = ((base_curr_p - base_vwap) / base_vwap * 100.0) if base_vwap > 0 else 0.0
                                    gap_thresh = await asyncio.to_thread(getattr(cfg, 'get_vrev_gap_threshold', lambda x: -0.67), t)
                                     
                                    if gap_pct <= gap_thresh:
                                        logging.info(f"⚡ [{t}] Gap Hijack Triggered! gap: {gap_pct:.2f}%, thresh: {gap_thresh}%")
                                        
                                        nuked_count = 0
                                        try:
                                            est_now = datetime.datetime.now(ZoneInfo('America/New_York'))
                                            d_str = est_now.strftime('%Y%m%d')
                                            resv_orders = await asyncio.to_thread(broker.get_reservation_orders, t, d_str, d_str)
                                            # 🚨 NEW: [V72.18 갭 하이재킹 예약 원장 맵핑 누수 및 일반 미체결 이중 방화벽 락온]
                                            for req in resv_orders:
                                                odno = req.get('ovrs_rsvn_odno') or req.get('odno')
                                                ord_dt = req.get('rsvn_ord_rcit_dt') or req.get('ord_dt', d_str)
                                                if odno:
                                                    try:
                                                        await asyncio.to_thread(broker.cancel_reservation_order, ord_dt, odno)
                                                        nuked_count += 1
                                                    except Exception as e:
                                                        logging.error(f"🚨 [{t}] 예약 덫 취소 실패: {e}")
                                             
                                            unfilled = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                            if isinstance(unfilled, list):
                                                for uo in unfilled:
                                                    dvsn = str(uo.get('ord_dvsn_cd') or uo.get('ord_dvsn') or '').strip().zfill(2)
                                                    if dvsn in ['36', '00']:
                                                        u_odno = uo.get('odno')
                                                        if u_odno:
                                                            try:
                                                                await asyncio.to_thread(broker.cancel_order, t, u_odno)
                                                                nuked_count += 1
                                                            except Exception as e:
                                                                logging.error(f"🚨 [{t}] 일반 덫(VWAP/LOC) 취소 실패: {e}")
                                            
                                            logging.info(f"⚡ [{t}] KIS 실원장 스캔: 예약 및 일반 덫 {nuked_count}건 팩트 파기 완료.")
                                        except Exception as e:
                                            logging.error(f"🚨 [{t}] KIS 실원장 덫 스캔 에러: {e}")
                                        
                                        await asyncio.sleep(2.0)
                                        
                                        seed = await asyncio.to_thread(cfg.get_seed, t)
                                        daily_limit = float(seed or 0.0) * 0.15
                                        
                                        alloc_cash = allocated_cash.get(t, 0.0)
                                        safe_alloc_cash = min(float(alloc_cash), daily_limit) if daily_limit > 0 else float(alloc_cash)
                                     
                                        total_spent = 0.0
                                        if hasattr(strategy, 'v_rev_plugin'):
                                            total_spent = float(strategy.v_rev_plugin.executed.get("BUY_BUDGET", {}).get(t, 0.0))
                                      
                                        rem_budget = max(0.0, safe_alloc_cash - total_spent)
                                         
                                        try:
                                            ask_price_val = await asyncio.wait_for(asyncio.to_thread(broker.get_ask_price, t), timeout=10.0)
                                            ask_price = float(ask_price_val or 0.0)
                                        except Exception:
                                            ask_price = 0.0
                                            
                                        try:
                                            curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, t), timeout=10.0)
                                            curr_p = float(curr_p_val or 0.0)
                                        except Exception:
                                            curr_p = 0.0
                                            
                                        exec_price = ask_price if ask_price > 0 else curr_p
                                        buy_qty = int(math.floor(rem_budget / exec_price)) if exec_price > 0 else 0
                                        
                                        if buy_qty > 0:
                                            res = await asyncio.to_thread(broker.send_order, t, "BUY", buy_qty, exec_price, "LIMIT")
                                            odno = res.get('odno', '') if isinstance(res, dict) else ''

                                            if res and res.get('rt_cd') == '0' and odno:
                                                vwap_cache[f"REV_{t}_gap_hijack_fired"] = True
                                                msg = f"⚡ <b>[{t}] 🤖 모멘텀 자율주행 (Gap Hijack) 섀도우 오버라이드 격발!</b>\n"
                                                msg += f"▫️ 기초자산({base_tkr}) VWAP 이탈률(<b>{gap_pct:+.2f}%</b>)이 임계치(<b>{gap_thresh}%</b>)를 하향 돌파했습니다.\n"
                                                msg += f"▫️ KIS 예약 덫({nuked_count}건)을 즉각 파기(Nuke)하고, 잔여 예산 100%를 매도 1호가로 일괄 스윕(Sweep) 타격했습니다!\n"
                                                msg += f"▫️ 스윕 수량: <b>{buy_qty}주</b> (단가: ${exec_price:.2f})"
                                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                                
                                                if hasattr(strategy, 'v_rev_plugin'):
                                                    await asyncio.to_thread(strategy.v_rev_plugin.record_execution, t, "BUY", buy_qty, exec_price)
                                                if queue_ledger:
                                                    await asyncio.to_thread(queue_ledger.add_lot, t, buy_qty, exec_price, "GAP_HIJACK_BUY")
                                      
                        except Exception as e:
                            logging.error(f"🚨 갭 스위칭 스캔 에러: {e}")

        try:
            await asyncio.wait_for(_do_vwap(), timeout=90.0)
        except Exception as e:
            logging.error(f"🚨 VWAP 섀도우 오버라이드 스케줄러 에러: {e}", exc_info=True)
