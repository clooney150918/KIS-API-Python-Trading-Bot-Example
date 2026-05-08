# ==========================================================
# FILE: scheduler_vwap.py
# ==========================================================
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

from scheduler_core import is_market_open

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
        if schedule.empty: return
        market_close = schedule.iloc[0]['market_close'].astimezone(est)
    except asyncio.TimeoutError:
        logging.error("⚠️ 장마감시간 달력 API 타임아웃. 16:00 강제 세팅.")
        market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
    except Exception:
        market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        
    vwap_start_time = market_close - datetime.timedelta(minutes=33, seconds=15)
    vwap_end_time = market_close 
    
    if not (vwap_start_time <= now_est <= vwap_end_time):
        return
    
    app_data = context.job.data
    cfg, broker, tx_lock = app_data['cfg'], app_data['broker'], app_data['tx_lock']
    strategy = app_data.get('strategy')
    strategy_rev = app_data.get('strategy_rev')
    queue_ledger = app_data.get('queue_ledger')
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
                
                if version == "V_REV" and is_manual_vwap:
                    continue
                
                if version == "V_REV" or (version == "V14" and is_manual_vwap):
                    if not vwap_cache.get(f"REV_{t}_nuked"):
                        try:
                            try:
                                curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, t), timeout=10.0)
                                curr_p = float(curr_p_val or 0.0)
                            except asyncio.TimeoutError:
                                logging.warning(f"⚠️ [{t}] 현재가 스캔 타임아웃. 0.0 폴백.")
                                curr_p = 0.0
                            except Exception:
                                curr_p = 0.0
                                
                            # 🚨 MODIFIED: [V54.02] 깡통 스냅샷 붕괴 방어. prev_c 다이렉트 추출 파이프라인 이식
                            prev_c = 0.0
                            snap_for_anchor = None
                            try:
                                if version == "V_REV" and strategy_rev:
                                    snap_for_anchor = await asyncio.to_thread(strategy_rev.load_daily_snapshot, t)
                                elif version == "V14" and is_manual_vwap and hasattr(strategy, 'v14_vwap_plugin'):
                                    snap_for_anchor = await asyncio.to_thread(strategy.v14_vwap_plugin.load_daily_snapshot, t)
                                
                                if snap_for_anchor:
                                    if snap_for_anchor.get("prev_c") and float(snap_for_anchor.get("prev_c", 0.0)) > 0:
                                        prev_c = float(snap_for_anchor["prev_c"])
                                        logging.info(f"🛡️ [{t}] 스냅샷 다이렉트 prev_c 팩트 수혈 완료: {prev_c}")
                                    elif "orders" in snap_for_anchor:
                                        buy_orders = [o for o in snap_for_anchor.get("orders", []) if o.get("side") == "BUY"]
                                        if buy_orders:
                                            p1_price = float(buy_orders[0].get("price", 0.0))
                                            if p1_price > 0:
                                                is_zs = snap_for_anchor.get("is_zero_start", snap_for_anchor.get("total_q", -1) == 0)
                                                if is_zs:
                                                    prev_c = round(p1_price / 1.15, 2)
                                                else:
                                                    prev_c = round(p1_price / 0.995, 2)
                                                logging.info(f"🛡️ [{t}] 스냅샷 역산 prev_c 강제 수혈 완료 (Buy1: {p1_price} -> prev_c: {prev_c})")
                            except Exception as e:
                                logging.warning(f"⚠️ [{t}] 스냅샷 팩트 수혈 실패: {e}")
                                
                            # 스냅샷 증발 시 API Fallback 가동
                            if prev_c <= 0.0:
                                try:
                                    prev_c_val = await asyncio.wait_for(asyncio.to_thread(broker.get_previous_close, t), timeout=10.0)
                                    prev_c = float(prev_c_val or 0.0)
                                    logging.info(f"🔄 [{t}] API Fallback prev_c 스캔 완료: {prev_c}")
                                except asyncio.TimeoutError:
                                    logging.warning(f"⚠️ [{t}] 전일종가 스캔 타임아웃. 0.0 폴백.")
                                    prev_c = 0.0
                                except Exception:
                                    prev_c = 0.0
                            
                            _, holdings = await asyncio.to_thread(broker.get_account_balance)
                            safe_holdings = holdings if isinstance(holdings, dict) else {}
                            h = safe_holdings.get(t) or {}
                            total_kis_qty = int(float(h.get('qty', 0)))
                            avg_price = float(h.get('avg', 0.0))
                            
                            avwap_qty = 0
                            if hasattr(strategy, 'load_avwap_state'):
                                avwap_state = await asyncio.to_thread(strategy.load_avwap_state, t, now_est)
                                avwap_qty = int(avwap_state.get('qty', 0))
                            
                            if version == "V_REV" and strategy_rev and queue_ledger:
                                rev_daily_budget = float(await asyncio.to_thread(cfg.get_seed, t) or 0.0) * 0.15
                                q_data = await asyncio.to_thread(queue_ledger.get_queue, t)
                                await asyncio.to_thread(
                                    strategy_rev.ensure_failsafe_snapshot,
                                    t, curr_p, prev_c, rev_daily_budget, q_data, total_kis_qty, avwap_qty
                                )
                            elif version == "V14" and is_manual_vwap and strategy and hasattr(strategy, 'v14_vwap_plugin'):
                                _, alloc_cash, _ = await asyncio.to_thread(cfg.calculate_v14_state, t)
                                await asyncio.to_thread(
                                    strategy.v14_vwap_plugin.ensure_failsafe_snapshot,
                                    t, curr_p, total_kis_qty, avwap_qty, avg_price, prev_c, alloc_cash
                                )

                            if version == "V14" and is_manual_vwap:
                                await asyncio.to_thread(broker.cancel_all_orders_safe, t, "BUY")
                                await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
                                msg = f"🌅 <b>[{t}] 하이브리드 타임 슬라이싱 기상 (자가 치유 가동)</b>\n"
                                msg += f"▫️ 장 마감 33분 전 진입을 확인하여 기존 LOC 덫 강제 취소(Nuke)했습니다.\n"
                                msg += f"▫️ 스케줄러 누락을 완벽히 극복하고 1분 단위 정밀 타격을 즉각 개시합니다. ⚔️"
                            else:
                                msg = f"🌅 <b>[{t}] 가상 에스크로 해제 및 엔진 기상</b>\n"
                                msg += f"▫️ 자전거래(FDS) 우회를 위해 설정된 <b>'가상 에스크로(Virtual Escrow)'를 해제</b>하고 자금을 실전 배치합니다.\n"
                                msg += f"▫️ 1분 단위 정밀 타격(VWAP 슬라이싱) 모드로 교전 수칙을 변경합니다. ⚔️"
                                
                            vwap_cache[f"REV_{t}_nuked"] = True
                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', disable_notification=True)
                            await asyncio.sleep(1.0)
                        except Exception as e:
                            logging.error(f"🚨 자가 치유 Nuke 실패: {e}", exc_info=True)
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
        if schedule.empty: return
        market_close = schedule.iloc[0]['market_close'].astimezone(est)
    except asyncio.TimeoutError:
        logging.error("⚠️ 장마감시간 달력 API 타임아웃. 16:00 강제 세팅.")
        market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
    except Exception:
        market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        
    vwap_start_time = market_close - datetime.timedelta(minutes=33, seconds=15)
    vwap_end_time = market_close 
    
    if not (vwap_start_time <= now_est <= vwap_end_time):
        return
        
    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    chat_id = context.job.chat_id
    base_map = app_data.get('base_map', {'SOXL': 'SOXX', 'TQQQ': 'QQQ'})
    
    regime_data = app_data.get('regime_data')
    
    vwap_cache = app_data.setdefault('vwap_cache', {})
    today_str = now_est.strftime('%Y%m%d')
    
    if vwap_cache.get('date') != today_str:
        vwap_cache.clear()
        vwap_cache['date'] = today_str

    async def _do_vwap():
        async with tx_lock:
            cash, holdings = await asyncio.to_thread(broker.get_account_balance)
            if holdings is None: return
            
            safe_holdings = holdings if isinstance(holdings, dict) else {}
            
            minutes_to_close = int(max(0, (market_close - now_est).total_seconds()) / 60)
            min_idx = 33 - minutes_to_close
            if min_idx < 0: min_idx = 0
            if min_idx > 29: min_idx = 29
            
            active_tickers = await asyncio.to_thread(cfg.get_active_tickers)
            for t in active_tickers:
                version = await asyncio.to_thread(cfg.get_version, t)
                is_manual_vwap = await asyncio.to_thread(getattr(cfg, 'get_manual_vwap_mode', lambda x: False), t)
                is_zero_start_session = False 

                if version == "V_REV" and is_manual_vwap:
                    continue

                profile = await asyncio.to_thread(cfg.get_vwap_profile, t)
                     
                target_keys = [f"15:{str(m).zfill(2)}" for m in range(27, 57)]
                total_target_vol = sum(profile.get(k, 0.0) for k in target_keys)
                time_str = now_est.strftime('%H:%M')
                
                if time_str in target_keys:
                    raw_weight = profile.get(time_str, 0.0)
                    current_weight = (raw_weight / total_target_vol) if total_target_vol > 0 else (1.0 / len(target_keys))
                else:
                    current_weight = 0.0

                if version == "V_REV" or (version == "V14" and is_manual_vwap):
                    if not vwap_cache.get(f"REV_{t}_nuked"):
                        try:
                            try:
                                curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, t), timeout=10.0)
                                curr_p = float(curr_p_val or 0.0)
                            except asyncio.TimeoutError:
                                logging.warning(f"⚠️ [{t}] 현재가 스캔 타임아웃. 0.0 폴백.")
                                curr_p = 0.0
                            except Exception:
                                curr_p = 0.0
                                
                            # 🚨 MODIFIED: [V54.02] 깡통 스냅샷 붕괴 방어. prev_c 다이렉트 추출 파이프라인 이식
                            prev_c = 0.0
                            snap_for_anchor = None
                            try:
                                if version == "V_REV" and strategy_rev:
                                    snap_for_anchor = await asyncio.to_thread(strategy_rev.load_daily_snapshot, t)
                                elif version == "V14" and is_manual_vwap and hasattr(strategy, 'v14_vwap_plugin'):
                                    snap_for_anchor = await asyncio.to_thread(strategy.v14_vwap_plugin.load_daily_snapshot, t)
                                
                                if snap_for_anchor:
                                    if snap_for_anchor.get("prev_c") and float(snap_for_anchor.get("prev_c", 0.0)) > 0:
                                        prev_c = float(snap_for_anchor["prev_c"])
                                        logging.info(f"🛡️ [{t}] 스냅샷 다이렉트 prev_c 팩트 수혈 완료: {prev_c}")
                                    elif "orders" in snap_for_anchor:
                                        buy_orders = [o for o in snap_for_anchor.get("orders", []) if o.get("side") == "BUY"]
                                        if buy_orders:
                                            p1_price = float(buy_orders[0].get("price", 0.0))
                                            if p1_price > 0:
                                                is_zs = snap_for_anchor.get("is_zero_start", snap_for_anchor.get("total_q", -1) == 0)
                                                if is_zs:
                                                    prev_c = round(p1_price / 1.15, 2)
                                                else:
                                                    prev_c = round(p1_price / 0.995, 2)
                                                logging.info(f"🛡️ [{t}] 스냅샷 역산 prev_c 강제 수혈 완료 (Buy1: {p1_price} -> prev_c: {prev_c})")
                            except Exception as e:
                                logging.warning(f"⚠️ [{t}] 스냅샷 팩트 수혈 실패: {e}")
                                
                            # 스냅샷 증발 시 API Fallback 가동
                            if prev_c <= 0.0:
                                try:
                                    prev_c_val = await asyncio.wait_for(asyncio.to_thread(broker.get_previous_close, t), timeout=10.0)
                                    prev_c = float(prev_c_val or 0.0)
                                    logging.info(f"🔄 [{t}] API Fallback prev_c 스캔 완료: {prev_c}")
                                except asyncio.TimeoutError:
                                    logging.warning(f"⚠️ [{t}] 전일종가 스캔 타임아웃. 0.0 폴백.")
                                    prev_c = 0.0
                                except Exception:
                                    prev_c = 0.0
                            
                            _, holdings = await asyncio.to_thread(broker.get_account_balance)
                            safe_holdings = holdings if isinstance(holdings, dict) else {}
                            h = safe_holdings.get(t) or {}
                            total_kis_qty = int(float(h.get('qty', 0)))
                            avg_price = float(h.get('avg', 0.0))
                            
                            avwap_qty = 0
                            if hasattr(strategy, 'load_avwap_state'):
                                avwap_state = await asyncio.to_thread(strategy.load_avwap_state, t, now_est)
                                avwap_qty = int(avwap_state.get('qty', 0))
                            
                            if version == "V_REV" and strategy_rev and queue_ledger:
                                rev_daily_budget = float(await asyncio.to_thread(cfg.get_seed, t) or 0.0) * 0.15
                                q_data = await asyncio.to_thread(queue_ledger.get_queue, t)
                                await asyncio.to_thread(
                                    strategy_rev.ensure_failsafe_snapshot,
                                    t, curr_p, prev_c, rev_daily_budget, q_data, total_kis_qty, avwap_qty
                                )
                            elif version == "V14" and is_manual_vwap and hasattr(strategy, 'v14_vwap_plugin'):
                                _, alloc_cash, _ = await asyncio.to_thread(cfg.calculate_v14_state, t)
                                await asyncio.to_thread(
                                    strategy.v14_vwap_plugin.ensure_failsafe_snapshot,
                                    t, curr_p, total_kis_qty, avwap_qty, avg_price, prev_c, alloc_cash
                                )

                            if version == "V14" and is_manual_vwap:
                                await asyncio.to_thread(broker.cancel_all_orders_safe, t, "BUY")
                                await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
                                msg = f"🌅 <b>[{t}] 하이브리드 타임 슬라이싱 기상 (자가 치유 가동)</b>\n"
                                msg += f"▫️ 장 마감 33분 전 진입을 확인하여 기존 LOC 덫 강제 취소(Nuke)했습니다.\n"
                                msg += f"▫️ 스케줄러 누락을 완벽히 극복하고 1분 단위 정밀 타격을 즉각 개시합니다. ⚔️"
                            else:
                                msg = f"🌅 <b>[{t}] 가상 에스크로 해제 및 엔진 기상 (자가 치유 가동)</b>\n"
                                msg += f"▫️ 장 마감 33분 전 진입을 확인하여 가상 에스크로를 해제했습니다.\n"
                                msg += f"▫️ 스케줄러 누락을 완벽히 극복하고 1분 단위 정밀 타격을 즉각 개시합니다. ⚔️"
                            
                            vwap_cache[f"REV_{t}_nuked"] = True
                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', disable_notification=True)
                            await asyncio.sleep(1.0)
                        except Exception as e:
                            logging.error(f"🚨 자가 치유 Nuke 실패: {e}", exc_info=True)
                            vwap_cache[f"REV_{t}_nuked"] = False
                            continue

                    try:
                        curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, t), timeout=10.0)
                        curr_p = float(curr_p_val or 0.0)
                    except asyncio.TimeoutError:
                        logging.warning(f"⚠️ [{t}] 현재가 스캔 타임아웃. 0.0 폴백.")
                        curr_p = 0.0
                    except Exception:
                        curr_p = 0.0
                        
                    # 🚨 MODIFIED: [V54.02] 깡통 스냅샷 붕괴 방어. prev_c 다이렉트 추출 파이프라인 이식
                    prev_c_live = 0.0
                    snap_for_anchor = None
                    if not vwap_cache.get(f"REV_{t}_anchor_prev_c"):
                        try:
                            if version == "V_REV" and strategy_rev:
                                snap_for_anchor = await asyncio.to_thread(strategy_rev.load_daily_snapshot, t)
                            elif version == "V14" and is_manual_vwap and hasattr(strategy, 'v14_vwap_plugin'):
                                snap_for_anchor = await asyncio.to_thread(strategy.v14_vwap_plugin.load_daily_snapshot, t)
                            
                            if snap_for_anchor:
                                if snap_for_anchor.get("prev_c") and float(snap_for_anchor.get("prev_c", 0.0)) > 0:
                                    prev_c_live = float(snap_for_anchor["prev_c"])
                                    logging.info(f"🛡️ [{t}] 스냅샷 다이렉트 prev_c 팩트 수혈 완료: {prev_c_live}")
                                elif "orders" in snap_for_anchor:
                                    buy_orders = [o for o in snap_for_anchor.get("orders", []) if o.get("side") == "BUY"]
                                    if buy_orders:
                                        p1_price = float(buy_orders[0].get("price", 0.0))
                                        if p1_price > 0:
                                            is_zs = snap_for_anchor.get("is_zero_start", snap_for_anchor.get("total_q", -1) == 0)
                                            if is_zs:
                                                prev_c_live = round(p1_price / 1.15, 2)
                                            else:
                                                prev_c_live = round(p1_price / 0.995, 2)
                                            logging.info(f"🛡️ [{t}] 스냅샷 역산 prev_c 강제 수혈 완료 (Buy1: {p1_price} -> prev_c: {prev_c_live})")
                        except Exception as e:
                            logging.warning(f"⚠️ [{t}] 스냅샷 팩트 수혈 실패: {e}")
                            
                        # 스냅샷 증발 시 API Fallback 가동
                        if prev_c_live <= 0.0:
                            try:
                                prev_c_live_val = await asyncio.wait_for(asyncio.to_thread(broker.get_previous_close, t), timeout=10.0)
                                prev_c_live = float(prev_c_live_val or 0.0)
                                logging.info(f"🔄 [{t}] API Fallback prev_c 스캔 완료: {prev_c_live}")
                            except asyncio.TimeoutError:
                                logging.warning(f"⚠️ [{t}] 전일종가 스캔 타임아웃. 0.0 폴백.")
                                prev_c_live = 0.0
                            except Exception:
                                prev_c_live = 0.0
                                
                        if prev_c_live > 0:
                            vwap_cache[f"REV_{t}_anchor_prev_c"] = prev_c_live
                            
                    prev_c = float(vwap_cache.get(f"REV_{t}_anchor_prev_c") or 0.0)

                    if curr_p <= 0 or prev_c <= 0: continue

                    if version == "V_REV":
                        strategy_rev = app_data.get('strategy_rev')
                        queue_ledger = app_data.get('queue_ledger')
                        if not strategy_rev or not queue_ledger: continue
                        
                        h = safe_holdings.get(t) or {}
                        actual_qty = int(float(h.get('qty', 0)))
                        
                        avwap_qty_for_shutdown = 0
                        if hasattr(strategy, 'load_avwap_state'):
                            avwap_state_sd = await asyncio.to_thread(strategy.load_avwap_state, t, now_est)
                            avwap_qty_for_shutdown = int(avwap_state_sd.get('qty', 0))
                            
                        pure_actual_qty = max(0, actual_qty - avwap_qty_for_shutdown)
                        
                        q_data = await asyncio.to_thread(queue_ledger.get_queue, t)
                        total_q = sum(item.get("qty", 0) for item in q_data)
                        
                        if pure_actual_qty == 0 and total_q > 0:
                            if vwap_cache.get(f"REV_{t}_sweep_msg_sent"):
                                continue
                            
                            if not vwap_cache.get(f"REV_{t}_panic_sell_warn"):
                                vwap_cache[f"REV_{t}_panic_sell_warn"] = True
                                await context.bot.send_message(
                                    chat_id=chat_id,
                                    text=f"🚨 <b>[비상] [{t}] 수동매매로 인한 잔고 증발이 감지되었습니다.</b>\n"
                                         f"▫️ 봇의 매매가 일시 정지됩니다.\n"
                                         f"▫️ 시드 오염을 막기 위해 즉시 <code>/reset</code> 커맨드를 실행하여 장부를 소각하십시오.",
                                    parse_mode='HTML'
                                )
                            continue
                        
                        cached_plan = await asyncio.to_thread(strategy_rev.load_daily_snapshot, t)
                        if not cached_plan:
                            today_str_est = now_est.strftime("%Y-%m-%d")
                            legacy_lots = [item for item in q_data if not str(item.get("date", "")).startswith(today_str_est)]
                            legacy_q = sum(int(item.get("qty", 0)) for item in legacy_lots if float(item.get('price', 0.0)) > 0)
                            is_zero_start = (legacy_q == 0)
                            if is_zero_start:
                                logging.warning(f"🚨 [{t}] V-REV 스냅샷 증발! 큐 장부 타임머신 역산 결과 0주 새출발 팩트 복원 완료.")
                            else:
                                is_zero_start = (total_q == 0)
                            
                            rev_alloc_cash = float(await asyncio.to_thread(cfg.get_seed, t) or 0.0) * 0.15
                            await asyncio.to_thread(
                                strategy_rev.ensure_failsafe_snapshot,
                                t, curr_p, prev_c, rev_alloc_cash, q_data, actual_qty, avwap_qty_for_shutdown
                            )
                        else:
                            is_zero_start = cached_plan.get("is_zero_start", cached_plan.get("total_q", -1) == 0)
                            
                        is_zero_start_session = is_zero_start 
                        virtual_q_data = [] if is_zero_start else q_data
                        
                        await asyncio.to_thread(strategy_rev._load_state_if_needed, t)
                            
                        if total_q > 0:
                            avg_price = sum(item.get("qty", 0) * item.get("price", 0.0) for item in q_data) / total_q
                            jackpot_trigger = avg_price * 1.010
                        else:
                            avg_price = 0.0
                            jackpot_trigger = float('inf')
                        
                        dates_in_queue = sorted(list(set(item.get('date') for item in q_data if item.get('date'))), reverse=True)
                        layer_1_qty = 0
                        layer_1_trigger = round(prev_c * 1.006, 2)
                        if dates_in_queue:
                            lots_for_date = [item for item in q_data if item.get('date') == dates_in_queue[0]]
                            layer_1_qty = sum(item.get('qty', 0) for item in lots_for_date)
                            if layer_1_qty > 0:
                                layer_1_price = sum(item.get('qty', 0) * item.get('price', 0.0) for item in lots_for_date) / layer_1_qty
                                layer_1_trigger = round(layer_1_price * 1.006, 2)
                        
                        target_sweep_qty = 0
                        sweep_type = ""

                        if not is_zero_start and minutes_to_close <= 3:
                            if total_q > 0 and curr_p >= jackpot_trigger:
                                target_sweep_qty = total_q
                                sweep_type = "잭팟 전량"
                            elif layer_1_qty > 0 and curr_p >= layer_1_trigger:
                                target_sweep_qty = layer_1_qty
                                sweep_type = "1층 잔여물량"
                                
                            if target_sweep_qty > 0:
                                await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
                                await asyncio.sleep(0.5)
                                
                                _, live_holdings = await asyncio.to_thread(broker.get_account_balance)
                                safe_live_holdings = live_holdings if isinstance(live_holdings, dict) else {}
                                
                                if safe_live_holdings and t in safe_live_holdings:
                                    h_live = safe_live_holdings[t]
                                    ord_psbl_qty = int(float(h_live.get('ord_psbl_qty', h_live.get('qty', 0))))
                                    
                                    avwap_qty_sweep = 0
                                    if hasattr(strategy, 'load_avwap_state'):
                                        avwap_state_sw = await asyncio.to_thread(strategy.load_avwap_state, t, now_est)
                                        avwap_qty_sweep = int(avwap_state_sw.get('qty', 0))
                                    
                                    pure_sellable_qty = max(0, ord_psbl_qty - avwap_qty_sweep)
                                    actual_sweep_qty = min(target_sweep_qty, pure_sellable_qty)
                                    
                                    if actual_sweep_qty > 0:
                                        try:
                                            bid_price_val = await asyncio.wait_for(asyncio.to_thread(broker.get_bid_price, t), timeout=10.0)
                                            bid_price = float(bid_price_val or 0.0)
                                        except asyncio.TimeoutError:
                                            logging.warning(f"⚠️ [{t}] 매수호가 스캔 타임아웃. 0.0 폴백.")
                                            bid_price = 0.0
                                        except Exception:
                                            bid_price = 0.0
                                            
                                        exec_price = bid_price if bid_price > 0 else curr_p

                                        res = await asyncio.to_thread(broker.send_order, t, "SELL", actual_sweep_qty, exec_price, "LIMIT")
                                        odno = res.get('odno', '') if isinstance(res, dict) else ''
                        
                                        if res and res.get('rt_cd') == '0' and odno:
                                            if not vwap_cache.get(f"REV_{t}_sweep_msg_sent"):
                                                msg = f"🌪️ <b>[{t}] V-REV 본대 {sweep_type} 3분 가속 스윕(Sweep) 개시!</b>\n"
                                                if "잭팟" in sweep_type:
                                                    msg += f"▫️ 장 마감 3분 전 데드존 철거. 잭팟 커트라인({jackpot_trigger:.2f}) 돌파를 확인했습니다.\n"
                                                else:
                                                    msg += f"▫️ 장 마감 3분 전 데드존 철거. 1층 앵커({layer_1_trigger:.2f}) 방어를 확인했습니다.\n"
                                                msg += f"▫️ 매도 가능 잔량이 0이 될 때까지 매 1분마다 지속 덤핑합니다! (현재 <b>{actual_sweep_qty}주</b> 매수호가 폭격) 🏆"
                                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                            vwap_cache[f"REV_{t}_sweep_msg_sent"] = True
                                            
                                            ccld_qty = 0
                                            for _ in range(4):
                                                await asyncio.sleep(2.0)
                                                unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                                safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []
                                                
                                                my_order = next((ox for ox in safe_unfilled if ox.get('odno') == odno), None)
                                                if my_order:
                                                    ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                                else:
                                                    ccld_qty = actual_sweep_qty
                                                    break
                                            
                                            if ccld_qty < actual_sweep_qty:
                                                try:
                                                    await asyncio.to_thread(broker.cancel_order, t, odno)
                                                    await asyncio.sleep(0.5)
                                                except Exception as e_cancel:
                                                    logging.warning(f"⚠️ [{t}] 스윕 잔여 주문 취소 실패: {e_cancel}")
                                                    
                                            if ccld_qty > 0:
                                                await asyncio.to_thread(strategy_rev.record_execution, t, "SELL", ccld_qty, exec_price)
                                                q_snap_before_pop = list(q_data)
                                                await asyncio.to_thread(queue_ledger.pop_lots, t, ccld_qty)
                                                remaining_after_pop = await asyncio.to_thread(queue_ledger.get_queue, t)
                                                remaining_qty_after = sum(item.get('qty', 0) for item in remaining_after_pop)
                                                if remaining_qty_after == 0 and total_q > 0:
                                                    try:
                                                        pending_file = f"data/pending_grad_{t}.json"
                                                        pending_data = {
                                                            "q_data_before": q_snap_before_pop,
                                                            "exec_price": exec_price,
                                                            "total_q": total_q
                                                        }
                                                        
                                                        def _save_pending_grad(f_path, p_data):
                                                            os.makedirs("data", exist_ok=True)
                                                            fd, tmp_path = tempfile.mkstemp(dir="data", text=True)
                                                            with os.fdopen(fd, 'w', encoding='utf-8') as _pf:
                                                                json.dump(p_data, _pf)
                                                                _pf.flush()
                                                                os.fsync(_pf.fileno())
                                                            os.replace(tmp_path, f_path)
                                                            
                                                        await asyncio.to_thread(_save_pending_grad, pending_file, pending_data)
                                                    except Exception as pg_e:
                                                        logging.error(f"🚨 [{t}] pending_grad 마커 파일 저장 실패: {pg_e}")
                                    else:
                                        if not vwap_cache.get(f"REV_{t}_sweep_skip_msg"):
                                            msg = f"⚠️ <b>[{t}] 스윕 피니셔 덤핑 생략 (MOC 락다운 감지)</b>\n▫️ 조건이 달성되었으나, 대상 물량이 수동 긴급 수혈(MOC) 등 취소 불가 상태로 미국 거래소에 묶여 있어 스윕 덤핑을 자동 스킵합니다."
                                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                            vwap_cache[f"REV_{t}_sweep_skip_msg"] = True
                                        
                            if target_sweep_qty > 0:
                                continue 
                        
                        try:
                            df_1min = await asyncio.wait_for(asyncio.to_thread(broker.get_1min_candles_df, t), timeout=10.0)
                            vwap_status = await asyncio.to_thread(strategy.analyze_vwap_dominance, df_1min)
                        except asyncio.TimeoutError:
                            logging.warning(f"⚠️ [{t}] 1분봉 스캔 타임아웃. 기본값 폴백.")
                            vwap_status = {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}
                        except Exception:
                            vwap_status = {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}
                        
                        current_regime = "BUY" if is_zero_start else ("SELL" if curr_p > prev_c else "BUY")
                        last_regime = vwap_cache.get(f"REV_{t}_regime")
                        
                        if not is_zero_start and last_regime and last_regime != current_regime:
                            await context.bot.send_message(
                                chat_id=chat_id, 
                                text=f"🔄 <b>[{t}] 실시간 공수 교대 발동!</b>\n"
                                     f"▫️ <b>[{last_regime} ➡️ {current_regime}]</b> 모드로 두뇌를 전환하며 궤도를 수정합니다.", 
                                parse_mode='HTML', disable_notification=True
                            )
                            try:
                                await asyncio.to_thread(broker.cancel_all_orders_safe, t, "BUY")
                                await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
                                await asyncio.to_thread(strategy_rev.reset_residual, t) 
                            except Exception as e:
                                err_msg = f"🛑 <b>[FATAL ERROR] {t} 공수 교대 중 기존 덫 취소 실패!</b>\n▫️ 2중 예산 소진 방어를 위해 당일 남은 V-REV 교전을 강제 중단(Hard-Lock)합니다.\n▫️ 상세 오류: {e}"
                                await context.bot.send_message(chat_id=chat_id, text=err_msg, parse_mode='HTML')
                                continue
                                
                        vwap_cache[f"REV_{t}_regime"] = current_regime
                        
                        if vwap_cache.get(f"REV_{t}_loc_fired"):
                            continue

                        rev_daily_budget = float(await asyncio.to_thread(cfg.get_seed, t) or 0.0) * 0.15
                        
                        target_orders = []
                        gap_thresh = await asyncio.to_thread(getattr(cfg, 'get_vrev_gap_threshold', lambda x: -0.67), t)
                        omni_filter = {"allow_buy": True}  
                        
                        if omni_filter["allow_buy"] and current_regime == "BUY" and not vwap_cache.get(f"REV_{t}_gap_hijack_fired"):
                            base_tkr = base_map.get(t, 'SOXX')
                            try:
                                base_curr_p_val = await asyncio.wait_for(asyncio.to_thread(broker.get_current_price, base_tkr), timeout=10.0)
                                base_curr_p = float(base_curr_p_val or 0.0)
                            except asyncio.TimeoutError:
                                base_curr_p = 0.0
                            except Exception:
                                base_curr_p = 0.0
                            
                            try:
                                df_1min_base = await asyncio.wait_for(asyncio.to_thread(broker.get_1min_candles_df, base_tkr), timeout=10.0)
                                if df_1min_base is not None and not df_1min_base.empty:
                                    df_b = df_1min_base.copy()
                                    df_b['tp'] = (df_b['high'].astype(float) + df_b['low'].astype(float) + df_b['close'].astype(float)) / 3.0
                                    df_b['vol'] = df_b['volume'].astype(float)
                                    df_b['vol_tp'] = df_b['tp'] * df_b['vol']
                                    
                                    c_vol = df_b['vol'].sum()
                                    base_vwap = df_b['vol_tp'].sum() / c_vol if c_vol > 0 else base_curr_p
                                    
                                    gap_pct = ((base_curr_p - base_vwap) / base_vwap * 100.0) if base_vwap > 0 else 0.0
                                    
                                    if gap_pct <= gap_thresh:
                                        total_spent = float(strategy_rev.executed["BUY_BUDGET"].get(t, 0.0))
                                        rem_budget = max(0.0, rev_daily_budget - total_spent)
                                        
                                        try:
                                            ask_price_val = await asyncio.wait_for(asyncio.to_thread(broker.get_ask_price, t), timeout=10.0)
                                            ask_price = float(ask_price_val or 0.0)
                                        except asyncio.TimeoutError:
                                            ask_price = 0.0
                                        except Exception:
                                            ask_price = 0.0
                                            
                                        exec_price = ask_price if ask_price > 0 else curr_p
                                        
                                        buy_qty = int(math.floor(rem_budget / exec_price))
                                        
                                        if buy_qty > 0:
                                            target_orders = [{'side': 'BUY', 'qty': buy_qty, 'price': exec_price, 'type': 'LIMIT', 'desc': '갭 스위치 스윕', 'bucket': 'BUY_GAP_HIJACK'}]
                                            vwap_cache[f"REV_{t}_gap_hijack_fired"] = True
                                            
                                            msg = f"⚡ <b>[{t}] 🤖 옴니 매트릭스 자율주행 (Gap Hijack) 발동!</b>\n"
                                            msg += f"▫️ 기초자산({base_tkr}) VWAP 이탈률(<b>{gap_pct:+.2f}%</b>)이 임계치(<b>{gap_thresh}%</b>)를 하향 돌파했습니다.\n"
                                            msg += f"▫️ VWAP 타임 슬라이싱 스케줄을 즉각 파기하고, 잔여 예산 100%를 매도 1호가로 전량 스윕(Sweep) 타격합니다!"
                                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                            except Exception as e:
                                logging.error(f"🚨 갭 스위칭 기초자산 스캔 에러: {e}")

                        if not target_orders:
                            rev_plan = None
                            try:
                                rev_plan = await asyncio.to_thread(
                                    strategy_rev.get_dynamic_plan,
                                    t, curr_p, prev_c, current_weight, vwap_status, min_idx, rev_daily_budget, virtual_q_data, False
                                )
                            except Exception as plan_e:
                                logging.error(f"🚨 [{t}] get_dynamic_plan 실행 에러 (해당 티커 건너뜀): {plan_e}")
                                
                            if rev_plan is None:
                                continue
                                
                            if not is_zero_start and rev_plan.get('trigger_loc') and minutes_to_close >= 15:
                                vwap_cache[f"REV_{t}_loc_fired"] = True
                                msg = f"🛡️ <b>[{t}] 60% 거래량 지배력 감지 (추세장 전환)</b>\n"
                                msg += f"▫️ 기관급 자금 쏠림으로 인해 위험한 1분 단위 타임 슬라이싱(VWAP)을 전면 중단합니다.\n"
                                msg += f"▫️ <b>잔여 할당량 전량을 양방향 LOC 방어선으로 전환 배치 완료!</b>\n"
                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML', disable_notification=True)
                                
                                for o in rev_plan.get('orders', []):
                                    if o['qty'] > 0:
                                        await asyncio.to_thread(broker.send_order, t, o['side'], o['qty'], o['price'], "LOC")
                                await asyncio.sleep(0.2)
                                continue
                            
                            target_orders = rev_plan.get('orders', [])

                    elif version == "V14":
                        if not is_manual_vwap:
                            continue
                            
                        h = safe_holdings.get(t, {'qty':0, 'avg':0.0})
                        actual_qty = int(h.get('qty', 0))
                        actual_avg = float(h.get('avg', 0.0))
                        
                        avwap_qty_v14 = 0
                        if hasattr(strategy, 'load_avwap_state'):
                            avwap_state_v14 = await asyncio.to_thread(strategy.load_avwap_state, t, now_est)
                            avwap_qty_v14 = int(avwap_state_v14.get('qty', 0))
                        
                        pure_qty_v14 = max(0, actual_qty - avwap_qty_v14)
                        
                        v14_vwap_plugin = strategy.v14_vwap_plugin
                        
                        _, v14_alloc_cash, _ = await asyncio.to_thread(cfg.calculate_v14_state, t)

                        cached_snap_v14 = await asyncio.to_thread(v14_vwap_plugin.load_daily_snapshot, t)
                        if not cached_snap_v14:
                            ledger_qty = 0
                            try:
                                today_str_est = now_est.strftime("%Y-%m-%d")
                                full_ledger = await asyncio.to_thread(cfg.get_ledger)
                                recs = [r for r in full_ledger if r['ticker'] == t and not str(r.get("date", "")).startswith(today_str_est)]
                                ledger_qty, _, _, _ = await asyncio.to_thread(cfg.calculate_holdings, t, recs)
                            except Exception: pass
                            
                            is_zero_start_session = (ledger_qty == 0)
                            if is_zero_start_session:
                                pure_qty_v14 = 0 
                            logging.warning(f"🚨 [{t}] V14_VWAP 스냅샷 증발! 메인 장부 역산 결과 0주 새출발 팩트 복원 완료.")
                            
                            await asyncio.to_thread(
                                v14_vwap_plugin.ensure_failsafe_snapshot,
                                t, curr_p, actual_qty, avwap_qty_v14, actual_avg, prev_c, v14_alloc_cash
                            )
                        else:
                            is_zero_start_session = cached_snap_v14.get("is_zero_start", cached_snap_v14.get("total_q", -1) == 0)
                            
                        plan = await asyncio.to_thread(
                            v14_vwap_plugin.get_dynamic_plan,
                            t, curr_p, prev_c, current_weight, min_idx, v14_alloc_cash, pure_qty_v14, actual_avg
                        )
                        target_orders = plan.get('orders', [])

                    for o in target_orders:
                        slice_qty = o['qty']
                        if slice_qty <= 0: continue
                        
                        target_price = o['price']
                        side = o['side']
                        bucket = o.get('bucket')

                        try:
                            ask_price_val = await asyncio.wait_for(asyncio.to_thread(broker.get_ask_price, t), timeout=10.0)
                            ask_price = float(ask_price_val or 0.0)
                        except asyncio.TimeoutError:
                            ask_price = 0.0
                        except Exception:
                            ask_price = 0.0
                            
                        try:
                            bid_price_val = await asyncio.wait_for(asyncio.to_thread(broker.get_bid_price, t), timeout=10.0)
                            bid_price = float(bid_price_val or 0.0)
                        except asyncio.TimeoutError:
                            bid_price = 0.0
                        except Exception:
                            bid_price = 0.0
                            
                        exec_price = ask_price if side == "BUY" else bid_price
                        if exec_price <= 0: exec_price = round(curr_p * 1.002, 2) if side == "BUY" else max(0.01, round(curr_p * 0.998, 2))
                        
                        async def _process_refund(unfilled_q):
                            if not bucket: return
                            r_val = unfilled_q * curr_p if side == "BUY" else unfilled_q
                            if version == "V_REV" and strategy_rev:
                                await asyncio.to_thread(strategy_rev.refund_residual, t, bucket, r_val)
                                logging.info(f"🔄 [{t}] VWAP {side} 잔차 환불 완료: 버킷({bucket}) +{r_val:.2f}")
                            elif version == "V14" and is_manual_vwap and hasattr(strategy, 'v14_vwap_plugin'):
                                await asyncio.to_thread(strategy.v14_vwap_plugin.refund_residual, t, bucket, r_val)
                                logging.info(f"🔄 [{t}] VWAP {side} 잔차 환불 완료: 버킷({bucket}) +{r_val:.2f}")

                        if side == "BUY":
                            if not is_zero_start_session and exec_price > target_price:
                                await _process_refund(slice_qty)
                                continue
                        elif side == "SELL":
                            if exec_price < target_price:
                                await _process_refund(slice_qty)
                                continue
                        
                        res = await asyncio.to_thread(broker.send_order, t, side, slice_qty, exec_price, "LIMIT")
                        odno = res.get('odno', '') if isinstance(res, dict) else ''
                        
                        ccld_qty = 0
                        if res and res.get('rt_cd') == '0' and odno:
                            for _ in range(4):
                                await asyncio.sleep(2.0)
                                unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []

                                my_order = next((ox for ox in safe_unfilled if ox.get('odno') == odno), None)
                                if my_order:
                                    ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                else:
                                    ccld_qty = slice_qty
                                    break
                                    
                            if ccld_qty < slice_qty:
                                try:
                                    await asyncio.to_thread(broker.cancel_order, t, odno)
                                    await asyncio.sleep(1.0)
                                except: pass
                        else:
                            err_msg = res.get('msg1', '알 수 없는 오류') if isinstance(res, dict) else '응답 없음'
                            logging.warning(f"🚨 [{t}] VWAP {side} 주문 KIS 서버 거절(Reject): {err_msg} (수량: {slice_qty}, 가격: {exec_price})")
                        
                        unfilled_qty = slice_qty - ccld_qty
                        if unfilled_qty > 0:
                            await _process_refund(unfilled_qty)
                                
                        if ccld_qty > 0:
                            if version == "V_REV":
                                await asyncio.to_thread(strategy_rev.record_execution, t, side, ccld_qty, exec_price)
                                if side == "BUY": await asyncio.to_thread(queue_ledger.add_lot, t, ccld_qty, exec_price, "VWAP_BUY")
                                elif side == "SELL": await asyncio.to_thread(queue_ledger.pop_lots, t, ccld_qty)
                            elif version == "V14":
                                await asyncio.to_thread(v14_vwap_plugin.record_execution, t, side, ccld_qty, exec_price)
                                
                        await asyncio.sleep(0.2)

        try:
            await asyncio.wait_for(_do_vwap(), timeout=90.0)
        except Exception as e:
            logging.error(f"🚨 VWAP 스케줄러 에러: {e}", exc_info=True)
