# ==========================================================
# FILE: vwap_core_engine.py
# ==========================================================
# 🚨 MODIFIED: [정량제(Fixed-Quantity) 팩트 스윕 락온] Gap Hijack 발동 시 잔여 예산을 억지로 100% 소진하던 맹독성 풀-스윕 로직을 영구 소각하고, 스냅샷에 락온된 '당일 잔여 목표 수량(Remaining Target Qty)'만을 100% 스윕 타격하여 하락장 시드 보존력(Runway)을 극대화함.
# 🚨 MODIFIED: [15:56 EST 정량제 캡핑 완결] 마지막 슬라이싱 틱(가중치 1.0) 도달 시, 예산을 재분배(global_rem_budget)하여 강제로 0원까지 탕진하던 낡은 정액제 찌꺼기 코드를 시스템 전역에서 전면 파기. 오직 정량(target_cum_qty - filled_qty)만을 정확히 긁어모아 절약된 예수금을 100% 보존.
# 🚨 MODIFIED: [상방 하이재킹 수익 캡핑(Profit Capping) 뇌관 100% 영구 소각] V-REV 전략의 거대 상승분(추세) 이익을 강제로 잘라먹던 '상방 하이재킹(+2.0% 도달 시 매도 덤핑)' 로직을 시스템 전역에서 영구 폐기하여 수익 극대화 팩트 수복 완료.
# 🚨 MODIFIED: [상태 오염 붕괴 방어] 기존 디스크 파일에 잔존하는 `upward_hijacked` 플래그를 무시(Bypass)하도록 슬라이싱 조건문 족쇄를 전면 파기하여, 폭등장에서도 1분 Slicing 엔진이 멈추지 않고 목표가 타격을 묵묵히 집행하도록 락온.
# 🚨 MODIFIED: [Thundering Herd 영구 소각] _retry_api 내의 await asyncio.sleep(0.06) 파편화 땜질 전면 삭제 및 GlobalThrottle(중앙 통제소)로 100% 위임.
# 🚨 MODIFIED: [재시작 붕괴 (Double Fire) 방어] 봇 재시작 시 메모리 증발로 인해 하방 하이재킹이 이중 격발되는 대참사를 막기 위해 디스크 크로스체크 전진 배치.
# 🚨 MODIFIED: [떨사오팔(Buy Low, Sell High) 절대 헌법 사수] 현재가가 매도(SELL) 타점 이상에 도달하여 '매도 조건'에 진입했을 경우, 장중 하방 갭(-2.0%)이 발생하더라도 맹독성 고점 추격 매수(Limit-Trap)를 막기 위해 하이재킹 스윕 매수를 100% 원천 차단하는 `is_sell_condition` 방어막 전격 결속.
# 🚨 NEW: [Case 47 자전거래(Wash Trade) 절대 방어망 결속] 암살자 덫과 본진 매수 덫이 교차하여 KIS 서버에서 리젝되는 현상을 막기 위해, 매수 타격 직전 암살자 덫을 임시 취소(Suspend)하고 오버나이트 설정에 따라 동적으로 재장전(Resume)하는 파이프라인 100% 팩트 이식.
# ==========================================================
import logging
import asyncio
import math
import time
import datetime
from zoneinfo import ZoneInfo
import html
import functools
import pandas as pd # NEW: DataFrame 벡터 연산 NameError 붕괴 방어

from scheduler_core import get_budget_allocation
from state_io_manager import _read_json_safe_sync, _atomic_write_json_sync
from global_throttle import GlobalThrottle # NEW: 중앙 통제 팩트 이관

def _safe_float(val):
    try:
        f_val = float(str(val or 0.0).replace(',', ''))
        if math.isnan(f_val) or math.isinf(f_val):
            return 0.0
        return f_val
    except Exception:
        return 0.0

async def _retry_api(func, *args, timeout=15.0, default=None, **kwargs):
    """ 🚨 [Case 31, 32, 33] TPS 캡핑(0.06s 땜질 소각) 및 지수 백오프 기반 중앙 통제망 래핑 """
    for attempt in range(3):
        try:
            # MODIFIED: 파편화된 sleep 소각 및 GlobalThrottle 동기 대기를 비동기 스레드로 격리하여 위임
            await asyncio.to_thread(GlobalThrottle.wait_api_sync)
            
            if asyncio.iscoroutinefunction(func):
                 return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
            else:
                p_func = functools.partial(func, *args, **kwargs)
                return await asyncio.wait_for(asyncio.to_thread(p_func), timeout=timeout)
        except Exception as e:
            if attempt == 2:
                func_name = getattr(func, '__name__', 'unknown_func')
                logging.debug(f"🚨 API 래퍼 최종 실패 ({func_name}): {e}")
                return default
            await asyncio.sleep(1.0 * (2 ** attempt))
    return default

async def _safe_send(context, chat_id, text, timeout=15.0, **kwargs):
    if not chat_id: return None
    try:
        return await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=text, **kwargs), timeout=timeout)
    except Exception as e:
        logging.error(f"🚨 텔레그램 전송 실패: {e}")
        return None

def _fetch_market_schedule_sync(now_est):
    """ 🚨 [제2헌법 준수] 달력 API(mcal) 스캔 로직 단일화 (GlobalThrottle 중앙 통제) """
    GlobalThrottle.wait_api_sync()
    import pandas_market_calendars as mcal
    nyse = mcal.get_calendar('NYSE')
    return nyse.schedule(start_date=now_est.date(), end_date=now_est.date())

async def _get_market_close_time(now_est):
    """ 🚨 [DRY 원칙 및 제5헌법 결속] 달력 스캔 중복 소각 및 3단 백오프 기반 Fail-Open 폴백 """
    schedule = None
    for attempt in range(3):
        try:
            schedule = await asyncio.wait_for(asyncio.to_thread(_fetch_market_schedule_sync, now_est), timeout=10.0)
            break
        except asyncio.TimeoutError:
            if attempt == 2: logging.error("⚠️ 장마감시간 달력 API 타임아웃. 평일 강제 마감시간(16:00 EST) 세팅.")
            else: await asyncio.sleep(1.0 * (2 ** attempt))
        except Exception as e:
            if attempt == 2: logging.error(f"⚠️ 장마감시간 달력 API 에러({e}). 평일 강제 마감시간(16:00 EST) 세팅.")
            else: await asyncio.sleep(1.0 * (2 ** attempt))

    if schedule is not None and not schedule.empty:
        return schedule.iloc[0]['market_close'].astimezone(now_est.tzinfo)
    elif schedule is not None and schedule.empty:
        return None 
    else:
        if now_est.weekday() < 5:
            return now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        else:
            return None

async def _read_state_safe(filepath, date_str, default_val):
    """ 🚨 [Case 47] 전역 파일 뮤텍스 강제 적용 상태 읽기 래퍼 """
    with GlobalThrottle.get_file_lock(filepath):
        return await _retry_api(_read_json_safe_sync, filepath, date_str, default=default_val)

async def _write_state_safe(filepath, state_dict):
    """ 🚨 [Case 47] 전역 파일 뮤텍스 강제 적용 원자적 쓰기 래퍼 """
    with GlobalThrottle.get_file_lock(filepath):
        return await _retry_api(_atomic_write_json_sync, filepath, state_dict)

async def execute_vwap_init(tx_lock, cfg, broker, chat_id, context, vwap_cache):
    async with tx_lock:
        active_tickers = await _retry_api(cfg.get_active_tickers, default=[])
        if isinstance(active_tickers, str): active_tickers = [active_tickers]
        elif not isinstance(active_tickers, list): active_tickers = []
        
        for raw_t in active_tickers:
            t = str(raw_t).strip().upper()
            if not t: continue
            
            try:
                version = await _retry_api(cfg.get_version, t, default="V14")
                is_manual_vwap = await _retry_api(getattr(cfg, 'get_manual_vwap_mode', lambda x: False), t, default=False)
                
                if version == "V_REV" or (version == "V14" and is_manual_vwap):
                    if not vwap_cache.get(f"REV_{t}_nuked"):
                        msg = f"🌅 <b>[{html.escape(str(t))}] 자체 1분 슬라이싱 VWAP 엔진 기상</b>\n"
                        msg += f"▫️ KIS 예약 덫 관망 및 장 마감 34분 전 로컬 펄스 타격 엔진의 가동 대기를 확인했습니다.\n"
                        if version == "V_REV":
                            msg += f"▫️ 운용종목 갭 이탈 감지 시 즉각 개입(Gap Hijack)하는 폭락장 스윕 모드가 함께 가동됩니다. ⚔️"

                        vwap_cache[f"REV_{t}_nuked"] = True
                        
                        await _safe_send(context, chat_id, msg, parse_mode='HTML', disable_notification=True)
            except Exception as e:
                logging.error(f"🚨 [{t}] 관측 모드 샌드박스 에러 (격리 완료): {e}")
                vwap_cache[f"REV_{t}_nuked"] = False 

async def execute_vwap_trade(tx_lock, cfg, broker, strategy, queue_ledger, chat_id, context, base_map, vwap_cache):
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    today_hyphen = now_est.strftime('%Y-%m-%d')
    
    kst_zone = ZoneInfo('Asia/Seoul')
    now_kst = datetime.datetime.now(kst_zone)
    today_kst_str = now_kst.strftime('%Y%m%d')

    async with tx_lock:
        res = await _retry_api(broker.get_account_balance, timeout=15.0)
        cash = _safe_float(res[0]) if isinstance(res, (list, tuple)) and len(res) > 0 else 0.0
        holdings = res[1] if isinstance(res, (list, tuple)) and len(res) > 1 else {}
        if not isinstance(holdings, dict): holdings = {}
                
        if res is None: return
        
        active_tickers = await _retry_api(cfg.get_active_tickers, default=[])
        if isinstance(active_tickers, str): active_tickers = [active_tickers]
        elif not isinstance(active_tickers, list): active_tickers = []
        
        alloc_res = await _retry_api(get_budget_allocation, cash, active_tickers, cfg, default=({}, {}))
        allocated_cash = alloc_res[1] if isinstance(alloc_res, (list, tuple)) and len(alloc_res) > 1 else {}
        if not isinstance(allocated_cash, dict): allocated_cash = {}
        
        t_curr_p = 0.0
        nuked_count = 0
        
        for raw_t in active_tickers:
            t = str(raw_t).strip().upper()
            if not t: continue
            
            actual_qty = int(_safe_float(holdings.get(t, {}).get('qty', 0)))

            vrev_q_qty = 0
            if queue_ledger:
                q_data = await _retry_api(queue_ledger.get_queue, t, default=[])
                vrev_q_qty = sum(int(_safe_float(item.get("qty"))) for item in (q_data or []) if isinstance(item, dict))

            try:
                version = await _retry_api(cfg.get_version, t, default="V14")
                is_manual_vwap = await _retry_api(getattr(cfg, 'get_manual_vwap_mode', lambda x: False), t, default=False)

                if version == "V_REV" or (version == "V14" and is_manual_vwap):
                    slice_file = f"data/vrev_slice_state_{t}.json"
                    
                    try:
                        after_state_file = f"data/vrev_aftermarket_state_{t}.json"
                        after_state = await _read_state_safe(after_state_file, today_hyphen, {})
                        if after_state.get('date') == today_hyphen:
                            pending_aftermarket = any(isinstance(o, dict) and str(o.get('status')) == 'PENDING' for o in after_state.get('orders', []))
                            if pending_aftermarket:
                                logging.info(f"⏳ [{t}] 애프터장(16:01) 이관 대기 중. 정규장 마감 직전(16:00)의 Gap Hijack 및 슬라이싱 엔진을 전면 바이패스합니다.")
                                continue
                    except Exception as e:
                        logging.error(f"🚨 [{t}] 애프터장 이관 상태 교차 검증 에러: {e}")

                    slice_state_disk = await _read_state_safe(slice_file, today_hyphen, {})
                    disk_hijacked = slice_state_disk.get('hijacked', False)

                    is_downward_hijacked_now = vwap_cache.get(f"REV_{t}_gap_hijack_fired", False) or disk_hijacked

                    # ======================================================
                    # [ 1. Gap Hijack (오직 하방 폭락장 정량제 스윕 감시) ]
                    # ======================================================
                    if version == "V_REV" and not is_downward_hijacked_now:
                        t_curr_p = _safe_float(await _retry_api(broker.get_current_price, t))
                        df_1min_t = await _retry_api(broker.get_1min_candles_df, t)
                                
                        if df_1min_t is not None and not df_1min_t.empty:
                            df_t = df_1min_t.copy()
                            df_t = df_t[df_t.index.date == now_est.date()]
                            
                            if 'time_est' in df_t.columns:
                                df_t = df_t[(df_t['time_est'] >= '093000') & (df_t['time_est'] <= '155900')]
                                
                            if not df_t.empty:
                                df_t['high'] = df_t['high'].ffill().bfill()
                                df_t['low'] = df_t['low'].ffill().bfill()
                                df_t['close'] = df_t['close'].ffill().bfill()
                                df_t['volume'] = df_t['volume'].ffill().bfill().fillna(0)

                                df_t['tp'] = (df_t['high'].astype(float) + df_t['low'].astype(float) + df_t['close'].astype(float)) / 3.0
                                df_t['vol'] = df_t['volume'].astype(float)
                                df_t['vol_tp'] = df_t['tp'] * df_t['vol']
                                
                                c_vol = df_t['vol'].sum()
                                t_vwap = df_t['vol_tp'].sum() / c_vol if c_vol > 0 else t_curr_p
                                
                                gap_pct = ((t_curr_p - t_vwap) / t_vwap * 100.0) if t_vwap > 0 else 0.0
                                
                                gap_thresh = _safe_float(await _retry_api(getattr(cfg, 'get_vrev_gap_threshold', lambda x: -2.0), t, default=-2.0))
                                if gap_thresh == -0.67: gap_thresh = -2.0
                                
                                # ----------------------------------------------------
                                # 🚨 [하방 매수 하이재킹 격발망]
                                # ----------------------------------------------------
                                if gap_pct <= gap_thresh:
                                    slice_state_check = slice_state_disk
                                    has_buy_plan = any(isinstance(o, dict) and str(o.get('side')) == 'BUY' for o in slice_state_check.get('orders', []))
                                    
                                    sell_orders = [o for o in slice_state_check.get('orders', []) if str(o.get('side')) == 'SELL']
                                    is_sell_condition = False
                                    for o in sell_orders:
                                        tp = _safe_float(o.get('target_price', 0.0))
                                        if tp > 0.0 and t_curr_p >= tp:
                                            is_sell_condition = True
                                            break
                                            
                                    if not has_buy_plan:
                                        if not vwap_cache.get(f"REV_{t}_gap_hijack_blocked_log", False):
                                            logging.info(f"⚡ [{t}] 하방 Gap Hijack 조건 도달({gap_pct:.2f}%) ➔ 🛑 금일 통합지시서에 매수(BUY) 플랜이 없어 스윕 매수를 전면 차단(Bypass)합니다.")
                                            vwap_cache[f"REV_{t}_gap_hijack_blocked_log"] = True
                                        
                                        vwap_cache[f"REV_{t}_gap_hijack_fired"] = True
                                        is_downward_hijacked_now = True
                                        
                                    elif is_sell_condition:
                                        if not vwap_cache.get(f"REV_{t}_gap_hijack_sell_blocked_log", False):
                                            logging.info(f"⚡ [{t}] 하방 Gap Hijack 조건 도달({gap_pct:.2f}%) ➔ 🛑 현재가(${t_curr_p:.2f})가 매도(SELL) 타점 이상이므로 스윕 매수를 차단하고 관망합니다 (Buy Low 원칙 사수).")
                                            vwap_cache[f"REV_{t}_gap_hijack_sell_blocked_log"] = True
                                        
                                    else:
                                        vwap_cache.pop(f"REV_{t}_gap_hijack_sell_blocked_log", None)
                                        logging.info(f"⚡ [{t}] Downward Gap Hijack Triggered! gap: {gap_pct:.2f}%, thresh: {gap_thresh}%")
                                        nuked_count = 0
                                        
                                        try:
                                            est_now = datetime.datetime.now(ZoneInfo('America/New_York'))
                                            d_str = est_now.strftime('%Y%m%d')
                                            
                                            resv_orders = await _retry_api(broker.get_reservation_orders, t, d_str, d_str, default=[])
                                            safe_resv_orders = resv_orders if isinstance(resv_orders, list) else []
                                            
                                            for req in safe_resv_orders:
                                                if not isinstance(req, dict): continue
                                                
                                                side_cd = str(req.get('sll_buy_dvsn_cd') or req.get('sll_buy_dvsn') or '')
                                                if side_cd == '01': continue 
                                                
                                                odno = str(req.get('ovrs_rsvn_odno') or req.get('odno') or '')
                                                ord_dt = str(req.get('rsvn_ord_rcit_dt') or req.get('ord_dt') or d_str)
                                                if odno:
                                                    c_res = await _retry_api(broker.cancel_reservation_order, ord_dt, odno)
                                                    if c_res: nuked_count += 1
                                        
                                            unfilled = await _retry_api(broker.get_unfilled_orders_detail, t, default=[])
                                            safe_unfilled = unfilled if isinstance(unfilled, list) else []
                                            
                                            for uo in safe_unfilled:
                                                if not isinstance(uo, dict): continue
                                                
                                                side_cd = str(uo.get('sll_buy_dvsn_cd') or uo.get('sll_buy_dvsn') or '')
                                                if side_cd == '01': continue
                                                
                                                dvsn = str(uo.get('ord_dvsn_cd') or uo.get('ord_dvsn') or '').strip().zfill(2)
                                                if dvsn in ['36', '00']:
                                                    u_odno = str(uo.get('odno') or '')
                                                    if u_odno:
                                                        c_res = await _retry_api(broker.cancel_order, t, u_odno)
                                                        if c_res: nuked_count += 1
                                                        
                                            logging.info(f"⚡ [{t}] KIS 실원장 스캔: 예약 및 일반 매수(BUY) 덫 {nuked_count}건 팩트 파기 완료 (SELL 구출망 보존).")
                                        except Exception as e:
                                            logging.error(f"🚨 [{t}] KIS 실원장 덫 스캔 에러: {e}")

                                        try:
                                            s_state = await _read_state_safe(slice_file, today_hyphen, {})
                                            s_state['hijacked'] = True
                                            s_state['date'] = today_hyphen
                                            await _write_state_safe(slice_file, s_state)
                                            logging.info(f"⚡ [{t}] 로컬 1분 슬라이싱 엔진 무효화 (hijacked) 선제 마킹 완료.")
                                        except Exception as e:
                                            logging.error(f"🚨 [{t}] 로컬 슬라이스 무효화 처리 에러: {e}")

                                        # 🚨 MODIFIED: [정량제 팩트 스윕 매수] 스냅샷 지시서의 당일 잔여 목표 수량(Remaining Target Qty) 정밀 산출
                                        buy_qty = 0
                                        for ox in slice_state_disk.get('orders', []):
                                            if str(ox.get('side')) == 'BUY':
                                                _tot = int(_safe_float(ox.get('total_qty', 0)))
                                                _fil = int(_safe_float(ox.get('filled_qty', 0)))
                                                if _tot - _fil > 0:
                                                    buy_qty += (_tot - _fil)

                                        ask_price = _safe_float(await _retry_api(broker.get_ask_price, t))
                                        curr_p = _safe_float(await _retry_api(broker.get_current_price, t))
                                        exec_price = ask_price if ask_price > 0 else curr_p
                                        
                                        if buy_qty > 0:
                                            res = await _retry_api(broker.send_order, t, "BUY", buy_qty, exec_price, "LIMIT")
                                            safe_res = res if isinstance(res, dict) else {}
                                            odno = str(safe_res.get('odno') or '')
                                            
                                            if safe_res.get('rt_cd') == '0' and odno:
                                                vwap_cache[f"REV_{t}_gap_hijack_fired"] = True
                                                is_downward_hijacked_now = True
                                                
                                                try:
                                                    final_slice_state = await _read_state_safe(slice_file, today_hyphen, {})
                                                    for o in final_slice_state.get('orders', []):
                                                        if str(o.get('side')) == 'BUY':
                                                            o['filled_qty'] = o.get('total_qty', 0)
                                                    
                                                    final_slice_state['hijacked'] = True
                                                    final_slice_state['date'] = today_hyphen
                                                    await _write_state_safe(slice_file, final_slice_state)
                                                except Exception as e:
                                                    logging.error(f"🚨 [{t}] 하이재킹 체결 후 로컬 지시서 수량(filled_qty) 만기 처리 중 I/O 에러: {e}")

                                                msg = f"⚡ <b>[{html.escape(str(t))}] 🤖 하방 모멘텀 자율주행 (Gap Hijack) 스윕 오버라이드 격발!</b>\n"
                                                msg += f"▫️ 당일 누적 VWAP 이탈률(<b>{gap_pct:+.2f}%</b>)이 임계치(<b>{gap_thresh}%</b>)를 하향 돌파했습니다.\n"
                                                msg += f"▫️ 예약/미체결 덫({nuked_count}건) 파기 후, 스냅샷 <b>잔여 목표 수량 전량</b>을 매도 1호가로 일괄 타격(Sweep)했습니다!\n"
                                                msg += f"▫️ 정량제 스윕 수량: <b>{buy_qty}주</b> (단가: ${exec_price:.2f})\n"
                                                msg += f"▫️ 절약된 막대한 예수금은 100% 온전히 보존되어 총알(Runway)로 반환됩니다."
                                                
                                                await _safe_send(context, chat_id, msg, parse_mode='HTML')
                                                
                                                if version == "V_REV":
                                                    if hasattr(strategy, 'v_rev_plugin'):
                                                        await _retry_api(strategy.v_rev_plugin.record_execution, t, "BUY", buy_qty, exec_price)
                                                    if queue_ledger:
                                                        await _retry_api(queue_ledger.add_lot, t, buy_qty, exec_price, "GAP_HIJACK_BUY")
                                                else:
                                                    if hasattr(strategy, 'v14_vwap_plugin'):
                                                        await _retry_api(strategy.v14_vwap_plugin.record_execution, t, "BUY", buy_qty, exec_price)
                                            else:
                                                err_msg = html.escape(str(safe_res.get('msg1') or '응답 없음/통신 장애'))
                                                logging.error(f"🚨 [{t}] 하방 갭 하이재킹 KIS 서버 거절: {err_msg}")
                                                reject_msg = (
                                                    f"🚨 <b>[{html.escape(str(t))}] 하방 갭 하이재킹 스윕(Sweep) 서버 거절 (Reject)!</b>\n"
                                                    f"▫️ 사유: <code>{err_msg}</code>\n"
                                                    f"▫️ 조치: 다음 스캔 시 재시도합니다."
                                                )
                                                await _safe_send(context, chat_id, reject_msg, parse_mode='HTML')
                                        else:
                                            vwap_cache[f"REV_{t}_gap_hijack_fired"] = True
                                            is_downward_hijacked_now = True
                                            logging.info(f"⚡ [{t}] 하방 Gap Hijack 격발 조건을 만족했으나 당일 정량 수량 충족으로 매수 생략 (플래그 락온 완료).")

                    # ======================================================
                    # [ 2. 자체 VWAP 1분 슬라이싱 로컬 엔진 가동 ]
                    # ======================================================
                    curr_time_obj = now_est.time()
                    time_start = datetime.time(15, 27)
                    time_end = datetime.time(15, 57, 59)
                    
                    if time_start <= curr_time_obj <= time_end:
                        slice_state = await _read_state_safe(slice_file, today_hyphen, {})
                        
                        if slice_state.get('date') != today_hyphen:
                            continue 
                            
                        is_state_hijacked = slice_state.get('hijacked', False) or is_downward_hijacked_now
                        
                        orders = slice_state.get('orders', [])
                        if not isinstance(orders, list): orders = []
                        if not orders: continue
                        
                        is_cleanup_phase = (curr_time_obj >= datetime.time(15, 57))
                            
                        curr_hm = now_est.strftime("%H:%M")
                        try:
                            vwap_profile = await _retry_api(cfg.get_vwap_profile, t, default={})
                            if not isinstance(vwap_profile, dict): vwap_profile = {}
                        except Exception: vwap_profile = {}
                        
                        cum_weight = _safe_float(vwap_profile.get(curr_hm, 0.0))
                        
                        if is_cleanup_phase:
                            cum_weight = 1.0
                        elif cum_weight == 0.0:
                            start_mins = 15 * 60 + 27
                            curr_mins = now_est.hour * 60 + now_est.minute
                            elapsed = max(0, curr_mins - start_mins + 1)
                            cum_weight = min(1.0, max(0.0, elapsed / 29.0))
                            
                        state_changed = False
                        
                        for o in orders:
                            if not isinstance(o, dict): continue
                            
                            total_qty = int(_safe_float(o.get('total_qty')))
                            filled_qty = int(_safe_float(o.get('filled_qty')))
                            target_price = _safe_float(o.get('target_price'))
                            side = str(o.get('side', 'BUY'))
                            last_odno = str(o.get('last_odno', ''))
                            
                            if is_state_hijacked and side == 'BUY':
                                continue
                            
                            if filled_qty >= total_qty and not last_odno:
                                continue
                            
                            ccld_qty_this_tick = 0
                            if last_odno:
                                cancel_successful = False
                                c_res = await _retry_api(broker.cancel_order, t, last_odno, timeout=10.0)
                                if isinstance(c_res, dict) and str(c_res.get('rt_cd', '')) == '0':
                                    cancel_successful = True
                                    
                                is_still_open = False
                                if not cancel_successful:
                                    unf = await _retry_api(broker.get_unfilled_orders_detail, t, default=[])
                                    safe_unf = unf if isinstance(unf, list) else []
                                    if any(isinstance(x, dict) and str(x.get('odno', '')) == last_odno for x in safe_unf):
                                        is_still_open = True
                                        
                                if is_still_open:
                                    logging.warning(f"🚨 [{t}] 취소 실패 및 미체결 잔존 확인 (Double Spending 방어). 다음 분으로 이연합니다.")
                                    continue
                                
                                try:
                                    _execs = await _retry_api(broker.get_execution_history, t, today_kst_str, today_kst_str, default=[])
                                    _safe_execs = _execs if isinstance(_execs, list) else []
                                    _filled_rec = next((ex for ex in _safe_execs if isinstance(ex, dict) and str(ex.get('odno', '')) == last_odno), None)
                                    
                                    if _filled_rec:
                                        ccld_qty_this_tick = int(_safe_float(_filled_rec.get('ft_ccld_qty')))
                                        real_exec_price = _safe_float(_filled_rec.get('ft_ccld_unpr3'))
                                        if real_exec_price == 0.0: real_exec_price = target_price
                                    else:
                                        ccld_qty_this_tick = 0
                                        real_exec_price = 0.0
                                except Exception as e:
                                    logging.error(f"🚨 [{t}] 자체 슬라이싱 체결 원장 교차 검증 에러: {e}")
                                    ccld_qty_this_tick = 0
                                    real_exec_price = 0.0
                                
                                if ccld_qty_this_tick > 0:
                                    processed_odnos = vwap_cache.setdefault(f"PROCESSED_ODNOS_{t}", set())
                                    if last_odno not in processed_odnos:
                                        processed_odnos.add(last_odno)

                                        def _sync_ledger_atomic(tkr, sde, c_qty, r_price, q_ledger, strat, ver):
                                            if ver == "V_REV":
                                                if q_ledger:
                                                    if sde == "BUY":
                                                        q_ledger.add_lot(tkr, c_qty, r_price, "VREV_VWAP_BUY")
                                                    else:
                                                        q_ledger.pop_lots(tkr, c_qty, r_price)
                                                if hasattr(strat, 'v_rev_plugin'):
                                                    strat.v_rev_plugin.record_execution(tkr, sde, c_qty, r_price)
                                            else:
                                                if hasattr(strat, 'v14_vwap_plugin'):
                                                    strat.v14_vwap_plugin.record_execution(tkr, sde, c_qty, r_price)

                                        try:
                                            p_sync = functools.partial(_sync_ledger_atomic, t, side, ccld_qty_this_tick, real_exec_price, queue_ledger, strategy, version)
                                            await asyncio.wait_for(asyncio.to_thread(p_sync), timeout=10.0)
                                            logging.info(f"💾 [{t}] 자체 슬라이싱 체결 장부 원자적 동기화 완료: {side} {ccld_qty_this_tick}주 @ ${real_exec_price:.2f}")
                                        except Exception as e:
                                            processed_odnos.remove(last_odno) 
                                            logging.error(f"🚨 [{t}] 자체 슬라이싱 체결 장부 동기화 실패 (캐시 롤백): {e}")
                                        
                                        msg_side = "매수" if side == "BUY" else "매도"
                                        logging.info(f"⚡ [{t}] 섀도 엔진 체결 팩트 장부 동기화 완료: {msg_side} {ccld_qty_this_tick}주 @ ${real_exec_price:.2f} (텔레그램 타전 바이패스)")

                                filled_qty += ccld_qty_this_tick
                                o['filled_qty'] = filled_qty
                                o['last_odno'] = ""
                                o['last_sent_qty'] = 0
                                state_changed = True
                            
                            if is_cleanup_phase:
                                continue 

                            target_cum_qty = round(total_qty * cum_weight)
                            
                            exec_price = 0.0
                            if side == "BUY":
                                exec_price = _safe_float(await _retry_api(broker.get_ask_price, t))
                            else:
                                exec_price = _safe_float(await _retry_api(broker.get_bid_price, t))
                                    
                            if exec_price <= 0.0:
                                exec_price = _safe_float(await _retry_api(broker.get_current_price, t))
                                         
                            qty_to_send = 0
                            if target_price > 0.0:
                                is_target_hit = False
                                if side == "BUY" and exec_price <= target_price:
                                    is_target_hit = True
                                elif side == "SELL" and exec_price >= target_price:
                                    is_target_hit = True

                                if not is_target_hit:
                                    continue 

                            # 🚨 MODIFIED: [1분 슬라이싱 정량제 캡핑] 정액제 강제 소진 로직을 소각하고, 오직 스냅샷 목표 수량 기반 정밀 슬라이싱(정량제) 100% 락온
                            qty_to_send = target_cum_qty - filled_qty
                                    
                            if qty_to_send <= 0: continue
                                      
                            if exec_price > 0:
                                if side == "SELL" and qty_to_send > 0:
                                    if version == "V_REV":
                                        if vrev_q_qty <= 0:
                                            qty_to_send = 0
                                        else:
                                            qty_to_send = min(qty_to_send, vrev_q_qty)
                                    else:
                                        if actual_qty <= 0:
                                            qty_to_send = 0
                                        else:
                                            qty_to_send = min(qty_to_send, actual_qty)

                                res = None
                                if qty_to_send > 0:
                                    # 🚨 [Case 47: 자전거래(Wash Trade) 절대 방어막 - V-REV 슬라이싱]
                                    avwap_state_file = f"data/avwap_trade_state_{t}.json"
                                    need_avwap_resume = False
                                    is_overnight = await _retry_api(getattr(cfg, 'get_avwap_overnight_mode', lambda x: False), t, default=False)
                                    avwap_qty_to_restore = 0
                                    avwap_price_to_restore = 0.0
                                    
                                    if side == "BUY":
                                        avwap_state = await _read_state_safe(avwap_state_file, today_hyphen, {})
                                        if avwap_state and avwap_state.get('qty', 0) > 0 and avwap_state.get('sell_odno'):
                                            avwap_sell_odno = avwap_state.get('sell_odno')
                                            logging.info(f"🛡️ [{t}] 자전거래 방어: 암살자 덫({avwap_sell_odno}) 임시 취소 집행")
                                            c_res = await _retry_api(broker.cancel_order, t, avwap_sell_odno, timeout=10.0)
                                            
                                            if isinstance(c_res, dict) and str(c_res.get('rt_cd', '')) == '0':
                                                avwap_state['sell_odno'] = ""
                                                if is_overnight:
                                                    avwap_state['suppress_sell'] = True
                                                    if chat_id and not vwap_cache.get(f"REV_{t}_wash_trade_msg"):
                                                        vwap_cache[f"REV_{t}_wash_trade_msg"] = True
                                                        await _safe_send(context, chat_id, f"🛡️ <b>[{html.escape(t)} 자전거래 방어 가동]</b>\n▫️ 암살자 오버나이트 모드(ON) 확인.\n▫️ 본진 슬라이싱 타격 중 자전거래 차단을 위해 암살자 매도 덫을 임시 수거합니다. (애프터장 재장전 예정)", parse_mode='HTML')
                                                
                                                await _write_state_safe(avwap_state_file, avwap_state)
                                                await asyncio.sleep(1.0)
                                                
                                                if not is_overnight:
                                                    need_avwap_resume = True
                                                    avwap_qty_to_restore = int(_safe_float(avwap_state.get('qty', 0)))
                                                    avwap_avg_price = _safe_float(avwap_state.get('avg_price', 0.0))
                                                    avwap_price_to_restore = math.ceil(avwap_avg_price * 1.01 * 100) / 100.0

                                    res = await _retry_api(broker.send_order, t, side, qty_to_send, exec_price, "LIMIT")

                                    if need_avwap_resume:
                                        logging.info(f"🛡️ [{t}] 자전거래 방어 해제: 암살자 덫 복구(재장전) 집행")
                                        s_res = await _retry_api(broker.send_order, t, "SELL", avwap_qty_to_restore, avwap_price_to_restore, "LIMIT", timeout=15.0)
                                        if isinstance(s_res, dict) and str(s_res.get('rt_cd', '')) == '0':
                                            avwap_state_fresh = await _read_state_safe(avwap_state_file, today_hyphen, {})
                                            avwap_state_fresh['sell_odno'] = str(s_res.get('odno', ''))
                                            await _write_state_safe(avwap_state_file, avwap_state_fresh)
                                else:
                                    logging.warning(f"🚨 [{t}] VWAP 슬라이싱 매도 스킵: 큐/잔고 0주 캡핑 (Ghost-Dumping 방어)")
                                    res = {'rt_cd': '999', 'msg1': '보유 수량 0주 캡핑으로 매도 스킵'}

                                safe_res = res if isinstance(res, dict) else {}
                                if safe_res.get('rt_cd') == '0' and safe_res.get('odno'):
                                    o['last_odno'] = safe_res.get('odno')
                                    o['last_sent_qty'] = qty_to_send
                                    o['last_price'] = exec_price
                                    state_changed = True
                                    logging.info(f"🔪 [{t}] 정밀 요격망(Slicing): {side} {qty_to_send}주 @ ${exec_price:.2f} (누적 {cum_weight*100:.1f}%)")
                                else:
                                    logging.error(f"🚨 [{t}] VWAP 슬라이싱 거절: {safe_res.get('msg1')}")
                                
                        if state_changed:
                            try:
                                await _write_state_safe(slice_file, slice_state)
                            except Exception as e:
                                logging.error(f"🚨 [{t}] 로컬 1분 슬라이싱 엔진 상태 기록 실패 (Atomic Write): {e}")

            except Exception as e:
                logging.error(f"🚨 [{t}] 섀도우 엔진 단일 종목 연산 중 치명적 오류 (Cascade 방어): {e}", exc_info=True)
                continue
