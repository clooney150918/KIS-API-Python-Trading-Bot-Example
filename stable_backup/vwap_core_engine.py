# ==========================================================
# FILE: vwap_core_engine.py
# ==========================================================
# 🚨 RESTORED: [상태 파일 스키마 팩트 교정 및 무지성 스윕 궁극 방어] 로컬 슬라이스 상태 파일(`vrev_slice_state.json`)의 단가 키값은 `price`가 아닌 `target_price`입니다. 이를 오인하여 타점을 $0.0으로 인식, 목표가를 무시하고 시간만 되면 매수와 매도를 동시에 격발하던 치명적 대참사를 원천 봉쇄했습니다. `_safe_float(o.get('target_price', o.get('price', 0.0)))` 듀얼 폴백 맵핑을 통해 100% 타점 요격망을 수복 완료했습니다.
# 🚨 MODIFIED: [자전거래 방어막 기억 상실(Wash Trade Amnesia) 궁극 수술] 오버나이트 모드로 이관된 암살자의 매도 덫이 존재함에도 불구하고, 상태 파일 읽기(`_read_state_safe`)의 엄격한 당일 날짜(`date_str`) 필터링으로 인해 방어막이 암살자를 인식하지 못해(빈 딕셔너리 반환) 본진 매수 시 자전거래 충돌(Reject)이 발생하던 대참사를 원천 봉쇄. `_read_json_ignore_date_sync` 헬퍼를 신규 주입하여 날짜와 무관하게 미체결 덫이 존재하면 100% 무조건 회수하도록 팩트 락온.
# 🚨 MODIFIED: [동기/비동기 스레드 데드락(Deadlock) 궁극 수술] `_read_state_safe` 및 `_write_state_safe` 내부에서 메인 스레드가 `GlobalThrottle.get_file_lock`을 쥔 채 백그라운드 스레드(`_retry_api`)를 호출하고, 백그라운드 스레드 역시 동일한 락을 요구하여 발생하던 '55초 타임아웃 연쇄 폭발'의 주범(교착 상태)을 완벽히 도려냈습니다. I/O 모듈 내부에 이미 락이 결속되어 있으므로 래퍼(Wrapper) 층의 락을 전면 소각했습니다.
# 🚨 MODIFIED: [O(N) API 중복 호출 맹점 궁극 수술] 덫(주문) 루프 내부에 기생하며 TimeoutError를 유발하던 `get_ask_price` 및 `get_bid_price` 중복 호출을 영구 소각. 호가 스캔을 루프 바깥(최상단)으로 전진 배치하여 종목당 단 1회의 호출만으로 모든 덫 타점을 연산하도록 O(1) 진공 압축 팩트 락온 완료.
# 🚨 MODIFIED: [전역 락(tx_lock) 데드락 붕괴 수술 (Case 50 헌법 사수)] `execute_vwap_trade` 함수 전체를 감싸 이벤트 루프를 통째로 마비시키던 `async with tx_lock:` 족쇄를 전면 소각. 오직 주문/취소(`send_order`, `cancel_order`) 및 잔고 스캔 임계 구역(Critical Section)에만 국소적으로 락을 래핑하여 병렬 처리(Parallel Execution) 성능 극대화 완료.
# 🚨 MODIFIED: [자본 잠김 마비(Capital Lock-up Paralysis) 궁극 수술] 자본 잠김으로 인해 매수 플랜이 애프터장으로 이관(pending_aftermarket=True)되었을 때, VWAP 슬라이싱 엔진 전체를 스킵(continue)해버리던 맹독성 버그를 원천 소각. 하방 Gap Hijack만 안전하게 차단하고 매도(SELL) 슬라이싱은 100% 정상 가동되도록 팩트 디커플링 완료.
# 🚨 MODIFIED: [정량제(Fixed-Quantity) 팩트 스윕 락온] Gap Hijack 발동 시 잔여 예산을 억지로 100% 소진하던 맹독성 풀-스윕 로직을 영구 소각하고, 스냅샷에 락온된 '당일 잔여 목표 수량(Remaining Target Qty)'만을 100% 스윕 타격하여 하락장 시드 보존력(Runway)을 극대화함.
# 🚨 NEW: [핑퐁 패러독스(Ping-Pong Paradox) 진공 압축 수술] 매 1분 슬라이싱 틱마다 암살자 덫을 취소하고 재장전하던 O(N) 맹독성 핑퐁 로직을 전면 소각. 첫 타격 시 단 1회만 취소 후 `suppress_sell=True` 팩트 락온으로 억제하여 KIS 서버 I/O 낭비 및 호가 순위 강등을 원천 봉쇄.
# 🚨 NEW: [하이재킹 타점 오염 방어막 (Price Over-Hijack Shield) 결속] 하방 Gap Hijack이 발동되었을 때, 실시간 현재가가 스냅샷 지시서의 최고 매수 목표가보다 비쌀 경우(Buy Low 원칙 위배), 무지성 스윕 매수를 강제로 차단하여 비싸게 타격되는 대참사 완벽 방어.
# 🚨 MODIFIED: [이중 타격(Double Spending) 기억 상실 붕괴 궁극 수술] 슬라이싱 중 사용자의 수동 /sync 개입 등으로 API 타임아웃이 발생하여 KIS 서버에서 체결 원장을 조회하지 못했을 때, 메모리에 저장된 주문번호(`last_odno`)를 강제 삭제(Amnesia)해버려 방금 샀던 수량을 다시 사버리는 '오버슈팅(Overbuying)' 패러독스를 완벽히 도려냈습니다. 이제 원장 조회 실패 시 주문 번호를 유지한 채 안전하게 다음 1분으로 검증을 이연(Delay)합니다.
# ==========================================================
import logging
import asyncio
import math
import time
import datetime
import json
from zoneinfo import ZoneInfo
import html
import functools
import pandas as pd 

from scheduler_core import get_budget_allocation
from state_io_manager import _read_json_safe_sync, _atomic_write_json_sync
from global_throttle import GlobalThrottle # 🚨 전역 통제소 결속

_MCAL_SCHEDULE_CACHE = {}

def _safe_float(val):
    try:
        f_val = float(str(val or 0.0).replace(',', ''))
        if math.isnan(f_val) or math.isinf(f_val):
            return 0.0
        return f_val
    except Exception:
        return 0.0

async def _retry_api(func, *args, timeout=15.0, default=None, **kwargs):
    for attempt in range(3):
        try:
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
    date_str = now_est.strftime('%Y-%m-%d')
    if date_str in _MCAL_SCHEDULE_CACHE:
        return _MCAL_SCHEDULE_CACHE[date_str]

    GlobalThrottle.wait_api_sync()
    import pandas_market_calendars as mcal
    nyse = mcal.get_calendar('NYSE')
    sched = nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
    
    _MCAL_SCHEDULE_CACHE[date_str] = sched
    return sched

async def _get_market_close_time(now_est):
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

def _read_json_ignore_date_sync(filepath):
    with GlobalThrottle.get_file_lock(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}

async def _read_state_safe(filepath, date_str, default_val):
    return await _retry_api(_read_json_safe_sync, filepath, date_str, default=default_val)

async def _write_state_safe(filepath, state_dict):
    return await _retry_api(_atomic_write_json_sync, filepath, state_dict)

async def execute_vwap_init(tx_lock, cfg, broker, chat_id, context, vwap_cache):
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
    t_ask_p = 0.0
    t_bid_p = 0.0
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
                
                t_curr_p = _safe_float(await _retry_api(broker.get_current_price, t))
                t_ask_p = _safe_float(await _retry_api(broker.get_ask_price, t))
                t_bid_p = _safe_float(await _retry_api(broker.get_bid_price, t))
                
                is_capital_locked_now = False
                try:
                    after_state_file = f"data/vrev_aftermarket_state_{t}.json"
                    after_state = await _read_state_safe(after_state_file, today_hyphen, {})
                    if after_state.get('date') == today_hyphen:
                        if any(isinstance(o, dict) and str(o.get('status')) == 'PENDING' for o in after_state.get('orders', [])):
                            is_capital_locked_now = True
                            logging.info(f"⏳ [{t}] 자본 잠김 감지: 애프터장(16:01) 이관 대기 중. 하방 Gap Hijack을 차단하고 매도 슬라이싱만 정상 집행합니다.")
                except Exception as e:
                    logging.error(f"🚨 [{t}] 애프터장 이관 상태 교차 검증 에러: {e}")

                slice_state_disk = await _read_state_safe(slice_file, today_hyphen, {})
                
                if slice_state_disk.get('date') != today_hyphen:
                    if not vwap_cache.get(f"REV_{t}_bypass_warned"):
                        msg = f"🚨 <b>[{html.escape(str(t))}] VWAP 슬라이싱 코어 바이패스(Bypass) 감지!</b>\n▫️ 당일({today_hyphen}) 생성된 지시서(slice_state)를 찾을 수 없습니다.\n▫️ 15:26 스케줄러 병목으로 본진 플랜 연산이 누락된 것으로 추정됩니다.\n▫️ <b>수동 타격 권고:</b> 관제탑( /avwap )에서 <b>[1회분 수동매수/매도]</b> 인라인 버튼을 통해 지연된 타격을 직접 지휘하십시오."
                        await _safe_send(context, chat_id, msg, parse_mode='HTML')
                        vwap_cache[f"REV_{t}_bypass_warned"] = True
                    continue

                disk_hijacked = slice_state_disk.get('hijacked', False)

                is_downward_hijacked_now = vwap_cache.get(f"REV_{t}_gap_hijack_fired", False) or disk_hijacked

                # ======================================================
                # [ 1. Gap Hijack (오직 하방 폭락장 정량제 스윕 감시) ]
                # ======================================================
                if version == "V_REV" and not is_downward_hijacked_now and not is_capital_locked_now:
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
                            
                            if gap_pct <= gap_thresh:
                                slice_state_check = slice_state_disk
                                has_buy_plan = any(isinstance(o, dict) and str(o.get('side')) == 'BUY' for o in slice_state_check.get('orders', []))
                                
                                sell_orders = [o for o in slice_state_check.get('orders', []) if str(o.get('side')) == 'SELL']
                                is_sell_condition = False
                                for o in sell_orders:
                                    tp_val = _safe_float(o.get('target_price', o.get('price', 0.0)))
                                    if tp_val > 0.0 and t_curr_p >= tp_val:
                                        is_sell_condition = True
                                        break
                                
                                # 🚨 NEW: [하이재킹 타점 오염 방어막] 지시서의 최고 매수 목표가 추출
                                buy_orders = [o for o in slice_state_check.get('orders', []) if str(o.get('side')) == 'BUY']
                                max_buy_target = 0.0
                                if buy_orders:
                                    max_buy_target = max(_safe_float(o.get('target_price', o.get('price', 0.0))) for o in buy_orders)

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
                                
                                # 🚨 NEW: 현재가가 스냅샷 매수 목표가보다 높은 경우 갭 하이재킹 차단 (Buy Low 원칙 사수)
                                elif max_buy_target > 0.0 and t_curr_p > max_buy_target:
                                    if not vwap_cache.get(f"REV_{t}_gap_hijack_price_blocked_log", False):
                                        logging.info(f"⚡ [{t}] 하방 Gap Hijack 조건 도달({gap_pct:.2f}%) ➔ 🛑 현재가(${t_curr_p:.2f})가 지시서 매수 목표가(${max_buy_target:.2f})보다 비쌉니다. 비싼 가격에 하이재킹되는 것을 차단하고 매수 단가를 기다립니다 (Buy Low).")
                                        vwap_cache[f"REV_{t}_gap_hijack_price_blocked_log"] = True
                                        
                                else:
                                    vwap_cache.pop(f"REV_{t}_gap_hijack_sell_blocked_log", None)
                                    vwap_cache.pop(f"REV_{t}_gap_hijack_price_blocked_log", None)
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
                                                async with tx_lock:
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
                                                    async with tx_lock:
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

                                    buy_qty = 0
                                    for ox in slice_state_disk.get('orders', []):
                                        if str(ox.get('side')) == 'BUY':
                                            _tot = int(_safe_float(ox.get('total_qty', 0)))
                                            _fil = int(_safe_float(ox.get('filled_qty', 0)))
                                            if _tot - _fil > 0:
                                                buy_qty += (_tot - _fil)

                                    exec_price = t_ask_p if t_ask_p > 0 else t_curr_p
                                    
                                    if buy_qty > 0:
                                        async with tx_lock:
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
                                                logging.error(f"🚨 [{t}] 하이재킹 체결 후 로컬 지시서 수량 만기 처리 에러: {e}")

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
                        target_price = _safe_float(o.get('target_price', o.get('price', 0.0)))
                        side = str(o.get('side', 'BUY'))
                        last_odno = str(o.get('last_odno', ''))
                        
                        if is_state_hijacked and side == 'BUY':
                            continue
                        
                        if filled_qty >= total_qty and not last_odno:
                            continue
                        
                        ccld_qty_this_tick = 0
                        if last_odno:
                            cancel_successful = False
                            async with tx_lock:
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
                                now_kst_fresh = datetime.datetime.now(ZoneInfo('Asia/Seoul'))
                                kis_search_start_fresh = (now_kst_fresh - datetime.timedelta(days=2)).strftime('%Y%m%d')
                                query_end_dt_fresh = now_kst_fresh.strftime('%Y%m%d')
                                
                                _execs = await _retry_api(broker.get_execution_history, t, kis_search_start_fresh, query_end_dt_fresh, default=None)
                                
                                # 🚨 MODIFIED: [이중 타격 방어망 결속] KIS 체결 원장 지연 및 API 타임아웃 시, 주문을 잊어버리지 않고 이연(Delay)하여 이중 타격 방어
                                if _execs is None:
                                    logging.warning(f"🚨 [{t}] 체결 원장 API 조회 실패(Timeout). 기억 상실(Amnesia) 방어를 위해 다음 분으로 이연합니다.")
                                    continue
                                
                                _safe_execs = _execs if isinstance(_execs, list) else []
                                _filled_rec = next((ex for ex in _safe_execs if isinstance(ex, dict) and str(ex.get('odno', '')) == last_odno), None)
                                
                                if _filled_rec:
                                    ccld_qty_this_tick = int(_safe_float(_filled_rec.get('ft_ccld_qty')))
                                    real_exec_price = _safe_float(_filled_rec.get('ft_ccld_unpr3'))
                                    if real_exec_price == 0.0: real_exec_price = target_price
                                else:
                                    # 🚨 MODIFIED: 취소 불가 & 미체결 없음 & 원장 조회 안됨 = 명백한 KIS 서버 Lag
                                    if not cancel_successful:
                                        logging.warning(f"🚨 [{t}] 미체결에도 없고 원장에도 없음. KIS 랙(Lag) 감지. 이중 타격 방어를 위해 다음 분으로 이연합니다.")
                                        continue
                                        
                                    ccld_qty_this_tick = 0
                                    real_exec_price = 0.0
                            except Exception as e:
                                logging.error(f"🚨 [{t}] 자체 슬라이싱 체결 원장 교차 검증 에러: {e}")
                                # 🚨 에러 발생 시에도 다음 틱으로 안전하게 이연
                                continue
                            
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
                            exec_price = t_ask_p
                        else:
                            exec_price = t_bid_p
                                
                        if exec_price <= 0.0:
                            exec_price = t_curr_p

                        if target_price > 0.0:
                            is_target_hit = False
                            if side == "BUY" and exec_price <= target_price:
                                is_target_hit = True
                            elif side == "SELL" and exec_price >= target_price:
                                is_target_hit = True

                            if not is_target_hit:
                                continue 
                                
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
                                avwap_state_file = f"data/avwap_trade_state_{t}.json"
                                
                                if side == "BUY":
                                    avwap_state = await _retry_api(_read_json_ignore_date_sync, avwap_state_file)
                                    if avwap_state and avwap_state.get('qty', 0) > 0 and avwap_state.get('sell_odno'):
                                        avwap_sell_odno = avwap_state.get('sell_odno')
                                        logging.info(f"🛡️ [{t}] 자전거래 방어: 암살자 덫({avwap_sell_odno}) 임시 취소 집행 및 핑퐁 패러독스 차단")
                                        async with tx_lock:
                                            c_res = await _retry_api(broker.cancel_order, t, avwap_sell_odno, timeout=10.0)
                                        
                                        if isinstance(c_res, dict) and str(c_res.get('rt_cd', '')) == '0':
                                            avwap_state['sell_odno'] = ""
                                            avwap_state['suppress_sell'] = True
                                            if chat_id and not vwap_cache.get(f"REV_{t}_wash_trade_msg"):
                                                vwap_cache[f"REV_{t}_wash_trade_msg"] = True
                                                await _safe_send(context, chat_id, f"🛡️ <b>[{html.escape(t)} 자전거래 방어 및 핑퐁 패러독스 차단 가동]</b>\n▫️ 본진 슬라이싱 타격 중 자전거래 및 무한 재장전 병목을 막기 위해 암살자 매도 덫을 일괄 수거(suppress_sell=True)합니다. (애프터장 재장전 예정)", parse_mode='HTML')
                                            
                                            await _write_state_safe(avwap_state_file, avwap_state)
                                            await asyncio.sleep(1.0)
                                            
                                async with tx_lock:
                                    res = await _retry_api(broker.send_order, t, side, qty_to_send, exec_price, "LIMIT")
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
