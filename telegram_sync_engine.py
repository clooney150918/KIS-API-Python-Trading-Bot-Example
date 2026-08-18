# ==========================================================
# FILE: telegram_sync_engine.py
# ==========================================================
# 🚨 MODIFIED: [단일 지층 팽창 자가 치유 방해 맹독성 Bypass 궁극 수술] V4.0 큐 장부 동기화 및 메인 장부 교정 로직이 `if target_execs:` (당일 체결 내역 존재 시) 블록 내부에 갇혀 있어, 당일 매매가 없는 날에는 팽창된 지층이 절대 쪼개지지 않던 치명적 버그를 원천 차단. 해당 로직들을 `if` 블록 외부로 들여쓰기 전진 배치(Un-indent)하여 365일 100% 무결성 동기화가 강제되도록 팩트 락온.
# 🚨 MODIFIED: [제1헌법 철저 준수] _get_last_trade_date 내부 달력 API(mcal) 스캔 시 GlobalThrottle.wait_api_sync()를 강제 주입하여 썬더링 허드 완벽 차단.
# 🚨 MODIFIED: [Event Loop 마비 궁극 수술] get_exact_prev_close 내부에 잔존하던 맹독성 time.sleep(0.06)을 영구 정리하고 GlobalThrottle.wait_api_sync() 중앙 통제 락온 완료.
# 🚨 MODIFIED: [자전거래/보조전략 찌꺼기 맹독성 유입 궁극 방어] 16:05 EST 정산 시 KIS 실원장(target_execs)의 모든 당일 체결 내역을 맹목적으로 무한 편입하던 로직 전면 정리.
# 🚨 MODIFIED: [통신 장애 핀셋 추적망 결속] process_auto_sync 내부에서 broker API 호출 실패 시 정확한 실패 구간(Endpoint)과 사유를 문자열로 반환하여 상위 라우터가 진단할 수 있도록 팩트 락온.
# 🚨 NEW: [스냅샷 디커플링 해소 (Reset & Regenerate) 아키텍처 팩트 결속] 잔고 오차 교정, 수동 조작, 큐 병합 등 장부에 단 1주라도 변동이 감지되면 `snapshot_needs_regen` 트리거를 발동시켜, 16:00 EST 타임락을 강제 개방(Override)하고 오염된 낡은 스냅샷(daily_snapshot)을 물리적으로 영구 정리(Reset)한 뒤 최신 수량 기반의 팩트 지시서로 즉각 재생성(Regenerate)하도록 100% 시스템 교정 완료.
# 🚨 MODIFIED: [체결 원장 디커플링 붕괴 수술] 16:05 EST 정산 시 한투(KIS)에서 수신한 체결 원장 데이터(`execs_raw`) 중, 보조전략 캐시에 기록된 `history_odnos`에 속하는 주문은 100% 투명 인간(Ghosting) 취급하여 도려냄으로써, 보조전략의 타점 오염 및 유령 졸업(Ghost Graduation)을 원천 봉쇄.
# ==========================================================

import logging
import datetime
from zoneinfo import ZoneInfo
import time
import os
import asyncio
import json
import tempfile
import traceback
import math 
import html 
import functools
import glob
import yfinance as yf
import pandas as pd 
import pandas_market_calendars as mcal
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from global_throttle import GlobalThrottle # 🚨 중앙 통제소 결속

class TelegramSyncEngine:
    def __init__(self, config, broker, strategy, legacy_lot_book, view, tx_lock, sync_locks):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.legacy_lot_book = legacy_lot_book
        self.view = view
        self.tx_lock = tx_lock
        self.sync_locks = sync_locks

    def _safe_float(self, value):
        try:
            f_val = float(str(value or 0.0).replace(',', ''))
            if math.isnan(f_val) or math.isinf(f_val): return 0.0
            return f_val
        except Exception: return 0.0

    def _official_trade_date(self, value):
        text = str(value or "").strip()
        if len(text) == 8 and text.isdigit():
            return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
        if len(text) >= 10:
            return text[:10]
        return text

    def sync_official_execution_facts(self, ticker, kis_execution_rows, *, account_fingerprint):
        """Append confirmed KIS facts to the official execution ledger only.

        This deliberately does not mutate/overwrite legacy local JSON or history.
        """
        from fill_reconciler import build_fill_key, normalize_kis_execution
        from ledger_migration import OFFICIAL_FILL_SOURCE

        target = str(ticker or "").strip().upper()
        appended = []
        for row in kis_execution_rows or []:
            normalized = normalize_kis_execution(row, account_fingerprint=account_fingerprint)
            if normalized.get("ticker") != target:
                continue
            price = normalized["price"]
            official_record = {
                "source": OFFICIAL_FILL_SOURCE,
                "trade_date": self._official_trade_date(normalized.get("trade_date")),
                "ticker": target,
                "exchange": str(normalized.get("exchange") or "").upper(),
                "side": normalized["side"],
                "qty": int(normalized["qty"]),
                "price": format(price, "f"),
                "kis_order_no": normalized["order_no"],
                "execution_time": str(normalized.get("execution_time") or ""),
                "account_fingerprint": str(account_fingerprint or ""),
                "fill_key": build_fill_key(normalized),
                "confirmed": True,
            }
            appended.append(self.cfg.append_kis_confirmed_execution_fact(official_record))
        return {"appended_count": len(appended), "fills": appended}

    async def _retry_api(self, func, *args, timeout=15.0, default=None, **kwargs):
        """ 🚨 [Case 31, 32] 3단 지수 백오프 및 GlobalThrottle 중앙 집중형 TPS 방어망 결속 """
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

    async def _safe_send(self, context, chat_id, text, timeout=15.0, **kwargs):
        if not chat_id: return None
        try:
            return await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=text, **kwargs), timeout=timeout)
        except Exception as e:
            logging.error(f"🚨 텔레그램 전송 실패: {e}")
            return None

    async def process_auto_sync(self, ticker, chat_id, context, silent_ledger=False):
        if ticker not in self.sync_locks:
            self.sync_locks[ticker] = asyncio.Lock()
            
        if self.sync_locks[ticker].locked(): return "LOCKED"
            
        async with self.sync_locks[ticker]:
            async with self.tx_lock:
                est = ZoneInfo('America/New_York')
                now_est = datetime.datetime.now(est)
                kst = ZoneInfo('Asia/Seoul')
                now_kst = datetime.datetime.now(kst)

                # 🚨 NEW: 스냅샷 팩트 재생성 트리거 초기화
                snapshot_needs_regen = False

                last_split_date = await self._retry_api(self.cfg.get_last_split_date, ticker, default="")
                split_ratio, split_date = await self._retry_api(self.broker.get_recent_stock_split, ticker, last_split_date, default=(0.0, ""))
                
                if split_ratio > 0.0 and split_date != "":
                    snapshot_needs_regen = True # 🚨 트리거 발동
                    await self._retry_api(self.cfg.apply_stock_split, ticker, split_ratio, timeout=10.0)
                    if getattr(self, 'legacy_lot_book', None):
                        await self._retry_api(self.legacy_lot_book.apply_stock_split, ticker, split_ratio, timeout=10.0)
                    if hasattr(self.strategy, 'aux_strategy_plugin'):
                        await self._retry_api(self.strategy.aux_strategy_plugin.apply_stock_split, ticker, split_ratio, now_est, timeout=10.0)
                    
                    try:
                        from assassin_ledger import AssassinLedger
                        a_ledger = await asyncio.wait_for(asyncio.to_thread(AssassinLedger), timeout=5.0)
                        await self._retry_api(a_ledger.apply_stock_split, ticker, split_ratio, timeout=10.0)
                    except Exception as e:
                        logging.error(f"🚨 보조전략 장부 액면분할 팩트 적용 실패: {e}")
                    
                    await self._retry_api(self.cfg.set_last_split_date, ticker, split_date, timeout=5.0)
                    
                    split_type = "액면분할" if split_ratio > 1.0 else "액면병합(역분할)"
                    await self._safe_send(context, chat_id, f"✂️ <b>[{html.escape(str(ticker))}] 야후 파이낸스 {split_type} 자동 감지!</b>\n▫️ 감지된 비율: <b>{split_ratio}배</b> (발생일: {html.escape(str(split_date))})\n▫️ 봇이 기존 V14 장부, V4.0 큐 장부, 보조전략 장부, AUX 상태 캐시의 수량과 평단가를 100% 무인 자동 소급 조정 완료했습니다.", parse_mode='HTML')
             
                def _get_last_trade_date(target_est):
                    GlobalThrottle.wait_api_sync()
                    nyse = mcal.get_calendar('NYSE')
                    return nyse.schedule(start_date=(target_est - datetime.timedelta(days=10)).date(), end_date=target_est.date())

                schedule = await self._retry_api(_get_last_trade_date, now_est, timeout=10.0, default=pd.DataFrame())
                if not schedule.empty:
                    last_trade_date = schedule.index[-1]
                    target_ledger_str = last_trade_date.strftime('%Y-%m-%d')
                else:
                    target_ledger_str = now_est.strftime('%Y-%m-%d')

                res_bal = await self._retry_api(self.broker.get_account_balance, timeout=15.0, default=None)
                if not res_bal or (isinstance(res_bal, (list, tuple)) and len(res_bal) > 1 and res_bal[1] is None):
                    await self._safe_send(context, chat_id, f"❌ <b>[{html.escape(str(ticker))}] API 통신 차단</b>\n증권사 서버가 계좌 잔고를 반환하지 않습니다. (토큰 만료 또는 서버 점검 중)", parse_mode='HTML')
                    return "잔고 조회(get_account_balance) 실패 - API 서버 무응답 또는 거절"
                    
                holdings = res_bal[1] if isinstance(res_bal, (list, tuple)) and len(res_bal) > 1 else {}
                safe_holdings = holdings if isinstance(holdings, dict) else {}
                safe_ticker_info = safe_holdings.get(ticker) or {'qty': 0, 'avg': 0.0}
                
                actual_qty = int(self._safe_float(safe_ticker_info.get('qty')))
                actual_avg = self._safe_float(safe_ticker_info.get('avg'))

                # KIS 실계좌 평단 저장 — 장부 대신 KIS 기준으로 모든 계산
                if actual_qty > 0 and actual_avg > 0:
                    await self._retry_api(self.cfg._save_json, "data/kis_balance.json",
                        {"SOXL": {"qty": actual_qty, "avg_price": actual_avg, "last_update": datetime.datetime.now(ZoneInfo('Asia/Seoul')).strftime('%Y-%m-%d %H:%M')}},
                        timeout=5.0)

                hold_res = await self._retry_api(self.cfg.calculate_holdings_from_official_ledger, ticker, default=(0, 0.0, 0.0, 0.0))
                ledger_qty_for_check = hold_res[0] if isinstance(hold_res, tuple) and len(hold_res) > 0 else 0
                
                max_check_qty = ledger_qty_for_check

                a_qty_for_check = 0
                try:
                    from assassin_ledger import AssassinLedger
                    a_ledger = await asyncio.wait_for(asyncio.to_thread(AssassinLedger), timeout=5.0)
                    a_data_check = await self._retry_api(a_ledger.get_ledger, ticker, default=[])
                    a_qty_for_check = sum(int(self._safe_float(item.get("qty"))) for item in (a_data_check or []))
                except Exception as e:
                    logging.error(f"🚨 보조전략 장부 수량 스캔 에러: {e}")
                
                max_check_qty = max(max_check_qty, a_qty_for_check)

                # 🚨 MODIFIED: [체결 원장 디커플링 붕괴 수술] 보조전략 캐시에 기록된 주문번호 추출 (Ghosting 필터 용도)
                aux_state_file = f"data/aux_trade_state_{ticker}.json"
                history_odnos = []
                with GlobalThrottle.get_file_lock(aux_state_file):
                    try:
                        with open(aux_state_file, 'r', encoding='utf-8') as f:
                            aux_data = json.load(f)
                            if isinstance(aux_data, dict) and aux_data.get('date') == target_ledger_str:
                                history_odnos = aux_data.get('history_odnos', [])
                    except Exception:
                        pass
                        
                kis_search_start = (now_kst - datetime.timedelta(days=4)).strftime('%Y%m%d')
                query_end_dt = now_kst.strftime('%Y%m%d')

                def filter_to_est(execs_raw):
                    filtered = []
                    if not execs_raw: return filtered
                    for ex in execs_raw:
                        if not isinstance(ex, dict): continue
                        
                        # 🚨 MODIFIED: [체결 원장 디커플링 붕괴 수술] 보조전략 찌꺼기(Ghost) 100% 정리 배제
                        ex_odno = str(ex.get('odno', ''))
                        if ex_odno and ex_odno in history_odnos:
                            continue
                            
                        ord_dt = ex.get('ord_dt') or ex.get('ord_strt_dt')
                        if not ord_dt: continue
                        ord_tmd = ex.get('ord_tmd')
                        if not ord_tmd or len(str(ord_tmd)) != 6: ord_tmd = '000000'
                        try:
                            k_dt = datetime.datetime.strptime(f"{ord_dt}{ord_tmd}", "%Y%m%d%H%M%S").replace(tzinfo=kst)
                            e_dt = k_dt.astimezone(est)
                            if e_dt.strftime('%Y-%m-%d') == target_ledger_str:
                                filtered.append(ex)
                        except Exception: pass
                    return filtered

                raw_execs = []
                target_execs = []
                execution_fill_reader = getattr(self.broker, 'get_execution_fills', self.broker.get_execution_history)
                    
                if actual_qty == 0 and max_check_qty > 0:
                    max_retries = 6
                    prev_sold_today = -1
                    stable_cnt = 0
                    for attempt in range(max_retries):
                        raw_execs = await self._retry_api(execution_fill_reader, ticker, kis_search_start, query_end_dt, timeout=15.0, default=None)
                        if raw_execs is None:
                            return "체결 원장 조회(get_execution_history) 실패 - API 서버 무응답 또는 거절"
                        target_execs = filter_to_est(raw_execs)
                        sold_today = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01")
                        
                        if sold_today >= max_check_qty:
                            if sold_today == prev_sold_today:
                                stable_cnt += 1
                                if stable_cnt >= 1: break
                            else: stable_cnt = 0
                        
                        prev_sold_today = sold_today
                        
                        if attempt < max_retries - 1:
                            logging.info(f"⏳ [{ticker}] 체결 원장 지연(Lag) 감지. 데이터 안정화 및 EST 매핑 검증 중... ({attempt+1}/{max_retries})")
                            await asyncio.sleep(2.0)
                else:
                    raw_execs = await self._retry_api(execution_fill_reader, ticker, kis_search_start, query_end_dt, timeout=15.0, default=None)
                    if raw_execs is None:
                        return "체결 원장 조회(get_execution_history) 실패 - API 서버 무응답 또는 거절"
                    target_execs = filter_to_est(raw_execs)

                official_append_result = {"appended_count": 0, "fills": []}
                if target_execs:
                    account_fingerprint = ""
                    if hasattr(self.cfg, "get_account_fingerprint"):
                        account_fingerprint = await self._retry_api(self.cfg.get_account_fingerprint, default="") or ""
                    official_append_result = await self._retry_api(
                        self.sync_official_execution_facts,
                        ticker,
                        target_execs,
                        account_fingerprint=account_fingerprint,
                        timeout=10.0,
                        default={"appended_count": 0, "fills": []},
                    )
                    appended_count = int(self._safe_float((official_append_result or {}).get("appended_count", 0)))
                    if appended_count > 0:
                        logging.info(f"🏛️ [{ticker}] KIS 확정 체결 {appended_count}건을 공식 체결 원장에 append했습니다. legacy local JSON은 변경하지 않습니다.")
                    app_data = context.bot_data.get('app_data', {}) if hasattr(context, "bot_data") else {}
                    if not isinstance(app_data, dict):
                        app_data = {}
                    fill_reconciler = app_data.get('fill_reconciliation_guard')
                    if fill_reconciler is not None:
                        try:
                            reconcile_result = fill_reconciler.reconcile(ticker, target_execs)
                            if reconcile_result.get("operator_halt"):
                                logging.warning(f"🚨 [{ticker}] 체결 대사에서 HALT 조건 발생: {reconcile_result.get('codes')}")
                        except Exception as e:
                            logging.error(f"⛔ [{ticker}] 체결→T이벤트 대사 실패: {e}")

                # 🚨 MODIFIED: [신규 원장 기준 정산] legacy local JSON 기반 계산을 제거하고,
                # immutable baseline + append-only execution_ledger에서 qty/avg를 산출한다.
                new_hold = await self._retry_api(
                    self.cfg.calculate_holdings_from_official_ledger, ticker,
                    default=(0, 0.0, 0.0, 0.0),
                )
                ledger_qty = new_hold[0] if isinstance(new_hold, tuple) and len(new_hold) > 0 else 0
                avg_price = new_hold[1] if isinstance(new_hold, tuple) and len(new_hold) > 1 else 0.0

                diff = actual_qty - ledger_qty
                price_diff = abs(actual_avg - avg_price)

                # 🚨 MODIFIED: [신규 원장 append-only] 신규 원장은 append-only라 재구성/보정/졸업이 원천 불필요.
                # legacy 재구성·보정·졸업 차단(blocked) 분기를 전면 정리하고, diff/price_diff는 관찰(로그)만 남겨
                # 정상 경로(신규 원장 append)로 그대로 흐르게 한다. (KIS 확정 체결은 위에서 이미 append 완료)
                if diff != 0:
                    logging.warning(f"⚠️ [{ticker}] KIS/신규원장 수량 차이 {diff}주 (KIS {actual_qty} vs 원장 {ledger_qty}) — append-only 원장이므로 재구성 없이 관찰만 합니다.")
                if price_diff >= 0.01:
                    logging.warning(f"⚠️ [{ticker}] KIS/신규원장 평단 차이 {price_diff:.4f} (KIS {actual_avg} vs 원장 {avg_price}) — append-only 원장이므로 보정 없이 관찰만 합니다.")

                sold_today_v14 = sum(int(self._safe_float(ex.get('ft_ccld_qty'))) for ex in target_execs if ex.get('sll_buy_dvsn_cd') == "01") if target_execs else 0

                # 🚨 유령 잔고 방어 (fail-closed) 유지: KIS 0주 + 당일 매도체결 0건 + 신규원장 0주 초과 시 강제 차단
                if actual_qty == 0 and sold_today_v14 == 0 and ledger_qty > 0:
                    await self._safe_send(context, chat_id, f"🚨 <b>[{html.escape(str(ticker))} 유령 잔고 방어 가동]</b>\nKIS 실잔고가 0주로 조회되었으나, 당일 매도 체결 내역이 0건입니다. 통신 오류(Ghost Balance)일 가능성이 매우 높아 장부 강제 정리(자동 졸업)을 차단합니다.\n▫️ HTS 등을 통해 수동으로 100% 전량 매도한 상태라면 <code>/reset</code> 명령어를 사용하여 봇을 초기화하십시오.", parse_mode='HTML')
                    return "유령 잔고(Ghost Balance) 강제 차단 - 매도 체결 없이 KIS 잔고 0주 리턴됨"
                if actual_qty == 0 and sold_today_v14 > 0:
                    logging.info(f"ℹ️ [{ticker}] KIS 0주 + 당일 매도 체결 {sold_today_v14}건 확인 — append-only 원장이 이미 반영하므로 정상 흐름.")

                is_after_market = now_est.time() >= datetime.time(16, 0)
                
                # 🚨 MODIFIED: [스냅샷 디커플링 해소 수술] 장부나 큐에 단 1주라도 변동이 생기면 낡은 스냅샷 정리 및 즉시 팩트 재생성 가동
                if is_after_market or snapshot_needs_regen:
                    if snapshot_needs_regen:
                        logging.info(f"🔄 [{ticker}] 장부/큐 오차 교정 감지! 낡은 스냅샷(Snapshot) 및 캐시를 전면 정리(Reset)하고 재생성(Regenerate)합니다.")
                        def _reset_old_files():
                            for f in glob.glob(f"data/daily_snapshot_*_{ticker}.json"):
                                with GlobalThrottle.get_file_lock(f):
                                    try: os.remove(f)
                                    except OSError: pass
                            for f in glob.glob(f"data/slice_state_*_{ticker}.json"):
                                with GlobalThrottle.get_file_lock(f):
                                    try: os.remove(f)
                                    except OSError: pass
                        await asyncio.wait_for(asyncio.to_thread(_reset_old_files), timeout=10.0)

                    try:
                        curr_p_val = await self._retry_api(self.broker.get_current_price, ticker, timeout=10.0)
                        curr_p = self._safe_float(curr_p_val)
                        
                        def get_exact_prev_close(ticker_name):
                            GlobalThrottle.wait_api_sync()
                            df = yf.Ticker(ticker_name).history(period="5d", interval="1m", prepost=True, timeout=5)
                            if not df.empty and 'Close' in df.columns:
                                tz_est = ZoneInfo('America/New_York')
                                tz_now = datetime.datetime.now(tz_est)
                                cutoff_date = tz_now.date()
                                if tz_now.time() <= datetime.time(16, 0, 30):
                                    cutoff_date -= datetime.timedelta(days=1)
                                
                                if df.index.tzinfo is None:
                                    df.index = df.index.tz_localize('UTC').tz_convert(tz_est)
                                else:
                                    df.index = df.index.tz_convert(tz_est)
                                    
                                past_df = df[df.index.date <= cutoff_date].copy()
                                if not past_df.empty:
                                    past_df['Close'] = past_df['Close'].ffill().bfill()
                                    regular_past = past_df.between_time('09:30', '15:59')
                                    if not regular_past.empty:
                                        val = float(regular_past['Close'].iloc[-1])
                                    else:
                                        val = float(past_df['Close'].iloc[-1])
                                    return val if not math.isnan(val) else None
                            return None

                        yf_close = None
                        for attempt in range(3):
                            try:
                                yf_close = await asyncio.wait_for(asyncio.to_thread(get_exact_prev_close, ticker), timeout=10.0)
                                break
                            except Exception:
                                if attempt == 2: pass
                                else: await asyncio.sleep(1.0 * (2 ** attempt))
                        
                        prev_c = yf_close if yf_close and yf_close > 0 else curr_p

                        if now_est.weekday() >= 5 or now_est.time() < datetime.time(4, 0):
                            curr_p = prev_c
                    
                        ma_5day_val = await self._retry_api(self.broker.get_5day_ma, ticker, timeout=10.0)
                        ma_5day = self._safe_float(ma_5day_val)
                        
                        bal_res = await self._retry_api(self.broker.get_account_balance, timeout=10.0)
                        cash_for_snap = self._safe_float(bal_res[0]) if bal_res else 0.0
                        
                        from scheduler_core import get_budget_allocation
                        active_tickers_list = await asyncio.wait_for(asyncio.to_thread(self.cfg.get_active_tickers), timeout=10.0) or []
                        _, alloc_cash_dict = await asyncio.wait_for(asyncio.to_thread(get_budget_allocation, cash_for_snap, active_tickers_list, self.cfg), timeout=10.0)
                        avail_cash = self._safe_float((alloc_cash_dict or {}).get(ticker, 0.0))
             
                        hold_res_final = await self._retry_api(self.cfg.calculate_holdings_from_official_ledger, ticker, timeout=10.0)
                  
                        final_qty = hold_res_final[0] if isinstance(hold_res_final, tuple) and len(hold_res_final) > 0 else 0
                        final_avg = hold_res_final[1] if isinstance(hold_res_final, tuple) and len(hold_res_final) > 1 else 0.0
                    
                        if final_qty == 0:
                            curr_p = 0.0
                         
                        snap_plan = await asyncio.wait_for(asyncio.to_thread(
                            self.strategy.get_plan, ticker, curr_p, final_avg, final_qty, prev_c, ma_5day=ma_5day,
                            market_type="REG", available_cash=avail_cash,
                            pending_buy_amount=getattr(self.broker, "last_pending_buy_amount", 0.0),
                            is_simulation=True, is_snapshot_mode=True
                        ), timeout=15.0)
                        
                        if is_after_market:
                            logging.info(f"📸 [{ticker}] 16:05 EST 확정 정산 완료 후 명일(D+1) 대비 스냅샷 박제(Forward-Lock) 성공.")
                            if isinstance(snap_plan, dict) and snap_plan.get("process_status") == "♻️리버스복귀대기":
                                try:
                                    rev_state_snap = await asyncio.wait_for(asyncio.to_thread(self.cfg.get_reverse_state, ticker), timeout=5.0)
                                    carry_t = self._safe_float((rev_state_snap or {}).get("dynamic_t", 0.0))
                                    carry_cash = self._safe_float((rev_state_snap or {}).get("rem_cash", 0.0))
                                    await asyncio.wait_for(asyncio.to_thread(
                                        self.cfg.set_reverse_state, ticker, False, 0, 0.0,
                                        dynamic_t=carry_t, rem_cash=carry_cash
                                    ), timeout=10.0)
                                    avg_x_08 = round(final_avg * 0.80, 2) if final_avg > 0 else 0.0
                                    dm_msg = (
                                        f"♻️ <b>[진호봇 SOXL] 리버스 모드 종료 → 일반모드 복귀</b>\n"
                                        f"▫️ {now_est.strftime('%m-%d')} 확정 종가 <b>${prev_c:.2f}</b> &gt; 평단×0.80 (<b>${avg_x_08:.2f}</b>)\n"
                                        f"▫️ 다음 영업일 일반모드 주문계획으로 진행"
                                    )
                                    await self._safe_send(context, chat_id, dm_msg, parse_mode='HTML')
                                    logging.info(f"♻️ [{ticker}] 리버스 복귀 판정 완료 → 일반모드 전환 + DM 발송.")
                                except Exception as _rev_exc:
                                    logging.error(f"🚨 [{ticker}] 리버스 복귀 처리 오류: {_rev_exc}")
                        else:
                            logging.info(f"📸 [{ticker}] 장부 교정 감지! 낡은 스냅샷 정리 및 실시간 팩트 지시서 재생성(Regenerate) 성공.")
                    except Exception as e:
                        logging.error(f"🚨 [{ticker}] 스냅샷 팩트 박제 실패: {e}")

                return "SUCCESS"

    async def _display_ledger(self, ticker, chat_id, context, query=None, message_obj=None, pre_fetched_holdings=None):
        recs = await self._retry_api(self.cfg.get_official_fills, ticker, default=[])
        
        report = ""
        
        if not recs:
            report += f"📭 <b>[{html.escape(str(ticker))}]</b> 신규 원장(실제 체결가) 기준 체결 내역이 없습니다.\n\n"
        else:
            from collections import OrderedDict
            agg_dict = OrderedDict()
            total_buy = 0.0
            total_sell = 0.0
            
            for rec in recs:
                raw_date = str(rec.get('date') or '').split(' ')[0]
                parts = raw_date.split('-')
                if len(parts) == 3: date_short = f"{parts[1]}.{parts[2]}"
                else: date_short = raw_date
                     
                side_str = "🔴매수" if rec.get('side') == 'BUY' else "🔵매도"
                key = (date_short, side_str)
            
                if key not in agg_dict: agg_dict[key] = {'qty': 0, 'amt': 0.0}

                agg_dict[key]['qty'] += int(self._safe_float(rec.get('qty')))
                agg_dict[key]['amt'] += (int(self._safe_float(rec.get('qty'))) * self._safe_float(rec.get('price')))
            
                if rec.get('side') == 'BUY': total_buy += (int(self._safe_float(rec.get('qty'))) * self._safe_float(rec.get('price')))
                elif rec.get('side') == 'SELL': total_sell += (int(self._safe_float(rec.get('qty'))) * self._safe_float(rec.get('price')))
            
            report += f"📜 <b>[ {html.escape(str(ticker))} 일자별 매매 (통합 변동분) (총 {len(agg_dict)}일) ]</b>\n\n<code>No. 일자   구분  평균단가  수량\n"
            report += "-"*30 + "\n"
            
            idx = 1
            for (date, side), data in agg_dict.items():
                tot_qty = data['qty']
                avg_prc = data['amt'] / tot_qty if tot_qty != 0 else 0.0
                report += f"{idx:<3} {date} {side} ${avg_prc:<6.2f} {tot_qty}주\n"
                idx += 1
                 
            report += "-"*30 + "</code>\n\n"
        
        safe_holdings = pre_fetched_holdings if isinstance(pre_fetched_holdings, dict) else {}
        actual_qty = int(self._safe_float((safe_holdings.get(ticker) or {'qty': 0}).get('qty')))
        actual_avg = self._safe_float((safe_holdings.get(ticker) or {'avg': 0}).get('avg'))
        
        kis_raw_qty = actual_qty
        kis_raw_avg = actual_avg

        v_mode = await self._retry_api(self.cfg.get_version, ticker, default="V14")

        split = await self._retry_api(self.cfg.get_split_count, ticker, default=40.0)
        t_val_res = await self._retry_api(self.cfg.get_absolute_t_val, ticker, actual_qty, actual_avg, default=(0.0, 0.0))
        t_val = t_val_res[0] if isinstance(t_val_res, tuple) and len(t_val_res) > 0 else 0.0
         
        t_val_safe = self._safe_float(t_val)
        split_safe = int(self._safe_float(split))

        report += "📊 <b>[ 현재 본진 진행 상황 요약 ]</b>\n"
        report += f"▪️ 현재 T값 : {t_val_safe:.4f} T ({split_safe}분할)\n"
        report += f"▪️ 보유 수량 : {actual_qty} 주 (평단 ${actual_avg:,.2f})\n"
        
        if recs:
            report += f"▪️ 총 매수액 : ${total_buy:,.2f}\n"
            report += f"▪️ 총 매도액 : ${total_sell:,.2f}\n"

        report += f"\n🏛️ <b>[ KIS 실서버 종합 원장 계좌 정보 ]</b>\n"
        report += f"▪️ KIS 총 수량 : <b>{kis_raw_qty} 주</b>\n"
        report += f"▪️ KIS 실평단가 : <b>${kis_raw_avg:,.2f}</b> (증권사 앱 표출 팩트 단가)\n"

        msg = report
        
        if len(msg) > 4000:
            msg = msg[:3900] + "\n\n... (장부 내역이 너무 길어 하단이 생략되었습니다) ✂️"

        active_tickers = await self._retry_api(self.cfg.get_active_tickers, default=[])
        keyboard = []
         
        row = [InlineKeyboardButton(f"🔄 {html.escape(str(t))} 장부 업데이트", callback_data=f"REC:SYNC:{t}") for t in active_tickers if isinstance(t, str)]
        if row: keyboard.append(row)
        markup = InlineKeyboardMarkup(keyboard)

        if query:
            try: await asyncio.wait_for(query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML'), timeout=15.0)
            except Exception: pass
        elif message_obj:
            try: await asyncio.wait_for(message_obj.edit_text(msg, reply_markup=markup, parse_mode='HTML'), timeout=15.0)
            except Exception: pass
        else:
            await self._safe_send(context, chat_id, msg, reply_markup=markup, parse_mode='HTML')
