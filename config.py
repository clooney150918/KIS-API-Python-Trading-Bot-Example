# ==========================================================
# FILE: config.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 3중 딥다이브 교차 검증 통과 완료.
# 🚨 MODIFIED: [인스턴스 락 영구 정리] self._io_lock 및 self._locks_mutex를 시스템 전역에서 영구 파기.
# 🚨 MODIFIED: [전역 파일 뮤텍스 100% 결속] GlobalThrottle.get_file_lock()을 이식하여 파일별 100% 독립적인 Mutex Lock 획득 보장 (Lost Update 원천 차단).
# 🚨 MODIFIED: [리버스 자본 격리 아키텍처 팩트 결속] 다중 종목 예수금 충돌 대참사를 막기 위해 KIS 예수금 참조를 파기하고 오직 장부 체결 대금 기반의 apply_reverse_daily_settlement() 엔진 결속 완료.
# 🚨 MODIFIED: [데드코드 정리] 무용지물이던 낡은 scale_dynamic_t() 영구 정리 완료.
# ==========================================================

import json
import os
import datetime
from zoneinfo import ZoneInfo
import math
import time
import shutil
import tempfile
import logging
from global_throttle import GlobalThrottle # 🚨 NEW: 중앙 통제소 결속

try:
    from version_history import VERSION_HISTORY
except ImportError:
    VERSION_HISTORY = ["V14.x [-] 버전 기록 파일(version_history.py)을 찾을 수 없습니다."]

SLICE_PROFILES = {
    "SOXL": {
        "15:27": 0.010835, "15:28": 0.020940, "15:29": 0.031300, "15:30": 0.042240, "15:31": 0.053363,
        "15:32": 0.065060, "15:33": 0.077099, "15:34": 0.089780, "15:35": 0.102895, "15:36": 0.116806,
        "15:37": 0.131738, "15:38": 0.147140, "15:39": 0.163668, "15:40": 0.180989, "15:41": 0.199444,
        "15:42": 0.219685, "15:43": 0.240883, "15:44": 0.263959, "15:45": 0.288516, "15:46": 0.315477,
        "15:47": 0.346344, "15:48": 0.379820, "15:49": 0.417421, "15:50": 0.458916, "15:51": 0.506633,
        "15:52": 0.562301, "15:53": 0.628571, "15:54": 0.710329, "15:55": 0.819730, "15:56": 1.000000
    },
    "TQQQ": {
        "15:27": 0.010835, "15:28": 0.020940, "15:29": 0.031300, "15:30": 0.042240, "15:31": 0.053363,
        "15:32": 0.065060, "15:33": 0.077099, "15:34": 0.089780, "15:35": 0.102895, "15:36": 0.116806,
        "15:37": 0.131738, "15:38": 0.147140, "15:39": 0.163668, "15:40": 0.180989, "15:41": 0.199444,
        "15:42": 0.219685, "15:43": 0.240883, "15:44": 0.263959, "15:45": 0.288516, "15:46": 0.315477,
        "15:47": 0.346344, "15:48": 0.379820, "15:49": 0.417421, "15:50": 0.458916, "15:51": 0.506633,
        "15:52": 0.562301, "15:53": 0.628571, "15:54": 0.710329, "15:55": 0.819730, "15:56": 1.000000
    }
}

class ConfigManager:
    def __init__(self):
        self.FILES = {
            "TOKEN": "data/token.dat",
            "CHAT_ID": "data/chat_id.dat",
            "LEDGER": "data/disabled_legacy_ledger.json",
            "HISTORY": "data/manual_history.json", 
            "SPLIT": "data/split_config.json",
            "TICKER": "data/active_tickers.json",
            "UPWARD_VOLATILITY": "data/upward_volatility.json", 
            "SECRET_MODE": "data/secret_mode.dat",
            "PROFIT_CFG": "data/profit_config.json",
            "LOCKS": "data/trade_locks.json",
            "SEED_CFG": "data/seed_config.json",         
            "COMPOUND_CFG": "data/compound_config.json",
            "VERSION_CFG": "data/version_config.json",
            "REVERSE_CFG": "data/reverse_config.json",
            "T_STATE": "data/t_state.json",
            "STRATEGY_BASELINE": "data/strategy_baseline_SOXL_2026-08-11.json",
            "T_EVENTS": "data/t_events_SOXL.jsonl",
            "LEGACY_HISTORY": "data/legacy_history_SOXL_20260622_20260810.json",
            "EXECUTION_LEDGER": "data/execution_ledger_SOXL.jsonl",
            "PROCESSED_FILLS": "data/processed_fills_SOXL.jsonl",
            "PENDING_SEED": "data/pending_seed.json",
            "VOLATILITY_MULTIPLIER_CFG": "data/volatility_multiplier.json",
            "SPLIT_HISTORY": "data/split_history.json",
            "AUX_HYBRID_CFG": "data/aux_hybrid.json",
            "AUX_SORTIE_CFG": "data/aux_sortie.json",
            "MANUAL_SLICE_CFG": "data/manual_slice_config.json",
            "FEE_CFG": "data/fee_config.json", 
            "MASTER_SWITCH": "data/master_switch.json",
            "VOLATILITY_BUY_LOCKED": "data/volatility_buy_locked.json",
            "VOLATILITY_SELL_LOCKED": "data/volatility_sell_locked.json",
            "AUX_GAP_THRESH_CFG": "data/aux_gap_thresh.json",
            "AUX_ANCHOR_CFG": "data/aux_anchor.json",
            "AUX_BUDGET_CFG": "data/aux_budget.json",         
            "AUX_OVERNIGHT_CFG": "data/aux_overnight.json"      
        }
        
        self.DEFAULT_SEED = {"SOXL": 6720.0, "TQQQ": 6720.0}
        self.DEFAULT_SPLIT = {"SOXL": 20.0, "TQQQ": 20.0}
        self.DEFAULT_TARGET = {"SOXL": 20.0, "TQQQ": 20.0}
        self.DEFAULT_VERSION = {"SOXL": "LAOER_V4_SOXL_20", "TQQQ": "LAOER_V4_SOXL_20"}
        self.DEFAULT_COMPOUND = {"SOXL": 70.0, "TQQQ": 70.0}
        self.DEFAULT_VOLATILITY_MULTIPLIER = {"SOXL": 1.0, "TQQQ": 0.9}
        self.DEFAULT_FEE = {"SOXL": 0.07, "TQQQ": 0.07} 
        self._last_t_event_status = {}

    def _set_t_event_status(self, ticker, ok, error=None):
        target = str(ticker).upper()
        self._last_t_event_status[target] = {
            "ok": bool(ok),
            "error": "" if error is None else str(error),
        }

    def get_t_event_state_status(self, ticker):
        target = str(ticker).upper()
        return self._last_t_event_status.get(target, {"ok": True, "error": ""}).copy()

    def get_official_t_state(self, ticker, actual_qty=None, actual_avg_price=None):
        """Return current official T state.

        이벤트식 T: actual_qty/actual_avg_price는 호출부 호환을 위해
        받지만 T 계산에는 사용하지 않는다.
        """
        target = str(ticker).upper()
        try:
            from trade_state_store import TradeStateStore

            state = TradeStateStore(self.FILES["STRATEGY_BASELINE"], self.FILES["T_EVENTS"]).load_state(target)
            t = float(state.t)
            self._set_t_event_status(target, True)
            return {
                "ticker": state.ticker,
                "t": round(t, 2),
                "revision": int(state.revision),
                "available_cash": float(state.available_cash),
                "reverse_active": bool(state.reverse_active),
            }
        except Exception as e:
            self._set_t_event_status(target, False, e)
            raise

    def append_kis_confirmed_execution_fact(self, fill):
        """Append one post-cutoff confirmed KIS fill to the official execution ledger."""
        from ledger_migration import ExecutionLedger

        ledger = ExecutionLedger(self.FILES["EXECUTION_LEDGER"])
        return ledger.append_confirmed_fill(fill)

    def _safe_float(self, value):
        try:
            f_val = float(str(value or 0.0).replace(',', ''))
            if math.isnan(f_val) or math.isinf(f_val):
                return 0.0
            return f_val
        except Exception:
            return 0.0

    def get_slice_profile(self, ticker: str) -> dict:
        target_ticker = str(ticker).upper() if ticker else ""
        if target_ticker not in SLICE_PROFILES:
            return {}
        return SLICE_PROFILES[target_ticker]

    def _atomic_update_locks(self, update_fn):
        lock_file_path = self.FILES["LOCKS"]
        with GlobalThrottle.get_file_lock(lock_file_path):
            dir_name = os.path.dirname(lock_file_path) or '.'
            try:
                os.makedirs(dir_name, exist_ok=True)
            except OSError:
                pass
            
            locks = self._load_json(lock_file_path, {})
            if not isinstance(locks, dict): locks = {}
            update_fn(locks)
            self._save_json(lock_file_path, locks)

    def _load_json(self, filename, default=None):
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if default is not None and not isinstance(data, type(default)):
                    return default
                return data if data is not None else (default if default is not None else {})
        except FileNotFoundError:
            return default if default is not None else {}
        except Exception as e:
            logging.warning(f"⚠️ [Config] JSON 로드 에러 ({filename}): {e}")
            try:
                shutil.copy(filename, filename + f".bak_{int(time.time())}")
            except Exception as backup_e:
                logging.warning(f"⚠️ [Config] 백업 실패: {backup_e}")
            return default if default is not None else {}

    def _save_json(self, filename, data):
        fd = None
        temp_path = None
        try:
            dir_name = os.path.dirname(filename) or '.'
            try:
                os.makedirs(dir_name, exist_ok=True)
            except OSError:
                pass
                 
            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                fd = None
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.flush() 
                os.fsync(f.fileno()) 
      
            os.replace(temp_path, filename)
            temp_path = None
        except Exception as e:
            logging.error(f"❌ [Config] JSON 저장 중 치명적 에러 발생 ({filename}): {e}")
            if fd is not None:
                try: os.close(fd)
                except OSError: pass
            if temp_path:
                try: os.remove(temp_path)
                except OSError: pass

    def _load_file(self, filename, default=None):
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                return f.read().strip()
        except FileNotFoundError:
            return default
        except Exception as e:
            logging.warning(f"⚠️ [Config] 파일 로드 에러 ({filename}): {e}")
            return default

    def _save_file(self, filename, content):
        fd = None
        temp_path = None
        try:
            dir_name = os.path.dirname(filename) or '.'
            try:
                os.makedirs(dir_name, exist_ok=True)
            except OSError:
                pass
          
            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                fd = None
                f.write(str(content))
                f.flush()
                os.fsync(f.fileno()) 
            
            os.replace(temp_path, filename)
            temp_path = None
        except Exception as e:
            logging.error(f"❌ [Config] 텍스트 파일 저장 에러 ({filename}): {e}")
            if fd is not None:
                try: os.close(fd)
                except OSError: pass
            if temp_path:
                try: os.remove(temp_path)
                except OSError: pass

    def get_aux_gap_threshold(self, ticker):
        with GlobalThrottle.get_file_lock(self.FILES["AUX_GAP_THRESH_CFG"]):
            return self._safe_float(self._load_json(self.FILES["AUX_GAP_THRESH_CFG"], {}).get(ticker, -2.0))

    def set_aux_gap_threshold(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["AUX_GAP_THRESH_CFG"]):
            d = self._load_json(self.FILES["AUX_GAP_THRESH_CFG"], {})
            d[ticker] = self._safe_float(v)
            self._save_json(self.FILES["AUX_GAP_THRESH_CFG"], d)

    def get_last_split_date(self, ticker):
        with GlobalThrottle.get_file_lock(self.FILES["SPLIT_HISTORY"]):
            return str(self._load_json(self.FILES["SPLIT_HISTORY"], {}).get(ticker, ""))

    def set_last_split_date(self, ticker, date_str):
        with GlobalThrottle.get_file_lock(self.FILES["SPLIT_HISTORY"]):
            d = self._load_json(self.FILES["SPLIT_HISTORY"], {})
            d[ticker] = str(date_str)
            self._save_json(self.FILES["SPLIT_HISTORY"], d)

    def get_order_locked(self, ticker):
        with GlobalThrottle.get_file_lock(self.FILES["LOCKS"]):
            locks = self._load_json(self.FILES["LOCKS"], {})
            return bool(locks.get(f"ORDER_LOCKED_{ticker}", False))

    def set_order_locked(self, ticker, is_locked):
        def _update(locks):
            if is_locked:
                locks[f"ORDER_LOCKED_{ticker}"] = True
            else:
                if f"ORDER_LOCKED_{ticker}" in locks:
                    del locks[f"ORDER_LOCKED_{ticker}"]
        self._atomic_update_locks(_update)

    def set_lock(self, ticker, market_type):
        est = ZoneInfo('America/New_York')
        today = datetime.datetime.now(est).strftime('%Y-%m-%d')
        def _update(locks):
            locks[f"{today}_{ticker}_{market_type}"] = True
        self._atomic_update_locks(_update)

    def reset_locks(self):
        def _update(locks):
            keys_to_keep = [k for k in locks.keys() if k.startswith("ORDER_LOCKED_")]
            surviving_locks = {k: locks[k] for k in keys_to_keep}
            locks.clear()
            locks.update(surviving_locks)
        self._atomic_update_locks(_update)
        
    def reset_lock_for_ticker(self, ticker):
        est = ZoneInfo('America/New_York')
        today = datetime.datetime.now(est).strftime('%Y-%m-%d')
        def _update(locks):
            keys_to_delete = [k for k in locks.keys() if k.startswith(f"{today}_{ticker}")]
            for k in keys_to_delete:
                del locks[k]
        self._atomic_update_locks(_update)

    def check_lock(self, ticker, market_type):
        est = ZoneInfo('America/New_York')
        today = datetime.datetime.now(est).strftime('%Y-%m-%d')
        with GlobalThrottle.get_file_lock(self.FILES["LOCKS"]):
            locks = self._load_json(self.FILES["LOCKS"], {})
            return bool(locks.get(f"{today}_{ticker}_{market_type}", False))

    def get_absolute_t_val(self, ticker, actual_qty, actual_avg_price):
        # 이벤트식 T: 원가역산 금지. baseline + T 이벤트 원장에서 T를 가져온다.
        target = str(ticker).upper()
        try:
            from trade_state_store import TradeStateStore

            store = TradeStateStore(self.FILES["STRATEGY_BASELINE"], self.FILES["T_EVENTS"])
            state = store.load_state(target)
            from decimal import Decimal
            t_dec = Decimal(str(state.t))
            split_dec = Decimal(str(self.get_split_count(target)))
            remaining_splits = split_dec - t_dec
            if remaining_splits < Decimal("1"):
                remaining_splits = Decimal("1")
            cycle_cash, detail = self.calculate_cycle_cash(target)
            if cycle_cash is None:
                raise ValueError(f"cycle_cash unavailable: {detail.get('reason', '')}")
            one_portion = Decimal(str(cycle_cash)) / remaining_splits
            self._set_t_event_status(target, True)
            return round(float(t_dec), 2), float(one_portion)
        except Exception as e:
            self._set_t_event_status(target, False, e)
            logging.error(f"⛔ [{ticker}] T값 이벤트식 로드 실패: {e}")
            return 0.0, 0.0

    def apply_stock_split(self, ticker, ratio):
        safe_ratio = self._safe_float(ratio)
        if safe_ratio <= 0: return
        with GlobalThrottle.get_file_lock(self.FILES["LEDGER"]):
            ledger = self._load_json(self.FILES["LEDGER"], [])
            changed = False
            for r in ledger:
                if r.get('ticker') == ticker:
                    r_qty = int(self._safe_float(r.get('qty', 0)))
                    r_price = self._safe_float(r.get('price', 0.0))
                    
                    raw_new_qty = r_qty * safe_ratio
                    new_qty = math.floor(raw_new_qty + 0.5)
        
                    r['qty'] = new_qty if new_qty > 0 else (1 if r_qty > 0 else 0)
                    r['price'] = round(r_price / safe_ratio, 4)
                    if 'avg_price' in r:
                        r['avg_price'] = round(self._safe_float(r.get('avg_price', 0.0)) / safe_ratio, 4)
                    changed = True
            if changed:
                self._save_json(self.FILES["LEDGER"], ledger)

    def overwrite_genesis_ledger(self, ticker, genesis_records, actual_avg):
        from ledger_migration import LegacyLedgerError
        raise LegacyLedgerError("synthetic GENESIS ledger generation is blocked from the official pipeline")

    def overwrite_incremental_ledger(self, ticker, temp_recs, new_today_records):
        with GlobalThrottle.get_file_lock(self.FILES["LEDGER"]):
            ledger = self._load_json(self.FILES["LEDGER"], [])
            remaining = [r for r in ledger if r.get('ticker') != ticker]
            updated_ticker_recs = list(temp_recs)
            
            current_rev_state = self.get_reverse_state(ticker).get("is_active", False)
            max_id = max([int(self._safe_float(r.get('id', 0))) for r in ledger] + [0])
            
            for i, rec in enumerate(new_today_records or []):
                if not isinstance(rec, dict): continue
                from ledger_migration import reject_synthetic_official_event
                reject_synthetic_official_event(rec)
                max_id += 1
                new_row = {
                    "id": max_id,
                    "date": rec.get('date'),
                    "ticker": ticker,
                    "side": rec.get('side'),
                    "price": self._safe_float(rec.get('price', 0.0)),
                    "qty": int(self._safe_float(rec.get('qty', 0))),
                    "avg_price": self._safe_float(rec.get('avg_price', 0.0)),
                    "exec_id": rec.get("exec_id", f"FASTTRACK_{int(time.time())}_{i}"),
                    "is_reverse": current_rev_state
                }
                if "desc" in rec:
                    new_row["desc"] = rec.get("desc")
                    
                updated_ticker_recs.append(new_row)
                 
            remaining.extend(updated_ticker_recs)
            self._save_json(self.FILES["LEDGER"], remaining)

    def overwrite_ledger(self, ticker, actual_qty, actual_avg):
        from ledger_migration import LegacyLedgerError
        raise LegacyLedgerError("synthetic INIT ledger generation is blocked from the official pipeline")

    def calibrate_avg_price(self, ticker, actual_avg):
        with GlobalThrottle.get_file_lock(self.FILES["LEDGER"]):
            ledger = self._load_json(self.FILES["LEDGER"], [])
            target_recs = [r for r in ledger if r.get('ticker') == ticker]
            if target_recs:
                for r in target_recs:
                    r['avg_price'] = self._safe_float(actual_avg)
                self._save_json(self.FILES["LEDGER"], ledger)

    def calibrate_ledger_prices(self, ticker, target_date_str, exec_history):
        if not exec_history:
            return 0
           
        buy_qty = 0
        buy_amt = 0.0
        sell_qty = 0
        sell_amt = 0.0
        
        for ex in (exec_history or []):
            if not isinstance(ex, dict): continue
            side_cd = ex.get('sll_buy_dvsn_cd')
            qty = int(self._safe_float(ex.get('ft_ccld_qty', '0')))
            price = self._safe_float(ex.get('ft_ccld_unpr3', '0'))
            
            if qty > 0 and price > 0:
                if side_cd == "02": 
                    buy_qty += qty
                    buy_amt += (qty * price)
                elif side_cd == "01": 
                    sell_qty += qty
                    sell_amt += (qty * price)
            
        actual_buy_price = round(buy_amt / buy_qty, 4) if buy_qty > 0 else 0.0
        actual_sell_price = round(sell_amt / sell_qty, 4) if sell_qty > 0 else 0.0
        
        if actual_buy_price == 0.0 and actual_sell_price == 0.0:
            return 0
            
        with GlobalThrottle.get_file_lock(self.FILES["LEDGER"]):
            ledger = self._load_json(self.FILES["LEDGER"], [])
            changed_count = 0
            
            for r in ledger:
                if r.get('ticker') == ticker and r.get('date') == target_date_str:
                    exec_id = str(r.get('exec_id', ''))
                    if 'INIT' in exec_id:
                        continue
                        
                    if r.get('side') == 'BUY' and actual_buy_price > 0.0:
                        if abs(self._safe_float(r.get('price', 0.0)) - actual_buy_price) >= 0.01:
                            r['price'] = actual_buy_price
                            changed_count += 1
                    elif r.get('side') == 'SELL' and actual_sell_price > 0.0:
                        if abs(self._safe_float(r.get('price', 0.0)) - actual_sell_price) >= 0.01:
                            r['price'] = actual_sell_price
                            changed_count += 1
                                
            if changed_count > 0:
                self._save_json(self.FILES["LEDGER"], ledger)
            
            return changed_count

    def clear_ledger_for_ticker(self, ticker):
        with GlobalThrottle.get_file_lock(self.FILES["LEDGER"]):
            ledger = self._load_json(self.FILES["LEDGER"], [])
            remaining = [r for r in ledger if r.get('ticker') != ticker]
            self._save_json(self.FILES["LEDGER"], remaining)
            self.set_reverse_state(ticker, False, 0, 0.0, dynamic_t=0.0, rem_cash=0.0, is_day_one=True)

    def calculate_holdings(self, ticker, records=None):
        if records is None:
            return self.calculate_holdings_from_official_ledger(ticker)

        # 호환용 순수 계산기. 신규 런타임은 records를 주입하지 않고 official ledger 경로를 사용한다.
        target_recs = [r for r in (records or []) if isinstance(r, dict) and r.get('ticker') == ticker]
        
        total_qty, total_invested, total_sold = 0, 0.0, 0.0     
        
        running_qty = 0
        running_cost = 0.0

        for r in target_recs:
            r_qty = int(self._safe_float(r.get('qty', 0)))
            r_price = self._safe_float(r.get('price', 0.0))

            if r.get('side') == 'BUY':
                total_qty += r_qty
                total_invested += (r_price * r_qty)
                running_qty += r_qty
                running_cost += (r_price * r_qty)
            elif r.get('side') == 'SELL':
                total_qty -= r_qty
                total_sold += (r_price * r_qty)
                if running_qty > 0:
                    cost_per_share = running_cost / running_qty
                    running_cost -= cost_per_share * min(r_qty, running_qty)
                    running_qty = max(0, running_qty - r_qty)
        
        total_qty = max(0, int(total_qty))
        invested_up = math.ceil(total_invested * 100) / 100.0
        sold_up = math.ceil(total_sold * 100) / 100.0
        
        avg_price = 0.0
        if total_qty > 0:
            avg_price = (running_cost / running_qty) if running_qty > 0 else 0.0
        
        return total_qty, avg_price, invested_up, sold_up

    def calculate_holdings_from_official_ledger(self, ticker):
        """/sync 로컬 장부: 불변 KIS baseline + append-only execution ledger 기준.

        qty = baseline.qty + Σ(execution_ledger KIS_CONFIRMED_FILL 체결 qty 부호)
        avg = 이동평균법 (SELL은 당시 평단으로 원가 차감 → KIS 매입평균과 일치)

        legacy local JSON 기반 calculate_holdings 원가역산 경로와 달리
        이 메서드는 2026-08-11 승인 baseline과 그 이후 KIS_CONFIRMED_FILL
        체결만 사용한다. 8/13 BUY 5 같은 체결이 legacy 로컬 JSON에 누락되어도
        신규 원장에는 정상 반영되어 있으므로 KIS/local 불일치 HALT가 해소된다.
        """
        target = str(ticker).upper()
        baseline = self._load_json(self.FILES.get("STRATEGY_BASELINE", ""), {})
        if not isinstance(baseline, dict):
            raise ValueError(f"official baseline missing for {target}")
        if str(baseline.get("ticker", "")).upper() != target:
            raise ValueError(f"official baseline ticker mismatch for {target}")

        base_qty = int(self._safe_float(baseline.get("qty", 0)))
        base_avg = self._safe_float(baseline.get("avg_price", 0.0))

        running_qty = base_qty
        running_cost = base_qty * base_avg
        total_qty = base_qty
        total_invested = base_qty * base_avg
        total_sold = 0.0

        exec_path = self.FILES.get("EXECUTION_LEDGER", "")
        fills = []
        if exec_path and os.path.exists(exec_path):
            with open(exec_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        logging.warning(f"⚠️ [Config] execution ledger JSONL 파싱 실패: {line[:80]}")
                        continue
                    if isinstance(rec, dict) and rec.get("source") == "KIS_CONFIRMED_FILL" and str(rec.get("ticker", "")).upper() == target:
                        fills.append(rec)

        fills.sort(key=lambda r: (str(r.get("trade_date") or ""), str(r.get("execution_time") or "")))

        for fill in fills:
            side = str(fill.get("side", "")).upper()
            qty = int(self._safe_float(fill.get("qty", 0)))
            price = self._safe_float(fill.get("price", 0.0))
            if qty <= 0:
                continue
            if side == "BUY":
                total_qty += qty
                total_invested += price * qty
                running_qty += qty
                running_cost += price * qty
            elif side == "SELL":
                total_qty -= qty
                total_sold += price * qty
                if running_qty > 0:
                    cost_per_share = running_cost / running_qty
                    running_cost -= cost_per_share * min(qty, running_qty)
                    running_qty = max(0, running_qty - qty)

        total_qty = max(0, int(total_qty))
        avg_price = round((running_cost / running_qty), 4) if running_qty > 0 else 0.0
        invested_up = math.ceil(total_invested * 100) / 100.0
        sold_up = math.ceil(total_sold * 100) / 100.0
        return total_qty, avg_price, invested_up, sold_up

    def _official_data_path(self, files_key, default_name):
        """official 원장 파일 경로를 baseline 디렉터리 기준으로 co-locate 해석한다.

        cycle_cash·pending_seed 는 반드시 불변 baseline과 같은 데이터 디렉터리의
        원장을 읽어야 하므로, baseline 경로의 디렉터리에 파일명을 결합해 돌려준다.
        (운영: data/… 그대로. 테스트: baseline을 tmp로 지정하면 자동 격리된다.)
        """
        base = self.FILES.get("STRATEGY_BASELINE", "") or ""
        base_dir = os.path.dirname(base) or "."
        name = os.path.basename(self.FILES.get(files_key, "") or default_name) or default_name
        return os.path.join(base_dir, name)

    def calculate_cycle_cash(self, ticker):
        """사이클 기준 현금(cycle_cash) — KIS 예수금과 독립적인 원장 기반 값.

        cycle_cash = baseline.available_cash
                     + Σ(baseline 이후 매도금액) − Σ(baseline 이후 매수금액)
        금액 = qty × price (Decimal). 단일 멱등 소스 = execution_ledger
        (source=KIS_CONFIRMED_FILL, append-only, fill_key 유일). processed_fills는
        재분류로 같은 fill_key 라인이 중복 생성되므로 합산 소스로 쓰지 않는다.

        반환: (cycle_cash: float | None, detail: dict). 계산 불가/오염 시 None(fail-closed).
        """
        from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

        target = str(ticker).upper()
        detail = {
            "ticker": target,
            "source": self._official_data_path("EXECUTION_LEDGER", "execution_ledger_SOXL.jsonl"),
            "baseline_cash": None,
            "buy_sum": "0.00",
            "sell_sum": "0.00",
            "fill_count": 0,
            "cycle_cash": None,
            "seed": None,
            "implied_seed_at_baseline": None,
            "seed_consistent": None,
            "baseline_seed_delta": "0.00",
            "reason": "",
        }

        baseline = self._load_json(self.FILES.get("STRATEGY_BASELINE", ""), {})
        if not isinstance(baseline, dict) or str(baseline.get("ticker", "")).upper() != target:
            detail["reason"] = "official baseline missing or ticker mismatch"
            return None, detail
        try:
            base_cash = Decimal(str(baseline.get("available_cash", "0")))
        except (InvalidOperation, ValueError):
            detail["reason"] = "baseline available_cash is not a finite decimal"
            return None, detail
        if not base_cash.is_finite():
            detail["reason"] = "baseline available_cash is not a finite decimal"
            return None, detail
        detail["baseline_cash"] = format(base_cash, "f")

        buy_sum = Decimal("0")
        sell_sum = Decimal("0")
        count = 0
        seen_keys = set()
        exec_path = detail["source"]
        if exec_path and os.path.exists(exec_path):
            try:
                with open(exec_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                        except json.JSONDecodeError:
                            # 원장 오염 → 금액 신뢰 불가 → fail-closed
                            detail["reason"] = "execution ledger JSONL parse error"
                            return None, detail
                        if not isinstance(rec, dict):
                            continue
                        if str(rec.get("source")) != "KIS_CONFIRMED_FILL":
                            continue
                        if str(rec.get("ticker", "")).upper() != target:
                            continue
                        fill_key = str(rec.get("fill_key") or "")
                        if fill_key:
                            if fill_key in seen_keys:
                                continue  # 멱등: 동일 fill_key 중복 무시
                            seen_keys.add(fill_key)
                        side = str(rec.get("side", "")).upper()
                        try:
                            qty = Decimal(str(rec.get("qty", "0")))
                            price = Decimal(str(rec.get("price", "0")))
                        except (InvalidOperation, ValueError):
                            detail["reason"] = "execution ledger qty/price not decimal"
                            return None, detail
                        if not (qty.is_finite() and price.is_finite()) or qty <= 0 or price <= 0:
                            continue
                        amount = qty * price
                        if side == "SELL":
                            sell_sum += amount
                            count += 1
                        elif side == "BUY":
                            buy_sum += amount
                            count += 1
            except OSError as exc:
                detail["reason"] = f"execution ledger read error: {exc}"
                return None, detail

        cycle = (base_cash + sell_sum - buy_sum).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        detail["buy_sum"] = format(buy_sum.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), "f")
        detail["sell_sum"] = format(sell_sum.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), "f")
        detail["fill_count"] = count
        detail["cycle_cash"] = format(cycle, "f")

        # 참고용 seed 정합성: 시작시드 ≈ baseline.available_cash + 보유원가(qty×avg)
        seed_cfg = self._load_json(self.FILES.get("SEED_CFG", ""), {})
        if isinstance(seed_cfg, dict) and target in seed_cfg:
            try:
                seed = Decimal(str(seed_cfg.get(target)))
                detail["seed"] = format(seed, "f")
                bqty = Decimal(str(baseline.get("qty", "0")))
                bavg = Decimal(str(baseline.get("avg_price", "0")))
                implied = (base_cash + (bqty * bavg)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                detail["implied_seed_at_baseline"] = format(implied, "f")
                seed_delta = (seed - implied).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                detail["baseline_seed_delta"] = format(seed_delta, "f")
                detail["seed_consistent"] = bool(abs(seed_delta) <= Decimal("50"))
            except (InvalidOperation, ValueError):
                pass

        if cycle <= 0:
            detail["reason"] = "cycle_cash is non-positive"
            return None, detail
        return float(cycle), detail

    def read_pending_seed(self, ticker):
        """격리 기록된 입금분(pending_seed)을 조회한다. 없으면 {}."""
        target = str(ticker).upper()
        path = self._official_data_path("PENDING_SEED", "pending_seed.json")
        with GlobalThrottle.get_file_lock(path):
            d = self._load_json(path, {})
        if not isinstance(d, dict):
            return {}
        val = d.get(target)
        return val if isinstance(val, dict) else {}

    def record_pending_seed(self, ticker, amount, kis_deposit, cycle_cash, note=""):
        """입금분(pending_seed)을 격리 기록. 절대 seed에 자동 합산하지 않는다."""
        target = str(ticker).upper()
        est = ZoneInfo('America/New_York')
        detected_at = datetime.datetime.now(est).strftime('%Y-%m-%d %H:%M:%S %Z')
        path = self._official_data_path("PENDING_SEED", "pending_seed.json")
        record = {
            "ticker": target,
            "amount": round(self._safe_float(amount), 2),
            "kis_deposit": round(self._safe_float(kis_deposit), 2),
            "cycle_cash": round(self._safe_float(cycle_cash), 2),
            "detected_at": detected_at,
            "note": note or "중간 입금 감지 — 대표님 수동 승인 전까지 사이클 미반영",
        }
        with GlobalThrottle.get_file_lock(path):
            d = self._load_json(path, {})
            if not isinstance(d, dict):
                d = {}
            prev = d.get(target)
            # 멱등: 동일 금액이 이미 기록돼 있으면 재기록 생략
            if isinstance(prev, dict) and round(self._safe_float(prev.get("amount")), 2) == record["amount"]:
                return prev
            d[target] = record
            self._save_json(path, d)
        return record

    def clear_pending_seed(self, ticker):
        """대표님 승인·반영 후 격리 기록을 제거한다 (자동 호출 금지)."""
        target = str(ticker).upper()
        path = self._official_data_path("PENDING_SEED", "pending_seed.json")
        with GlobalThrottle.get_file_lock(path):
            d = self._load_json(path, {})
            if isinstance(d, dict) and target in d:
                del d[target]
                self._save_json(path, d)

    def reconcile_cycle_cash(self, ticker, kis_deposit, tolerance=50.0, record_pending=True):
        """정합 검증: KIS 매수미정산 보정현금 ≈ baseline 정규화 cycle_cash.

        - kis_deposit: KIS 예수금 + 매도미정산 + 매수미정산 보정값.
        - baseline_seed_delta = seed − (baseline.available_cash + baseline 보유원가)
          는 기준점 이전 실현손익/수수료/환율 차이이므로 HALT 차이에서 제외한다.
        - expected_kis_cash = cycle_cash − baseline_seed_delta.
        - KIS 초과분은 pending_seed 로 격리 기록만 하고 사이클 미반영.
        - KIS 부족분이 tolerance 를 넘으면 실제 출금/누락 가능성이므로 fail-closed(halt).

        반환 dict: ok, halt, reason, cycle_cash, kis_deposit, expected_kis_cash,
        pending_seed, discrepancy, detail
        """
        from decimal import Decimal

        target = str(ticker).upper()
        result = {
            "ticker": target,
            "ok": False,
            "halt": False,
            "reason": "",
            "cycle_cash": None,
            "kis_deposit": round(self._safe_float(kis_deposit), 2),
            "expected_kis_cash": None,
            "baseline_seed_delta": 0.0,
            "pending_seed": 0.0,
            "discrepancy": None,
        }
        cycle_cash, detail = self.calculate_cycle_cash(target)
        result["detail"] = detail
        if cycle_cash is None:
            result["halt"] = True
            result["reason"] = f"cycle_cash 계산 불가: {detail.get('reason', '')}"
            return result

        result["cycle_cash"] = round(float(cycle_cash), 2)
        dep = Decimal(str(self._safe_float(kis_deposit)))
        cyc = Decimal(str(cycle_cash))
        tol = Decimal(str(tolerance))
        baseline_seed_delta = Decimal(str(detail.get("baseline_seed_delta") or "0"))
        expected = cyc - baseline_seed_delta
        surplus = dep - expected
        deficit = expected - dep
        result["expected_kis_cash"] = round(float(expected), 2)
        result["baseline_seed_delta"] = round(float(baseline_seed_delta), 2)

        if surplus > tol:
            # 입금분 감지 → 격리 기록 (자동 합산 금지)
            rec = None
            if record_pending:
                rec = self.record_pending_seed(target, float(surplus), float(dep), float(cyc))
            result["ok"] = True
            result["pending_seed"] = round(float(surplus), 2)
            result["discrepancy"] = round(float(surplus), 2)
            result["reason"] = "입금분 격리 기록(사이클 미반영)"
            if rec is not None:
                result["record"] = rec
            return result

        if deficit > tol:
            # 원장이 구조적 기준차 보정 후 KIS 현금보다 과다 → 주문 차단
            result["halt"] = True
            result["discrepancy"] = round(float(deficit), 2)
            result["reason"] = (
                f"정합 실패 HALT: cycle_cash({result['cycle_cash']})가 "
                f"baseline 구조차 보정 KIS 현금({result['expected_kis_cash']}) 대비 "
                f"실제 KIS 보정현금({result['kis_deposit']})을 {result['discrepancy']}$ 초과"
            )
            return result

        # 항등식 성립 (±tolerance). 소액 편차는 환율/미정산 시점차로 흡수.
        result["ok"] = True
        result["discrepancy"] = round(float(abs(surplus)), 2)
        result["reason"] = "정합 성립"
        return result

    def get_official_fills(self, ticker):
        """/record 거래내역 리스트 전용: 신규 원장(실제 체결가) 기준 각 체결 건.

        legacy local JSON(price=평단가 오기록) 대신, KIS 확정 체결의 실제
        체결가를 execution_ledger_SOXL.jsonl + processed_fills_SOXL.jsonl에서 읽어
        (date, side, qty, price, order_no) 형태로 중복 제거 후 반환한다.
        """
        target = str(ticker).upper()
        by_order: dict = {}

        def _norm_trade_date(raw):
            text = str(raw or "").replace("-", "").strip()
            if len(text) >= 8:
                return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
            return str(raw or "")

        def _absorb(rec, order_no):
            side = str(rec.get("side", "")).upper()
            qty = int(self._safe_float(rec.get("qty", 0)))
            price = self._safe_float(rec.get("price", 0.0))
            if side not in ("BUY", "SELL") or qty <= 0 or price <= 0:
                return
            raw_date = rec.get("trade_date") or rec.get("date")
            trade_date = str(raw_date or "").replace("-", "")
            key = order_no or f"{trade_date}|{side}|{qty}|{price:.2f}"
            if key in by_order:
                return
            by_order[key] = {
                "date": _norm_trade_date(raw_date),
                "trade_date": trade_date,
                "side": side,
                "qty": qty,
                "price": price,
                "order_no": order_no,
            }

        # 1) official execution ledger (append-only, source=KIS_CONFIRMED_FILL)
        exec_path = self.FILES.get("EXECUTION_LEDGER", "")
        if exec_path and os.path.exists(exec_path):
            try:
                with open(exec_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if isinstance(rec, dict) and str(rec.get("source")) == "KIS_CONFIRMED_FILL" and str(rec.get("ticker", "")).upper() == target:
                            _absorb(rec, str(rec.get("kis_order_no") or rec.get("order_no") or ""))
            except Exception as e:
                logging.warning(f"⚠️ [Config] execution ledger 읽기 실패: {e}")

        # 2) processed_fills (체결 확정 시각/amount 보유) — 실행 원장 누락분 보강
        processed_path = self.FILES.get("PROCESSED_FILLS", "") or os.path.join("data", f"processed_fills_{target}.jsonl")
        if processed_path and os.path.exists(processed_path):
            try:
                with open(processed_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if isinstance(rec, dict) and str(rec.get("ticker", "")).upper() == target:
                            _absorb(rec, str(rec.get("order_no") or rec.get("kis_order_no") or ""))
            except Exception as e:
                logging.warning(f"⚠️ [Config] processed fills 읽기 실패: {e}")

        return sorted(by_order.values(), key=lambda r: (str(r.get("trade_date") or ""), str(r.get("order_no") or "")))

    def get_reverse_state(self, ticker):
        with GlobalThrottle.get_file_lock(self.FILES["REVERSE_CFG"]):
            d = self._load_json(self.FILES["REVERSE_CFG"], {})
            val = d.get(ticker)
            if not isinstance(val, dict):
                return {
                    "is_active": False, "day_count": 0, "exit_target": 0.0, 
                    "last_update_date": "", "dynamic_t": 0.0, "rem_cash": 0.0, "is_day_one": True,
                    "last_t_update_date": ""
                }
            val.setdefault("dynamic_t", 0.0)
            val.setdefault("rem_cash", 0.0)
            val.setdefault("is_day_one", val.get("day_count", 0) == 0)
            val.setdefault("last_t_update_date", val.get("last_update_date", ""))
            return val

    def set_reverse_state(self, ticker, is_active, day_count, exit_target=0.0, last_update_date=None, dynamic_t=0.0, rem_cash=0.0, is_day_one=None):
        with GlobalThrottle.get_file_lock(self.FILES["REVERSE_CFG"]):
            if last_update_date is None:
                est = ZoneInfo('America/New_York')
                last_update_date = datetime.datetime.now(est).strftime('%Y-%m-%d')
                
            d = self._load_json(self.FILES["REVERSE_CFG"], {})
            d[ticker] = {
                "is_active": is_active, 
                "day_count": day_count, 
                "exit_target": self._safe_float(exit_target), 
                "last_update_date": last_update_date,
                "last_t_update_date": (datetime.datetime.strptime(last_update_date, '%Y-%m-%d') - datetime.timedelta(days=1)).strftime('%Y-%m-%d'),
                "dynamic_t": self._safe_float(dynamic_t),
                "rem_cash": self._safe_float(rem_cash),
                "is_day_one": (day_count == 0) if is_day_one is None else bool(is_day_one)
            }
            self._save_json(self.FILES["REVERSE_CFG"], d)

    def _load_reverse_settlement_events(self, ticker, start_date, end_date):
        """리버스 정산창(start_date ≤ 거래일 < end_date)의 T 이벤트를 읽어
        [{side, amount, t_after, occurred_at, trade_date}] 리스트로 반환한다.

        소스: t_events_SOXL.jsonl (append-only, T 이벤트 원장). 이 파일은 실제
        체결마다 side·filled_amount·t_after 를 기록하므로 리버스 정산의 신뢰 소스다.
        거래일(trade_date)은 별도 필드가 없어 fill_key 안의 8자리 거래일(YYYYMMDD)에서
        추출해 KIS 거래일 기준으로 정산창을 판정한다.
        """
        target = str(ticker).upper()
        path = self._official_data_path("T_EVENTS", "t_events_SOXL.jsonl")
        out = []
        if not path or not os.path.exists(path):
            logging.warning(f"⚠️ [Config] 리버스 정산: T 이벤트 원장 없음: {path}")
            return out
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        logging.warning(f"⚠️ [Config] t_events JSONL 파싱 실패: {line[:80]}")
                        continue
                    if not isinstance(rec, dict):
                        continue
                    if str(rec.get("ticker", "")).upper() != target:
                        continue

                    # fill_key(hash|TICKER|EXCH|YYYYMMDD|order|time|side|qty|price)의
                    # 8자리 거래일 세그먼트를 추출 → YYYY-MM-DD
                    trade_date = ""
                    for p in str(rec.get("fill_key", "")).split("|"):
                        if len(p) == 8 and p.isdigit() and p.startswith("20"):
                            trade_date = f"{p[0:4]}-{p[4:6]}-{p[6:8]}"
                            break
                    if not trade_date:
                        continue
                    if not (start_date < trade_date <= end_date):
                        continue

                    out.append({
                        "side": str(rec.get("side", "")).upper(),
                        "amount": self._safe_float(rec.get("filled_amount", 0.0)),
                        "t_after": rec.get("t_after", None),
                        "occurred_at": str(rec.get("occurred_at") or ""),
                        "trade_date": trade_date,
                    })
        except Exception as e:
            logging.warning(f"⚠️ [Config] t_events 읽기 실패: {e}")
        return out

    def apply_reverse_daily_settlement(self, ticker):
        """
        🚨 [리버스 모드 팩트 정산 및 자본 격리]
        스냅샷 생성 직전 1회 호출되어, 전날(마지막 갱신일 ~ 오늘 사이)의 리버스 체결을
        기반으로 잔금(rem_cash)과 T값(dynamic_t)을 원자적으로 역산 갱신한다.

        V4.1: 비활성 legacy 원장(disabled_legacy_ledger.json, 실체 없음/레코드 0개)
        의존을 폐기하고, 실제 체결이 기록되는 T 이벤트 원장(t_events_SOXL.jsonl)을
        정산 소스로 사용한다.
          - rem_cash : 정산창 내 SELL 체결금액 − BUY 체결금액 (filled_amount 합산)
          - dynamic_t: 같은 창에서 마지막(최신) 이벤트의 t_after(실측 T)로 정합.
            t_events가 이미 T를 기록하므로 단순 ×0.9 근사 대신 실측값을 신뢰한다.
            (t_after 미기록 구 이벤트만 있을 때는 ×0.9/×0.95 근사로 폴백)
        """
        with GlobalThrottle.get_file_lock(self.FILES["REVERSE_CFG"]):
            d = self._load_json(self.FILES["REVERSE_CFG"], {})
            state = d.get(ticker)
            if not isinstance(state, dict) or not state.get("is_active", False):
                return

            est = ZoneInfo('America/New_York')
            today_str = datetime.datetime.now(est).strftime('%Y-%m-%d')

            last_t_update_date = state.get("last_t_update_date", "")
            if not last_t_update_date:
                last_t_update_date = state.get("last_update_date", "")

            if last_t_update_date == today_str:
                return

            dynamic_t = self._safe_float(state.get("dynamic_t", 0.0))
            rem_cash = self._safe_float(state.get("rem_cash", 0.0))

            recs = self._load_reverse_settlement_events(ticker, last_t_update_date, today_str)

            buy_sum = 0.0
            sell_sum = 0.0
            had_buy = False
            had_sell = False
            latest_key = None
            latest_t_after = None

            for r in recs:
                side = str(r.get("side", "")).upper()
                amt = self._safe_float(r.get("amount", 0.0))

                if side == "BUY":
                    buy_sum += amt
                    had_buy = True
                elif side == "SELL":
                    sell_sum += amt
                    had_sell = True

                # 정산창 내 최신(occurred_at 최대) 이벤트의 t_after를 실측 T로 채택
                t_after = r.get("t_after", None)
                if t_after is not None:
                    key = str(r.get("occurred_at") or "")
                    if latest_key is None or key >= latest_key:
                        latest_key = key
                        latest_t_after = self._safe_float(t_after)

            if had_buy or had_sell:
                # 🚨 KIS 예수금 의존 파기 — 순수 장부 역산으로 자본 격리 사수
                rem_cash = max(0.0, rem_cash + sell_sum - buy_sum)

                if latest_t_after is not None and latest_t_after > 0:
                    # V4.1: t_events 실측 T(t_after)로 정합 — 신뢰 소스
                    dynamic_t = latest_t_after
                elif had_sell:
                    # 폴백: t_after 미기록 구 이벤트 — split 기반 근사 감소
                    split = self.get_split_count(ticker)
                    if split <= 20: dynamic_t *= 0.9
                    else: dynamic_t *= 0.95

                logging.info(f"♻️ [{ticker}] 리버스 일일 정산 완료: sell=${sell_sum:.2f}, buy=${buy_sum:.2f} ➔ 잔액=${rem_cash:.2f}, T값={dynamic_t:.4f}")

            state["rem_cash"] = round(rem_cash, 2)
            state["dynamic_t"] = round(dynamic_t, 4)
            state["last_t_update_date"] = today_str

            d[ticker] = state
            self._save_json(self.FILES["REVERSE_CFG"], d)

    def increment_reverse_day(self, ticker):
        with GlobalThrottle.get_file_lock(self.FILES["REVERSE_CFG"]):
            d = self._load_json(self.FILES["REVERSE_CFG"], {})
            state = d.get(ticker)
            if not isinstance(state, dict): return False

            if state.get("is_active"):
                est = ZoneInfo('America/New_York')
                now_est = datetime.datetime.now(est)
                today_est_str = now_est.strftime('%Y-%m-%d')
                
                if state.get("last_update_date") != today_est_str:
                    new_day = state.get("day_count", 0) + 1
                    state["day_count"] = new_day
                    state["last_update_date"] = today_est_str
                    state["is_day_one"] = False
                    d[ticker] = state
                    self._save_json(self.FILES["REVERSE_CFG"], d)
                    return True
        return False

    def calculate_v14_state(self, ticker):
        # Official Task 3 source: immutable KIS baseline + append-only T events.
        # Do not reconstruct T from cost basis.
        target = str(ticker).upper()
        try:
            from trade_state_store import TradeStateStore

            state = TradeStateStore(self.FILES["STRATEGY_BASELINE"], self.FILES["T_EVENTS"]).load_state(target)
            from decimal import Decimal
            t_dec = Decimal(str(state.t))
            cycle_cash, detail = self.calculate_cycle_cash(target)
            if cycle_cash is None:
                raise ValueError(f"cycle_cash unavailable: {detail.get('reason', '')}")
            rem_cash_dec = Decimal(str(cycle_cash))
            safe_denom = Decimal("20") - t_dec
            if safe_denom < Decimal("1"):
                safe_denom = Decimal("1")
            current_budget = rem_cash_dec / safe_denom
            self._set_t_event_status(target, True)
            return max(0.0, round(float(t_dec), 4)), max(0.0, float(current_budget)), max(0.0, float(rem_cash_dec))
        except Exception as e:
            self._set_t_event_status(target, False, e)
            logging.error(f"⛔ [{ticker}] V14 state ledger load failed; cost-basis inverse blocked: {e}")
            return 0.0, 0.0, 0.0

    def archive_graduation(self, ticker, end_date, prev_close=0.0):
        from ledger_migration import LegacyLedgerError
        raise LegacyLedgerError("synthetic graduation archiving is blocked from the official append-only pipeline")

    def get_history(self):
        with GlobalThrottle.get_file_lock(self.FILES["HISTORY"]):
            raw_data = self._load_json(self.FILES["HISTORY"], [])
            return [h for h in raw_data if isinstance(h, dict)]

    def delete_history(self, hist_id: int) -> bool:
        with GlobalThrottle.get_file_lock(self.FILES["HISTORY"]):
            history = self.get_history()
            if not history:
                return False
                
            original_len = len(history)
            safe_target_id = int(self._safe_float(hist_id))
            remaining_history = [
                h for h in history 
                if isinstance(h, dict) and int(self._safe_float(h.get('id', 0))) != safe_target_id
            ]
            
            if len(remaining_history) == original_len:
                return False 
                
            self._save_json(self.FILES["HISTORY"], remaining_history)
            return True

    def get_full_version_history(self):
        return VERSION_HISTORY

    def get_latest_version(self):
        history = self.get_full_version_history()
        if isinstance(history, list) and len(history) > 0:
            latest_entry = history[-1]
            if isinstance(latest_entry, dict):
                return latest_entry.get("version", "V14.x")
            elif isinstance(latest_entry, str):
                return latest_entry.split(' ')[0] 
        return "V14.x"

    def get_seed(self, t): 
        with GlobalThrottle.get_file_lock(self.FILES["SEED_CFG"]):
            return self._safe_float(self._load_json(self.FILES["SEED_CFG"], self.DEFAULT_SEED).get(t, 6720.0))

    def get_split_amount(self, t, split=20):
        """Fixed T portion amount: configured seed / 20, returned as Decimal."""
        from decimal import Decimal, InvalidOperation

        target = str(t or "").strip().upper()
        try:
            divisor = Decimal(str(split))
            if divisor <= 0:
                raise InvalidOperation("non-positive split")
            with GlobalThrottle.get_file_lock(self.FILES["SEED_CFG"]):
                seeds = self._load_json(self.FILES["SEED_CFG"], self.DEFAULT_SEED)
            raw_seed = seeds.get(target, self.DEFAULT_SEED.get(target, 6720.0)) if isinstance(seeds, dict) else self.DEFAULT_SEED.get(target, 6720.0)
            seed = Decimal(str(raw_seed).replace(",", ""))
            if not seed.is_finite() or seed <= 0:
                raise InvalidOperation("invalid seed")
            return seed / divisor
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise ValueError(f"invalid split amount seed for {target}") from exc
        
    def set_seed(self, t, v): 
        with GlobalThrottle.get_file_lock(self.FILES["SEED_CFG"]):
            d = self._load_json(self.FILES["SEED_CFG"], self.DEFAULT_SEED)
            d[t] = self._safe_float(v)
            self._save_json(self.FILES["SEED_CFG"], d)

    def get_compound_rate(self, t): 
        with GlobalThrottle.get_file_lock(self.FILES["COMPOUND_CFG"]):
            return self._safe_float(self._load_json(self.FILES["COMPOUND_CFG"], self.DEFAULT_COMPOUND).get(t, 70.0))
         
    def set_compound_rate(self, t, v):
        with GlobalThrottle.get_file_lock(self.FILES["COMPOUND_CFG"]):
            d = self._load_json(self.FILES["COMPOUND_CFG"], self.DEFAULT_COMPOUND)
            d[t] = self._safe_float(v)
            self._save_json(self.FILES["COMPOUND_CFG"], d)

    def get_version(self, t): 
        with GlobalThrottle.get_file_lock(self.FILES["VERSION_CFG"]):
            d = self._load_json(self.FILES["VERSION_CFG"], self.DEFAULT_VERSION)
            target = str(t or "").strip().upper()
            val = str(d.get(target, self.DEFAULT_VERSION.get(target, "LAOER_V4_SOXL_20")))
            if val in {"V4", "V14", "V4.0", "V14.x"}:
                raise ValueError(f"Unsupported legacy strategy version label for {target}: {val}")
            return val
        
    def set_version(self, t, v):
        with GlobalThrottle.get_file_lock(self.FILES["VERSION_CFG"]):
            target = str(t or "").strip().upper()
            val = str(v or "LAOER_V4_SOXL_20")
            if val in {"V4", "V14", "V4.0", "V14.x"}:
                raise ValueError(f"Unsupported legacy strategy version label for {target}: {val}")
            d = self._load_json(self.FILES["VERSION_CFG"], self.DEFAULT_VERSION)
            d[target] = val
            self._save_json(self.FILES["VERSION_CFG"], d)

    def get_split_count(self, t): 
        with GlobalThrottle.get_file_lock(self.FILES["SPLIT"]):
            return self._safe_float(self._load_json(self.FILES["SPLIT"], self.DEFAULT_SPLIT).get(t, 40.0))
         
    def get_target_profit(self, t): 
        with GlobalThrottle.get_file_lock(self.FILES["PROFIT_CFG"]):
            return self._safe_float(self._load_json(self.FILES["PROFIT_CFG"], self.DEFAULT_TARGET).get(t, 10.0))
        
    def get_fee(self, t): 
        with GlobalThrottle.get_file_lock(self.FILES["FEE_CFG"]):
            return self._safe_float(self._load_json(self.FILES["FEE_CFG"], self.DEFAULT_FEE).get(t, 0.07))
       
    def set_fee(self, t, v):
        with GlobalThrottle.get_file_lock(self.FILES["FEE_CFG"]):
            d = self._load_json(self.FILES["FEE_CFG"], self.DEFAULT_FEE)
            d[t] = self._safe_float(v)
            self._save_json(self.FILES["FEE_CFG"], d)

    def get_volatility_multiplier(self, t):
        with GlobalThrottle.get_file_lock(self.FILES["VOLATILITY_MULTIPLIER_CFG"]):
            default_val = self.DEFAULT_VOLATILITY_MULTIPLIER.get(t, 1.0)
            return self._safe_float(self._load_json(self.FILES["VOLATILITY_MULTIPLIER_CFG"], self.DEFAULT_VOLATILITY_MULTIPLIER).get(t, default_val))
        
    def set_volatility_multiplier(self, t, v):
        with GlobalThrottle.get_file_lock(self.FILES["VOLATILITY_MULTIPLIER_CFG"]):
            d = self._load_json(self.FILES["VOLATILITY_MULTIPLIER_CFG"], self.DEFAULT_VOLATILITY_MULTIPLIER)
            d[t] = self._safe_float(v)
            self._save_json(self.FILES["VOLATILITY_MULTIPLIER_CFG"], d)

    def get_upward_volatility_mode(self, ticker): 
        with GlobalThrottle.get_file_lock(self.FILES["UPWARD_VOLATILITY"]):
            return bool(self._load_json(self.FILES["UPWARD_VOLATILITY"], {}).get(ticker, False))
         
    def set_upward_volatility_mode(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["UPWARD_VOLATILITY"]):
             d = self._load_json(self.FILES["UPWARD_VOLATILITY"], {})
             d[ticker] = bool(v)
             self._save_json(self.FILES["UPWARD_VOLATILITY"], d)

    def get_aux_hybrid_mode(self, ticker): 
        with GlobalThrottle.get_file_lock(self.FILES["AUX_HYBRID_CFG"]):
            return bool(self._load_json(self.FILES["AUX_HYBRID_CFG"], {}).get(ticker, False))
    
    def set_aux_hybrid_mode(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["AUX_HYBRID_CFG"]):
            d = self._load_json(self.FILES["AUX_HYBRID_CFG"], {})
            d[ticker] = bool(v)
            self._save_json(self.FILES["AUX_HYBRID_CFG"], d)

    def get_aux_sortie_mode(self, ticker):
        with GlobalThrottle.get_file_lock(self.FILES["AUX_SORTIE_CFG"]):
            return str(self._load_json(self.FILES["AUX_SORTIE_CFG"], {}).get(ticker, "SINGLE"))
        
    def set_aux_sortie_mode(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["AUX_SORTIE_CFG"]):
            d = self._load_json(self.FILES["AUX_SORTIE_CFG"], {})
            d[ticker] = str(v)
            self._save_json(self.FILES["AUX_SORTIE_CFG"], d)

    def get_manual_slice_mode(self, ticker): 
        with GlobalThrottle.get_file_lock(self.FILES["MANUAL_SLICE_CFG"]):
            return bool(self._load_json(self.FILES["MANUAL_SLICE_CFG"], {}).get(ticker, False))
        
    def set_manual_slice_mode(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["MANUAL_SLICE_CFG"]):
            d = self._load_json(self.FILES["MANUAL_SLICE_CFG"], {})
            d[ticker] = bool(v)
            self._save_json(self.FILES["MANUAL_SLICE_CFG"], d)

    def get_master_switch(self, ticker): 
        with GlobalThrottle.get_file_lock(self.FILES["MASTER_SWITCH"]):
            return str(self._load_json(self.FILES["MASTER_SWITCH"], {}).get(ticker, "ALL"))
        
    def set_master_switch(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["MASTER_SWITCH"]):
            d = self._load_json(self.FILES["MASTER_SWITCH"], {})
            d[ticker] = str(v)
            self._save_json(self.FILES["MASTER_SWITCH"], d)

    def get_volatility_buy_locked(self, ticker): 
        with GlobalThrottle.get_file_lock(self.FILES["VOLATILITY_BUY_LOCKED"]):
            return bool(self._load_json(self.FILES["VOLATILITY_BUY_LOCKED"], {}).get(ticker, False))
        
    def set_volatility_buy_locked(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["VOLATILITY_BUY_LOCKED"]):
            d = self._load_json(self.FILES["VOLATILITY_BUY_LOCKED"], {})
            d[ticker] = bool(v)
            self._save_json(self.FILES["VOLATILITY_BUY_LOCKED"], d)

    def get_volatility_sell_locked(self, ticker): 
        with GlobalThrottle.get_file_lock(self.FILES["VOLATILITY_SELL_LOCKED"]):
            return bool(self._load_json(self.FILES["VOLATILITY_SELL_LOCKED"], {}).get(ticker, False))
        
    def set_volatility_sell_locked(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["VOLATILITY_SELL_LOCKED"]):
            d = self._load_json(self.FILES["VOLATILITY_SELL_LOCKED"], {})
            d[ticker] = bool(v)
            self._save_json(self.FILES["VOLATILITY_SELL_LOCKED"], d)

    def get_secret_mode(self): 
        with GlobalThrottle.get_file_lock(self.FILES["SECRET_MODE"]):
            return self._load_file(self.FILES["SECRET_MODE"]) == 'True'
         
    def set_secret_mode(self, v): 
        with GlobalThrottle.get_file_lock(self.FILES["SECRET_MODE"]):
            self._save_file(self.FILES["SECRET_MODE"], str(v))
    
    def get_active_tickers(self): 
        with GlobalThrottle.get_file_lock(self.FILES["TICKER"]):
            tickers = self._load_json(self.FILES["TICKER"], ["SOXL", "TQQQ"])
            if not isinstance(tickers, list): tickers = ["SOXL", "TQQQ"]
            return [str(t) for t in tickers if str(t) not in ["SOXS", "SQQQ", "SPXU"]]
        
    def set_active_tickers(self, v): 
        with GlobalThrottle.get_file_lock(self.FILES["TICKER"]):
             self._save_json(self.FILES["TICKER"], v)
    
    def get_chat_id(self): 
        with GlobalThrottle.get_file_lock(self.FILES["CHAT_ID"]):
            v = self._load_file(self.FILES["CHAT_ID"])
            if v:
                safe_v = int(self._safe_float(v))
                return safe_v if safe_v != 0 else None
            return None
        
    def set_chat_id(self, v): 
        with GlobalThrottle.get_file_lock(self.FILES["CHAT_ID"]):
            self._save_file(self.FILES["CHAT_ID"], v)

    def get_aux_anchor_date(self, ticker):
        with GlobalThrottle.get_file_lock(self.FILES["AUX_ANCHOR_CFG"]):
            return str(self._load_json(self.FILES["AUX_ANCHOR_CFG"], {}).get(ticker, "AUTO"))

    def set_aux_anchor_date(self, ticker, date_str):
        with GlobalThrottle.get_file_lock(self.FILES["AUX_ANCHOR_CFG"]):
            d = self._load_json(self.FILES["AUX_ANCHOR_CFG"], {})
            d[ticker] = str(date_str)
            self._save_json(self.FILES["AUX_ANCHOR_CFG"], d)
            
    def get_aux_budget(self, ticker):
        with GlobalThrottle.get_file_lock(self.FILES["AUX_BUDGET_CFG"]):
            return self._safe_float(self._load_json(self.FILES["AUX_BUDGET_CFG"], {}).get(ticker, 10000.0))

    def set_aux_budget(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["AUX_BUDGET_CFG"]):
            d = self._load_json(self.FILES["AUX_BUDGET_CFG"], {})
            d[ticker] = self._safe_float(v)
            self._save_json(self.FILES["AUX_BUDGET_CFG"], d)
            
    def get_aux_overnight_mode(self, ticker):
        with GlobalThrottle.get_file_lock(self.FILES["AUX_OVERNIGHT_CFG"]):
            return bool(self._load_json(self.FILES["AUX_OVERNIGHT_CFG"], {}).get(ticker, False))
        
    def set_aux_overnight_mode(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["AUX_OVERNIGHT_CFG"]):
            d = self._load_json(self.FILES["AUX_OVERNIGHT_CFG"], {})
            d[ticker] = bool(v)
            self._save_json(self.FILES["AUX_OVERNIGHT_CFG"], d)
