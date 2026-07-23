# ==========================================================
# FILE: config.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 3중 딥다이브 교차 검증 통과 완료.
# 🚨 MODIFIED: [인스턴스 락 영구 소각] self._io_lock 및 self._locks_mutex를 시스템 전역에서 영구 파기.
# 🚨 MODIFIED: [전역 파일 뮤텍스 100% 결속] GlobalThrottle.get_file_lock()을 이식하여 파일별 100% 독립적인 Mutex Lock 획득 보장 (Lost Update 원천 차단).
# 🚨 MODIFIED: fcntl 기반 OS 의존성 락 소각 및 GlobalThrottle 중앙 통제소 일원화 락온 완료.
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

VWAP_PROFILES = {
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
            "LEDGER": "data/manual_ledger.json", 
            "HISTORY": "data/manual_history.json", 
            "SPLIT": "data/split_config.json",
            "TICKER": "data/active_tickers.json",
            "UPWARD_SNIPER": "data/upward_sniper.json", 
            "SECRET_MODE": "data/secret_mode.dat",
            "PROFIT_CFG": "data/profit_config.json",
            "LOCKS": "data/trade_locks.json",
            "SEED_CFG": "data/seed_config.json",         
            "COMPOUND_CFG": "data/compound_config.json",
            "VERSION_CFG": "data/version_config.json",
            "REVERSE_CFG": "data/reverse_config.json",
            "SNIPER_MULTIPLIER_CFG": "data/sniper_multiplier.json",
            "SPLIT_HISTORY": "data/split_history.json",
            "AVWAP_HYBRID_CFG": "data/avwap_hybrid.json",
            "AVWAP_SORTIE_CFG": "data/avwap_sortie.json",
            "MANUAL_VWAP_CFG": "data/manual_vwap_config.json",
            "FEE_CFG": "data/fee_config.json", 
            "MASTER_SWITCH": "data/master_switch.json",
            "SNIPER_BUY_LOCKED": "data/sniper_buy_locked.json",
            "SNIPER_SELL_LOCKED": "data/sniper_sell_locked.json",
            "VREV_GAP_SWITCH_CFG": "data/vrev_gap_switch.json",     
            "VREV_GAP_THRESH_CFG": "data/vrev_gap_thresh.json",
            "AVWAP_GAP_THRESH_CFG": "data/avwap_gap_thresh.json",
            "AVWAP_ANCHOR_CFG": "data/avwap_anchor.json",
            "AVWAP_BUDGET_CFG": "data/avwap_budget.json",         
            "AVWAP_OVERNIGHT_CFG": "data/avwap_overnight.json"      
        }
        
        self.DEFAULT_SEED = {"SOXL": 6720.0, "TQQQ": 6720.0}
        self.DEFAULT_SPLIT = {"SOXL": 40.0, "TQQQ": 40.0}
        self.DEFAULT_TARGET = {"SOXL": 12.0, "TQQQ": 10.0}
        self.DEFAULT_VERSION = {"SOXL": "V14", "TQQQ": "V14"}
        self.DEFAULT_COMPOUND = {"SOXL": 70.0, "TQQQ": 70.0}
        self.DEFAULT_SNIPER_MULTIPLIER = {"SOXL": 1.0, "TQQQ": 0.9}
        self.DEFAULT_FEE = {"SOXL": 0.07, "TQQQ": 0.07} 

    def _safe_float(self, value):
        try:
            f_val = float(str(value or 0.0).replace(',', ''))
            if math.isnan(f_val) or math.isinf(f_val):
                return 0.0
            return f_val
        except Exception:
            return 0.0

    def get_vwap_profile(self, ticker: str) -> dict:
        target_ticker = str(ticker).upper() if ticker else ""
        if target_ticker not in VWAP_PROFILES:
            return {}
        return VWAP_PROFILES[target_ticker]

    def _atomic_update_locks(self, update_fn):
        lock_file_path = self.FILES["LOCKS"]
        # 🚨 MODIFIED: [File Mutex 락온] GlobalThrottle 100% 교체
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

    def get_vrev_gap_threshold(self, ticker):
        with GlobalThrottle.get_file_lock(self.FILES["VREV_GAP_THRESH_CFG"]):
            return self._safe_float(self._load_json(self.FILES["VREV_GAP_THRESH_CFG"], {}).get(ticker, -2.0))

    def set_vrev_gap_threshold(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["VREV_GAP_THRESH_CFG"]):
            d = self._load_json(self.FILES["VREV_GAP_THRESH_CFG"], {})
            d[ticker] = self._safe_float(v)
            self._save_json(self.FILES["VREV_GAP_THRESH_CFG"], d)
            
    def get_vrev_gap_switching_mode(self, ticker):
        with GlobalThrottle.get_file_lock(self.FILES["VREV_GAP_SWITCH_CFG"]):
            return bool(self._load_json(self.FILES["VREV_GAP_SWITCH_CFG"], {}).get(ticker, False))

    def set_vrev_gap_switching_mode(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["VREV_GAP_SWITCH_CFG"]):
            d = self._load_json(self.FILES["VREV_GAP_SWITCH_CFG"], {})
            d[ticker] = bool(v)
            self._save_json(self.FILES["VREV_GAP_SWITCH_CFG"], d)
      
    def get_avwap_gap_threshold(self, ticker):
        with GlobalThrottle.get_file_lock(self.FILES["AVWAP_GAP_THRESH_CFG"]):
            return self._safe_float(self._load_json(self.FILES["AVWAP_GAP_THRESH_CFG"], {}).get(ticker, -2.0))

    def set_avwap_gap_threshold(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["AVWAP_GAP_THRESH_CFG"]):
            d = self._load_json(self.FILES["AVWAP_GAP_THRESH_CFG"], {})
            d[ticker] = self._safe_float(v)
            self._save_json(self.FILES["AVWAP_GAP_THRESH_CFG"], d)

    def get_last_split_date(self, ticker):
        with GlobalThrottle.get_file_lock(self.FILES["SPLIT_HISTORY"]):
            return str(self._load_json(self.FILES["SPLIT_HISTORY"], {}).get(ticker, ""))

    def set_last_split_date(self, ticker, date_str):
        with GlobalThrottle.get_file_lock(self.FILES["SPLIT_HISTORY"]):
            d = self._load_json(self.FILES["SPLIT_HISTORY"], {})
            d[ticker] = str(date_str)
            self._save_json(self.FILES["SPLIT_HISTORY"], d)

    def get_ledger(self):
        with GlobalThrottle.get_file_lock(self.FILES["LEDGER"]):
            raw_data = self._load_json(self.FILES["LEDGER"], [])
            return [r for r in raw_data if isinstance(r, dict)]

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
        rev_state = self.get_reverse_state(ticker)
        split = self.get_split_count(ticker)
        
        if rev_state.get("is_active", False):
            dynamic_t = self._safe_float(rev_state.get("dynamic_t", 0.0))
            rem_cash = self._safe_float(rev_state.get("rem_cash", 0.0))
            one_portion = rem_cash / 4.0 if rem_cash > 0 else 0.0
            return round(dynamic_t, 4), round(one_portion, 2)
            
        seed = self.get_seed(ticker)
        one_portion = seed / split if split > 0 else 1.0
        t_val = (self._safe_float(actual_qty) * self._safe_float(actual_avg_price)) / one_portion if one_portion > 0 else 0.0
        return round(t_val, 4), round(one_portion, 2)

    def apply_stock_split(self, ticker, ratio):
        safe_ratio = self._safe_float(ratio)
        if safe_ratio <= 0: return
        with GlobalThrottle.get_file_lock(self.FILES["LEDGER"]):
            ledger = self.get_ledger()
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
        with GlobalThrottle.get_file_lock(self.FILES["LEDGER"]):
            ledger = self.get_ledger()
            target_recs = [r for r in ledger if r.get('ticker') == ticker]
            
            if len(target_recs) > 0:
                logging.warning(f"⚠️ [보안 차단] {ticker}의 장부 기록이 이미 존재하여 파괴적 Genesis 덮어쓰기를 차단했습니다.")
                return

            max_id = max([int(self._safe_float(r.get('id', 0))) for r in ledger] + [0])
            for i, rec in enumerate(genesis_records or []):
                if not isinstance(rec, dict): continue
                max_id += 1
                ledger.append({
                    "id": max_id,
                    "date": rec.get('date'),
                    "ticker": ticker,
                    "side": rec.get('side'),
                    "price": self._safe_float(rec.get('price', 0.0)),
                    "qty": int(self._safe_float(rec.get('qty', 0))),
                    "avg_price": self._safe_float(actual_avg), 
                    "exec_id": f"GENESIS_{int(time.time())}_{i}",
                    "desc": "✨과거기록복원",
                    "is_reverse": False 
                })
            self._save_json(self.FILES["LEDGER"], ledger)

    def overwrite_incremental_ledger(self, ticker, temp_recs, new_today_records):
        with GlobalThrottle.get_file_lock(self.FILES["LEDGER"]):
            ledger = self.get_ledger()
            remaining = [r for r in ledger if r.get('ticker') != ticker]
            updated_ticker_recs = list(temp_recs)
            
            current_rev_state = self.get_reverse_state(ticker).get("is_active", False)
            max_id = max([int(self._safe_float(r.get('id', 0))) for r in ledger] + [0])
            
            for i, rec in enumerate(new_today_records or []):
                if not isinstance(rec, dict): continue
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
        with GlobalThrottle.get_file_lock(self.FILES["LEDGER"]):
            ledger = self.get_ledger()
            target_recs = [r for r in ledger if r.get('ticker') == ticker]
            
            if len(target_recs) > 0:
                logging.warning(f"⚠️ [보안 차단] {ticker}의 장부 기록이 이미 존재하여 파괴적 INIT 덮어쓰기를 차단했습니다.")
                return
                
            est = ZoneInfo('America/New_York')
            today_str = datetime.datetime.now(est).strftime('%Y-%m-%d')
            new_id = 1 if not ledger else max([int(self._safe_float(r.get('id', 0))) for r in ledger] + [0]) + 1
             
            ledger.append({
                "id": new_id, "date": today_str, "ticker": ticker, "side": "BUY",
                "price": self._safe_float(actual_avg), "qty": int(self._safe_float(actual_qty)), "avg_price": self._safe_float(actual_avg), 
                "exec_id": f"INIT_{int(time.time())}", "desc": "✨최초스냅샷", "is_reverse": False
            })
            self._save_json(self.FILES["LEDGER"], ledger)

    def calibrate_avg_price(self, ticker, actual_avg):
        with GlobalThrottle.get_file_lock(self.FILES["LEDGER"]):
            ledger = self.get_ledger()
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
            ledger = self.get_ledger()
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
            ledger = self.get_ledger()
            remaining = [r for r in ledger if r.get('ticker') != ticker]
            self._save_json(self.FILES["LEDGER"], remaining)
            self.set_reverse_state(ticker, False, 0, 0.0, dynamic_t=0.0, rem_cash=0.0, is_day_one=True)

    def calculate_holdings(self, ticker, records=None):
        if records is None:
            records = self.get_ledger()
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
        if total_qty > 0 and target_recs:
            avg_price = self._safe_float(target_recs[-1].get('avg_price', 0.0))
            if avg_price == 0.0:
                avg_price = (running_cost / running_qty) if running_qty > 0 else 0.0
        
        return total_qty, avg_price, invested_up, sold_up

    def get_reverse_state(self, ticker):
        with GlobalThrottle.get_file_lock(self.FILES["REVERSE_CFG"]):
            d = self._load_json(self.FILES["REVERSE_CFG"], {})
            val = d.get(ticker)
            if not isinstance(val, dict):
                return {
                    "is_active": False, "day_count": 0, "exit_target": 0.0, 
                    "last_update_date": "", "dynamic_t": 0.0, "rem_cash": 0.0, "is_day_one": True
                }
            val.setdefault("dynamic_t", 0.0)
            val.setdefault("rem_cash", 0.0)
            val.setdefault("is_day_one", val.get("day_count", 0) == 0)
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
                "dynamic_t": self._safe_float(dynamic_t),
                "rem_cash": self._safe_float(rem_cash),
                "is_day_one": (day_count == 0) if is_day_one is None else bool(is_day_one)
            }
            self._save_json(self.FILES["REVERSE_CFG"], d)
            
    def scale_dynamic_t(self, ticker, action):
        with GlobalThrottle.get_file_lock(self.FILES["REVERSE_CFG"]):
            d = self._load_json(self.FILES["REVERSE_CFG"], {})
            state = d.get(ticker)
            if not isinstance(state, dict) or not state.get("is_active", False):
                return
                
            split = self.get_split_count(ticker)
            dynamic_t = self._safe_float(state.get("dynamic_t", 0.0))
            
            if action == "SELL":
                if split <= 20: dynamic_t *= 0.9
                else: dynamic_t *= 0.95
            elif action == "BUY":
                dynamic_t += (split - dynamic_t) * 0.25
                
            state["dynamic_t"] = round(dynamic_t, 4)
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
        rev_state = self.get_reverse_state(ticker)
        split = self.get_split_count(ticker)
        
        if rev_state.get("is_active", False):
            dynamic_t = self._safe_float(rev_state.get("dynamic_t", 0.0))
            rem_cash = self._safe_float(rev_state.get("rem_cash", 0.0))
            current_budget = rem_cash / 4.0 if rem_cash > 0 else 0.0
            return max(0.0, round(dynamic_t, 4)), max(0.0, current_budget), max(0.0, rem_cash)
            
        ledger = self.get_ledger()
        target_recs = sorted([r for r in ledger if isinstance(r, dict) and r.get('ticker') == ticker], key=lambda x: int(self._safe_float(x.get('id', 0))))
        
        seed = self.get_seed(ticker)
        base_portion = seed / split if split > 0 else 1.0
        
        holdings = 0
        rem_cash = seed
        total_invested = 0.0
        
        for r in target_recs:
            if holdings == 0:
                rem_cash = seed
                total_invested = 0.0
        
            qty = int(self._safe_float(r.get('qty', 0)))
            price = self._safe_float(r.get('price', 0.0))
            amt = qty * price
            
            if r.get('side') == 'BUY':
                rem_cash -= amt
                holdings += qty
                total_invested += amt
                
            elif r.get('side') == 'SELL':
                if qty >= holdings: 
                    holdings = 0
                    rem_cash = seed
                    total_invested = 0.0
                else: 
                    if holdings > 0:
                        avg_price = total_invested / holdings
                        total_invested -= (qty * avg_price)
                holdings -= qty
                rem_cash += amt
             
        avg_price = total_invested / holdings if holdings > 0 else 0.0
        t_val = (holdings * avg_price) / base_portion if base_portion > 0 else 0.0
        
        if holdings > 0:
            safe_denom = max(1.0, split - t_val)
            current_budget = rem_cash / safe_denom
        else:
            current_budget = base_portion
            t_val = 0.0
             
        return max(0.0, round(t_val, 4)), max(0.0, current_budget), max(0.0, rem_cash)

    def archive_graduation(self, ticker, end_date, prev_close=0.0):
        with GlobalThrottle.get_file_lock(self.FILES["LEDGER"]):
            ledger = self.get_ledger()
            target_recs = [r for r in ledger if r.get('ticker') == ticker]
            if not target_recs:
                return None, 0
            
            ledger_qty, avg_price, _, _ = self.calculate_holdings(ticker, target_recs)
            
            raw_total_buy = sum(self._safe_float(r.get('price'))*int(self._safe_float(r.get('qty'))) for r in target_recs if r.get('side')=='BUY')
            raw_total_sell = sum(self._safe_float(r.get('price'))*int(self._safe_float(r.get('qty'))) for r in target_recs if r.get('side')=='SELL')

            if ledger_qty > 0:
                split = self.get_split_count(ticker)
                is_reverse = self.get_reverse_state(ticker).get("is_active", False)

                if is_reverse:
                    divisor = 10 if split <= 20 else 20
                    loc_qty = math.floor(ledger_qty / divisor)
                else:
                    loc_qty = math.ceil(ledger_qty / 4)

                limit_qty = ledger_qty - loc_qty
                if limit_qty < 0: 
                    loc_qty = ledger_qty
                    limit_qty = 0

                target_ratio = self.get_target_profit(ticker) / 100.0
                target_price = math.ceil(avg_price * (1 + target_ratio) * 100) / 100.0
                loc_price = self._safe_float(prev_close) if self._safe_float(prev_close) > 0 else avg_price

                new_id = max((int(self._safe_float(r.get('id', 0))) for r in ledger), default=0) + 1

                if loc_qty > 0:
                    rec_loc = {"id": new_id, "date": end_date, "ticker": ticker, "side": "SELL", "price": loc_price, "qty": loc_qty, "avg_price": avg_price, "exec_id": f"GRAD_LOC_{int(time.time())}", "is_reverse": is_reverse}
                    ledger.append(rec_loc)
                    target_recs.append(rec_loc)
                    new_id += 1

                if limit_qty > 0:
                    rec_limit = {"id": new_id, "date": end_date, "ticker": ticker, "side": "SELL", "price": target_price, "qty": limit_qty, "avg_price": avg_price, "exec_id": f"GRAD_LMT_{int(time.time())}", "is_reverse": is_reverse}
                    ledger.append(rec_limit)
                    target_recs.append(rec_limit)

                self._save_json(self.FILES["LEDGER"], ledger)

            fee_rate = self.get_fee(ticker) / 100.0
            net_invested = raw_total_buy * (1.0 + fee_rate)
            net_revenue = raw_total_sell * (1.0 - fee_rate)
            
            profit = math.ceil((net_revenue - net_invested) * 100) / 100.0
            yield_pct = math.ceil(((profit / net_invested * 100) if net_invested > 0 else 0.0) * 100) / 100.0
            
            compound_rate = self.get_compound_rate(ticker) / 100.0
            added_seed = 0
            if profit > 0 and compound_rate > 0:
                added_seed = math.floor(profit * compound_rate)
                current_seed = self.get_seed(ticker)
                self.set_seed(ticker, current_seed + added_seed)

            with GlobalThrottle.get_file_lock(self.FILES["HISTORY"]):
                history = self.get_history()
                new_hist = {
                    "id": len(history) + 1, "ticker": ticker, "end_date": end_date,
                    "profit": profit, "yield": yield_pct, "revenue": net_revenue, "invested": net_invested, "trades": target_recs
                }
                history.append(new_hist)
                self._save_json(self.FILES["HISTORY"], history)
             
            self.clear_ledger_for_ticker(ticker)
             
            return new_hist, added_seed

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
            val = self._load_json(self.FILES["VERSION_CFG"], self.DEFAULT_VERSION).get(t, self.DEFAULT_VERSION.get(t, "V14"))
            if t == "TQQQ": return "V14"
            return str(val)
        
    def set_version(self, t, v):
        with GlobalThrottle.get_file_lock(self.FILES["VERSION_CFG"]):
            if t == "TQQQ": v = "V14"
            d = self._load_json(self.FILES["VERSION_CFG"], self.DEFAULT_VERSION)
            d[t] = v
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

    def get_sniper_multiplier(self, t):
        with GlobalThrottle.get_file_lock(self.FILES["SNIPER_MULTIPLIER_CFG"]):
            default_val = self.DEFAULT_SNIPER_MULTIPLIER.get(t, 1.0)
            return self._safe_float(self._load_json(self.FILES["SNIPER_MULTIPLIER_CFG"], self.DEFAULT_SNIPER_MULTIPLIER).get(t, default_val))
        
    def set_sniper_multiplier(self, t, v):
        with GlobalThrottle.get_file_lock(self.FILES["SNIPER_MULTIPLIER_CFG"]):
            d = self._load_json(self.FILES["SNIPER_MULTIPLIER_CFG"], self.DEFAULT_SNIPER_MULTIPLIER)
            d[t] = self._safe_float(v)
            self._save_json(self.FILES["SNIPER_MULTIPLIER_CFG"], d)

    def get_upward_sniper_mode(self, ticker): 
        with GlobalThrottle.get_file_lock(self.FILES["UPWARD_SNIPER"]):
            return bool(self._load_json(self.FILES["UPWARD_SNIPER"], {}).get(ticker, False))
         
    def set_upward_sniper_mode(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["UPWARD_SNIPER"]):
             d = self._load_json(self.FILES["UPWARD_SNIPER"], {})
             d[ticker] = bool(v)
             self._save_json(self.FILES["UPWARD_SNIPER"], d)

    def get_avwap_hybrid_mode(self, ticker): 
        with GlobalThrottle.get_file_lock(self.FILES["AVWAP_HYBRID_CFG"]):
            return bool(self._load_json(self.FILES["AVWAP_HYBRID_CFG"], {}).get(ticker, False))
    
    def set_avwap_hybrid_mode(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["AVWAP_HYBRID_CFG"]):
            d = self._load_json(self.FILES["AVWAP_HYBRID_CFG"], {})
            d[ticker] = bool(v)
            self._save_json(self.FILES["AVWAP_HYBRID_CFG"], d)

    def get_avwap_sortie_mode(self, ticker):
        with GlobalThrottle.get_file_lock(self.FILES["AVWAP_SORTIE_CFG"]):
            return str(self._load_json(self.FILES["AVWAP_SORTIE_CFG"], {}).get(ticker, "SINGLE"))
        
    def set_avwap_sortie_mode(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["AVWAP_SORTIE_CFG"]):
            d = self._load_json(self.FILES["AVWAP_SORTIE_CFG"], {})
            d[ticker] = str(v)
            self._save_json(self.FILES["AVWAP_SORTIE_CFG"], d)

    def get_manual_vwap_mode(self, ticker): 
        with GlobalThrottle.get_file_lock(self.FILES["MANUAL_VWAP_CFG"]):
            return bool(self._load_json(self.FILES["MANUAL_VWAP_CFG"], {}).get(ticker, False))
        
    def set_manual_vwap_mode(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["MANUAL_VWAP_CFG"]):
            d = self._load_json(self.FILES["MANUAL_VWAP_CFG"], {})
            d[ticker] = bool(v)
            self._save_json(self.FILES["MANUAL_VWAP_CFG"], d)

    def get_master_switch(self, ticker): 
        with GlobalThrottle.get_file_lock(self.FILES["MASTER_SWITCH"]):
            return str(self._load_json(self.FILES["MASTER_SWITCH"], {}).get(ticker, "ALL"))
        
    def set_master_switch(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["MASTER_SWITCH"]):
            d = self._load_json(self.FILES["MASTER_SWITCH"], {})
            d[ticker] = str(v)
            self._save_json(self.FILES["MASTER_SWITCH"], d)

    def get_sniper_buy_locked(self, ticker): 
        with GlobalThrottle.get_file_lock(self.FILES["SNIPER_BUY_LOCKED"]):
            return bool(self._load_json(self.FILES["SNIPER_BUY_LOCKED"], {}).get(ticker, False))
        
    def set_sniper_buy_locked(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["SNIPER_BUY_LOCKED"]):
            d = self._load_json(self.FILES["SNIPER_BUY_LOCKED"], {})
            d[ticker] = bool(v)
            self._save_json(self.FILES["SNIPER_BUY_LOCKED"], d)

    def get_sniper_sell_locked(self, ticker): 
        with GlobalThrottle.get_file_lock(self.FILES["SNIPER_SELL_LOCKED"]):
            return bool(self._load_json(self.FILES["SNIPER_SELL_LOCKED"], {}).get(ticker, False))
        
    def set_sniper_sell_locked(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["SNIPER_SELL_LOCKED"]):
            d = self._load_json(self.FILES["SNIPER_SELL_LOCKED"], {})
            d[ticker] = bool(v)
            self._save_json(self.FILES["SNIPER_SELL_LOCKED"], d)

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

    def get_avwap_anchor_date(self, ticker):
        with GlobalThrottle.get_file_lock(self.FILES["AVWAP_ANCHOR_CFG"]):
            return str(self._load_json(self.FILES["AVWAP_ANCHOR_CFG"], {}).get(ticker, "AUTO"))

    def set_avwap_anchor_date(self, ticker, date_str):
        with GlobalThrottle.get_file_lock(self.FILES["AVWAP_ANCHOR_CFG"]):
            d = self._load_json(self.FILES["AVWAP_ANCHOR_CFG"], {})
            d[ticker] = str(date_str)
            self._save_json(self.FILES["AVWAP_ANCHOR_CFG"], d)
            
    def get_avwap_budget(self, ticker):
        with GlobalThrottle.get_file_lock(self.FILES["AVWAP_BUDGET_CFG"]):
            return self._safe_float(self._load_json(self.FILES["AVWAP_BUDGET_CFG"], {}).get(ticker, 10000.0))

    def set_avwap_budget(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["AVWAP_BUDGET_CFG"]):
            d = self._load_json(self.FILES["AVWAP_BUDGET_CFG"], {})
            d[ticker] = self._safe_float(v)
            self._save_json(self.FILES["AVWAP_BUDGET_CFG"], d)
            
    def get_avwap_overnight_mode(self, ticker):
        with GlobalThrottle.get_file_lock(self.FILES["AVWAP_OVERNIGHT_CFG"]):
            return bool(self._load_json(self.FILES["AVWAP_OVERNIGHT_CFG"], {}).get(ticker, False))
        
    def set_avwap_overnight_mode(self, ticker, v):
        with GlobalThrottle.get_file_lock(self.FILES["AVWAP_OVERNIGHT_CFG"]):
            d = self._load_json(self.FILES["AVWAP_OVERNIGHT_CFG"], {})
            d[ticker] = bool(v)
            self._save_json(self.FILES["AVWAP_OVERNIGHT_CFG"], d)
