# ==========================================================
# FILE: assassin_ledger.py
# ==========================================================
# 🚨 NEW: [암살자 100% 독립 장부 도메인] 오버나이트 허용 시 본진 물량 탈취(Ghost Selling)를 막기 위한 물리적 디커플링
# 🚨 MODIFIED: [제4헌법 준수] 원자적 쓰기(Atomic Write) 강제 락온 및 .bak 파일 듀얼 세이프티
# 🚨 MODIFIED: [Case 16 위반 교정] 임시 파일 스코프 최상단 전진 배치(Hoisting)
# 🚨 MODIFIED: [Insight 14 & 25] NaN, Infinity 및 String-Comma 맹독성 데이터 방어막 100% 내재화
# 🚨 MODIFIED: [Lost Update 궁극 방어] 인스턴스 락(self._lock) 영구 소각 및 GlobalThrottle 파일 뮤텍스 강제 래핑 완료.
# ==========================================================

import os
import json
import time
import math
import shutil
import tempfile
from zoneinfo import ZoneInfo
from datetime import datetime
import logging
from global_throttle import GlobalThrottle # 🚨 전역 락 엔진

class AssassinLedger:
    def __init__(self, file_path="data/assassin_ledger.json"):
        self.file_path = file_path
        # 🚨 인스턴스 락 영구 소각 완료
        self._ensure_file()

    def _safe_float(self, value):
        try:
            val = float(str(value or 0.0).replace(',', ''))
            if math.isnan(val) or math.isinf(val):
                return 0.0
            return val
        except Exception:
            return 0.0

    def _ensure_file(self):
        # 🚨 전역 파일 뮤텍스로 원자성 보장
        with GlobalThrottle.get_file_lock(self.file_path):
            try:
                dir_name = os.path.dirname(self.file_path) or '.'
                os.makedirs(dir_name, exist_ok=True)
            except OSError:
                pass
            try:
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    pass
            except FileNotFoundError:
                self._save_unsafe_no_lock({})
            except Exception:
                pass

    def _get_trading_date_str(self):
        est = ZoneInfo('America/New_York')
        return datetime.now(est).strftime("%Y-%m-%d")

    def _load_unsafe_no_lock(self):
        """ 🚨 호출부에서 GlobalThrottle Lock을 잡고 진입하므로 순수 읽기만 수행 """
        last_exc = None
        for attempt in range(3):
            try:
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if not content.strip():
                        return {} 
                    return json.loads(content)
            except json.JSONDecodeError as e:
                last_exc = e
                break
            except FileNotFoundError:
                return {}
            except Exception as e:
                last_exc = e
                time.sleep(1.0 * (2 ** attempt))
        
        backup_path = self.file_path + ".bak"
        try:
            with open(backup_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logging.warning(f"🚨 [AssassinLedger] JSON 손상 감지. 백업 파일({backup_path}) 복원 완료.")
                try:
                    self._save_unsafe_no_lock(data)
                except Exception as heal_e:
                    logging.error(f"🚨 [AssassinLedger] 자가 치유 I/O 통신 에러: {heal_e}")
                return data
        except FileNotFoundError:
            pass
        except Exception as be:
            logging.error(f"🚨 [AssassinLedger] 백업 복원도 실패: {be}")
        
        raise RuntimeError(f"🚨 [FATAL ERROR] {self.file_path} 암살자 장부 읽기 실패. 원인: {last_exc}")

    def _save_unsafe_no_lock(self, data):
        """ 🚨 호출부에서 GlobalThrottle Lock을 잡고 진입하므로 순수 쓰기만 수행 """
        dir_name = os.path.dirname(self.file_path) or '.'
        for attempt in range(3):
            fd = None
            tmp_path = None
            try:
                fd, tmp_path = tempfile.mkstemp(dir=dir_name, text=True)
                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                    fd = None
                    json.dump(data, f, ensure_ascii=False, indent=4)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, self.file_path)
                tmp_path = None
                
                bak_path = self.file_path + ".bak"
                bak_tmp_path = bak_path + ".tmp"
                try: 
                    shutil.copy2(self.file_path, bak_tmp_path)
                    os.replace(bak_tmp_path, bak_path)
                except Exception:
                    try: os.remove(bak_tmp_path)
                    except OSError: pass
                
                return
            except Exception as e:
                logging.warning(f"⚠️ [AssassinLedger] 암살자 장부 저장 재시도 ({attempt+1}/3): {e}")
                if fd is not None:
                    try: os.close(fd)
                    except OSError: pass
                if tmp_path:
                    try: os.remove(tmp_path)
                    except OSError: pass
                time.sleep(1.0 * (2 ** attempt))
                   
        logging.error(f"🚨 [AssassinLedger] 장부 저장 최종 실패: {self.file_path}")

    def apply_stock_split(self, ticker, ratio):
        if ratio <= 0: return
        with GlobalThrottle.get_file_lock(self.file_path):
            data = self._load_unsafe_no_lock()
            q = data.get(ticker, [])
            changed = False
            for lot in q:
                old_qty = int(self._safe_float(lot.get("qty", 0)))
                raw_new_qty = old_qty * ratio
                new_qty = math.floor(raw_new_qty + 0.5)
                lot["qty"] = new_qty if new_qty > 0 else (1 if old_qty > 0 else 0)
                
                old_price = self._safe_float(lot.get("price", 0.0))
                lot["price"] = round(old_price / ratio, 4)
                changed = True
            if changed:
                data[ticker] = q
                self._save_unsafe_no_lock(data)

    def get_ledger(self, ticker):
        with GlobalThrottle.get_file_lock(self.file_path):
            data = self._load_unsafe_no_lock()
            q = data.get(ticker, [])
            return [lot for lot in q if int(self._safe_float(lot.get("qty"))) > 0]

    def add_lot(self, ticker, qty, price, lot_type="ASSASSIN_BUY"):
        qty = int(self._safe_float(qty))
        if qty <= 0: return
        
        price_f = self._safe_float(price)
        if price_f <= 0.0:
            logging.error(f"🚨 [AssassinLedger] add_lot 중단: {ticker} — 유효하지 않은 매수 가격 (price={price}). 로트 추가 취소.")
            return
            
        with GlobalThrottle.get_file_lock(self.file_path):
            data = self._load_unsafe_no_lock()
            q = data.get(ticker, [])
            q = [lot for lot in q if int(self._safe_float(lot.get("qty"))) > 0] 
            
            today_str = self._get_trading_date_str()
            
            if q and str(q[-1].get("date", "")).startswith(today_str):
                old_qty = int(self._safe_float(q[-1].get("qty")))
                old_price = self._safe_float(q[-1].get("price"))
                
                new_qty = old_qty + qty
                new_price = ((old_qty * old_price) + (qty * price_f)) / new_qty if new_qty > 0 else 0.0
                
                q[-1]["qty"] = new_qty
                q[-1]["price"] = round(new_price, 4)
                q[-1]["date"] = datetime.now(ZoneInfo('America/New_York')).strftime("%Y-%m-%d %H:%M:%S")
            else:
                q.append({
                    "qty": qty,
                    "price": price_f, 
                    "date": datetime.now(ZoneInfo('America/New_York')).strftime("%Y-%m-%d %H:%M:%S"),
                    "type": lot_type
                })
            
            data[ticker] = q
            self._save_unsafe_no_lock(data)

    def pop_lots(self, ticker, target_qty, sold_price=0.0):
        original_target = int(self._safe_float(target_qty))
        target_qty = original_target
        if target_qty <= 0: return 0
        
        with GlobalThrottle.get_file_lock(self.file_path):
            data = self._load_unsafe_no_lock()
            q = data.get(ticker, [])
            q = [lot for lot in q if int(self._safe_float(lot.get("qty"))) > 0] 
            
            if not q: return 0
            
            popped_total = 0

            while q and target_qty > 0:
                last_lot = q[-1]
                lot_qty = int(self._safe_float(last_lot.get("qty")))
                
                if lot_qty == 0:
                    q.pop()
                    continue
                    
                if lot_qty <= target_qty:
                    popped = q.pop()
                    popped_qty = int(self._safe_float(popped.get("qty")))
                    popped_total += popped_qty
                    target_qty -= popped_qty
                else:
                    last_lot["qty"] = lot_qty - target_qty
                    popped_total += target_qty
                    target_qty = 0
                    
            if popped_total < original_target:
                logging.warning(f"⚠️ [AssassinLedger] pop_lots 미달: {ticker} — 요청 {original_target}주 중 {popped_total}주만 차감.")

            data[ticker] = q
            self._save_unsafe_no_lock(data)
            return popped_total

    def clear_ledger(self, ticker):
        with GlobalThrottle.get_file_lock(self.file_path):
            data = self._load_unsafe_no_lock()
            data[ticker] = []
            self._save_unsafe_no_lock(data)
