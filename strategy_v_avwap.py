# ==========================================================
# FILE: strategy_v_avwap.py
# ==========================================================
# 🚨 NEW: [멱등성 수술] 액면분할 시 AVWAP 캐시 팩트를 정밀 보정하는 apply_stock_split 이식 완료
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import math
import random
import time 
import yfinance as yf
import pandas as pd
import numpy as np 
import json
import os
import tempfile

class VAvwapHybridPlugin:
    def __init__(self):
        self.plugin_name = "AVWAP_V79.50_MA5_ANCHOR"
        self.leverage = 3.0       

    def _get_logical_date_str(self, now_est):
        if now_est.hour < 4 or (now_est.hour == 4 and now_est.minute < 4):
            target_date = now_est - datetime.timedelta(days=1)
        else:
            target_date = now_est
        return target_date.strftime('%Y%m%d')

    def _get_state_file(self, ticker, now_est):
        return f"data/avwap_state_persistent_{ticker}.json"

    def load_state(self, ticker, now_est):
        file_path = self._get_state_file(ticker, now_est)
        today_str = self._get_logical_date_str(now_est)

        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if not isinstance(data, dict):
                    data = {}

                if data.get('date') != today_str:
                    qty = int(float(str(data.get('qty') or 0).replace(',', ''))) 
                    if qty > 0:
                        data['bought'] = True
                        data['shutdown'] = False
                        data['executed_buy'] = True 
                    else:
                        data['qty'] = 0
                        data['avg_price'] = 0.0
                        data['shutdown'] = False
                        data['strikes'] = 0
                        data['bought'] = False
                        data['daily_bought_qty'] = 0
                        data['daily_sold_qty'] = 0
                        data['executed_buy'] = False
                        
                        data['limit_order_placed'] = False
                        data['placed_target_th'] = 0.0
                        data['trap_placed_time'] = ""
                        data['buy_odno'] = ""  

                    data['PM_H'] = 0.0
                    data['PM_L'] = 0.0
                    data['T_H'] = 0.0
                    data['T_L'] = 0.0
                    data['offset'] = 0.0
                    data['dump_jitter_sec'] = random.randint(0, 180)
                    
                    data.pop('pm_locked', None)

                    data['date'] = today_str
                    self.save_state(ticker, now_est, data)
                
                data['PM_H'] = float(str(data.get('PM_H') or 0.0).replace(',', ''))
                data['PM_L'] = float(str(data.get('PM_L') or 0.0).replace(',', ''))
                data['T_H'] = float(str(data.get('T_H') or 0.0).replace(',', ''))
                data['T_L'] = float(str(data.get('T_L') or 0.0).replace(',', ''))
                data['offset'] = float(str(data.get('offset') or 0.0).replace(',', ''))
                data['executed_buy'] = bool(data.get('executed_buy'))
                
                data['limit_order_placed'] = bool(data.get('limit_order_placed'))
                data['placed_target_th'] = float(str(data.get('placed_target_th') or 0.0).replace(',', ''))
                data['trap_placed_time'] = str(data.get('trap_placed_time') or "")
                data['buy_odno'] = str(data.get('buy_odno') or "") 

                return data
            except Exception:
                pass

        return {
            "executed_buy": False, "shutdown": False, "strikes": 0, "qty": 0, 
            "avg_price": 0.0, "daily_bought_qty": 0, "daily_sold_qty": 0, 
            "dump_jitter_sec": random.randint(0, 180),
            "PM_H": 0.0, "PM_L": 0.0, "T_H": 0.0, "T_L": 0.0, "offset": 0.0,
            "limit_order_placed": False, "placed_target_th": 0.0, "trap_placed_time": "", "buy_odno": ""
        }

    def save_state(self, ticker, now_est, state_data):
        file_path = self._get_state_file(ticker, now_est)
        today_str = self._get_logical_date_str(now_est)

        merged_data = {}
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    merged_data = json.load(f)
                if not isinstance(merged_data, dict):
                    merged_data = {}
            except Exception:
                pass

        if merged_data.get('date') != today_str:
            merged_data = {}

        merged_data.update(state_data)
        merged_data['date'] = today_str

        try:
            dir_name = os.path.dirname(file_path) or '.'
            if not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)

            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(merged_data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno()) 
            os.replace(temp_path, file_path)
        except Exception as e:
            logging.error(f"🚨 [V_AVWAP] 상태 저장 실패 (원자적 쓰기 에러): {e}")

    # 🚨 NEW: [멱등성 수술] AVWAP 암살자 상태 캐시 액면분할 정밀 소급 적용
    def apply_stock_split(self, ticker, ratio, now_est):
        if ratio <= 0: return
        state = self.load_state(ticker, now_est)
        qty = int(float(str(state.get("qty", 0)).replace(',', '')))
        if qty > 0:
            new_qty = math.floor((qty * ratio) + 0.5)
            old_avg = float(str(state.get("avg_price", 0.0)).replace(',', ''))
            state["qty"] = new_qty
            state["avg_price"] = round(old_avg / ratio, 4)
            
            daily_bought = int(float(str(state.get("daily_bought_qty", 0)).replace(',', '')))
            daily_sold = int(float(str(state.get("daily_sold_qty", 0)).replace(',', '')))
            state["daily_bought_qty"] = math.floor((daily_bought * ratio) + 0.5)
            state["daily_sold_qty"] = math.floor((daily_sold * ratio) + 0.5)
            
            placed_target_th = float(str(state.get("placed_target_th", 0.0)).replace(',', ''))
            if placed_target_th > 0:
                state["placed_target_th"] = round(placed_target_th / ratio, 2)
            
            self.save_state(ticker, now_est, state)

    def fetch_macro_context(self, base_ticker):
        for attempt in range(3):
            try:
                time.sleep(0.06) 
                tkr = yf.Ticker(base_ticker)
                df_1m = tkr.history(period="5d", interval="1m", prepost=False, timeout=5)
    
                prev_vwap = 0.0
                prev_close = 0.0
    
                est = ZoneInfo('America/New_York')
                now_est = datetime.datetime.now(est)
    
                if now_est.hour < 4 or (now_est.hour == 4 and now_est.minute < 5):
                    today_est = (now_est - datetime.timedelta(days=1)).date()
                else:
                    today_est = now_est.date()
    
                if not df_1m.empty:
                    if df_1m.index.tz is None:
                        df_1m.index = df_1m.index.tz_localize('UTC').tz_convert(est)
                    else:
                        df_1m.index = df_1m.index.tz_convert(est)
    
                    df_past_1m = df_1m[df_1m.index.date < today_est].copy()
    
                    if not df_past_1m.empty:
                        last_date = df_past_1m.index.date[-1]
                        df_prev_day = df_past_1m[df_past_1m.index.date == last_date].copy()
                        df_prev_day = df_prev_day.between_time('09:30', '15:59')
    
                        if not df_prev_day.empty:
                            prev_close = float(np.nan_to_num(df_prev_day['Close'].iloc[-1], nan=0.0))
                            df_prev_day['tp'] = (df_prev_day['High'].astype(float) + df_prev_day['Low'].astype(float) + df_prev_day['Close'].astype(float)) / 3.0
                            df_prev_day['vol'] = df_prev_day['Volume'].astype(float)
                            df_prev_day['vol_tp'] = df_prev_day['tp'] * df_prev_day['vol']
    
                            cum_vol = df_prev_day['vol'].sum()
                            if cum_vol > 0:
                                prev_vwap = df_prev_day['vol_tp'].sum() / cum_vol
                            else:
                                prev_vwap = prev_close
    
                df_30m = tkr.history(period="60d", interval="30m", timeout=5)
                avg_vol_20 = 0.0
    
                if not df_30m.empty:
                    if df_30m.index.tz is None:
                        df_30m.index = df_30m.index.tz_localize('UTC').tz_convert(est)
                    else:
                        df_30m.index = df_30m.index.tz_convert(est)
    
                    first_30m = df_30m[df_30m.index.time == datetime.time(9, 30)]
                    past_first_30m = first_30m[first_30m.index.date < today_est]
    
                    if len(past_first_30m) >= 20:
                        avg_vol_20 = float(np.nan_to_num(past_first_30m['Volume'].tail(20).mean(), nan=0.0))
                    elif len(past_first_30m) > 0:
                        avg_vol_20 = float(np.nan_to_num(past_first_30m['Volume'].mean(), nan=0.0))
    
                if prev_vwap == 0.0:
                    prev_vwap = prev_close
    
                return {
                    "prev_close": prev_close,
                    "prev_vwap": prev_vwap,
                    "avg_vol_20": avg_vol_20
                }
    
            except Exception as e:
                logging.debug(f"⚠️ [V_AVWAP] YF 기초자산 매크로 컨텍스트 추출 오류 (시도 {attempt+1}/3): {e}")
                if attempt == 2: return None
                time.sleep(1.0 * (2 ** attempt))

    def get_decision(self, base_ticker=None, exec_ticker=None, base_curr_p=0.0, exec_curr_p=0.0, base_day_open=0.0, avwap_avg_price=0.0, avwap_qty=0, avwap_alloc_cash=0.0, context_data=None, df_1min_base=None, df_1min_exec=None, now_est=None, avwap_state=None, regime_data=None, is_simulation=False, sortie_mode="SINGLE", **kwargs):
        is_holiday = kwargs.get('is_holiday', False)
        now_est = now_est or datetime.datetime.now(ZoneInfo('America/New_York'))
        
        if now_est.weekday() >= 5:
            is_holiday = True

        avwap_qty = avwap_qty if avwap_qty != 0 else kwargs.get('current_qty', 0)
        exec_curr_p = exec_curr_p if exec_curr_p > 0 else kwargs.get('exec_curr_p', 0.0)
        avwap_avg_price = avwap_avg_price if avwap_avg_price > 0 else kwargs.get('avwap_avg_price', kwargs.get('avg_price', 0.0))
        avwap_alloc_cash = avwap_alloc_cash if avwap_alloc_cash > 0 else kwargs.get('alloc_cash', kwargs.get('avwap_alloc_cash', 0.0))
        
        amp5 = float(kwargs.get('amp5', 0.0))
        prev_c = float(kwargs.get('prev_close', 0.0))
        ma_5day = float(kwargs.get('ma_5day', 0.0))
        
        anchor_price = ma_5day if ma_5day > 0 else prev_c
        
        curr_pm_h = 0.0
        curr_pm_l = 0.0
        curr_c = 0.0
        curr_l = 0.0
        curr_offset = 0.0
        curr_t_h = 0.0
        curr_t_l = 0.0
        curr_candle_time_str = "" 
        
        curr_time = now_est.time()
        
        time_0400 = datetime.time(4, 0)
        time_0930 = datetime.time(9, 30)
        time_1300 = datetime.time(13, 0)

        persistent_state = self.load_state(exec_ticker, now_est)
        
        is_shutdown = bool(persistent_state.get('shutdown'))
        executed_buy = bool(persistent_state.get('executed_buy'))
        limit_order_placed = bool(persistent_state.get('limit_order_placed'))
        placed_target_th = float(str(persistent_state.get('placed_target_th') or 0.0).replace(',', ''))
        
        trap_placed_time = str((avwap_state or {}).get('trap_placed_time') or persistent_state.get('trap_placed_time') or "")
        
        dump_jitter_sec = int(float(str(persistent_state.get('dump_jitter_sec') or 0).replace(',', '')))
        pm_h = float(str(persistent_state.get('PM_H') or 0.0).replace(',', ''))
        pm_l = float(str(persistent_state.get('PM_L') or 0.0).replace(',', ''))
        t_h = float(str(persistent_state.get('T_H') or 0.0).replace(',', ''))
        t_l = float(str(persistent_state.get('T_L') or 0.0).replace(',', ''))
        offset = float(str(persistent_state.get('offset') or 0.0).replace(',', ''))
        
        base_dump_dt = datetime.datetime.combine(now_est.date(), datetime.time(15, 20)).replace(tzinfo=ZoneInfo('America/New_York'))
        dynamic_dump_dt = base_dump_dt - datetime.timedelta(seconds=dump_jitter_sec)
        time_dynamic_dump = dynamic_dump_dt.time()

        if curr_time >= time_0400:
            df_1m = df_1min_exec
            if df_1m is not None and not df_1m.empty and 'time_est' in df_1m.columns:
                curr_time_str_raw = curr_time.strftime('%H%M%S')
                df_today = df_1m[(df_1m['time_est'] >= '040000') & (df_1m['time_est'] <= curr_time_str_raw)]
                
                if not df_today.empty:
                    curr_candle_time_str = str(df_today.iloc[-1]['time_est'])

                    slice_end_str = '092959' if curr_time >= time_0930 else curr_time_str_raw
                    df_pm = df_1m[(df_1m['time_est'] >= '040000') & (df_1m['time_est'] <= slice_end_str)]
                    
                    if not df_pm.empty:
                        curr_pm_h = float(np.nan_to_num(df_pm['close'].astype(float).max(), nan=0.0))
                        curr_pm_l = float(np.nan_to_num(df_pm['close'].astype(float).min(), nan=0.0))
                    else:
                        curr_pm_h = 0.0
                        curr_pm_l = 0.0

                    curr_c = float(np.nan_to_num(df_today.iloc[-1].get('close', 0.0), nan=0.0))
                    curr_l = float(np.nan_to_num(df_today.iloc[-1].get('low', 0.0), nan=0.0))
                    
                    curr_offset = anchor_price * amp5 * 0.45
                    
                    curr_t_h = curr_pm_h - curr_offset
                    curr_t_l = curr_pm_l + curr_offset

                    pm_h = curr_pm_h
                    pm_l = curr_pm_l
                    t_h = curr_t_h
                    t_l = curr_t_l
                    offset = curr_offset

                    persistent_state['PM_H'] = pm_h
                    persistent_state['PM_L'] = pm_l
                    persistent_state['T_H'] = t_h
                    persistent_state['T_L'] = t_l
                    persistent_state['offset'] = offset
                    
                    if not is_simulation:
                        self.save_state(exec_ticker, now_est, persistent_state)

        def _build_res(action, reason, qty=0, target_price=0.0, t_time=None):
            return {
                'action': action,
                'reason': reason,
                'qty': qty,
                'target_price': target_price,
                'vwap': 0.0,
                'base_curr_p': base_curr_p,
                'prev_vwap': (context_data or {}).get('prev_vwap', 0.0) if isinstance(context_data, dict) else 0.0,
                'PM_H': pm_h,
                'PM_L': pm_l,
                'T_H': t_h,
                'T_L': t_l,
                'offset': offset,
                'limit_order_placed': limit_order_placed,
                'placed_target_th': placed_target_th,
                'trap_placed_time': trap_placed_time if t_time is None else t_time,
                'buy_odno': str(persistent_state.get('buy_odno') or "") 
            }

        if is_holiday:
            if avwap_qty > 0:
                return _build_res('HOLD', '미국_증시_휴장일(보유물량_이월)')
            return _build_res('WAIT', '미국_증시_휴장일(관측_중지)')

        if anchor_price <= 0 or amp5 <= 0:
            return _build_res('WAIT', '진입_평가용_필수데이터_결측_대기')

        if avwap_qty > 0:
            safe_avg = avwap_avg_price if avwap_avg_price > 0 else exec_curr_p

            if safe_avg <= 0:
                return _build_res('SELL', 'CORRUPT_PRICE_EMERGENCY_DUMP', qty=avwap_qty, target_price=exec_curr_p)

            if curr_time >= time_dynamic_dump:
                persistent_state["shutdown"] = True
                if not is_simulation:
                    self.save_state(exec_ticker, now_est, persistent_state)
                return _build_res('SELL', '동적_덤핑_타임라인_도달_전량_시장가_덤핑', qty=avwap_qty, target_price=exec_curr_p)

            exit_target_price = round(safe_avg * 1.02, 2)
            if exec_curr_p >= exit_target_price:
                return _build_res('SELL', '목표가(+2.0%)_도달_순수모멘텀_익절_격발', qty=avwap_qty, target_price=exit_target_price)

            return _build_res('HOLD', '보유중_순수익절(+2.0%)_및_동적덤핑_감시중')

        if is_shutdown:
            return _build_res('WAIT', '당일영구동결_상태(신규진입금지)')

        if curr_time >= time_dynamic_dump:
            persistent_state["shutdown"] = True
            if not is_simulation:
                self.save_state(exec_ticker, now_est, persistent_state)
            return _build_res('SHUTDOWN', '동적_덤핑_타임라인_도달_신규진입_영구동결')
            
        if executed_buy and sortie_mode == "SINGLE":
            return _build_res('WAIT', '일일_1회_타격_완료_매매_종료(단일타격_모드)')

        if curr_time >= time_0930 and not executed_buy:
            persistent_state["shutdown"] = True
            persistent_state["limit_order_placed"] = False
            persistent_state["placed_target_th"] = 0.0
            persistent_state["trap_placed_time"] = ""
            persistent_state["buy_odno"] = ""  
            limit_order_placed = False
            placed_target_th = 0.0
            trap_placed_time = ""
            
            if not is_simulation:
                self.save_state(exec_ticker, now_est, persistent_state)
                
            logging.info(f"🛑 [09:30 기요틴 셧다운] 프리장 매수 체결 불발. 정규장 폭락 휩소를 회피하기 위해 당일 매매를 종료(퇴근)합니다.")
            return _build_res('SHUTDOWN', '09:30_기요틴_프리장미체결_정규장회피_당일퇴근')

        if avwap_qty == 0 and curr_time >= time_1300 and not limit_order_placed:
            persistent_state["shutdown"] = True
            persistent_state["limit_order_placed"] = False
            persistent_state["placed_target_th"] = 0.0
            persistent_state["trap_placed_time"] = ""
            persistent_state["buy_odno"] = ""  
            limit_order_placed = False
            placed_target_th = 0.0
            trap_placed_time = ""
            
            if not is_simulation:
                self.save_state(exec_ticker, now_est, persistent_state)
            
            return _build_res('SHUTDOWN', '13:00_장마감3시간전_신규진입_타임라인_영구차단(SHUTDOWN)')
            
        if not limit_order_placed:
            if curr_l > 0 and curr_l <= t_h:
                safe_budget = avwap_alloc_cash * 0.95
                buy_qty = max(0, int(math.floor(safe_budget / t_h))) if t_h > 0 else 0 
                
                if buy_qty > 0:
                    persistent_state['limit_order_placed'] = True
                    persistent_state['placed_target_th'] = t_h
                    persistent_state['trap_placed_time'] = curr_candle_time_str 
                    limit_order_placed = True
                    placed_target_th = t_h
                    trap_placed_time = curr_candle_time_str
                    
                    if not is_simulation:
                        self.save_state(exec_ticker, now_est, persistent_state)
                    
                    logging.info(f"🚀 [V79.50 덫 장전] 1분봉 저가({curr_l:.2f}) T_H 순수 관통. 지정가({placed_target_th:.2f}) 타격 락온! (기준 캔들: {curr_candle_time_str})")
                    return _build_res('PLACE_TRAP', 'T_H순수관통_지정가_덫장전', qty=buy_qty, target_price=placed_target_th, t_time=curr_candle_time_str)
                else:
                    return _build_res('WAIT', '조건_충족이나_예산부족(0주)_덫장전_보류')
        else:
            is_time_shield_active = False
            if trap_placed_time and curr_candle_time_str:
                try:
                    t1 = datetime.datetime.strptime(trap_placed_time, '%H%M%S')
                    t2 = datetime.datetime.strptime(curr_candle_time_str, '%H%M%S')
                    diff_sec = (t2 - t1).total_seconds()
                    if diff_sec < 0: 
                        diff_sec += 86400
                    if 0 <= diff_sec <= 60:
                        is_time_shield_active = True
                except Exception:
                    pass

            if curr_l > 0 and curr_l <= placed_target_th:
                if is_time_shield_active:
                    logging.info(f"🛡️ [Case 31 시차 패러독스 방어] 1분봉 시차 패러독스 차단: 장전시각({trap_placed_time}) 직후 캔들({curr_candle_time_str}) 노이즈 관통 바이패스. 덫 자폭(Self-Destruct) 방어!")
                    return _build_res('TRAP_WAIT', f'주문전송_지연방어(1분패러독스)_지정가덫({placed_target_th:.2f})_시장대기중', qty=0, target_price=placed_target_th)
                return _build_res('VERIFY_TRAP_FILL', '지정가덫_하향관통_실체결검증_및_익절덫동시투하_요청', qty=0, target_price=placed_target_th)
            else:
                reason_msg = f'선제지정가덫({placed_target_th:.2f})_시장대기중'
                if is_time_shield_active:
                    reason_msg = f'1분봉_시차패러독스_타임쉴드_가동중({placed_target_th:.2f})'
                return _build_res('TRAP_WAIT', reason_msg, qty=0, target_price=placed_target_th)

        return _build_res('WAIT', '동적_순수타격선_도달_감시중')
