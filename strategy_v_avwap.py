# ==========================================================
# FILE: strategy_v_avwap.py
# ==========================================================
# MODIFIED: [V59.00 AVWAP 암살자 예산 100% 수혈 및 15:25 전량 덤핑 팩트 교정]
# MODIFIED: [V60.00 옴니 매트릭스 진입 차단망 전면 폐기 및 데드코드 소각]
# MODIFIED: [V61.00 숏(SOXS) 전면 소각 작전 지시서 적용]
# NEW: [V65.00 AVWAP 동적 하드스탑 락온]
# NEW: [V66.00 AVWAP 암살자 덤핑 지터(Jitter) 분산 락온]
# NEW: [V75.04 상태 캐시 기억상실(Amnesia) 완벽 수술]
# MODIFIED: [V76.01 ATR5 동적 하드스탑 영구 소각 및 투트랙 엑시트 절대 락온]
# MODIFIED: [V76.02 타점 역전 패러독스 하드 마진 락온 (매니저 제안 수혈)]
# MODIFIED: [V76.03 암살자 덤핑 지터(Jitter) 코어 연산 디커플링 해체 및 동적 타임라인 락온]
# NEW: [V77.00 V7.1 백테스트 절대 동기화 롤백 (Animal Spirit 야성 회복)]
# MODIFIED: [V77.01 데이터 기아 방어 및 런타임 무결성 팩트 수술] 
# NEW: [V77.02 프리마켓 관제탑 데이터 기아 및 런타임 붕괴 완벽 수술]
# MODIFIED: [V77.03 갭상승 휩소 원천 차단 및 Strict Touch 절대 락온]
# NEW: [V77.04 Operation Dawn Sniper - 프리장 선제 타격 및 50% 팩트 오프셋 롤백]
# MODIFIED: [V77.06 3.0% 한계 돌파 팩트 롤백] 
# NEW: [V77.08] 백테스트 절대 동기화 - T_H 지정가 덫 선제 장전 및 상태기계 3.0% 청산 절대 락온
# MODIFIED: [V77.09] 타점 역전 패러독스 강제 캡핑(Clamping) 영구 소각 및 순수 수학적 교차(Cross-over) 허용
# MODIFIED: [V77.12] 추격 매수(Negative Slippage) 원천 차단 및 순수 지정가(T_H) 절대 락온 타격 엔진 이식
# MODIFIED: [V77.13 수학적 락온 및 환각 수술] 0주 예산 산출 시 상태 변이(Split-Brain) 원천 차단
# MODIFIED: [V77.14 백테스트 절대기준 동기화] 5분봉 과잉 방어 철거 및 순수 T_H 관통 타격 롤백
# MODIFIED: [V77.18 프리마켓 시계열 경계 누수 완벽 수술 및 T_H/T_L 절대 앵커 락온 (정규장 데이터 유입 원천 차단)]
# MODIFIED: [V77.21 09:30 기요틴 셧다운 락온] 정규장 T_L 하향 돌파 로직 영구 소각 및 프리장 체결 불발 시 09:30 정각 무조건 셧다운(퇴근) 적용
# NEW: [V77.30 관제탑 렌더링 무결성 사수] 암살자가 퇴근(Shutdown)해도 관제탑 레이더에 팩트 데이터가 영구 표출되도록 파싱 스코프 전진 배치 완료
# 🚨 NEW: [Case 11] 다중 출격(Multi-Sortie) 파이프라인 및 조건부 기요틴 엔진 팩트 락온
# 🚨 NEW: [Case 31 절대 위반 수술] 1분봉 시차 패러독스(Time-Shield) 원천 차단. 덫 장전 직후 60초간의 하향 돌파 노이즈를 멱등하게 바이패스하여 덫 자폭 방어막 완벽 이식 완료.
# 🚨 MODIFIED: [V78.00 팩트 교정] AVWAP 오프셋 연산 50% -> 45% 하향 락온 적용.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import math
import random
import yfinance as yf
import pandas as pd
import json
import os
import tempfile

class VAvwapHybridPlugin:
    def __init__(self):
        self.plugin_name = "AVWAP_V78.20_MULTI_SORTIE"
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

                if data.get('date') != today_str:
                    qty = data.get('qty', 0)
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

                    data['PM_H'] = 0.0
                    data['PM_L'] = 0.0
                    data['T_H'] = 0.0
                    data['T_L'] = 0.0
                    data['offset'] = 0.0
                    data['dump_jitter_sec'] = random.randint(0, 180)
                    
                    data.pop('pm_locked', None)

                    data['date'] = today_str
                    self.save_state(ticker, now_est, data)
                
                data['PM_H'] = float(data.get('PM_H', 0.0))
                data['PM_L'] = float(data.get('PM_L', 0.0))
                data['T_H'] = float(data.get('T_H', 0.0))
                data['T_L'] = float(data.get('T_L', 0.0))
                data['offset'] = float(data.get('offset', 0.0))
                data['executed_buy'] = bool(data.get('executed_buy', False))
                
                data['limit_order_placed'] = bool(data.get('limit_order_placed', False))
                data['placed_target_th'] = float(data.get('placed_target_th', 0.0))
                data['trap_placed_time'] = str(data.get('trap_placed_time', ""))

                return data
            except Exception:
                pass

        return {
            "executed_buy": False, "shutdown": False, "strikes": 0, "qty": 0, 
            "avg_price": 0.0, "daily_bought_qty": 0, "daily_sold_qty": 0, 
            "dump_jitter_sec": random.randint(0, 180),
            "PM_H": 0.0, "PM_L": 0.0, "T_H": 0.0, "T_L": 0.0, "offset": 0.0,
            "limit_order_placed": False, "placed_target_th": 0.0, "trap_placed_time": ""
        }

    def save_state(self, ticker, now_est, state_data):
        file_path = self._get_state_file(ticker, now_est)
        today_str = self._get_logical_date_str(now_est)

        merged_data = {}
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    merged_data = json.load(f)
            except Exception:
                pass

        if merged_data.get('date') != today_str:
            merged_data = {}

        merged_data.update(state_data)
        merged_data['date'] = today_str

        try:
            dir_name = os.path.dirname(file_path)
            if dir_name and not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)

            fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(merged_data, f, ensure_ascii=False, indent=4)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, file_path)
        except Exception as e:
            logging.error(f"🚨 [V_AVWAP] 상태 저장 실패 (원자적 쓰기 에러): {e}")

    def fetch_macro_context(self, base_ticker):
        try:
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
                        prev_close = float(df_prev_day['Close'].iloc[-1])
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
                    avg_vol_20 = float(past_first_30m['Volume'].tail(20).mean())
                elif len(past_first_30m) > 0:
                    avg_vol_20 = float(past_first_30m['Volume'].mean())

            if prev_vwap == 0.0:
                prev_vwap = prev_close

            return {
                "prev_close": prev_close,
                "prev_vwap": prev_vwap,
                "avg_vol_20": avg_vol_20
            }

        except Exception as e:
            logging.error(f"🚨 [V_AVWAP] YF 기초자산 매크로 컨텍스트 추출 실패 ({base_ticker}): {e}")
            return None

    def get_decision(self, base_ticker=None, exec_ticker=None, base_curr_p=0.0, exec_curr_p=0.0, base_day_open=0.0, avwap_avg_price=0.0, avwap_qty=0, avwap_alloc_cash=0.0, context_data=None, df_1min_base=None, df_1min_exec=None, now_est=None, avwap_state=None, regime_data=None, is_simulation=False, sortie_mode="SINGLE", **kwargs):
        avwap_qty = avwap_qty if avwap_qty != 0 else kwargs.get('current_qty', 0)
        exec_curr_p = exec_curr_p if exec_curr_p > 0 else kwargs.get('exec_curr_p', 0.0)
        avwap_avg_price = avwap_avg_price if avwap_avg_price > 0 else kwargs.get('avwap_avg_price', kwargs.get('avg_price', 0.0))
        avwap_alloc_cash = avwap_alloc_cash if avwap_alloc_cash > 0 else kwargs.get('alloc_cash', kwargs.get('avwap_alloc_cash', 0.0))
        amp5 = float(kwargs.get('amp5', 0.0))
        prev_c = float(kwargs.get('prev_close', 0.0))
        
        curr_pm_h = 0.0
        curr_pm_l = 0.0
        curr_c = 0.0
        curr_l = 0.0
        curr_offset = 0.0
        curr_t_h = 0.0
        curr_t_l = 0.0
        curr_candle_time_str = "" 
        
        now_est = now_est or datetime.datetime.now(ZoneInfo('America/New_York'))
        curr_time = now_est.time()
        
        time_0400 = datetime.time(4, 0)
        time_0930 = datetime.time(9, 30)

        persistent_state = self.load_state(exec_ticker, now_est)
        is_shutdown = persistent_state.get('shutdown', False)
        executed_buy = persistent_state.get('executed_buy', False)
        
        limit_order_placed = persistent_state.get('limit_order_placed', False)
        placed_target_th = persistent_state.get('placed_target_th', 0.0)
        trap_placed_time = avwap_state.get('trap_placed_time', "") if avwap_state else persistent_state.get('trap_placed_time', "")
        
        dump_jitter_sec = persistent_state.get('dump_jitter_sec', 0)
        base_dump_dt = datetime.datetime.combine(now_est.date(), datetime.time(15, 20)).replace(tzinfo=ZoneInfo('America/New_York'))
        dynamic_dump_dt = base_dump_dt - datetime.timedelta(seconds=dump_jitter_sec)
        time_dynamic_dump = dynamic_dump_dt.time()
        
        pm_h = persistent_state.get('PM_H', 0.0)
        pm_l = persistent_state.get('PM_L', 0.0)
        t_h = persistent_state.get('T_H', 0.0)
        t_l = persistent_state.get('T_L', 0.0)
        offset = persistent_state.get('offset', 0.0)

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
                        curr_pm_h = float(df_pm['close'].max())
                        curr_pm_l = float(df_pm['close'].min())
                    else:
                        curr_pm_h = 0.0
                        curr_pm_l = 0.0

                    curr_c = float(df_today.iloc[-1]['close'])
                    curr_l = float(df_today.iloc[-1]['low'])
                    
                    # 🚨 MODIFIED: [V78.00 팩트 교정] 진폭 오프셋 50% -> 45% 하향 락온
                    curr_offset = prev_c * amp5 * 0.45
                    
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
                'prev_vwap': context_data.get('prev_vwap', 0.0) if context_data else 0.0,
                'PM_H': pm_h,
                'PM_L': pm_l,
                'T_H': t_h,
                'T_L': t_l,
                'offset': offset,
                'limit_order_placed': limit_order_placed,
                'placed_target_th': placed_target_th,
                'trap_placed_time': trap_placed_time if t_time is None else t_time 
            }

        if avwap_qty > 0:
            safe_avg = avwap_avg_price if avwap_avg_price > 0 else exec_curr_p

            if safe_avg <= 0:
                return _build_res('SELL', 'CORRUPT_PRICE_EMERGENCY_DUMP', qty=avwap_qty, target_price=exec_curr_p)

            if curr_time >= time_dynamic_dump:
                persistent_state["shutdown"] = True
                if not is_simulation:
                    self.save_state(exec_ticker, now_est, persistent_state)
                return _build_res('SELL', '동적_덤핑_타임라인_도달_전량_시장가_덤핑', qty=avwap_qty, target_price=exec_curr_p)

            # 🚨 MODIFIED: [V78.00] 투트랙 익절 목표가 2.0% 락온 (2.0% 도달 시 즉각 수익 확정)
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

        if prev_c <= 0 or amp5 <= 0:
            return _build_res('WAIT', '진입_평가용_필수데이터_결측_대기')
            
        if executed_buy and sortie_mode == "SINGLE":
            return _build_res('WAIT', '일일_1회_타격_완료_매매_종료(단일타격_모드)')

        if curr_time >= time_0930 and not executed_buy:
            persistent_state["shutdown"] = True
            persistent_state["limit_order_placed"] = False
            persistent_state["placed_target_th"] = 0.0
            persistent_state["trap_placed_time"] = ""
            limit_order_placed = False
            placed_target_th = 0.0
            trap_placed_time = ""
            
            if not is_simulation:
                self.save_state(exec_ticker, now_est, persistent_state)
                
            logging.info(f"🛑 [09:30 기요틴 셧다운] 프리장 매수 체결 불발. 정규장 폭락 휩소를 회피하기 위해 당일 매매를 종료(퇴근)합니다.")
            return _build_res('SHUTDOWN', '09:30_기요틴_프리장미체결_정규장회피_당일퇴근')
            
        if not limit_order_placed:
            if curr_l > 0 and curr_l <= t_h:
                safe_budget = avwap_alloc_cash * 0.95
                buy_qty = int(math.floor(safe_budget / t_h)) if t_h > 0 else 0
                
                if buy_qty > 0:
                    persistent_state['limit_order_placed'] = True
                    persistent_state['placed_target_th'] = t_h
                    persistent_state['trap_placed_time'] = curr_candle_time_str 
                    limit_order_placed = True
                    placed_target_th = t_h
                    trap_placed_time = curr_candle_time_str
                    
                    if not is_simulation:
                        self.save_state(exec_ticker, now_est, persistent_state)
                    
                    logging.info(f"🚀 [V77.14 덫 장전] 1분봉 저가({curr_l:.2f}) T_H 순수 관통. 지정가({placed_target_th:.2f}) 타격 락온! (기준 캔들: {curr_candle_time_str})")
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
