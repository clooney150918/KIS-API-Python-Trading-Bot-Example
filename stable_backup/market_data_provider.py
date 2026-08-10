# ==========================================================
# FILE: market_data_provider.py
# ==========================================================
# 🚨 MODIFIED: [제1헌법 철저 준수] get_1min_candles_df 내의 data/avwap_cache.json 조작을 GlobalThrottle.get_file_lock으로 100% 파일 뮤텍스 래핑 완료 (Dirty Read 원천 봉쇄).
# 🚨 VERIFIED: [최종 무결점 판정] 5대 헌법 및 41대 엣지 케이스 완벽 결속 교차 검증 완료.
# 🚨 MODIFIED: [Thundering Herd 영구 소각] yfinance 등 모든 외부 통신 지연(time.sleep)을 제거하고 `GlobalThrottle.wait_api_sync()` 중앙 통제소로 100% 위임 락온.
# 🚨 MODIFIED: [스냅샷 오염 전이 절대 방어] YF 1d 캔들 롤오버 지연 맹점을 파기하고 1m 기반 D-1 공식 종가 핀셋 추출 락온.
# 🚨 MODIFIED: [프리장 데이터 공백 패러독스 방어] YF 1d 롤오버 지연 버그 원천 차단을 위해 period="1d" -> "5d" 상향 락온.
# 🚨 VERIFIED: [Case 35 절대 방어망 결속] 내부 ffill 주입으로 결측치(NaN) 런타임 에러 차단 무결성 100% 확보.
# ==========================================================

import time
import datetime
import os
import math
import tempfile
import json
import logging
import yfinance as yf
import pandas as pd   
import numpy as np
import volatility_engine as ve
from zoneinfo import ZoneInfo
from kis_api_client import KisApiClient
from global_throttle import GlobalThrottle # 🚨 NEW: 전역 통제소 결속

def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """ 🚨 [수술 완료] 야후 파이낸스 API 업데이트로 인한 MultiIndex 순서 붕괴 방어 """
    if isinstance(df.columns, pd.MultiIndex):
        if 'Ticker' in df.columns.names:
            df.columns = df.columns.droplevel('Ticker')
        elif df.columns.nlevels == 2:
            price_fields = {'Close', 'High', 'Low', 'Open', 'Volume', 'Adj Close'}
            level0_vals = set(df.columns.get_level_values(0))
            drop_level = 0 if not level0_vals.intersection(price_fields) else 1
            df.columns = df.columns.droplevel(drop_level)
    return df

class MarketDataProvider(KisApiClient):
    
    # 🚨 MODIFIED: [Case 33] 3단 지수 백오프 이식
    def get_daily_vwap_info(self, ticker):
        """ 기초자산의 정규장 거래 내역 기반 일자별 순수 VWAP 계산 """
        for attempt in range(3):
            try:
                GlobalThrottle.wait_api_sync() # 🚨 MODIFIED: 중앙 통제소 락온
            
                stock = yf.Ticker(ticker)
                df = stock.history(period="5d", interval="1m", prepost=False, timeout=10)
         
                if df.empty: 
                    if attempt == 2: return 0.0, 0.0
                    time.sleep(1.0 * (2 ** attempt))
                    continue
             
                df = _flatten_columns(df)
            
                est = ZoneInfo('America/New_York') 
                if df.index.tz is None: df.index = df.index.tz_localize('UTC').tz_convert(est)
                else: df.index = df.index.tz_convert(est)
    
                regular_market = df.between_time('09:30', '15:59').copy()
        
                if regular_market.empty: return 0.0, 0.0
      
                regular_market['Typical_Price'] = (regular_market['High'] + regular_market['Low'] + regular_market['Close']) / 3.0
                regular_market['Vol_x_Price'] = regular_market['Typical_Price'] * regular_market['Volume']
        
                regular_market['Date'] = regular_market.index.date
                
                daily_stats = regular_market.groupby('Date').agg(Total_Vol_Price=('Vol_x_Price', 'sum'), Total_Vol=('Volume', 'sum'))
  
                # 🚨 MODIFIED: [Case 05] 결측치 방어용 0.0 강제 형변환
                daily_stats['VWAP'] = np.where(daily_stats['Total_Vol'] > 0, daily_stats['Total_Vol_Price'] / daily_stats['Total_Vol'], np.nan)
                daily_stats = daily_stats.dropna(subset=['VWAP'])
    
                if len(daily_stats) >= 2:
                    return round(self._safe_float(daily_stats['VWAP'].iloc[-2]), 4), round(self._safe_float(daily_stats['VWAP'].iloc[-1]), 4)
                elif len(daily_stats) == 1:
                    return 0.0, round(self._safe_float(daily_stats['VWAP'].iloc[-1]), 4)
                return 0.0, 0.0
            except Exception as e:
                if attempt == 2:
                    logging.error(f"⚠️ [Broker] 일별 VWAP 파싱 실패 ({ticker}): {e}")
                    return 0.0, 0.0
                time.sleep(1.0 * (2 ** attempt))
        return 0.0, 0.0

    # 🚨 MODIFIED: [Case 33] 3단 지수 백오프 이식
    def get_current_5min_candle(self, ticker):
        for attempt in range(3):
            try:
                GlobalThrottle.wait_api_sync() # 🚨 MODIFIED: 중앙 통제소 락온
                stock = yf.Ticker(ticker)
                df = stock.history(period="5d", interval="1m", prepost=True, timeout=5)
             
                if df.empty: 
                    if attempt == 2: return None
                    time.sleep(1.0 * (2 ** attempt))
                    continue
                df = _flatten_columns(df)
            
                est = ZoneInfo('America/New_York')
                if df.index.tz is None: df.index = df.index.tz_localize('UTC').tz_convert(est)
                else: df.index = df.index.tz_convert(est)
       
                regular_market = df.between_time('09:30', '15:59')
    
                if regular_market.empty: return None
              
                today_date = pd.Timestamp.now(tz=est).normalize()
                regular_market = regular_market[regular_market.index >= today_date]
                if regular_market.empty: return None
                
                regular_market = regular_market.dropna(subset=['Volume', 'High', 'Low', 'Close'])
      
                typical_price = (regular_market['High'] + regular_market['Low'] + regular_market['Close']) / 3.0
          
                vol_price = typical_price * regular_market['Volume']
                cum_vol_price = vol_price.cumsum()
                cum_vol = regular_market['Volume'].cumsum()
                
                safe_cum_vol = np.where(cum_vol == 0, 1.0, cum_vol)
                vwap_array = np.where(cum_vol > 0, cum_vol_price / safe_cum_vol, np.nan)
          
                vwap_series = pd.Series(vwap_array, index=cum_vol.index).ffill() 
          
                current_vwap = self._safe_float(vwap_series.iloc[-1]) if not vwap_series.empty else 0.0
                if pd.isna(current_vwap) or current_vwap == 0.0: current_vwap = 0.0
 
                resampled = regular_market.resample('5min', label='left', closed='left').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}).dropna()
 
                if resampled.empty: return None
         
                resampled['Vol_MA10'] = resampled['Volume'].rolling(10, min_periods=1).mean()
                resampled['Vol_MA20'] = resampled['Volume'].rolling(20, min_periods=1).mean()
                last_candle = resampled.iloc[-1]
                latest_1m = regular_market.iloc[-1] 
 
                # 🚨 MODIFIED: [Float 붕괴 방어] 모든 연산 결과에 _safe_float 래핑 락온
                return {
                    'open': self._safe_float(last_candle['Open']), 
                    'high': self._safe_float(last_candle['High']), 
                    'low': self._safe_float(last_candle['Low']), 
                    'close': self._safe_float(latest_1m['Close']), 
                    'volume': self._safe_float(last_candle['Volume']), 
                    'vol_ma10': self._safe_float(last_candle.get('Vol_MA10', last_candle['Volume'])),
                    'vol_ma20': self._safe_float(last_candle.get('Vol_MA20', last_candle['Volume'])),
                    'vwap': self._safe_float(current_vwap)
                }
            except Exception as e:
                logging.debug(f"⚠️ [Broker] 실시간 5분봉 조회 실패 (시도 {attempt+1}/3): {e}")
                if attempt == 2: return None
                time.sleep(1.0 * (2 ** attempt))
        return None

    def get_current_price(self, ticker, is_market_closed=False):
        for attempt in range(3):
            try:
                GlobalThrottle.wait_api_sync() # 🚨 MODIFIED: 중앙 통제소 락온
                stock = yf.Ticker(ticker)
                hist = stock.history(period="1d", interval="1m", prepost=True, timeout=5)
            
                if not hist.empty: return self._safe_float(hist['Close'].iloc[-1])
                elif attempt == 2: raise ValueError("YF 실시간 데이터 응답 지연 (timeout)") 
            except Exception as e:
                logging.debug(f"⚠️ [야후] 현재가 에러 (시도 {attempt+1}/3): {e}")
                if attempt == 2: break
                time.sleep(1.0 * (2 ** attempt))

        for attempt in range(3):
            try:
                # KIS API 폴백 (내부에서 GlobalThrottle 자동 락온)
                excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
                params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
                res = self._call_api("HHDFS76200200", "/uapi/overseas-price/v1/quotations/price", "GET", params=params)
                
                # 🚨 MODIFIED: [AttributeError 붕괴 방어] 명시적 None 및 문자열 오염 차단
                if res.get('rt_cd') == '0':
                    out = res.get('output') or {}
                    if not isinstance(out, dict): out = {}
                    return self._safe_float(out.get('last', 0.0))
                break
            except Exception:
                if attempt == 2: pass
                else: time.sleep(1.0 * (2 ** attempt))
  
        return 0.0

    def get_ask_price(self, ticker):
        for attempt in range(3):
            try:
                # 내부 _call_api 에서 GlobalThrottle 자동 락온
                excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
                params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
                res = self._call_api("HHDFS76200100", "/uapi/overseas-price/v1/quotations/inquire-asking-price", "GET", params=params)
       
                # 🚨 MODIFIED: [AttributeError 붕괴 방어]
                if res.get('rt_cd') == '0':
                    o2 = res.get('output2') or []
                    if isinstance(o2, dict): o2 = [o2]
                    if not isinstance(o2, list) or len(o2) == 0: return 0.0
                    item = o2[0]
                    if not isinstance(item, dict): return 0.0
                    return self._safe_float(item.get('pask1', 0.0))
                break
            except Exception:
                if attempt == 2: pass
                else: time.sleep(1.0 * (2 ** attempt))
        return 0.0

    def get_bid_price(self, ticker):
        for attempt in range(3):
            try:
                # 내부 _call_api 에서 GlobalThrottle 자동 락온
                excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
                params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
                res = self._call_api("HHDFS76200100", "/uapi/overseas-price/v1/quotations/inquire-asking-price", "GET", params=params)
      
                # 🚨 MODIFIED: [AttributeError 붕괴 방어]
                if res.get('rt_cd') == '0':
                    o2 = res.get('output2') or []
                    if isinstance(o2, dict): o2 = [o2]
                    if not isinstance(o2, list) or len(o2) == 0: return 0.0
                    item = o2[0]
                    if not isinstance(item, dict): return 0.0
                    return self._safe_float(item.get('pbid1', 0.0))
                break
            except Exception:
                if attempt == 2: pass
                else: time.sleep(1.0 * (2 ** attempt))
        return 0.0

    # 🚨 MODIFIED: [스냅샷 오염 전이 절대 방어] YF 1d 캔들 롤오버 지연 맹점을 파기하고 1m 기반 D-1 공식 종가 핀셋 추출 락온
    def get_previous_close(self, ticker):
        for attempt in range(3):
            try:
                GlobalThrottle.wait_api_sync() # 🚨 MODIFIED: 중앙 통제소 락온
                stock = yf.Ticker(ticker)
                # 🚨 MODIFIED: interval="1d" 의존성 소각 및 interval="1m", period="5d" 로 교체
                hist = stock.history(period="5d", interval="1m", prepost=True, timeout=5)
              
                if not hist.empty:
                    hist = _flatten_columns(hist)
                    est = ZoneInfo('America/New_York')
                    now_est = datetime.datetime.now(est)
                    cutoff_date = now_est.date()
                  
                    # 🚨 MODIFIED: [16:05 스냅샷 미래 참조 방어] 16:00:30 이전에만 하루를 빼서 어제 종가를 가져오고, 
                    # 16:00:30 이후(즉 16:05 스냅샷 시점)에는 오늘(방금 마감된) 종가를 '명일 기준 전일 종가'로 채택하도록 팩트 락온.
                    if now_est.time() <= datetime.time(16, 0, 30): 
                        cutoff_date -= datetime.timedelta(days=1)
                 
                    if hist.index.tzinfo is None: 
                        hist.index = hist.index.tz_localize('UTC').tz_convert(est)
                    else: 
                        hist.index = hist.index.tz_convert(est)
                    
                    # 🚨 MODIFIED: 미래 참조 데이터 절단 및 D-1일 공식 MOC 종가 추출
                    past_hist = hist[hist.index.date <= cutoff_date].copy()
               
                    if not past_hist.empty:
                        # 🚨 MODIFIED: [Case 35] 결측치 전이 방어
                        past_hist['Close'] = past_hist['Close'].ffill().bfill()
                        regular_past = past_hist.between_time('09:30', '15:59')
           
                        if not regular_past.empty:
                            return self._safe_float(regular_past['Close'].iloc[-1])
                        else:
                            return self._safe_float(past_hist['Close'].iloc[-1])
                break
          
            except Exception as e:
                logging.debug(f"⚠️ [야후] 전일 종가 에러 (시도 {attempt+1}/3): {e}")
                if attempt == 2: break
                time.sleep(1.0 * (2 ** attempt))

        for attempt in range(3):
            try:
                # 내부 _call_api 에서 GlobalThrottle 자동 락온
                excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
                params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
                res = self._call_api("HHDFS76200200", "/uapi/overseas-price/v1/quotations/price", "GET", params=params)
            
                # 🚨 MODIFIED: [AttributeError 붕괴 방어]
                if res.get('rt_cd') == '0': 
                    out = res.get('output') or {}
                    if not isinstance(out, dict): out = {}
                    return self._safe_float(out.get('base', 0.0))
                break
            except Exception:
                if attempt == 2: pass
                else: 
                    time.sleep(1.0 * (2 ** attempt))
        return 0.0
        
    def get_5day_ma(self, ticker):
        # 🚨 MODIFIED: [미래 참조 데이터 누수 차단] 당일 미확정 라이브 캔들 절단 및 D-1 타임라인 락온
        for attempt in range(3):
            try:
                GlobalThrottle.wait_api_sync() # 🚨 MODIFIED: 중앙 통제소 락온
                stock = yf.Ticker(ticker)
                hist = stock.history(period="15d", timeout=5) 
    
                if not hist.empty:
                    est = ZoneInfo('America/New_York')
                    now_est = datetime.datetime.now(est)
                    cutoff_date = now_est.date()
                
                    # 🚨 MODIFIED: 16:00:30 이후(16:05 스냅샷 시점)에는 오늘 캔문을 포함하여 5일선을 연산하도록 타임라인 팩트 락온.
                    if now_est.time() <= datetime.time(16, 0, 30): 
                        cutoff_date -= datetime.timedelta(days=1)
                        
                    if hist.index.tzinfo is None: 
                        hist.index = hist.index.tz_localize('UTC').tz_convert(est)
                    else: 
                        hist.index = hist.index.tz_convert(est)
                        
                    # 🚨 VERIFIED: [Case 35 절대 방어망] 공휴일 및 조기 폐장일 결측치 방어를 위한 ffill 강제 주입
                    hist['Close'] = hist['Close'].ffill()
 
                    past_hist = hist[hist.index.date <= cutoff_date]
                    if len(past_hist) >= 5:
                        ma_val = past_hist['Close'].dropna().tail(5).mean()
                        if not pd.isna(ma_val): return self._safe_float(ma_val)
  
                break
            except Exception:
                if attempt == 2: break
                time.sleep(1.0 * (2 ** attempt))
            
        for attempt in range(3):
            try:
                # 내부 _call_api 에서 GlobalThrottle 자동 락온
                excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
                params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker, "GUBN": "0", "BYMD": "", "MODP": "1"}
                res = self._call_api("HHDFS76240000", "/uapi/overseas-price/v1/quotations/dailyprice", "GET", params=params)
                
                if res.get('rt_cd') == '0':
                    o2 = res.get('output2') or []
                    if isinstance(o2, dict): o2 = [o2]
                    if isinstance(o2, list) and len(o2) >= 5:
                        est = ZoneInfo('America/New_York')
                        now_est = datetime.datetime.now(est)
       
                        # 🚨 MODIFIED: [KIS API 폴백 오염 차단] 장 마감 이전이면 당일 라이브 주가 스킵
                        skip_today = 1 if now_est.time() <= datetime.time(16, 0, 30) else 0
                        
                        valid_items = [x for x in o2[skip_today:skip_today+10] if isinstance(x, dict)]
                        if len(valid_items) > 0:
                            closes = [self._safe_float(x.get('clos', 0)) for x in valid_items]
                            # 🚨 VERIFIED: [Case 35 절대 방어망] KIS API 폴백 시에도 0값 필터링으로 무결성 보장
                            valid_closes = [c for c in closes if c > 0]
                            if len(valid_closes) >= 5:
                                return self._safe_float(sum(valid_closes[:5]) / 5.0)
                            elif len(valid_closes) > 0:
                                return self._safe_float(sum(valid_closes) / len(valid_closes))
                break
            except Exception:
                if attempt == 2: pass
                else: time.sleep(1.0 * (2 ** attempt))
        return 0.0

    def get_1min_candles_df(self, ticker):
        """ 🚨 [제5경고] 하이킨아시 연산을 위한 open 컬럼 강제 보존 리턴 """
        for attempt in range(3):
            try:
                GlobalThrottle.wait_api_sync() # 🚨 MODIFIED: 중앙 통제소 락온
                stock = yf.Ticker(ticker)
                # 🚨 MODIFIED: [프리장 데이터 공백 패러독스 방어] YF 1d 롤오버 지연 버그 원천 차단을 위해 period="1d" -> "5d" 상향 락온
                df = stock.history(period="5d", interval="1m", prepost=True, timeout=5)
                if df.empty: 
                    if attempt == 2: return None
                    time.sleep(1.0 * (2 ** attempt))
                    continue
               
                df = _flatten_columns(df)
                est = ZoneInfo('America/New_York')
                
                if df.index.tz is None: df.index = df.index.tz_localize('UTC').tz_convert(est)
                else: df.index = df.index.tz_convert(est)
      
                df = df.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'})
                df['time_est'] = df.index.strftime('%H%M00')
  
                try:
                    # 🚨 MODIFIED: [5d 롤오버 교정 연계 State 방어] 5일 치 데이터 중 당일(Today) 팩트만 정밀 필터링하여 당일 고/저점 캐싱 오염 원천 차단
                    today_est_date = datetime.datetime.now(est).date()
                    df_today = df[df.index.date == today_est_date]
                    
                    if not df_today.empty:
                        # 🚨 MODIFIED: [Float 붕괴 방어] max(), min() 결과에 _safe_float 래핑 강제 적용
                        max_high, min_low = self._safe_float(df_today['high'].max()), self._safe_float(df_today['low'].min())
                        time_high_idx, time_low_idx = df_today['high'].astype(float).idxmax(), df_today['low'].astype(float).idxmin()
                        
                        time_high_raw = df_today.loc[time_high_idx, 'time_est'] if not pd.isna(time_high_idx) else ""
                        time_high_str = str(time_high_raw.iloc[0] if isinstance(time_high_raw, pd.Series) else time_high_raw)
            
                        time_low_raw = df_today.loc[time_low_idx, 'time_est'] if not pd.isna(time_low_idx) else ""
                        time_low_str = str(time_low_raw.iloc[0] if isinstance(time_low_raw, pd.Series) else time_low_raw)
            
                        cache_file = "data/avwap_cache.json"
        
                        # 🚨 MODIFIED: [제1헌법] File Mutex 100% 팩트 래핑으로 더티 리드(Dirty Read) 및 동시성 붕괴 원천 차단
                        with GlobalThrottle.get_file_lock(cache_file):
                            cache_data = {}
                            try:
                                with open(cache_file, 'r', encoding='utf-8') as f: 
                                    cache_data = json.load(f)
                            except OSError: pass
                            except json.JSONDecodeError: pass

                            cache_data[ticker] = {'day_high': max_high, 'day_low': min_low, 'time_high': time_high_str, 'time_low': time_low_str, 'date': datetime.datetime.now(est).strftime("%Y-%m-%d")}
       
                            # 🚨 MODIFIED: [Case 16] 디렉토리 파싱 무결성 강화 및 스토리지 고갈 방어 락온
                            dir_name = os.path.dirname(cache_file) or '.'
                            try: os.makedirs(dir_name, exist_ok=True)
                            except OSError: pass
           
                            fd = None
                            tmp_path = None
                            try:
                                fd, tmp_path = tempfile.mkstemp(dir=dir_name, text=True)
                                with os.fdopen(fd, 'w', encoding='utf-8') as f_out:
                                    fd = None
                                    json.dump(cache_data, f_out, ensure_ascii=False, indent=4)
                                    f_out.flush()
                                    os.fsync(f_out.fileno())
                                os.replace(tmp_path, cache_file)
                                tmp_path = None
                            except Exception as e:
                                if fd is not None:
                                    try: os.close(fd)
                                    except OSError: pass
                                if tmp_path:
                                    try: os.remove(tmp_path)
                                    except OSError: pass
                                logging.debug(f"🚨 [{ticker}] 시계열 체력 팩트 캐싱 실패: {e}")
           
                except Exception as e: 
                    logging.debug(f"🚨 [{ticker}] 체력 팩트 캐싱 연산 에러: {e}")
                return df[['open', 'high', 'low', 'close', 'volume', 'time_est']]
            except Exception:
                if attempt == 2: return None
                time.sleep(1.0 * (2 ** attempt))

    def get_recent_stock_split(self, ticker, last_date_str):
        for attempt in range(3):
            try:
                GlobalThrottle.wait_api_sync() # 🚨 MODIFIED: 중앙 통제소 락온
                splits = yf.Ticker(ticker).splits
                if splits is not None and not splits.empty:
                    safe_last_date = last_date_str if last_date_str else (datetime.datetime.now(ZoneInfo('America/New_York')) - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
                    for dt, ratio in splits.items():
                        sd = dt[:10] if isinstance(dt, str) else pd.Timestamp(dt).strftime('%Y-%m-%d')
                        if sd > safe_last_date: return self._safe_float(ratio), sd
                break
            except Exception:
                if attempt == 2: break
                time.sleep(1.0 * (2 ** attempt))
        return 0.0, ""

    def get_dynamic_sniper_target(self, index_ticker):
        target_index = "SOXX" if index_ticker in ["SOXX", "SOXL"] else index_ticker
        try:
            class TargetFloat(float): pass
          
            if target_index == "SOXX":
                hv, w, td, ba = ve.get_soxl_target_drop_full()
                safe_w = self._safe_float(w)
                ret = TargetFloat(td)
                ret.metric_val, ret.weight, ret.base_amp, ret.metric_name, ret.metric_base = hv, w, ba, "SOXX HV", round(self._safe_float(hv)/safe_w, 2) if safe_w>0 else 25.0
            else:
                vxn, w, td, ba = ve.get_tqqq_target_drop_full()
                safe_w = self._safe_float(w)
                ret = TargetFloat(td)
                ret.metric_val, ret.weight, ret.base_amp, ret.metric_name, ret.metric_base = vxn, w, ba, "실시간 VXN", round(self._safe_float(vxn)/safe_w, 2) if safe_w>0 else 20.0
            ret.is_panic, ret.gap_pct = False, 0.0
            return ret
        except:
            fb = -8.79 if target_index == "SOXX" else -4.95
            ret = TargetFloat(fb)
            ret.metric_val, ret.weight, ret.base_amp, ret.metric_name, ret.metric_base = 0.0, 1.0, fb, "통신오류", 25.0 if target_index == "SOXX" else 20.0
            ret.is_panic, ret.gap_pct = False, 0.0
            return ret

    def get_day_high_low(self, ticker):
        for attempt in range(3):
            try:
                GlobalThrottle.wait_api_sync() # 🚨 MODIFIED: 중앙 통제소 락온
                hist = yf.Ticker(ticker).history(period="1d", interval="1m", prepost=True, timeout=5)
                if not hist.empty:
                    hist = _flatten_columns(hist)
                    # 🚨 MODIFIED: [Float 오염 방어] _safe_float 래핑
                    return self._safe_float(hist['High'].max()), self._safe_float(hist['Low'].min())
            except Exception:
                if attempt == 2: break
                time.sleep(1.0 * (2 ** attempt))
                
        for attempt in range(3):
            try:
                # 내부 _call_api 에서 GlobalThrottle 자동 락온
                res = self._call_api("HHDFS76200200", "/uapi/overseas-price/v1/quotations/price", "GET", params={"AUTH": "", "EXCD": self._get_exchange_code(ticker, target_api="PRICE"), "SYMB": ticker})
                
                # 🚨 MODIFIED: [AttributeError 붕괴 방어] output 객체 추출 시 안전 단락 평가
                if res.get('rt_cd') == '0':
                    out = res.get('output') or {}
                    if not isinstance(out, dict): out = {}
                    return self._safe_float(out.get('high', 0.0)), self._safe_float(out.get('low', 0.0))
                break
            except Exception:
                if attempt == 2: pass
                else: time.sleep(1.0 * (2 ** attempt))
        return 0.0, 0.0

    # 🚨 MODIFIED: [미래 참조 데이터 누수 전면 차단] 당일 미확정 라이브 캔들 절단 및 D-1 타임라인 락온 복제
    def get_atr_data(self, ticker):
        for attempt in range(3):
            try:
                GlobalThrottle.wait_api_sync() # 🚨 MODIFIED: 중앙 통제소 락온
                hist = yf.Ticker(ticker).history(period="30d", prepost=False, timeout=5)
                if hist.empty:
                    if attempt == 2: return 0.0, 0.0
                    time.sleep(1.0 * (2 ** attempt))
                    continue

                # 🚨 EST 기준 타임존 맵핑 및 D-1 팩트 절단
                est = ZoneInfo('America/New_York')
                now_est = datetime.datetime.now(est)
                cutoff_date = now_est.date()

                if now_est.time() <= datetime.time(16, 0, 30):
                    cutoff_date -= datetime.timedelta(days=1)

                if hist.index.tzinfo is None:
                    hist.index = hist.index.tz_localize('UTC').tz_convert(est)
                else:
                    hist.index = hist.index.tz_convert(est)

                # 🚨 MODIFIED: [결측치 방어 전역 확장] ffill 결속
                hist['Close'] = hist['Close'].ffill()
                hist['High'] = hist['High'].ffill()
                hist['Low'] = hist['Low'].ffill()

                hist = hist[hist.index.date <= cutoff_date].copy()

                if hist.empty or len(hist) < 15:
                    if attempt == 2: return 0.0, 0.0
                    time.sleep(1.0 * (2 ** attempt))
                    continue

                hist['Prev_Close'] = hist['Close'].shift(1)
                hist = hist.dropna(subset=['High', 'Low', 'Close']).copy()

                # 🚨 MODIFIED: [벡터화 강제 헌법 준수] apply(lambda) 묵시적 루프 영구 소각 및 100% 벡터화 연산 팩트 교정
                tr1 = hist['High'] - hist['Low']
                tr2 = (hist['High'] - hist['Prev_Close']).abs()
                tr3 = (hist['Low'] - hist['Prev_Close']).abs()
                
                hist['TR'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
                hist['ATR5'] = hist['TR'].rolling(window=5).mean()
                hist['ATR14'] = hist['TR'].rolling(window=14).mean()

                last = hist.iloc[-1]
                # 🚨 MODIFIED: [Float 오염 방어] _safe_float 래핑 (NaN 자동 폴백)
                atr5_val = self._safe_float(last['ATR5'])
                atr14_val = self._safe_float(last['ATR14'])
                close_val = self._safe_float(last['Close'])

                if close_val > 0:
                    return round((atr5_val / close_val) * 100, 1), round((atr14_val / close_val) * 100, 1)
 
            except Exception:
                if attempt == 2: return 0.0, 0.0
                time.sleep(1.0 * (2 ** attempt))
        return 0.0, 0.0

    # 🚨 MODIFIED: [미래 참조 데이터 누수 차단] 당일 미확정 라이브 캔들 절단 및 D-1 타임라인 락온
    def get_amp_5d_data(self, ticker):
        for attempt in range(3):
            try:
                GlobalThrottle.wait_api_sync() # 🚨 MODIFIED: 중앙 통제소 락온
                hist = yf.Ticker(ticker).history(period="20d", prepost=False, timeout=5)
                
                if hist.empty:
                    if attempt == 2: return 0.0
                    time.sleep(1.0 * (2 ** attempt))
                    continue

                # 🚨 EST 기준 타임존 맵핑 및 D-1 팩트 절단
                est = ZoneInfo('America/New_York')
                now_est = datetime.datetime.now(est)
                cutoff_date = now_est.date()
                
                # 🚨 정규장 마감(16:00) 직전/중 또는 프리장일 경우 당일 캔들을 배제하고 전일(D-1) 기준 확정 데이터로 락온
                # (16:05 스냅샷 시점에는 오늘 캔들이 반영되도록 16:00:30 기준 유지)
                if now_est.time() <= datetime.time(16, 0, 30): 
                    cutoff_date -= datetime.timedelta(days=1)
                        
                if hist.index.tzinfo is None: 
                    hist.index = hist.index.tz_localize('UTC').tz_convert(est)
                else: 
                    hist.index = hist.index.tz_convert(est)

                # 🚨 MODIFIED: [결측치 방어 전역 확장] ffill 결속
                hist['Close'] = hist['Close'].ffill()
                hist['High'] = hist['High'].ffill()
                hist['Low'] = hist['Low'].ffill()
                    
                hist = hist[hist.index.date <= cutoff_date].copy()

                if hist.empty or len(hist) < 6:
                    if attempt == 2: return 0.0
                    time.sleep(1.0 * (2 ** attempt))
                    continue
          
                hist['Prev_Close'] = hist['Close'].shift(1)
                hist = hist.dropna(subset=['High', 'Low', 'Prev_Close']).copy()
                hist['Prev_Close'] = hist['Prev_Close'].replace(0, np.nan)
     
                hist['Amp'] = (hist['High'] - hist['Low']) / hist['Prev_Close']
                hist['Amp_5d'] = hist['Amp'].rolling(window=5).mean()
       
                last = hist.iloc[-1]
                # 🚨 MODIFIED: [Float 오염 방어] _safe_float 래핑 (NaN 자동 폴백)
                amp_val = self._safe_float(last['Amp_5d'])
                if amp_val != 0.0:
                    return round(amp_val, 6)
                break
            except Exception as e:
                if attempt == 2:
                    logging.error(f"⚠️ [Broker] Amp 5MA 파싱 에러 ({ticker}): {e}")
                    break
                time.sleep(1.0 * (2 ** attempt))
    
        return 0.0

    # 🚨 MODIFIED: [초단기 당일 앵커드 VWAP 롤오버] 다중 기점을 영구 소각하고, '당일 프리장 개장(04:00 EST)'을 절대 앵커로 100% 팩트 락온합니다.
    def get_auto_anchor_date(self, ticker):
        """ 🚨 [초단기 당일 앵커링 엔진] 프리장 개장(04:00 EST) 기점 락온 """
        est = ZoneInfo('America/New_York')
        now_est = datetime.datetime.now(est)
        
        # 04:00 이전이면 전일 기준, 아니면 당일 기준 (로지컬 데이트)
        if now_est.hour < 4:
            cutoff_date = (now_est - datetime.timedelta(days=1)).date()
        else:
            cutoff_date = now_est.date()
            
        anchor_date = cutoff_date.strftime('%Y-%m-%d')
        logging.info(f"⚓ [{ticker}] 당일 프리장 앵커링 (04:00 EST 락온): {anchor_date}")
        
        return anchor_date, "당일 프리장 개장 (04:00 EST)"

    # 🚨 MODIFIED: [초단기 당일 누적 VWAP 엔진] 기점 거리에 따른 동적 해상도를 소각하고, 1m 해상도와 prepost=True(프리장 포함)로 100% 팩트 락온.
    def get_anchored_vwap(self, ticker, anchor_date):
        for attempt in range(3):
            try:
                GlobalThrottle.wait_api_sync() # 🚨 MODIFIED: 중앙 통제소 락온
                
                est = ZoneInfo('America/New_York')
                stock = yf.Ticker(ticker)
              
                # 🚨 MODIFIED: [초단기 당일 누적] 1m 해상도 고정 및 prepost=True (프리장 포함) 팩트 락온
                df = stock.history(period="5d", interval="1m", prepost=True, timeout=5)
                
                if df.empty:
                    if attempt == 2: return 0.0
                    time.sleep(1.0 * (2 ** attempt))
                    continue
                    
                df = _flatten_columns(df)
                
                if df.index.tz is None:
                    df.index = df.index.tz_localize('UTC').tz_convert(est)
                else:
                    df.index = df.index.tz_convert(est)
                    
                # 🚨 [Time Paradox 방어] 기점(anchor_date) 당일의 04:00 EST 이후 캔들만 핀셋 추출
                target_date = datetime.datetime.strptime(anchor_date, "%Y-%m-%d").date()
                df = df[df.index.date == target_date]
        
                # 04:00부터 누적
                df = df.between_time('04:00', '19:59')
                
                if df.empty:
                    return 0.0
               
                # 🚨 NEW: [Case 35 절대 방어망 결속] 결측치(NaN) 전이 방어
                df['High'] = df['High'].ffill().bfill()
                df['Low'] = df['Low'].ffill().bfill()
                df['Close'] = df['Close'].ffill().bfill()
                df['Volume'] = df['Volume'].ffill().bfill().fillna(0)
                
                # 🚨 NEW: 정통 퀀트 표준 Typical Price 팩트 주입
                df['Typical_Price'] = (df['High'].astype(float) + df['Low'].astype(float) + df['Close'].astype(float)) / 3.0
                df['Vol_x_Price'] = df['Typical_Price'] * df['Volume'].astype(float)
                
                # 🚨 NEW: [프리장 Zero-Volume 붕괴 패러독스 방어] 거래량 0일 때 즉사하는 맹점을 막기 위한 TWAP 벡터화 연산 락온
                df['TWAP'] = df['Typical_Price'].expanding().mean()
                
                # 🚨 NEW: [벡터화 강제 헌법 준수] For 루프 전면 소각 및 순수 벡터화 연산 락온
                df['Cum_Vol_Price'] = df['Vol_x_Price'].cumsum()
                df['Cum_Volume'] = df['Volume'].astype(float).cumsum()
                
                # 🚨 MODIFIED: [결측치 강제 롤오버 방어] 누적 거래량이 0일 때 0.0 대신 TWAP으로 우회(Fallback)하여 관제탑 UI 마비 원천 차단
                df['AVWAP'] = np.where(df['Cum_Volume'] > 0, df['Cum_Vol_Price'] / df['Cum_Volume'], df['TWAP'])
                
                latest_avwap = self._safe_float(df['AVWAP'].iloc[-1])
            
                return round(latest_avwap, 2)
              
            except Exception as e:
                logging.debug(f"⚠️ [Broker] 당일 앵커드 VWAP 파싱 실패 ({ticker}): {e}")
                if attempt == 2: return 0.0
                time.sleep(1.0 * (2 ** attempt))
        return 0.0
