# ==========================================================
# FILE: broker.py
# ==========================================================
# MODIFIED: [V28.15 장부 2배 뻥튀기(Double Counting) 원천 차단]
# MODIFIED: [V28.27 GCP 무한 대기 교착(Deadlock) 및 액면분할 에러 전면 수술]
# MODIFIED: [V28.34 17시 잔고 스캔 API 크래시 완벽 방어 및 타입 세이프 쉴드 이식]
# MODIFIED: [V29.18 런타임 붕괴 방어 및 페이징 결함 수술]
# MODIFIED: [V30.09 핫픽스] pytz 영구 적출 및 ZoneInfo 도입으로 타임존 무결성 100% 확보
# 🚨 MODIFIED: [V44.76 팩트 교정] 당일 고가/저가 스캔 시 프리마켓 진폭 100% 합산 롤백
# 🚨 MODIFIED: [V47.00 하이킨아시 파서 open 컬럼 강제 수혈 락온]
# 🚨 MODIFIED: [V61.01 숏(SOXS) 전면 소각 작전 지시서 적용] 싱글 롱 모멘텀 압축
# 🚨 MODIFIED: [V71.05 KIS VWAP 30분 압축 타격 및 예약 주문 파이프라인 정밀 수술]
# 🚨 MODIFIED: [V71.14 지정가 VWAP 일반주문(Regular Order) 100% 팩트 락온 및 예약주문 내 데드코드 전면 폐기]
# 🚨 MODIFIED: [V71.16 KIS API 알고리즘 타임 파라미터 명세 100% 팩트 교정]
# - START_TIME / END_TIME 이라는 존재하지 않는 키값을 던져 
#   '❌(시작시간이 장시간을 벗어났습니다.)' 리젝을 유발하던 치명적 맹점 수술.
# - KIS 공식 명세인 ALGO_ORD_STRT_TMD / ALGO_ORD_END_TMD 로 정밀 역배선 완료.
# 🚨 NEW: [V71.23 KIS API 알고리즘 타임 파라미터 유령 Key 소각 및 팩트 교정]
# - 과거 명세서 오판으로 삽입된 ALGO_ORD_STRT_TMD 키를 전면 철거.
# - 팩트에 맞게 START_TIME / END_TIME 으로 100% 원상 복구 완료.
# ==========================================================

import requests
import json
import time
import datetime
import os
import math
import yfinance as yf
from zoneinfo import ZoneInfo
import tempfile
import shutil  
import pandas as pd   
import numpy as np
import volatility_engine as ve
import logging  

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

class KoreaInvestmentBroker:
  
    def __init__(self, app_key, app_secret, cano, acnt_prdt_cd="01"):
        self.app_key = app_key
        self.app_secret = app_secret
        self.cano = cano
        self.acnt_prdt_cd = acnt_prdt_cd
        self.base_url = "https://openapi.koreainvestment.com:9443"
        self.token_file = f"data/token_{cano}.dat" 
        self.token = None
        self._excg_cd_cache = {} 
        self._get_access_token()

    def _get_access_token(self, force=False):
        kst = ZoneInfo('Asia/Seoul')
        
        if not force and os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r') as f:
                    saved = json.load(f)
                expire_time = datetime.datetime.strptime(saved['expire'], '%Y-%m-%d %H:%M:%S')
                now_kst_naive = datetime.datetime.now(kst).replace(tzinfo=None)
        
                if expire_time > now_kst_naive + datetime.timedelta(hours=1):
                    self.token = saved['token']
                    return
            except Exception: pass

        if force and os.path.exists(self.token_file):
            try: os.remove(self.token_file)
            except Exception: pass

        url = f"{self.base_url}/oauth2/tokenP"
        body = {"grant_type": "client_credentials", "appkey": self.app_key, "appsecret": self.app_secret}
        
        try:
            res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body), timeout=10)
            data = res.json()
 
            if 'access_token' in data:
                self.token = data['access_token']
                expire_str = (datetime.datetime.now(kst).replace(tzinfo=None) + datetime.timedelta(seconds=int(data['expires_in']))).strftime('%Y-%m-%d %H:%M:%S')
            
                dir_name = os.path.dirname(self.token_file)
               
                if dir_name and not os.path.exists(dir_name):
                    os.makedirs(dir_name, exist_ok=True)
   
                fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
                try:
                    with os.fdopen(fd, 'w', encoding='utf-8') as f:
                        json.dump({'token': self.token, 'expire': expire_str}, f)
                    f.flush()
                    os.fsync(f.fileno())
                    shutil.move(temp_path, self.token_file)
                finally:
                    if os.path.exists(temp_path):
                        try: os.remove(temp_path)
                        except Exception: pass
            else:
                print(f"❌ [Broker] 토큰 발급 실패: {data.get('error_description', '알 수 없는 오류')}")
        except Exception as e:
            print(f"❌ [Broker] 토큰 통신 에러: {e}")

    def _get_header(self, tr_id):
        return {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self.token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
            "custtype": "P"
        }

    def _api_request(self, method, url, headers, params=None, data=None):
        TOKEN_EXPIRY_KEYWORDS = frozenset([
            'expired', '인증', 'authorization', 'egt0001', 'egt0002', 'oauth', 
            '접근토큰이 만료', '토큰이 유효하지'
        ])
    
        for attempt in range(2): 
            try:
                if method.upper() == "GET":
                    res = requests.get(url, headers=headers, params=params, timeout=10)
                else:
                    res = requests.post(url, headers=headers, data=json.dumps(data) if data else None, timeout=10)
                 
                resp_json = res.json()
         
                if resp_json.get('rt_cd') != '0':
                    msg1_lower = resp_json.get('msg1', '').lower()
                    msg_cd = resp_json.get('msg_cd', '').lower()
                    
                    if any(x in msg1_lower or x in msg_cd for x in TOKEN_EXPIRY_KEYWORDS):
                        if attempt == 0: 
                            old_token = self.token 
                            print(f"\n🚨 [안전장치 가동] API 토큰 만료 감지! : {msg1_lower}")
                            self._get_access_token(force=True)
                            
                            if self.token == old_token or self.token is None:
                                print("🚨 [Broker] 토큰 갱신 실패. 재시도 중단.")
                                return res, resp_json
                            
                            headers["authorization"] = f"Bearer {self.token}"
                            time.sleep(1.0)
                            continue
 
                return res, resp_json
            except Exception as e:
                print(f"⚠️ API 통신 중 예외 발생: {e}")
                if attempt == 1: return None, {}
                time.sleep(1.0)
        return None, {}

    def _call_api(self, tr_id, url_path, method="GET", params=None, body=None):
        headers = self._get_header(tr_id)
        url = f"{self.base_url}{url_path}"
        res, resp_json = self._api_request(method, url, headers, params=params, data=body)
        if not resp_json: return {'rt_cd': '999', 'msg1': '통신 오류 또는 최대 재시도 횟수 초과'}
        return resp_json

    def _ceil_2(self, value):
        if value is None: return 0.0
        return max(0.01, math.ceil(value * 100) / 100.0)

    def _safe_float(self, value):
        try: return float(str(value).replace(',', ''))
        except Exception: return 0.0

    def _get_exchange_code(self, ticker, target_api="PRICE"):
        if ticker in self._excg_cd_cache:
            codes = self._excg_cd_cache[ticker]
            return codes['PRICE'] if target_api == "PRICE" else codes['ORDER']

        price_cd = "NAS"
        order_cd = "NASD"
        dynamic_success = False

        try:
            for prdt_type in ["512", "513", "529"]:
                params = {"PRDT_TYPE_CD": prdt_type, "PDNO": ticker}
                res = self._call_api("CTPF1702R", "/uapi/overseas-price/v1/quotations/search-info", "GET", params=params)
                
                if res.get('rt_cd') == '0' and res.get('output'):
                    excg_name = str(res['output'].get('ovrs_excg_cd', '')).upper()
                    if "NASD" in excg_name or "NASDAQ" in excg_name:
                        price_cd, order_cd = "NAS", "NASD"
                        dynamic_success = True
                        break
                    elif "NYSE" in excg_name or "NEW YORK" in excg_name:
                        price_cd, order_cd = "NYS", "NYSE"
                        dynamic_success = True
                        break
                    elif "AMEX" in excg_name:
                        price_cd, order_cd = "AMS", "AMEX"
                        dynamic_success = True
                        break
        except Exception as e:
            print(f"⚠️ [Broker] 거래소 동적 획득 실패: {ticker} - {e}")

        if not dynamic_success:
            if ticker == "SOXL": price_cd, order_cd = "AMS", "AMEX"
            elif ticker == "TQQQ": price_cd, order_cd = "NAS", "NASD"

        self._excg_cd_cache[ticker] = {'PRICE': price_cd, 'ORDER': order_cd}
        return price_cd if target_api == "PRICE" else order_cd

    def get_account_balance(self):
        """ 🚨 [제3경고] API 잔고 응답 중복 합산 절대 방어 락온 """
        cash = 0.0
        holdings = {}
        api_success = False 
  
        params = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "WCRC_FRCR_DVSN_CD": "02", "NATN_CD": "840", "TR_MKET_CD": "00", "INQR_DVSN_CD": "00"}
        res = self._call_api("CTRP6504R", "/uapi/overseas-stock/v1/trading/inquire-present-balance", "GET", params=params)
   
        if res.get('rt_cd') == '0':
            api_success = True
            o2 = res.get('output2', {})
            if isinstance(o2, list): o2 = o2[0] if len(o2) > 0 else {}
            
            dncl_amt = self._safe_float(o2.get('frcr_dncl_amt_2', 0))     
            sll_amt = self._safe_float(o2.get('frcr_sll_amt_smtl', 0))      
            buy_amt = self._safe_float(o2.get('frcr_buy_amt_smtl', 0))      
            raw_bp = dncl_amt + sll_amt - buy_amt
            cash = max(0.0, math.floor((raw_bp * 0.9945) * 100) / 100.0)

        target_excgs = ["NASD", "AMEX", "NYSE"] 
  
        for excg in target_excgs:
            fk200, nk200 = "", ""
            for attempt in range(20): 
                params_hold = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg, "TR_CRCY_CD": "USD", "CTX_AREA_FK200": fk200, "CTX_AREA_NK200": nk200}
                headers = self._get_header("TTTS3012R")
                url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-balance"
                res_hold, resp_json = self._api_request("GET", url, headers, params=params_hold)
    
                if res_hold and resp_json.get('rt_cd') == '0':
                    api_success = True
                    if cash <= 0:
                        o2 = resp_json.get('output2', {})
                        if isinstance(o2, list): o2 = o2[0] if len(o2) > 0 else {}
                        new_cash = self._safe_float(o2.get('ovrs_ord_psbl_amt', 0))
                        if new_cash > cash: cash = new_cash
                   
                    for item in (resp_json.get('output1') or []):
                        ticker = item.get('ovrs_pdno')
                        if not ticker: continue
 
                        qty = int(self._safe_float(item.get('ovrs_cblc_qty', 0)))
                        ord_psbl_qty = int(self._safe_float(item.get('ord_psbl_qty', 0)))
                        avg = self._safe_float(item.get('pchs_avg_pric', 0))
                
                        if qty > 0 and ord_psbl_qty == 0: ord_psbl_qty = qty
                        if qty > 0:
                            if ticker not in holdings: 
                                holdings[ticker] = {'qty': qty, 'ord_psbl_qty': ord_psbl_qty, 'avg': avg}
                            else:
                                prev = holdings[ticker]
                                # 💡 [멱등성 가드] 동일 수량/평단가 중복 응답 무시
                                if prev['qty'] == qty and abs(prev['avg'] - avg) < 0.001: continue 
                                total_qty = prev['qty'] + qty
                                new_avg = ((prev['avg'] * prev['qty']) + (avg * qty)) / total_qty if total_qty > 0 else avg
                                holdings[ticker]['qty'] = total_qty
                                holdings[ticker]['ord_psbl_qty'] += ord_psbl_qty
                                holdings[ticker]['avg'] = new_avg

                    tr_cont = res_hold.headers.get('tr_cont', '') if hasattr(res_hold, 'headers') else ''
                    fk200 = (resp_json.get('ctx_area_fk200', '') or '').strip()
                    nk200 = (resp_json.get('ctx_area_nk200', '') or '').strip()
                    if tr_cont in ['M', 'F'] and nk200:
                        time.sleep(0.2)
                        continue
                    else: break
                else: break
        
        if api_success: return cash, holdings
        else: return cash, None

    def get_daily_vwap_info(self, ticker):
        """ 기초자산의 정규장 거래 내역 기반 일자별 순수 VWAP 계산 """
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period="5d", interval="1m", prepost=False, timeout=10)
            if df.empty: return 0.0, 0.0
            df = _flatten_columns(df)
            est = ZoneInfo('America/New_York') # 🚨 [제3헌법] EST 락온
            if df.index.tz is None: df.index = df.index.tz_localize('UTC').tz_convert(est)
            else: df.index = df.index.tz_convert(est)

            regular_market = df.between_time('09:30', '15:59').copy()
            if regular_market.empty: return 0.0, 0.0
            regular_market['Typical_Price'] = (regular_market['High'] + regular_market['Low'] + regular_market['Close']) / 3.0
            regular_market['Vol_x_Price'] = regular_market['Typical_Price'] * regular_market['Volume']
    
            regular_market['Date'] = regular_market.index.date
            daily_stats = regular_market.groupby('Date').agg(Total_Vol_Price=('Vol_x_Price', 'sum'), Total_Vol=('Volume', 'sum'))
            daily_stats['VWAP'] = np.where(daily_stats['Total_Vol'] > 0, daily_stats['Total_Vol_Price'] / daily_stats['Total_Vol'], np.nan)
            daily_stats = daily_stats.dropna(subset=['VWAP'])

            if len(daily_stats) >= 2:
                return round(float(daily_stats['VWAP'].iloc[-2]), 4), round(float(daily_stats['VWAP'].iloc[-1]), 4)
            elif len(daily_stats) == 1:
                return 0.0, round(float(daily_stats['VWAP'].iloc[-1]), 4)
            return 0.0, 0.0
        except Exception as e:
            logging.error(f"⚠️ [Broker] 일별 VWAP 파싱 실패 ({ticker}): {e}")
            return 0.0, 0.0

    def get_current_5min_candle(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period="5d", interval="1m", prepost=True, timeout=5)
            if df.empty: return None
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
            vwap_series = pd.Series(np.where(cum_vol > 0, cum_vol_price / cum_vol, np.nan), index=cum_vol.index).ffill() 
            current_vwap = float(vwap_series.iloc[-1]) if not vwap_series.empty else 0.0
            if pd.isna(current_vwap): current_vwap = 0.0
    
            resampled = regular_market.resample('5min', label='left', closed='left').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}).dropna()
            if resampled.empty: return None
            resampled['Vol_MA10'] = resampled['Volume'].rolling(10, min_periods=1).mean()
            resampled['Vol_MA20'] = resampled['Volume'].rolling(20, min_periods=1).mean()
            last_candle = resampled.iloc[-1]
            latest_1m = regular_market.iloc[-1] 
  
            return {
                'open': float(last_candle['Open']), 'high': float(last_candle['High']), 'low': float(last_candle['Low']),    
                'close': float(latest_1m['Close']), 'volume': float(last_candle['Volume']), 
                'vol_ma10': float(last_candle.get('Vol_MA10', last_candle['Volume'])),
                'vol_ma20': float(last_candle.get('Vol_MA20', last_candle['Volume'])),
                'vwap': current_vwap  
            }
        except Exception as e:
            print(f"⚠️ [Broker] 실시간 5분봉 조회 실패 ({ticker}): {e}")
            return None

    def get_current_price(self, ticker, is_market_closed=False):
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1d", interval="1m", prepost=True, timeout=5)
            if not hist.empty: return float(hist['Close'].iloc[-1])
            else: raise ValueError("YF 실시간 데이터 응답 지연 (timeout)") 
        except Exception as e:
            print(f"⚠️ [야후] 현재가 에러, 한투 API 우회 가동: {e}")

        try:
            excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
            params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
            res = self._call_api("HHDFS76200200", "/uapi/overseas-price/v1/quotations/price", "GET", params=params)
            if res.get('rt_cd') == '0': return float(res.get('output', {}).get('last', 0.0))
        except Exception: pass
        return 0.0

    def get_ask_price(self, ticker):
        try:
            excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
            params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
            res = self._call_api("HHDFS76200100", "/uapi/overseas-price/v1/quotations/inquire-asking-price", "GET", params=params)
            if res.get('rt_cd') == '0':
                o2 = res.get('output2', [])
                if isinstance(o2, list) and len(o2) > 0: return float(o2[0].get('pask1', 0.0))
                elif isinstance(o2, dict): return float(o2.get('pask1', 0.0))
        except Exception: pass
        return 0.0

    def get_bid_price(self, ticker):
        try:
            excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
            params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
            res = self._call_api("HHDFS76200100", "/uapi/overseas-price/v1/quotations/inquire-asking-price", "GET", params=params)
            if res.get('rt_cd') == '0':
                o2 = res.get('output2', [])
                if isinstance(o2, list) and len(o2) > 0: return float(o2[0].get('pbid1', 0.0))
                elif isinstance(o2, dict): return float(o2.get('pbid1', 0.0))
        except Exception: pass
        return 0.0

    def get_previous_close(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="5d", timeout=5)
            if not hist.empty:
                est = ZoneInfo('America/New_York')
                now_est = datetime.datetime.now(est)
                cutoff_date = now_est.date()
                if now_est.time() <= datetime.time(16, 0, 30): cutoff_date -= datetime.timedelta(days=1)
                if hist.index.tzinfo is None: hist.index = hist.index.tz_localize('UTC').tz_convert(est)
                else: hist.index = hist.index.tz_convert(est)
                past_hist = hist[hist.index.date <= cutoff_date]
                if not past_hist.empty: return float(past_hist['Close'].dropna().iloc[-1])
        except Exception as e:
            print(f"⚠️ [야후] 전일 종가 파싱 에러, 한투 API 우회 가동: {e}")

        try:
            excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
            params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
            res = self._call_api("HHDFS76200200", "/uapi/overseas-price/v1/quotations/price", "GET", params=params)
            if res.get('rt_cd') == '0': return float(res.get('output', {}).get('base', 0.0))
        except Exception: pass
        return 0.0
       
    def get_5day_ma(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="10d", timeout=5) 
            if len(hist) >= 5: return float(hist['Close'][-5:].mean())
        except Exception: pass
        try:
            excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
            params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker, "GUBN": "0", "BYMD": "", "MODP": "1"}
            res = self._call_api("HHDFS76240000", "/uapi/overseas-price/v1/quotations/dailyprice", "GET", params=params)
            if res.get('rt_cd') == '0':
                o2 = res.get('output2', [])
                if isinstance(o2, list) and len(o2) >= 5:
                    closes = [float(x['clos']) for x in o2[:5]]
                    return sum(closes) / len(closes)
        except Exception: pass
        return 0.0

    def get_1min_candles_df(self, ticker):
        """ 🚨 [제5경고] 하이킨아시 연산을 위한 open 컬럼 강제 보존 리턴 """
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period="1d", interval="1m", prepost=True, timeout=5)
            if df.empty: return None
            df = _flatten_columns(df)
            est = ZoneInfo('America/New_York')
            if df.index.tz is None: df.index = df.index.tz_localize('UTC').tz_convert(est)
            else: df.index = df.index.tz_convert(est)
            df = df.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'})
            df['time_est'] = df.index.strftime('%H%M00')
            
            try:
                max_high, min_low = float(df['high'].max()), float(df['low'].min())
                time_high_idx, time_low_idx = df['high'].astype(float).idxmax(), df['low'].astype(float).idxmin()
                time_high_str = df.loc[time_high_idx, 'time_est'] if not pd.isna(time_high_idx) else ""
                time_low_str = df.loc[time_low_idx, 'time_est'] if not pd.isna(time_low_idx) else ""
                cache_file = "data/avwap_cache.json"
                cache_data = {}
                if os.path.exists(cache_file):
                    try:
                        with open(cache_file, 'r', encoding='utf-8') as f: cache_data = json.load(f)
                    except Exception: pass
                cache_data[ticker] = {'day_high': max_high, 'day_low': min_low, 'time_high': time_high_str, 'time_low': time_low_str, 'date': datetime.datetime.now(est).strftime("%Y-%m-%d")}
                os.makedirs('data', exist_ok=True)
                fd, tmp_path = tempfile.mkstemp(dir='data', text=True) # 🚨 원자적 쓰기
                with os.fdopen(fd, 'w', encoding='utf-8') as f_out:
                    json.dump(cache_data, f_out, ensure_ascii=False, indent=4)
                    f_out.flush()
                    os.fsync(f_out.fileno())
                os.replace(tmp_path, cache_file)
            except Exception as e: logging.error(f"🚨 [{ticker}] 시계열 체력 팩트 캐싱 실패: {e}")
            return df[['open', 'high', 'low', 'close', 'volume', 'time_est']]
        except Exception: return None

    def get_unfilled_orders_detail(self, ticker):
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        valid_orders = []
        fk200, nk200 = "", ""
        for attempt in range(10):
            params = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd, "SORT_SQN": "DS", "CTX_AREA_FK200": fk200, "CTX_AREA_NK200": nk200}
            res, resp_json = self._api_request("GET", f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-nccs", self._get_header("TTTS3018R"), params=params)
            if res and resp_json.get('rt_cd') == '0':
                output = resp_json.get('output', [])
                if isinstance(output, dict): output = [output]
                valid_orders.extend([item for item in output if item.get('pdno') == ticker])
                tr_cont = res.headers.get('tr_cont', '') if hasattr(res, 'headers') else ''
                fk200 = (resp_json.get('ctx_area_fk200', '') or '').strip()
                nk200 = (resp_json.get('ctx_area_nk200', '') or '').strip()
                if tr_cont in ['M', 'F'] and nk200: time.sleep(0.3); continue
                else: break
            else: return False
        return valid_orders

    def get_reservation_orders(self, ticker, start_date, end_date):
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        valid_orders = []
        fk200, nk200 = "", ""
        
        for attempt in range(15):
            params = {
                "CANO": self.cano,
                "ACNT_PRDT_CD": self.acnt_prdt_cd,
                "INQR_STRT_DT": start_date,
                "INQR_END_DT": end_date,
                "INQR_DVSN_CD": "00",
                "OVRS_EXCG_CD": excg_cd,
                "PRDT_TYPE_CD": "",
                "CTX_AREA_FK200": fk200,
                "CTX_AREA_NK200": nk200
            }
            
            res, resp_json = self._api_request("GET", f"{self.base_url}/uapi/overseas-stock/v1/trading/order-resv-list", self._get_header("TTTT3039R"), params=params)
            
            if res and resp_json.get('rt_cd') == '0':
                output = resp_json.get('output', [])
                if isinstance(output, dict): output = [output]
                
                valid_orders.extend([item for item in output if item.get('pdno') == ticker])
                
                tr_cont = res.headers.get('tr_cont', '') if hasattr(res, 'headers') else ''
                fk200 = (resp_json.get('ctx_area_fk200', '') or '').strip()
                nk200 = (resp_json.get('ctx_area_nk200', '') or '').strip()
                
                if tr_cont in ['M', 'F'] and nk200:
                    time.sleep(0.3)
                    continue
                else:
                    break
            else:
                break
             
        return valid_orders

    def cancel_all_orders_safe(self, ticker, side=None):
        for i in range(3):
            orders = self.get_unfilled_orders_detail(ticker)
            if orders is False: return False
            if not orders: return True
            target_orders = orders
            if side == "BUY": target_orders = [o for o in orders if o.get('sll_buy_dvsn_cd') == '02']
            elif side == "SELL": target_orders = [o for o in orders if o.get('sll_buy_dvsn_cd') == '01']
            if not target_orders: return True
            for o in target_orders: self.cancel_order(ticker, o.get('odno'))
            time.sleep(5)
        final_orders = self.get_unfilled_orders_detail(ticker)
        if final_orders is False: return False
        failed_orders = [o for o in final_orders if (side == "BUY" and o.get('sll_buy_dvsn_cd') == '02') or (side == "SELL" and o.get('sll_buy_dvsn_cd') == '01')]
        return len(failed_orders) == 0
         
    def cancel_targeted_orders(self, ticker, side, target_ord_dvsn):
        sll_buy_cd = '02' if side == "BUY" else '01'
        orders = self.get_unfilled_orders_detail(ticker)
        if not orders: return 0
        target_orders = [o for o in orders if o.get('sll_buy_dvsn_cd') == sll_buy_cd and (o.get('ord_dvsn_cd') or o.get('ord_dvsn') or '') == target_ord_dvsn]
        for o in target_orders: self.cancel_order(ticker, o.get('odno')); time.sleep(0.3)
        return len(target_orders)

    def cancel_orders_by_price(self, ticker, side, target_prices):
        sll_buy_cd = '02' if side == "BUY" else '01'
        orders = self.get_unfilled_orders_detail(ticker)
        if not orders: return 0
        target_orders = []
        for o in orders:
             if o.get('sll_buy_dvsn_cd') == sll_buy_cd:
                o_price = 0.0
                for rp in [o.get('ft_ord_unpr3', 0), o.get('ord_unpr', 0), o.get('ovrs_ord_unpr', 0)]:
                    try: 
                        val = float(rp)
                        if val > 0: o_price = val; break 
                    except (TypeError, ValueError): pass
                for tp in target_prices:
                    if o_price > 0 and abs(o_price - tp) < 0.005: target_orders.append(o); break
        for o in target_orders: self.cancel_order(ticker, o.get('odno')); time.sleep(0.3)
        return len(target_orders)

    # 🚨 MODIFIED: [V71.16 KIS API 알고리즘 타임 파라미터 명세 100% 팩트 교정]
    def send_order(self, ticker, side, qty, price, order_type="LIMIT", start_time=None, end_time=None):
        try: order_qty = int(float(qty))
        except (TypeError, ValueError): return {'rt_cd': '999', 'msg1': f'유효하지 않은 주문 수량: {qty!r}'}
        if order_qty <= 0: return {'rt_cd': '999', 'msg1': f'수량 오류: {qty}'}

        for attempt in range(2):
            tr_id = "TTTT1002U" if side == "BUY" else "TTTT1006U"
            excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
            # 🚨 36: VWAP 추가 및 32: LOO 강제 매핑
            ord_dvsn = {"LOC": "34", "MOC": "33", "LOO": "32", "MOO": "31", "VWAP": "36"}.get(order_type, "00")
            final_price = 0 if order_type in ["MOC", "MOO"] else self._ceil_2(price)
         
            if order_type not in ["MOC", "MOO"] and final_price <= 0.0: return {'rt_cd': '999', 'msg1': f'가격 오류: {price}'}
            
            # 🚨 MODIFIED: [V72.24 KIS 명세 반영 SLL_TYPE 누수 방어 이식]
            sll_type = "00" if side == "SELL" else ""
            
            body = {
                "CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd, 
                "PDNO": ticker, "ORD_QTY": str(order_qty), "OVRS_ORD_UNPR": str(final_price), 
                "ORD_SVR_DVSN_CD": "0", "ORD_DVSN": ord_dvsn, "SLL_TYPE": sll_type
            }
            
            # 🚨 NEW: [V71.23 KIS API 알고리즘 타임 파라미터 유령 Key 소각 및 팩트 교정]
            # KIS 명세서 팩트 기반 START_TIME / END_TIME 절대 락온
            if order_type == "VWAP":
                if start_time and end_time:
                    body["ALGO_ORD_TMD_DVSN_CD"] = "00"
                    body["START_TIME"] = start_time  # 🚨 KIS 명세 팩트 복구
                    body["END_TIME"] = end_time     # 🚨 KIS 명세 팩트 복구
                else:
                    body["ALGO_ORD_TMD_DVSN_CD"] = "02"

            res = self._call_api(tr_id, "/uapi/overseas-stock/v1/trading/order", "POST", body=body)
            if res.get('rt_cd') != '0' and attempt == 0 and any(x in res.get('msg1', '') for x in ["거래소", "시장", "exchange", "코드"]):
                if ticker in self._excg_cd_cache: del self._excg_cd_cache[ticker]
                time.sleep(0.5); continue
            return {'rt_cd': res.get('rt_cd', '999'), 'msg1': res.get('msg1', '오류'), 'odno': res.get('output', {}).get('ODNO', '') if isinstance(res.get('output'), dict) else ''}
        return {'rt_cd': '999', 'msg1': '거래소 캐시 재시도 초과'}

    def cancel_order(self, ticker, order_id):
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        body = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd, "PDNO": ticker, "ORGN_ODNO": order_id, "RVSE_CNCL_DVSN_CD": "02", "ORD_QTY": "0", "OVRS_ORD_UNPR": "0", "ORD_SVR_DVSN_CD": "0"}
        self._call_api("TTTT1004U", "/uapi/overseas-stock/v1/trading/order-rvsecncl", "POST", body=body)

    def send_daytime_order(self, ticker, side, qty, price):
        try: order_qty = int(float(qty))
        except: return {'rt_cd': '999', 'msg1': '수량 오류'}
        if order_qty <= 0: return {'rt_cd': '999', 'msg1': '수량 오류'}
        tr_id = "TTTS6036U" if side == "BUY" else "TTTS6037U"
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        body = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd, "PDNO": ticker, "ORD_QTY": str(order_qty), "OVRS_ORD_UNPR": str(self._ceil_2(price)), "CTAC_TLNO": "", "MGCO_APTM_ODNO": "", "ORD_SVR_DVSN_CD": "0", "ORD_DVSN": "00"}
        res = self._call_api(tr_id, "/uapi/overseas-stock/v1/trading/daytime-order", "POST", body=body)
        return {'rt_cd': res.get('rt_cd', '999'), 'msg1': res.get('msg1', '오류'), 'odno': res.get('output', {}).get('ODNO', '') if isinstance(res.get('output'), dict) else ''}

    def cancel_daytime_order(self, ticker, order_id, qty="100", price="0"):
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        body = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd, "PDNO": ticker, "ORGN_ODNO": order_id, "RVSE_CNCL_DVSN_CD": "02", "ORD_QTY": str(qty), "OVRS_ORD_UNPR": str(price), "CTAC_TLNO": "", "MGCO_APTM_ODNO": "", "ORD_SVR_DVSN_CD": "0"}
        return self._call_api("TTTS6038U", "/uapi/overseas-stock/v1/trading/daytime-order-rvsecncl", "POST", body=body)

    # 🚨 MODIFIED: [V71.14 지정가 VWAP(Limit VWAP) 예약주문 데드코드 전면 폐기 및 일반주문 이관]
    def send_reservation_order(self, ticker, side, qty, price, order_type="LIMIT"):
        try: order_qty = int(float(qty))
        except: return {'rt_cd': '999', 'msg1': '수량 오류'}
        
        tr_id = "TTTT3014U" if side == "BUY" else "TTTT3016U"
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        final_price = str(self._ceil_2(price))
        
        body = {
            "CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "PDNO": ticker,
            "OVRS_EXCG_CD": excg_cd, "FT_ORD_QTY": str(order_qty)
        }
        
        if order_type == "LOC":
            body["ORD_DVSN"] = "34" # 34: LOC (장마감지정가)
            body["FT_ORD_UNPR3"] = final_price
        else:
            body["ORD_DVSN"] = "00" # 일반 지정가 예약
            body["FT_ORD_UNPR3"] = final_price
            
        res = self._call_api(tr_id, "/uapi/overseas-stock/v1/trading/order-resv", "POST", body=body)
        rt_cd = res.get('rt_cd', '999')
        msg1 = res.get('msg1', '오류')
        odno = res.get('output', {}).get('ODNO', '') if isinstance(res.get('output'), dict) else ''
        return {'rt_cd': rt_cd, 'msg1': msg1, 'odno': odno}

    def cancel_reservation_order(self, order_date, order_id):
        body = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "RSVN_ORD_RCIT_DT": order_date, "OVRS_RSVN_ODNO": order_id}
        return self._call_api("TTTT3017U", "/uapi/overseas-stock/v1/trading/order-resv-ccnl", "POST", body=body)

    def get_execution_history(self, ticker, start_date, end_date):
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        odno_map = {}
        for attempt in range(10): 
            params = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "PDNO": ticker, "ORD_STRT_DT": start_date, "ORD_END_DT": end_date, "SLL_BUY_DVSN": "00", "CCLD_NCCS_DVSN": "00", "OVRS_EXCG_CD": excg_cd, "SORT_SQN": "DS", "CTX_AREA_FK200": "", "CTX_AREA_NK200": ""}
            res, resp_json = self._api_request("GET", f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-ccnl", self._get_header("TTTS3035R"), params=params)
 
            if res and resp_json.get('rt_cd') == '0':
                output = resp_json.get('output', [])
                if isinstance(output, dict): output = [output] 
                for item in output:
                    try:
                        iq, ip = float(item.get('ft_ccld_qty', 0)), float(item.get('ft_ccld_unpr3', 0))
                        if iq > 0:
                            odno = item.get('odno', f"__nk_{id(item)}")
                            if odno not in odno_map: odno_map[odno] = {"item": dict(item), "total_qty": iq, "total_amt": iq * ip}
                            else: odno_map[odno]["total_qty"] += iq; odno_map[odno]["total_amt"] += (iq * ip)
                    except: continue
                if res.headers.get('tr_cont', '') in ['M', 'F']: time.sleep(0.3); continue
                else: break
            else: break
        return [{"ft_ccld_qty": str(d["total_qty"]), "ft_ccld_unpr3": str(d["total_amt"]/d["total_qty"] if d["total_qty"]>0 else 0), **d["item"]} for d in odno_map.values()]

    def get_genesis_ledger(self, ticker, limit_date_str=None):
        _, h = self.get_account_balance()
        if not h: return None, 0, 0.0
        t_info = h.get(ticker, {'qty': 0, 'avg': 0.0})
        curr_qty = int(t_info.get('qty', 0))
        if curr_qty == 0: return [], 0, 0.0
        ledger_records, est, target_date, loop = [], ZoneInfo('America/New_York'), datetime.datetime.now(ZoneInfo('America/New_York')), 0
        while curr_qty > 0 and loop < 365:
            if target_date.weekday() < 5: loop += 1
            date_str = target_date.strftime('%Y%m%d')
            if limit_date_str and date_str < limit_date_str: break 
            execs = self.get_execution_history(ticker, date_str, date_str)
            if execs:
                execs.sort(key=lambda x: x.get('ord_tmd', '000000'), reverse=True)
                for ex in execs:
                    side, eq, ep = ex.get('sll_buy_dvsn_cd'), int(float(ex.get('ft_ccld_qty', 0))), float(ex.get('ft_ccld_unpr3', 0))
                    rq = eq
                    if side == "02":
                        if curr_qty <= eq: rq, curr_qty = curr_qty, 0
                        else: curr_qty -= eq
                    else: curr_qty += eq
                    ledger_records.append({'date': f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}", 'side': "BUY" if side=="02" else "SELL", 'qty': rq, 'price': ep})
                    if curr_qty == 0: break
            target_date -= datetime.timedelta(days=1); time.sleep(0.1) 
        if curr_qty > 0: ledger_records.append({'date': 'INCOMPLETE', 'side': 'UNKNOWN', 'qty': curr_qty, 'price': float(t_info.get('avg', 0.0)), 'is_incomplete': True})
        ledger_records.reverse()
        return ledger_records, int(t_info.get('qty', 0)), float(t_info.get('avg', 0.0))

    def get_recent_stock_split(self, ticker, last_date_str):
        try:
            splits = yf.Ticker(ticker).splits
            if splits is not None and not splits.empty:
                safe_last_date = last_date_str if last_date_str else (datetime.datetime.now(ZoneInfo('America/New_York')) - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
                for dt, ratio in splits.items():
                    sd = dt[:10] if isinstance(dt, str) else pd.Timestamp(dt).strftime('%Y-%m-%d')
                    if sd > safe_last_date: return float(ratio), sd
        except: pass
        return 0.0, ""

    def get_dynamic_sniper_target(self, index_ticker):
        target_index = "SOXX" if index_ticker in ["SOXX", "SOXL"] else index_ticker
        try:
            class TargetFloat(float): pass
            if target_index == "SOXX":
                hv, w, td, ba = ve.get_soxl_target_drop_full()
                ret = TargetFloat(td); ret.metric_val, ret.weight, ret.base_amp, ret.metric_name, ret.metric_base = hv, w, ba, "SOXX HV", round(hv/w, 2) if w>0 else 25.0
            else:
                vxn, w, td, ba = ve.get_tqqq_target_drop_full()
                ret = TargetFloat(td); ret.metric_val, ret.weight, ret.base_amp, ret.metric_name, ret.metric_base = vxn, w, ba, "실시간 VXN", round(vxn/w, 2) if w>0 else 20.0
            ret.is_panic, ret.gap_pct = False, 0.0; return ret
        except:
            fb = -8.79 if target_index == "SOXX" else -4.95
            ret = TargetFloat(fb); ret.metric_val, ret.weight, ret.base_amp, ret.metric_name, ret.metric_base = 0.0, 1.0, fb, "통신오류", 25.0 if target_index == "SOXX" else 20.0
            ret.is_panic, ret.gap_pct = False, 0.0; return ret

    def get_day_high_low(self, ticker):
        """ 🚨 [제3헌법] 정규장 데이터 슬라이싱 디커플링 """
        try:
            hist = yf.Ticker(ticker).history(period="1d", interval="1m", prepost=True, timeout=5)
            if not hist.empty:
                hist = _flatten_columns(hist)
                return float(hist['High'].max()), float(hist['Low'].min())
        except: pass
        try:
            res = self._call_api("HHDFS76200200", "/uapi/overseas-price/v1/quotations/price", "GET", params={"AUTH": "", "EXCD": self._get_exchange_code(ticker, target_api="PRICE"), "SYMB": ticker})
            if res.get('rt_cd') == '0':
                out = res.get('output', {})
                return float(out.get('high', 0.0)), float(out.get('low', 0.0))
        except: pass
        return 0.0, 0.0

    def get_atr_data(self, ticker):
        try:
            hist = yf.Ticker(ticker).history(period="30d", prepost=True, timeout=5)
            if hist.empty or len(hist) < 15: return 0.0, 0.0
            hist['Prev_Close'] = hist['Close'].shift(1)
            hist = hist.dropna(subset=['High', 'Low', 'Close']).copy()
            hist['TR'] = hist.apply(lambda row: max(row['High']-row['Low'], abs(row['High']-row['Prev_Close']), abs(row['Low']-row['Prev_Close'])), axis=1)
            hist['ATR5'], hist['ATR14'] = hist['TR'].rolling(window=5).mean(), hist['TR'].rolling(window=14).mean()
            last = hist.iloc[-1]
            if last['Close'] > 0: return round((float(last['ATR5'])/last['Close'])*100, 1), round((float(last['ATR14'])/last['Close'])*100, 1)
        except: pass
        return 0.0, 0.0
