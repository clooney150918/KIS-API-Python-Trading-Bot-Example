# ==========================================================
# [volatility_engine.py] - 🌟 100% 통합 무결점 완성본 🌟
# ⚠️ V3.2 패치: 기초지수 1년 ATR 절대 진폭 고정 및 공포지수 방향타 스위치 엔진 탑재
# 💡 [V24.09 패치] 야후 파이낸스 교착(Deadlock) 방어용 timeout=5 전면 이식 완료
# 💡 [V24.11 패치] 클래스 래퍼(VolatilityEngine) 구조 도입 및 calculate_weight 공통 인터페이스 신설
# 🚨 [PEP 8 포맷팅 패치] 미사용 변수(weight) 100% 소각 (Ruff F841 교정 완료)
# 🚨 [V27.17 그랜드 수술] 코파일럿 합작 - 가중치 무제한 폭주(Black Swan) 락온 방어(0.5~2.0), 
# UnboundLocalError 런타임 즉사 교정, 임시 파일 찌꺼기(Disk Leak) 소각, 
# 야후 파이낸스 다중인덱스(MultiIndex) 붕괴 스마트 우회 엔진 및 ATR 최소 데이터 검증망 이식
# 🚨 MODIFIED: [V40.XX 옴니 매트릭스 전면 수술] 후행성 60MA/120MA 엔진 전면 소각 및
# 전일 VWAP vs 당일 실시간 VWAP 동행 지표(Coincident Indicator) 듀얼 모멘텀 엔진으로 100% 교체.
# 🚨 MODIFIED: [V61.00 숏(SOXS) 전면 소각 작전 지시서 적용]
# _fetch_vwap_momentum_regime_sync 내부의 하락장(BEAR, SOXS) 판별 블록 전면 소각 및 하락장 시 NONE 타겟 락온으로 간소화.
# 🚨 MODIFIED: [V61.01 숏(SOXS) 전면 소각 작전 지시서 적용] determine_market_regime 독스트링 내 SOXS 환각 텍스트 100% 영구 적출 완료.
# 🚨 MODIFIED: [V61.03 데드코드 소각] 시스템 전역에서 호출되지 않는 레거시 함수 get_tqqq_target_drop, get_soxl_target_drop 영구 적출 완료.
# 🚨 MODIFIED: [V61.04 들여쓰기 붕괴 방어] 런타임 즉사(IndentationError)를 유발하던 스페이스 오차 100% 팩트 교정 완료.
# 🚨 MODIFIED: [제4경고 절대 헌법 준수] 횡보장 락다운 영구 소각 및 롱(SOXL) 진입 무조건 허용 락온
# 🚨 MODIFIED: [결함 3 수술] ATR14 연산 내 ZeroDivision 런타임 붕괴 방어용 replace(0, np.nan) 락온 팩트 결속
# 🚨 NEW: [Case 32, Case 33] yfinance 타임아웃 3단 지수 백오프 및 TPS 캡핑 방어막 전면 이식 완료
# ==========================================================
import yfinance as yf
import pandas as pd
import numpy as np
import os
import json
import tempfile
import logging
import asyncio
import time
from zoneinfo import ZoneInfo
from datetime import datetime

CACHE_FILE = "data/volatility_cache.json"

WEIGHT_MIN = 0.5   
WEIGHT_MAX = 2.0   

QQQ_DEFAULT_ATR_PCT  = 1.65   
SOXX_DEFAULT_ATR_PCT = 2.93   
MIN_ATR_ROWS = 14  

def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        if 'Ticker' in df.columns.names:
            df.columns = df.columns.droplevel('Ticker')
        elif df.columns.nlevels == 2:
            price_fields = {'Close', 'High', 'Low', 'Open', 'Volume', 'Adj Close'}
            level0_vals = set(df.columns.get_level_values(0))
            drop_level = 0 if not level0_vals.intersection(price_fields) else 1
            df.columns = df.columns.droplevel(drop_level)
    return df

def _load_cache(key, default_val):
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                data = json.load(f)
                val = data.get(key)
                if val is not None and float(val) > 0:
                    return float(val)
        except Exception:
            pass
    return default_val

def _save_cache(key, value):
    data = {}
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                data = json.load(f)
        except Exception:
            pass
    
    data[key] = value
    
    dir_name = os.path.dirname(CACHE_FILE)
    if dir_name and not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)
         
    fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, CACHE_FILE)
    except Exception as e:
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        logging.error(f"⚠️ [Engine] 캐시 저장 실패 및 임시 파일 소각: {e}")

# 🚨 MODIFIED: [Case 33] 3단 지수 백오프 이식
def _calculate_1y_atr(ticker, cache_key, default_atr):
    for attempt in range(3):
        try:
            time.sleep(0.06) # 🚨 MODIFIED: [Case 32] TPS 캡핑
            df = yf.download(ticker, period="2y", interval="1d", progress=False, timeout=5)
            if df.empty:
                if attempt < 2:
                    time.sleep(1.0 * (2 ** attempt))
                    continue
                return _load_cache(cache_key, default_atr)
                
            df = _flatten_columns(df)
                    
            df['Prev_Close'] = df['Close'].shift(1)
            
            tr1 = df['High'] - df['Low']
            tr2 = (df['High'] - df['Prev_Close']).abs()
            tr3 = (df['Low'] - df['Prev_Close']).abs()
            
            df['TR'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            df['ATR14'] = df['TR'].rolling(window=14).mean()
            
            df['Close'] = df['Close'].replace(0, np.nan)
            df['ATR14_pct'] = (df['ATR14'] / df['Close']) * 100
            
            df_valid = df.dropna(subset=['ATR14_pct'])
            df_1y = df_valid.tail(252)
            
            if df_1y.empty or len(df_1y) < MIN_ATR_ROWS:
                logging.warning(f"⚠️ [Engine] {ticker} ATR 데이터 부족 ({len(df_1y)}행 < {MIN_ATR_ROWS}): 캐시/기본값 사용")
                return _load_cache(cache_key, default_atr)
                
            atr_1y_avg = float(df_1y['ATR14_pct'].mean())
            if pd.isna(atr_1y_avg) or atr_1y_avg <= 0:
                raise ValueError("Invalid ATR")
                
            _save_cache(cache_key, atr_1y_avg)
            return atr_1y_avg
            
        except Exception as e:
            logging.debug(f"⚠️ [Engine] {ticker} ATR 연산 오류 (시도 {attempt+1}/3): {e}")
            if attempt < 2: time.sleep(1.0 * (2 ** attempt))
    return _load_cache(cache_key, default_atr)

def get_tqqq_target_drop_full():
    for attempt in range(3):
        try:
            time.sleep(0.06)
            vxn_data = yf.download("^VXN", period="2y", interval="1d", progress=False, timeout=5)
            
            if vxn_data.empty: 
                if attempt < 2:
                    time.sleep(1.0 * (2 ** attempt))
                    continue
                fallback_amp = round(-(QQQ_DEFAULT_ATR_PCT * 3), 2)
                return 0.0, 1.0, fallback_amp, fallback_amp
                
            vxn_data = _flatten_columns(vxn_data)
                    
            valid_closes = vxn_data['Close'].dropna()
            valid_closes_1y = valid_closes.tail(252)
            
            if valid_closes_1y.empty:
                if attempt < 2:
                    time.sleep(1.0 * (2 ** attempt))
                    continue
                fallback_amp = round(-(QQQ_DEFAULT_ATR_PCT * 3), 2)
                return 0.0, 1.0, fallback_amp, fallback_amp
                 
            current_vxn = float(valid_closes_1y.iloc[-1])
            
            try:
                mean_vxn = float(valid_closes_1y.mean())
                if pd.isna(mean_vxn) or mean_vxn <= 0:
                    raise ValueError("Invalid Mean")
                _save_cache("VXN_MEAN", mean_vxn)
            except Exception:
                mean_vxn = _load_cache("VXN_MEAN", 20.0)
                
            if mean_vxn <= 0:
                weight = 1.0
            else:
                raw_weight = current_vxn / mean_vxn
                weight = max(WEIGHT_MIN, min(WEIGHT_MAX, raw_weight))
            
            qqq_1y_atr = _calculate_1y_atr("QQQ", "QQQ_ATR_1Y", QQQ_DEFAULT_ATR_PCT)
            base_amp = round(-(qqq_1y_atr * 3), 2)
            target_drop = base_amp
            
            return current_vxn, weight, target_drop, base_amp
            
        except Exception as e:
            if attempt == 2:
                logging.error(f"❌ VXN 상세 스캔 오류: {e}")
                fallback_amp = round(-(QQQ_DEFAULT_ATR_PCT * 3), 2)
                return 0.0, 1.0, fallback_amp, fallback_amp
            time.sleep(1.0 * (2 ** attempt))

def get_soxl_target_drop_full():
    for attempt in range(3):
        try:
            time.sleep(0.06)
            soxx_data = yf.download("SOXX", period="2y", interval="1d", progress=False, timeout=5)
            if soxx_data.empty or len(soxx_data) < 21: 
                if attempt < 2:
                    time.sleep(1.0 * (2 ** attempt))
                    continue
                fallback_amp = round(-(SOXX_DEFAULT_ATR_PCT * 3), 2)
                return 0.0, 1.0, fallback_amp, fallback_amp
            
            soxx_data = _flatten_columns(soxx_data)
                    
            closes = soxx_data['Close'].dropna()
            log_returns = np.log(closes / closes.shift(1))
            hv_20d = log_returns.rolling(window=20).std() * np.sqrt(252) * 100
            
            valid_hvs = hv_20d.dropna()
            valid_hvs_1y = valid_hvs.tail(252)
            
            if valid_hvs_1y.empty:
                if attempt < 2:
                    time.sleep(1.0 * (2 ** attempt))
                    continue
                fallback_amp = round(-(SOXX_DEFAULT_ATR_PCT * 3), 2)
                return 0.0, 1.0, fallback_amp, fallback_amp
                
            latest_hv = float(valid_hvs_1y.iloc[-1])
             
            try:
                mean_hv = float(valid_hvs_1y.mean())
                if pd.isna(mean_hv) or mean_hv <= 0:
                    raise ValueError("Invalid Mean")
                _save_cache("SOXX_HV_MEAN", mean_hv)
            except Exception:
                mean_hv = _load_cache("SOXX_HV_MEAN", 25.0)
             
            if mean_hv <= 0:
                weight = 1.0
            else:
                raw_weight = latest_hv / mean_hv
                weight = max(WEIGHT_MIN, min(WEIGHT_MAX, raw_weight))
            
            soxx_1y_atr = _calculate_1y_atr("SOXX", "SOXX_ATR_1Y", SOXX_DEFAULT_ATR_PCT)
            base_amp = round(-(soxx_1y_atr * 3), 2)
            target_drop = base_amp
            
            return latest_hv, weight, target_drop, base_amp
            
        except Exception as e:
            if attempt == 2:
                logging.error(f"❌ SOXX HV 상세 연산 오류: {e}")
                fallback_amp = round(-(SOXX_DEFAULT_ATR_PCT * 3), 2)
                return 0.0, 1.0, fallback_amp, fallback_amp
            time.sleep(1.0 * (2 ** attempt))

def _fetch_vwap_momentum_regime_sync(broker_instance=None) -> dict:
    for attempt in range(3):
        try:
            time.sleep(0.06)
            ticker = yf.Ticker("SOXX")
            df = ticker.history(period="1d", interval="1m", prepost=False, timeout=5)
            
            if df.empty:
                if attempt == 2: return {"status": "error", "msg": "YF 실시간 1분봉 데이터 부재"}
                time.sleep(1.0 * (2 ** attempt))
                continue
                
            df = _flatten_columns(df)
            
            day_open = float(df['Open'].iloc[0]) if not pd.isna(df['Open'].iloc[0]) else 0.0
            current_price = float(df['Close'].iloc[-1]) if not pd.isna(df['Close'].iloc[-1]) else 0.0
            
            if day_open == 0.0 or current_price == 0.0:
                if attempt == 2: return {"status": "error", "msg": "결측치(NaN) 유입으로 시가/현재가 연산 불가"}
                time.sleep(1.0 * (2 ** attempt))
                continue

            if broker_instance is not None:
                prev_vwap, curr_vwap = broker_instance.get_daily_vwap_info("SOXX")
            else:
                from broker import KoreaInvestmentBroker
                temp_broker = KoreaInvestmentBroker("MOCK", "MOCK", "MOCK")
                prev_vwap, curr_vwap = temp_broker.get_daily_vwap_info("SOXX")

            if prev_vwap == 0.0 or curr_vwap == 0.0:
                if attempt == 2: return {"status": "error", "msg": "VWAP 파싱 실패 (결측치 유입)"}
                time.sleep(1.0 * (2 ** attempt))
                continue

            if curr_vwap > prev_vwap and current_price > day_open:
                regime = "BULL"
                target_ticker = "SOXL"
                msg_desc = "상승장 (VWAP 상승 & 양봉)"
            elif curr_vwap < prev_vwap and current_price < day_open:
                regime = "BEAR"
                target_ticker = "NONE" 
                msg_desc = "하락장 (VWAP 하락 & 음봉) - 숏 타격 영구 소각"
            else:
                regime = "SIDEWAYS"
                target_ticker = "SOXL"
                msg_desc = "횡보장 (VWAP과 캔들 방향 충돌)"
                
            return {
                "status": "success",
                "regime": regime,
                "target_ticker": target_ticker,
                "close": current_price,
                "prev_vwap": prev_vwap,
                "curr_vwap": curr_vwap,
                "day_open": day_open,
                "desc": msg_desc
            }
            
        except Exception as e:
            logging.debug(f"⚠️ 옴니 매트릭스 에러 (시도 {attempt+1}/3): {e}")
            if attempt == 2: return {"status": "error", "msg": str(e)}
            time.sleep(1.0 * (2 ** attempt))

async def determine_market_regime(broker_instance=None) -> dict:
    try:
        result = await asyncio.wait_for(
            asyncio.to_thread(_fetch_vwap_momentum_regime_sync, broker_instance),
            timeout=15.0
        )
        return result
    except asyncio.TimeoutError:
        return {"status": "error", "msg": "YF 통신 타임아웃 (15초 초과)"}
    except Exception as e:
        return {"status": "error", "msg": f"비동기 래핑 오류: {str(e)}"}

class VolatilityEngine:
    def __init__(self):
        pass
        
    def calculate_weight(self, ticker):
        try:
            if ticker == "TQQQ":
                _, weight, _, _ = get_tqqq_target_drop_full()
            elif ticker == "SOXL":
                _, weight, _, _ = get_soxl_target_drop_full()
            else:
                weight = 1.0

            clamped = max(WEIGHT_MIN, min(WEIGHT_MAX, float(weight)))
            return {'weight': clamped}

        except Exception as e:
            logging.error(f"⚠️ [VolatilityEngine] {ticker} 가중치 산출 래퍼 오류: {e}")
            return {'weight': 1.0}
