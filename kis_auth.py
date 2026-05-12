"""
Created on 2025-07-01
KIS API 인증, 토큰 관리 및 HTTP 통신 래퍼 코어 (V71.00 무결점 방탄 아키텍처)
"""

import os
import json
import time
import tempfile
import logging
import requests
from datetime import datetime
# NEW: [제3헌법, 제10경고] pytz 영구 적출 및 ZoneInfo 100% 락온
from zoneinfo import ZoneInfo

# 로깅 설정
logger = logging.getLogger(__name__)

# NEW: [제3헌법] 전역 타임존 단일 소스 락온
EST_TZ = ZoneInfo('America/New_York')
KST_TZ = ZoneInfo('Asia/Seoul')

TOKEN_FILE = "kis_token.json"
CONFIG_FILE = "config.json"

# API 도메인 락온
REAL_DOMAIN = "https://openapi.koreainvestment.com:9443"
DEMO_DOMAIN = "https://openapivts.koreainvestment.com:29443"

class DictWrapper:
    """
    [치명적 경고 5] 결측치(None) 런타임 붕괴 방어용 Safe Casting 래퍼
    """
    def __init__(self, d):
        self._data = d if isinstance(d, dict) else {}
        for k, v in self._data.items():
            if isinstance(v, dict):
                setattr(self, k, DictWrapper(v))
            elif isinstance(v, list):
                setattr(self, k, [DictWrapper(i) if isinstance(i, dict) else i for i in v])
            else:
                setattr(self, k, v)

    def __getattr__(self, item):
        return None

    def get(self, key, default=None):
        return self._data.get(key, default)

class KISResponse:
    """
    KIS API 통신 규격 멱등성 보장 래퍼 클래스
    """
    def __init__(self, status_code: int, headers: dict, body: dict):
        self.status_code = status_code
        self.headers = headers
        self.body = body
        self.header_wrap = DictWrapper(headers)
        self.body_wrap = DictWrapper(body)

    def isOK(self) -> bool:
        # KIS API 성공 코드 '0' 팩트 스캔
        return self.status_code == 200 and str(self.body.get('rt_cd', '')) == '0'

    def getBody(self):
        return self.body_wrap

    def getHeader(self):
        return self.header_wrap

    def getErrorCode(self) -> str:
        return str(self.body.get('msg_cd', ''))

    def getErrorMessage(self) -> str:
        return str(self.body.get('msg1', ''))

    def printError(self, url: str):
        logger.error(f"🚨 API Error [{url}]: {self.getErrorCode()} - {self.getErrorMessage()}")

def _read_config() -> dict:
    """스레드 세이프 동기 설정 읽기"""
    # NEW: [제16경고] 스코프 리프트
    data = {}
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
    except Exception as e:
        logger.error(f"🚨 환경설정 읽기 실패: {e}")
    return data

def _write_token_sync(data: dict):
    """
    [제4헌법, 치명적 경고 8] 파일 파손(Torn Write) 방지 원자적 쓰기 코어
    """
    # NEW: [제16경고] 스코프 리프트
    temp_name = ""
    dir_name = ""
    
    try:
        dir_name = os.path.dirname(TOKEN_FILE) or "."
        with tempfile.NamedTemporaryFile('w', dir=dir_name, delete=False, encoding='utf-8') as tf:
            json.dump(data, tf, indent=4, ensure_ascii=False)
            temp_name = tf.name
            
        os.replace(temp_name, TOKEN_FILE)
    except Exception as e:
        logger.error(f"🚨 토큰 원자적 쓰기 중 치명적 오류: {e}")
        if temp_name and os.path.exists(temp_name):
            try:
                os.remove(temp_name)
            except:
                pass

def _read_token_sync() -> dict:
    """토큰 파일 읽기 코어"""
    # NEW: [제16경고] 스코프 리프트
    data = {}
    try:
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
    except Exception as e:
        logger.error(f"🚨 토큰 읽기 실패: {e}")
    return data

def issue_token(env_dv: str, app_key: str, app_secret: str) -> str:
    """토큰 신규 발급 엔진"""
    # NEW: [제16경고] 스코프 리프트
    base_url = ""
    url = ""
    headers = {}
    body = {}
    res = None
    data = {}
    new_token = ""
    
    try:
        base_url = REAL_DOMAIN if env_dv == "real" else DEMO_DOMAIN
        url = f"{base_url}/oauth2/tokenP"
        
        headers = {"content-type": "application/json"}
        body = {
            "grant_type": "client_credentials",
            "appkey": app_key,
            "appsecret": app_secret
        }
        
        # [치명적 경고 6] 동기 I/O 타임아웃 10초 족쇄
        res = requests.post(url, headers=headers, json=body, timeout=10.0)
        if res.status_code == 200:
            data = res.json()
            new_token = data.get("access_token", "")
            
            if new_token:
                # 토큰 발급 시각 KST 저장 (KIS API 서버 기준 만료 판별용)
                token_data = {
                    "access_token": new_token,
                    "issue_time_kst": datetime.now(KST_TZ).strftime("%Y-%m-%d %H:%M:%S")
                }
                _write_token_sync(token_data)
                logger.info("✅ KIS API 인증 토큰 신규 발급 및 원자적 쓰기 무결점 완료.")
        else:
            logger.error(f"🚨 토큰 발급 API 오류: {res.status_code} - {res.text}")
            
    except Exception as e:
        logger.error(f"🚨 토큰 발급 중 런타임 붕괴 방어: {e}")
        
    return new_token

def get_valid_token(env_dv: str, app_key: str, app_secret: str) -> str:
    """캐시된 토큰 유효성 검증 및 반환 코어"""
    # NEW: [제16경고] 스코프 리프트
    token_data = {}
    token = ""
    issue_time_str = ""
    issue_time = None
    now_kst = None
    
    try:
        token_data = _read_token_sync()
        token = token_data.get("access_token", "")
        issue_time_str = token_data.get("issue_time_kst", "")
        
        if token and issue_time_str:
            issue_time = datetime.strptime(issue_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST_TZ)
            now_kst = datetime.now(KST_TZ)
            # 토큰 유효기간 24시간. 23시간 경과 시 자동 갱신 락온
            if (now_kst - issue_time).total_seconds() < 82800:
                return token
                
        logger.info("🔹 캐시된 토큰이 만료되었거나 존재하지 않습니다. 신규 발급 파이프라인을 가동합니다.")
        token = issue_token(env_dv, app_key, app_secret)
        
    except Exception as e:
        logger.error(f"🚨 토큰 검증 중 오류 발생: {e}")
        
    return token

def get_hashkey(env_dv: str, app_key: str, app_secret: str, data: dict) -> str:
    """POST 통신용 Hashkey 발급 엔진"""
    # NEW: [제16경고] 스코프 리프트
    base_url = ""
    url = ""
    headers = {}
    res = None
    hash_val = ""
    
    try:
        base_url = REAL_DOMAIN if env_dv == "real" else DEMO_DOMAIN
        url = f"{base_url}/uapi/hashkey"
        headers = {
            "content-type": "application/json",
            "appkey": app_key,
            "appsecret": app_secret
        }
        
        res = requests.post(url, headers=headers, json=data, timeout=10.0)
        if res.status_code == 200:
            hash_val = res.json().get("HASH", "")
        else:
            logger.error(f"🚨 Hashkey 발급 실패: {res.status_code} - {res.text}")
            
    except Exception as e:
        logger.error(f"🚨 Hashkey 발급 중 치명적 런타임 오류: {e}")
        
    return hash_val

def _url_fetch(api_url: str, ptr_id: str, tr_cont: str, params: dict, postFlag: bool = False) -> KISResponse:
    """
    [제1헌법] 통신 I/O 래퍼 코어 (broker.py에서 asyncio.to_thread로 감싸져 호출됨)
    """
    # NEW: [제16경고] 스코프 리프트
    config_data = {}
    env_dv = "real"
    app_key = ""
    app_secret = ""
    cano = ""
    acnt_prdt_cd = ""
    base_url = ""
    full_url = ""
    token = ""
    headers = {}
    res = None
    kis_res = None
    
    try:
        config_data = _read_config()
        env_dv = config_data.get("env_dv", "real")
        app_key = config_data.get("app_key", "")
        app_secret = config_data.get("app_secret", "")
        cano = config_data.get("cano", "")
        acnt_prdt_cd = config_data.get("acnt_prdt_cd", "")
        
        base_url = REAL_DOMAIN if env_dv == "real" else DEMO_DOMAIN
        full_url = f"{base_url}{api_url}"
        
        token = get_valid_token(env_dv, app_key, app_secret)
        
        headers = {
            "content-type": "application/json",
            "authorization": f"Bearer {token}",
            "appkey": app_key,
            "appsecret": app_secret,
            "tr_id": ptr_id,
            "custtype": "P"
        }
        
        if tr_cont:
            headers["tr_cont"] = tr_cont
            
        if postFlag:
            # POST 전용 Hashkey 장입 및 멱등성 락온
            headers["hashkey"] = get_hashkey(env_dv, app_key, app_secret, params)
            res = requests.post(full_url, headers=headers, json=params, timeout=10.0)
        else:
            res = requests.get(full_url, headers=headers, params=params, timeout=10.0)
            
        kis_res = KISResponse(res.status_code, dict(res.headers), res.json() if res.text else {})
        
    except requests.exceptions.Timeout:
        logger.error(f"🚨 KIS API 통신 중 10초 타임아웃 피격: {full_url}")
        kis_res = KISResponse(504, {}, {"msg_cd": "TIMEOUT", "msg1": "API 타임아웃 발생"})
    except Exception as e:
        logger.error(f"🚨 KIS API 통신 중 런타임 붕괴 방어: {e}")
        kis_res = KISResponse(500, {}, {"msg_cd": "ERROR", "msg1": str(e)})
        
    return kis_res

def smart_sleep(seconds: float = 0.2):
    """API Rate Limit 초과 방어용 슬립 엔진"""
    time.sleep(seconds)
