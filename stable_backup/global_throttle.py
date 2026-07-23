# ==========================================================
# FILE: global_throttle.py
# 🚨 NEW: 1인용 로컬 봇 극한 최적화를 위한 중앙 통제소
# 🚨 1. KIS API 글로벌 토큰 버킷 (초당 18건 캡핑 강제)
# 🚨 2. JSON 파일 병렬 I/O 충돌 방어용 File Mutex (경쟁 조건 차단)
# 🚨 MODIFIED: [데드락 원천 차단] 동일 스레드 내 중복 락 획득 시 발생하는 교착 상태(Deadlock)를 방어하기 위해 threading.Lock()을 threading.RLock()으로 전면 교체 완료.
# 🚨 MODIFIED: [Lost Update 궁극 방어] 파일 경로 정규화 패러독스 및 defaultdict 락 발급 경합 조건 완벽 수술 완료.
# ==========================================================
import os
import time
import threading

class GlobalThrottle:
    _instance = None
    # 🚨 MODIFIED: [데드락 붕괴 수술] 싱글톤 락 RLock 교체
    _lock = threading.RLock()
    
    # 🚨 API TPS 제어 (초당 20건 제한 -> 여유 버퍼 고려 초당 18건: 0.055초 간격)
    # 🚨 MODIFIED: [데드락 붕괴 수술] API 락 RLock 교체
    _api_lock = threading.RLock()
    _last_api_call = 0.0
    _min_api_interval = 0.055 
    
    # 🚨 파일 I/O 충돌 방지용 경로별 독립 Lock
    # 🚨 MODIFIED: [딕셔너리 경합 원천 차단] defaultdict 소각 및 순수 dict 락온
    _file_locks = {}

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(GlobalThrottle, cls).__new__(cls)
        return cls._instance

    @classmethod
    def wait_api_sync(cls):
        """ 
        🚨 [API 썬더링 허드 완벽 방어] 
        비동기 태스크 50개가 동시에 to_thread로 깨어나더라도, 
        이 Lock을 통해 KIS 서버에는 무조건 0.055초 간격으로 1발씩 정밀하게 발사됩니다.
        """
        with cls._api_lock:
            now = time.perf_counter()
            elapsed = now - cls._last_api_call
            if elapsed < cls._min_api_interval:
                time.sleep(cls._min_api_interval - elapsed)
            cls._last_api_call = time.perf_counter()

    @classmethod
    def get_file_lock(cls, filepath: str) -> threading.RLock:
        """ 
        🚨 [Lost Update 원천 차단] 
        파일 경로별로 독립적인 Mutex Lock을 반환하여, 
        A 스레드가 읽고 쓰는 동안 B 스레드가 개입하여 데이터가 증발하는 현상을 차단합니다.
        """
        # 🚨 MODIFIED: [경로 정규화 패러독스 방어] 절대 경로 기반 SSOT 식별자 100% 락온
        normalized_path = os.path.abspath(os.path.normpath(filepath)).lower()
        
        # 🚨 MODIFIED: [락 발급기 경합 원천 차단] 싱글톤 락 기반 원자적 락 발급 강제
        with cls._lock:
            if normalized_path not in cls._file_locks:
                cls._file_locks[normalized_path] = threading.RLock()
            return cls._file_locks[normalized_path]
