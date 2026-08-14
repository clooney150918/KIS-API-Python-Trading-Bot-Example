# ==========================================================
# FILE: global_throttle.py
# ==========================================================
# 🚨 NEW: 1인용 로컬 봇 극한 최적화를 위한 중앙 통제소
# 🚨 1. KIS API 글로벌 토큰 버킷 (초당 18건 캡핑 강제)
# 🚨 2. JSON 파일 병렬 I/O 충돌 방어용 File Mutex (경쟁 조건 차단)
# 🚨 MODIFIED: [데드락 원천 차단] 동일 스레드 내 중복 락 획득 시 발생하는 교착 상태(Deadlock)를 방어하기 위해 threading.Lock()을 threading.RLock()으로 전면 교체 완료.
# 🚨 MODIFIED: [Lost Update 궁극 방어] 파일 경로 정규화 패러독스 및 defaultdict 락 발급 경합 조건 완벽 수술 완료.
# 🚨 MODIFIED: [ThreadPool 고갈 패러독스 궁극 수술] 락(Lock)을 쥔 상태에서 time.sleep()을 실행하여 백그라운드 스레드풀 전체를 마비시키던 맹독성 병목을 원천 소각하고, '미래 슬롯 예약(Non-blocking Token Bucket)' 방식으로 재설계하여 100% 병렬 비동기 대기를 락온 완료.
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
        🚨 [API 썬더링 허드 및 ThreadPool 고갈 완벽 방어] 
        락 내부에서 sleep을 강행하여 시스템을 멈추게 하던 코드를 파기하고, 
        자신의 슬롯을 예약한 뒤 락을 즉각 해제하여 스레드들이 외부에서 병렬로 대기하도록 구조 혁신.
        """
        sleep_time = 0.0
        with cls._api_lock:
            now = time.perf_counter()
            # 🚨 MODIFIED: 미래 시점 슬롯 예약 구조 팩트 주입
            if cls._last_api_call > now:
                sleep_time = cls._last_api_call + cls._min_api_interval - now
                cls._last_api_call = cls._last_api_call + cls._min_api_interval
            else:
                elapsed = now - cls._last_api_call
                if elapsed < cls._min_api_interval:
                    sleep_time = cls._min_api_interval - elapsed
                    cls._last_api_call = now + sleep_time
                else:
                    cls._last_api_call = now

        # 🚨 MODIFIED: 락을 해제한 상태에서 안전하게 병렬 대기
        if sleep_time > 0:
            time.sleep(sleep_time)

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
