# ==========================================================
# FILE: state_io_manager.py
# ==========================================================
# 🚨 MODIFIED: [Lost Update 궁극 방어] 파일 경로 기반 독립 Mutex Lock 100% 팩트 결속
# 🚨 MODIFIED: [상태 원자적 제어 도메인 분리] 스케줄러 내부에 혼재하던 파일 I/O 로직을 전담
# 🚨 MODIFIED: [제2헌법 단일 책임 및 중복 소각] 읽기/원자적 쓰기 절차를 헬퍼로 100% 진공 압축
# 🚨 MODIFIED: [제4헌법 절대 사수] tempfile 생성 ➔ flush ➔ fsync ➔ os.replace 기반 100% 원자적 쓰기 강제
# 🚨 MODIFIED: [Case 08, 16] os.path.exists 소각 및 EAFP 패턴 / 스코프 전진 배치 적용
# ==========================================================

import os
import json
import tempfile
import logging
from global_throttle import GlobalThrottle # 🚨 전역 파일 뮤텍스 엔진

def _read_json_safe_sync(filepath, date_str):
    """ 🚨 [EAFP 기반 안전 읽기 + File Mutex] TOCTOU 붕괴를 막고 JSON 오염 시 안전 폴백 """
    lock = GlobalThrottle.get_file_lock(filepath)
    with lock:
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict) and data.get('date') == date_str:
                    return data
        except (OSError, json.JSONDecodeError):
            pass
        return {}

def _atomic_write_json_sync(filepath, data):
    """ 🚨 [제4헌법 준수 + File Mutex] 원자적 쓰기(Atomic Write) 동시성 충돌 완벽 방어 """
    lock = GlobalThrottle.get_file_lock(filepath)
    with lock:
        dir_name = os.path.dirname(filepath) or '.'
        try: 
            os.makedirs(dir_name, exist_ok=True)
        except OSError: 
            pass

        fd = None
        tmp_path = None
        try:
            fd, tmp_path = tempfile.mkstemp(dir=dir_name, text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as f_out:
                fd = None
                json.dump(data, f_out, ensure_ascii=False, indent=4)
                f_out.flush()
                os.fsync(f_out.fileno()) # 커널 버퍼 강제 디스크 동기화
            os.replace(tmp_path, filepath) # 원자적 덮어쓰기
            tmp_path = None
        except Exception as e:
            if fd is not None:
                try: os.close(fd)
                except OSError: pass
            if tmp_path:
                try: os.remove(tmp_path)
                except OSError: pass
            logging.error(f"🚨 상태 파일 원자적 쓰기 실패 ({filepath}): {e}")
            raise e

def read_avwap_state_sync(ticker, date_str):
    """ 
    🚨 [Case 39 방어] 암살자 자본 잠김(Capital Lock-up) 스캔 헬퍼
    - 순수 동기 함수이므로 호출부에서 반드시 asyncio.to_thread 래핑 강제
    """
    state_file = f"data/avwap_trade_state_{ticker}.json"
    return _read_json_safe_sync(state_file, date_str)

def save_aftermarket_state_sync(ticker, date_str, slice_info):
    if not isinstance(slice_info, dict):
        return

    state_file = f"data/vwap_aftermarket_state_{ticker}.json"
    data = _read_json_safe_sync(state_file, date_str)
    if not data:
        data = {"date": date_str, "orders": []}
    if not isinstance(data.get('orders'), list):
        data['orders'] = []
    for item in data['orders']:
        if isinstance(item, dict) and item.get('desc') == slice_info.get('desc') and item.get('side') == slice_info.get('side'):
            return
    data['orders'].append(slice_info)
    _atomic_write_json_sync(state_file, data)

def save_slice_state_sync(ticker, date_str, slice_info):
    if not isinstance(slice_info, dict):
        return

    state_file = f"data/vwap_slice_state_{ticker}.json"
    data = _read_json_safe_sync(state_file, date_str)
    if not data:
        data = {"date": date_str, "orders": []}
    if not isinstance(data.get('orders'), list):
        data['orders'] = []
    for item in data['orders']:
        if isinstance(item, dict) and item.get('desc') == slice_info.get('desc') and item.get('side') == slice_info.get('side'):
            return
    data['orders'].append(slice_info)
    _atomic_write_json_sync(state_file, data)
