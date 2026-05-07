import os

# MODIFIED: [출력 파일명 락온] 사용자 지시에 따라 code.txt 유지
output_filename = 'code.txt'

# 합칠 파일들이 들어있는 폴더 경로 (기본값: 현재 폴더)
folder_path = '.' 

# NEW: [코어 아키텍처 화이트리스트 락온] 노이즈 차단을 위해 병합 대상 .py 파일을 명시적으로 하드코딩
TARGET_FILES = [
    "broker.py",
    "config.py",
    "main.py",
    "manual_ledger.py",
    "plugin_updater.py",
    "queue_ledger.py",
    "scheduler_core.py",
    "scheduler_regular.py",
    "scheduler_sniper.py",
    "scheduler_vwap.py",
    "strategy.py",
    "strategy_reversion.py",
    "strategy_v14.py",
    "strategy_v14_vwap.py",
    "strategy_v_avwap.py",
    "telegram_avwap_console.py",
    "telegram_bot.py",
    "telegram_callbacks.py",
    "telegram_states.py",
    "telegram_sync_engine.py",
    "telegram_view.py",
    "version_history.py",
    "volatility_engine.py",
    "vwap_data.py"
]

with open(output_filename, 'w', encoding='utf-8') as outfile:
    # MODIFIED: [화이트리스트 스캔 복원] 디렉토리 전체 스캔을 폐기하고 TARGET_FILES 배열을 순회하여 병합
    for filename in TARGET_FILES:
        file_path = os.path.join(folder_path, filename)
        
        # NEW: [파일 존재 여부 검증 방어막] 파일이 누락되었을 경우 발생하는 FileNotFoundError 런타임 붕괴 방어
        if os.path.isfile(file_path):
            outfile.write(f"\n{'='*50}\n")
            outfile.write(f"FILE: {filename}\n")
            outfile.write(f"{'='*50}\n\n")
            
            with open(file_path, 'r', encoding='utf-8') as infile:
                outfile.write(infile.read())
                outfile.write("\n")
        else:
            print(f"⚠️ 경고: {filename} 파일을 찾을 수 없어 병합에서 제외되었습니다.")

print(f"🚀 성공! 화이트리스트에 지정된 파이썬 코드가 '{output_filename}' 파일에 무결점으로 병합 완료되었습니다.")
