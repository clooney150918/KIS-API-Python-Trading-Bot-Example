import os

# MODIFIED: [출력 파일명 변경] 사용자 지시에 따라 combined_code.txt에서 code.txt로 하드코딩 락온
output_filename = 'code.txt'

# 합칠 파일들이 들어있는 폴더 경로 (기본값: 현재 폴더)
folder_path = '.' 

# NEW: [AVWAP 생태계 12대 코어 파일 배열 하드코딩] 전체 순회 중단을 위한 타겟팅 배열 선언
target_files = [
    'main.py',
    'config.py',
    'broker.py',
    'vwap_data.py',
    'volatility_engine.py',
    'scheduler_sniper.py',
    'strategy.py',
    'strategy_v_avwap.py',
    'telegram_states.py',
    'telegram_avwap_console.py',
    'telegram_bot.py',
    'telegram_view.py'
]

with open(output_filename, 'w', encoding='utf-8') as outfile:
    # MODIFIED: [반복문 제어 변경] os.listdir 기반 무지성 스캔을 폐기하고 target_files 배열 기반 정밀 추출로 디커플링
    for filename in target_files:
        file_path = os.path.join(folder_path, filename)
        
        # NEW: [결측치(FileNotFound) 방어막] 폴더 내 파일 누락 시 런타임 에러 붕괴 방지 및 상태 기록
        if os.path.exists(file_path):
            outfile.write(f"\n{'='*50}\n")
            outfile.write(f"FILE: {filename}\n")
            outfile.write(f"{'='*50}\n\n")
            
            with open(file_path, 'r', encoding='utf-8') as infile:
                outfile.write(infile.read())
                outfile.write("\n")
        else:
            outfile.write(f"\n{'='*50}\n")
            outfile.write(f"FILE: {filename} (🚨 NOT FOUND)\n")
            outfile.write(f"{'='*50}\n\n")

print(f"성공! '{output_filename}' 파일에 AVWAP 코어 생태계 병합이 완료되었습니다.")
