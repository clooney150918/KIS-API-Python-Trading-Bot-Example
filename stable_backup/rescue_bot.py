# ==========================================================
# [rescue_bot.py]
# 🚑 무결점 롤백 구조대 (Rescue Bot) 
# 💡 단일 장애점(SPOF) 원천 차단 및 독립 텔레그램 롱폴링 프로세스
# 🛡️ [보안 수술 1] 텔레그램 토큰 하드코딩 전면 철거 (.env 동적 로드 이식)
# 🛡️ [보안 수술 2] 관리자(ADMIN) CHAT_ID 화이트리스트 접근 통제 락온 완료
# ==========================================================
import os
import time
import requests
import subprocess
from dotenv import load_dotenv

# 1. 비밀 금고(.env) 오픈 및 환경변수 로드
load_dotenv()

# 2. 메인 봇 이름, 구조대 토큰, 그리고 관리자 CHAT_ID 로드
daemon_name = os.getenv("daemon_name", "mybot") 
RESCUE_BOT_TOKEN = os.getenv("RESCUE_BOT_TOKEN")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")

# 🚨 필수 보안 설정 팩트 체크
if not RESCUE_BOT_TOKEN or not ADMIN_CHAT_ID:
    print("🚨 [치명적 에러] .env 파일에 RESCUE_BOT_TOKEN 또는 ADMIN_CHAT_ID가 누락되었습니다!")
    exit(1)

# 메인 봇의 작업 경로 추론 (사용자 환경에 맞게 수정 필요)
MAIN_BOT_DIR = "/home/pipios4006" 
BACKUP_DIR = f"{MAIN_BOT_DIR}/stable_backup"

def send_message(text):
    # 💡 무조건 등록된 관리자(장군님)에게만 메시지 발송
    url = f"https://api.telegram.org/bot{RESCUE_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": ADMIN_CHAT_ID, "text": text})
    except Exception as e:
        print(f"🚨 [통신 에러] 텔레그램 메시지 전송 실패: {e}")

def execute_rollback():
    send_message(f"🚑 [구조대 출동] {daemon_name} 메인 봇의 심폐소생술(Rollback)을 시작합니다...")
    
    # 1. 백업 폴더 존재 여부 팩트 체크
    if not os.path.exists(BACKUP_DIR):
        send_message("🚨 [실패] stable_backup 폴더를 찾을 수 없습니다! 백업된 파일이 없습니다.")
        return

    try:
        # 2. 백업된 파이썬 파일들을 메인 폴더로 강제 복구 (덮어쓰기)
        subprocess.run(f"cp -p {BACKUP_DIR}/*.py {MAIN_BOT_DIR}/", shell=True, check=True)
        send_message("✅ [1/2] 안정화된 과거 코드(stable_backup)로 덮어쓰기 복구 완료!")
        
        # 3. 메인 데몬 강제 재시작 (심폐소생술)
        subprocess.run(["sudo", "systemctl", "restart", daemon_name], check=True)
        send_message(f"✅ [2/2] {daemon_name} 데몬 심장 제세동(Restart) 완료! 메인 봇이 정상적으로 부활했습니다. 🎉")
        
    except Exception as e:
        send_message(f"🚨 [치명적 에러] 롤백 과정 중 예외가 발생했습니다: {e}")

def main():
    print(f"🚑 {daemon_name}_rescue 구조대 봇 가동 시작... (보안 모드 ON)")
    offset = None
    url = f"https://api.telegram.org/bot{RESCUE_BOT_TOKEN}/getUpdates"
    
    # 절대 죽지 않는 무한 루프 (Long Polling)
    while True:
        try:
            params = {"timeout": 30, "offset": offset}
            response = requests.get(url, params=params, timeout=40).json()
            
            if response.get("ok"):
                for result in response.get("result", []):
                    offset = result["update_id"] + 1
                    message = result.get("message", {})
                    
                    # 수신된 메시지의 chat_id를 문자열로 추출
                    incoming_chat_id = str(message.get("chat", {}).get("id", ""))
                    text = message.get("text", "")
                    
                    # 🛡️ [철통 보안 락온] 등록된 장군님의 ID가 아니면 가차 없이 씹어버립니다!
                    if incoming_chat_id != str(ADMIN_CHAT_ID):
                        continue
                    
                    if text == "/rescue" or text == "/rollback":
                        execute_rollback()
                    elif text == "/start":
                        send_message(f"🚑 PIPIOS 롤백 구조대 대기 중입니다.\n메인 봇({daemon_name})이 뻗었을 때 /rescue 명령어를 하달하십시오.")
                        
        except Exception as e:
            time.sleep(5) # 네트워크 에러 시 5초 대기 후 불사조처럼 재시도

if __name__ == "__main__":
    main()
