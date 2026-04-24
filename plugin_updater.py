# ==========================================================
# [plugin_updater.py]
# ⚠️ 자가 업데이트 및 GCP 데몬 제어 전용 플러그인
# 💡 깃허브 원격 저장소 강제 동기화 (git fetch & reset --hard)
# 💡 OS 레벨 데몬 재가동 제어 (sudo systemctl restart)
# 🚨 [V27.00 핫픽스] 사용자별 데몬 이름(DAEMON_NAME) .env 동적 로드 이식 완료
# 🛡️ [V27.05 추가] 업데이트 직전 stable_backup 폴더로 롤백용 안전띠 결속 기능 탑재
# 🚨 [V30.06 NEW] 장중 업데이트 레드존(Red-Zone) 원천 차단:
# VWAP 타임 슬라이싱 및 장마감 정산의 무결성을 위해 
# EST 14:55 ~ 16:10 사이의 업데이트 및 재가동을 100% 차단함.
# ==========================================================
import logging
import asyncio
import subprocess
import os
import datetime
import pytz
from dotenv import load_dotenv

class SystemUpdater:
    def __init__(self):
        self.remote_branch = "origin/main"
        
        load_dotenv()
        # 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막] systemd 데몬 이름(daemon_name)은 .env 파일이 아닌 OS의 .service 파일 내 Environment 속성에서 다이렉트로 주입받아야 한다. 인프라 샌드박스와 애플리케이션 설정의 혼용을 절대 금지한다.
        # MODIFIED: [환경변수 스캔 범위 확장] systemd에서 주입한 소문자 daemon_name 우선 조회 및 대문자 폴백 팩트 교정
        self.daemon_name = os.getenv("daemon_name") or os.getenv("DAEMON_NAME", "mybot")

    def is_update_allowed(self):
        """
        현재 시간이 업데이트 금지 시간대(레드존)인지 검사합니다.
        기준: 14:55 EST ~ 16:10 EST (VWAP 가동 및 장마감 정산 보호)
        """
        est = pytz.timezone('US/Eastern')
        now_est = datetime.datetime.now(est)
        curr_time = now_est.time()
        
        start_lock = datetime.time(14, 55)
        end_lock = datetime.time(16, 10)
        
        if start_lock <= curr_time <= end_lock:
            return False, "⚠️ <b>[배포 금지]</b> 지금은 VWAP 타격 및 장마감 정산 윈도우입니다. (14:55~16:10 EST 업데이트 강제 차단)"
        return True, ""

    async def _create_safety_backup(self):
        """
        [롤백 봇(Rescue) 전용 아키텍처]
        업데이트를 시도한다는 것 = 현재 코드가 정상 작동 중이라는 뜻이므로,
        새로운 코드를 받기 전에 현재 파이썬 파일들을 stable_backup 폴더에 피신시킵니다.
        """
        try:
            backup_dir = "stable_backup"
            os.makedirs(backup_dir, exist_ok=True)
            
            # 현재 폴더의 모든 .py 파일들을 stable_backup 폴더로 복사 (에러 무시)
            proc = await asyncio.create_subprocess_shell(
                f"cp -p *.py {backup_dir}/ 2>/dev/null || true",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            await proc.communicate()
            logging.info("🛡️ [Updater] 롤백 봇을 위한 안전띠(stable_backup) 결속 완료")
        except Exception as e:
            logging.error(f"🚨 [Updater] 안전띠 결속 중 에러 발생 (업데이트는 계속 진행): {e}")

    async def pull_latest_code(self):
        """
        깃허브 서버와 통신하여 로컬의 변경 사항을 완벽히 무시하고
        원격 저장소의 최신 코드로 강제 덮어쓰기(Hard Reset)를 수행합니다.
        """
        # 🚨 [V30.06] 업데이트 레드존(Red-Zone) 선제 검사
        allowed, msg = self.is_update_allowed()
        if not allowed:
            logging.warning(f"🛑 [Updater] 깃허브 강제 동기화 차단 (레드존): {msg}")
            return False, msg

        # 💡 [안전띠 결속] 깃허브 동기화 직전에 현재 상태를 백업합니다!
        await self._create_safety_backup()

        try:
            fetch_proc = await asyncio.create_subprocess_shell(
                "git fetch --all",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            _, fetch_err = await fetch_proc.communicate()
            
            if fetch_proc.returncode != 0:
                error_msg = fetch_err.decode('utf-8').strip()
                logging.error(f"🚨 [Updater] Git Fetch 실패: {error_msg}")
                return False, f"Git Fetch 실패: {error_msg} (서버에서 git init 및 remote add 명령을 선행하십시오)"

            reset_proc = await asyncio.create_subprocess_shell(
                f"git reset --hard {self.remote_branch}",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            _, reset_err = await reset_proc.communicate()
            
            if reset_proc.returncode != 0:
                error_msg = reset_err.decode('utf-8').strip()
                logging.error(f"🚨 [Updater] Git Reset 실패: {error_msg}")
                return False, f"Git Reset 실패: {error_msg}"

            logging.info("✅ [Updater] 깃허브 최신 코드 강제 동기화 완료")
            return True, "깃허브 최신 코드가 로컬에 완벽히 동기화되었습니다."
            
        except Exception as e:
            logging.error(f"🚨 [Updater] 동기화 중 치명적 예외 발생: {e}")
            return False, f"업데이트 프로세스 예외 발생: {e}"

    def restart_daemon(self):
        """
        GCP 리눅스 OS에 데몬 재가동 명령을 하달합니다.
        격발 즉시 봇 프로세스가 SIGTERM 신호를 받고 종료되므로,
        반드시 텔레그램 보고 메시지를 선행 발송한 후 호출해야 합니다.
        """
        # 🚨 [V30.06] 재가동 레드존(Red-Zone) Fail-Safe 방어막
        allowed, _ = self.is_update_allowed()
        if not allowed:
            logging.error("❌ 레드존 시간대 데몬 재가동 시도가 감지되어 OS 강제 차단했습니다.")
            return False

        try:
            logging.info(f"🔄 [Updater] OS 쉘에 {self.daemon_name} 데몬 재가동 명령을 하달합니다.")
            
            subprocess.Popen(
                ["sudo", "systemctl", "restart", self.daemon_name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            return True
        except Exception as e:
            logging.error(f"🚨 [Updater] 데몬 재가동 명령 하달 실패: {e}")
            return False
