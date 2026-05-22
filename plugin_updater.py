# ==========================================================
# [plugin_updater.py]
# ⚠️ 자가 업데이트 및 GCP 데몬 제어 전용 플러그인
# 🚨 MODIFIED: [V44.53 제1헌법 및 16계명 절대 락온] 달력 API(mcal) 스캔을 비동기(to_thread) 래핑
# 🚨 MODIFIED: [V75.05 레드존 팩트 교정] 제9경고에 따라 불필요한 레드존을 진공 압축하여 15:12 ~ 15:31 EST 구간으로 정밀 락온 완료.
# 🚨 MODIFIED: [Case 14 절대 헌법 준수] 달력 API 타임아웃 5.0초를 10.0초로 팩트 교정하여 타임아웃 헌법 일원화.
# ==========================================================
import logging
import asyncio
import subprocess
import os
import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

class SystemUpdater:
    def __init__(self):
        self.remote_branch = "origin/main"
        
        load_dotenv()
        # 🚨 [AI 에이전트 절대 주의] systemd 데몬 이름은 OS의 .service 파일 내 Environment 속성 다이렉트 주입
        self.daemon_name = os.getenv("daemon_name") or os.getenv("DAEMON_NAME", "mybot")

    # 🚨 [제1헌법 준수] 동기 I/O 차단을 위해 async 격상
    async def is_update_allowed(self):
        """
        현재 시간이 업데이트 금지 시간대(레드존)인지 검사합니다.
        기준: 15:12 EST ~ 15:31 EST (VWAP 가동 및 장마감 정산 보호)
        """
        est = ZoneInfo('America/New_York')
        now_est = datetime.datetime.now(est)
        
        if now_est.weekday() >= 5:
            return True, ""

        def _check_holiday():
            import pandas_market_calendars as mcal
            nyse = mcal.get_calendar('NYSE')
            schedule = nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
            return schedule.empty

        try:
            # MODIFIED: [Case 14 절대 헌법 준수] 타임아웃 10.0초로 팩트 교정
            is_holiday = await asyncio.wait_for(asyncio.to_thread(_check_holiday), timeout=10.0)
            if is_holiday:
                return True, ""
        except asyncio.TimeoutError:
            logging.error("⚠️ [Updater] 달력 API 타임아웃. 휴장일 판별을 건너뛰고 시간 검사 강제 진행 (Fail-Open).")
        except Exception as e:
            logging.debug(f"업데이트 락다운 달력 스캔 에러 (무시하고 시간 검사 진행): {e}")

        curr_time = now_est.time()
        
        start_lock = datetime.time(15, 12)
        end_lock = datetime.time(15, 31)
        
        if start_lock <= curr_time <= end_lock:
            return False, "⚠️ <b>[배포 금지]</b> 지금은 암살자 덤핑 및 본진 덫 장전 디커플링 핵심 윈도우입니다. (15:12~15:31 EST 업데이트 강제 차단)"
        return True, ""

    async def _create_safety_backup(self):
        try:
            backup_dir = "stable_backup"
            os.makedirs(backup_dir, exist_ok=True)
            
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
        allowed, msg = await self.is_update_allowed()
        if not allowed:
            logging.warning(f"🛑 [Updater] 깃허브 강제 동기화 차단 (레드존): {msg}")
            return False, msg

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

    async def restart_daemon(self):
        allowed, _ = await self.is_update_allowed()
        if not allowed:
            logging.error("❌ 레드존 시간대 데몬 재가동 시도가 감지되어 OS 강제 차단했습니다.")
            return False

        try:
            logging.info(f"🔄 [Updater] 좀비 셧다운 방어를 위해 파이썬 프로세스를 즉시 자폭(Hard Kill)시킵니다. (systemd가 부활시킴)")
            os._exit(0)
            return True
        except Exception as e:
            logging.error(f"🚨 [Updater] 데몬 자폭 명령 하달 실패: {e}")
            return False
