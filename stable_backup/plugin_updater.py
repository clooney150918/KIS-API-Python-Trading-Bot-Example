# ==========================================================
# [plugin_updater.py]
# ⚠️ 자가 업데이트 및 GCP 데몬 제어 전용 플러그인
# 🚨 MODIFIED: [V44.53 제1헌법 및 16계명 절대 락온] 달력 API(mcal) 스캔을 비동기(to_thread) 래핑
# 🚨 MODIFIED: [V75.05 레드존 팩트 교정] 제9경고에 따라 불필요한 레드존을 진공 압축하여 15:12 ~ 15:31 EST 구간으로 정밀 락온 완료.
# 🚨 MODIFIED: [Case 14 절대 헌법 준수] 달력 API 타임아웃 5.0초를 10.0초로 팩트 교정하여 타임아웃 헌법 일원화.
# 🚨 MODIFIED: [제1헌법 철저 준수] 낡은 time.sleep(0.06) 땜질 코드를 영구 소각하고 GlobalThrottle.wait_api_sync() 중앙 통제소 락온 결속.
# 🚨 MODIFIED: [제1헌법 교정] 서브프로세스 교착 방어를 위한 30초 타임아웃 족쇄 및 os.makedirs 비동기 래핑 전면 결속
# 🚨 NEW: [좀비 프로세스 방어] 서브프로세스 TimeoutError 발생 시 .kill() 직후 await .wait()를 강제 호출하여 OS 자원 누수(Zombie) 원천 차단
# 🚨 MODIFIED: [Indentation 붕괴 수술] 시스템 전역에 유입된 1~2칸 스페이스 오차 12개소 정밀 타격 교정 (컴파일 즉사 에러 100% 소각)
# ==========================================================
import logging
import asyncio
import subprocess
import os
import time
import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from global_throttle import GlobalThrottle # 🚨 NEW: 중앙 통제소 결속

class SystemUpdater:
    def __init__(self):
        self.remote_branch = "origin/main"
        
        load_dotenv()
        self.daemon_name = os.getenv("daemon_name") or os.getenv("DAEMON_NAME", "mybot")

    async def is_update_allowed(self):
        est = ZoneInfo('America/New_York')
        now_est = datetime.datetime.now(est)
        
        if now_est.weekday() >= 5:
            return True, ""

        def _check_holiday():
            # 🚨 MODIFIED: [제1헌법] 파편화된 sleep 땜질 소각 및 중앙 통제소 API 락온 강제
            GlobalThrottle.wait_api_sync()
            import pandas_market_calendars as mcal
            nyse = mcal.get_calendar('NYSE')
            schedule = nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
            return schedule.empty

        is_holiday = False
        # 🚨 NEW: [Case 33] 3단 지수 백오프 이식
        for attempt in range(3):
            try:
                is_holiday = await asyncio.wait_for(asyncio.to_thread(_check_holiday), timeout=10.0)
                break
            except asyncio.TimeoutError:
                # 🚨 MODIFIED: [Indentation 붕괴 수술] 17칸 -> 16칸 교정
                if attempt == 2:
                    logging.error("⚠️ [Updater] 달력 API 타임아웃. Fail-Open 평일 강제 검사 진행.")
                else:
                    await asyncio.sleep(1.0 * (2 ** attempt))
            except Exception as e:
                if attempt == 2:
                    logging.debug(f"업데이트 락다운 달력 스캔 에러 (무시하고 시간 검사 진행): {e}")
                else:
                    await asyncio.sleep(1.0 * (2 ** attempt))

        if is_holiday:
            return True, ""

        curr_time = now_est.time()
        
        # 🚨 MODIFIED: [Case 09] 레드존 락온
        start_lock = datetime.time(15, 12)
        end_lock = datetime.time(15, 31)
        
        if start_lock <= curr_time <= end_lock:
            return False, "⚠️ <b>[배포 금지]</b> 지금은 암살자 덤핑 및 본진 덫 장전 디커플링 핵심 윈도우입니다. (15:12~15:31 EST 업데이트 강제 차단)"
        return True, ""

    async def _create_safety_backup(self):
        try:
            backup_dir = "stable_backup"
            # 🚨 MODIFIED: [제1헌법] os.makedirs 비동기 격리 (이벤트 루프 차단 방어)
            await asyncio.to_thread(os.makedirs, backup_dir, exist_ok=True)
            
            # 🚨 MODIFIED: [Indentation 붕괴 수술] 13칸 -> 12칸 교정
            proc = await asyncio.create_subprocess_shell(
                f"cp -p *.py {backup_dir}/ 2>/dev/null || true",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            # 🚨 MODIFIED: [제1헌법 및 제5헌법] 서브프로세스 통신 시 30초 타임아웃(wait_for) 족쇄 체결
            # 🚨 MODIFIED: [Indentation 붕괴 수술] 13칸 -> 12칸 교정
            try:
                await asyncio.wait_for(proc.communicate(), timeout=30.0)
                logging.info("🛡️ [Updater] 롤백 봇을 위한 안전띠(stable_backup) 결속 완료")
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait() # 🚨 NEW: 좀비 프로세스 누수 방어
                logging.error("🚨 [Updater] 안전띠 결속 서브프로세스 통신 타임아웃 (30초 초과). 백업을 건너뜁니다.")
        except Exception as e:
            logging.error(f"🚨 [Updater] 안전띠 결속 중 에러 발생 (업데이트는 계속 진행): {e}")

    async def pull_latest_code(self):
        allowed, msg = await self.is_update_allowed()
        if not allowed:
            # 🚨 MODIFIED: [Indentation 붕괴 수술] 13칸 -> 12칸 교정
            logging.warning(f"🛑 [Updater] 깃허브 강제 동기화 차단 (레드존): {msg}")
            return False, msg

        await self._create_safety_backup()

        try:
            fetch_proc = await asyncio.create_subprocess_shell(
                "git fetch --all",
                stdout=subprocess.PIPE,
                # 🚨 MODIFIED: [Indentation 붕괴 수술] 17칸 -> 16칸 교정
                stderr=subprocess.PIPE
            )
            # 🚨 MODIFIED: [제1헌법 준수] 서브프로세스 30초 타임아웃 족쇄 체결
            try:
                _, fetch_err = await asyncio.wait_for(fetch_proc.communicate(), timeout=30.0)
            except asyncio.TimeoutError:
                fetch_proc.kill()
                # 🚨 MODIFIED: [Indentation 붕괴 수술] 17칸 -> 16칸 교정
                await fetch_proc.wait() # 🚨 NEW: 좀비 프로세스 누수 방어
                return False, "Git Fetch 통신 지연 타임아웃 (30초 초과)"
            
            if fetch_proc.returncode != 0:
                error_msg = fetch_err.decode('utf-8').strip()
                # 🚨 MODIFIED: [Indentation 붕괴 수술] 17칸 -> 16칸 교정
                logging.error(f"🚨 [Updater] Git Fetch 실패: {error_msg}")
                return False, f"Git Fetch 실패: {error_msg} (서버에서 git init 및 remote add 명령을 선행하십시오)"

            reset_proc = await asyncio.create_subprocess_shell(
                f"git reset --hard {self.remote_branch}",
                stdout=subprocess.PIPE,
                # 🚨 MODIFIED: [Indentation 붕괴 수술] 17칸 -> 16칸 교정
                stderr=subprocess.PIPE
            )
            # 🚨 MODIFIED: [제1헌법 준수] 서브프로세스 30초 타임아웃 족쇄 체결
            try:
                _, reset_err = await asyncio.wait_for(reset_proc.communicate(), timeout=30.0)
            except asyncio.TimeoutError:
                # 🚨 MODIFIED: [Indentation 붕괴 수술] 17칸 -> 16칸 교정
                reset_proc.kill()
                await reset_proc.wait() # 🚨 NEW: 좀비 프로세스 누수 방어
                return False, "Git Reset 통신 지연 타임아웃 (30초 초과)"
            
            if reset_proc.returncode != 0:
                error_msg = reset_err.decode('utf-8').strip()
                # 🚨 MODIFIED: [Indentation 붕괴 수술] 17칸 -> 16칸 교정
                logging.error(f"🚨 [Updater] Git Reset 실패: {error_msg}")
                return False, f"Git Reset 실패: {error_msg}"

            logging.info("✅ [Updater] 깃허브 최신 코드 강제 동기화 완료")
            return True, "깃허브 최신 코드가 로컬에 완벽히 동기화되었습니다."
            
        except Exception as e:
            # 🚨 MODIFIED: [Indentation 붕괴 수술] 13칸 -> 12칸 교정
            logging.error(f"🚨 [Updater] 동기화 중 치명적 예외 발생: {e}")
            return False, f"업데이트 프로세스 예외 발생: {e}"

    async def restart_daemon(self):
        allowed, _ = await self.is_update_allowed()
        if not allowed:
            logging.error("❌ 레드존 시간대 데몬 재가동 시도가 감지되어 OS 강제 차단했습니다.")
            return False

        # 🚨 MODIFIED: [Indentation 붕괴 수술] 9칸 -> 8칸 교정
        try:
            logging.info(f"🔄 [Updater] 좀비 셧다운 방어를 위해 파이썬 프로세스를 즉시 자폭(Hard Kill)시킵니다. (systemd가 부활시킴)")
            # 🚨 MODIFIED: [Case 15] 파이썬 하드 킬(os._exit(0)) 무중단 아키텍처 사수
            os._exit(0)
            return True
        except Exception as e:
            logging.error(f"🚨 [Updater] 데몬 자폭 명령 하달 실패: {e}")
            return False
