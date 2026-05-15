# ==========================================================
# FILE: telegram_bot.py
# ==========================================================
# 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
# 제1헌법: queue_ledger.get_queue 등 모든 파일 I/O 및 락 점유 메서드는 무조건 asyncio.to_thread로 래핑하여 이벤트 루프 교착(Deadlock)을 원천 차단함.
# MODIFIED: [V44.47 이벤트 루프 데드락 영구 소각] 다이렉트 파일 I/O 및 config/ledger 접근 메서 전면 비동기 래핑 완료.
# MODIFIED: [V44.48 수동 조작 데드코드 영구 소각 및 런타임 무결성 확보] 큐 장부에 존재하지 않는 _load 메서 호출 찌꺼기 100% 소각.
# MODIFIED: [V54.04 런타임 붕괴(Split-Brain) 근본 원인 팩트 수술]
# 삼위일체 소각(/reset) 시 V_REV 모드라면 is_active를 False로 끄지 않고 True로 보존하여 '0주 새출발' 상태를 100% 팩트 락온.
# 모드 스위칭(SET_VER_CONFIRM) 시에도 version과 is_active 플래그가 완벽히 동기화되도록 디커플링 배선 정밀 교정 완료.
# MODIFIED: [V55.00 오퍼레이션 SSOT - 텔레그램 다이렉트 I/O 병목 및 동시성 오염 원천 차단]
# DEL_Q(삭제), CLEAR_Q(초기화), RESET(삼위일체 소각) 격발 시 존재하던 지저분한 다이렉트 파일 I/O(open, json, tempfile) 찌꺼기를 100% 영구 소각하고,
# QueueLedger의 스레드 세이프(Thread-safe) 코어 메서드(delete_lot, clear_queue)로 직결(Lock-on) 완료.
# MODIFIED: [V56.00 차세대 AVWAP 실전 암살자 전면 재가동 락온]
# - Phantom Radar 암살자 제어 메뉴 영구 봉인 락다운 전면 해체.
# - MODE 라우터 내 AVWAP_WARN, AVWAP_ON, AVWAP_OFF 팩트 제어 로직 100% 복구 완료.
# MODIFIED: [V59.02 잔재 데드코드 영구 소각] 
# 15:25 전량 덤핑 헌법에 따라 의미를 상실한 AVWAP_SET 라우터 내 TARGET_MANUAL, TARGET_AUTO, EARLY, MULTI 제어 콜백 파이프라인(데드코드)을 전면 철거하고 REFRESH 기능만 보존 완료.
# MODIFIED: [V59.03 관제탑 진입 배선 복구] 
# settlement 메뉴에서 '관제탑' 버튼 클릭 시 cmd_avwap을 정상 호출하도록 AVWAP:MENU 라우팅 배선 복구 완료.
# NEW: [V59.06] VWAP 런타임 엑스레이(Dry-Run) 진단 엔진 라우터 이식 완료 (순수 Read-Only 섀도 연산)
# MODIFIED: [V60.00 옴니 매트릭스 락다운 데드코드 전면 폐기]
# XRAY 진단 엔진 내부에서 매수 방아쇠를 강제로 잠그던 옴니 매트릭스 스캔 블록 및 시각적 브리핑 요소를 영구 소각함.
# MODIFIED: [V61.00 숏(SOXS) 전면 작전 지시서 적용]
# 1) SET_VER 및 SET_VER_CONFIRM 콜백 내 SOXS 락다운 방어막 텍스트를 시스템 영구 폐기 경고로 오버라이드 완료.
# 2) TICKER 액션 내 SOXS 경고문 교정 및 '듀얼 모멘텀' 텍스트를 '싱글 모멘텀'으로 팩트 교정 완료.
# NEW: [AVWAP 수동 개입 엣지 케이스 방어] 수동 매도 후 유령 물량을 0주로 강제 동기화하는 SYNC_ZERO 라우터 신설
# MODIFIED: [V61.06 런타임 붕괴 방어] MODE 및 INPUT 라우터 내 IndentationError(들여쓰기) 팩트 완벽 교정
# MODIFIED: [V66.07 오퍼레이션 SSOT - 엑스레이 환각 소각 및 VWAP 최초 명중 타전망 이식]
# 엑스레이 진단 시 무조건 낡은 인메모리 상태를 강제 폐기(None)하고 최신 JSON 팩트 파일을 로드하도록 배선 교정 완료.
# NEW: [KIS VWAP 알고리즘 대통합 수술] 수동 VWAP 설정(AUTO/MANUAL) 텔레그램 콜백 라우팅을 전면 소각하고 단일 KIS VWAP 예약 장전 모드로 팩트 락온 완료.
# MODIFIED: [런타임 즉사 방어] SYNC_ZERO 콜백 라우터 내 IndentationError 팩트 무결점 4배수 교정 완료.
# MODIFIED: [V71.02 XRAY 엔진 라우팅 영구 소각]
# KIS 자체 VWAP 알고리즘 위임에 따라 1분 단위 시뮬레이션의 의미가 상실된 런타임 엑스레이(Dry-Run) 진단 콜백 라우터를 전면 적출 완료.
# MODIFIED: [V71.14 지정가 VWAP 일반주문 역배선 팩트 락온]
# MODIFIED: [V71.15 V-REV 수동 격발 렌더링 증발(Silent Skip) 버그 수술]
# MODIFIED: [V71.24 일반주문 VWAP 팩트 롤백 대수술]
# - KST 기반 지연 격발 엔진, 1시간 단위 타임 시프트(Time-Shift), 타임존 변환 데드코드를 전면 소각 완료.
# - 코어 엔진이 지시서에 주입해준 EST 시간(152500, 155500)을 일반주문망으로 즉시 직결(Direct Pass)하는 팩트 락온 진공 압축.
# MODIFIED: [V71.26 KST 타임라인 동적 래핑 수술]
# - 수동 주문(EXEC) 라우터 내에 폴백(Fallback)으로 방치되어 있던 '152500' 등 EST 하드코딩 찌꺼기를 100% 영구 소각.
# - 퀀트 엔진이 서머타임을 판독하여 주입한 KST 팩트 시간만을 다이렉트 패스하도록 무결점 역배선 개통 완료.
# NEW: [V71.28 수동 주문(EXEC) 최신 예산 팩트 스냅샷 강제 갱신 엔진 탑재]
# MODIFIED: [V71.29 수동 주문 예산 기아(Data Starvation) 맹점 수술]
# - EXEC 격발 시 텔레그램 내부의 낡은 예산 할당 함수(_calculate_budget_allocation)가 V-REV 예산을 $0.0으로 
#   강제 오판하여 매수 지시서가 공중 증발하던 치명적 하극상 맹점 원천 차단.
# - 코어 엔진(scheduler_core)의 get_budget_allocation으로 다이렉트 배선을 교체하여 매수 타점 100% 장전 락온.
# MODIFIED: [V72.01 V-REV 수동 주문(EXEC) 시각적 디커플링 해체]
# - 수동 주문 실행 시 V-REV 모드임에도 V14 고유의 '💎' 아이콘이 하드코딩되어 
#   표출되던 시각적 환각(UI 디커플링) 현상을 모드별 맞춤 아이콘('⚖️' / '💎')으로 100% 팩트 교정 완료.
# MODIFIED: [V72.15 settlement 콜백 라우팅 증발 맹점 영구 복원]
# - V59/V61 대수술 중 누락되었던 SET_VER, SET_VER_CONFIRM, AVWAP 라우터를 100% 팩트 복구.
# - 0주 상태에서만 코어 스위칭이 가능하도록 0주 락온(Lock-on) 방어막 완벽 이식.
# 🚨 NEW: [V72.16 AVWAP 정점요격 스위치 및 유실된 라우터 전면 복구]
# - 과거 대수술 시 통째로 유실되었던 MODE 액션 라우터와 AVWAP_SET 라우터 100% 원상 복구 완료.
# - APEX_ON / APEX_OFF 분기망 신설하여 텔레그램 수신 즉시 비동기 래핑으로 config 상태 팩트 제어.
# - 제자리 메뉴 새로고침(cmd_settlement) 배선 개통으로 시각적 디커플링 원천 차단.
# 🚨 NEW: [V73.00 UI 렌더링 디커플링 해체]
# - 텔레그램 시작 화면 및 통합 지시서에 잔존하는 17:05 KST 예약 장전 레거시 텍스트를 15:26 EST 지연 장전으로 팩트 교정하여 시각적 환각을 100퍼센트 해체합니다. (telegram_view 연동)
# - 수동 주문(EXEC) 시 생성되는 스냅샷 기반 덫 장전 프로세스 무결점 유지.
# 🚨 NEW: [통합 지시서 수동 매매 취소 버튼 탑재 및 KIS 다이렉트 팩트 취소 라우팅 개통]
# - CANCEL_EXEC 콜백 라우터를 신설하여 수동 매매 취소 기능을 개통. 
# - KIS 예약 원장과 일반 미체결 원장을 비동기로 이중 스캔하고 팩트로 파기하여 제1헌법, 제19경고를 100% 완벽하게 준수.
# 🚨 MODIFIED: [통합 지시서 수동 제어(EXEC/CANCEL) 완벽 스위칭 작전]
# - CANCEL_EXEC 덫 파기 완료 시(nuked_count > 0), 당일 매매 잠금(REG Lock)을 강제로 해제하도록
#   cfg.reset_lock_for_ticker를 비동기로 호출하는 무결성 락온 파이프라인 개통 완료.
# 🚨 MODIFIED: [V75.02 원격 로그 추출 엔진 팩트 교정 및 데이터 증발 수술]
# - cmd_log 내 Traceback 데이터 증발 방어를 위한 꼬리 캡처(_grep_tail_logs) 무결성 락온
# 🚨 MODIFIED: [V75.03 관찰자 효과 및 시각적 환각 원천 수술]
# - get_decision 비동기 래핑 및 is_simulation=True 강제 주입 (제1헌법 준수 및 런타임 오염 차단)
# - 낡은 10시/15시 텍스트 소각 및 09:30~09:34 캔들 대기 / 지터 덤핑 타임라인 팩트 교정
# 🚨 MODIFIED: [V75.06 런타임 즉사 방어] 들여쓰기(IndentationError) 팩트 완벽 교정
# - cmd_sync 내 AVWAP 레이더 스캔 블록의 찌그러진 들여쓰기를 전면 교정하여 런타임 크래시 영구 소각
# 🚨 MODIFIED: [V75.08 관제탑 새로고침 시각적 깜빡임(Flickering) 영구 소각]
# - 사용자의 지시에 따라, 새로고침 시 메시지가 로딩 텍스트로 줄어들었다가 다시 팽창하는 깜빡임 현상을 원천 차단.
# - 중간 렌더링 과정을 생략하고 최신 레이더 텍스트만 제자리에 1회 덮어쓰기(Edit)하는 다이렉트 락온 적용 완료.
# 🚨 MODIFIED: [V75.05 제20경고 절대 헌법 준수: V-REV 매수 타점 1층 평단가 앵커 락온 및 타점 배수 팩트 교정]
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import time
import os
import math 
import asyncio
import html
import json
import tempfile
import yfinance as yf
import pandas_market_calendars as mcal 

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, CommandHandler, CallbackQueryHandler, MessageHandler, filters

from telegram_view import TelegramView 
from telegram_sync_engine import TelegramSyncEngine
from telegram_states import TelegramStates
from telegram_callbacks import TelegramCallbacks

class TelegramController:
    def __init__(self, config, broker, strategy, tx_lock=None, queue_ledger=None, strategy_rev=None):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.view = TelegramView()
        self.user_states = {} 
        self.admin_id = self.cfg.get_chat_id()
        self.sync_locks = {} 
        self.tx_lock = tx_lock or asyncio.Lock()
        
        self.queue_ledger = queue_ledger
        self.strategy_rev = strategy_rev 

        self.sync_engine = TelegramSyncEngine(self.cfg, self.broker, self.strategy, self.queue_ledger, self.view, self.tx_lock, self.sync_locks)
        self.states_handler = TelegramStates(self.cfg, self.broker, self.queue_ledger, self.sync_engine)
        self.callbacks_handler = TelegramCallbacks(self.cfg, self.broker, self.strategy, self.queue_ledger, self.sync_engine, self.view, self.tx_lock)

    def _is_admin(self, update: Update):
        if self.admin_id is None:
            self.admin_id = self.cfg.get_chat_id()
             
        if self.admin_id is None:
            print("⚠️ 보안 경고: ADMIN_CHAT_ID가 설정되지 않아 알 수 없는 사용자의 접근을 차단했습니다.")
            return False
            
        return update.effective_chat.id == int(self.admin_id)

    def _get_dst_info(self):
        est = ZoneInfo('America/New_York')
        now_est = datetime.datetime.now(est)
        is_dst = now_est.dst() != datetime.timedelta(0)
         
        if is_dst:
            return (17, "🌞 <b>서머타임 적용 (Summer)</b>")
        else:
            return (18, "❄️ <b>서머타임 해제 (Winter)</b>")

    async def _get_market_status(self):
        est = ZoneInfo('America/New_York')
        now = datetime.datetime.now(est)
         
        def _fetch_schedule():
            nyse = mcal.get_calendar('NYSE')
            return nyse.schedule(start_date=now.date(), end_date=now.date())

        try:
            schedule = await asyncio.wait_for(asyncio.to_thread(_fetch_schedule), timeout=10.0)
        except Exception as e:
            logging.error(f"⚠️ [달력 API 에러/타임아웃] 평일 강제 개장(Fail-Open) 폴백 가동: {e}")
            if now.weekday() < 5:
                return "REG", "🔥 정규장 (Fail-Open)"
            else:
                return "CLOSE", "⛔ 장마감 (Fail-Closed)"
         
        if schedule.empty:
            return "CLOSE", "⛔ 장휴일"
        
        market_open = schedule.iloc[0]['market_open'].astimezone(est)
        market_close = schedule.iloc[0]['market_close'].astimezone(est)
        pre_start = market_open.replace(hour=4, minute=0)
        after_end = market_close.replace(hour=20, minute=0)

        if pre_start <= now < market_open:
            return "PRE", "🌅 프리마켓"
        elif market_open <= now < market_close:
            return "REG", "🔥 정규장"
        elif market_close <= now < after_end:
            return "AFTER", "🌙 애프터마켓"
        else:
            return "CLOSE", "⛔ 장마감"

    def _calculate_budget_allocation(self, cash, tickers):
        sorted_tickers = sorted(tickers, key=lambda x: 0 if x == "SOXL" else (1 if x == "TQQQ" else 2))
        allocated = {}
        rem_cash = cash
     
        for tx in sorted_tickers:
            rev_state = self.cfg.get_reverse_state(tx)
            is_rev = rev_state.get("is_active", False)
            
            if is_rev:
                allocated[tx] = 0.0 
            else:
                split = self.cfg.get_split_count(tx)
                portion = self.cfg.get_seed(tx) / split if split > 0 else 0
                if rem_cash >= portion:
                    allocated[tx] = portion
                    rem_cash -= portion
                else: 
                    allocated[tx] = 0
             
        return sorted_tickers, allocated

    def setup_handlers(self, application):
        application.add_handler(CommandHandler("start", self.cmd_start))
        application.add_handler(CommandHandler("sync", self.cmd_sync))
        application.add_handler(CommandHandler("record", self.cmd_record))
        application.add_handler(CommandHandler("history", self.cmd_history))
        application.add_handler(CommandHandler("settlement", self.cmd_settlement))
        application.add_handler(CommandHandler("seed", self.cmd_seed))
        application.add_handler(CommandHandler("ticker", self.cmd_ticker))
        application.add_handler(CommandHandler("mode", self.cmd_mode))
        application.add_handler(CommandHandler("version", self.cmd_version))
    
        application.add_handler(CommandHandler("queue", self.cmd_queue))
        application.add_handler(CommandHandler("add_q", self.cmd_add_q))
        application.add_handler(CommandHandler("clear_q", self.cmd_clear_q))
        
        application.add_handler(CommandHandler("reset", self.cmd_reset))
        application.add_handler(CommandHandler("update", self.cmd_update))
    
        application.add_handler(CommandHandler("avwap", self.cmd_avwap))
        application.add_handler(CommandHandler("log", self.cmd_log))
        application.add_handler(CommandHandler("error", self.cmd_log))
        
        application.add_handler(CallbackQueryHandler(self.handle_callback))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.callbacks_handler.handle_callback(update, context, self)

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update):
            return
            
        text = update.message.text
        chat_id = update.effective_chat.id
        
        state = self.user_states.get(chat_id)
     
        if "장부 조회" in text:
            return await self.cmd_record(update, context)
        elif "시드 변경" in text:
            return await self.cmd_seed(update, context)
        elif "모드 전환" in text:
            return await self.cmd_ticker(update, context)
        elif "분할 변경" in text or "환경 설정" in text or "세팅" in text:
            return await self.cmd_settlement(update, context)
        elif "스나이퍼" in text:
            return await self.cmd_mode(update, context)
        elif "명예의 전당" in text or "졸업" in text:
             return await self.cmd_history(update, context)
        elif "암살자" in text or "조기" in text or "avwap" in text.lower():
            return await self.cmd_avwap(update, context)
        elif "로그" in text or "에러" in text:
            return await self.cmd_log(update, context)
            
        await self.states_handler.handle_message(update, context, self)

    async def cmd_avwap(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update): return
        
        loading_text = "⏳ <b>[AVWAP 듀얼 모멘텀 관제탑]</b>\n레이더망을 가동하여 시장 데이터를 스캔 중..."
        
        status_msg = None
        if update.callback_query:
            # 🚨 MODIFIED: [V75.08 시각적 깜빡임 영구 소각]
            # 새로고침(콜백 쿼리) 시 메시지가 로딩 텍스트로 축소되었다가 다시 확장되는 깜빡임을 막기 위해,
            # 중간 렌더링 과정을 생략하고 기존 메시지를 유지한 상태에서 팩트 데이터만 수집합니다.
            status_msg = update.callback_query.message
        else:
            # 신규 명령어(/avwap 등)로 진입 시에만 로딩 텍스트 표출
            status_msg = await update.message.reply_text(loading_text, parse_mode='HTML')
            
        try:
            from telegram_avwap_console import AvwapConsolePlugin
            plugin = AvwapConsolePlugin(self.cfg, self.broker, self.strategy, self.tx_lock)
            app_data = context.bot_data.get('app_data', {})
            if not app_data:
                try:
                    jobs = context.job_queue.jobs() if context.job_queue else []
                    if jobs and len(jobs) > 0 and jobs[0].data is not None: app_data = jobs[0].data
                except Exception: app_data = {}
 
            msg, markup = await asyncio.wait_for(plugin.get_console_message(app_data), timeout=10.0)
            
            try:
                # 🚨 MODIFIED: [V75.08 다이렉트 1회 덮어쓰기 락온]
                # 수집된 최신 팩트 데이터로 메시지를 단 한 번만 다이렉트 교체합니다.
                await status_msg.edit_text(msg, reply_markup=markup, parse_mode='HTML')
            except Exception as edit_e:
                # 시장 데이터가 1초 전과 완전히 동일하여 텍스트가 변하지 않았을 때 
                # 텔레그램 API가 뱉어내는 Message is not modified 에러를 무시(Bypass)합니다.
                if "Message is not modified" not in str(edit_e):
                    raise edit_e
                    
        except asyncio.TimeoutError:
            logging.error("🚨 AVWAP 관제탑 호출 타임아웃 (네트워크 지연)")
            await status_msg.edit_text("❌ <b>[네트워크 지연 발생]</b>\n야후 파이낸스 또는 증권사 서버 응답이 지연되어 스캔을 강제 종료했습니다. 잠시 후 다시 시도해 주세요.", parse_mode='HTML')
        except Exception as e:
            logging.error(f"🚨 AVWAP 관제탑 호출 내부 에러: {e}")
            await status_msg.edit_text(f"❌ <b>[시스템 에러]</b>\n독립 관제탑 호출 중 내부 오류가 발생했습니다:\n<code>{e}</code>", parse_mode='HTML')

    async def cmd_log(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update): return
        status_msg = await update.message.reply_text("🔍 <b>[원격 진단]</b> 최근 시스템 에러 로그를 핀셋 추출 중...", parse_mode='HTML')
        try:
            est = ZoneInfo('America/New_York')
            today_str = datetime.datetime.now(est).strftime('%Y%m%d')
            log_path = f"logs/bot_app_{today_str}.log"
            if not os.path.exists(log_path):
                return await status_msg.edit_text("📭 <b>[진단 결과]</b> 오늘자 로그 파일이 생성되지 않았습니다.", parse_mode='HTML')
                
            # MODIFIED: [V75.02 원격 로그 추출 엔진 팩트 교정 및 데이터 증발 수술]
            # 파이썬 Traceback의 멀티라인 구조가 훼손되지 않도록 파일의 끝(Tail)에서 100% 추출하여 반환
            def _grep_tail_logs(path, limit=50):
                with open(path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                tail_lines = lines[-limit:]
                return [line.strip() for line in reversed(tail_lines)]
                
            error_logs = await asyncio.to_thread(_grep_tail_logs, log_path)
            if not error_logs:
                return await status_msg.edit_text("✅ <b>[진단 결과]</b> 최근 감지된 시스템 결함이 없습니다. 무결점 순항 중!", parse_mode='HTML')
            report = self.view.format_log_report(error_logs)
            await status_msg.edit_text(report, parse_mode='HTML')
        except Exception as e:
            logging.error(f"🚨 원격 로그 추출 실패: {e}")
            await status_msg.edit_text(f"🚨 <b>[진단 실패]</b> 로그 추출 중 오류 발생:\n<code>{str(e)}</code>", parse_mode='HTML')

    async def cmd_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update): return
        from plugin_updater import SystemUpdater
        updater = SystemUpdater()
        allowed, fail_msg = await updater.is_update_allowed()
        if not allowed:
            return await update.message.reply_text(f"🛑 <b>[작전 중 업데이트 거부]</b>\n\n{fail_msg}", parse_mode='HTML')
        status_msg = await update.message.reply_text("⏳ <b>[시스템 업데이트]</b> 깃허브 원격 서버와 통신을 시작합니다...", parse_mode='HTML')
        try:
            success, msg = await updater.pull_latest_code()
            import html
            safe_msg = html.escape(msg)
            if success:
                await status_msg.edit_text(f"✅ <b>[동기화 완료]</b> {safe_msg}\n\n🔄 시스템 데몬(pipiosbot)을 OS 단에서 재가동합니다. 다운타임 후 봇이 다시 깨어납니다.", parse_mode='HTML')
                await updater.restart_daemon()
            else:
                await status_msg.edit_text(f"❌ <b>[동기화 실패]</b>\n▫️ 사유: {safe_msg}", parse_mode='HTML')
        except Exception as e:
            import html
            safe_err = html.escape(str(e))
            await status_msg.edit_text(f"🚨 <b>[치명적 오류]</b> 플러그인 호출 및 프로세스 예외 발생: {safe_err}", parse_mode='HTML')

    async def cmd_queue(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update): return
        args = context.args
        if not args: return await update.message.reply_text("❌ 종목명을 입력하세요. 예: /queue SOXL")
        ticker = args[0].upper()
        if not getattr(self, 'queue_ledger', None):
            from queue_ledger import QueueLedger
            self.queue_ledger = QueueLedger()
        q_data = await asyncio.to_thread(self.queue_ledger.get_queue, ticker)
        msg, reply_markup = self.view.get_queue_management_menu(ticker, q_data)
        await update.message.reply_text(text=msg, reply_markup=reply_markup, parse_mode='HTML')

    async def cmd_add_q(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update): return
        try:
            args = context.args
            if len(args) < 4:
                return await update.message.reply_text("❌ 정확한 양식: <code>/add_q SOXL 2026-04-06 20 52.16</code>", parse_mode='HTML')
            ticker = args[0].upper()
            date_str = args[1]
            try:
                qty = int(args[2])
                price = float(args[3])
            except ValueError: return await update.message.reply_text("❌ 수량은 정수, 평단가는 숫자로 입력하세요.")
            try:
                curr_p = await asyncio.wait_for(asyncio.to_thread(self.broker.get_current_price, ticker), timeout=3.0)
                if curr_p and curr_p > 0:
                    if price < curr_p * 0.7 or price > curr_p * 1.3:
                        return await update.message.reply_text(f"🚨 <b>오입력 차단:</b> 입력하신 평단가(<b>${price:.2f}</b>)가 현재가 대비 ±30%를 벗어납니다. 오타를 확인하세요!", parse_mode='HTML')
            except Exception: pass
            if not getattr(self, 'queue_ledger', None):
                from queue_ledger import QueueLedger
                self.queue_ledger = QueueLedger()
            q_data = await asyncio.to_thread(self.queue_ledger.get_queue, ticker)
            q_data.append({"qty": qty, "price": price, "date": f"{date_str} 23:59:59", "type": "MANUAL_OVERRIDE"})
            q_data.sort(key=lambda x: x.get('date', ''), reverse=True)
            await asyncio.to_thread(self.queue_ledger.overwrite_queue, ticker, q_data)
            chat_id = update.effective_chat.id
            if ticker not in self.sync_engine.sync_locks: self.sync_engine.sync_locks[ticker] = asyncio.Lock()
            if not self.sync_engine.sync_locks[ticker].locked(): await self.sync_engine.process_auto_sync(ticker, chat_id, context, silent_ledger=False)
            await update.message.reply_text(f"✅ <b>[{ticker}] 수동 지층 삽입 완료!</b>\n▫️ {date_str} | {qty}주 | ${price:.2f}", parse_mode='HTML')
        except Exception as e: await update.message.reply_text(f"❌ 알 수 없는 에러 발생: {e}")

    async def cmd_clear_q(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update): return
        args = context.args
        if not args: return await update.message.reply_text("❌ 종목명을 입력하세요. 예: /clear_q SOXL")
        ticker = args[0].upper()
        try:
            if not getattr(self, 'queue_ledger', None):
                from queue_ledger import QueueLedger
                self.queue_ledger = QueueLedger()
            await asyncio.to_thread(self.queue_ledger.clear_queue, ticker)
            chat_id = update.effective_chat.id
            if ticker not in self.sync_engine.sync_locks: self.sync_engine.sync_locks[ticker] = asyncio.Lock()
            if not self.sync_engine.sync_locks[ticker].locked(): await self.sync_engine.process_auto_sync(ticker, chat_id, context, silent_ledger=True)
            await update.message.reply_text(f"🗑️ <b>[{ticker}] 장부가 완전히 소각되었습니다.</b>\n새로운 지층을 구축할 준비가 완료되었습니다.", parse_mode='HTML')
        except Exception as e: await update.message.reply_text(f"❌ 소각 중 에러 발생: {e}")

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update): return
        target_hour, season_icon = self._get_dst_info()
        latest_version = await asyncio.to_thread(self.cfg.get_latest_version) 
        msg = self.view.get_start_message(target_hour, season_icon, latest_version) 
        await update.message.reply_text(msg, parse_mode='HTML')

    async def cmd_sync(self, update, context):
        if not self._is_admin(update):
            return
        
        await update.message.reply_text("🔄 시장 분석 및 지시서 작성 중...")
        
        async with self.tx_lock:
            cash, holdings = await asyncio.to_thread(self.broker.get_account_balance)
            
        if holdings is None:
            await update.message.reply_text("❌ KIS API 통신 오류로 계좌 정보를 불러올 수 없습니다. 잠시 후 다시 시도해주세요.")
            return

        target_hour, _ = self._get_dst_info() 
        dst_txt = "🌞 서머타임 (17:30)" if target_hour == 17 else "❄️ 겨울 (18:30)"
        status_code, status_text = await self._get_market_status()
        
        tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
        render_tickers = list(tickers)
        sorted_tickers, allocated_cash = await asyncio.to_thread(self._calculate_budget_allocation, cash, render_tickers)
        
        ticker_data_list = []
        total_buy_needed = 0.0

        app_data = context.bot_data.get('app_data', {})
        if not app_data:
            try:
                jobs = context.job_queue.jobs() if context.job_queue else []
                app_data = jobs[0].data if jobs and jobs[0].data is not None else {}
            except Exception:
                app_data = {}

        tracking_cache = app_data.setdefault('sniper_tracking', {})

        est = ZoneInfo('America/New_York')
        now_est = datetime.datetime.now(est)
        
        is_sniper_active_time = False
        try:
            def _check_schedule():
                nyse = mcal.get_calendar('NYSE')
                return nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
            schedule = await asyncio.wait_for(asyncio.to_thread(_check_schedule), timeout=10.0)
            if not schedule.empty:
                market_open = schedule.iloc[0]['market_open'].astimezone(est)
                switch_time = market_open + datetime.timedelta(minutes=30)
                if now_est >= switch_time:
                    is_sniper_active_time = True
        except Exception:
            if now_est.weekday() < 5 and now_est.time() >= datetime.time(10, 0):
                is_sniper_active_time = True

        for t in sorted_tickers:
            is_avwap_active = False
            avwap_budget = 0.0
            avwap_qty = 0
            avwap_avg = 0.0
            avwap_status_txt = "OFF"
            avwap_strikes = 0
            avwap_base_ticker = "N/A"
            avwap_base_price = 0.0
            avwap_base_vwap = 0.0
            avwap_prev_vwap = 0.0
            avwap_rolling_tp = 0.0
            avwap_gap_pct = 0.0

            h = holdings.get(t, {'qty':0, 'avg':0})
            curr = await asyncio.to_thread(self.broker.get_current_price, t, is_market_closed=(status_code == "CLOSE"))
            prev_close = await asyncio.to_thread(self.broker.get_previous_close, t)
            ma_5day = await asyncio.to_thread(self.broker.get_5day_ma, t)
            day_high, day_low = await asyncio.to_thread(self.broker.get_day_high_low, t)
            
            actual_avg = float(h['avg']) if h['avg'] else 0.0
            actual_qty = int(h['qty'])
            
            safe_prev_close = prev_close if prev_close else 0.0
            
            if status_code in ["AFTER", "CLOSE", "PRE"]:
                try:
                    def get_yf_close():
                        df = yf.Ticker(t).history(period="5d", interval="1d")
                        return float(df['Close'].iloc[-1]) if not df.empty else None
                    yf_close = await asyncio.wait_for(asyncio.to_thread(get_yf_close), timeout=3.0)
                    if yf_close and yf_close > 0:
                        safe_prev_close = yf_close
                except Exception as e:
                    logging.debug(f"YF 정규장 종가 롤오버 스캔 실패 ({t}): {e}")

            if status_code == "CLOSE":
                curr = safe_prev_close

            idx_ticker = "SOXX" if t == "SOXL" else "QQQ"
            dynamic_pct_obj = await asyncio.to_thread(self.broker.get_dynamic_sniper_target, idx_ticker)
            dynamic_pct = float(dynamic_pct_obj) if dynamic_pct_obj is not None else (8.79 if t == "SOXL" else 4.95)
            
            tracking_status = tracking_cache.get(t, {})
            current_day_high = tracking_status.get('day_high', day_high) 
            hybrid_target_price = current_day_high * (1 - (abs(dynamic_pct) / 100.0))
            trigger_reason = f"-{abs(dynamic_pct)}%"
            
            is_locked_reg = await asyncio.to_thread(self.cfg.check_lock, t, "REG")
            is_locked_sniper = await asyncio.to_thread(self.cfg.check_lock, t, "SNIPER")
            is_already_ordered = is_locked_reg or is_locked_sniper
             
            ver = await asyncio.to_thread(self.cfg.get_version, t)
            is_manual_vwap = await asyncio.to_thread(getattr(self.cfg, 'get_manual_vwap_mode', lambda x: False), t)
            
            # 🚨 MODIFIED: [V72.05 장마감 실시간 팩트 스캔 및 스냅샷 디커플링 해제]
            force_realtime = status_code in ["CLOSE", "AFTER"]
            
            cached_snap = None
            if not force_realtime:
                if ver == "V_REV":
                    cached_snap = await asyncio.to_thread(self.strategy.v_rev_plugin.load_daily_snapshot, t)
                elif ver == "V14":
                     if is_manual_vwap:
                        cached_snap = await asyncio.to_thread(self.strategy.v14_vwap_plugin.load_daily_snapshot, t)
                     else:
                        if hasattr(self.strategy, 'v14_plugin') and hasattr(self.strategy.v14_plugin, 'load_daily_snapshot'):
                            cached_snap = await asyncio.to_thread(self.strategy.v14_plugin.load_daily_snapshot, t)
            
            if dynamic_pct_obj and hasattr(dynamic_pct_obj, 'metric_val'):
                real_val = float(dynamic_pct_obj.metric_val)
            else:
                real_val = 0.0
            vol_status = "ON" if real_val >= 20.0 else "OFF"

            logic_qty = actual_qty
            is_zero_start_fact = (actual_qty == 0)
            if cached_snap:
                if actual_qty == 0:
                    logic_qty = 0
                    is_zero_start_fact = True
                else:
                    if "total_q" in cached_snap:
                         logic_qty = cached_snap["total_q"]
                    elif "initial_qty" in cached_snap:
                        logic_qty = cached_snap["initial_qty"]
                    is_zero_start_fact = cached_snap.get("is_zero_start", logic_qty == 0)

            try:
                 jobs = context.job_queue.jobs() if context.job_queue else []
                 job_data = jobs[0].data if jobs and jobs[0].data is not None else {}
                 regime_data = job_data.get('regime_data')
            except Exception:
                regime_data = None

            # 🚨 MODIFIED: [V72.05 장마감 실시간 팩트 스캔] force_realtime 플래그를 is_snapshot_mode에 강제 주입
            plan = await asyncio.to_thread(
                self.strategy.get_plan,
                t, curr, actual_avg, logic_qty, safe_prev_close, ma_5day=ma_5day,
                market_type="REG", available_cash=allocated_cash.get(t, 0.0),
                is_simulation=True, regime_data=regime_data,
                is_snapshot_mode=force_realtime
            )
             
            split = await asyncio.to_thread(self.cfg.get_split_count, t)
            safe_seed = await asyncio.to_thread(self.cfg.get_seed, t)
            
            t_val = plan.get('t_val', 0.0)
            is_rev = plan.get('is_reverse', False)
            
            v_rev_q_qty = 0
            v_rev_q_lots = 0
            v_rev_guidance = ""
            
            l1_qty = 0
            l1_price = 0.0

            if ver == "V_REV":
                if not getattr(self, 'queue_ledger', None):
                    from queue_ledger import QueueLedger
                    self.queue_ledger = QueueLedger()
               
                q_list = await asyncio.to_thread(self.queue_ledger.get_queue, t)
                v_rev_q_lots = len(q_list)
                v_rev_q_qty = sum(item.get('qty', 0) for item in q_list)
                
                if q_list:
                    l1_qty = int(float(q_list[-1].get('qty', 0)))
                    l1_price = float(q_list[-1].get('price', 0.0))

                one_portion_cash = safe_seed * 0.15
                plan['one_portion'] = one_portion_cash
                half_portion_cash = one_portion_cash * 0.5
            
                tag = "VWAP" if is_manual_vwap else "LOC"
                
                snap_sells_for_ui = [o for o in cached_snap.get("orders", []) if o.get('side') == 'SELL'] if cached_snap else []
                if cached_snap and snap_sells_for_ui and logic_qty > 0:
                     for o in snap_sells_for_ui:
                         desc_label = o.get('desc', '매도').split('(')[0]
                         v_rev_guidance += f" 🔵 {desc_label} ${o['price']:.2f} <b>{o['qty']}주</b> ({tag})\n"
                         
                elif q_list and logic_qty > 0:
                    trigger_l1 = round(l1_price * 1.006, 2)
                    
                    # 🚨 MODIFIED: [V72.13 UI 렌더링 팩트 교정 및 LIFO 독립 탈출 미러링]
                    valid_q_data = [item for item in q_list if float(item.get('price', 0.0)) > 0]
                    total_q = sum(int(float(item.get("qty", 0))) for item in valid_q_data)
                    total_inv = sum(float(item.get('qty', 0)) * float(item.get('price', 0.0)) for item in valid_q_data)
                    q_avg_price = (total_inv / total_q) if total_q > 0 else 0.0
                    
                    upper_qty = total_q - l1_qty
                    trigger_upper = round(q_avg_price * 1.010, 2) if upper_qty > 0 else 0.0
                    
                    available_l1 = min(l1_qty, logic_qty)
                    available_upper = min(upper_qty, logic_qty - available_l1)
                    
                    sell_dict = {}
                    if available_l1 > 0 and trigger_l1 > 0:
                         sell_dict[trigger_l1] = sell_dict.get(trigger_l1, 0) + available_l1
                    if available_upper > 0 and trigger_upper > 0:
                         sell_dict[trigger_upper] = sell_dict.get(trigger_upper, 0) + available_upper
                    
                    for price in sorted(sell_dict.keys()):
                        s_qty = sell_dict[price]
                        
                        if price == trigger_l1 and price == trigger_upper:
                            desc_str = "통합탈출"
                        elif price == trigger_l1:
                            desc_str = "1층탈출"
                        elif price == trigger_upper:
                             desc_str = "총평단탈출"
                        else:
                            desc_str = "잔여탈출"
                        v_rev_guidance += f" 🔵 {desc_str} ${price:.2f} <b>{s_qty}주</b> ({tag})\n"
                else:
                    v_rev_guidance += " 🔵 매도: 대기 물량 없음 (관망)\n"
               
                # 🚨 MODIFIED: [V75.05 제20경고 절대 헌법 준수: V-REV 매수 타점 1층 평단가 앵커 락온 및 타점 배수 팩트 교정]
                safe_anchor = l1_price if l1_price > 0.0 else safe_prev_close
                if safe_anchor > 0:
                    b1_price = round(safe_prev_close * 1.15 if is_zero_start_fact else safe_anchor * 0.9976, 2)
                    b2_price = round(safe_prev_close * 0.999 if is_zero_start_fact else safe_anchor * 0.9887, 2)
                    
                    b1_qty = math.floor(half_portion_cash / b1_price) if b1_price > 0 else 0
                    b2_qty = math.floor(half_portion_cash / b2_price) if b2_price > 0 else 0
                    
                    if b1_qty > 0:
                         v_rev_guidance += f" 🔴 매수1(Buy1) ${b1_price:.2f} <b>{b1_qty}주</b> ({tag})\n"
                    if b2_qty > 0:
                        v_rev_guidance += f" 🔴 매수2(Buy2) ${b2_price:.2f} <b>{b2_qty}주</b> ({tag})\n"
                 
                    # 🚨 MODIFIED: [V72.27 0주 새출발 줍줍 생략 레거시 UI 영구 소각]
                    # 시각적 환각(디커플링)을 유발하던 하드코딩 텍스트 100% 적출 완료.
                else:
                    v_rev_guidance += " 🔴 매수 대기: 타점 연산 대기 중\n"

                # 🚨 MODIFIED: [V72.18 수동 VWAP 경고문 영구 소각] KIS 자체 VWAP 자동화에 따라 수동 설정 경고 텍스트 100% 전면 철거.

            is_avwap_hybrid_on = False
            if hasattr(self.cfg, 'get_avwap_hybrid_mode'):
                is_avwap_hybrid_on = await asyncio.to_thread(self.cfg.get_avwap_hybrid_mode, t)

            if is_avwap_hybrid_on:
                is_avwap_active = True
                avwap_qty = tracking_cache.get(f"AVWAP_QTY_{t}", 0)
                avwap_avg = tracking_cache.get(f"AVWAP_AVG_{t}", 0.0)
                avwap_budget = cash
                avwap_strikes = tracking_cache.get(f"AVWAP_STRIKES_{t}", 0)

                if tracking_cache.get(f"AVWAP_SHUTDOWN_{t}"):
                    avwap_status_txt = "🛑 당일 영구동결 (SHUTDOWN)"
                elif tracking_cache.get(f"AVWAP_BOUGHT_{t}"):
                    avwap_status_txt = "🎯 딥매수 완료 (익절/손절 감시중)"
                elif tracking_cache.get(f"AVWAP_COOLDOWN_{t}"):
                    avwap_status_txt = "⏳ 자연 쿨다운 (VWAP 갭 회복 대기중)"
                else:
                    avwap_status_txt = "👀 상승장 필터 스캔 및 갭 타점 대기"

                avwap_base_ticker = 'SOXX' if t == 'SOXL' else ('QQQ' if t == 'TQQQ' else t)
                
                avwap_ctx = tracking_cache.get(f"AVWAP_CTX_{t}")
                if not avwap_ctx:
                     try:
                         avwap_ctx = await asyncio.wait_for(asyncio.to_thread(self.strategy.v_avwap_plugin.fetch_macro_context, avwap_base_ticker), timeout=4.0)
                         if avwap_ctx: tracking_cache[f"AVWAP_CTX_{t}"] = avwap_ctx
                     except Exception: pass

                if status_code in ["PRE", "REG"] and not tracking_cache.get(f"AVWAP_SHUTDOWN_{t}"):
                    try:
                        df_1min_base = await asyncio.wait_for(asyncio.to_thread(self.broker.get_1min_candles_df, avwap_base_ticker), timeout=3.0)
                        base_curr_p = float(await asyncio.wait_for(asyncio.to_thread(self.broker.get_current_price, avwap_base_ticker), timeout=3.0) or 0.0)
                        
                        if hasattr(self.strategy, 'v_avwap_plugin'):
                            avwap_state_dict = {"strikes": tracking_cache.get(f"AVWAP_STRIKES_{t}", 0), "cooldown_active": tracking_cache.get(f"AVWAP_COOLDOWN_{t}", False)}
                            
                            # 🚨 MODIFIED: [V75.03 관찰자 효과 및 시각적 환각 원천 수술] 비동기 래핑 및 is_simulation=True 주입
                            is_apex_on = await asyncio.to_thread(getattr(self.cfg, 'get_avwap_apex_mode', lambda x: True), t)
                            decision = await asyncio.wait_for(
                                asyncio.to_thread(
                                    self.strategy.v_avwap_plugin.get_decision,
                                    base_ticker=avwap_base_ticker, exec_ticker=t,
                                    base_curr_p=base_curr_p, exec_curr_p=curr,
                                    df_1min_base=df_1min_base, avwap_qty=avwap_qty,
                                    now_est=now_est, avwap_state=avwap_state_dict,
                                    context_data=avwap_ctx,
                                    is_apex_on=is_apex_on,
                                    is_simulation=True
                                ),
                                timeout=10.0
                            )
                             
                            avwap_base_price = decision.get('base_curr_p', base_curr_p)
                            avwap_base_vwap = decision.get('vwap', 0.0)
                            avwap_prev_vwap = decision.get('prev_vwap', 0.0)
                            avwap_rolling_tp = decision.get('rolling_tp', 0.0)
                            avwap_gap_pct = decision.get('gap_pct', 0.0)
                            
                            if "대기" in avwap_status_txt:
                                reason = decision.get('reason', '타점 계산중')
                                avwap_status_txt = f"⏳ 대기 ({reason})"
                    except Exception as e:
                        logging.error(f"🚨 [{t}] AVWAP 실시간 레이더 스캔 타임아웃/에러: {e}")

                if not tracking_cache.get(f"AVWAP_BOUGHT_{t}") and not tracking_cache.get(f"AVWAP_SHUTDOWN_{t}"):
                    curr_time = now_est.time()
                    time_0930 = datetime.time(9, 30)
                    time_0934 = datetime.time(9, 34, 59)
                    
                    dump_jitter_sec = tracking_cache.get(f"AVWAP_DUMP_JITTER_{t}", 0)
                    base_dump_dt = datetime.datetime.combine(now_est.date(), datetime.time(15, 20)).replace(tzinfo=ZoneInfo('America/New_York'))
                    dynamic_dump_dt = base_dump_dt - datetime.timedelta(seconds=dump_jitter_sec)
                    time_dynamic_dump = dynamic_dump_dt.time()
         
                    if curr_time < time_0930:
                        avwap_status_txt = "⏳ 프리장 관측 중 (정규장 대기)"
                    elif time_0930 <= curr_time <= time_0934:
                        avwap_status_txt = "⏳ 캔들 형성 대기 중"
                    elif curr_time >= time_dynamic_dump:
                        avwap_status_txt = "⛔ 금일 감시 종료"

            upward_sniper_mode_on = await asyncio.to_thread(self.cfg.get_upward_sniper_mode, t)
            target_val = await asyncio.to_thread(self.cfg.get_target_profit, t)
            escrow_val = await asyncio.to_thread(self.cfg.get_escrow_cash, t)
            avwap_gap_thresh_val = await asyncio.to_thread(getattr(self.cfg, 'get_avwap_gap_threshold', lambda x: -0.67), t) if is_avwap_active else -0.67
            vrev_gap_switch_val = await asyncio.to_thread(getattr(self.cfg, 'get_vrev_gap_switching_mode', lambda x: False), t)
            vrev_gap_thresh_val = await asyncio.to_thread(getattr(self.cfg, 'get_vrev_gap_threshold', lambda x: -0.67), t)

            ticker_data_list.append({
                'ticker': t, 'version': ver, 't_val': t_val, 'split': split, 'curr': curr, 'avg': actual_avg, 'qty': actual_qty,
                'profit_amt': (curr - actual_avg) * actual_qty if actual_qty > 0 else 0, 
                'profit_pct': (curr - actual_avg) / actual_avg * 100 if actual_avg > 0 else 0,
                'upward_sniper': "ON" if upward_sniper_mode_on else "OFF",
                'target': target_val, 'star_pct': round(plan.get('star_ratio', 0) * 100, 2) if 'star_ratio' in plan else 0.0,
                'seed': safe_seed, 'one_portion': plan.get('one_portion', 0.0), 'plan': plan,
                'is_locked': is_already_ordered, 'mode': "REG",
                'is_reverse': is_rev, 'star_price': plan.get('star_price', 0.0),
                'escrow': escrow_val,
                'hybrid_target': hybrid_target_price,
                'trigger_reason': trigger_reason,
                'sniper_trigger': abs(float(dynamic_pct)), 
                'day_high': day_high,
                'day_low': day_low,
                'prev_close': safe_prev_close,
                'tracking_info': tracking_status,
                'dynamic_obj': dynamic_pct_obj,
                'is_sniper_active_time': is_sniper_active_time,
                'vol_weight': round(real_val, 2), 
                'vol_status': vol_status,
                'v_rev_q_lots': v_rev_q_lots,
                'v_rev_q_qty': v_rev_q_qty,
                'v_rev_guidance': v_rev_guidance,
                'avwap_active': is_avwap_active,
                'avwap_budget': avwap_budget,
                'avwap_qty': avwap_qty,
                'avwap_avg': avwap_avg,
                'avwap_status': avwap_status_txt,
                'avwap_strikes': avwap_strikes,
                'avwap_base_ticker': avwap_base_ticker if is_avwap_active else 'N/A',
                'avwap_base_price': avwap_base_price if is_avwap_active else 0.0,
                'avwap_base_vwap': avwap_base_vwap if is_avwap_active else 0.0,
                'avwap_prev_vwap': avwap_prev_vwap if is_avwap_active else 0.0,
                'avwap_rolling_tp': avwap_rolling_tp if is_avwap_active else 0.0,
                'avwap_gap_pct': avwap_gap_pct if is_avwap_active else 0.0,
                'avwap_gap_thresh': avwap_gap_thresh_val,
                'vrev_gap_switch': vrev_gap_switch_val,
                'vrev_gap_thresh': vrev_gap_thresh_val,
                'is_manual_vwap': is_manual_vwap,
                'is_zero_start': is_zero_start_fact,
                'has_snapshot': bool(cached_snap)
            })
            
            total_buy_needed += sum(o['price']*o['qty'] for o in plan.get('orders', []) if o.get('side')=='BUY')

        surplus = cash - total_buy_needed
        rp_amount = surplus * 0.95 if surplus > 0 else 0
        
        try:
            def get_exchange_rate():
                df = yf.Ticker("KRW=X").history(period="1d", timeout=3)
                return float(df['Close'].iloc[-1]) if not df.empty else 0.0
            exchange_rate = await asyncio.wait_for(asyncio.to_thread(get_exchange_rate), timeout=3.0)
        except Exception as e:
            logging.debug(f"⚠️ 야후 파이낸스 환율 스캔 타임아웃: {e}")
            exchange_rate = 0.0

        final_msg, markup = self.view.create_sync_report(
            status_text, dst_txt, cash, rp_amount, ticker_data_list, 
            status_code in ["PRE", "REG"], p_trade_data={}, 
            exchange_rate=exchange_rate
        )

        await update.message.reply_text(final_msg, reply_markup=markup, parse_mode='HTML')

    async def cmd_record(self, update, context):
        if not self._is_admin(update): return
        chat_id = update.message.chat_id
        status_msg = await context.bot.send_message(chat_id, "🛡️ <b>장부 무결성 검증 및 동기화 중...</b>", parse_mode='HTML')
        success_tickers = []
        active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
        for t in active_tickers:
            res = await self.sync_engine.process_auto_sync(t, chat_id, context, silent_ledger=True)
            if res == "SUCCESS": success_tickers.append(t)
        if success_tickers: 
            async with self.tx_lock:
                _, holdings = await asyncio.to_thread(self.broker.get_account_balance)
            await self.sync_engine._display_ledger(success_tickers[0], chat_id, context, message_obj=status_msg, pre_fetched_holdings=holdings)
        else:
            await status_msg.edit_text("✅ <b>동기화 완료</b> (표시할 진행 중인 장부가 없거나 에러 대기 중입니다)", parse_mode='HTML')

    async def cmd_history(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update): return
        target_msg = update.callback_query.message if update.callback_query else update.message
        try: history_data = await asyncio.to_thread(self.cfg.get_history)
        except Exception: history_data = []
        if not history_data:
            await target_msg.reply_text("📭 <b>명예의 전당 (졸업 기록)이 비어있습니다.</b>", parse_mode='HTML')
            return
        sorted_hist = sorted(history_data, key=lambda x: x.get('end_date', ''), reverse=True)
        msg = "🏆 <b>[ 명예의 전당 (과거 졸업 기록) ]</b>\n\n상세 내역을 조회할 기록을 선택하세요.\n"
        keyboard = []
        for h in sorted_hist[:15]: 
            t = h.get('ticker', 'UNK')
            p = h.get('profit', 0.0)
            date_str = h.get('end_date', '')[:10].replace("-", ".")
            sign = "+" if p >= 0 else "-"
            btn_text = f"🏅 {date_str} [{t}] {sign}${abs(p):.2f}"
            keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"HIST:VIEW:{h['id']}")])
        keyboard.append([InlineKeyboardButton("❌ 닫기", callback_data="RESET:CANCEL")])
        await target_msg.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

    async def cmd_mode(self, update, context):
        if not self._is_admin(update): return
        active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
        report = "📊 <b>[ 자율주행 변동성 마스터 지표 상세 분석 ]</b>\n\n"
        report += "<b>[ 🧭 지수 범위 범례 (ON/OFF 권장) ]</b>\n"
        report += "🧊 <code>~ 15.00</code> : 극저변동성 (OFF)\n"
        report += "🟩 <code>15.00 ~ 20.00</code> : 정상 궤도 (OFF)\n"
        report += "🟨 <code>20.00 ~ 25.00</code> : 변동성 확대 (ON)\n"
        report += "🟥 <code>25.00 이상 </code> : 패닉 셀링 (ON)\n\n"
        for t in active_tickers:
            idx_ticker = "SOXX" if t == "SOXL" else "QQQ"
            dynamic_pct_obj = await asyncio.to_thread(self.broker.get_dynamic_sniper_target, idx_ticker)
            if dynamic_pct_obj and hasattr(dynamic_pct_obj, 'metric_val'):
                real_val = float(dynamic_pct_obj.metric_val)
                real_name = dynamic_pct_obj.metric_name
            else:
                real_val = 0.0
                real_name = "지표"
            if real_val <= 15.0: diag_text = "극저변동성 (우측 꼬리 절단 방지를 위해 스나이퍼 OFF)"; status_icon = "🧊"
            elif real_val <= 20.0: diag_text = "정상 궤도 안착 (스나이퍼 OFF)"; status_icon = "🟩"
            elif real_val <= 25.0: diag_text = "변동성 확대 장세 (계좌 방어를 위해 스나이퍼 ON)"; status_icon = "🟨"
            else: diag_text = "패닉 셀링 및 시스템 충격 (스나이퍼 필수 가동)"; status_icon = "🟥"
            report += f"💠 <b>[ {t} 국면 분석 ]</b>\n▫️ 당일 절대 지수({real_name}): {real_val:.2f}\n▫️ 진단 : {status_icon} {diag_text}\n\n"
        report += "🎯 <b>[ 수동 상방 스나이퍼 독립 제어 ]</b>\n"
        keyboard = []
        for t in active_tickers:
            is_sniper = await asyncio.to_thread(self.cfg.get_upward_sniper_mode, t)
            status_txt = 'ON (가동중)' if is_sniper else 'OFF (대기중)'
            report += f"▫️ {t} 현재 상태 : {status_txt}\n"
            keyboard.append([InlineKeyboardButton(f"{t} ⚪ OFF", callback_data=f"MODE:OFF:{t}"), InlineKeyboardButton(f"{t} 🎯 ON", callback_data=f"MODE:ON:{t}")])
        await update.message.reply_text(report, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

    async def cmd_reset(self, update, context):
        if not self._is_admin(update): return
        active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
        msg, markup = self.view.get_reset_menu(active_tickers)
        await update.message.reply_text(msg, reply_markup=markup, parse_mode='HTML')

    async def cmd_seed(self, update, context):
        if not self._is_admin(update): return
        msg = "💵 <b>[ 종목별 시드머니 관리 ]</b>\n\n"
        keyboard = []
        active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
        for t in active_tickers:
            current_seed = await asyncio.to_thread(self.cfg.get_seed, t)
            msg += f"💎 <b>{t}</b>: ${current_seed:,.0f}\n"
            keyboard.append([
                InlineKeyboardButton(f"➕ {t} 추가", callback_data=f"SEED:ADD:{t}"), 
                InlineKeyboardButton(f"➖ {t} 감소", callback_data=f"SEED:SUB:{t}"),
                InlineKeyboardButton(f"🔢 {t} 고정", callback_data=f"SEED:SET:{t}")
            ])
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

    async def cmd_ticker(self, update, context):
        if not self._is_admin(update): return
        active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
        msg, markup = self.view.get_ticker_menu(active_tickers)
        await update.message.reply_text(msg, reply_markup=markup, parse_mode='HTML')

    async def cmd_settlement(self, update, context):
        if not self._is_admin(update): return
        active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
        atr_data = {}
        dynamic_target_data = {} 
        if update.callback_query: status_msg = await update.callback_query.message.reply_text("⏳ <b>실시간 시장 지표 연산 중...</b>", parse_mode='HTML')
        else: status_msg = await update.message.reply_text("⏳ <b>실시간 시장 지표 연산 중...</b>", parse_mode='HTML')
        try:
            jobs = context.job_queue.jobs() if context.job_queue else []
            app_data = jobs[0].data if jobs and len(jobs) > 0 and jobs[0].data is not None else context.bot_data.get('app_data', {})
        except Exception: app_data = context.bot_data.get('app_data', {})
        tracking_cache = app_data.get('sniper_tracking', {})
        for t in active_tickers: atr_data[t] = (0.0, 0.0); dynamic_target_data[t] = None
        msg, markup = self.view.get_settlement_message(active_tickers, self.cfg, atr_data, tracking_cache)
        if update.callback_query:
            try:
                await update.callback_query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                await status_msg.delete()
            except Exception as e:
                 if "Message is not modified" not in str(e): await status_msg.edit_text(msg, reply_markup=markup, parse_mode='HTML')
        else: await status_msg.edit_text(msg, reply_markup=markup, parse_mode='HTML')

    async def cmd_version(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_admin(update): return
        history_data = await asyncio.to_thread(self.cfg.get_full_version_history)
        msg, markup = self.view.get_version_message(history_data, page_index=None)
        await update.message.reply_text(msg, parse_mode='HTML', reply_markup=markup)
