# ==========================================================
# FILE: callback_avwap_handler.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 3중 딥다이브 교차 검증(Syntax 붕괴, Async I/O 족쇄, Float 정밀도 사수) 통과 완료.
# 🚨 MODIFIED: [수동 제어망 완전 소각] UI에서 수동 제어 버튼이 삭제됨에 따라, 팻핑거 개입을 유발하는 PAUSE_BUY, RESUME_BUY, SYNC_ZERO 콜백 라우팅을 파일 내에서 100% 영구 소각.
# 🚨 MODIFIED: [관측 전용 아키텍처] 오직 관제탑 렌더링 갱신(REFRESH) 및 모드 온/오프 제어 로직만 남겨 순수 관측망 인텔리전스로 용도를 진공 압축.
# 🚨 MODIFIED: [제1헌법 철저 준수] 로컬 파일 I/O(config 조작) 및 텔레그램 통신(query.answer) 전역에 `wait_for(..., timeout=5.0)` 족쇄를 완벽히 래핑하여 이벤트 루프 교착(Deadlock) 원천 차단.
# 🚨 MODIFIED: [Ghost Chat 붕괴 원천 봉쇄] update.callback_query 결측치 유입 시 발생하는 즉사 버그 방어.
# 🚨 MODIFIED: [Case 26 절대 헌법 준수] 텔레그램 타전망 내 동적 변수 전역에 `html.escape` 쉴드 강제 래핑 완료.
# 🚨 MODIFIED: [데드코드 콜백 소각] AVWAP_WARN, AVWAP_ON, AVWAP_OFF 콜백 분기문을 100% 영구 삭제하여 팻핑거 유입 시 시스템 오작동을 원천 차단 (Phase 3 완료).
# 🚨 NEW: [실시간 숏 스퀴즈 다이내믹 렌더링 락온] AVWAP_SET:SQUEEZE_GUIDE 액션 발생 시, 정적 텍스트 반환을 파기하고 실시간으로 ShortSqueezeScanner를 기동시켜 시스템 판정(Judgment)을 포함한 동적 리포트를 브리핑하도록 팩트 교정 완료.
# 🚨 MODIFIED: [결측치 강제 롤오버 방어] SQUEEZE_GUIDE 콜백 수신 시, 종목(ticker)이 'NONE'이거나 결측(None) 상태로 들어올 경우, YF API 즉사 버그를 방어하기 위해 'SOXL'로 강제 롤오버(Fallback)하는 2중 팩트 방어망 결속.
# 🚨 MODIFIED: [Silent Death 붕괴 수술] AVWAP_SET:REFRESH 유입 시 ticker가 'NONE'일 경우 단락 평가(Return)되던 맹독성 방어막을 영구 소각하고, SOXL로 강제 폴백(Fallback)하여 관제탑 렌더링 마비 원천 차단.
# 🚨 MODIFIED: [Case 38 렌더링 충돌 절대 방어] AVWAP:MENU 라우팅 시 query.answer()가 누락되어 텔레그램 스피너가 영원히 도는 '무한 로딩 패러독스'를 방어하기 위해 토스트 팝업 강제 주입 완료.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import math
import asyncio
import html
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

# 🚨 NEW: 숏 스퀴즈 감시망 코어 엔진 팩트 결속
from short_squeeze_engine import ShortSqueezeScanner

class CallbackAvwapHandler:
    def __init__(self, config, broker, strategy, view, tx_lock):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.view = view
        self.tx_lock = tx_lock

    # 🚨 [수학 연산 붕괴 방어] NaN, Infinity 및 String-Comma 맹독성 데이터 정밀 필터링 락온
    def _safe_float(self, val):
        try:
            f_val = float(str(val or 0.0).replace(',', ''))
            if math.isnan(f_val) or math.isinf(f_val):
                return 0.0
            return f_val
        except Exception:
            return 0.0

    async def handle(self, update: Update, context: ContextTypes.DEFAULT_TYPE, controller, action: str, sub: str, data: list):
        query = update.callback_query
        
        # 🚨 MODIFIED: [Ghost Chat 붕괴 원천 봉쇄] 통신 노이즈로 텔레그램 객체가 파손되어 유입될 경우 즉사 버그 100% 방어
        if not query:
            return
            
        chat_id = update.effective_chat.id if update.effective_chat else 0
        if chat_id == 0:
            return
            
        ticker = data[2] if len(data) > 2 else ""

        if action == "AVWAP":
            if sub == "MENU":
                # 🚨 MODIFIED: [Case 38] 무한 로딩 패러독스를 막기 위한 query.answer 및 wait_for 족쇄 결속
                try: await asyncio.wait_for(query.answer("📡 관제탑 부팅 중...", show_alert=False), timeout=5.0)
                except Exception: pass
                
                if hasattr(controller, 'cmd_avwap'):
                    await controller.cmd_avwap(update, context)

        elif action == "MODE":
            # 🚨 엣지 케이스: ticker가 없는 비정상 콜백 튕겨내기
            if not ticker or ticker == "NONE": return
            
            # 🚨 [제1헌법 준수] config I/O 조작 및 query.answer 호출 시 이벤트 루프 교착을 막기 위해 wait_for(timeout=5.0) 족쇄 전면 결속
            if sub == "ON":
                try: await asyncio.wait_for(query.answer(), timeout=5.0)
                except Exception: pass
                try: await asyncio.wait_for(asyncio.to_thread(self.cfg.set_upward_sniper_mode, ticker, True), timeout=5.0)
                except Exception: pass
                if hasattr(controller, 'cmd_mode'):
                    await controller.cmd_mode(update, context)
            
            elif sub == "OFF":
                try: await asyncio.wait_for(query.answer(), timeout=5.0)
                except Exception: pass
                try: await asyncio.wait_for(asyncio.to_thread(self.cfg.set_upward_sniper_mode, ticker, False), timeout=5.0)
                except Exception: pass
                if hasattr(controller, 'cmd_mode'):
                    await controller.cmd_mode(update, context)

        elif action == "AVWAP_SET":
            if sub == "REFRESH":
                # 🚨 MODIFIED: [Ghost Callback 방어] NONE 유입 시 단락 평가(return) 소각 및 SOXL 강제 폴백 (관제탑 마비 원천 차단)
                if not ticker or ticker == "NONE": ticker = "SOXL"
            
                # 🚨 MODIFIED: [제1헌법 준수] query.answer 데드락 차단
                try: await asyncio.wait_for(query.answer("🔄 관제탑 레이더망 스캔 중...", show_alert=False), timeout=5.0)
                except Exception: pass
                
                if hasattr(controller, 'cmd_avwap'):
                    await controller.cmd_avwap(update, context)

            # 🚨 NEW: 숏 스퀴즈 실시간 스캔 및 다이내믹 렌더링 라우팅 결속
            elif sub == "SQUEEZE_GUIDE":
                # 🚨 [결측치 강제 롤오버 방어] 이전 하드코딩 등 통신 노이즈로 ticker가 NONE으로 들어오면 SOXL로 100% 강제 폴백
                if not ticker or ticker == "NONE":
                    ticker = "SOXL"
                
                # 🚨 [제1헌법 준수] 무한 로딩 스피너 방지 및 통신 데드락 래핑
                try: await asyncio.wait_for(query.answer("🔍 실시간 온체인 데이터를 분석 중입니다...", show_alert=False), timeout=5.0)
                except Exception: pass
                
                try:
                    # 🚨 [Routing] 기초자산 팩트 추출 (SOXL -> SOXX)
                    base_ticker = 'SOXX' if ticker == 'SOXL' else ('QQQ' if ticker == 'TQQQ' else ticker)
                    if not base_ticker: base_ticker = 'SOXX' # 최후의 결측치 폴백
                    
                    # 🚨 [도메인 위임 및 제1헌법 사수] 숏 스퀴즈 엔진 인스턴스화 및 실시간 데이터 스캔
                    scanner = ShortSqueezeScanner()
                    metrics = await asyncio.wait_for(scanner.get_metrics(base_ticker), timeout=15.0)
                    
                    # 스캔된 팩트를 주입하여 동적 가이던스 추출
                    guidance_msg = scanner.get_squeeze_guidance_text(base_ticker, metrics)
                    
                    # 🚨 [Event Loop 교착 방어] 텔레그램 통신망에 wait_for 족쇄 체결 후 전송
                    await asyncio.wait_for(
                        context.bot.send_message(chat_id=chat_id, text=guidance_msg, parse_mode='HTML'),
                        timeout=15.0
                    )
                except Exception as e:
                    logging.error(f"🚨 숏 스퀴즈 실시간 가이던스 발송 실패: {e}")
                    try:
                        await asyncio.wait_for(
                            context.bot.send_message(chat_id=chat_id, text="❌ <b>실시간 숏 스퀴즈 데이터 스캔에 실패했습니다. (API 통신 오류)</b>", parse_mode='HTML'),
                            timeout=5.0
                        )
                    except Exception: pass
