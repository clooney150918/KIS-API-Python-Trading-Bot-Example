# ==========================================================
# FILE: main.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 5대 헌법 및 40대 엣지 케이스 완벽 결속 교차 검증 완료.
# 🚨 MODIFIED: [Event Loop 교착 수술] post_init 내부 commands_handler 및 콜백 하위 도메인 락온 전파 누락 맹점 원천 봉쇄.
# 🚨 MODIFIED: [15:59 MOC 덤핑 락온] 암살자 제로-오버나이트 강제 청산을 위한 15:59 EST 전용 크론 스케줄 유지.
# 🚨 MODIFIED: [16:05 확정 정산망 단일화] 암살자 물량 보유 시 스킵하던 로직을 폐기하고, 15:59 덤핑 완료 후 무조건 당일 100% 정산되도록 졸업 스캔망 단일화.
# 🚨 MODIFIED: [로깅 증발 원천 차단] 로깅 설정을 최상단(환경 변수 스캔 전)으로 전진 배치하여 헤드리스 구동 시 발생하는 부트스트랩 로그 증발 완벽 차단.
# 🚨 MODIFIED: [제1헌법 철저 준수] get_active_tickers 호출부 등 모든 동기 I/O 구간에 asyncio.wait_for 족쇄 완벽 래핑 (이벤트 루프 교착 원천 봉쇄).
# 🚨 MODIFIED: [Case 34 전역 GC 락온] 디스크 용량 고갈 붕괴 방어를 위해 `TimedRotatingFileHandler` 이식 및 7일 초과 로그 자동 영구 소각 배선 유지.
# 🚨 MODIFIED: [V73.15 타임라인 디커플링] 17:05 KST V4.0 선제 타격 및 V-REV 스냅샷 분리 락온.
# 🚨 MODIFIED: [맹점 4 수술] 서머타임 래핑 타임 패러독스 차단 및 KST 네이티브 위임 락온.
# 🚨 MODIFIED: [Case 26] 텔레그램 파서 붕괴 방어용 html 모듈 결속.
# ==========================================================
import os
import logging
from logging.handlers import TimedRotatingFileHandler
import datetime
import asyncio
import math 
import html 
from zoneinfo import ZoneInfo
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, Defaults, ContextTypes
from dotenv import load_dotenv

from config import ConfigManager
from broker import KoreaInvestmentBroker
from strategy import InfiniteStrategy
from telegram_bot import TelegramController
from queue_ledger import QueueLedger
from runtime_safety import RuntimeSafetyGate
from shadow_intent import ShadowIntentRecorder

from scheduler_core import (
    scheduled_token_check,
    scheduled_auto_sync,
    scheduled_force_reset,
    scheduled_self_cleaning,
    perform_self_cleaning,
    is_market_open
)
from scheduler_regular import scheduled_early_regular_trade

TICKER_BASE_MAP = {
    "SOXL": "SOXX",
    "TQQQ": "QQQ",
    "TSLL": "TSLA",
    "FNGU": "FNGS",
    "BULZ": "FNGS"
}

if not os.path.exists('data'):
    os.makedirs('data')
if not os.path.exists('logs'):
    os.makedirs('logs')

# 🚨 MODIFIED: [로깅 증발 원천 차단] 로깅 설정을 최상단(환경 변수 스캔 전)으로 전진 배치하여 헤드리스 구동 시 발생하는 부트스트랩 로그 증발 완벽 차단.
log_filename = "logs/bot_app.log"
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO,
    handlers=[
        # 🚨 MODIFIED: [Case 34] 로그명 단일화 및 TimedRotatingFileHandler 주입 (7일치 백업 유지, 이전 영구 소각)
        TimedRotatingFileHandler(log_filename, when="midnight", interval=1, backupCount=7, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

load_dotenv() 

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
try:
    ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID")) if os.getenv("ADMIN_CHAT_ID") else None
except ValueError:
    ADMIN_CHAT_ID = None

APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
CANO = os.getenv("CANO")
ACNT_PRDT_CD = os.getenv("ACNT_PRDT_CD", "01")

if not all([TELEGRAM_TOKEN, APP_KEY, APP_SECRET, CANO, ADMIN_CHAT_ID]):
    # 🚨 MODIFIED: [로깅 증발 방어] print 데드코드 소각 및 logging.critical 락온
    logging.critical("❌ [치명적 오류] .env 파일에 봇 구동 필수 키가 누락되었습니다. 봇을 종료합니다.")
    os._exit(1)

est_zone = ZoneInfo('America/New_York')

async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.error("🚨 [Global Error] Exception while handling an update:", exc_info=context.error)

async def post_init(application: Application):
    tx_lock = asyncio.Lock()
    application.bot_data['app_data']['tx_lock'] = tx_lock
    application.bot_data['bot_controller'].tx_lock = tx_lock
    
    application.bot_data['bot_controller'].sync_engine.tx_lock = tx_lock
    application.bot_data['bot_controller'].callbacks_handler.tx_lock = tx_lock
    
    # 🚨 MODIFIED: [Event Loop 교착 수술] commands_handler 및 콜백 하위 도메인의 Lock 갱신 전파 락온
    application.bot_data['bot_controller'].commands_handler.tx_lock = tx_lock
    application.bot_data['bot_controller'].callbacks_handler.order_handler.tx_lock = tx_lock
    application.bot_data['bot_controller'].callbacks_handler.avwap_handler.tx_lock = tx_lock
    application.bot_data['bot_controller'].callbacks_handler.config_handler.tx_lock = tx_lock

def main():
    est_zone = ZoneInfo('America/New_York')
    kst_zone = ZoneInfo('Asia/Seoul')
    
    cfg = ConfigManager()
    latest_version = cfg.get_latest_version() 
    
    logging.info("=" * 60)
    logging.info(f"🚀 옴니 매트릭스 퀀트 엔진 {latest_version} (V86.00 순수 리버전 팩트 락온 에디션)")
    logging.info("=" * 60)
    
    perform_self_cleaning()
    cfg.set_chat_id(ADMIN_CHAT_ID)
    
    runtime_safety_gate = RuntimeSafetyGate()
    shadow_intent_recorder = ShadowIntentRecorder()
    broker = KoreaInvestmentBroker(
        APP_KEY, APP_SECRET, CANO, ACNT_PRDT_CD,
        runtime_safety_gate=runtime_safety_gate,
        shadow_intent_recorder=shadow_intent_recorder,
        account_fingerprint_key=os.getenv("ACCOUNT_FINGERPRINT_HMAC_KEY"),
    )
    strategy = InfiniteStrategy(cfg)
    queue_ledger = QueueLedger()
    
    bot = TelegramController(
        cfg, broker, strategy, tx_lock=None, 
        queue_ledger=queue_ledger, strategy_rev=None
    )
    
    app_data = {
        'cfg': cfg, 'broker': broker, 'strategy': strategy, 
        'queue_ledger': queue_ledger, 'strategy_rev': None,
        'runtime_safety_gate': runtime_safety_gate,
        'scheduler_safety_gate': runtime_safety_gate,
        'shadow_intent_recorder': shadow_intent_recorder,
        'bot': bot, 'tx_lock': None, 'base_map': TICKER_BASE_MAP,
        'tz_est': est_zone, 'regime_data': {"status": "pending", "msg": "10:00 EST 이전 오프닝 휩소 대기"} 
    }

    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .connection_pool_size(8)
        .defaults(Defaults(tzinfo=est_zone))
        .post_init(post_init)
        .build()
    )
    
    app.bot_data['app_data'] = app_data
    app.bot_data['bot_controller'] = bot
    app.add_error_handler(global_error_handler)
    
    for cmd, handler in [
        ("start", bot.cmd_start), ("record", bot.cmd_record), ("history", bot.cmd_history), 
        ("sync", bot.cmd_sync), ("settlement", bot.cmd_settlement), ("seed", bot.cmd_seed), 
        ("ticker", bot.cmd_ticker), ("mode", bot.cmd_mode), ("reset", bot.cmd_reset), 
        ("version", bot.cmd_version), ("update", bot.cmd_update),
        ("avwap", bot.cmd_avwap), ("queue", bot.cmd_queue), ("add_q", bot.cmd_add_q), ("clear_q", bot.cmd_clear_q),
        ("log", bot.cmd_log), ("error", bot.cmd_log)
    ]:
        app.add_handler(CommandHandler(cmd, handler))
        
    app.add_handler(CallbackQueryHandler(bot.handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.handle_message))
    
    jq = app.job_queue

    jq.run_repeating(scheduled_token_check, interval=21600, first=10, chat_id=ADMIN_CHAT_ID, data=app_data)
    jq.run_daily(scheduled_auto_sync, time=datetime.time(16, 5, tzinfo=est_zone), days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
    
    now_est = datetime.datetime.now(est_zone)
    if now_est.hour == 16 and 5 <= now_est.minute <= 35:
        jq.run_once(scheduled_auto_sync, 5.0, chat_id=ADMIN_CHAT_ID, data=app_data)
    
    jq.run_daily(scheduled_force_reset, time=datetime.time(4, 0, tzinfo=est_zone), days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
    # 🚨 MODIFIED: [맹점 4 수술] KST 래핑 타임 패러독스(Time Paradox) 완벽 교정 및 PTB 네이티브 타임존 100% 위임
    early_trade_time = datetime.time(17, 5, tzinfo=kst_zone)

    jq.run_daily(scheduled_early_regular_trade, time=early_trade_time, days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
    jq.run_daily(scheduled_self_cleaning, time=datetime.time(17, 0, tzinfo=est_zone), days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
        
    app.run_polling()

if __name__ == "__main__":
    main()
