# ==========================================================
# FILE: main.py
# ==========================================================
# 🚨 MODIFIED: [Case 34 전역 GC 락온] 디스크 용량 고갈 붕괴 방어를 위해 `TimedRotatingFileHandler` 이식 및 7일 초과 로그 자동 영구 소각 배선 개통
# 🚨 MODIFIED: [V73.15 타임라인 디커플링 대통합] 17:05 KST V14 선제 타격 및 V-REV 스냅샷 분리 락온
# 🚨 MODIFIED: [맹점 4 수술] 서머타임 래핑 타임 패러독스 차단 및 KST 네이티브 위임 락온
# ==========================================================
import os
import logging
from logging.handlers import TimedRotatingFileHandler # 🚨 NEW: [Case 34] 로깅 로테이션 모듈 전진 배치
import datetime
import asyncio
import math 
from zoneinfo import ZoneInfo
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, Defaults, ContextTypes
from dotenv import load_dotenv

from config import ConfigManager
from broker import KoreaInvestmentBroker
from strategy import InfiniteStrategy
from telegram_bot import TelegramController
from queue_ledger import QueueLedger
from strategy_reversion import ReversionStrategy
from volatility_engine import VolatilityEngine, determine_market_regime

from scheduler_core import (
    scheduled_token_check,
    scheduled_auto_sync,
    scheduled_force_reset,
    scheduled_self_cleaning,
    perform_self_cleaning,
    is_market_open
)
from scheduler_sniper import scheduled_sniper_monitor
from scheduler_vwap import scheduled_vwap_trade, scheduled_vwap_init_and_cancel
from scheduler_regular import scheduled_early_regular_trade, scheduled_regular_trade_delayed

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
    print("❌ [치명적 오류] .env 파일에 봇 구동 필수 키가 누락되었습니다. 봇을 종료합니다.")
    exit(1)

est_zone = ZoneInfo('America/New_York')

# 🚨 MODIFIED: [Case 34] 로그명 단일화 및 TimedRotatingFileHandler 주입 (7일치 백업 유지, 이전 영구 소각)
log_filename = "logs/bot_app.log"

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO,
    handlers=[
        TimedRotatingFileHandler(log_filename, when="midnight", interval=1, backupCount=7, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logging.getLogger("yfinance").setLevel(logging.CRITICAL)

async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.error("🚨 [Global Error] Exception while handling an update:", exc_info=context.error)

async def scheduled_volatility_scan(context):
    try:
        is_open = await asyncio.wait_for(asyncio.to_thread(is_market_open), timeout=10.0)
    except asyncio.TimeoutError:
        logging.error("⚠️ [volatility_scan] 달력 API 타임아웃. 평일 강제 개장 처리합니다.")
        est = ZoneInfo('America/New_York')
        is_open = datetime.datetime.now(est).weekday() < 5
    except Exception:
        est = ZoneInfo('America/New_York')
        is_open = datetime.datetime.now(est).weekday() < 5
        
    if not is_open:
        return

    async def _do_scan():
        app_data = context.job.data
        cfg = app_data['cfg']
        broker = app_data['broker']
        base_map = app_data.get('base_map', TICKER_BASE_MAP)
        
        print("\n" + "=" * 60)
        print("📈 [자율주행 변동성 & 시장 국면 스캔 완료] (10:00 EST 스냅샷)")
        
        for attempt in range(3):
            regime_data = await determine_market_regime(broker)
            if regime_data.get("status") == "success":
                break
            if attempt < 2:
                logging.warning(f"⚠️ 옴니 매트릭스 스캔 실패 (시도 {attempt+1}/3). 10초 후 재시도합니다.")
                await asyncio.sleep(10.0)
        
        app_data['regime_data'] = regime_data
        
        if regime_data.get("status") == "success":
            regime = regime_data.get("regime")
            target_ticker = regime_data.get("target_ticker")
            close_p = regime_data.get("close", 0.0)
            prev_vwap = regime_data.get("prev_vwap", 0.0)
            curr_vwap = regime_data.get("curr_vwap", 0.0)
            desc = regime_data.get("desc", "")
            print(f"🏛️ 옴니 매트릭스: [{regime}] 타겟: {target_ticker} ({desc}) | 종가: {close_p:.2f}, 당일VWAP: {curr_vwap:.2f}, 전일VWAP: {prev_vwap:.2f}")
        else:
            print(f"⚠️ 옴니 매트릭스 판별 실패: {regime_data.get('msg')}")

        active_tickers = await asyncio.to_thread(cfg.get_active_tickers)
        
        if not active_tickers:
            print("📊 현재 운용 중인 종목이 없습니다.")
        else:
            briefing_lines = []
            vol_engine = VolatilityEngine()
            for ticker in active_tickers:
                target_base = base_map.get(ticker, ticker)
                try:
                    weight_data = await asyncio.wait_for(
                        asyncio.to_thread(vol_engine.calculate_weight, target_base),
                        timeout=10.0
                    )
                    raw_weight = weight_data.get('weight', 1.0) if isinstance(weight_data, dict) else weight_data
                    real_weight = float(raw_weight)
                   
                    if not math.isfinite(real_weight):
                        raise ValueError(f"비정상 수학 수치 산출: {real_weight}")
                except asyncio.TimeoutError:
                    real_weight = 1.0
                except Exception as e:
                    real_weight = 1.0 
                    
                status_text = "OFF 권장" if real_weight <= 1.0 else "ON 권장"
                briefing_lines.append(f"{ticker}({target_base}): {real_weight:.2f} ({status_text})")
                
            print(f"📊 [자율주행 지표] {' | '.join(briefing_lines)}")
        print("=" * 60 + "\n")

    try:
        await asyncio.wait_for(_do_scan(), timeout=60.0)
    except Exception as e:
        logging.error(f"🚨 [volatility_scan] 전역 타임아웃(60초) 또는 런타임 붕괴 발생: {e}")

async def post_init(application: Application):
    tx_lock = asyncio.Lock()
    application.bot_data['app_data']['tx_lock'] = tx_lock
    application.bot_data['bot_controller'].tx_lock = tx_lock
    
    application.bot_data['bot_controller'].sync_engine.tx_lock = tx_lock
    application.bot_data['bot_controller'].callbacks_handler.tx_lock = tx_lock

def main():
    est_zone = ZoneInfo('America/New_York')
    kst_zone = ZoneInfo('Asia/Seoul')
    
    cfg = ConfigManager()
    latest_version = cfg.get_latest_version() 
    
    print("=" * 60)
    print(f"🚀 옴니 매트릭스 퀀트 엔진 {latest_version} (V79.50 팩트 교정본)")
    print("=" * 60)
    
    perform_self_cleaning()
    cfg.set_chat_id(ADMIN_CHAT_ID)
    
    broker = KoreaInvestmentBroker(APP_KEY, APP_SECRET, CANO, ACNT_PRDT_CD)
    strategy = InfiniteStrategy(cfg)
    queue_ledger = QueueLedger()
    strategy_rev = ReversionStrategy(cfg)
    
    bot = TelegramController(
        cfg, broker, strategy, tx_lock=None, 
        queue_ledger=queue_ledger, strategy_rev=strategy_rev
    )
    
    app_data = {
        'cfg': cfg, 'broker': broker, 'strategy': strategy, 
        'queue_ledger': queue_ledger, 'strategy_rev': strategy_rev,  
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
    jq.run_daily(scheduled_volatility_scan, time=datetime.time(10, 0, tzinfo=est_zone), days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
    
    # 🚨 MODIFIED: [맹점 4 수술] KST 래핑 타임 패러독스(Time Paradox) 완벽 교정 및 PTB 네이티브 타임존 100% 위임
    early_trade_time = datetime.time(17, 5, tzinfo=kst_zone)

    jq.run_daily(scheduled_early_regular_trade, time=early_trade_time, days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
    
    # 🚨 [15:26 EST] V-REV 본진 덫 지연 장전 락온
    delayed_trade_time = datetime.time(15, 26, tzinfo=est_zone)
    jq.run_daily(scheduled_regular_trade_delayed, time=delayed_trade_time, days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)

    jq.run_daily(scheduled_vwap_init_and_cancel, time=datetime.time(15, 26, tzinfo=est_zone), days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)

    jq.run_repeating(scheduled_sniper_monitor, interval=60, first=30, chat_id=ADMIN_CHAT_ID, data=app_data)
    jq.run_repeating(scheduled_vwap_trade, interval=60, first=30, chat_id=ADMIN_CHAT_ID, data=app_data)
    jq.run_daily(scheduled_self_cleaning, time=datetime.time(17, 0, tzinfo=est_zone), days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
        
    app.run_polling()

if __name__ == "__main__":
    main()
