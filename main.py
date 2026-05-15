# ==========================================================
# FILE: main.py
# ==========================================================
# 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
# 제1헌법: 파일 I/O 및 달력 API(mcal)는 무조건 asyncio.to_thread로 래핑하여 이벤트 루프 교착(Deadlock)을 원천 차단함.
# 제3헌법: KST 타임존 및 is_dst 기반의 동적 스케줄링 전면 소각. 모든 스케줄러 등록 시 ZoneInfo('America/New_York') 기준의 절대 시간으로 하드코딩 락온.
# MODIFIED: [V44.47 KST 타임 패러독스 영구 소각] APScheduler 잡 등록 배선 EST 100% 락온 완료.
# NEW: [전역 타임아웃 이식] scheduled_volatility_scan 이벤트 루프 교착 방어 타임아웃 래퍼 적용.
# NEW: [환각 방어막 이식] 10:00 EST 옴니 매트릭스 타임라인 보호용 백신 주석 하드코딩.
# NEW: [V48.02 애프터마켓 로터리 덫 전면 폐기] 애프터마켓 3% 지정가 덫 스케줄러 영구 소각 및 배선 철거 완료.
# 🚨 NEW: [타임 패러독스 완벽 수술] PTB 버그 회피 명목의 Naive Time 주입 환각 소각 및 EST 절대 락온 복구 완료.
# 🚨 MODIFIED: [PTB 요일 패러독스 영구 소각] KST-EST 시차로 인한 스케줄 증발 차단을 위해 365일 전면 개방(Fail-Open) 락온.
# NEW: [V66.02 원격 로그 핀셋 추출 엔진 탑재] 텔레그램 CommandHandler 배열에 log 및 error 명령어 배선 결속 완료.
# 🚨 MODIFIED: [V71.00 옴니 매트릭스 퀀트 엔진 락온]
# - 17:05 KST 예방 덫 스케줄을 EST로 동적 래핑하여 서머타임 패러독스 원천 차단.
# - vwap_init_and_cancel 기상 시각을 15:25 EST로 전진 배치하여 아키텍처 무결성 확보.
# 🚨 NEW: [V73.00 본진 통합 지시서 덫 장전 디커플링 및 자전거래 원천 차단]
# - 17:05 KST 스케줄을 스냅샷 박제 전용 코루틴(scheduled_snapshot_only)으로 역할 축소.
# - 암살자 전량 덤핑(15:17~15:20) 이후인 15:26 EST에 실제 덫을 투하하는 코루틴(scheduled_regular_trade_delayed) 신설 락온.
# - vwap_init_and_cancel 기상 시각을 15:26 EST로 동기화하여 섀도우 관측 궤 교정 완료.
# 🚨 MODIFIED: [V73.10 확정 정산 타임라인 16:05 EST 전진 배치]
# - 애프터마켓 3% 덫 소각에 따라 무의미해진 21:00 EST 가결제 롤오버 대기를 전면 폐기.
# - 정규장 마감 직후인 16:05 EST에 확정 정산 및 졸업 판별을 즉각 격발하도록 스케줄 및 콜드스타트 타임 윈도우 동기화 락온.
# 🚨 NEW: [V75.02 글로벌 에러 핸들러 누락 팩트 교정]
# - 글로벌 에러 핸들러 배선 결속을 통해 PTB 내부 네트워크 타임아웃 및 스케줄러 붕괴 팩트 로깅 완비
# 🚨 MODIFIED: [V75.03 핫픽스] ContextTypes 임포트 누락으로 인한 런타임 즉사 맹점 100% 수술 완료
# ==========================================================
import os
import logging
import datetime
import asyncio
import math 
from zoneinfo import ZoneInfo
# MODIFIED: [V75.03] ContextTypes 누락 팩트 수혈
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
from scheduler_regular import scheduled_snapshot_only, scheduled_regular_trade_delayed

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
log_filename = f"logs/bot_app_{datetime.datetime.now(est_zone).strftime('%Y%m%d')}.log"

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO,
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# NEW: [V75.02] 글로벌 에러 핸들러 탑재 (날것의 에러 및 타임아웃 팩트 소화)
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
                    logging.warning(f"[{ticker}] 변동성 지표 산출 타임아웃 (10초 초과). 중립 안전마진(1.0) 강제 적용.")
                    real_weight = 1.0
                except Exception as e:
                    logging.warning(f"[{ticker}] 변동성 지표 산출 실패. 중립 안전마진(1.0) 강제 적용: {e}")
                    real_weight = 1.0 
                    
                status_text = "OFF 권장" if real_weight <= 1.0 else "ON 권장"
                briefing_lines.append(f"{ticker}({target_base}): {real_weight:.2f} ({status_text})")
                
            print(f"📊 [자율주행 지표] {' | '.join(briefing_lines)} (상세 게이지: /mode)")
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
    print(f"🚀 옴니 매트릭스 퀀트 엔진 {latest_version} (V73.10 락온)")
    print(f"⏰ 자동 동기화: 16:05 EST 확정 정산 엔진 락온 가동")
    print("🛡️ 1-Tier 자율주행 지표 스캔 대기 중... (매일 10:00 EST 격발)")
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

    # NEW: [V75.02] 글로벌 에러 핸들러 락온
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

    # 🚨 [EST 100% 락온] 토큰 갱신: 6시간 간격 정기 스캔으로 KST 종속성 소각
    jq.run_repeating(scheduled_token_check, interval=21600, first=10, chat_id=ADMIN_CHAT_ID, data=app_data)
    
    # 🚨 MODIFIED: [V73.10 확정 정산 16:05 EST 전진 배치]
    # - 애프터마켓 3% 덫 소각에 따라 무의미해진 21:00 EST 가결제 롤오버 대기를 전면 폐기.
    # - 정규장 마감 직후인 16:05 EST에 확정 정산 및 졸업 판별을 즉각 격발하도록 스케줄 시프트 락온.
    jq.run_daily(scheduled_auto_sync, time=datetime.time(16, 5, tzinfo=est_zone), days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
    
    # 🚨 MODIFIED: [V73.10 콜드 스타트 방어막 동기화] 16:05~16:35 EST 사이 부팅 시 지각 기상 1회성 스케줄 강제 격발
    now_est = datetime.datetime.now(est_zone)
    if now_est.hour == 16 and 5 <= now_est.minute <= 35:
        jq.run_once(scheduled_auto_sync, 5.0, chat_id=ADMIN_CHAT_ID, data=app_data)
        logging.info("🚀 [콜드 스타트 락온] 확정 정산 스케줄 누락(Late Wake-up) 방어를 위해 5초 뒤 1회성 스냅샷/졸업카드를 강제 격발합니다.")
        print("🚀 [콜드 스타트 방어막 가동] 확정 정산 누락을 방지하기 위해 5초 뒤 1회성 스케줄을 강제 격발합니다.")
    
    # 🚨 [EST 100% 락온] 매매 초기화: 04:00 EST
    jq.run_daily(scheduled_force_reset, time=datetime.time(4, 0, tzinfo=est_zone), days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
    
    # 🚨 [EST 100% 락온] 10:00 EST 옴니 매트릭스
    jq.run_daily(scheduled_volatility_scan, time=datetime.time(10, 0, tzinfo=est_zone), days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
    
    # 🚨 [제3헌법] 17:05 KST를 EST로 동적 변환
    dt_now_kst = datetime.datetime.now(kst_zone)
    target_kst_dt = datetime.datetime.combine(dt_now_kst.date(), datetime.time(17, 5), tzinfo=kst_zone)
    target_est_dt = target_kst_dt.astimezone(est_zone)
    regular_snapshot_time = datetime.time(hour=target_est_dt.hour, minute=target_est_dt.minute, tzinfo=est_zone)

    # 🚨 [V73.00 디커플링 락온] 정규장 스냅샷 박제 전용 코루틴
    jq.run_daily(scheduled_snapshot_only, time=regular_snapshot_time, days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
    
    # 🚨 [V73.00 제13경고 준수] 15:26 EST 본진 덫 지연 장전 락온
    delayed_trade_time = datetime.time(15, 26, tzinfo=est_zone)
    jq.run_daily(scheduled_regular_trade_delayed, time=delayed_trade_time, days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)

    # 🚨 [EST 100% 락온] VWAP 1분 타격 개시 전 Fail-Safe: 15:26 EST
    jq.run_daily(scheduled_vwap_init_and_cancel, time=datetime.time(15, 26, tzinfo=est_zone), days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)

    # 매 1분 스나이퍼 및 VWAP 타격
    jq.run_repeating(scheduled_sniper_monitor, interval=60, first=30, chat_id=ADMIN_CHAT_ID, data=app_data)
    jq.run_repeating(scheduled_vwap_trade, interval=60, first=30, chat_id=ADMIN_CHAT_ID, data=app_data)
    
    # 🚨 [EST 100% 락온] 자정 청소 작업: 17:00 EST
    jq.run_daily(scheduled_self_cleaning, time=datetime.time(17, 0, tzinfo=est_zone), days=tuple(range(7)), chat_id=ADMIN_CHAT_ID, data=app_data)
        
    app.run_polling()

if __name__ == "__main__":
    main()
