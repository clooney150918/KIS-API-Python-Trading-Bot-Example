# ==========================================================
# FILE: short_squeeze_engine.py
# ==========================================================
# 🚨 MODIFIED: [API Thundering Herd 방어] YF 모듈 통신 직전 하드코딩된 await asyncio.sleep(0.06)을 영구 소각하고, 백그라운드 스레드 내부에서 GlobalThrottle.wait_api_sync()를 호출하도록 팩트 교정 완료.
# 🚨 VERIFIED: [도메인 주도 설계 (DDD)] 숏 스퀴즈 감시 및 공매도 데이터 스캔 전담 엔진 유지.
# ==========================================================
import asyncio
import logging
import math
import yfinance as yf
from global_throttle import GlobalThrottle # 🚨 NEW: 중앙 통제소 결속

class ShortSqueezeScanner:
    def __init__(self):
        self.proxy_map = {
            "SOXX": "NVDA",  
            "QQQ": "AAPL",   
            "FNGS": "META"
        }

    def _safe_float(self, val) -> float:
        if val is None:
            return 0.0
        try:
            f_val = float(str(val).replace(',', ''))
            if math.isnan(f_val) or math.isinf(f_val):
                return 0.0
            return f_val
        except (ValueError, TypeError):
            return 0.0

    def _fetch_yf_info_sync(self, ticker_symbol: str) -> dict:
        try:
            GlobalThrottle.wait_api_sync() # 🚨 MODIFIED: 중앙 통제소 락온 (동기 스레드 내부 실행)
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            return info if isinstance(info, dict) else {}
        except Exception as e:
            logging.debug(f"⚠️ [{ticker_symbol}] YF 동기 통신 내부 예외: {e}")
            return {}

    async def get_metrics(self, base_ticker: str) -> dict:
        info = {}
        scan_target = self.proxy_map.get(base_ticker, base_ticker)
        
        for attempt in range(3):
            try:
                # 🚨 MODIFIED: 파편화된 sleep 소각 완료
                info = await asyncio.wait_for(
                    asyncio.to_thread(self._fetch_yf_info_sync, scan_target),
                    timeout=10.0
                )
                
                if info:
                    break
                    
            except asyncio.TimeoutError:
                if attempt == 2:
                    logging.error(f"🚨 [{scan_target}] 숏 스퀴즈 감시망 YF 통신 타임아웃(10초) 초과. 빈 데이터 반환 폴백.")
                else:
                    await asyncio.sleep(1.0 * (2 ** attempt))
            except Exception as e:
                if attempt == 2:
                    logging.error(f"🚨 [{scan_target}] 숏 스퀴즈 감시망 치명적 통신 오류: {e}")
                else:
                    await asyncio.sleep(1.0 * (2 ** attempt))

        if not isinstance(info, dict):
            info = {}

        short_pct_of_float = self._safe_float(info.get('shortPercentOfFloat', 0.0)) * 100.0
        days_to_cover = self._safe_float(info.get('shortRatio', 0.0))
        shares_short = self._safe_float(info.get('sharesShort', 0.0))

        is_si_high = short_pct_of_float >= 15.0
        is_dtc_high = days_to_cover >= 4.0

        if is_si_high and is_dtc_high:
            status = "🚨 <b>[위험] 숏 스퀴즈 화약고 (임계치 초과)</b>"
            action = "롱(Long) 포지션 폭등 대기 / 숏(Short) 포지션 절대 진입 금지"
        elif is_si_high or is_dtc_high:
            status = "⚠️ <b>[주의] 수급 왜곡 초기 징후 감지</b>"
            action = "변동성 확대 주의 및 리스크 관리 요망"
        else:
            status = "✅ <b>[안정] 정상 수급 범위 내 거래 중</b>"
            action = "기술적 지표 및 펀더멘털 기반 정상 매매 인가"
            
        if short_pct_of_float == 0.0 and days_to_cover == 0.0:
            status = "데이터 집계 대기 중..."
            action = "판단 보류 (YF 데이터 결측)"

        return {
            "Scan_Target": scan_target,
            "SI_Float": round(short_pct_of_float, 2),
            "DTC": round(days_to_cover, 2),
            "Shares_Short": shares_short,
            "Status": status,
            "Action": action
        }

    def get_squeeze_guidance_text(self, base_ticker: str, metrics: dict) -> str:
        scan_target = metrics.get("Scan_Target", base_ticker)
        si_float = metrics.get("SI_Float", 0.0)
        dtc = metrics.get("DTC", 0.0)
        status = metrics.get("Status", "알 수 없음")
        action = metrics.get("Action", "알 수 없음")

        msg = f"💡 <b>[ {base_ticker} (대장주: {scan_target}) 실시간 숏 스퀴즈 진단 리포트 ]</b>\n\n"
        
        msg += "📊 <b>[ 실시간 온체인 스캔 결과 ]</b>\n"
        if si_float == 0.0 and dtc == 0.0:
            msg += "⚠️ <b>데이터 결측:</b> Yahoo Finance 서버 지연 또는 통신 오류로 데이터를 불러올 수 없습니다.\n\n"
        else:
            msg += f"▫️ 공매도 잔고율 (SI): <b>{si_float:.2f}%</b>\n"
            msg += f"▫️ 숏 커버링 일수 (DTC): <b>{dtc:.2f}일</b>\n"
            msg += f"▫️ <b>시스템 판정: {status}</b>\n"
            msg += f"▫️ <b>알고리즘 권고:</b> {action}\n\n"

        msg += "📚 <b>[ 3대 핵심 온체인 지표 해석 팩트 ]</b>\n"
        msg += "▫️ <b>유통주식수 대비 공매도 비율 (SI % of Float)</b>\n"
        msg += "🔹 <b>정의:</b> 거래 가능한 주식 중 공매도 세력이 빌려 판 주식의 비율.\n"
        msg += "🔹 <b>임계치:</b> <b>15.0%</b> 초과 시 '화약고', 20% 초과 시 극단적 위험군.\n"
        msg += "🔹 <b>해석:</b> 수치가 높을수록 잠재적 대기 매수세(갚아야 할 물량)가 거대함을 의미.\n\n"
        
        msg += "▫️ <b>숏 레이시오 (Days to Cover, DTC)</b>\n"
        msg += "🔹 <b>정의:</b> 숏 세력이 보유량을 모두 사서 갚으려면 걸리는 예상 일수.\n"
        msg += "🔹 <b>임계치:</b> <b>4.0일</b> 이상일 경우 숏 탈출 병목 심각 판정.\n"
        msg += "🔹 <b>해석:</b> 높을수록 주가 급등 시 매수 대기열이 엉키며 주가가 수직 상승(슈팅)함.\n\n"
        
        msg += "⚙️ <b>[ 숏 스퀴즈 연쇄 폭발 메커니즘 ]</b>\n"
        msg += "🔸 <b>1단계:</b> 과도한 공매도 물량 집중 (SI &gt; 15%).\n"
        msg += "🔸 <b>2단계:</b> 호재 유입 등으로 주가 상향 돌파 시작.\n"
        msg += "🔸 <b>3단계:</b> 숏 세력 손실 확대 및 증권사 담보금 부족 강제 청산(마진콜).\n"
        msg += "🔸 <b>4단계:</b> 파산을 막기 위한 시장가 매수(Buy-to-Cover)가 쏟아지며 본질 가치를 무시하고 폭등.\n"
        
        return msg
