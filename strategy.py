# ==========================================================
# FILE: strategy.py
# ==========================================================
# [strategy.py] - 🌟 V61.00 롱 단일 모멘텀 암살자 전용 라우터 🌟
# ⚠️ 이 주석 및 파일명 표기는 절대 지우지 마세요.
# MODIFIED: [V32.00 그랜드 수술] 불필요한 AVWAP 동적 파라미터 수신 배선 완전 소각
# NEW: [V40.XX 옴니 매트릭스 절대 헌법] TQQQ(V14) 런타임 강제 라우팅(Bypass) 쉴드 이식
# MODIFIED: [V40.XX 옴니 매트릭스 전면 수술] 후행성 60MA/120MA 엔진 전면 소각 및
# 전일 VWAP vs 당일 실시간 VWAP 동행 지표(Coincident Indicator) 듀얼 모멘텀 엔진 수신 및 라우팅 락온
# MODIFIED: [V54.06 SSOT 코어 통일 및 Split-Brain 영구 소각]
# 1) V_REV 모드 판별 시 version="V_REV" 자체를 단일 진실 공급원(SSOT)으로 락온.
# 2) get_plan 내부 V_REV 더미 반환 시 is_reverse=True 로 강제 결속하여 UI 렌더링 엇박자 해체.
# MODIFIED: [V60.00 옴니 매트릭스 락다운 엔진 전면 폐기]
# 기회비용을 훼손하던 apply_omni_matrix_filter 엔진 및 관련 매수 차단 로직 100% 영구 소각 완료.
# MODIFIED: [V61.00 숏(SOXS) 전면 소각 작전 지시서 적용]
# [ V61 절대 헌법 ]: 숏(SOXS) 운용은 시스템 전역에서 100% 영구 소각되었습니다. 
# get_plan 진입부의 SOXS 우회 방어막을 제거하고 롱 단일 모멘텀 아키텍처로 진공 압축 완료.
# MODIFIED: [V61.01 시각적 오염 마커 클리닝] 주석문에 유입된 외부 에디터 렌더링 마커 태그 찌꺼기 100% 도려내어 코드 결벽성 복구.
# MODIFIED: [V71.16 V-REV 런타임 붕괴 및 지시서 증발 맹점 완벽 수술]
# - V-REV 모드가 더미 깡통 데이터([])를 반환하여 17:05 스케줄러 및 수동 주문(EXEC) 시 
#   주문이 100% 증발(Data Starvation)하던 치명적 라우팅 누수 원천 차단.
# - QueueLedger를 동적 로드하고 get_dynamic_plan과 직결하여 팩트 기반 VWAP 예약 주문을 완벽히 반환하도록 역배선 개통 완료.
# 🚨 NEW: [V72.16 AVWAP 정점요격 스위치 탑재]
# get_avwap_decision 호출 규격에 is_apex_on 파라미터를 추가하여,
# 전투 사령부에서 추출한 스위치 상태를 암살자 코어(strategy_v_avwap)로 다이렉트 수혈 배선 개통 완료.
# ==========================================================
import logging
import pandas as pd
from zoneinfo import ZoneInfo
from strategy_v14 import V14Strategy
from strategy_v_avwap import VAvwapHybridPlugin  
from strategy_reversion import ReversionStrategy
from strategy_v14_vwap import V14VwapStrategy

class InfiniteStrategy:
    
    def __init__(self, config):
        self.cfg = config
        self.v14_plugin = V14Strategy(config)
        self.v_avwap_plugin = VAvwapHybridPlugin()
        # MODIFIED: [V42 U-Curve 락온 무결성 복구] ReversionStrategy 객체 생성 시 config 인자 주입 배선 100% 복구
        self.v_rev_plugin = ReversionStrategy(config)
        self.v14_vwap_plugin = V14VwapStrategy(config)

    def analyze_vwap_dominance(self, df):
        if df is None or len(df) < 10:
            return {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}
            
        try:
            # 🚨 MODIFIED: [V44.61 팩트 수술] 프리마켓 거래량 노이즈가 지배력 연산을 오염시키는 맹점 원천 차단
            if 'time_est' in df.columns:
                df = df[(df['time_est'] >= '093000') & (df['time_est'] <= '155900')]
            
            if df.empty or len(df) < 10:
                return {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}

            if 'high' in df.columns and 'low' in df.columns:
                typical_price = (df['high'].astype(float) + df['low'].astype(float) + df['close'].astype(float)) / 3.0
            else:
                typical_price = df['close'].astype(float)
            
            vol_x_price = typical_price * df['volume'].astype(float)
            total_vol = df['volume'].astype(float).sum()
            
            if total_vol == 0:
                return {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}
                
            vwap_price = vol_x_price.sum() / total_vol
      
            df_temp = pd.DataFrame()
            df_temp['volume'] = df['volume'].astype(float)
            df_temp['vol_x_price'] = vol_x_price
            df_temp['cum_vol'] = df_temp['volume'].cumsum()
            df_temp['cum_vol_price'] = df_temp['vol_x_price'].cumsum()
            df_temp['running_vwap'] = df_temp['cum_vol_price'] / df_temp['cum_vol']
            
            idx_10pct = int(len(df_temp) * 0.1)
            vwap_start = df_temp['running_vwap'].iloc[idx_10pct]
            vwap_end = df_temp['running_vwap'].iloc[-1]
            vwap_slope = vwap_end - vwap_start
            
            vol_above = df[df['close'].astype(float) > vwap_price]['volume'].astype(float).sum()
            
            vol_above_pct = vol_above / total_vol if total_vol > 0 else 0
            
            daily_open = df['open'].astype(float).iloc[0]
            daily_close = df['close'].astype(float).iloc[-1]
            
            is_up_day = daily_close > daily_open
            is_down_day = daily_close < daily_open
            
            is_strong_up = is_up_day and (vwap_slope > 0) and (vol_above_pct > 0.60)
            is_strong_down = is_down_day and (vwap_slope < 0) and ((1 - vol_above_pct) > 0.60)
            
            return {
                "vwap_price": round(vwap_price, 2),
                "is_strong_up": bool(is_strong_up),
                "is_strong_down": bool(is_strong_down),
                "vol_above_pct": round(vol_above_pct, 4),
                "vwap_slope": round(vwap_slope, 4)
            }
        except Exception:
            return {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}

    def get_plan(self, ticker, current_price, avg_price, qty, prev_close, ma_5day=0.0, market_type="REG", available_cash=0, is_simulation=False, vwap_status=None, is_snapshot_mode=False, regime_data=None):
        version = self.cfg.get_version(ticker)
        
        # 🚨 MODIFIED: [V61.00 숏(SOXS) 전면 소각 작전 지시서 적용]
        # [ V61 절대 헌법 ]: 숏(SOXS) 운용은 시스템 전역에서 100% 영구 소각되었습니다.
        # TQQQ는 무조건 V14 전용 락온 상태를 유지합니다.
        
        if ticker.upper() == "TQQQ" and version != "V14":
            logging.warning(f"🚨 [{ticker}] 절대 헌법 위반 감지. V14 모드로 강제 라우팅합니다.")
            self.cfg.set_version(ticker, "V14")
            version = "V14"

        if version in ["V13", "V17", "V_VWAP", "V_AVWAP"]:
            logging.warning(f"[{ticker}] 폐기된 레거시 모드({version}) 감지. V14 엔진으로 강제 라우팅합니다.")
            self.cfg.set_version(ticker, "V14")
            version = "V14"

        is_vwap_enabled = getattr(self.cfg, 'get_manual_vwap_mode', lambda x: False)(ticker)
        
        # 기본 플랜 산출
        if version == "V14" and is_vwap_enabled:
            plan = self.v14_vwap_plugin.get_plan(
                ticker=ticker, current_price=current_price, avg_price=avg_price, qty=qty,
                prev_close=prev_close, ma_5day=ma_5day, market_type=market_type,
                available_cash=available_cash, is_simulation=is_simulation,
                is_snapshot_mode=is_snapshot_mode
            )
        elif version == "V_REV":
            # 🚨 MODIFIED: [V71.16 V-REV 런타임 붕괴 및 지시서 증발 맹점 완벽 수술]
            # V-REV 모드 더미 반환 로직 전면 소각 및 get_dynamic_plan 다이렉트 락온
            try:
                from queue_ledger import QueueLedger
                ql = QueueLedger()
                q_data = ql.get_queue(ticker)
                
                plan = self.v_rev_plugin.get_dynamic_plan(
                    ticker=ticker, curr_p=current_price, prev_c=prev_close,
                    current_weight=1.0, vwap_status={}, min_idx=-1,
                    alloc_cash=available_cash, q_data=q_data,
                    is_snapshot_mode=is_snapshot_mode, market_type=market_type
                )
                
                # EXEC 모듈 및 스케줄러 호환성을 위해 core_orders 배열 팩트 주입
                plan['core_orders'] = [o for o in plan.get('orders', [])]
                plan['bonus_orders'] = []
                plan['is_reverse'] = True
                plan['t_val'] = 0.0
                plan['star_price'] = 0.0
                plan['one_portion'] = 0.0
            except Exception as e:
                logging.error(f"🚨 V-REV 플랜 생성 실패: {e}")
                plan = {
                    'core_orders': [], 'bonus_orders': [], 'orders': [],
                    't_val': 0.0, 'is_reverse': True, 'star_price': 0.0, 'one_portion': 0.0
                }
        else:
            plan = self.v14_plugin.get_plan(
                ticker=ticker, current_price=current_price, avg_price=avg_price, qty=qty,
                prev_close=prev_close, ma_5day=ma_5day, market_type=market_type,
                available_cash=available_cash, is_simulation=is_simulation, vwap_status=vwap_status,
                is_snapshot_mode=is_snapshot_mode
            )
                
        return plan

    def capture_vrev_snapshot(self, ticker, clear_price, avg_price, qty):
        if qty <= 0: return None
        
        raw_total_buy = avg_price * qty
        raw_total_sell = clear_price * qty
        
        fee_rate = self.cfg.get_fee(ticker) / 100.0
        net_invested = raw_total_buy * (1.0 + fee_rate)
        net_revenue = raw_total_sell * (1.0 - fee_rate)
        
        realized_pnl = net_revenue - net_invested
        realized_pnl_pct = (realized_pnl / net_invested) * 100 if net_invested > 0 else 0.0
    
        return {
            "ticker": ticker,
            "clear_price": clear_price,
            "avg_price": avg_price,
            "cleared_qty": qty,
            "realized_pnl": realized_pnl,
            "realized_pnl_pct": realized_pnl_pct,
            "captured_at": pd.Timestamp.now(tz=ZoneInfo('America/New_York'))
        }

    def load_avwap_state(self, ticker, now_est):
        if hasattr(self.v_avwap_plugin, 'load_state'):
            return self.v_avwap_plugin.load_state(ticker, now_est)
        return {}

    def save_avwap_state(self, ticker, now_est, state_data):
        if hasattr(self.v_avwap_plugin, 'save_state'):
            self.v_avwap_plugin.save_state(ticker, now_est, state_data)

    def fetch_avwap_macro(self, base_ticker):
        return self.v_avwap_plugin.fetch_macro_context(base_ticker)

    # 🚨 NEW: [V72.16 AVWAP 정점요격 스위치 탑재] is_apex_on 파라미터 수혈
    def get_avwap_decision(self, base_ticker, exec_ticker, base_curr_p, exec_curr_p, base_day_open, avg_price, qty, alloc_cash, context_data, df_1min_base, now_est, avwap_state=None, regime_data=None, is_apex_on=True, **kwargs):
        # MODIFIED: [V60.00] AVWAP 옴니 매트릭스 락다운 필터 100% 영구 소각 완료.
        return self.v_avwap_plugin.get_decision(
            base_ticker=base_ticker, exec_ticker=exec_ticker, base_curr_p=base_curr_p, exec_curr_p=exec_curr_p, 
            base_day_open=base_day_open, avwap_avg_price=avg_price, avwap_qty=qty, avwap_alloc_cash=alloc_cash,
            context_data=context_data, df_1min_base=df_1min_base, now_est=now_est, avwap_state=avwap_state, is_apex_on=is_apex_on, **kwargs
        )
