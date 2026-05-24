# ==========================================================
# FILE: strategy.py
# ==========================================================
# [strategy.py] - 🌟 V61.00 롱 단일 모멘텀 암살자 전용 라우터 🌟
# 🚨 MODIFIED: [V54.06 SSOT 코어 통일 및 Split-Brain 영구 소각]
# 🚨 MODIFIED: [순수익 2.0% 절대 보장 타점 공식 릴레이 배선 개통]
# 🚨 NEW: [Case 11] 다중 출격(Multi-Sortie) 모드 파라미터 수혈 배선 이식
# 🚨 NEW: [라우팅 누수 방어] V14 스나이퍼 감시 라우터 배선 개통 완료
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
        self.v_rev_plugin = ReversionStrategy(config)
        self.v14_vwap_plugin = V14VwapStrategy(config)

    def analyze_vwap_dominance(self, df):
        if df is None or len(df) < 10:
            return {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}
            
        try:
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
        
        # 🚨 MODIFIED: [Case 04 준수] TQQQ는 오직 V14로만 강제 라우팅 (SOXS 소각)
        if ticker.upper() == "TQQQ" and version != "V14":
            logging.warning(f"🚨 [{ticker}] 절대 헌법 위반 감지. V14 모드로 강제 라우팅합니다.")
            self.cfg.set_version(ticker, "V14")
            version = "V14"

        if version in ["V13", "V17", "V_VWAP", "V_AVWAP"]:
            logging.warning(f"[{ticker}] 폐기된 레거시 모드({version}) 감지. V14 엔진으로 강제 라우팅합니다.")
            self.cfg.set_version(ticker, "V14")
            version = "V14"

        is_vwap_enabled = getattr(self.cfg, 'get_manual_vwap_mode', lambda x: False)(ticker)
        
        if version == "V14" and is_vwap_enabled:
            plan = self.v14_vwap_plugin.get_plan(
                ticker=ticker, current_price=current_price, avg_price=avg_price, qty=qty,
                prev_close=prev_close, ma_5day=ma_5day, market_type=market_type,
                available_cash=available_cash, is_simulation=is_simulation,
                is_snapshot_mode=is_snapshot_mode
            )
        elif version == "V_REV":
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

    # 🚨 NEW: [라우팅 누수 방어] V14 스나이퍼 감시 라우터 배선 개통 완료
    def check_sniper_condition(self, ticker, cfg, broker, chat_id):
        version = self.cfg.get_version(ticker)
        if version == "V14":
            if hasattr(self.v14_plugin, 'check_sniper_condition'):
                return self.v14_plugin.check_sniper_condition(ticker, cfg, broker, chat_id)
        return {"action": "HOLD", "reason": "스나이퍼 감시 대기(또는 모듈 없음)", "limit_price": 0.0, "qty": 0}

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

    def get_avwap_decision(self, base_ticker, exec_ticker, base_curr_p, exec_curr_p, base_day_open, avg_price, qty, alloc_cash, context_data, df_1min_base, now_est, avwap_state=None, regime_data=None, is_simulation=False, df_1min_exec=None, sortie_mode="SINGLE", **kwargs):
        # 🚨 NEW: [Case 11] 다중 출격(Multi-Sortie) 모드 파라미터 수혈 배선
        return self.v_avwap_plugin.get_decision(
            base_ticker=base_ticker, exec_ticker=exec_ticker, base_curr_p=base_curr_p, exec_curr_p=exec_curr_p, 
            base_day_open=base_day_open, avwap_avg_price=avg_price, avwap_qty=qty, avwap_alloc_cash=alloc_cash,
            context_data=context_data, df_1min_base=df_1min_base, df_1min_exec=df_1min_exec, now_est=now_est, avwap_state=avwap_state, is_simulation=is_simulation, sortie_mode=sortie_mode, **kwargs
        )
