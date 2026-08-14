# ==========================================================
# FILE: strategy.py
# ==========================================================
# 🚨 MODIFIED: [0주 오인 패러독스 차단] V-REV 플랜 생성 시 KIS 실잔고(actual_qty) 및 KIS 실평단가(actual_avg) 팩트를 직접 주입
# 🚨 MODIFIED: [스냅샷 절대주의 락온] get_plan 통로를 통해 is_snapshot_mode 파라미터 완벽 바이패스 유지
import logging
import pandas as pd
import numpy as np
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

    def _safe_float(self, val):
        try:
            f_val = float(str(val or 0.0).replace(',', ''))
            if pd.isna(f_val) or np.isinf(f_val):
                return 0.0
            return f_val
        except Exception:
            return 0.0

    def analyze_vwap_dominance(self, df):
        if df is None or len(df) < 10:
            return {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}
            
        try:
            if 'volume' not in df.columns or 'close' not in df.columns:
                return {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}

            if 'time_est' in df.columns:
                df = df[(df['time_est'] >= '093000') & (df['time_est'] <= '155900')].copy()
            
            required_cols = [c for c in ['open', 'high', 'low', 'close', 'volume'] if c in df.columns]
            df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=required_cols)
            
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
            
            df_temp['running_vwap'] = np.where(df_temp['cum_vol'] > 0, df_temp['cum_vol_price'] / df_temp['cum_vol'], 0.0)
            
            idx_10pct = int(len(df_temp) * 0.1)
            vwap_start = df_temp['running_vwap'].iloc[idx_10pct]
            vwap_end = df_temp['running_vwap'].iloc[-1]
            vwap_slope = vwap_end - vwap_start
            
            vol_above = df[df['close'].astype(float) > vwap_price]['volume'].astype(float).sum()
            vol_above_pct = vol_above / total_vol if total_vol > 0 else 0
             
            daily_open = df['open'].astype(float).iloc[0] if 'open' in df.columns else df['close'].astype(float).iloc[0]
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
        except Exception as e:
            logging.debug(f"⚠️ VWAP Dominance 분석 에러: {e}")
            return {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}

    def get_plan(self, ticker, current_price, avg_price, qty, prev_close, ma_5day=0.0, market_type="REG", available_cash=0, is_simulation=False, vwap_status=None, is_snapshot_mode=False, regime_data=None):
        if not self.cfg:
            logging.error("🚨 [FATAL] Config 객체 결측. 플랜 생성을 강제 중단하고 빈 지시서를 반환합니다.")
            return {
                'core_orders': [], 'bonus_orders': [], 'orders': [],
                't_val': 0.0, 'is_reverse': False, 'star_price': 0.0, 'one_portion': 0.0
            }

        safe_ticker = str(ticker or "").strip().upper()
        if not safe_ticker:
            return {
                'core_orders': [], 'bonus_orders': [], 'orders': [],
                't_val': 0.0, 'is_reverse': False, 'star_price': 0.0, 'one_portion': 0.0
            }

        current_price = self._safe_float(current_price)
        avg_price = self._safe_float(avg_price)
        qty = int(self._safe_float(qty))
        prev_close = self._safe_float(prev_close)
        ma_5day = self._safe_float(ma_5day)
        available_cash = self._safe_float(available_cash)

        version = str(self.cfg.get_version(safe_ticker) or "V14").strip().upper()
        
        if safe_ticker == "TQQQ" and version != "V14":
            self.cfg.set_version(safe_ticker, "V14")
            version = "V14"

        if version in ["V13", "V17", "V_VWAP", "V_AVWAP"]:
            self.cfg.set_version(safe_ticker, "V14")
            version = "V14"

        try:
             is_vwap_enabled = getattr(self.cfg, 'get_manual_vwap_mode', lambda x: False)(safe_ticker)
        except Exception:
            is_vwap_enabled = False
        
        if version == "V14" and is_vwap_enabled:
            plan = self.v14_vwap_plugin.get_plan(
                ticker=safe_ticker, current_price=current_price, avg_price=avg_price, qty=qty,
                prev_close=prev_close, ma_5day=ma_5day, market_type=market_type,
                available_cash=available_cash, is_simulation=is_simulation,
                is_snapshot_mode=is_snapshot_mode
            )
        elif version == "V_REV":
            try:
                from queue_ledger import QueueLedger
                try:
                    ql = QueueLedger()
                    q_data = ql.get_queue(safe_ticker) or []
                except Exception as q_err:
                    logging.error(f"🚨 V-REV 큐 장부 파일 로드 실패: {q_err}")
                    q_data = []
                
                # 🚨 MODIFIED: [0주 오인 패러독스 차단] KIS 실잔고(actual_qty) 및 실평단가(actual_avg) 팩트를 명시적 전달
                plan = self.v_rev_plugin.get_dynamic_plan(
                    ticker=safe_ticker, curr_p=current_price, prev_c=prev_close,
                    current_weight=1.0, vwap_status={}, min_idx=-1,
                    alloc_cash=available_cash, q_data=q_data,
                    is_snapshot_mode=is_snapshot_mode, market_type=market_type,
                    actual_qty=qty, actual_avg=avg_price 
                )
                
                plan['core_orders'] = [o for o in plan.get('orders', [])]
                plan['bonus_orders'] = []
                plan['is_reverse'] = True
                plan['t_val'] = 0.0
                plan['star_price'] = 0.0
                plan['one_portion'] = 0.0
            except Exception as e:
                logging.error(f"🚨 V-REV 플랜 생성 실패 (런타임 예외): {e}")
                plan = {
                    'core_orders': [], 'bonus_orders': [], 'orders': [],
                    't_val': 0.0, 'is_reverse': True, 'star_price': 0.0, 'one_portion': 0.0
                }
        else:
            plan = self.v14_plugin.get_plan(
                ticker=safe_ticker, current_price=current_price, avg_price=avg_price, qty=qty,
                prev_close=prev_close, ma_5day=ma_5day, market_type=market_type,
                available_cash=available_cash, is_simulation=is_simulation, vwap_status=vwap_status,
                is_snapshot_mode=is_snapshot_mode
            )
            
        return plan

    def check_sniper_condition(self, ticker, cfg, broker, chat_id):
        safe_ticker = str(ticker or "").strip().upper()
        if not safe_ticker: 
            return {"action": "HOLD", "reason": "스나이퍼 감시 대기(종목결측)", "limit_price": 0.0, "qty": 0}
        
        if not self.cfg:
            return {"action": "HOLD", "reason": "스나이퍼 감시 대기(Config 결측)", "limit_price": 0.0, "qty": 0}

        try:
            version = str(self.cfg.get_version(safe_ticker) or "V14").strip().upper()
        except Exception:
            version = "V14"

        if version == "V14":
            if hasattr(self.v14_plugin, 'check_sniper_condition'):
                return self.v14_plugin.check_sniper_condition(safe_ticker, cfg, broker, chat_id)
                
        return {"action": "HOLD", "reason": "스나이퍼 감시 대기(또는 모듈 없음)", "limit_price": 0.0, "qty": 0}

    def capture_vrev_snapshot(self, ticker, clear_price, avg_price, qty):
        safe_ticker = str(ticker or "").strip().upper()
        if not safe_ticker: return None
        
        qty = int(self._safe_float(qty))
        if qty <= 0: return None
        
        clear_price = self._safe_float(clear_price)
        avg_price = self._safe_float(avg_price)
        
        try:
            if not self.cfg: raise ValueError("Config 인스턴 결측")
            fee_rate = max(0.0, self._safe_float(self.cfg.get_fee(safe_ticker)) / 100.0)
        except Exception as e:
            logging.debug(f"⚠️ [{safe_ticker}] 수수료율 캐싱 실패. 기본값 0.07% 락온: {e}")
            fee_rate = 0.0007
            
        raw_total_buy = avg_price * qty
        raw_total_sell = clear_price * qty
        
        net_invested = raw_total_buy * (1.0 + fee_rate)
        net_revenue = raw_total_sell * (1.0 - fee_rate)
        
        realized_pnl = net_revenue - net_invested
        realized_pnl_pct = (realized_pnl / net_invested) * 100 if net_invested > 0 else 0.0
    
        return {
            "ticker": safe_ticker,
            "clear_price": clear_price,
            "avg_price": avg_price,
            "cleared_qty": qty,
            "realized_pnl": realized_pnl,
            "realized_pnl_pct": realized_pnl_pct,
            "captured_at": pd.Timestamp.now(tz=ZoneInfo('America/New_York'))
        }

    def load_avwap_state(self, ticker, now_est):
        safe_ticker = str(ticker or "").strip().upper()
        if not safe_ticker: return {}
        
        if hasattr(self.v_avwap_plugin, 'load_state'):
            return self.v_avwap_plugin.load_state(safe_ticker, now_est)
        return {}

    def save_avwap_state(self, ticker, now_est, state_data):
        safe_ticker = str(ticker or "").strip().upper()
        if not safe_ticker: return
        
        if hasattr(self.v_avwap_plugin, 'save_state'):
            self.v_avwap_plugin.save_state(safe_ticker, now_est, state_data)

    def fetch_avwap_macro(self, base_ticker):
        safe_base_ticker = str(base_ticker or "").strip().upper()
        if not safe_base_ticker: return None
        return self.v_avwap_plugin.fetch_macro_context(safe_base_ticker)

    def get_avwap_decision(self, base_ticker=None, exec_ticker=None, base_curr_p=0.0, exec_curr_p=0.0, base_day_open=0.0, avg_price=0.0, qty=0, alloc_cash=0.0, context_data=None, df_1min_base=None, df_1min_exec=None, now_est=None, avwap_state=None, regime_data=None, is_simulation=False, sortie_mode="SINGLE", **kwargs):
        safe_base_ticker = str(base_ticker or "").strip().upper()
        safe_exec_ticker = str(exec_ticker or "").strip().upper()
         
        if not safe_base_ticker or not safe_exec_ticker: return {}
        
        return self.v_avwap_plugin.get_decision(
            base_ticker=safe_base_ticker, exec_ticker=safe_exec_ticker, base_curr_p=base_curr_p, exec_curr_p=exec_curr_p, 
            base_day_open=base_day_open, avwap_avg_price=avg_price, avwap_qty=qty, avwap_alloc_cash=alloc_cash,
            context_data=context_data, df_1min_base=df_1min_base, df_1min_exec=df_1min_exec, now_est=now_est, avwap_state=avwap_state, is_simulation=is_simulation, sortie_mode=sortie_mode, **kwargs
        )
