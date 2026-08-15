import logging
from strategy_v14 import V4Strategy

class _StubPlugin:
    """V4.0 정리: 제거된 플러그인용 빈 스텁"""
    def __getattr__(self, name): return lambda *a, **kw: ({}, None, [], 0)[0]
    async def load_daily_snapshot(self, *a, **kw): return {}
    def fetch_macro_context(self, *a, **kw): return {}
    def get_dynamic_plan(self, *a, **kw): return {}
    def get_plan(self, *a, **kw): return {}
    def get_lots(self, *a, **kw): return []

class InfiniteStrategy:
    def __init__(self, config):
        self.cfg = config
        self.v4 = V4Strategy(config)
        self.v_rev_plugin = _StubPlugin()
        self.aux_strategy_plugin = _StubPlugin()
        self.v14_slice_plugin = _StubPlugin()

    def _halt_plan(self, ticker, reason):
        trade_date = self.v4._get_logical_date_str() if getattr(self, "v4", None) else ""
        return {
            'strategy': 'LAOER_V4_SOXL_20',
            'strategy_revision': 1,
            't_revision': 0,
            'trade_date': trade_date,
            'ticker': str(ticker or '').strip().upper(),
            'core_orders': [], 'bonus_orders': [], 'orders': [],
            'total_q': 0, 'avg_price': 0.0,
            't_val': 0.0, 'is_reverse': False, 'star_price': 0.0,
            'star_ratio': 0.0, 'target_price': 0.0, 'one_portion': 0.0,
            'process_status': '⛔VERSION_HALT', 'intent_ids': [],
            'source_balance_at': '',
            'safety': {'halted': True, 'reason': reason},
        }

    def get_plan(self, ticker, current_price, avg_price, qty, prev_close,
                 ma_5day=0.0, market_type="REG", available_cash=0,
                 is_simulation=False, is_snapshot_mode=False, **kwargs):
        if not self.cfg:
            logging.error("🚨 [FATAL] Config 객체 결측. 플랜 생성 중단.")
            return self._halt_plan(ticker, "Config missing")

        safe_ticker = str(ticker or "").strip().upper()
        if not safe_ticker:
            return self._halt_plan(ticker, "Ticker missing")

        try:
            requested_version = str(self.cfg.get_version(safe_ticker) or "LAOER_V4_SOXL_20")
        except ValueError as exc:
            logging.error("⛔ [%s] unsupported legacy strategy version: %s", safe_ticker, exc)
            return self._halt_plan(safe_ticker, str(exc))
        if requested_version != "LAOER_V4_SOXL_20":
            return self._halt_plan(safe_ticker, f"Unsupported strategy version for official routing: {requested_version}")
        return self.v4.get_plan(
            ticker=safe_ticker, current_price=current_price, avg_price=avg_price,
            qty=qty, prev_close=prev_close, ma_5day=ma_5day,
            market_type=market_type, available_cash=available_cash,
            is_simulation=is_simulation, is_snapshot_mode=is_snapshot_mode,
            **kwargs,
        )

    def load_aux_state(self, *a, **kw): return {}
    def save_aux_state(self, *a, **kw): pass
    def fetch_aux_macro(self, *a, **kw): return None
    def get_aux_decision(self, *a, **kw): return {}
    def check_volatility_condition(self, *a, **kw):
        return {"action": "HOLD", "reason": "V4.0 — 변동성 제거됨", "limit_price": 0.0, "qty": 0}
