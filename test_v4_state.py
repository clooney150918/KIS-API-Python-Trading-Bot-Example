from config import ConfigManager
from strategy_v14 import V4Strategy

cfg = ConfigManager()
kis = cfg._load_json("data/kis_balance.json", {}).get("SOXL", {})
qty = int(kis.get("qty", 0))
avg = float(kis.get("avg_price", 0.0))
t, portion = cfg.get_absolute_t_val("SOXL", qty, avg)
strategy = V4Strategy(cfg)
plan = strategy.get_plan(
    "SOXL", 130.0, avg, qty, 130.0,
    ma_5day=130.0,
    available_cash=838.09,
    is_simulation=True,
    is_snapshot_mode=False,
)
print(f"qty={qty}")
print(f"avg={avg:.4f}")
print(f"t={t:.2f}")
print(f"portion={portion:.2f}")
print(f"plan_t={plan.get('t_val')}")
print(f"star_pct={plan.get('star_ratio', 0)*100:.2f}")
print(f"star_price={plan.get('star_price', 0):.2f}")
print(f"reverse={plan.get('is_reverse')}")
print(f"status={plan.get('process_status')}")
real_plan = strategy.get_plan(
    "SOXL", 130.0, avg, qty, 130.0,
    ma_5day=130.0,
    available_cash=838.09,
    is_simulation=False,
    is_snapshot_mode=True,
)
print(f"real_status={real_plan.get('process_status')}")
print(f"real_orders={len(real_plan.get('orders', []))}")
print(f"real_halted={real_plan.get('safety', {}).get('halted')}")
