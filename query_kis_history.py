import os
import json
from kis_order_engine import KisOrderEngine

engine = KisOrderEngine(
    os.environ["APP_KEY"],
    os.environ["APP_SECRET"],
    os.environ["CANO"],
)
ranges = [
    ("20260622", "20260630"),
    ("20260701", "20260710"),
    ("20260711", "20260720"),
    ("20260721", "20260731"),
    ("20260801", "20260811"),
]
all_items = []
for start, end in ranges:
    part = engine.get_execution_history("SOXL", start, end)
    items = list(part.values()) if isinstance(part, dict) else list(part)
    print(f"RANGE {start}-{end}={len(items)}")
    all_items.extend(items)

print(f"RAW_TOTAL={len(all_items)}")
rows = []
for idx, data in enumerate(all_items):
    item = data.get("item", data)
    qty = int(float(data.get("total_qty", item.get("ft_ccld_qty", 0))))
    if qty <= 0:
        continue
    total_amt = data.get("total_amt")
    if total_amt is not None:
        price = float(total_amt) / qty
    else:
        price = float(item.get("ft_ccld_unpr3", item.get("avg_prvs", 0)))
    side_name = str(item.get("sll_buy_dvsn_cd_name", item.get("sll_buy_dvsn_cd", "")))
    side = "BUY" if ("매수" in side_name or side_name in ("02", "2")) else "SELL"
    odno = str(item.get("odno", idx))
    rows.append({
        "date": item.get("ord_dt", item.get("ord_dt", "")),
        "side": side,
        "qty": qty,
        "price": round(price, 4),
        "odno": odno,
    })
rows.sort(key=lambda x: (x["date"], x["odno"]))
with open("data/kis_execution_history_SOXL_20260622_20260811.json", "w", encoding="utf-8") as f:
    json.dump(rows, f, ensure_ascii=False, indent=2)
print(json.dumps(rows, ensure_ascii=False))
