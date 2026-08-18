"""cycle_cash(사이클 기준 현금) + pending_seed(입금분 격리) 회귀 테스트.

승인 설계:
  cycle_cash = baseline.available_cash + Σ매도 − Σ매수 (KIS 예수금 미사용)
  1회매수금 = cycle_cash / (20 − T)
  입금분(pending_seed) = KIS 예수금 − cycle_cash (양수일 때만) → 격리 기록, 사이클 미반영
  정합: KIS 예수금 ≈ cycle_cash + pending_seed (±$50), 초과 시 fail-closed HALT
"""

import json

from config import ConfigManager
from strategy_v14 import V4Strategy


BASELINE = {
    "schema_version": 1,
    "ticker": "SOXL",
    "as_of": "2026-08-11",
    "qty": 98,
    "avg_price": "158.0735",
    "available_cash": "1482.88",
    "t": "18.32",
    "reverse_active": False,
    "source": "CEO_APPROVED_KIS_BASELINE",
    "legacy_execution_count": 72,
    "immutable": True,
}

# 실제 원장(2026-08-11 baseline 이후) 4건: SELL 24@142.16, BUY 5@145.36,
# SELL 19@144.95, BUY 5@151.53
LEDGER_FILLS = [
    {"source": "KIS_CONFIRMED_FILL", "trade_date": "2026-08-12", "ticker": "SOXL", "exchange": "AMEX",
     "side": "SELL", "qty": 24, "price": "142.16000000", "kis_order_no": "0030744485",
     "execution_time": "224924", "fill_key": "|SOXL|AMEX|20260812|0030744485|224924|SELL|24|142.16", "confirmed": True},
    {"source": "KIS_CONFIRMED_FILL", "trade_date": "2026-08-13", "ticker": "SOXL", "exchange": "AMEX",
     "side": "BUY", "qty": 5, "price": "145.36000000", "kis_order_no": "0030935292",
     "execution_time": "200412", "fill_key": "|SOXL|AMEX|20260813|0030935292|200412|BUY|5|145.36", "confirmed": True},
    {"source": "KIS_CONFIRMED_FILL", "trade_date": "2026-08-14", "ticker": "SOXL", "exchange": "AMEX",
     "side": "SELL", "qty": 19, "price": "144.95000000", "kis_order_no": "0031222677",
     "execution_time": "195400", "fill_key": "|SOXL|AMEX|20260814|0031222677|195400|SELL|19|144.95", "confirmed": True},
    {"source": "KIS_CONFIRMED_FILL", "trade_date": "2026-08-17", "ticker": "SOXL", "exchange": "AMEX",
     "side": "BUY", "qty": 5, "price": "151.53000000", "kis_order_no": "0030008288",
     "execution_time": "170613", "fill_key": "|SOXL|AMEX|20260817|0030008288|170613|BUY|5|151.53", "confirmed": True},
]

# baseline.available_cash 1482.88 + 매도 6165.89 − 매수 1484.45 = 6164.32
EXPECTED_CYCLE_CASH_WITH_FILLS = 6164.32
BASELINE_CYCLE_CASH = 1482.88


def _hermetic_cfg(tmp_path, *, ledger_lines=None, seed=17659.0):
    """baseline을 tmp에 두면 execution_ledger/pending_seed 도 자동 격리(co-locate)된다."""
    baseline_path = tmp_path / "baseline.json"
    events_path = tmp_path / "events.jsonl"
    baseline_path.write_text(json.dumps(BASELINE), encoding="utf-8")
    events_path.write_text("", encoding="utf-8")

    cfg = ConfigManager()
    cfg.FILES["STRATEGY_BASELINE"] = str(baseline_path)
    cfg.FILES["T_EVENTS"] = str(events_path)
    cfg.FILES["T_STATE"] = str(tmp_path / "t_state.json")
    cfg.FILES["REVERSE_CFG"] = str(tmp_path / "reverse.json")
    cfg.FILES["SPLIT"] = str(tmp_path / "split.json")
    cfg.FILES["SEED_CFG"] = str(tmp_path / "seed.json")
    cfg.FILES["PROFIT_CFG"] = str(tmp_path / "profit.json")
    cfg.FILES["LOCKS"] = str(tmp_path / "locks.json")
    cfg.FILES["VERSION_CFG"] = str(tmp_path / "version.json")
    (tmp_path / "t_state.json").write_text("{}", encoding="utf-8")
    (tmp_path / "reverse.json").write_text("{}", encoding="utf-8")
    (tmp_path / "split.json").write_text(json.dumps({"SOXL": 20.0}), encoding="utf-8")
    (tmp_path / "seed.json").write_text(json.dumps({"SOXL": seed}), encoding="utf-8")
    (tmp_path / "profit.json").write_text(json.dumps({"SOXL": 12.0}), encoding="utf-8")
    (tmp_path / "locks.json").write_text("{}", encoding="utf-8")
    (tmp_path / "version.json").write_text(json.dumps({"SOXL": "LAOER_V4_SOXL_20"}), encoding="utf-8")

    # co-located 원장 파일명은 기본값 execution_ledger_SOXL.jsonl
    if ledger_lines is not None:
        (tmp_path / "execution_ledger_SOXL.jsonl").write_text(ledger_lines, encoding="utf-8")
    return cfg


def _ledger_text(fills):
    return "".join(json.dumps(f, ensure_ascii=False) + "\n" for f in fills)


# ---------------------------------------------------------------------------
# 1) cycle_cash 계산 + 멱등성
# ---------------------------------------------------------------------------
def test_cycle_cash_empty_ledger_equals_baseline(tmp_path):
    cfg = _hermetic_cfg(tmp_path)
    cycle_cash, detail = cfg.calculate_cycle_cash("SOXL")
    assert cycle_cash == BASELINE_CYCLE_CASH
    assert detail["fill_count"] == 0
    assert detail["baseline_cash"] == "1482.88"


def test_cycle_cash_matches_ledger_sums(tmp_path):
    cfg = _hermetic_cfg(tmp_path, ledger_lines=_ledger_text(LEDGER_FILLS))
    cycle_cash, detail = cfg.calculate_cycle_cash("SOXL")
    assert cycle_cash == EXPECTED_CYCLE_CASH_WITH_FILLS
    assert detail["sell_sum"] == "6165.89"
    assert detail["buy_sum"] == "1484.45"
    assert detail["fill_count"] == 4


def test_cycle_cash_is_idempotent_on_duplicate_fill_key(tmp_path):
    # 동일 fill_key 라인 중복 → 중복 합산 없이 동일 결과
    dup = _ledger_text(LEDGER_FILLS) + json.dumps(LEDGER_FILLS[0], ensure_ascii=False) + "\n"
    cfg = _hermetic_cfg(tmp_path, ledger_lines=dup)
    cycle_cash, detail = cfg.calculate_cycle_cash("SOXL")
    assert cycle_cash == EXPECTED_CYCLE_CASH_WITH_FILLS
    assert detail["fill_count"] == 4


def test_cycle_cash_fail_closed_on_corrupt_ledger(tmp_path):
    bad = _ledger_text(LEDGER_FILLS[:1]) + '{"source":"KIS_CONFIRMED_FILL","ticker":"SOXL"'  # unterminated
    cfg = _hermetic_cfg(tmp_path, ledger_lines=bad)
    cycle_cash, detail = cfg.calculate_cycle_cash("SOXL")
    assert cycle_cash is None
    assert "parse error" in detail["reason"]


def test_cycle_cash_seed_consistency_flag(tmp_path):
    # 시작시드(17659) vs baseline.available_cash+보유원가(≈16974) → ±$50 밖 → False
    cfg = _hermetic_cfg(tmp_path, seed=17659.0)
    _cc, detail = cfg.calculate_cycle_cash("SOXL")
    assert detail["seed"] == "17659.0"
    assert detail["implied_seed_at_baseline"] == "16974.08"
    assert detail["seed_consistent"] is False


# ---------------------------------------------------------------------------
# 2) 입금분(pending_seed) 격리 + 정합 HALT
# ---------------------------------------------------------------------------
def test_deposit_recorded_as_pending_seed_and_not_added_to_cycle(tmp_path):
    cfg = _hermetic_cfg(tmp_path)  # cycle_cash = 1482.88
    seed_before = (tmp_path / "seed.json").read_text(encoding="utf-8")

    # 대표님 $7,000 입금 → KIS 예수금 = 1482.88 + 7000 = 8482.88
    rec = cfg.reconcile_cycle_cash("SOXL", 8482.88)
    assert rec["ok"] is True
    assert rec["halt"] is False
    assert rec["pending_seed"] == 7000.0
    assert rec["cycle_cash"] == 1482.88

    # 격리 파일에 기록되고 seed_config 는 불변
    pending = cfg.read_pending_seed("SOXL")
    assert pending["amount"] == 7000.0
    assert pending["cycle_cash"] == 1482.88
    assert pending["kis_deposit"] == 8482.88
    assert (tmp_path / "seed.json").read_text(encoding="utf-8") == seed_before

    # cycle_cash 자체는 입금분을 흡수하지 않음
    cycle_cash, _d = cfg.calculate_cycle_cash("SOXL")
    assert cycle_cash == BASELINE_CYCLE_CASH


def test_pending_seed_record_is_idempotent(tmp_path):
    cfg = _hermetic_cfg(tmp_path)
    first = cfg.record_pending_seed("SOXL", 7000.0, 8482.88, 1482.88)
    second = cfg.record_pending_seed("SOXL", 7000.0, 8482.88, 1482.88)
    assert first["amount"] == second["amount"] == 7000.0


def test_reconcile_halts_when_ledger_cash_exceeds_deposit(tmp_path):
    # 원장이 실제 예수금보다 과다(±$50 초과) → fail-closed
    cfg = _hermetic_cfg(tmp_path)  # cycle_cash = 1482.88
    rec = cfg.reconcile_cycle_cash("SOXL", 1000.0)
    assert rec["halt"] is True
    assert rec["ok"] is False
    assert rec["discrepancy"] == 482.88


def test_reconcile_within_tolerance_is_ok_without_pending(tmp_path):
    cfg = _hermetic_cfg(tmp_path)  # cycle_cash = 1482.88
    rec = cfg.reconcile_cycle_cash("SOXL", 1500.0)  # +17.12, ±$50 이내
    assert rec["ok"] is True
    assert rec["halt"] is False
    assert rec["pending_seed"] == 0.0
    assert cfg.read_pending_seed("SOXL") == {}


# ---------------------------------------------------------------------------
# 3) get_plan 배선: 1회매수금이 cycle_cash 기반 (KIS 예수금 부풀림 무시)
# ---------------------------------------------------------------------------
def _make_strategy(cfg):
    strategy = V4Strategy(cfg)
    strategy.load_daily_snapshot = lambda ticker: None
    strategy.save_daily_snapshot = lambda ticker, data: None
    return strategy


def test_get_plan_one_portion_uses_cycle_cash_not_inflated_deposit(tmp_path):
    cfg = _hermetic_cfg(tmp_path)  # cycle_cash = 1482.88, T=18.32, denom=1.68
    strategy = _make_strategy(cfg)
    # available_cash 에 입금 부풀린 값(20000)을 넣어도 1회매수금은 cycle_cash 기준
    plan = strategy.get_plan(
        "SOXL", current_price=100.0, avg_price=158.0735, qty=98, prev_close=99.0,
        available_cash=20000.0, market_type="REG", is_snapshot_mode=False,
    )
    expected = 1482.88 / (20 - 18.32)
    assert abs(plan["one_portion"] - expected) < 1e-6
    # 부풀린 예수금 기준(20000/1.68≈11904)이 아님
    assert plan["one_portion"] < 1000.0


def test_get_plan_one_portion_reflects_ledger_fills(tmp_path):
    cfg = _hermetic_cfg(tmp_path, ledger_lines=_ledger_text(LEDGER_FILLS))
    strategy = _make_strategy(cfg)
    plan = strategy.get_plan(
        "SOXL", current_price=100.0, avg_price=158.0735, qty=98, prev_close=99.0,
        available_cash=20000.0, market_type="REG", is_snapshot_mode=False,
    )
    expected = EXPECTED_CYCLE_CASH_WITH_FILLS / (20 - 18.32)
    assert abs(plan["one_portion"] - expected) < 1e-6


def test_get_plan_fail_closed_when_cycle_cash_uncomputable(tmp_path):
    bad = '{"source":"KIS_CONFIRMED_FILL","ticker":"SOXL"'  # corrupt
    cfg = _hermetic_cfg(tmp_path, ledger_lines=bad)
    strategy = _make_strategy(cfg)
    plan = strategy.get_plan(
        "SOXL", current_price=100.0, avg_price=158.0735, qty=98, prev_close=99.0,
        available_cash=1482.88, market_type="REG", is_snapshot_mode=False,
    )
    assert plan["orders"] == []
    assert plan["process_status"].startswith("⛔")
    assert plan.get("safety", {}).get("halted") is True


def test_get_plan_snapshot_records_pending_seed_on_deposit(tmp_path):
    cfg = _hermetic_cfg(tmp_path)  # cycle_cash = 1482.88
    strategy = _make_strategy(cfg)
    # 일일 정산 스냅샷 + 입금 부풀린 예수금 → pending_seed 격리 기록
    kis_cash = (1482.88 + 7000.0) * 0.9945  # KIS 파생 가용현금 형태
    strategy.get_plan(
        "SOXL", current_price=100.0, avg_price=158.0735, qty=98, prev_close=99.0,
        available_cash=kis_cash, market_type="REG", is_snapshot_mode=True,
    )
    pending = cfg.read_pending_seed("SOXL")
    assert pending, "입금분이 격리 기록돼야 한다"
    assert pending["amount"] > 6000.0  # ≈7000 (0.9945 환산 오차 포함)
