# PRD: 진호봇 무한매수법 V4.0 순정 리팩토링

**작성일**: 2026-08-10  
**작성자**: Claude (코드베이스 분석 기반)  
**대상 브랜치**: master  
**작업 디렉토리**: `/opt/bots/soxl-trading-jinho/`

---

## 목차

1. [개요 및 배경](#1-개요-및-배경)
2. [방법론 사양 (V4.0 수식 풀스펙)](#2-방법론-사양)
3. [아키텍처 설계 (현재 → 목표)](#3-아키텍처-설계)
4. [파일별 변경 상세](#4-파일별-변경-상세)
5. [텔레그램 UI 변경 상세](#5-텔레그램-ui-변경-상세)
6. [설정 파일 변경](#6-설정-파일-변경)
7. [마이그레이션 단계](#7-마이그레이션-단계)
8. [리스크 및 주의사항](#8-리스크-및-주의사항)
9. [검증 시나리오](#9-검증-시나리오)

---

## 1. 개요 및 배경

### 1.1 목적

현재 봇은 VWAP 하이브리드, V-REV(ReversionStrategy), AVWAP(암살자), 스나이퍼 등 다수의 파생 전략 모듈이 혼재되어 있으며, 일부는 코드 내 버그(`attr_data` 미정의 등)가 있는 상태로 사실상 사용되지 않는 데드코드입니다. 이번 리팩토링의 목적:

1. **방법론 순정화**: 라오어 무한매수법 V4.0 공식만을 정확히 구현
2. **코드베이스 경량화**: 사용하지 않는 18개 모듈 완전 제거
3. **버그 수정**: `attr_data` NameError, `snap` 미정의, 별% 공식 오류, 매도수량 ceil→floor 수정
4. **유지보수성 향상**: 의존성 그래프 단순화, 플러그인 라우팅 제거
5. **사용자 경험 유지**: 텔레그램 명령어·UI 형식 최대한 보존

### 1.2 현황 파악

| 구분 | 현재 | 목표 |
|------|------|------|
| 전략 모듈 | V14 + AVWAP + V-REV + VWAP (4개) | V4.0 단독 (1개) |
| 분할수 기본값 | 40 (SOXL) | 20 (SOXL) |
| 별% 공식 | `target% - 2×T` | `N/2 - T` |
| 매도수량 | `ceil(Q/4)` | `floor(Q/4)` |
| 파일 수 | ~40개 | ~22개 (18개 삭제) |
| 스케줄러 작업 | 8개 (스나이퍼, VWAP, V-REV 포함) | 4개 (V4.0 핵심만) |

### 1.3 현재 코드 버그 목록 (즉시 수정 필요)

| # | 파일 | 위치 | 버그 내용 | 심각도 |
|---|------|------|-----------|--------|
| B1 | strategy_v14.py | 277번 줄 | `attr_data.get(...)` — `attr_data` 미정의. 리버스 모드 2일차부터 NameError 발생 | CRITICAL |
| B2 | strategy_v14.py | 218번 줄 | `snap` 미정의 가능성 — is_snapshot_mode=True + 리버스 탈출 조건 동시 충족 시 NameError | HIGH |
| B3 | strategy_v14.py | 231번 줄 | 별% = `target_pct - 2×T` (V4.0 원문: `N/2 - T`) — 잘못된 공식 | HIGH |
| B4 | strategy_v14.py | 354번 줄 | 매도수량 `math.ceil(qty/4)` (V4.0 원문: `floor(Q/4)`) | MEDIUM |
| B5 | strategy_v14.py | 354번 줄 | 전반전에도 매도 주문 생성 — V4.0 전반전은 별지점 LOC 매도 후반전 진입 전까지 매도 없음 | MEDIUM |
| B6 | config.py | DEFAULT_SPLIT | `{"SOXL": 40.0}` — V4.0 N=20이므로 20이 맞음 | MEDIUM |
| B7 | scheduler_regular.py | 19번 줄 | `from state_io_manager import read_avwap_state_sync` — 삭제 예정 파일 import | LOW |

---

## 2. 방법론 사양

### 2.1 기본 파라미터

| 기호 | 설명 | V4.0 값 |
|------|------|---------|
| N | 총 분할수 | 20 (SOXL) |
| B | 1회 매입금 (고정) | `seed / N` — 사이클 재시작 시에만 갱신 |
| T | 현재 매수 회차 (누적 투입 회수) | `(qty × avg_price) / B`, range: [0, N) |
| profit_target | 목표 수익률 | 사용자 설정 (기본 12%) |

### 2.2 핵심 공식

#### 별지점 (Star Point)

```
별% = N/2 - T          (N=20이면 10-T, T값이 커질수록 별% 감소)
별가 = ceil(avg_price × (1 + 별%/100))  [센트 단위 올림]
매수 타점 = 별가 - $0.01  (LOC 주문용)
```

예시 (avg_price=$100, T=3):
- 별% = 10 - 3 = 7%
- 별가 = ceil(100 × 1.07) = $107.00
- LOC 매수 타점 = $106.99

#### 목표가 (Target Price)

```
목표가 = ceil(avg_price × (1 + profit_target/100))
```

#### 1회 매입금 (고정)

```
B = seed / N
```

> **중요**: 대표님 선호에 따라 "매일 잔금 재계산 없음". 사이클(졸업)이 끝날 때 seed를 재설정하면 B가 갱신됨.

### 2.3 일반 매수·매도 로직

#### 새출발 (Q=0, 첫 매수)

```
매수가 = ceil(prev_close × 1.15) - $0.01  (LOC)
매수 수량 = floor(B/2 / 매수가)  [2회 분할]
```
새출발 시 매도 주문 없음.

#### 전반전 (T < N/2 = 10)

```
타점1(평단): p_avg = min(avg_price, 별가) - $0.01  (LOC)
타점2(별값): p_star = 별가 - $0.01                (LOC)

예산 분할:
  q_avg  = floor((B/2) / p_avg)
  q_star = floor((B/2) / p_star)
  
매도: 별가 LOC, floor(Q/4) 주
```

> 전반전 매도 수량이 0이면 매도 주문 생성하지 않음.

#### 후반전 (N/2 ≤ T < N-1)

```
타점: p_star = 별가 - $0.01  (LOC)
매수: q_star = floor(B / p_star)   [전액 별가 단일 타격]

매도: 별가 LOC, floor(Q/4) 주
```

#### 줍줍 (보너스 매수, 5.6절)

핵심 매수 수량 `q_base` 확정 후 추가 1~5주 LOC 사다리:

```
for n in 1..5:
    줍줍가격_n = floor(B / (q_base + n) × 100) / 100  ($0.01 단위 내림)
    if 줍줍가격_n > $0.01:
        LOC 매수 1주 @ 줍줍가격_n
```

줍줍 주문은 내림차순 가격 정렬 후 발행.

### 2.4 리버스 모드

#### 진입 조건

```
T > N-1  (T >= 20)
```

#### 1일차 (진입 당일)

```
MOC 매도: floor(Q/N) 주 (종가 시장가)
```

#### 2일차 이후

```
별가_rev = 직전 5거래일 확정 종가의 단순 평균 (ma_5day 파라미터)
  ↳ ma_5day 미수신 시: prev_close 사용

LOC 매수: floor((rem_cash/4) / (별가_rev - $0.01)) 주
LOC 매도: floor(Q/N) 주 @ 별가_rev

T_new = T_old × 0.95  (매일 5% 감소 — apply_reverse_daily_settlement에서 처리)
```

#### 리버스 탈출 조건

```
SOXL: 현재손실률 >= -20%  (loss_pct >= -20.0)
TQQQ: 현재손실률 >= -15%

탈출 시: is_rev_active = False, T·rem_cash 유지, 일반 모드로 롤오버
```

### 2.5 대박 익절 (Jackpot)

```
현재가 >= 목표가  AND  T > N-1
→ 전량 LIMIT 매도 @ 목표가
```

### 2.6 Anti-Wash Trade Shield

LOC 매수 가격이 LOC/MOC 매도 가격 이상이면 가격 강제 교정:
```
수정 후 매수가 = min(sell_price, 기존매수가) - $0.01
```
MOC 매도가 있으면 같은 봉에 LOC 매수 금지.

### 2.7 복리 (compound)

이번 PRD 범위 외. 추후 선택 기능으로 별도 분리 구현.

---

## 3. 아키텍처 설계

### 3.1 현재 아키텍처

```
main.py
├── InfiniteStrategy (strategy.py)          ← 라우터 역할
│   ├── V14Strategy (strategy_v14.py)       ← 실제 사용
│   ├── VAvwapHybridPlugin (strategy_v_avwap.py)   ← [제거]
│   ├── ReversionStrategy (strategy_reversion.py)   ← [제거]
│   └── V14VwapStrategy (strategy_v14_vwap.py)      ← [제거]
├── QueueLedger (queue_ledger.py)           ← [제거]
├── ReversionStrategy (strategy_reversion.py)       ← [제거]
├── VolatilityEngine (volatility_engine.py)         ← [제거]
├── scheduler_sniper.py                     ← [제거]
├── scheduler_vwap.py                       ← [제거]
├── scheduler_regular.py (V-REV 로직 혼재) ← [정리]
└── TelegramController (telegram_bot.py)
    └── CallbacksHandler (telegram_callbacks.py)
        ├── CallbackOrderHandler (EMERGENCY MOC 포함) ← [EMERGENCY MOC 제거]
        ├── CallbackQueueHandler (V-REV 큐)           ← [제거]
        ├── CallbackAvwapHandler (암살자)             ← [제거]
        └── CallbackConfigHandler (AVWAP/V-REV 설정) ← [V-REV/AVWAP 부분 제거]
```

**스케줄러 작업 현재 (8개)**:
```
04:00 EST  scheduled_force_reset
10:00 EST  scheduled_volatility_scan   ← [제거]
15:26 EST  scheduled_regular_trade_delayed (V-REV)  ← [제거]
15:26 EST  scheduled_vwap_init_and_cancel           ← [제거]
15:59 EST  scheduled_sniper_monitor (강제 덤핑)     ← [제거]
16:01 EST  scheduled_aftermarket_vrev_trade         ← [제거]
16:05 EST  scheduled_auto_sync
17:05 KST  scheduled_early_regular_trade (V14 LOC)  ← [유지]
매 60초    scheduled_sniper_monitor                 ← [제거]
매 60초    scheduled_vwap_trade                     ← [제거]
매 6시간   scheduled_token_check                    ← [유지]
매일 17:00 scheduled_self_cleaning                  ← [유지]
```

### 3.2 목표 아키텍처

```
main.py
├── V4Strategy (strategy_v14.py → 클래스명 변경)   ← 직결, 라우터 없음
├── broker.py                                        ← 유지
├── scheduler_core.py                                ← 유지 (V-REV 부분만 제거)
├── scheduler_regular.py                             ← 정리 (V4.0 LOC 트리거만)
└── TelegramController (telegram_bot.py)
    └── CallbacksHandler (telegram_callbacks.py)
        ├── CallbackOrderHandler                     ← EMERGENCY MOC 제거
        └── CallbackConfigHandler                    ← AVWAP/V-REV 항목 제거
```

**스케줄러 작업 목표 (5개)**:
```
04:00 EST  scheduled_force_reset
16:05 EST  scheduled_auto_sync
17:05 KST  scheduled_early_regular_trade (V4.0 LOC 장전)
매 6시간   scheduled_token_check
매일 17:00 scheduled_self_cleaning
```

### 3.3 데이터 흐름 (목표)

```
17:05 KST 트리거
  → broker.get_holdings() [KIS API]
  → V4Strategy.get_plan(is_snapshot_mode=True) [스냅샷 생성 + 플랜 계산]
  → execute_order_list() [LOC/MOC 주문 전송]
  → 텔레그램 통보

16:05 EST 트리거
  → broker.sync_account() [KIS 실계좌 정산]
  → TelegramSyncEngine.run_settlement() [졸업 판정]
  → 텔레그램 통보
```

---

## 4. 파일별 변경 상세

### 4.1 삭제 파일 (18개)

이 파일들은 빈 스텁으로 대체하거나 단계적으로 삭제. 스텁 형태 예시:

```python
# STUB: 이 모듈은 V4.0 리팩토링으로 제거됨
raise ImportError("이 모듈은 삭제되었습니다.")
```

| 파일 | 이유 |
|------|------|
| `strategy_v_avwap.py` | AVWAP(암살자) 전략 — V4.0 미사용 |
| `strategy_reversion.py` | V-REV 전략 — V4.0 미사용 |
| `strategy_v14_vwap.py` | V14+VWAP 하이브리드 — V4.0 미사용 |
| `scheduler_sniper.py` | 스나이퍼 모니터 — V4.0 미사용 |
| `scheduler_vwap.py` | VWAP/V-REV 애프터장 스케줄 — V4.0 미사용 |
| `vwap_core_engine.py` | VWAP 계산 엔진 — 삭제 |
| `vwap_aftermarket_engine.py` | 애프터장 VWAP — 삭제 |
| `callback_avwap_handler.py` | 암살자 콜백 핸들러 — 삭제 |
| `telegram_avwap_console.py` | 암살자 관제탑 UI — 삭제 |
| `queue_ledger.py` | V-REV LIFO 큐 — V4.0 미사용 |
| `assassin_ledger.py` | 암살자 독립 장부 — 삭제 |
| `short_squeeze_engine.py` | 숏 스퀴즈 탐지 — 삭제 |
| `volatility_engine.py` | 변동성·시장국면 엔진 — V4.0 미사용 |
| `state_io_manager.py` | AVWAP 상태 I/O — 삭제 |
| `mergy.py` | 장부 병합 유틸리티 — 삭제 |
| `rescue_bot.py` | 복구 봇 — 삭제 |
| `plugin_updater.py` | 자가 업데이트 플러그인 — 삭제 |
| `kis_check.py` | KIS 연결 점검 스크립트 — 삭제 |

### 4.2 수정 파일 상세

---

#### `strategy_v14.py` → 클래스명 `V4Strategy`로 변경

**변경 1: B1 버그 수정 — `attr_data` NameError (line 277)**

```python
# BEFORE (broken):
rev_star = self._safe_float(attr_data.get('close_5day_avg', 0.0))

# AFTER (V4.0 원문: 직전 5거래일 평균 종가):
rev_star = ma_5day if ma_5day > 0.0 else (prev_close if prev_close > 0.0 else current_price)
```

**변경 2: B2 버그 수정 — `snap` 미정의 가능성 (함수 최상단)**

```python
def get_plan(self, ...):
    snap = None   # ← 초기화 추가 (함수 최상단)
    ...
```

**변경 3: B3 버그 수정 — 별% 공식 (line 231)**

```python
# BEFORE (잘못된 공식):
star_ratio_percent = target_pct_val - 2.0 * t_val
star_ratio = star_ratio_percent / 100.0

# AFTER (V4.0 원문: 별% = N/2 - T):
star_ratio_percent = (split / 2.0) - t_val   # N=split=20, 별% = 10 - T
star_ratio = star_ratio_percent / 100.0
```

> 이 수식 변경으로 인해 별가가 달라짐. 기존 T값이 유지된 상태에서 적용 시 매수/매도 타점이 변경됨. §8.1 참조.

**변경 4: B4 버그 수정 — 매도수량 ceil→floor (line 354)**

```python
# BEFORE:
q_sell = math.ceil(qty / 4)

# AFTER (V4.0 원문: floor(Q/4)):
q_sell = math.floor(qty / 4)
```

**변경 5: B5 버그 수정 — 전반전 매도 조건 추가**

전반전(T < N/2)에서는 V4.0 원문상 별지점 LOC 매도가 있으나, 매도 수량이 0이면 주문을 생성하지 않음:

```python
# 기존: 조건 없이 매도 주문 생성
# 수정: q_sell == 0 시 매도 주문 생략 (기존 코드에는 q_sell > 0 조건이 이미 있음 — OK)
```

**변경 6: DEFAULT_SPLIT 반영 — split 기본값 처리**

```python
split = self._safe_float(self.cfg.get_split_count(ticker))
if split <= 0: split = 20.0  # V4.0: N=20 (기존 40 → 20)
```

**변경 7: 클래스명 변경**

```python
# BEFORE:
class V14Strategy:

# AFTER:
class V4Strategy:
```

**변경 8: 스냅샷 파일명 변경**

```python
# BEFORE:
snap_file = f"data/daily_snapshot_V14_{today_str}_{ticker}.json"

# AFTER:
snap_file = f"data/daily_snapshot_V4_{today_str}_{ticker}.json"
```

> 기존 스냅샷과의 연속성을 위해 마이그레이션 스크립트 작성 필요. §7 참조.

---

#### `strategy.py` — InfiniteStrategy 단순화

**변경: 플러그인 라우터 제거, V4Strategy 직결**

```python
# BEFORE (라우터):
from strategy_v14 import V14Strategy
from strategy_v_avwap import VAvwapHybridPlugin
from strategy_reversion import ReversionStrategy
from strategy_v14_vwap import V14VwapStrategy

class InfiniteStrategy:
    def __init__(self, config):
        self.v14_plugin = V14Strategy(config)
        self.v_avwap_plugin = VAvwapHybridPlugin()
        self.v_rev_plugin = ReversionStrategy(config)
        self.v14_vwap_plugin = V14VwapStrategy(config)
    
    def get_plan(self, ...):
        version = self.cfg.get_version(ticker)
        if version == "V14" and is_vwap_enabled: → v14_vwap_plugin
        elif version == "V_REV": → v_rev_plugin
        else: → v14_plugin

# AFTER (직결):
from strategy_v14 import V4Strategy

class InfiniteStrategy:
    def __init__(self, config):
        self.cfg = config
        self.v4 = V4Strategy(config)

    def get_plan(self, ticker, current_price, avg_price, qty, prev_close,
                 ma_5day=0.0, market_type="REG", available_cash=0,
                 is_simulation=False, is_snapshot_mode=False, **kwargs):
        # 버전 강제 V4.0으로 고정
        self.cfg.set_version(ticker, "V4")
        return self.v4.get_plan(
            ticker=ticker, current_price=current_price, avg_price=avg_price,
            qty=qty, prev_close=prev_close, ma_5day=ma_5day,
            market_type=market_type, available_cash=available_cash,
            is_simulation=is_simulation, is_snapshot_mode=is_snapshot_mode
        )
```

**제거 메서드**: `analyze_vwap_dominance()`, `check_sniper_condition()`, `capture_vrev_snapshot()`, `load_avwap_state()`, `save_avwap_state()`, `fetch_avwap_macro()`, `get_avwap_decision()`

---

#### `config.py` — 설정 정리

**변경 1: FILES 딕셔너리 — 삭제 파일 경로 제거**

```python
# BEFORE (30개 항목):
self.FILES = {
    ...
    "UPWARD_SNIPER": "data/upward_sniper.json",
    "AVWAP_HYBRID_CFG": "data/avwap_hybrid.json",
    "AVWAP_SORTIE_CFG": "data/avwap_sortie.json",
    "MANUAL_VWAP_CFG": "data/manual_vwap_config.json",
    "SNIPER_MULTIPLIER_CFG": "data/sniper_multiplier.json",
    "SNIPER_BUY_LOCKED": "data/sniper_buy_locked.json",
    "SNIPER_SELL_LOCKED": "data/sniper_sell_locked.json",
    "VREV_GAP_SWITCH_CFG": "data/vrev_gap_switch.json",
    "VREV_GAP_THRESH_CFG": "data/vrev_gap_thresh.json",
    "AVWAP_GAP_THRESH_CFG": "data/avwap_gap_thresh.json",
    "AVWAP_ANCHOR_CFG": "data/avwap_anchor.json",
    "AVWAP_BUDGET_CFG": "data/avwap_budget.json",
    "AVWAP_OVERNIGHT_CFG": "data/avwap_overnight.json",
    ...
}

# AFTER (핵심만):
self.FILES = {
    "TOKEN": "data/token.dat",
    "CHAT_ID": "data/chat_id.dat",
    "LEDGER": "data/manual_ledger.json",
    "HISTORY": "data/manual_history.json",
    "SPLIT": "data/split_config.json",
    "TICKER": "data/active_tickers.json",
    "SECRET_MODE": "data/secret_mode.dat",
    "PROFIT_CFG": "data/profit_config.json",
    "LOCKS": "data/trade_locks.json",
    "SEED_CFG": "data/seed_config.json",
    "COMPOUND_CFG": "data/compound_config.json",
    "VERSION_CFG": "data/version_config.json",
    "REVERSE_CFG": "data/reverse_config.json",
    "FEE_CFG": "data/fee_config.json",
    "MASTER_SWITCH": "data/master_switch.json",
    "SPLIT_HISTORY": "data/split_history.json",
}
```

**변경 2: DEFAULT_SPLIT 수정**

```python
# BEFORE:
self.DEFAULT_SPLIT = {"SOXL": 40.0, "TQQQ": 40.0}

# AFTER (V4.0: N=20):
self.DEFAULT_SPLIT = {"SOXL": 20.0, "TQQQ": 20.0}
```

**변경 3: VWAP_PROFILES 테이블 제거**

`VWAP_PROFILES` 딕셔너리 전체 삭제 (400줄 절감).

**변경 4: 제거 메서드**

```
get_vwap_profile()
get_manual_vwap_mode() / set_manual_vwap_mode()
get_sniper_multiplier() / set_sniper_multiplier()
get_avwap_hybrid_cfg() / set_avwap_hybrid_cfg()
get_avwap_budget() / set_avwap_budget()
get_avwap_overnight() / set_avwap_overnight()
get_avwap_anchor() / set_avwap_anchor()
get_vrev_gap_switch() / set_vrev_gap_switch()
```

**유지 메서드 (V4.0 필수)**:

```
get_seed() / set_seed()
get_split_count() / set_split_count()
get_target_profit() / set_target_profit()
get_fee() / set_fee()
get_version() / set_version()
get_reverse_state() / set_reverse_state()
apply_reverse_daily_settlement()
get_active_tickers() / set_active_tickers()
get_ledger() / save_ledger()
get_history() / save_history()
get_compound() / set_compound()  (미래 복리 기능용 stub 유지)
```

---

#### `main.py` — 스케줄러·import 정리

**제거 import**:

```python
# BEFORE (제거 대상):
from queue_ledger import QueueLedger
from strategy_reversion import ReversionStrategy
from volatility_engine import VolatilityEngine, determine_market_regime
from scheduler_sniper import scheduled_sniper_monitor
from scheduler_vwap import scheduled_vwap_trade, scheduled_vwap_init_and_cancel, scheduled_aftermarket_vrev_trade

# AFTER (유지):
from scheduler_core import (scheduled_token_check, scheduled_auto_sync,
                             scheduled_force_reset, scheduled_self_cleaning,
                             perform_self_cleaning, is_market_open)
from scheduler_regular import scheduled_early_regular_trade
```

**제거 app_data 항목**:

```python
# BEFORE:
queue_ledger = QueueLedger()
strategy_rev = ReversionStrategy(cfg)
app_data = {'cfg': ..., 'queue_ledger': queue_ledger, 'strategy_rev': strategy_rev, ...}

# AFTER:
app_data = {'cfg': ..., 'broker': ..., 'strategy': ..., 'bot': ..., 'tx_lock': None, 'tz_est': est_zone}
```

**제거 스케줄 등록**:

```python
# 제거:
jq.run_daily(scheduled_volatility_scan, ...)
jq.run_daily(scheduled_regular_trade_delayed, ...)       # V-REV
jq.run_daily(scheduled_vwap_init_and_cancel, ...)        # VWAP
jq.run_daily(scheduled_sniper_monitor, ...)              # 15:59 스나이퍼 덤핑
jq.run_daily(scheduled_aftermarket_vrev_trade, ...)      # 16:01 V-REV 애프터장
jq.run_repeating(scheduled_sniper_monitor, ...)          # 60초 스나이퍼 모니터
jq.run_repeating(scheduled_vwap_trade, ...)              # 60초 VWAP 거래
```

**제거 CommandHandler 등록**:

```python
# 제거:
("avwap", bot.cmd_avwap),
("queue", bot.cmd_queue),
("add_q", bot.cmd_add_q),
("clear_q", bot.cmd_clear_q),
```

**제거 post_init Lock 전파**:

```python
# 제거:
application.bot_data['bot_controller'].callbacks_handler.avwap_handler.tx_lock = tx_lock
```

**제거 `scheduled_volatility_scan` 함수 전체** (main.py 내 인라인 정의된 98-201번 줄).

---

#### `scheduler_regular.py` — V4.0 전용 정리

**제거**:

```python
# BEFORE:
from state_io_manager import read_avwap_state_sync  # ← 삭제

# scheduled_early_regular_trade 내부:
avwap_state = read_avwap_state_sync(ticker, now_est)  # ← 삭제
# avwap 관련 조건 분기 전체 삭제
```

**제거 함수**:
- `scheduled_regular_trade_delayed()` 전체 삭제 (V-REV 본진 지연 엔진)
- 자본잠김이관(capital lock-up) 관련 로직 삭제

**유지 함수**:
- `scheduled_early_regular_trade()` — V4.0 LOC 장전 핵심 트리거 (17:05 KST)

---

#### `scheduler_core.py` — V-REV 코드 제거

**제거**:

- `get_budget_allocation()` 내부 V-REV 큐 조회 로직
- 리버스(V-REV) 모드 탐지 분기
- `VolatilityEngine` import 및 사용

**유지**:

- `is_market_open()`, `is_market_active_now()`
- `scheduled_token_check()`, `scheduled_auto_sync()`, `scheduled_force_reset()`, `scheduled_self_cleaning()`
- `perform_self_cleaning()`, `get_budget_allocation()` (V-REV 없는 순수 예산 배분)
- 16:05 EST 졸업 스캔망 (Scenario 1, 3)
- 04:00 EST 락 초기화

---

#### `telegram_commands.py` — AVWAP 명령어 제거

**제거 CommandHandler 메서드**:

```python
async def cmd_avwap(self, ...)  # AVWAP 관제탑 콘솔 전체
async def cmd_queue(self, ...)  # V-REV 큐 관리
async def cmd_add_q(self, ...)  # V-REV 큐 수동 추가
async def cmd_clear_q(self, ...) # V-REV 큐 초기화
```

**유지 CommandHandler 메서드**:

```python
cmd_start, cmd_sync, cmd_record, cmd_history
cmd_settlement, cmd_seed, cmd_ticker
cmd_mode  ← 스나이퍼 대신 "수동 개입 모드" 등으로 용도 재정의 가능
cmd_version, cmd_update, cmd_reset, cmd_log
```

**`/mode` 명령어 재정의**:

기존: "상방 스나이퍼 ON/OFF"  
변경: V4.0에서 불필요하므로 **숨김 처리 또는 "테스트 모드"로 재정의**.  
단, 텔레그램 명령어 목록에서는 제거하고 미응답 처리.

---

#### `telegram_view.py` — V-REV·AVWAP·스나이퍼 UI 제거

**변경 1: `get_start_message()` — 운영 스케줄·명령어 목록 정리**

```
제거 항목:
  🔹 04:00: 프리장 VWAP 스캔 개시
  🔹 09:30: 정규장 VWAP 초기화 및 스캔
  🔹 15:27: V-REV 1분 슬라이싱 타격
  🔹 15:59: 암살자 오버나이트 강제 덤핑
  ▶️ /avwap : 트레이딩 레이더 관제탑

추가 항목:
  🔹 17:05 KST: V4.0 LOC 장전 및 스냅샷 박제
  🔹 16:05 EST: 정산 스캔 & 당일 사이클 졸업

버전 표기 변경:
  "무한매수법 V14" → "무한매수법 V4.0"
  "옴니 매트릭스 퀀트 엔진" → "무한매수법 V4.0 봇"
```

**변경 2: `get_reset_menu()` — 암살자 버튼 제거**

```python
# BEFORE:
keyboard.append([
    InlineKeyboardButton(f"🔫 {safe_t} 암살자 장부 초기화", callback_data=f"RESET:AVWAP:{t}")
])

# AFTER: 이 줄 삭제
```

**변경 3: `get_queue_management_menu()` — 전체 삭제**

V-REV 큐 관리 메뉴 자체가 V4.0에 불필요.

**변경 4: `get_queue_action_confirm_menu()` — 삭제**

**변경 5: `get_emergency_moc_confirm_menu()` — 삭제**

**변경 6: `create_ledger_dashboard()` — V-REV 장부 섹션 제거**

장부 대시보드에서 V-REV 큐(LIFO) 표시 섹션 삭제.

**변경 7: settlement 메뉴 — AVWAP/스나이퍼 버튼 제거**

```
제거: 🔫 암살자 1회 타격 예산 버튼
제거: 🌙 오버나이트 토글 버튼
제거: V-REV 버전 선택 버튼 (SET_VER:V_REV)
제거: AVWAP 모드 관련 버튼
```

---

#### `telegram_bot.py` — 의존성 정리

**변경 1: 생성자 파라미터 제거**

```python
# BEFORE:
class TelegramController:
    def __init__(self, cfg, broker, strategy, tx_lock=None,
                 queue_ledger=None, strategy_rev=None):

# AFTER:
class TelegramController:
    def __init__(self, cfg, broker, strategy, tx_lock=None):
```

**변경 2: avwap_handler 제거**

```python
# BEFORE:
from callback_avwap_handler import CallbackAvwapHandler
self.avwap_handler = CallbackAvwapHandler(...)

# AFTER: 삭제
```

---

#### `telegram_callbacks.py` — AVWAP/QUEUE 라우팅 제거

```python
# BEFORE:
if data.startswith(("QUEUE:", "DEL_REQ:", "EDIT_Q:", "DEL_Q:")):
    await self.queue_handler.handle(...)
elif data.startswith(("EMERGENCY_REQ:", "EMERGENCY_CONFIRM:")):
    await self.order_handler.handle_emergency(...)
elif data.startswith(("AVWAP:", "MODE:")):
    await self.avwap_handler.handle(...)

# AFTER: 위 세 분기 삭제
```

**유지 라우팅**:

```python
EXEC:, MANUAL_PORTION:  → CallbackOrderHandler
SET_VER:, INPUT:, CONFIG:, RESET:  → CallbackConfigHandler
```

---

#### `callback_config_handler.py` — AVWAP/V-REV 콜백 제거

**제거**:

```python
# SET_VER:V_REV 처리 분기
# CONFIG:AVWAP_BUDGET, CONFIG:OVERNIGHT 처리 분기
# RESET:AVWAP, RESET:AVWAP_CONFIRM 처리 분기
# get_avwap_reset_confirm_menu() 호출부
# assassin_ledger import 및 참조
```

---

#### `callback_order_handler.py` — EMERGENCY MOC 제거

```python
# BEFORE:
async def handle_emergency(self, query, app_data):
    # V-REV 큐 1지층 긴급 MOC 매도

# AFTER: 메서드 전체 삭제
```

> EMERGENCY MOC는 V-REV 큐의 최상단 지층을 MOC로 강제 청산하는 기능으로, V4.0에 큐 개념이 없으므로 제거.

---

#### `callback_queue_handler.py` — 전체 스텁화

```python
# V4.0 리팩토링으로 V-REV 큐 기능 제거됨
class CallbackQueueHandler:
    async def handle(self, *args, **kwargs):
        pass
```

---

#### `order_executor.py` — 자본잠김이관 제거

**제거**:

- `is_capital_locked` 감지 로직
- 자본잠김 시 V-REV 계획 이관 분기
- `strategy_rev` 파라미터 참조

**유지**:

- `execute_order_list()` — LOC/LIMIT/MOC 주문 전송
- 주문 결과 로깅 및 텔레그램 통보

---

#### `telegram_states.py` — V-REV 큐 편집 상태 제거

**제거**:

- `EDIT_QUEUE_LAYER` 상태
- 큐 지층 수동 편집 핸들러

**유지**:

- SEED, SPLIT, TARGET, COMPOUND, FEE 설정 입력 핸들러
- snapshot/slice 상태 파일 초기화 핸들러

---

### 4.3 유지 파일 (무변경)

| 파일 | 역할 |
|------|------|
| `broker.py` | KisOrderEngine 파사드 |
| `kis_api_client.py` | KIS REST API 클라이언트 |
| `kis_order_engine.py` | 주문 실행 엔진 |
| `global_throttle.py` | API TPS 제어 + 파일 뮤텍스 |
| `market_data_provider.py` | YFinance 시세 제공자 |
| `telegram_sync_engine.py` | 16:05 KIS 실계좌 정산 엔진 |

---

## 5. 텔레그램 UI 변경 상세

### 5.1 제거 명령어

| 명령어 | 현재 기능 | 처리 방식 |
|--------|-----------|-----------|
| `/avwap` | AVWAP 관제탑 | 삭제 |
| `/queue` | V-REV 큐 관리 | 삭제 |
| `/add_q` | V-REV 큐 지층 추가 | 삭제 |
| `/clear_q` | V-REV 큐 초기화 | 삭제 |

### 5.2 유지 명령어 (V4.0 용도 맞게 설명 수정)

| 명령어 | 현재 설명 | 변경 후 설명 |
|--------|-----------|-------------|
| `/start` | 옴니 매트릭스 퀀트 엔진 | 무한매수법 V4.0 봇 시작 |
| `/sync` | 통합 지시서 조회 | V4.0 지시서 조회 (변경 없음) |
| `/settlement` | 코어스위칭/전술설정 | 설정 관리 (V-REV/AVWAP 항목 제거) |
| `/mode` | 상방 스나이퍼 ON/OFF | 제거 또는 미응답 처리 |
| `/version` | 버전 및 업데이트 내역 | "무한매수법 V4.0" 표기 |

### 5.3 제거 인라인 버튼

| 버튼 | 위치 | 처리 |
|------|------|------|
| `🔫 {ticker} 암살자 장부 초기화` | /reset 메뉴 | 삭제 |
| `🩸 1지층 수동 긴급 수혈 (MOC)` | /queue 메뉴 | 메뉴 자체 삭제 |
| `✏️ N지층 수정 / 🗑️ N지층 삭제` | /queue 메뉴 | 메뉴 자체 삭제 |
| `🔫 암살자 1회 타격 예산` | /settlement 메뉴 | 삭제 |
| `🌙 오버나이트 토글` | /settlement 메뉴 | 삭제 |
| `V-REV 버전 선택` | /settlement 버전 메뉴 | 삭제 |

### 5.4 문자열 치환 목록

| 변경 전 | 변경 후 | 적용 파일 |
|---------|---------|----------|
| `무한매수법 V14` | `무한매수법 V4.0` | telegram_view.py |
| `옴니 매트릭스 퀀트 엔진` | `무한매수법 V4.0 봇` | telegram_view.py |
| `V14` (버전 표기) | `V4.0` | telegram_view.py |
| `V-REV 큐` | (제거) | telegram_view.py |
| `암살자` | (제거) | telegram_view.py |
| `스나이퍼` | (제거) | telegram_view.py |
| `VWAP` | (제거) | telegram_view.py |

---

## 6. 설정 파일 변경

### 6.1 data/ 파일 처리 방침

| 파일 | 처리 |
|------|------|
| `data/seed_config.json` | 유지 |
| `data/split_config.json` | 유지 — 값이 40이면 20으로 마이그레이션 |
| `data/version_config.json` | 유지 — 값 "V14" → "V4" 로 마이그레이션 |
| `data/reverse_config.json` | 유지 |
| `data/manual_ledger.json` | 유지 |
| `data/manual_history.json` | 유지 |
| `data/profit_config.json` | 유지 |
| `data/fee_config.json` | 유지 |
| `data/compound_config.json` | 유지 (복리 미구현이지만 데이터 보존) |
| `data/trade_locks.json` | 유지 |
| `data/active_tickers.json` | 유지 |
| `data/daily_snapshot_V14_*.json` | §7.2 마이그레이션 스크립트로 V4_ 복사 |
| `data/avwap_*.json` | 삭제 가능 (V4.0 미사용) |
| `data/upward_sniper.json` | 삭제 가능 |
| `data/sniper_*.json` | 삭제 가능 |
| `data/vrev_*.json` | 삭제 가능 |
| `data/queue_*.json` | 삭제 가능 (V-REV 큐 데이터) |
| `data/assassin_*.json` | 삭제 가능 |

### 6.2 split_config.json 마이그레이션

기존 값이 40인 경우 20으로 자동 변환:

```python
# 마이그레이션 스크립트 (별도 실행):
split_cfg = cfg._load_json("data/split_config.json", {})
for ticker in split_cfg:
    if split_cfg[ticker] >= 40.0:
        split_cfg[ticker] = 20.0  # V4.0: N=20
cfg._save_json("data/split_config.json", split_cfg)
```

> ⚠️ T값이 split에 반비례하므로, split을 40→20으로 줄이면 T값이 2배로 계산됨. 기존 보유 포지션이 있는 경우 T값 수동 교정 필요. §8.2 참조.

---

## 7. 마이그레이션 단계

### Phase 1: 사전 백업 (0일차)

```bash
# 전체 데이터 백업
cp -r /opt/bots/soxl-trading-jinho/data/ /opt/bots/soxl-trading-jinho/data_backup_$(date +%Y%m%d)/

# 현재 스냅샷 백업
cp /opt/bots/soxl-trading-jinho/data/daily_snapshot_V14_*.json /tmp/

# Docker 컨테이너 정지
docker stop soxl-trading-jinho  # 또는 해당 컨테이너명
```

### Phase 2: 삭제 대상 스텁화 (1일차)

삭제 파일들을 즉시 제거하는 대신 먼저 ImportError 스텁으로 교체. 런타임 오류로 인한 누락 참조 발견.

```bash
# 스텁화할 파일 목록
for f in strategy_v_avwap strategy_reversion strategy_v14_vwap \
          scheduler_sniper scheduler_vwap vwap_core_engine \
          vwap_aftermarket_engine callback_avwap_handler \
          telegram_avwap_console queue_ledger assassin_ledger \
          short_squeeze_engine volatility_engine state_io_manager \
          mergy rescue_bot plugin_updater kis_check; do
  echo "# STUB: V4.0 리팩토링으로 제거됨" > /opt/bots/soxl-trading-jinho/${f}.py
done
```

### Phase 3: 핵심 버그 수정 (1일차 — 즉시)

**B1 수정** (`strategy_v14.py:277`):

```python
# 기존:
rev_star = self._safe_float(attr_data.get('close_5day_avg', 0.0))
# 수정:
rev_star = ma_5day if ma_5day > 0.0 else (prev_close if prev_close > 0.0 else current_price)
```

**B2 수정** (함수 최상단):

```python
def get_plan(self, ...):
    snap = None  # 추가
```

### Phase 4: V4.0 공식 적용 (2일차)

1. `strategy_v14.py` 별% 공식 수정 (B3)
2. `strategy_v14.py` 매도수량 ceil→floor (B4)
3. `config.py` DEFAULT_SPLIT 40→20 수정 (B6)
4. `strategy_v14.py` 클래스명 V14Strategy → V4Strategy

### Phase 5: import 정리 (2일차)

1. `strategy.py` 플러그인 import 제거, V4Strategy 직결
2. `main.py` 삭제 모듈 import 제거
3. `scheduler_regular.py` `state_io_manager` import 제거

### Phase 6: 스케줄러 정리 (3일차)

1. `main.py` 불필요 스케줄 등록 삭제
2. `scheduler_core.py` V-REV 코드 제거
3. `scheduler_regular.py` V-REV/AVWAP 분기 제거

### Phase 7: 텔레그램 정리 (3일차)

1. `telegram_view.py` UI 문자열·버튼 정리
2. `telegram_commands.py` AVWAP 명령어 제거
3. `telegram_bot.py` 생성자 파라미터 정리
4. `telegram_callbacks.py` AVWAP/QUEUE 라우팅 제거
5. `callback_config_handler.py` AVWAP/V-REV 콜백 제거

### Phase 8: 스냅샷 파일명 마이그레이션 (4일차 — 운용 재개 전)

```python
# 마이그레이션 스크립트 (one-time):
import os, shutil, glob

for old_path in glob.glob("data/daily_snapshot_V14_*.json"):
    new_path = old_path.replace("daily_snapshot_V14_", "daily_snapshot_V4_")
    shutil.copy2(old_path, new_path)
    print(f"복사: {old_path} → {new_path}")
```

### Phase 9: 검증 및 Docker 재기동 (4일차)

1. `python3 -c "from strategy_v14 import V4Strategy; print('OK')"` 실행
2. `python3 -c "from strategy import InfiniteStrategy; print('OK')"` 실행
3. `python3 -c "from main import main; print('OK')"` 실행 (import only)
4. Docker 컨테이너 재기동
5. 텔레그램 `/start` 응답 확인
6. 텔레그램 `/sync` 지시서 확인

### Phase 10: 구버전 파일 정리 (5일차 이후)

1. 스텁 파일들을 완전히 삭제
2. `data_backup_*` 폴더 정리
3. 구버전 스냅샷 파일(`daily_snapshot_V14_*`) 삭제 검토

---

## 8. 리스크 및 주의사항

### 8.1 별% 공식 변경에 따른 타점 변화

**리스크**: `target% - 2T` → `N/2 - T` 공식 변경 후 기존 포지션의 별가가 크게 달라질 수 있음.

예시 (avg_price=$100, T=5, target%=12%, N=20):
- 기존: 별% = 12 - 2×5 = 2% → 별가 = $102
- V4.0: 별% = 10 - 5 = 5% → 별가 = $105

**대응**:
- 공식 변경 전 대표님께 타점 변화를 설명하고 동의 후 적용
- 변경 전일 스냅샷의 star_price를 수동으로 확인
- 필요 시 `data/daily_snapshot_V4_*.json`을 직접 편집하여 조정 가능

### 8.2 DEFAULT_SPLIT 변경에 따른 T값 변화

**리스크**: split 40→20 변경 시 T값 계산: `T = (qty × avg) / (seed/N)`. split이 20이 되면 portion이 2배 커지므로 T값이 2배 작아짐.

예시 (qty=10, avg=$100, seed=$6720):
- split=40: portion=$168, T = 10×100/168 ≈ 5.95 (후반전)
- split=20: portion=$336, T = 10×100/336 ≈ 2.98 (전반전)

**대응**:
- `data/split_config.json`을 변경하기 전에 현재 T값 계산하여 기록
- 기존 split=40 포지션이 있다면 split은 40으로 유지하고 V4.0 수식(별% 공식, 매도 floor)만 적용하는 것도 고려
- **권장**: 현재 사이클 졸업 후 새 사이클 시작 시 split=20 적용

### 8.3 리버스 모드 중 적용 시 주의

리버스 모드가 활성화된 상태에서 리팩토링을 적용할 경우:

- `attr_data` 버그(B1) 수정은 즉시 적용 (현재 CRITICAL 버그)
- 별% 공식 변경은 리버스 모드에서는 영향 없음 (리버스는 ma_5day 기반)
- `DEFAULT_SPLIT` 변경은 리버스 탈출 후 재계산 시 영향

### 8.4 /queue, /avwap 제거 후 잔여 데이터

- `data/queue_*.json`, `data/avwap_*.json` 파일은 텔레그램 UI 제거 후에도 data/ 폴더에 잔존
- 즉각 삭제하지 않고 보존 권장 (재활성화 가능성 대비)
- 30일 후 수동 삭제

### 8.5 EMERGENCY MOC 제거 후 긴급 청산 수단

기존 EMERGENCY MOC는 V-REV 큐의 최상단 지층을 긴급 청산하는 수단이었음. V4.0에는 큐 개념이 없으나, 포지션 전체 청산이 필요한 경우:

- `/reset → 🔥 장부 영구 소각` 후 HTS 수동 청산 절차 안내
- 또는 callback_order_handler.py에 "전량 MOC 매도" 버튼 신규 추가 검토 (별도 스프린트)

---

## 9. 검증 시나리오

### 9.1 새출발 시나리오 (Q=0)

**입력값**: ticker=SOXL, qty=0, prev_close=$25.00, seed=$6720, split=20

**기대값**:
```
B = $6720 / 20 = $336
매수가 = ceil($25 × 1.15) - $0.01 = ceil($28.75) - $0.01 = $28.99
q1 = floor(($336/2) / $28.99) = floor($168 / $28.99) = 5주
q2 = floor(($336 - $168) / $28.99) = 5주
→ LOC 매수 5주 @$28.99 × 2건
→ 줍줍: n=1~5, 줍줍가 = floor($336/(5+n)×100)/100
  n=1: floor($336/6×100)/100 = $56.00 → 1주 LOC @$56.00 (but $56 > $28.99, 워시트레이드 쉴드에 의해 제거됨)
  실제로는 new_buy_price 조정됨
```

**검증 포인트**: 매도 주문 없음 확인, LOC 2건 + 줍줍 사다리 생성 확인.

### 9.2 전반전 매수·매도 시나리오 (T=3)

**입력값**: qty=100, avg_price=$10.00, T≈3 (검증 필요), seed=$6720, split=20

**계산**:
```
B = $336
T = (100 × $10) / $336 ≈ 2.98 → T < 10 (전반전)
별% = 10 - 2.98 = 7.02%
별가 = ceil($10 × 1.0702) = ceil($10.702) = $10.71
p_avg = min($10, $10.71) - $0.01 = $9.99
p_star = $10.70

q_avg = floor(($336/2) / $9.99) = floor($168 / $9.99) = 16주
q_star = floor(($336/2) / $10.70) = floor($168 / $10.70) = 15주
q_sell = floor(100 / 4) = 25주  [V4.0 원문: floor]
```

**기대 주문**:
```
LOC 매수 16주 @$9.99  ← 평단매수
LOC 매수 15주 @$10.70 ← 별값매수
LOC 매도 25주 @$10.71 ← 별값매도
줍줍 1주 ×5 (사다리)
```

### 9.3 후반전 매수 시나리오 (T=12)

**입력값**: qty=200, avg_price=$10.00, split=20, seed=$6720

**계산**:
```
T = (200 × $10) / $336 ≈ 5.95 → T > 10이 되려면...
실제로 T≈12가 되려면 qty×avg = T×B = 12×$336 = $4032
  즉 qty=403, avg=$10 → 403주 보유 시
별% = 10 - 12 = -2% → 별가 = ceil($10 × 0.98) = $9.80
p_star = $9.79
q_star = floor($336 / $9.79) = 34주
q_sell = floor(403 / 4) = 100주
```

**기대 주문**:
```
LOC 매수 34주 @$9.79   ← 별값매수(통합)
LOC 매도 100주 @$9.80  ← 별값매도(쿼터)
줍줍 1주 ×5 (별가 하락 사다리)
```

### 9.4 리버스 진입 시나리오

**입력값**: T≥19 (split=20, T=19.5), is_snapshot_mode=True

**기대값**:
```
T > N-1 = 19 → 리버스 모드 강제 진입
1일차: MOC 매도 floor(Q/20)주
process_status = "♻️리버스(1일차 진입)"
```

### 9.5 리버스 별지점 시나리오 (2일차 이후)

**입력값**: is_rev_active=True, ma_5day=$9.50, qty=100, rem_cash=$500

**기대값** (B1 버그 수정 후):
```
rev_star = $9.50  ← ma_5day에서 직접 취득
buy_budget = $500 / 4 = $125
buy_price = $9.50 - $0.01 = $9.49
buy_qty = floor($125 / $9.49) = 13주
sell_qty = floor(100 / 20) = 5주

LOC 매수 13주 @$9.49 ← 리버스쿼터매수
LOC 매도  5주 @$9.50 ← 리버스분할매도
```

### 9.6 텔레그램 UI 검증

| 검증 항목 | 기대 결과 |
|-----------|-----------|
| `/start` 응답 | "무한매수법 V4.0 봇" 표기, /avwap 명령어 없음 |
| `/sync` 응답 | V4.0 지시서 표출, V-REV/AVWAP 섹션 없음 |
| `/settlement` 응답 | AVWAP 예산/오버나이트 버튼 없음 |
| `/reset` 응답 | 암살자 장부 초기화 버튼 없음 |
| `/avwap` 요청 | 미응답 또는 "명령어 없음" 응답 |
| `/queue` 요청 | 미응답 또는 "명령어 없음" 응답 |
| `/version` 응답 | "V4.0" 버전 표기 |

### 9.7 줍줍 사다리 검증

**입력값**: q_base=10주, B=$336

**기대 줍줍 주문**:
```
n=1: floor($336/11×100)/100 = floor($30.545...×100)/100 = $30.54, 1주
n=2: floor($336/12×100)/100 = floor($28.00×100)/100    = $28.00, 1주
n=3: floor($336/13×100)/100 = floor($25.846...×100)/100 = $25.84, 1주
n=4: floor($336/14×100)/100 = floor($24.00×100)/100    = $24.00, 1주
n=5: floor($336/15×100)/100 = floor($22.40×100)/100    = $22.40, 1주

내림차순 정렬: $30.54 → $28.00 → $25.84 → $24.00 → $22.40
```

---

## 부록: 파일 변경 요약표

| 파일 | 작업 | 핵심 변경 |
|------|------|-----------|
| `strategy_v14.py` | 수정 | B1~B4 버그 수정, 클래스명 V4Strategy, 별% 공식 수정 |
| `strategy.py` | 수정 | 플러그인 제거, V4Strategy 직결 |
| `config.py` | 수정 | FILES 정리, DEFAULT_SPLIT 20, VWAP_PROFILES 삭제 |
| `main.py` | 수정 | 5개 import 제거, 7개 스케줄 제거, 4개 커맨드 제거 |
| `scheduler_regular.py` | 수정 | state_io_manager 제거, V-REV 분기 제거 |
| `scheduler_core.py` | 수정 | V-REV/Volatility 코드 제거 |
| `telegram_commands.py` | 수정 | cmd_avwap/queue/add_q/clear_q 제거 |
| `telegram_view.py` | 수정 | V4.0 문자열, V-REV/AVWAP 버튼 제거 |
| `telegram_bot.py` | 수정 | queue_ledger/strategy_rev/avwap_handler 제거 |
| `telegram_callbacks.py` | 수정 | AVWAP/QUEUE 라우팅 제거 |
| `callback_config_handler.py` | 수정 | AVWAP/V-REV 콜백 제거 |
| `callback_order_handler.py` | 수정 | handle_emergency() 삭제 |
| `callback_queue_handler.py` | 스텁화 | 빈 클래스 |
| `order_executor.py` | 수정 | 자본잠김이관 제거 |
| `telegram_states.py` | 수정 | EDIT_QUEUE_LAYER 상태 제거 |
| 18개 파일 | 삭제/스텁 | 위 §4.1 목록 |
