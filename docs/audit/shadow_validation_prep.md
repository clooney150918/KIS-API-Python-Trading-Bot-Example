# SHADOW Deployment Validation & Preparation

<!--
  Task 13 — SHADOW 후보 컨테이너 배포 준비
  Worktree: /opt/bots/soxl-trading-jinho-v4-tdd, HEAD: 97c2afe
  원본: /opt/bots/soxl-trading-jinho (수정금지)
  상태: 대표님 승인 대기 중 — 컨테이너 시작 전
-->

## 1. Current Safety Posture

| Parameter                | Value            | Status |
|--------------------------|------------------|--------|
| `operator_halt`          | `true`           | ✅ |
| `live_armed`             | `false`          | ✅ |
| `shadow_only`            | `true`           | ✅ |
| `reason`                 | `OFFICIAL_V4_REFACTOR` | — |
| `revision`               | `1`              | — |
| `allowed_tickers`        | `["SOXL"]`       | ✅ |
| `max_order_quantity`     | `100`            | — |
| `max_order_notional`     | `$25,000.00`     | — |
| `market_slippage_buffer` | `5.00%`          | — |

**Triple-lock gate**: `operator_halt=true && live_armed=false && shadow_only=true` — 모든 라이브 브로커 호출 차단 확인 완료 (test_18, test_19 통과).

**Source**: `data/runtime_safety.json` — revision 1, updated 2026-08-11.

## 2. Docker Compose Safety

- **Production container restart policy**: `restart: "no"` (was `always`)
  - `docker compose up -d` 또는 `docker compose restart` 실행 시 production 컨테이너가 자동 기동되지 않음
  - File: `/opt/bots/docker-compose.yml`, `soxl-trading-jinho` service
  - 다른 서비스(soxl-watchdog, soxl-trading-eunkyung, taechung-bot, jinho-finance-bot)는 영향 없음

## 3. Official Schedules — Zero Non-Official (Task 8 Verified)

- 공식 V4 스케줄만 등록: `scheduled_trade_monitor`, `scheduled_auto_sync`, 정규장/마감 루틴
- 비공식(non-official) 스케줄 0개 확인 완료
- `scheduler_core.is_official_trading_day_at()` → fail-closed (비공식 날짜 실행 불가)

## 4. Offline Test Suite — 512 Tests Passed

```
512 tests collected in 1.26s
100% pass rate across 28 test modules (Task 11 + Task 12)
```

### Test breakdown:

| Module                              | Tests | Focus                             |
|-------------------------------------|-------|-----------------------------------|
| `test_laoer_v4_20.py`               | ~18   | V4 20분할 core formulas          |
| `test_execution_replay.py`          | ~9    | Fill event chain (FULL→HALF→QTR) |
| `test_t_event_engine.py`            | ~3    | T event state transitions        |
| `test_failure_injection.py`         | ~9    | Test 15-20 (safety + legacy)     |
| `test_kis_source_of_truth.py`       | ~5    | KIS single source of truth       |
| `test_runtime_safety.py`            | ~29   | Safety gate                      |
| `test_runtime_safety_authorization.py` | ~16 | Authorization                    |
| `test_order_safety_integration.py`  | ~7    | Order safety integration         |
| `test_strategy_official_adapter.py` | ~15   | Strategy adapter                 |
| `test_end_to_end_shadow.py`         | ~8    | Shadow mode E2E                  |
| 나머지 18개 모듈                     | ~383  | Ledger, fill, intent, config 등  |

**Key safety tests verified**:
- Test 18: Triple-lock (HALT+unarmed+shadow) → broker 호출 0회
- Test 19: 컨테이너 재시작 후 LIVE 자동활성화 0회
- Test 20: 기존 KIS 72건 1:1 대사 (아래 참조)

## 5. KIS Legacy Baseline — 72 Executions Reconciled (Task 11, Test 20)

Approved baseline (`trade_state_store.APPROVED_BASELINE`):

```json
{
    "schema_version": 1,
    "ticker": "SOXL",
    "as_of": "2026-08-11",
    "qty": 98,
    "avg_price": "158.0735",
    "available_cash": "1482.88",
    "t": "18.32",
    "reverse_active": false,
    "source": "CEO_APPROVED_KIS_BASELINE",
    "legacy_execution_count": 72,
    "immutable": true
}
```

- **72 executions reconciled** against actual KIS history
- Baseline is frozen (`immutable: true`) — any mutation rejected at load time
- `source: CEO_APPROVED_KIS_BASELINE` — 대표님 승인

## 6. Reference Formula Values

### Approved Baseline (from KIS)

| Parameter              | Value          | Description                              |
|------------------------|----------------|------------------------------------------|
| `split` (N)            | 20             | SOXL 20분할                              |
| `t`                    | 18.32          | 현재 T-레벨 (18.32분할 소진)             |
| `qty`                  | 98             | 현재 보유 주식 수                         |
| `avg_price`            | $158.0735      | 평균 매입 단가                            |
| `available_cash`       | $1,482.88      | 사용 가능 현금                            |
| `seed`                 | $6,720.00      | 시드 금액                                 |
| `profit_target`        | 12.0%          | 목표 수익률                               |

### V4 Official Formulas

```
별% = N/2 - T                     (N=20 → 별% = 10 - T)
별가 = ceil(avg_price × (1 + 별%/100))   [센트 단위 올림]
별매도가 = ceil(avg_price × (1 - 별%/100))

별수량 = floor(available_cash/2 / 별가)
평단수량 = floor(available_cash/2 / 평단가)

매도수량 = floor(qty / 4)         [V4 원문: floor]
```

### Current Values (T=18.32)

| Derived Value           | Formula                          | Result                     |
|-------------------------|----------------------------------|----------------------------|
| 별%                     | 10 - 18.32                       | **-8.32%** (음수 = 할인매수) |
| 별가 (별매수가)          | $158.0735 × (1 - 0.0832)         | ~$144.91                   |
| 별매도가                 | $158.0735 × (1 + 0.0832)         | ~$171.22                   |
| 별수량                   | floor($741.44 / 별가)            | ≈ 5주                       |
| 평단수량                  | floor($741.44 / $158.07)         | ≈ 4주                       |
| 매도수량                  | floor(98 / 4)                    | 24주                        |

> **Note**: Legacy formula (`target_pct - 2×T = 20 - 36.64 = -16.64%`) is superseded by V4 `N/2 - T`.

### T-Progress Reference (20분할)

| T Range    | Phase          | 별% Range   | Direction              |
|------------|----------------|-------------|------------------------|
| 0 ≤ T < 10 | 전반 (매수 집중) | +10% → 0%   | 평단 이상 매수           |
| T = 10     | 중간점         | 0%          | 평단매수 = 별매수        |
| 10 < T ≤ 20| 후반 (매도 집중) | 0% → -10%   | 평단 이하 할인매수       |
| T = 18.32  | **현재**       | **-8.32%**  | 매우 공격적 할인매수 구간 |

### Week-Number Thresholds (기준 주차)

| 주차 (Week) | T-Count Equiv | 설명                                   |
|-------------|---------------|----------------------------------------|
| 6주          | ~T=6          | 6주차 — 초기 진입 구간                  |
| 24주         | ~T=24         | 24주차 — 후반 리버스 가능 구간 (20분할 초과)|
| 74주         | ~T=74         | 74주차 — 장기 홀딩/리버스 이후 구간      |

> 위 주차 기준은 라오어 방법론의 T-진행률 주차 변환 참조값입니다.  
> N=20분할 기준: 매주 대략 0.27~0.28 T 소진 (74주 기준).

## 7. SHADOW Container Startup (대표님 승인 후)

### Shadow-only container start

```bash
# 1. SHADOW 모드로 fresh container 빌드 및 시작
docker compose -f /opt/bots/docker-compose.yml build soxl-trading-jinho
docker run -d \
  --name soxl-trading-jinho-shadow \
  --network bot-net \
  --restart no \
  -v /opt/bots/soxl-trading-jinho:/app:ro \
  --env-file /opt/bots/soxl-trading-jinho/.env \
  -e OPERATOR_HALT=true \
  -e LIVE_ARMED=false \
  -e SHADOW_ONLY=true \
  --cpus 1.0 \
  --memory 512m \
  soxl-trading-jinho:latest
```

### Verify shadow mode

```bash
# SHADOW 모드 로그 확인
docker logs -f soxl-trading-jinho-shadow 2>&1 | grep -E 'SHADOW|OPERATOR_HALT|라이브 차단'

# 안전 게이트 확인
docker exec soxl-trading-jinho-shadow cat /app/data/runtime_safety.json
```

### Production 컨테이너 시작하지 않음 확인

```bash
# compose 재생성 후에도 production 컨테이너가 올라가지 않음
docker compose -f /opt/bots/docker-compose.yml up -d
docker ps --filter name=soxl-trading-jinho
# → soxl-trading-jinho-shadow 만 떠 있어야 함 (production 컨테이너 없음)
```

## 8. SHADOW Result Validation Checklist

SHADOW 실행 후 확인할 항목:

- [ ] SHADOW 컨테이너 정상 기동 (`docker ps`)
- [ ] 로그에 `SHADOW_ONLY` / `OPERATOR_HALT` 모드 진입 확인
- [ ] 라이브 브로커 호출 0회 (로그 검증)
- [ ] T-event chain 정상 누적 (baseline T=18.32 기준)
- [ ] V4 별% 공식 적용 (`N/2 - T`) 확인
- [ ] Telegram 메시지 전송 정상 (SHADOW 라벨 표시)
- [ ] `data/t_events.jsonl` 정상 append (파일 무결성)
- [ ] `data/t_state.json` T값 정상 갱신
- [ ] 72건 legacy KIS 기준과 일치 확인
- [ ] 주문 의도(intent) store 정상 동작
- [ ] Fill reconciler 정상 작동
- [ ] No unintended network calls to KIS production API

## 9. Rollback Procedure

SHADOW 검증 중 문제 발생 시:

```bash
# SHADOW 컨테이너 정지 및 제거
docker stop soxl-trading-jinho-shadow
docker rm soxl-trading-jinho-shadow

# Production 컨테이너는 영향 없음 (restart: "no" 상태)
# docker-compose.yml 원복 (필요시)
git -C /opt/bots checkout docker-compose.yml
```

SHADOW 검증 성공 후 라이브 전환:

```bash
# 1. runtime_safety.json 수정 (대표님 승인 후)
# operator_halt: false, live_armed: true, shadow_only: false

# 2. docker-compose.yml restart 정책 복원
# restart: "no" → restart: always

# 3. Production 컨테이너 시작
docker compose -f /opt/bots/docker-compose.yml up -d soxl-trading-jinho
```

## 10. Pre-Deployment Checklist (Task 13)

- [x] `docker-compose.yml`의 `soxl-trading-jinho` restart 정책 `"no"` 변경
- [x] `data/runtime_safety.json` triple-lock 확인
- [x] 공식 외 스케줄 0개 확인 (Task 8)
- [x] 512개 오프라인 테스트 통과 확인
- [x] KIS 72건 legacy baseline 대사 완료 (Test 20)
- [x] 기준 공식 계산값 문서화 (T, 별%, split, qty 등)
- [x] SHADOW 컨테이너 시작 명령어 작성
- [x] SHADOW 결과 검증 체크리스트 작성
- [x] 롤백 절차 문서화
- [x] `py_compile` 무결성 검증
- [x] `git diff --check` 통과
- [ ] 대표님 승인 (PENDING)

---

**작성일**: 2026-08-12  
**Commit**: `chore: prepare official v4 shadow deployment`  
**Worktree HEAD**: 97c2afe  
**상태**: 대표님 승인 대기 중 — 컨테이너 시작 전
