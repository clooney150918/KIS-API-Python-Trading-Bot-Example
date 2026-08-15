# ==========================================================
# FILE: order_executor.py
# ==========================================================
# 🚨 MODIFIED: [Thundering Herd 영구 소각] 파편화된 await asyncio.sleep(0.06) 땜질 전면 삭제.
# 🚨 MODIFIED: [중앙 통제소 위임] 모든 API 지연을 GlobalThrottle(중앙 통제소)로 100% 위임하여 이벤트 루프 교착 상태 완벽 방어.
# 🚨 MODIFIED: [자본 잠김 패러독스 파기] 자본 잠김(is_capital_locked) 시 매수(BUY) 주문만 애프터장으로 지연 이관하고, 매도(SELL) 주문은 100% 정상적으로 정규장 슬라이싱/지정가 타격을 강행하도록 매도 탈출망 디커플링 결속 완료.
# ==========================================================
import asyncio
import logging
import html
import hashlib
import inspect
import json
import math
from state_io_manager import save_slice_state_sync, save_aftermarket_state_sync
from runtime_safety import (
    RuntimeSafetyGate,
    account_fingerprint,
    canonical_order_values,
    order_submission_ambiguous_result,
    resolve_account_fingerprint_key,
    safety_block_result,
    shadow_record_failure_decision,
)
from order_intent_store import (
    DuplicateOrderIntentError,
    InvalidOrderIntentError,
    REQUIRED_PLAN_FIELDS,
    STRATEGY as OFFICIAL_ORDER_STRATEGY,
    compute_intent_id,
)
from shadow_intent import ShadowIntentRecorder


_RISK_REFERENCE_ORDER_TYPES = frozenset({"MARKET", "MOC", "MOO"})
ORDER_SUBMISSION_TIMEOUT_SECONDS = 15.0


def _supports_keyword(callable_obj, keyword):
    """Return whether callable_obj explicitly accepts keyword or arbitrary kwargs."""
    try:
        parameters = inspect.signature(callable_obj).parameters
    except (TypeError, ValueError):
        return False
    parameter = parameters.get(keyword)
    if parameter is not None and parameter.kind in {
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.KEYWORD_ONLY,
    }:
        return True
    return any(
        candidate.kind == inspect.Parameter.VAR_KEYWORD
        for candidate in parameters.values()
    )

def _safe_float(val):
    try:
        f_val = float(str(val or 0.0).replace(',', ''))
        if math.isnan(f_val) or math.isinf(f_val): return 0.0
        return f_val
    except Exception:
        return 0.0


def _order_idempotency_key(trade_date, ticker, order_index, order, side, order_type):
    """Build a stable retry key for one planned order occurrence."""
    identity = {
        "trade_date": str(trade_date),
        "ticker": str(ticker).strip().upper(),
        "order_index": order_index,
        "description": str(order.get("desc", "")),
        "side": side,
        "quantity": str(order.get("qty")),
        "price": str(order.get("price")),
        "risk_reference_price": str(order.get("risk_reference_price")),
        "order_type": order_type,
    }
    canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

def _coerce_positive_int(value, field):
    if isinstance(value, bool):
        raise InvalidOrderIntentError(f"{field} must be a positive integer")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.isdigit():
        parsed = int(value)
    else:
        raise InvalidOrderIntentError(f"{field} must be a positive integer")
    if parsed <= 0:
        raise InvalidOrderIntentError(f"{field} must be a positive integer")
    return parsed


def _is_official_order(order):
    return (
        order.get("strategy") == OFFICIAL_ORDER_STRATEGY
        or "intent_id" in order
        or "strategy_revision" in order
        or "t_revision" in order
        or "event_type" in order
    )


def _current_t_revision_from_provider(provider, ticker):
    if provider is None:
        raise InvalidOrderIntentError("ORDER_INTENT_T_REVISION_UNAVAILABLE: current T revision provider missing")
    try:
        status = provider(ticker)
    except Exception as exc:
        raise InvalidOrderIntentError("ORDER_INTENT_T_REVISION_UNAVAILABLE: current T revision provider failed") from exc
    if isinstance(status, dict):
        if status.get("status") not in {"OK", "CURRENT", "READY"}:
            raise InvalidOrderIntentError("ORDER_INTENT_T_REVISION_UNAVAILABLE: current T revision status not ok")
        for key in ("t_revision", "current_t_revision", "revision"):
            if key in status:
                status = status[key]
                break
        else:
            raise InvalidOrderIntentError("ORDER_INTENT_T_REVISION_UNAVAILABLE: current T revision missing")
    try:
        return _coerce_positive_int(status, "current_t_revision")
    except InvalidOrderIntentError as exc:
        raise InvalidOrderIntentError("ORDER_INTENT_T_REVISION_UNAVAILABLE: current T revision invalid") from exc


def _assert_official_t_revision_current(order, ticker, current_t_revision_provider):
    order_revision = _coerce_positive_int(order.get("t_revision"), "t_revision")
    current_revision = _current_t_revision_from_provider(current_t_revision_provider, ticker)
    if order_revision != current_revision:
        raise InvalidOrderIntentError(
            f"ORDER_INTENT_STALE_T_REVISION: expected {current_revision}, got {order_revision}"
        )


def _fill_guard_forbids_new_orders(fill_reconciliation_guard, ticker):
    if fill_reconciliation_guard is None:
        return False
    try:
        if callable(fill_reconciliation_guard):
            return bool(fill_reconciliation_guard(ticker))
        if hasattr(fill_reconciliation_guard, "forbid_new_orders"):
            return bool(fill_reconciliation_guard.forbid_new_orders(ticker))
    except Exception:
        return True
    return True


def _order_success_cache_key(trade_date, ticker, order, side, order_type, fallback_key, current_t_revision_provider=None):
    """Use official deterministic intent_id for successful-order de-dupe.

    Legacy/non-official orders fall back to the existing transport idempotency key
    so old UI description strings never define cache identity.
    """
    is_official_order = _is_official_order(order)
    if not is_official_order:
        return fallback_key

    missing_fields = []
    for field in REQUIRED_PLAN_FIELDS:
        if field not in order:
            missing_fields.append(field)
    if missing_fields:
        raise InvalidOrderIntentError(
            f"missing required official order intent field(s): {', '.join(missing_fields)}"
        )

    order_ticker = str(order.get("ticker", "")).strip().upper()
    executor_ticker = str(ticker).strip().upper()
    if order_ticker != executor_ticker:
        raise InvalidOrderIntentError("ticker must match executor ticker")

    _assert_official_t_revision_current(order, order_ticker, current_t_revision_provider)

    intent_payload = {
        "strategy": order.get("strategy"),
        "strategy_revision": order.get("strategy_revision"),
        "t_revision": order.get("t_revision"),
        "ticker": order.get("ticker"),
        "trade_date": order.get("trade_date"),
        "event_type": order.get("event_type"),
        "side": side,
        "order_type": order.get("order_type"),
        "price": str(order.get("price")),
        "qty": order.get("qty"),
    }
    canonical_intent_id = compute_intent_id(intent_payload)
    explicit_intent_id = order.get("intent_id")
    if explicit_intent_id and str(explicit_intent_id) != canonical_intent_id:
        raise InvalidOrderIntentError("ORDER_INTENT_ID_MISMATCH")
    return canonical_intent_id


def _official_intent_payload(order, side, order_type):
    return {
        "strategy": order.get("strategy"),
        "strategy_revision": order.get("strategy_revision"),
        "t_revision": order.get("t_revision"),
        "ticker": order.get("ticker"),
        "trade_date": order.get("trade_date"),
        "event_type": order.get("event_type"),
        "side": side,
        "order_type": order_type,
        "price": str(order.get("price")),
        "qty": order.get("qty"),
    }


def _ensure_planned_official_intent(order_intent_store, order, side, order_type):
    if order_intent_store is None or not hasattr(order_intent_store, "create_planned"):
        return None
    try:
        return order_intent_store.create_planned(_official_intent_payload(order, side, order_type))
    except DuplicateOrderIntentError:
        return None


async def execute_order_list(broker, ticker, orders_list, successful_orders_cache, is_market_active_now, today_str, is_capital_locked=False, order_category="1차 필수", runtime_safety_gate=None, shadow_intent_recorder=None, current_t_revision_provider=None, order_intent_store=None, t_event_store=None, fill_reconciliation_guard=None):
    msgs = ""
    all_success = True
    loop_fail_reason = ""

    if not isinstance(orders_list, list):
        return False, "🚨 <b>스냅샷 오염: 주문 리스트 결측</b>\n", "주문 리스트 타입 에러"

    for order_index, o in enumerate(orders_list):
        try:
            if not isinstance(o, dict): continue

            official_order = _is_official_order(o)
            o_side, o_type = canonical_order_values(
                o.get('side', 'BUY'),
                o.get('order_type') if official_order else o.get('type', o.get('order_type', 'LOC')),
            )
            o_qty = int(_safe_float(o.get('qty')))
            o_price = _safe_float(o.get('price'))
            idempotency_key = _order_idempotency_key(
                today_str, ticker, order_index, o, o_side, o_type
            )
            
            o_desc = html.escape(str(o.get('desc', '주문')))

            try:
                order_key = _order_success_cache_key(
                    today_str,
                    ticker,
                    o,
                    o_side,
                    o_type,
                    idempotency_key,
                    current_t_revision_provider=current_t_revision_provider,
                )
            except InvalidOrderIntentError as error:
                error_text = str(error)
                if "ORDER_INTENT_ID_MISMATCH" in error_text:
                    code = "ORDER_INTENT_ID_MISMATCH"
                elif "ORDER_INTENT_STALE_T_REVISION" in error_text:
                    code = "ORDER_INTENT_STALE_T_REVISION"
                elif "ORDER_INTENT_T_REVISION_UNAVAILABLE" in error_text:
                    code = "ORDER_INTENT_T_REVISION_UNAVAILABLE"
                else:
                    code = "ORDER_INTENT_INVALID"
                all_success = False
                reason = html.escape(error_text)
                loop_fail_reason = f"[{ticker}] {order_category} 주문 의도 검증 실패: {code} {reason}"
                msgs += f"└ {order_category}: {o_desc} {o_qty}주 (${o_price}): ❌({code}: {reason})\n"
                break
            if o_qty <= 0:
                msgs += f"⚠️ {order_category}: 수량 0주 산출로 타격 바이패스 (안전 격리)\n"
                continue
            if official_order and order_intent_store is None:
                code = "ORDER_INTENT_STORE_UNAVAILABLE"
                all_success = False
                loop_fail_reason = f"[{ticker}] {order_category} 주문 의도 원장 미연결: {code}"
                msgs += f"└ {order_category}: {o_desc} {o_qty}주 (${o_price}): ❌({code})\n"
                break
            if official_order and _fill_guard_forbids_new_orders(fill_reconciliation_guard, ticker):
                code = "PARTIAL_FILL_OPEN"
                all_success = False
                loop_fail_reason = f"[{ticker}] {order_category} 체결 대사 안전 차단: {code}"
                msgs += f"└ {order_category}: {o_desc} {o_qty}주 (${o_price}): ❌({code})\n"
                break
            if order_key in successful_orders_cache:
                msgs += f"└ {order_category}: {o_desc} {o_qty}주 (${o_price}): ✅(기장전 보존)\n"
                continue

            broker_method = (
                broker.send_order
                if is_market_active_now
                else broker.send_reservation_order
            )
            supports_risk_reference = _supports_keyword(
                broker_method, "risk_reference_price"
            )
            supports_idempotency_key = _supports_keyword(
                broker_method, "idempotency_key"
            )
            if o_type in _RISK_REFERENCE_ORDER_TYPES and not supports_risk_reference:
                decision = RuntimeSafetyGate.denied(
                    "BROKER_CAPABILITY_MISSING",
                    "broker order method cannot carry required risk reference price",
                    shadow_only=False,
                    ticker=str(ticker),
                    side=o_side,
                )
                blocked = safety_block_result(decision)
                code = blocked['safety_decision']['code']
                all_success = False
                loop_fail_reason = f"[{ticker}] {order_category} 안전 게이트 차단: {code}"
                msgs += f"└ {order_category}: {o_desc} {o_qty}주 (${o_price}): ❌({code})\n"
                break

            gate = runtime_safety_gate or getattr(broker, 'runtime_safety_gate', None)
            fingerprint = None
            if gate is None:
                decision = RuntimeSafetyGate.denied(
                    "SAFETY_NOT_CONFIGURED",
                    "runtime safety gate was not injected",
                    ticker=str(ticker),
                    side=o_side,
                )
            else:
                key = resolve_account_fingerprint_key(
                    getattr(broker, 'account_fingerprint_key', None)
                )
                if key is not None:
                    fingerprint = account_fingerprint(
                        getattr(broker, 'cano', None),
                        getattr(broker, 'acnt_prdt_cd', None),
                        key=key,
                    )
                decision = gate.authorize(
                    ticker,
                    o_side,
                    o.get('qty'),
                    o.get('price'),
                    account_fingerprint=fingerprint,
                    account_fingerprint_key_available=key is not None,
                    order_type=o_type,
                    risk_reference_price=o.get('risk_reference_price'),
                    market_quote_preflight=True,
                )
            if not decision.can_submit and decision.code != "MARKET_QUOTE_REQUIRED":
                if decision.code == "SHADOW_ONLY":
                    recorder = shadow_intent_recorder or ShadowIntentRecorder()
                    try:
                        recorder.record(
                            ticker=ticker,
                            side=o_side,
                            quantity=o.get('qty'),
                            price=o.get('price'),
                            order_type=o_type,
                            safety_revision=decision.revision,
                            risk_reference_price=o.get('risk_reference_price'),
                            idempotency_key=idempotency_key,
                        )
                    except Exception as error:
                        decision = shadow_record_failure_decision(decision, error)
                blocked = safety_block_result(decision)
                code = blocked['safety_decision']['code']
                all_success = False
                loop_fail_reason = f"[{ticker}] {order_category} 안전 게이트 차단: {code}"
                msgs += f"└ {order_category}: {o_desc} {o_qty}주 (${o_price}): ❌({code})\n"
                break

            if official_order:
                try:
                    _ensure_planned_official_intent(order_intent_store, o, o_side, o_type)
                except Exception as error:
                    all_success = False
                    err_msg = f"ORDER_INTENT_PLANNED_RECORD_FAILED {html.escape(str(error))}"
                    loop_fail_reason = f"[{ticker}] {order_category}: {err_msg}"
                    msgs += f"└ {order_category}: {o_desc} {o_qty}주 (${o_price}): ❌({err_msg})\n"
                    break

            res = {}

            try:
                # 🚨 MODIFIED: [매도 탈출망 확보] 자본이 잠기더라도 매도(SELL) 주문은 애프터장으로 날리지 않고 정규장 엔진(VWAP 등)으로 정상 인계
                if is_capital_locked and o_side == 'BUY':
                    slice_info = {"ticker": ticker, "side": o_side, "total_qty": o_qty, "filled_qty": 0, "target_price": o_price, "desc": o_desc, "status": "PENDING"}
                    await asyncio.wait_for(asyncio.to_thread(save_aftermarket_state_sync, ticker, today_str, slice_info), timeout=10.0)
                    res = {'rt_cd': '0', 'msg1': '애프터장 매수 지연 이관 완료', 'odno': f'AFTERMARKET_{id(o)}'}

                elif o_type == 'VWAP':
                    slice_info = {"ticker": ticker, "side": o_side, "total_qty": o_qty, "filled_qty": 0, "target_price": o_price, "desc": o_desc, "status": "PENDING"}
                    await asyncio.wait_for(asyncio.to_thread(save_slice_state_sync, ticker, today_str, slice_info), timeout=10.0)
                    res = {'rt_cd': '0', 'msg1': '로컬 자체 VWAP 엔진 위임 완료', 'odno': f'LOCAL_VWAP_{id(o)}'}

                else:
                    kwargs = {}
                    if supports_risk_reference:
                        kwargs["risk_reference_price"] = o.get('risk_reference_price')
                    if supports_idempotency_key:
                        kwargs["idempotency_key"] = idempotency_key
                    res = await asyncio.wait_for(
                        asyncio.to_thread(
                            broker_method,
                            ticker,
                            o_side,
                            o_qty,
                            o_price,
                            o_type,
                            **kwargs,
                        ),
                        timeout=ORDER_SUBMISSION_TIMEOUT_SECONDS,
                    )
            except asyncio.TimeoutError:
                res = order_submission_ambiguous_result(
                    "broker order call timed out; worker may still complete",
                    ticker=ticker,
                    side=o_side,
                )
            except Exception as error:
                res = order_submission_ambiguous_result(
                    f"broker order call raised {type(error).__name__}",
                    ticker=ticker,
                    side=o_side,
                )

            safe_res = res if isinstance(res, dict) else {}
            safety_decision = safe_res.get('safety_decision')
            is_ambiguous = (
                isinstance(safety_decision, dict)
                and safety_decision.get('code') == 'ORDER_SUBMISSION_AMBIGUOUS'
            )
            if is_ambiguous:
                reason = safety_decision.get('reason') or safe_res.get('msg1')
                safe_res = order_submission_ambiguous_result(
                    reason, ticker=ticker, side=o_side
                )
                if gate is not None and hasattr(gate, 'latch_ambiguous_submission'):
                    gate.latch_ambiguous_submission(reason)

            is_success = safe_res.get('rt_cd') == '0'
            err_msg = html.escape(str(safe_res.get('msg1') or '오류/잔금패스'))

            if is_success:
                odno = str(safe_res.get('odno') or '').strip()
                if official_order and not odno:
                    is_success = False
                    all_success = False
                    err_msg = "ORDER_ACCEPTED_WITHOUT_ORDER_NO HALT_REQUIRED RECONCILIATION_REQUIRED"
                    loop_fail_reason = f"[{ticker}] {order_category}: {err_msg}"
                    if gate is not None and hasattr(gate, 'latch_ambiguous_submission'):
                        gate.latch_ambiguous_submission("KIS accepted official order without ODNO")
                else:
                    if official_order and order_intent_store is not None:
                        try:
                            accepted_order = {
                                "account_fingerprint": fingerprint,
                                "ticker": str(ticker).strip().upper(),
                                "exchange": str(o.get("exchange") or o.get("ovrs_excg_cd") or "AMEX").strip().upper(),
                                "trade_date": str(today_str).strip().replace("-", ""),
                                "order_no": odno,
                            }
                            if hasattr(order_intent_store, "record_accepted_order"):
                                order_intent_store.record_accepted_order(order_key, accepted_order)
                            else:
                                order_intent_store.transition_status(order_key, "SUBMITTED")
                        except Exception as error:
                            is_success = False
                            all_success = False
                            err_msg = f"ORDER_INTENT_SUBMITTED_RECORD_FAILED {html.escape(str(error))}"
                            loop_fail_reason = f"[{ticker}] {order_category}: {err_msg}"
                    if is_success:
                        # rt_cd==0 is acceptance only: cache the accepted ODNO/intent so
                        # the executor does not resubmit, but never mutate T here.
                        successful_orders_cache.add(order_key)
            else:
                all_success = False
                if is_ambiguous:
                    err_msg = (
                        "ORDER_SUBMISSION_AMBIGUOUS "
                        "HALT_REQUIRED RECONCILIATION_REQUIRED"
                    )
                    loop_fail_reason = f"[{ticker}] {order_category}: {err_msg}"
                else:
                    loop_fail_reason = f"[{ticker}] {order_category} 거절: {err_msg}"

            status_icon = '✅' if is_success else f'❌({err_msg})'
            msgs += f"└ {order_category}: {o_desc} {o_qty}주 (${o_price}): {status_icon}\n"
            if not is_success:
                break

            await asyncio.sleep(0.2)

        except Exception as e:
            all_success = False
            loop_fail_reason = f"[{ticker}] {order_category} 치명적 오류"
            logging.error(f"🚨 [{ticker}] execute_order_list 개별 덫 처리 오류: {e}")
            msgs += f"└ {order_category} 시스템 오류: {html.escape(str(e))}\n"
            break

    return all_success, msgs, loop_fail_reason
