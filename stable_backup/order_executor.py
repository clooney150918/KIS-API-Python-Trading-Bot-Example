# ==========================================================
# FILE: order_executor.py
# ==========================================================
# 🚨 MODIFIED: [Thundering Herd 영구 소각] 파편화된 await asyncio.sleep(0.06) 땜질 전면 삭제.
# 🚨 MODIFIED: [중앙 통제소 위임] 모든 API 지연을 GlobalThrottle(중앙 통제소)로 100% 위임하여 이벤트 루프 교착 상태 완벽 방어.
# ==========================================================
import asyncio
import logging
import html
import math
from state_io_manager import save_slice_state_sync, save_aftermarket_state_sync

def _safe_float(val):
    try:
        f_val = float(str(val or 0.0).replace(',', ''))
        if math.isnan(f_val) or math.isinf(f_val): return 0.0
        return f_val
    except Exception:
        return 0.0

async def execute_order_list(broker, ticker, orders_list, successful_orders_cache, is_market_active_now, today_str, is_capital_locked=False, order_category="1차 필수"):
    msgs = ""
    all_success = True
    loop_fail_reason = ""

    if not isinstance(orders_list, list):
        return False, "🚨 <b>스냅샷 오염: 주문 리스트 결측</b>\n", "주문 리스트 타입 에러"

    for o in orders_list:
        try:
            if not isinstance(o, dict): continue

            o_type = str(o.get('type', 'LOC'))
            o_side = str(o.get('side', 'BUY'))
            o_qty = int(_safe_float(o.get('qty')))
            o_price = _safe_float(o.get('price'))
            
            if o_qty <= 0:
                msgs += f"⚠️ {order_category}: 수량 0주 산출로 타격 바이패스 (안전 격리)\n"
                continue
            
            o_desc = html.escape(str(o.get('desc', '주문')))

            order_key = f"{ticker}_{o_desc}"
            if order_key in successful_orders_cache:
                msgs += f"└ {order_category}: {o_desc} {o_qty}주 (${o_price}): ✅(기장전 보존)\n"
                continue

            res = {}
            
            for attempt in range(3):
                try:
                    # 🚨 MODIFIED: 파편화된 sleep 소각 (GlobalThrottle 위임)

                    if is_capital_locked:
                        slice_info = {"ticker": ticker, "side": o_side, "total_qty": o_qty, "filled_qty": 0, "target_price": o_price, "desc": o_desc, "status": "PENDING"}
                        await asyncio.wait_for(asyncio.to_thread(save_aftermarket_state_sync, ticker, today_str, slice_info), timeout=10.0)
                        res = {'rt_cd': '0', 'msg1': '애프터장 지연 이관 완료', 'odno': f'AFTERMARKET_{id(o)}'}
                        
                    elif o_type == 'VWAP':
                        slice_info = {"ticker": ticker, "side": o_side, "total_qty": o_qty, "filled_qty": 0, "target_price": o_price, "desc": o_desc, "status": "PENDING"}
                        await asyncio.wait_for(asyncio.to_thread(save_slice_state_sync, ticker, today_str, slice_info), timeout=10.0)
                        res = {'rt_cd': '0', 'msg1': '로컬 자체 VWAP 엔진 위임 완료', 'odno': f'LOCAL_VWAP_{id(o)}'}
                        
                    elif is_market_active_now:
                        res = await asyncio.wait_for(asyncio.to_thread(broker.send_order, ticker, o_side, o_qty, o_price, o_type), timeout=15.0)
                    else:
                        res = await asyncio.wait_for(asyncio.to_thread(broker.send_reservation_order, ticker, o_side, o_qty, o_price, o_type), timeout=15.0)

                    break
                    
                except asyncio.TimeoutError:
                    if attempt < 2:
                        await asyncio.sleep(1.0 * (2 ** attempt))
                    else:
                        res = {'rt_cd': '999', 'msg1': 'API 통신 타임아웃 (10~15초 초과)'}
                except Exception as e:
                    if attempt < 2:
                        await asyncio.sleep(1.0 * (2 ** attempt))
                    else:
                        res = {'rt_cd': '999', 'msg1': f'통신/I/O 오류: {str(e)}'}

            safe_res = res if isinstance(res, dict) else {}
            is_success = safe_res.get('rt_cd') == '0'
            err_msg = html.escape(str(safe_res.get('msg1') or '오류/잔금패스'))

            if is_success:
                successful_orders_cache.add(order_key)
            else:
                all_success = False
                loop_fail_reason = f"[{ticker}] {order_category} 거절: {err_msg}"

            status_icon = '✅' if is_success else f'❌({err_msg})'
            msgs += f"└ {order_category}: {o_desc} {o_qty}주 (${o_price}): {status_icon}\n"
            
            await asyncio.sleep(0.2)

        except Exception as e:
            all_success = False
            loop_fail_reason = f"[{ticker}] {order_category} 치명적 오류"
            logging.error(f"🚨 [{ticker}] execute_order_list 개별 덫 처리 오류: {e}")
            msgs += f"└ {order_category} 시스템 오류: {html.escape(str(e))}\n"

    return all_success, msgs, loop_fail_reason
