# ==========================================================
# FILE: vwap_aftermarket_engine.py
# ==========================================================
# 🚨 MODIFIED: [Thundering Herd 영구 소각] _retry_api 및 루프 내부의 파편화된 await asyncio.sleep(0.06) 땜질 전면 삭제.
# 🚨 MODIFIED: [중앙 통제소 위임] 모든 API 지연을 GlobalThrottle(중앙 통제소)로 100% 위임하여 이벤트 루프 교착 상태 완벽 방어.
# 🚨 NEW: [Case 47 자전거래(Wash Trade) 절대 방어망 결속] 암살자 오버나이트 모드 허용 시 기장전된 +1.0% 지정가 익절 덫과 본진(V-REV)의 16:01 애프터장 지연 고가 매수 덫이 충돌하여 KIS 서버에서 리젝(Reject)당하는 맹독성 패러독스를 완벽히 차단하기 위해, 타격 직전 암살자 덫 임시 취소 및 타격 직후 원상 복구(재장전) 파이프라인 100% 팩트 이식 완료.
# ==========================================================
import logging
import asyncio
import math
import datetime
from zoneinfo import ZoneInfo
import html
import functools

from state_io_manager import _read_json_safe_sync, _atomic_write_json_sync

def _safe_float(val):
    try:
        f_val = float(str(val or 0.0).replace(',', ''))
        if math.isnan(f_val) or math.isinf(f_val):
            return 0.0
        return f_val
    except Exception:
        return 0.0

async def _retry_api(func, *args, timeout=15.0, default=None, **kwargs):
    """ 🚨 [Case 32, 33] 중앙 집중형 TPS 캡핑 (GlobalThrottle 위임) 및 지수 백오프 래퍼 """
    for attempt in range(3):
        try:
            if asyncio.iscoroutinefunction(func):
                return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout)
            else:
                p_func = functools.partial(func, *args, **kwargs)
                return await asyncio.wait_for(asyncio.to_thread(p_func), timeout=timeout)
        except asyncio.TimeoutError:
            if attempt == 2:
                func_name = getattr(func, '__name__', 'unknown_func')
                logging.debug(f"🚨 API 래퍼 타임아웃 ({func_name})")
                return default
            await asyncio.sleep(1.0 * (2 ** attempt))
        except Exception as e:
            if attempt == 2:
                func_name = getattr(func, '__name__', 'unknown_func')
                logging.debug(f"🚨 API 래퍼 최종 실패 ({func_name}): {e}")
                return default
            await asyncio.sleep(1.0 * (2 ** attempt))
    return default

async def _safe_send(context, chat_id, text, timeout=15.0, **kwargs):
    if not chat_id: return None
    try:
        return await asyncio.wait_for(context.bot.send_message(chat_id=chat_id, text=text, **kwargs), timeout=timeout)
    except asyncio.TimeoutError:
        logging.error("🚨 텔레그램 전송 타임아웃")
        return None
    except Exception as e:
        logging.error(f"🚨 텔레그램 전송 실패: {e}")
        return None

def _sync_aftermarket_ledger_atomic(tkr, sde, c_qty, r_price, q_ledger, strat, ver):
    if ver == "V_REV":
        if q_ledger:
            if sde == "BUY":
                q_ledger.add_lot(tkr, c_qty, r_price, "VREV_AFTERMARKET_BUY")
            else:
                q_ledger.pop_lots(tkr, c_qty, r_price)
        if hasattr(strat, 'v_rev_plugin'):
            strat.v_rev_plugin.record_execution(tkr, sde, c_qty, r_price)
    else:
        if hasattr(strat, 'v14_vwap_plugin'):
            strat.v14_vwap_plugin.record_execution(tkr, sde, c_qty, r_price)

async def execute_aftermarket_trade(tx_lock, cfg, broker, strategy, queue_ledger, chat_id, context):
    est = ZoneInfo('America/New_York')
    now_est = datetime.datetime.now(est)
    today_hyphen = now_est.strftime('%Y-%m-%d')

    async with tx_lock:
        try:
            active_tickers = await _retry_api(cfg.get_active_tickers, default=[])
        except Exception:
            active_tickers = []
        
        if isinstance(active_tickers, str): active_tickers = [active_tickers]
        elif not isinstance(active_tickers, list): active_tickers = []
        
        for raw_t in active_tickers:
            t = str(raw_t).strip().upper()
            if not t: continue
            
            try:
                try:
                    version = await _retry_api(cfg.get_version, t, default="V14")
                    is_manual_vwap = await _retry_api(getattr(cfg, 'get_manual_vwap_mode', lambda x: False), t, default=False)
                except Exception:
                    version = "V14"
                    is_manual_vwap = False
                
                if version != "V_REV" and not (version == "V14" and is_manual_vwap): continue
                
                state_file = f"data/vrev_aftermarket_state_{t}.json"
                try:
                    after_state = await _retry_api(_read_json_safe_sync, state_file, today_hyphen, default={})
                except Exception:
                    after_state = {}
                
                if after_state.get('date') != today_hyphen: continue
                orders = after_state.get('orders', [])
                if not isinstance(orders, list) or not orders: continue
                
                cash = 0.0
                for attempt in range(3):
                    try:
                        cash_tuple = await _retry_api(broker.get_account_balance, timeout=15.0)
                        cash = _safe_float(cash_tuple[0]) if isinstance(cash_tuple, (list, tuple)) and len(cash_tuple) > 0 else 0.0
                        if cash > 0.0: break
                    except Exception:
                        pass
                    
                    if attempt < 2:
                        logging.warning(f"⏳ [{t}] 애프터장 타격 대기: KIS 예수금 정산 지연(0.0). 10초 후 재스캔합니다.")
                        await asyncio.sleep(10.0)
                
                if cash <= 0.0:
                    logging.error(f"🚨 [{t}] 애프터장 예수금 확보 실패 (자본 잠김 미해소). 타격 중단.")
                    continue

                # 🚨 NEW: [Case 47 자전거래(Wash Trade) 절대 방어망 결속]
                avwap_state_file = f"data/avwap_trade_state_{t}.json"
                avwap_state = {}
                try:
                    avwap_state = await _retry_api(_read_json_safe_sync, avwap_state_file, today_hyphen, default={})
                except Exception as e:
                    logging.error(f"🚨 [{t}] 암살자 상태 스캔 에러: {e}")
                    
                restored_avwap_sell = False
                avwap_qty_to_restore = 0
                avwap_price_to_restore = 0.0
                
                if avwap_state and avwap_state.get('qty', 0) > 0 and avwap_state.get('sell_odno'):
                    avwap_sell_odno = avwap_state.get('sell_odno')
                    logging.info(f"🛡️ [{t}] 자전거래 방어: 암살자 오버나이트 매도 덫({avwap_sell_odno}) 임시 취소 집행")
                    
                    avwap_avg_price = _safe_float(avwap_state.get('avg_price', 0.0))
                    avwap_qty_to_restore = int(_safe_float(avwap_state.get('qty', 0)))
                    avwap_price_to_restore = math.ceil(avwap_avg_price * 1.01 * 100) / 100.0 if avwap_avg_price > 0 else 0.0
                    
                    c_res = await _retry_api(broker.cancel_order, t, avwap_sell_odno, timeout=10.0)
                    if isinstance(c_res, dict) and str(c_res.get('rt_cd', '')) == '0':
                        restored_avwap_sell = True
                        avwap_state['sell_odno'] = ""
                        await _retry_api(_atomic_write_json_sync, avwap_state_file, avwap_state, timeout=10.0)
                        await asyncio.sleep(1.0)
                    else:
                        logging.warning(f"⚠️ [{t}] 암살자 오버나이트 덫 임시 취소 실패. 자전거래 리젝 위험 잔존.")

                msgs = ""
                state_changed = False
                
                for o in orders:
                    if not isinstance(o, dict) or str(o.get('status')) == 'COMPLETED': continue
                    
                    side = str(o.get('side', 'BUY'))
                    total_qty = int(_safe_float(o.get('total_qty')))
                    target_price = _safe_float(o.get('target_price'))
                    desc = html.escape(str(o.get('desc', '')))
                    
                    exec_price = 0.0
                    for p_attempt in range(3):
                        try:
                            if side == "BUY": 
                                p_val = await _retry_api(broker.get_ask_price, t, timeout=10.0)
                            else: 
                                p_val = await _retry_api(broker.get_bid_price, t, timeout=10.0)
                            exec_price = _safe_float(p_val)
                            break
                        except Exception:
                            if p_attempt == 2: exec_price = 0.0
                            else: await asyncio.sleep(1.0 * (2 ** p_attempt))
                    
                    if exec_price <= 0.0:
                        for p_attempt in range(3):
                            try:
                                p_val = await _retry_api(broker.get_current_price, t, timeout=10.0)
                                exec_price = _safe_float(p_val)
                                break
                            except Exception:
                                if p_attempt == 2: exec_price = 0.0
                                else: await asyncio.sleep(1.0 * (2 ** p_attempt))
                                
                    is_target_hit = False
                    if target_price > 0.0 and exec_price > 0.0:
                        if side == "BUY" and exec_price <= target_price: is_target_hit = True
                        elif side == "SELL" and exec_price >= target_price: is_target_hit = True
                        
                    if not is_target_hit:
                        msgs += f"⏸️ {desc}: 목표가(${target_price:.2f}) 미도달 (현재가 ${exec_price:.2f}) ➔ 관망 유지\n"
                        continue

                    if side == "BUY":
                        max_buy = int(math.floor(cash / exec_price)) if exec_price > 0 else 0
                        if total_qty > max_buy:
                            logging.warning(f"🚨 [{t}] 애프터장 매수 수량 캡핑 가동 (가용 현금 한도 초과): {total_qty} -> {max_buy}")
                            total_qty = max_buy
                        if total_qty <= 0:
                            msgs += f"⚠️ {desc}: KIS 예수금 부족으로 애프터장 타격 스킵\n"
                            continue
                    else:
                        rt_qty = total_qty
                        for attempt_bal in range(3):
                            try:
                                bal_tuple = await _retry_api(broker.get_account_balance, timeout=10.0)
                                if isinstance(bal_tuple, (list, tuple)) and len(bal_tuple) > 1:
                                    rt_qty = int(_safe_float(bal_tuple[1].get(t, {}).get('qty', 0)))
                                break
                            except Exception:
                                if attempt_bal == 2: pass
                                else: await asyncio.sleep(1.0 * (2 ** attempt_bal))
                        
                        total_qty = min(total_qty, rt_qty)
                        if total_qty <= 0:
                            msgs += f"⚠️ {desc}: KIS 보유 잔고 0주 (Ghost-Dumping 방어)\n"
                            o['status'] = 'COMPLETED'
                            state_changed = True
                            continue
                        
                    res = None
                    for s_attempt in range(3):
                        try:
                            res = await _retry_api(broker.send_order, t, side, total_qty, target_price, "LIMIT", timeout=15.0)
                            break
                        except Exception as e:
                            if s_attempt == 2: logging.error(f"🚨 [{t}] 애프터장 타격 에러: {e}")
                            else: await asyncio.sleep(1.0 * (2 ** s_attempt))
                            
                    safe_res = res if isinstance(res, dict) else {}
                    is_success = safe_res.get('rt_cd') == '0'
                    odno = str(safe_res.get('odno') or '')
                    
                    if is_success and odno:
                        o['status'] = 'COMPLETED'
                        o['odno'] = odno
                        if side == "BUY": cash -= (total_qty * target_price) 
                        state_changed = True
                        msgs += f"🎯 {desc}: {total_qty}주 @ ${target_price:.2f} ➔ 일괄 타격 완료 (LIMIT)\n"
                        
                        try:
                            p_sync = functools.partial(_sync_aftermarket_ledger_atomic, t, side, total_qty, target_price, queue_ledger, strategy, version)
                            await asyncio.wait_for(asyncio.to_thread(p_sync), timeout=10.0)
                            logging.info(f"💾 [{t}] 애프터장 체결 장부 원자적 동기화 완료: {side} {total_qty}주 @ ${target_price:.2f}")
                        except Exception as e:
                            logging.error(f"🚨 [{t}] 애프터장 타격 장부 동기화 실패: {e}")
                    else:
                        err_msg = html.escape(str(safe_res.get('msg1') or '거절'))
                        msgs += f"❌ {desc}: 16:01 타격 실패 ({err_msg})\n"
                    
                if msgs.strip() and chat_id:
                    await _safe_send(context, chat_id, f"🌃 <b>[{html.escape(t)}] 16:01 EST 애프터장 일괄 타격 보고</b>\n\n{msgs.strip()}", parse_mode='HTML')

                # 🚨 NEW: [Case 47 절대 방어망 결속] 자전거래 방어 해제: 암살자 오버나이트 덫 원상 복구
                avwap_state_fresh = await _retry_api(_read_json_safe_sync, avwap_state_file, today_hyphen, default={})
                
                if avwap_state_fresh and avwap_state_fresh.get('qty', 0) > 0:
                    need_to_restore = False
                    if restored_avwap_sell:
                        need_to_restore = True
                    elif avwap_state_fresh.get('suppress_sell', False):
                        need_to_restore = True
                        
                    if need_to_restore:
                        logging.info(f"🛡️ [{t}] 자전거래 방어 해제: 암살자 오버나이트 덫 재장전 집행")
                        avwap_qty_to_restore = int(_safe_float(avwap_state_fresh.get('qty', 0)))
                        avwap_avg_price = _safe_float(avwap_state_fresh.get('avg_price', 0.0))
                        avwap_price_to_restore = math.ceil(avwap_avg_price * 1.01 * 100) / 100.0
                        
                        s_res = await _retry_api(broker.send_order, t, "SELL", avwap_qty_to_restore, avwap_price_to_restore, "LIMIT", timeout=15.0)
                        
                        if isinstance(s_res, dict) and s_res.get('rt_cd') == '0':
                            new_odno = str(s_res.get('odno', ''))
                            avwap_state_fresh['sell_odno'] = new_odno
                            avwap_state_fresh['suppress_sell'] = False
                            await _retry_api(_atomic_write_json_sync, avwap_state_file, avwap_state_fresh, timeout=10.0)
                            
                            if chat_id:
                                await _safe_send(context, chat_id, f"🛡️ <b>[{html.escape(t)} 자전거래 방어 시스템]</b>\n▫️ 본진 애프터장 타격 완료 후 암살자 +1.0% 지정가 익절 덫을 안전하게 재장전했습니다.", parse_mode='HTML')
                        else:
                            err_msg = html.escape(str(s_res.get('msg1', '통신 장애'))) if isinstance(s_res, dict) else '응답 없음'
                            logging.error(f"🚨 [{t}] 암살자 오버나이트 덫 재장전 실패: {err_msg}")
                            if chat_id:
                                await _safe_send(context, chat_id, f"🚨 <b>[{html.escape(t)} 자전거래 방어 시스템 오류]</b>\n▫️ 암살자 매도 덫 재장전에 실패했습니다. 앱을 확인해주세요!\n▫️ 사유: <code>{err_msg}</code>", parse_mode='HTML')

                if state_changed:
                    try:
                        await asyncio.wait_for(asyncio.to_thread(_atomic_write_json_sync, state_file, after_state), timeout=10.0)
                    except Exception as e:
                        logging.error(f"🚨 [{t}] 애프터장 상태 파일 원자적 갱신 실패: {e}")
                    
            except Exception as e:
                logging.error(f"🚨 [{t}] 애프터장 루프 치명적 오류: {e}", exc_info=True)
