# ==========================================================
# FILE: callback_order_handler.py
# ==========================================================
# 🚨 MODIFIED: [주문 통신 전담 도메인] KIS API 수동 주문, 수동 취소, 비상 수혈 로직 100% 분리 락온
# 🚨 MODIFIED: [스냅샷 붕괴 방어망 결속] EXEC 수동 명령 격발 시 발생하는 get_plan() 타임아웃/에러를 흡수하기 위해 try-except 샌드박스를 강제 래핑하여 봇의 치명적 마비(Silent Death)를 완벽 차단.
# 🚨 MODIFIED: [미래 참조(Look-ahead) 데이터 절단] YF 1d 캔들 호출 시, 장마감(16:00 EST) 이전이라면 오늘 생성 중인 라이브 캔들(현재가)을 칼같이 절단(Cut-off)하고 D-1일 공식 MOC 종가만을 100% 핀셋 추출하여 갭상승 캔들 누수 원천 차단.
# 🚨 MODIFIED: [스냅샷 절대주의 사수] EXEC 수동명령어 호출 시 기존 스냅샷을 파기하는 `_nuke_old_snapshot` 로직을 영구 소각하고 `is_snapshot_mode=False`를 강제 래핑하여 락온된 스냅샷을 절대 덮어쓰지 않고 불러오도록 팩트 교정 완료.
# 🚨 MODIFIED: [MOC 공식 종가 오버라이드] KIS의 낡은 종가를 배제하고 YF 공식 종가로 무조건 덮어쓰도록 `<= 0.0` 제약 100% 소각.
# 🚨 MODIFIED: [현재가 보존 락온 복구] 장마감 시에만 현재가(curr)를 전일 종가(prev_close)로 강제 덮어씌워 렌더링 무결성 100% 사수.
# 🚨 MODIFIED: [SyntaxError 붕괴 수술] EMERGENCY_EXEC 내부의 엇갈린 들여쓰기(else)를 팩트 교정하여 무한 크래시 루프 원천 봉쇄 완료.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import os
import math
import asyncio
import yfinance as yf
import html
from telegram import Update
from telegram.ext import ContextTypes

class CallbackOrderHandler:
    def __init__(self, config, broker, strategy, queue_ledger, sync_engine, view, tx_lock):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.queue_ledger = queue_ledger
        self.sync_engine = sync_engine
        self.view = view
        self.tx_lock = tx_lock

    async def handle(self, update: Update, context: ContextTypes.DEFAULT_TYPE, controller, action: str, sub: str, data: list):
        query = update.callback_query
        chat_id = update.effective_chat.id

        if action == "EMERGENCY_REQ":
            ticker = sub
            status_code, _ = await controller.commands_handler._get_market_status()
            if status_code not in ["PRE", "REG"]:
                await query.answer("❌ [격발 차단] 현재 장운영시간(정규장/프리장)이 아닙니다.", show_alert=True)
                return
                
            if not getattr(self, 'queue_ledger', None):
                from queue_ledger import QueueLedger
                self.queue_ledger = await asyncio.to_thread(QueueLedger)
            
            q_data = await asyncio.to_thread(self.queue_ledger.get_queue, ticker) or []
            valid_q_data = [item for item in q_data if isinstance(item, dict)]
            
            total_q = sum(int(float(str(item.get("qty") or 0).replace(',', ''))) for item in valid_q_data)
            
            if total_q == 0 or not valid_q_data:
                await query.answer("⚠️ 큐(Queue)가 텅 비어있어 수혈할 잔여 물량이 없습니다.", show_alert=True)
                return
            
            try:
                await query.answer()
            except Exception:
                pass
            emergency_qty = int(float(str(valid_q_data[-1].get('qty') or 0).replace(',', ''))) 
            emergency_price = float(str(valid_q_data[-1].get('price') or 0.0).replace(',', ''))
            
            msg, markup = self.view.get_emergency_moc_confirm_menu(ticker, emergency_qty, emergency_price)
            try:
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            except Exception:
                pass

        elif action == "EMERGENCY_EXEC":
            ticker = sub
            status_code, _ = await controller.commands_handler._get_market_status()
            
            if status_code not in ["PRE", "REG"]:
                await query.answer("❌ [격발 차단] 현재 장운영시간(정규장/프리장)이 아닙니다.", show_alert=True)
                return
             
            if not getattr(self, 'queue_ledger', None):
                from queue_ledger import QueueLedger
                self.queue_ledger = await asyncio.to_thread(QueueLedger)
     
            q_data = await asyncio.to_thread(self.queue_ledger.get_queue, ticker) or []
            valid_q_data = [item for item in q_data if isinstance(item, dict)]
            
            if not valid_q_data:
                await query.answer("⚠️ 큐(Queue)가 텅 비어있어 수혈할 잔여 물량이 없습니다.", show_alert=True)
                return
            
            try:
                await query.answer("⏳ KIS 서버에 수동 긴급 수혈(MOC) 명령을 격발합니다...", show_alert=False)
            except Exception:
                pass
            
            emergency_qty = int(float(str(valid_q_data[-1].get('qty') or 0).replace(',', ''))) 
            
            if emergency_qty > 0:
                await asyncio.sleep(0.06) 
                async with self.tx_lock:
                    try:
                        res = await asyncio.wait_for(
                            asyncio.to_thread(self.broker.send_order, ticker, "SELL", emergency_qty, 0.0, "MOC"),
                            timeout=10.0
                        )
                    except Exception as e:
                        logging.error(f"🚨 긴급수혈 통신 에러/타임아웃: {e}")
                        res = None
                    
                    # 🚨 MODIFIED: [Syntax 붕괴 수술] else 구문 들여쓰기 100% 정밀 교정
                    if isinstance(res, dict) and str(res.get('rt_cd', '')) == '0':
                        await asyncio.to_thread(self.queue_ledger.pop_lots, ticker, emergency_qty)
                        msg = f"🚨 <b>[{html.escape(str(ticker))}] 수동 긴급 수혈 (Emergency MOC) 격발 완료!</b>\n"
                        msg += f"▫️ 포트폴리오 매니저의 승인 하에 최근 로트 <b>{emergency_qty}주</b>를 시장가(MOC)로 강제 청산했습니다.\n"
                        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                        
                        new_q_data = await asyncio.to_thread(self.queue_ledger.get_queue, ticker) or []
                        new_msg, markup = self.view.get_queue_management_menu(ticker, new_q_data)
                        try:
                            await query.edit_message_text(new_msg, reply_markup=markup, parse_mode='HTML')
                        except Exception:
                            pass
                    else:
                        err_msg = html.escape(str(res.get('msg1') or '알 수 없는 에러')) if isinstance(res, dict) else '응답 없음/통신 장애'
                        try:
                            await query.edit_message_text(f"❌ <b>[{html.escape(str(ticker))}] 수동 긴급 수혈 실패:</b> {err_msg}", parse_mode='HTML')
                        except Exception:
                            pass

        elif action == "EXEC":
            t = sub
            ver = str(await asyncio.to_thread(self.cfg.get_version, t) or "")

            try:
                await query.answer()
                await query.edit_message_text(f"🚀 {html.escape(str(t))} 수동 강제 전송 시작 (최신 잔고 스냅샷 강제 갱신 중)...")
            except Exception:
                pass
            
            async with self.tx_lock:
                holdings = None
                cash = 0.0
                for attempt in range(3):
                    try:
                        await asyncio.sleep(0.06)
                        res = await asyncio.wait_for(asyncio.to_thread(self.broker.get_account_balance), timeout=10.0)
                        cash, holdings = res[0] if isinstance(res, (list, tuple)) and len(res) > 0 else 0.0, res[1] if isinstance(res, (list, tuple)) and len(res) > 1 else {}
                        break
                    except Exception:
                        if attempt == 2: holdings = None
                        else: await asyncio.sleep(1.0 * (2 ** attempt))
                
            if holdings is None:
                try:
                    await query.edit_message_text("❌ API 통신 오류로 잔고를 확인할 수 없어 실행을 차단합니다. 잠시 후 다시 시도해 주세요.")
                except Exception:
                    pass
                return

            try:
                from scheduler_core import get_budget_allocation
                active_tickers_list = await asyncio.to_thread(self.cfg.get_active_tickers) or []
                _, alloc_cash_dict = await asyncio.to_thread(get_budget_allocation, cash, active_tickers_list, self.cfg)
                alloc_cash_dict = alloc_cash_dict or {}
                allocated_budget = float(str(alloc_cash_dict.get(t) or 0.0).replace(',', ''))
            except Exception as e:
                logging.error(f"🚨 예산 할당 모듈 로드 실패 (N빵 강제 분할 폴백): {e}")
                try:
                    active_tickers_list = await asyncio.to_thread(self.cfg.get_active_tickers) or []
                    div_count = max(1, len(active_tickers_list))
                except Exception:
                    div_count = 1
                allocated_budget = float(str(cash).replace(',', '')) / div_count  
            
            if not isinstance(holdings, dict):
                holdings = {}
            h = holdings.get(t) or {'qty':0, 'avg':0}
            
            curr_p, prev_c = 0.0, 0.0
            for attempt in range(3):
                try:
                    await asyncio.sleep(0.06)
                    curr_p_val = await asyncio.wait_for(asyncio.to_thread(self.broker.get_current_price, t), timeout=10.0)
                    curr_p = float(str(curr_p_val or 0.0).replace(',', ''))
                    prev_c_val = await asyncio.wait_for(asyncio.to_thread(self.broker.get_previous_close, t), timeout=10.0)
                    prev_c = float(str(prev_c_val or 0.0).replace(',', ''))
                    break
                except Exception:
                    if attempt == 2: pass
                    else: await asyncio.sleep(1.0 * (2 ** attempt))
                    
            safe_avg = float(str(h.get('avg') or 0.0).replace(',', '')) 
            safe_qty = max(0, int(float(str(h.get('qty') or 0).replace(',', ''))))
            
            status_code, _ = await controller.commands_handler._get_market_status()
            if status_code in ["AFTER", "CLOSE", "PRE"]:
                try:
                    def get_exact_prev_close(ticker_name):
                        time.sleep(0.06)
                        df = yf.Ticker(ticker_name).history(period="5d", interval="1d", timeout=5)
                        if not df.empty and 'Close' in df.columns:
                            tz_est = ZoneInfo('America/New_York')
                            tz_now = datetime.datetime.now(tz_est)
                            cutoff_date = tz_now.date()
                            if tz_now.time() <= datetime.time(16, 0, 30):
                                cutoff_date -= datetime.timedelta(days=1)
                            
                            if df.index.tzinfo is None:
                                df.index = df.index.tz_localize('UTC').tz_convert(tz_est)
                            else:
                                df.index = df.index.tz_convert(tz_est)
                                
                            past_df = df[df.index.date <= cutoff_date]
                            if not past_df.empty:
                                val = float(past_df['Close'].iloc[-1])
                                return val if not math.isnan(val) else None
                        return None
                    
                    yf_close = None
                    for attempt in range(3):
                        try:
                            yf_close = await asyncio.wait_for(asyncio.to_thread(get_exact_prev_close, t), timeout=10.0)
                            break
                        except Exception:
                            if attempt == 2: pass
                            else: await asyncio.sleep(1.0 * (2 ** attempt))
                    
                    if yf_close and yf_close > 0:
                        prev_c = yf_close
                except Exception as e:
                    logging.debug(f"YF 정규장 종가 롤오버 스캔 실패 ({t}): {e}")
                
                if status_code == "CLOSE":
                    curr_p = prev_c
          
            ma_5day = 0.0
            for attempt in range(3):
                try:
                    await asyncio.sleep(0.06)
                    ma_5day_val = await asyncio.wait_for(asyncio.to_thread(self.broker.get_5day_ma, t), timeout=10.0)
                    ma_5day = float(str(ma_5day_val or 0.0).replace(',', ''))
                    break
                except Exception:
                    if attempt == 2: ma_5day = 0.0
                    else: await asyncio.sleep(1.0 * (2 ** attempt))
                    
            is_manual_vwap = await asyncio.to_thread(getattr(self.cfg, 'get_manual_vwap_mode', lambda x: False), t)
            
            try:
                plan = await asyncio.to_thread(self.strategy.get_plan, t, curr_p, safe_avg, safe_qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_budget, is_simulation=True, is_snapshot_mode=False)
            except Exception as e:
                logging.error(f"🚨 [{t}] 수동 전송 플랜 생성 에러 (샌드박스 방어): {e}")
                plan = {}
            
            if not isinstance(plan, dict):
                plan = {}

            icon = "⚖️" if ver == "V_REV" else "💎"
            title = f"{icon} <b>[{html.escape(str(t))}] 예방적 덫 수동 주문 실행</b>\n"
            msg = title
            all_success = True
       
            target_orders = plan.get('core_orders') or plan.get('orders') or []
            if not isinstance(target_orders, list): target_orders = []
            
            is_market_active_now = status_code in ["PRE", "REG", "AFTER"]
            
            est_z = ZoneInfo('America/New_York')
            kst_z = ZoneInfo('Asia/Seoul')
            curr_est = datetime.datetime.now(est_z)
            
            b_start = curr_est.replace(hour=15, minute=26, second=0, microsecond=0)
            s_start = curr_est + datetime.timedelta(minutes=3)
            a_start = max(b_start, s_start)
            b_end = curr_est.replace(hour=15, minute=56, second=0, microsecond=0)
            
            dyn_start_t = a_start.astimezone(kst_z).strftime("%H%M%S")
            dyn_end_t = b_end.astimezone(kst_z).strftime("%H%M%S")

            for o in target_orders:
                if not isinstance(o, dict): continue
                try:
                    await asyncio.sleep(0.06)
                    if str(o.get('type', '')) == 'VWAP' or is_market_active_now:
                        res = await asyncio.wait_for(
                            asyncio.to_thread(
                                self.broker.send_order, 
                                t, str(o.get('side', '')), int(float(str(o.get('qty') or 0).replace(',', ''))), float(str(o.get('price') or 0.0).replace(',', '')), str(o.get('type', '')),
                                start_time=dyn_start_t if str(o.get('type', '')) == 'VWAP' else None,
                                end_time=dyn_end_t if str(o.get('type', '')) == 'VWAP' else None
                            ),
                            timeout=10.0
                        )
                    else:
                        res = await asyncio.wait_for(
                            asyncio.to_thread(
                                self.broker.send_reservation_order, 
                                t, str(o.get('side', '')), int(float(str(o.get('qty') or 0).replace(',', ''))), float(str(o.get('price') or 0.0).replace(',', '')), str(o.get('type', ''))
                            ),
                            timeout=10.0
                        )
                except Exception as e:
                    logging.error(f"🚨 V14/VREV 1차 덫 장전 통신 에러/타임아웃: {e}")
                    res = None
            
                is_success = isinstance(res, dict) and str(res.get('rt_cd', '')) == '0'
                if not is_success:
                    all_success = False
                
                err_msg = html.escape(str(res.get('msg1') or '오류')) if isinstance(res, dict) else '응답 없음/통신 장애'
                status_icon = '✅' if is_success else f'❌({err_msg})'
                msg += f"└ 1차 필수: {html.escape(str(o.get('desc', '')))} {int(float(str(o.get('qty') or 0).replace(',', '')))}주: {status_icon}\n"
                await asyncio.sleep(0.2) 
            
            target_bonus = plan.get('bonus_orders') or []
            if not isinstance(target_bonus, list): target_bonus = []
            
            for o in target_bonus:
                if not isinstance(o, dict): continue
                try:
                    await asyncio.sleep(0.06)
                    if str(o.get('type', '')) == 'VWAP' or is_market_active_now:
                        res = await asyncio.wait_for(
                            asyncio.to_thread(
                                self.broker.send_order, 
                                t, str(o.get('side', '')), int(float(str(o.get('qty') or 0).replace(',', ''))), float(str(o.get('price') or 0.0).replace(',', '')), str(o.get('type', '')),
                                start_time=dyn_start_t if str(o.get('type', '')) == 'VWAP' else None,
                                end_time=dyn_end_t if str(o.get('type', '')) == 'VWAP' else None
                            ),
                            timeout=10.0
                        )
                    else:
                        res = await asyncio.wait_for(
                            asyncio.to_thread(
                                self.broker.send_reservation_order, 
                                t, str(o.get('side', '')), int(float(str(o.get('qty') or 0).replace(',', ''))), float(str(o.get('price') or 0.0).replace(',', '')), str(o.get('type', ''))
                            ),
                            timeout=10.0
                        )
                except Exception as e:
                    logging.error(f"🚨 V14/VREV 2차 보너스 덫 장전 통신 에러/타임아웃: {e}")
                    res = None
        
                is_success = isinstance(res, dict) and str(res.get('rt_cd', '')) == '0'
                err_msg = html.escape(str(res.get('msg1') or '잔금패스')) if isinstance(res, dict) else '응답 없음/통신 장애'
                status_icon = '✅' if is_success else f'❌({err_msg})'
                msg += f"└ 2차 보너스: {html.escape(str(o.get('desc', '')))} {int(float(str(o.get('qty') or 0).replace(',', '')))}주: {status_icon}\n"
                await asyncio.sleep(0.2) 
            
            if len(target_orders) == 0 and len(target_bonus) == 0:
                 msg += "\n💤 <b>장전할 주문이 없습니다 (관망/예산소진)</b>"
            elif all_success and len(target_orders) > 0:
                await asyncio.to_thread(self.cfg.set_lock, t, "REG")
                msg += "\n🔒 <b>필수 주문 전송 완료 (잠금 설정됨)</b>"
            else:
                msg += "\n⚠️ <b>일부 필수 주문 실패 (매매 잠금 보류)</b>"

            await context.bot.send_message(chat_id, msg, parse_mode='HTML')

        elif action == "CANCEL_EXEC":
            t = sub
            try:
                await query.answer()
                await query.edit_message_text(f"🛑 <b>[{html.escape(str(t))}] 수동 매매(일반/예약 덫) 취소 집행 중...</b>", parse_mode='HTML')
            except Exception:
                pass
            
            nuked_count = 0
            err_count = 0
            
            try:
                est_now = datetime.datetime.now(ZoneInfo('America/New_York'))
                d_str = est_now.strftime('%Y%m%d')
                
                resv_orders = []
                for attempt in range(3):
                    try:
                        await asyncio.sleep(0.06)
                        resv_orders = await asyncio.wait_for(
                            asyncio.to_thread(self.broker.get_reservation_orders, t, d_str, d_str),
                            timeout=10.0
                        )
                        break
                    except Exception:
                        if attempt == 2: resv_orders = []
                        else: await asyncio.sleep(1.0 * (2 ** attempt))
                
                if resv_orders and isinstance(resv_orders, list):
                    for req in resv_orders:
                        if not isinstance(req, dict): continue
                        odno = str(req.get('ovrs_rsvn_odno') or req.get('odno') or '')
                        ord_dt = str(req.get('rsvn_ord_rcit_dt') or req.get('ord_dt') or d_str)
                        if odno:
                            try:
                                await asyncio.sleep(0.06)
                                await asyncio.wait_for(
                                    asyncio.to_thread(self.broker.cancel_reservation_order, ord_dt, odno),
                                    timeout=10.0
                                )
                                nuked_count += 1
                                await asyncio.sleep(0.2)
                            except Exception as e:
                                logging.error(f"🚨 [{t}] 수동 예약 덫 취소 실패: {e}")
                                err_count += 1
            except Exception as e:
                err_count += 1

            try:
                unfilled = []
                for attempt in range(3):
                    try:
                        await asyncio.sleep(0.06)
                        unfilled = await asyncio.wait_for(
                            asyncio.to_thread(self.broker.get_unfilled_orders_detail, t),
                            timeout=10.0
                        )
                        break
                    except Exception:
                        if attempt == 2: unfilled = []
                        else: await asyncio.sleep(1.0 * (2 ** attempt))
                        
                if unfilled and isinstance(unfilled, list):
                    for uo in unfilled:
                        if not isinstance(uo, dict): continue
                        u_odno = str(uo.get('odno') or '')
                        if u_odno:
                            try:
                                await asyncio.sleep(0.06)
                                await asyncio.wait_for(
                                    asyncio.to_thread(self.broker.cancel_order, t, u_odno),
                                    timeout=10.0
                                )
                                nuked_count += 1
                                await asyncio.sleep(0.2)
                            except Exception as e:
                                logging.error(f"🚨 [{t}] 수동 일반 덫 취소 실패: {e}")
                                err_count += 1
            except Exception as e:
                err_count += 1

            if nuked_count > 0:
                await asyncio.to_thread(self.cfg.reset_lock_for_ticker, t)

            if err_count > 0:
                await context.bot.send_message(chat_id, f"⚠️ <b>[{html.escape(str(t))}] 수동 취소 완료 (일부 오류 발생)</b>\n▫️ 총 <b>{nuked_count}건</b>의 덫을 파기하고 매매 잠금을 해제했으나, {err_count}건의 오류가 발생했습니다.", parse_mode='HTML')
            elif nuked_count > 0:
                await context.bot.send_message(chat_id, f"🛑 <b>[{html.escape(str(t))}] 수동 취소 팩트 집행 완료</b>\n▫️ 총 <b>{nuked_count}건</b>의 미체결 및 예약 덫을 100% 파기(Nuke)하고 당일 매매 잠금을 <b>해제(Unlock)</b>했습니다.", parse_mode='HTML')
            else:
                await context.bot.send_message(chat_id, f"ℹ️ <b>[{html.escape(str(t))}] 수동 취소 결과</b>\n▫️ 취소할 덫이 없습니다.", parse_mode='HTML')
