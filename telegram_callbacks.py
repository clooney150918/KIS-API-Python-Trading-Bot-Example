# ==========================================================
# FILE: telegram_callbacks.py
# ==========================================================
# 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
# 제1헌법: queue_ledger.get_queue 등 모든 파일 I/O 및 락 점유 메서드는 무조건 asyncio.to_thread로 래핑하여 이벤트 루프 교착(Deadlock)을 원천 차단함.
# 🚨 MODIFIED: [통합 지시서 수동 제어(EXEC/CANCEL) 완벽 스위칭 작전]
# - CANCEL_EXEC 덫 파기 완료 시(nuked_count > 0), 당일 매매 잠금(REG Lock)을 강제로 해제하도록
#   cfg.reset_lock_for_ticker를 비동기로 호출하는 무결성 락온 파이프라인 개통 완료.
# 🚨 MODIFIED: [V75.11 수동주문 시장 국면 다이내믹 라우팅 락온 (✨사용자 제보 박제)]
# - 수동 주문(EXEC) 시, 미국 정규장/프리장 시간대(PRE, REG, AFTER)라면 KIS 예약주문 시간 
#   제약(10:00~22:20 KST)에 걸려 리젝당하는 맹점 완벽 해체.
# - 장중에는 VWAP, LOC 등 모든 주문을 무조건 일반주문(send_order)으로 강제 직결하고, 
#   장마감(CLOSE) 시간대에만 예약주문 API를 타도록 스마트 폴백 엔진 이식 완료.
# ==========================================================
import logging
import datetime
from zoneinfo import ZoneInfo
import os
import json
import time
import math
import asyncio
import tempfile
import yfinance as yf
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

class TelegramCallbacks:
    def __init__(self, config, broker, strategy, queue_ledger, sync_engine, view, tx_lock):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.queue_ledger = queue_ledger
        self.sync_engine = sync_engine
        self.view = view
        self.tx_lock = tx_lock

    async def _get_max_holdings_qty(self, ticker, kis_qty):
        v14_qty = 0
        vrev_qty = 0
        
        try:
            ledger = await asyncio.to_thread(self.cfg.get_ledger)
            net = 0
            for r in ledger:
                if r.get('ticker') == ticker:
                    q = int(float(r.get('qty', 0)))
                    net += q if r.get('side') == 'BUY' else -q
            v14_qty = max(0, net)
        except Exception:
            pass

        try:
            if getattr(self, 'queue_ledger', None):
                q_data = await asyncio.to_thread(self.queue_ledger.get_queue, ticker)
                vrev_qty = sum(int(float(lot.get('qty', 0))) for lot in q_data if int(float(lot.get('qty', 0))) > 0)
        except Exception:
            pass

        return max(kis_qty, v14_qty, vrev_qty)

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE, controller):
        query = update.callback_query
        chat_id = update.effective_chat.id
        data = query.data.split(":")
        action, sub = data[0], data[1] if len(data) > 1 else ""

        if action == "UPDATE":
            await query.answer()
            if sub == "CONFIRM":
                from plugin_updater import SystemUpdater
                updater = SystemUpdater()
                await query.edit_message_text("⏳ <b>[업데이트 승인됨]</b> GitHub 코드를 강제 페칭합니다...", parse_mode='HTML')
                try:
                    success, msg = await updater.pull_latest_code()
                    import html
                    safe_msg = html.escape(msg)
                    if success:
                        await query.edit_message_text(f"✅ <b>[업데이트 완료]</b> {safe_msg}\n\n🔄 데몬을 재가동합니다. 잠시 후 봇이 응답할 것입니다.", parse_mode='HTML')
                        await updater.restart_daemon()
                    else:
                        await query.edit_message_text(f"❌ <b>[동기화 실패]</b>\n▫️ 사유: {safe_msg}", parse_mode='HTML')
                except Exception as e:
                    import html
                    safe_err = html.escape(str(e))
                    await query.edit_message_text(f"🚨 <b>[치명적 오류]</b> 프로세스 예외 발생: {safe_err}", parse_mode='HTML')

            elif sub == "CANCEL":
                await query.edit_message_text("❌ 자가 업데이트를 취소했습니다.", parse_mode='HTML')

        elif action == "QUEUE":
            await query.answer()
            if sub == "VIEW":
                ticker = data[2]
                if getattr(self, 'queue_ledger', None):
                    q_data = await asyncio.to_thread(self.queue_ledger.get_queue, ticker)
                else:
                    q_data = []
             
                msg, markup = self.view.get_queue_management_menu(ticker, q_data)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')

        elif action == "EMERGENCY_REQ":
            ticker = sub
            status_code, _ = await controller._get_market_status()
            if status_code not in ["PRE", "REG"]:
                await query.answer("❌ [격발 차단] 현재 장운영시간(정규장/프리장)이 아닙니다.", show_alert=True)
                return
                
            if not getattr(self, 'queue_ledger', None):
                from queue_ledger import QueueLedger
                self.queue_ledger = QueueLedger()
            
            q_data = await asyncio.to_thread(self.queue_ledger.get_queue, ticker)
            total_q = sum(item.get("qty", 0) for item in q_data)
            
            if total_q == 0:
                await query.answer("⚠️ 큐(Queue)가 텅 비어있어 수혈할 잔여 물량이 없습니다.", show_alert=True)
                return
            
            await query.answer()
            emergency_qty = q_data[-1].get('qty', 0)
            emergency_price = q_data[-1].get('price', 0.0)
            
            msg, markup = self.view.get_emergency_moc_confirm_menu(ticker, emergency_qty, emergency_price)
            await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')

        elif action == "EMERGENCY_EXEC":
            ticker = sub
            status_code, _ = await controller._get_market_status()
            
            if status_code not in ["PRE", "REG"]:
                await query.answer("❌ [격발 차단] 현재 장운영시간(정규장/프리장)이 아닙니다.", show_alert=True)
                return
             
            if not getattr(self, 'queue_ledger', None):
                from queue_ledger import QueueLedger
                self.queue_ledger = QueueLedger()
     
            q_data = await asyncio.to_thread(self.queue_ledger.get_queue, ticker)
            if not q_data:
                await query.answer("⚠️ 큐(Queue)가 텅 비어있어 수혈할 잔여 물량이 없습니다.", show_alert=True)
                return
            
            await query.answer("⏳ KIS 서버에 수동 긴급 수혈(MOC) 명령을 격발합니다...", show_alert=False)
            
            emergency_qty = q_data[-1].get('qty', 0)
            
            if emergency_qty > 0:
                async with self.tx_lock:
                    res = await asyncio.to_thread(self.broker.send_order, ticker, "SELL", emergency_qty, 0.0, "MOC")
                    
                    if res.get('rt_cd') == '0':
                        await asyncio.to_thread(self.queue_ledger.pop_lots, ticker, emergency_qty)
                        msg = f"🚨 <b>[{ticker}] 수동 긴급 수혈 (Emergency MOC) 격발 완료!</b>\n"
                        msg += f"▫️ 포트폴리오 매니저의 승인 하에 최근 로트 <b>{emergency_qty}주</b>를 시장가(MOC)로 강제 청산했습니다.\n"
                        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                        
                        new_q_data = await asyncio.to_thread(self.queue_ledger.get_queue, ticker)
                        new_msg, markup = self.view.get_queue_management_menu(ticker, new_q_data)
                        await query.edit_message_text(new_msg, reply_markup=markup, parse_mode='HTML')
                    else:
                        err_msg = res.get('msg1', '알 수 없는 에러')
                        await query.edit_message_text(f"❌ <b>[{ticker}] 수동 긴급 수혈 실패:</b> {err_msg}", parse_mode='HTML')

        elif action == "DEL_REQ":
            await query.answer()
            ticker = sub
            target_date = ":".join(data[2:])
            
            if getattr(self, 'queue_ledger', None):
                 q_data = await asyncio.to_thread(self.queue_ledger.get_queue, ticker)
            else:
                q_data = []
             
            qty, price = 0, 0.0
            for item in q_data:
                 if item.get('date') == target_date:
                    qty = item.get('qty', 0)
                    price = item.get('price', 0.0)
                    break
        
            msg, markup = self.view.get_queue_action_confirm_menu(ticker, target_date, qty, price)
            await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')

        elif action in ["DEL_Q", "EDIT_Q"]:
            ticker = sub
            target_date = ":".join(data[2:])
            
            try:
                if action == "DEL_Q":
                    if getattr(self, 'queue_ledger', None):
                         await asyncio.to_thread(self.queue_ledger.delete_lot, ticker, target_date)
                     
                    await query.answer("✅ 지층 삭제 완료. KIS 원장과 동기화합니다.", show_alert=False)
                    if ticker not in self.sync_engine.sync_locks:
                        self.sync_engine.sync_locks[ticker] = asyncio.Lock()
                    if not self.sync_engine.sync_locks[ticker].locked():
                        await self.sync_engine.process_auto_sync(ticker, chat_id, context, silent_ledger=True)
        
                    final_q = await asyncio.to_thread(self.queue_ledger.get_queue, ticker) if getattr(self, 'queue_ledger', None) else []
                    msg, markup = self.view.get_queue_management_menu(ticker, final_q)
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            
                elif action == "EDIT_Q":
                    await query.answer("✏️ 수정 모드 진입", show_alert=False)
                    short_date = target_date[:10]
                    controller.user_states[chat_id] = f"EDITQ_{ticker}_{target_date}"
                     
                    prompt = f"✏️ <b>[{ticker} 지층 수정 모드]</b>\n"
                    prompt += f"선택하신 <b>[{short_date}]</b> 지층을 재설정합니다.\n\n"
                    prompt += "새로운 <b>[수량]</b>과 <b>[평단가]</b>를 띄어쓰기로 입력하세요.\n"
                    prompt += "(예: <code>229 52.16</code>)\n\n"
                    prompt += "<i>(입력을 취소하려면 숫자 이외의 문자를 보내주세요)</i>"
                    await query.edit_message_text(prompt, parse_mode='HTML')
            except Exception as e:
                await query.answer(f"❌ 처리 중 에러 발생: {e}", show_alert=True)

        elif action == "VERSION":
            await query.answer()
            history_data = await asyncio.to_thread(self.cfg.get_full_version_history)
            if sub == "LATEST":
                msg, markup = self.view.get_version_message(history_data, page_index=None)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            elif sub == "PAGE":
                page_idx = int(data[2])
                msg, markup = self.view.get_version_message(history_data, page_index=page_idx)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
      
        elif action == "RESET":
            await query.answer()
            if sub == "MENU":
                active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
                msg, markup = self.view.get_reset_menu(active_tickers)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            elif sub == "LOCK": 
                ticker = data[2]
                await asyncio.to_thread(self.cfg.reset_lock_for_ticker, ticker)
                await query.edit_message_text(f"✅ <b>[{ticker}] 금일 매매 잠금이 해제되었습니다.</b>", parse_mode='HTML')
            elif sub == "REV":
                ticker = data[2]
                msg, markup = self.view.get_reset_confirm_menu(ticker)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            elif sub == "CONFIRM":
                ticker = data[2]
                
                current_ver = await asyncio.to_thread(self.cfg.get_version, ticker)
                is_rev_active = (current_ver == "V_REV")
                await asyncio.to_thread(self.cfg.set_reverse_state, ticker, is_rev_active, 0)
                
                await asyncio.to_thread(self.cfg.clear_escrow_cash, ticker)
             
                ledger = await asyncio.to_thread(self.cfg.get_ledger)
                ledger_data = [r for r in ledger if r.get('ticker') != ticker]
                await asyncio.to_thread(self.cfg._save_json, self.cfg.FILES["LEDGER"], ledger_data)
                
                def _process_reset_files():
                    backup_file = self.cfg.FILES["LEDGER"].replace(".json", "_backup.json")
                    if os.path.exists(backup_file):
                        try:
                            with open(backup_file, 'r', encoding='utf-8') as f:
                                b_data = json.load(f)
                            b_data = [r for r in b_data if r.get('ticker') != ticker]
                        
                            fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(backup_file) or '.')
                            with os.fdopen(fd, 'w', encoding='utf-8') as f_out:
                                json.dump(b_data, f_out, ensure_ascii=False, indent=4)
                                f_out.flush()
                                os.fsync(f_out.fileno())
                            os.replace(tmp_path, backup_file)
                        except Exception:
                            pass
                             
                await asyncio.to_thread(_process_reset_files)
            
                if getattr(self, 'queue_ledger', None):
                    await asyncio.to_thread(self.queue_ledger.clear_queue, ticker)
            
                await query.edit_message_text(f"✅ <b>[{ticker}] 삼위일체 소각(Nuke) 및 초기화 완료!</b>\n▫️ 본장부, 백업장부, 큐(Queue), 에스크로의 찌꺼기 데이터가 100% 영구 삭제되었습니다.\n▫️ 다음 매수 진입 시 0주 새출발 디커플링 타점 모드로 완벽히 재시작합니다.", parse_mode='HTML')
       
            elif sub == "CANCEL":
                 await query.edit_message_text("❌ 닫았습니다.", parse_mode='HTML')

        elif action == "REC":
            await query.answer()
            if sub == "VIEW": 
                async with self.tx_lock:
                     _, holdings = await asyncio.to_thread(self.broker.get_account_balance)
                await self.sync_engine._display_ledger(data[2], chat_id, context, query=query, pre_fetched_holdings=holdings)
            elif sub == "SYNC": 
                ticker = data[2]
          
                if ticker not in self.sync_engine.sync_locks:
                    self.sync_engine.sync_locks[ticker] = asyncio.Lock()
                     
                if not self.sync_engine.sync_locks[ticker].locked():
                     await query.edit_message_text(f"🔄 <b>[{ticker}] 잔고 기반 대시보드 업데이트 중...</b>", parse_mode='HTML')
                     res = await self.sync_engine.process_auto_sync(ticker, chat_id, context, silent_ledger=True)
                     if res == "SUCCESS": 
                         async with self.tx_lock:
                            _, holdings = await asyncio.to_thread(self.broker.get_account_balance)
                         await self.sync_engine._display_ledger(ticker, chat_id, context, message_obj=query.message, pre_fetched_holdings=holdings)

        elif action == "HIST":
            await query.answer()
            if sub == "VIEW":
                hid = int(data[2])
                hist_data = await asyncio.to_thread(self.cfg.get_history)
                target = next((h for h in hist_data if h['id'] == hid), None)
                if target:
                    safe_trades = target.get('trades', [])
                    for t_rec in safe_trades:
                        if 'ticker' not in t_rec:
                            t_rec['ticker'] = target['ticker']
                        if 'side' not in t_rec:
                              t_rec['side'] = 'BUY'
                      
                    qty, avg, invested, sold = await asyncio.to_thread(self.cfg.calculate_holdings, target['ticker'], safe_trades)
  
                    try:
                        msg, markup = self.view.create_ledger_dashboard(target['ticker'], qty, avg, invested, sold, safe_trades, 0, 0, is_history=True, history_id=hid)
                    except TypeError:
                        msg, markup = self.view.create_ledger_dashboard(target['ticker'], qty, avg, invested, sold, safe_trades, 0, 0, is_history=True)
                        
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
             
            elif sub == "LIST":
                if hasattr(controller, 'cmd_history'):
                    await controller.cmd_history(update, context)

            elif sub == "IMG":
                ticker = data[2]
                target_id = int(data[3]) if len(data) > 3 else None
                
                hist_data = await asyncio.to_thread(self.cfg.get_history)
                hist_list = [h for h in hist_data if h['ticker'] == ticker]
                 
                if not hist_list:
                    await context.bot.send_message(chat_id, f"📭 <b>[{ticker}]</b> 발급 가능한 졸업 기록이 존재하지 않습니다.", parse_mode='HTML')
                    return
                
                target_hist = None
                if target_id:
                    target_hist = next((h for h in hist_list if h.get('id') == target_id), None)
                
                if not target_hist:
                    target_hist = sorted(hist_list, key=lambda x: x.get('end_date', ''), reverse=True)[0]
                
                try:
                    await query.edit_message_text(f"🎨 <b>[{ticker}] 프리미엄 졸업 카드를 렌더링 중입니다...</b>", parse_mode='HTML')

                    img_path = await asyncio.to_thread(
                        self.view.create_profit_image,
                        ticker=target_hist['ticker'],
                        profit=target_hist['profit'],
                        yield_pct=target_hist['yield'],
                        invested=target_hist['invested'],
                        revenue=target_hist['revenue'],
                        end_date=target_hist['end_date']
                    )
            
                    if img_path and os.path.exists(img_path):
                        with open(img_path, 'rb') as f_out:
                            if img_path.lower().endswith('.gif'):
                                await context.bot.send_animation(chat_id=chat_id, animation=f_out)
                            else:
                                 await context.bot.send_photo(chat_id=chat_id, photo=f_out)
                        await query.delete_message()
                    else:
                        await query.edit_message_text("❌ 이미지 생성에 실패했습니다.", parse_mode='HTML')
                except Exception as e:
                    logging.error(f"📸 👑 졸업 이미지 생성/발송 실패: {e}")
                    await query.edit_message_text("❌ 이미지 생성 중 오류가 발생했습니다.", parse_mode='HTML')
            
        elif action == "EXEC":
            t = sub
            ver = await asyncio.to_thread(self.cfg.get_version, t)

            await query.answer()
            await query.edit_message_text(f"🚀 {t} 수동 강제 전송 시작 (최신 잔고 스냅샷 강제 갱신 중)...")
            
            async with self.tx_lock:
                cash, holdings = await asyncio.to_thread(self.broker.get_account_balance)
                
            if holdings is None:
                return await query.edit_message_text("❌ API 통신 오류로 잔고를 확인할 수 없어 실행을 차단합니다. 잠시 후 다시 시도해 주세요.")

            # 🚨 NEW: [스냅샷 강제 갱신 (Snapshot Override) 락온]
            def _nuke_old_snapshot():
                est = ZoneInfo('America/New_York')
                now_est = datetime.datetime.now(est)
                if now_est.hour < 4 or (now_est.hour == 4 and now_est.minute < 4):
                    target_date = now_est - datetime.timedelta(days=1)
                else:
                    target_date = now_est
                today_str = target_date.strftime("%Y-%m-%d")
                
                for prefix in ["REV", "V14VWAP", "V14"]:
                    fpath = f"data/daily_snapshot_{prefix}_{today_str}_{t}.json"
                    if os.path.exists(fpath):
                        try: os.remove(fpath)
                        except: pass
            
            await asyncio.to_thread(_nuke_old_snapshot)

            active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
            
            from scheduler_core import get_budget_allocation
            _, allocated_cash = await asyncio.to_thread(get_budget_allocation, cash, active_tickers, self.cfg)
            
            h = holdings.get(t, {'qty':0, 'avg':0})
            curr_p = float(await asyncio.to_thread(self.broker.get_current_price, t) or 0.0)
            prev_c = float(await asyncio.to_thread(self.broker.get_previous_close, t) or 0.0)
            safe_avg = float(h.get('avg') or 0.0)
            safe_qty = int(float(h.get('qty') or 0))
            
            status_code, _ = await controller._get_market_status()
            if status_code in ["AFTER", "CLOSE", "PRE"]:
                try:
                    def get_yf_close():
                        df = yf.Ticker(t).history(period="5d", interval="1d")
                        return float(df['Close'].iloc[-1]) if not df.empty else None
                    yf_close = await asyncio.wait_for(asyncio.to_thread(get_yf_close), timeout=3.0)
                    if yf_close and yf_close > 0:
                        prev_c = yf_close
                except Exception as e:
                    logging.debug(f"YF 정규장 종가 롤오버 스캔 실패 ({t}): {e}")
                if curr_p > 0 and prev_c == 0.0:
                    prev_c = curr_p
         
            ma_5day = await asyncio.to_thread(self.broker.get_5day_ma, t)
            is_manual_vwap = await asyncio.to_thread(getattr(self.cfg, 'get_manual_vwap_mode', lambda x: False), t)
            
            logic_qty_v14 = safe_qty
            plan = await asyncio.to_thread(self.strategy.get_plan, t, curr_p, safe_avg, logic_qty_v14, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash.get(t, 0.0), is_simulation=True, is_snapshot_mode=True)
            
            if safe_qty == 0:
                for o in plan.get('core_orders', []):
                    if o['side'] == 'BUY' and 'Buy1' in o.get('desc', ''):
                        o['price'] = round(prev_c * 1.15, 2)

            icon = "⚖️" if ver == "V_REV" else "💎"
            title = f"{icon} <b>[{t}] 예방적 덫 수동 주문 실행</b>\n"
            msg = title
            all_success = True
       
            target_orders = plan.get('core_orders', plan.get('orders', []))
            
            # 🚨 MODIFIED: [V75.11 수동주문 시장 국면 다이내믹 라우팅 락온]
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
                if o['type'] in ["VWAP", "LOC", "LIMIT"] or is_market_active_now:
                    res = await asyncio.to_thread(
                        self.broker.send_order, 
                        t, o['side'], o['qty'], o['price'], o['type'],
                        start_time=dyn_start_t if o['type'] == 'VWAP' else None,
                        end_time=dyn_end_t if o['type'] == 'VWAP' else None
                    )
                else:
                    res = await asyncio.to_thread(
                        self.broker.send_reservation_order, 
                        t, o['side'], o['qty'], o['price'], o['type']
                    )
                
                is_success = res.get('rt_cd') == '0'
                if not is_success:
                    all_success = False
                
                err_msg = res.get('msg1', '오류')
                status_icon = '✅' if is_success else f'❌({err_msg})'
                msg += f"└ 1차 필수: {o['desc']} {o['qty']}주: {status_icon}\n"
                await asyncio.sleep(0.2) 
            
            target_bonus = plan.get('bonus_orders', [])
            for o in target_bonus:
                if o['type'] in ["VWAP", "LOC", "LIMIT"] or is_market_active_now:
                    res = await asyncio.to_thread(
                        self.broker.send_order, 
                        t, o['side'], o['qty'], o['price'], o['type'],
                        start_time=dyn_start_t if o['type'] == 'VWAP' else None,
                        end_time=dyn_end_t if o['type'] == 'VWAP' else None
                    )
                else:
                    res = await asyncio.to_thread(
                        self.broker.send_reservation_order, 
                        t, o['side'], o['qty'], o['price'], o['type']
                    )
                 
                is_success = res.get('rt_cd') == '0'
                err_msg = res.get('msg1', '잔금패스')
                status_icon = '✅' if is_success else f'❌({err_msg})'
                msg += f"└ 2차 보너스: {o['desc']} {o['qty']}주: {status_icon}\n"
                await asyncio.sleep(0.2) 
            
            if len(target_orders) == 0 and len(target_bonus) == 0:
                 msg += "\n💤 <b>장전할 주문이 없습니다 (관망/예산소진)</b>"
            elif all_success and len(target_orders) > 0:
                await asyncio.to_thread(self.cfg.set_lock, t, "REG")
                msg += "\n🔒 <b>필수 주문 전송 완료 (잠금 설정됨)</b>"
            else:
                msg += "\n⚠️ <b>일부 필수 주문 실패 (매매 잠금 보류)</b>"

            await context.bot.send_message(chat_id, msg, parse_mode='HTML')

        # NEW: [통합 지시서 수동 매매 취소 버튼 탑재 및 KIS 다이렉트 팩트 취소 라우팅 개통]
        elif action == "CANCEL_EXEC":
            t = sub
            await query.answer()
            await query.edit_message_text(f"🛑 <b>[{t}] 수동 매매(일반/예약 덫) 취소 집행 중...</b>", parse_mode='HTML')
            
            nuked_count = 0
            err_count = 0
            
            # 1. 예약 원장 덫 파기 (제19경고 준수)
            try:
                est_now = datetime.datetime.now(ZoneInfo('America/New_York'))
                d_str = est_now.strftime('%Y%m%d')
                
                resv_orders = await asyncio.wait_for(
                    asyncio.to_thread(self.broker.get_reservation_orders, t, d_str, d_str),
                    timeout=10.0
                )
                
                if resv_orders and isinstance(resv_orders, list):
                    for req in resv_orders:
                        odno = req.get('ovrs_rsvn_odno') or req.get('odno')
                        ord_dt = req.get('rsvn_ord_rcit_dt') or req.get('ord_dt', d_str)
                        if odno:
                            try:
                                await asyncio.to_thread(self.broker.cancel_reservation_order, ord_dt, odno)
                                nuked_count += 1
                                await asyncio.sleep(0.2)
                            except Exception as e:
                                logging.error(f"🚨 [{t}] 수동 예약 덫 취소 실패: {e}")
                                err_count += 1
            except asyncio.TimeoutError:
                logging.error(f"🚨 [{t}] 예약 덫 스캔 타임아웃")
                err_count += 1
            except Exception as e:
                logging.error(f"🚨 [{t}] 예약 덫 스캔 에러: {e}")
                err_count += 1

            # 2. 일반 미체결 덫 파기
            try:
                unfilled = await asyncio.wait_for(
                    asyncio.to_thread(self.broker.get_unfilled_orders_detail, t),
                    timeout=10.0
                )
                if unfilled and isinstance(unfilled, list):
                    for uo in unfilled:
                        u_odno = uo.get('odno')
                        if u_odno:
                            try:
                                await asyncio.to_thread(self.broker.cancel_order, t, u_odno)
                                nuked_count += 1
                                await asyncio.sleep(0.2)
                            except Exception as e:
                                logging.error(f"🚨 [{t}] 수동 일반 덫 취소 실패: {e}")
                                err_count += 1
            except asyncio.TimeoutError:
                logging.error(f"🚨 [{t}] 일반 덫 스캔 타임아웃")
                err_count += 1
            except Exception as e:
                logging.error(f"🚨 [{t}] 일반 덫 스캔 에러: {e}")
                err_count += 1

            # 🚨 MODIFIED: [통합 지시서 수동 제어(EXEC/CANCEL) 완벽 스위칭 작전]
            if nuked_count > 0:
                await asyncio.to_thread(self.cfg.reset_lock_for_ticker, t)

            # 결과 타전
            if err_count > 0:
                await context.bot.send_message(chat_id, f"⚠️ <b>[{t}] 수동 취소 완료 (일부 오류 발생)</b>\n▫️ 총 <b>{nuked_count}건</b>의 덫을 파기하고 매매 잠금을 해제했으나, {err_count}건의 오류가 발생했습니다.", parse_mode='HTML')
            elif nuked_count > 0:
                await context.bot.send_message(chat_id, f"🛑 <b>[{t}] 수동 취소 팩트 집행 완료</b>\n▫️ 총 <b>{nuked_count}건</b>의 미체결 및 예약 덫을 100% 파기(Nuke)하고 당일 매매 잠금을 <b>해제(Unlock)</b>했습니다.", parse_mode='HTML')
            else:
                await context.bot.send_message(chat_id, f"ℹ️ <b>[{t}] 수동 취소 결과</b>\n▫️ 취소할 덫이 없습니다.", parse_mode='HTML')

        # 🚨 MODIFIED: [V72.15 settlement 콜백 라우팅 증발 맹점 영구 복원]
        elif action == "SET_VER":
            await query.answer()
            ticker = data[2]
            
            try:
                _, holdings = await asyncio.to_thread(self.broker.get_account_balance)
                kis_qty = int(float(holdings.get(ticker, {}).get('qty', 0))) if holdings else 0
            except Exception:
                kis_qty = 0
                
            max_qty = await self._get_max_holdings_qty(ticker, kis_qty)
            
            if max_qty > 0:
                await query.edit_message_text(f"🛑 <b>[{ticker} 모드 전환 차단]</b>\n\n현재 계좌 또는 장부에 단 1주라도 잔고({max_qty}주)가 존재하면 코어 스위칭이 불가능합니다.\n전량 익절(0주) 후 0주 새출발 상태에서 다시 시도해 주십시오.", parse_mode='HTML')
                return
                
            if sub == "V_REV":
                msg, markup = self.view.get_vrev_mode_selection_menu(ticker)
            elif sub == "V14":
                msg, markup = self.view.get_v14_mode_selection_menu(ticker)
            else:
                return
             
            await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')

        # 🚨 MODIFIED: [V72.15 settlement 콜백 라우팅 증발 맹점 영구 복원]
        elif action == "SET_VER_CONFIRM":
            await query.answer()
            ticker = data[2]
             
            if sub == "V_REV":
                await asyncio.to_thread(self.cfg.set_version, ticker, "V_REV")
                await asyncio.to_thread(self.cfg.set_reverse_state, ticker, True, 0, 0.0)
                await asyncio.to_thread(self.cfg.set_manual_vwap_mode, ticker, False) 
                msg = f"✅ <b>[{ticker}] V-REV 역추세 모드(VWAP 자동) 락온 완료!</b>\n▫️ 다음 타격부터 역추세 엔진이 전면 가동됩니다."
            elif sub == "V14_LOC":
                await asyncio.to_thread(self.cfg.set_version, ticker, "V14")
                await asyncio.to_thread(self.cfg.set_reverse_state, ticker, False, 0, 0.0)
                await asyncio.to_thread(self.cfg.set_manual_vwap_mode, ticker, False)
                msg = f"✅ <b>[{ticker}] V14 오리지널 (LOC 단일 타격) 락온 완료!</b>\n▫️ 다음 타격부터 오리지널 무매법이 가동됩니다."
            elif sub == "V14_VWAP":
                await asyncio.to_thread(self.cfg.set_version, ticker, "V14")
                await asyncio.to_thread(self.cfg.set_reverse_state, ticker, False, 0, 0.0)
                await asyncio.to_thread(self.cfg.set_manual_vwap_mode, ticker, True)
                msg = f"✅ <b>[{ticker}] V14 오리지널 (VWAP 자동) 락온 완료!</b>\n▫️ 다음 타격부터 VWAP 알고리즘에 위임합니다."
            else:
                return
                
            await query.edit_message_text(msg, parse_mode='HTML')

        # 🚨 MODIFIED: [V72.15 settlement 콜백 라우팅 증발 맹점 영구 복원]
        elif action == "AVWAP":
            if sub == "MENU":
                await controller.cmd_avwap(update, context)

        # 🚨 NEW: [V72.16 AVWAP 정점요격 스위치 및 유실된 라우터 전면 복구]
        elif action == "MODE":
            ticker = data[2]
            if sub == "ON":
                await query.answer()
                await asyncio.to_thread(self.cfg.set_upward_sniper_mode, ticker, True)
                if hasattr(controller, 'cmd_mode'):
                    await controller.cmd_mode(update, context)
            elif sub == "OFF":
                await query.answer()
                await asyncio.to_thread(self.cfg.set_upward_sniper_mode, ticker, False)
                if hasattr(controller, 'cmd_mode'):
                    await controller.cmd_mode(update, context)
            elif sub == "AVWAP_WARN":
                await query.answer()
                msg, markup = self.view.get_avwap_warning_menu(ticker)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            elif sub == "AVWAP_ON":
                await query.answer()
                await asyncio.to_thread(self.cfg.set_avwap_hybrid_mode, ticker, True)
                if hasattr(controller, 'cmd_settlement'):
                    await controller.cmd_settlement(update, context)
            elif sub == "AVWAP_OFF":
                await query.answer()
                await asyncio.to_thread(self.cfg.set_avwap_hybrid_mode, ticker, False)
                if hasattr(controller, 'cmd_settlement'):
                    await controller.cmd_settlement(update, context)
            elif sub == "APEX_ON":
                await query.answer(f"🎯 [{ticker}] 정점요격 전술 가동!", show_alert=False)
                await asyncio.to_thread(self.cfg.set_avwap_apex_mode, ticker, True)
                if hasattr(controller, 'cmd_settlement'):
                    await controller.cmd_settlement(update, context)
            elif sub == "APEX_OFF":
                await query.answer(f"⚪ [{ticker}] 정점요격 전술 해제 (지터 덤핑 롤백)", show_alert=False)
                await asyncio.to_thread(self.cfg.set_avwap_apex_mode, ticker, False)
                if hasattr(controller, 'cmd_settlement'):
                    await controller.cmd_settlement(update, context)

        elif action == "AVWAP_SET":
            ticker = data[2]
            if sub == "SYNC_ZERO":
                await query.answer()
                try:
                    app_data = context.bot_data.get('app_data', {})
                    tracking_cache = app_data.get('sniper_tracking', {})
                    
                    tracking_cache[f"AVWAP_QTY_{ticker}"] = 0
                    tracking_cache[f"AVWAP_AVG_{ticker}"] = 0.0
                    tracking_cache[f"AVWAP_BOUGHT_{ticker}"] = False
                    tracking_cache[f"AVWAP_SHUTDOWN_{ticker}"] = True

                    est = ZoneInfo('America/New_York')
                    now_est = datetime.datetime.now(est)

                    if hasattr(self.strategy, 'v_avwap_plugin'):
                        state_data = {
                            'bought': False,
                            'shutdown': True,
                            'qty': 0,
                            'avg_price': 0.0,
                            'strikes': tracking_cache.get(f"AVWAP_STRIKES_{ticker}", 0),
                            'daily_bought_qty': tracking_cache.get(f"AVWAP_DAILY_BOUGHT_{ticker}", 0),
                            'daily_sold_qty': tracking_cache.get(f"AVWAP_DAILY_SOLD_{ticker}", 0),
                            'first_scan_done': tracking_cache.get(f"AVWAP_FIRST_SCAN_DONE_{ticker}", False),
                            'first_scan_passed': tracking_cache.get(f"AVWAP_FIRST_SCAN_PASSED_{ticker}", False),
                            'dump_jitter_sec': tracking_cache.get(f"AVWAP_DUMP_JITTER_{ticker}", 0)
                        }
                        await asyncio.to_thread(self.strategy.v_avwap_plugin.save_state, ticker, now_est, state_data)
                    
                    await query.edit_message_text(f"🧯 <b>[{ticker}] AVWAP 수동 청산 (0주 락온) 완료!</b>\n▫️ 암살자 물량이 0주로 강제 포맷되었으며, 금일 남은 시간 동안 영구 동결(SHUTDOWN)됩니다.", parse_mode='HTML')
                except Exception as e:
                    logging.error(f"🚨 수동 0주 동기화 에러: {e}")
                    await query.edit_message_text(f"❌ 수동 0주 동기화 중 에러 발생: {e}", parse_mode='HTML')
            elif sub == "REFRESH":
                await query.answer()
                if hasattr(controller, 'cmd_avwap'):
                    await controller.cmd_avwap(update, context)

        elif action == "TICKER":
            await query.answer()
            if sub == "ALL":
                target_tickers = ["SOXL", "TQQQ"]
                msg_txt = "SOXL + TQQQ 통합"
            elif "," in sub:
                if "SOXS" in sub.split(","):
                    await context.bot.send_message(chat_id, "⚠️ [V61.00 절대 헌법] 숏(SOXS) 운용은 시스템 전역에서 100% 영구 소각되었습니다.")
                    return
                target_tickers = sub.split(",")
                msg_txt = " + ".join(target_tickers) + " 싱글 모멘텀"
            else:
                if sub == "SOXS":
                    await context.bot.send_message(chat_id, "⚠️ [V61.00 절대 헌법] 숏(SOXS) 운용은 시스템 전역에서 100% 영구 소각되었습니다.")
                    return
                target_tickers = [sub]
                msg_txt = sub + " 전용"
               
            await asyncio.to_thread(self.cfg.set_active_tickers, target_tickers)
            await query.edit_message_text(f"✅ <b>[운용 종목 락온 완료]</b>\n▫️ <b>{msg_txt}</b> 모드로 전환되었습니다.\n▫️ /sync를 눌러 확인하십시오.", parse_mode='HTML')
            
        elif action == "SEED":
            await query.answer()
            ticker = data[2]
            controller.user_states[chat_id] = f"SEED_{sub}_{ticker}"
            await context.bot.send_message(chat_id, f"💵 [{ticker}] 시드머니 금액 입력:", parse_mode='HTML')
            
        elif action == "INPUT":
            await query.answer()
            ticker = data[2]
            controller.user_states[chat_id] = f"CONF_{sub}_{ticker}"
           
            if sub == "SPLIT":
                ko_name = "분할 횟수"
            elif sub == "TARGET":
                ko_name = "목표 수익률(%)"
            elif sub == "COMPOUND":
                ko_name = "자동 복리율(%)"
            elif sub == "STOCK_SPLIT":
                 ko_name = "액면 분할/병합 비율 (예: 10분할은 10, 10병합은 0.1)"
            elif sub == "FEE":
                ko_name = "증권사 수수료율(%)"
            else:
                 ko_name = "값"
            
            desc = "숫자만 입력하세요.\n(예: 액면분할 시 1주가 10주가 되었다면 10 입력, 10주가 1주로 병합되었다면 0.1 입력)" if sub == "STOCK_SPLIT" else "숫자만 입력하세요."
            await context.bot.send_message(chat_id, f"✏️ <b>[{ticker}] {ko_name}</b>를 설정합니다.\n{desc}", parse_mode='HTML')
