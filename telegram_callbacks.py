# ==========================================================
# FILE: telegram_callbacks.py
# ==========================================================
# 🚨 MODIFIED: [V77.31] 수동 요격(MANUAL_FIRE_REQ/EXEC) 및 수동 청산 진입 전 시간대 이중 필터링 락온
# 🚨 NEW: [Case 11] 다중 출격(Multi-Sortie) 스위칭 라우터 배선 완벽 개통
# 🚨 MODIFIED: [Case 16, 26] 전역 스코프 전진 배치로 NameError 런타임 붕괴 완벽 차단
# 🚨 MODIFIED: [라우팅 누수 방어] 장마감(주말) 시 LOC/LIMIT 덫이 일반 주문으로 빠지는 KIS Reject 맹점 완벽 차단
# 🚨 NEW: [Case 31] 수동 0주 락온 및 수동 요격 시 `trap_placed_time` 팩트 초기화 및 동기화
# 🚨 MODIFIED: [결함 1 수술] AVWAP 익절 덫 타점 2.0% 하향 락온 (타점 역전 패러독스 소각 및 회전율 극대화)
# 🚨 MODIFIED: [결함 2 수술 Case 02/08] 수동 장부 소각 시 KIS 실잔고 0주 강제 동기화 및 0주 팩트 다이렉트 덮어쓰기 락온
# 🚨 MODIFIED: [결함 3 수술 Case 28] 수동 요격 승인 전 실시간 현재가 팩트 스캔 및 타점 이탈 팻핑거 팝업 생성 원천 차단
# 🚨 MODIFIED: [Case 31 팩트 수술] 수동 요격 시 1분봉 시차 패러독스 방어망(Time-Shield) 멱등성 동기화를 위해 초 단위 0 초기화 락온
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
# MODIFIED: [Case 16, 26] 전역 스코프 전진 배치로 NameError 런타임 붕괴 완벽 차단
import html  
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
                    # MODIFIED: [Case 16, 26] html 모듈 전역 스코프 전진 배치로 중복 import 소각
                    safe_msg = html.escape(msg)
                    if success:
                        await query.edit_message_text(f"✅ <b>[업데이트 완료]</b> {safe_msg}\n\n🔄 데몬을 재가동합니다. 잠시 후 봇이 응답할 것입니다.", parse_mode='HTML')
                        await updater.restart_daemon()
                    else:
                        await query.edit_message_text(f"❌ <b>[동기화 실패]</b>\n▫️ 사유: {safe_msg}", parse_mode='HTML')
                except Exception as e:
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
                        err_msg = html.escape(res.get('msg1', '알 수 없는 에러'))
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
                safe_err = html.escape(str(e))
                await query.answer(f"❌ 처리 중 에러 발생: {safe_err}", show_alert=True)

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
                    # MODIFIED: [결함 2 수술 Case 02/08] KIS 실잔고(0주) 강제 동기화 락온
                    await asyncio.to_thread(self.queue_ledger.sync_with_broker, ticker, 0, 0.0)

                # MODIFIED: [결함 2 수술 Case 02/08] 당일 매매 잠금 즉각 해제 락온
                await asyncio.to_thread(self.cfg.reset_lock_for_ticker, ticker)

                # MODIFIED: [결함 2 수술 Case 02/08] 전일 종가 스캔 및 0주 팩트 스냅샷 원자적 오버라이드 락온
                try:
                    prev_c_val = await asyncio.wait_for(asyncio.to_thread(self.broker.get_previous_close, ticker), timeout=5.0)
                    prev_c = float(prev_c_val or 0.0)
                except Exception as e:
                    logging.error(f"🚨 수동 소각 후 전일 종가 스캔 에러: {e}")
                    prev_c = 0.0
                
                if prev_c > 0:
                    try:
                        async with self.tx_lock:
                            cash_val, _ = await asyncio.wait_for(asyncio.to_thread(self.broker.get_account_balance), timeout=10.0)
                            cash = float(cash_val or 0.0)
                            from scheduler_core import get_budget_allocation
                            active_tickers_list = await asyncio.to_thread(self.cfg.get_active_tickers)
                            _, alloc_cash_dict = await asyncio.to_thread(get_budget_allocation, cash, active_tickers_list, self.cfg)
                            available_cash = alloc_cash_dict.get(ticker, 0.0)
                            
                            # 0주 팩트 스냅샷 원자적 덮어쓰기 집행 (V-REV/V14 라우터 공통 지원)
                            await asyncio.to_thread(
                                self.strategy.get_plan, 
                                ticker, 0.0, 0.0, 0, prev_c, 
                                ma_5day=0.0, market_type="REG", available_cash=available_cash, 
                                is_simulation=True, is_snapshot_mode=True
                            )
                    except Exception as e:
                        logging.error(f"🚨 0주 강제 스냅샷 오버라이드 에러: {e}")

                await query.edit_message_text(f"✅ <b>[{ticker}] 삼위일체 소각(Nuke) 및 초기화 완료!</b>\n▫️ 본장부, 백업장부, 큐(Queue) 찌꺼기 데이터가 100% 영구 삭제되었습니다.\n▫️ KIS 실잔고 0주 동기화, 매매 잠금 해제 및 0주 새출발 디커플링 타점 스냅샷 원자적 덮어쓰기가 완벽히 집행되었습니다.", parse_mode='HTML')
       
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
                if o['type'] == 'VWAP' or is_market_active_now:
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
                
                err_msg = html.escape(res.get('msg1', '오류'))
                status_icon = '✅' if is_success else f'❌({err_msg})'
                msg += f"└ 1차 필수: {o['desc']} {o['qty']}주: {status_icon}\n"
                await asyncio.sleep(0.2) 
            
            target_bonus = plan.get('bonus_orders', [])
            for o in target_bonus:
                if o['type'] == 'VWAP' or is_market_active_now:
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
                err_msg = html.escape(res.get('msg1', '잔금패스'))
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

        elif action == "CANCEL_EXEC":
            t = sub
            await query.answer()
            await query.edit_message_text(f"🛑 <b>[{t}] 수동 매매(일반/예약 덫) 취소 집행 중...</b>", parse_mode='HTML')
            
            nuked_count = 0
            err_count = 0
            
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
                err_count += 1
            except Exception as e:
                err_count += 1

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
                err_count += 1
            except Exception as e:
                err_count += 1

            if nuked_count > 0:
                await asyncio.to_thread(self.cfg.reset_lock_for_ticker, t)

            if err_count > 0:
                await context.bot.send_message(chat_id, f"⚠️ <b>[{t}] 수동 취소 완료 (일부 오류 발생)</b>\n▫️ 총 <b>{nuked_count}건</b>의 덫을 파기하고 매매 잠금을 해제했으나, {err_count}건의 오류가 발생했습니다.", parse_mode='HTML')
            elif nuked_count > 0:
                await context.bot.send_message(chat_id, f"🛑 <b>[{t}] 수동 취소 팩트 집행 완료</b>\n▫️ 총 <b>{nuked_count}건</b>의 미체결 및 예약 덫을 100% 파기(Nuke)하고 당일 매매 잠금을 <b>해제(Unlock)</b>했습니다.", parse_mode='HTML')
            else:
                await context.bot.send_message(chat_id, f"ℹ️ <b>[{t}] 수동 취소 결과</b>\n▫️ 취소할 덫이 없습니다.", parse_mode='HTML')

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

        elif action == "AVWAP":
            if sub == "MENU":
                if hasattr(controller, 'cmd_avwap'):
                    await controller.cmd_avwap(update, context)

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
            
            elif sub == "AVWAP_SORTIE":
                tgt_val = data[3]
                await query.answer(f"✅ 작전 궤도를 {tgt_val} 모드로 스위칭합니다.", show_alert=False)
                await asyncio.to_thread(self.cfg.set_avwap_sortie_mode, ticker, tgt_val)
                if hasattr(controller, 'cmd_settlement'):
                    await controller.cmd_settlement(update, context)

        elif action == "AVWAP_SET":
            ticker = data[2]
            if sub == "SYNC_ZERO":
                status_code, _ = await controller._get_market_status()
                if status_code not in ["PRE", "REG"]:
                    return await query.answer("❌ [격발 차단] 현재 장운영시간(정규장/프리장)이 아닙니다.", show_alert=True)
                    
                await query.answer()
                try:
                    app_data = context.bot_data.get('app_data', {})
                    tracking_cache = app_data.get('sniper_tracking', {})
                    
                    tracking_cache[f"AVWAP_QTY_{ticker}"] = 0
                    tracking_cache[f"AVWAP_AVG_{ticker}"] = 0.0
                    tracking_cache[f"AVWAP_BOUGHT_{ticker}"] = False
                    tracking_cache[f"AVWAP_SHUTDOWN_{ticker}"] = True
                    # NEW: [Case 31] 타임 패러독스 방어 초기화
                    tracking_cache[f"AVWAP_TRAP_PLACED_TIME_{ticker}"] = ""

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
                            'trap_odno': tracking_cache.get(f"AVWAP_TRAP_ODNO_{ticker}", ""),
                            'PM_H': tracking_cache.get(f"AVWAP_PM_H_{ticker}", 0.0),
                            'PM_L': tracking_cache.get(f"AVWAP_PM_L_{ticker}", 0.0),
                            'T_H': tracking_cache.get(f"AVWAP_T_H_{ticker}", 0.0),
                            'T_L': tracking_cache.get(f"AVWAP_T_L_{ticker}", 0.0),
                            'offset': tracking_cache.get(f"AVWAP_OFFSET_{ticker}", 0.0),
                            'whipsaw_mode': tracking_cache.get(f"AVWAP_WHIPSAW_MODE_{ticker}", False),
                            'whipsaw_armed': tracking_cache.get(f"AVWAP_WHIPSAW_ARMED_{ticker}", False),
                            'whipsaw_checked': tracking_cache.get(f"AVWAP_WHIPSAW_CHECKED_{ticker}", False),
                            'dump_jitter_sec': tracking_cache.get(f"AVWAP_DUMP_JITTER_{ticker}", 0),
                            'trap_placed_time': ""
                        }
                        await asyncio.to_thread(self.strategy.v_avwap_plugin.save_state, ticker, now_est, state_data)
                    
                    await query.edit_message_text(f"🧯 <b>[{ticker}] AVWAP 수동 청산 (0주 락온) 완료!</b>\n▫️ 암살자 물량이 0주로 강제 포맷되었으며, 금일 남은 시간 동안 영구 동결(SHUTDOWN)됩니다.", parse_mode='HTML')
                except Exception as e:
                    logging.error(f"🚨 수동 0주 동기화 에러: {e}")
                    safe_err = html.escape(str(e))
                    await query.edit_message_text(f"❌ 수동 0주 동기화 중 에러 발생: {safe_err}", parse_mode='HTML')
            elif sub == "REFRESH":
                await query.answer()
                if hasattr(controller, 'cmd_avwap'):
                    await controller.cmd_avwap(update, context)
            
            elif sub == "MANUAL_FIRE_REQ":
                try:
                    status_code, _ = await controller._get_market_status()
                    if status_code not in ["PRE", "REG"]:
                        return await query.answer("❌ [격발 차단] 현재 장운영시간(정규장/프리장)이 아닙니다.", show_alert=True)
                        
                    app_data = context.bot_data.get('app_data', {})
                    tracking_cache = app_data.get('sniper_tracking', {})
                    
                    t_h = tracking_cache.get(f"AVWAP_T_H_{ticker}", 0.0)
                    if t_h <= 0.0:
                        if hasattr(self.strategy, 'v_avwap_plugin'):
                            est = ZoneInfo('America/New_York')
                            now_est = datetime.datetime.now(est)
                            state = await asyncio.to_thread(self.strategy.v_avwap_plugin.load_state, ticker, now_est)
                            t_h = float(state.get('T_H', 0.0))
                            
                    if t_h <= 0.0:
                        return await query.answer(f"❌ [{ticker}] 수동 요격 불가\n▫️ T_H(지정가 덫 기준선) 데이터가 존재하지 않습니다. 스캔 대기.", show_alert=True)

                    # MODIFIED: [결함 3 수술 Case 28] 팝업 렌더링 전 실시간 현재가 팩트 스캔 및 타점 이탈 팻핑거 검증 강제
                    try:
                        curr_p_val = await asyncio.wait_for(asyncio.to_thread(self.broker.get_current_price, ticker), timeout=5.0)
                        curr_p = float(curr_p_val or 0.0)
                    except Exception as e:
                        logging.error(f"🚨 수동 요격 전 현재가 스캔 에러: {e}")
                        curr_p = 0.0
                        
                    if curr_p <= 0.0:
                        return await query.answer(f"❌ [{ticker}] 수동 요격 불가\n▫️ 실시간 현재가를 스캔할 수 없습니다.", show_alert=True)
                        
                    if curr_p >= t_h:
                        return await query.answer(f"🛡️ [{ticker}] 수동 요격 차단 (타점 이탈)\n▫️ 현재가(${curr_p:.2f})가 T_H(${t_h:.2f}) 이상으로 팝업 렌더링을 차단합니다.", show_alert=True)

                    await query.answer("⚠️ 요격 확인 팝업 생성 중...", show_alert=False)
                    
                    msg = f"🚨 <b>[{ticker} 사이보그 엑시트 최종 승인 대기]</b>\n\n"
                    msg += f"▫️ 지정가 타점: <b>${t_h:.2f} (T_H 기준)</b>\n"
                    msg += "▫️ 승인 즉시 가용 예산의 95%가 시장가성 지정가로 딥매수 타격됩니다.\n\n"
                    msg += "⚠️ <b>포트폴리오 매니저 경고:</b>\n"
                    msg += "현재가가 T_H보다 같거나 높을 경우, 시스템이 요격을 강제 차단합니다. 정말로 요격을 집행하시겠습니까?"

                    keyboard = [
                        [InlineKeyboardButton(f"🔥 [{ticker}] 수동 요격 최종 승인 (Fire!)", callback_data=f"AVWAP_SET:MANUAL_FIRE_EXEC:{ticker}")],
                        [InlineKeyboardButton("❌ 작전 취소 (안전 모드 복귀)", callback_data="AVWAP_SET:REFRESH:NONE")]
                    ]
                    await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                    
                except Exception as e:
                    logging.error(f"🚨 수동 요격 확인창 생성 에러: {e}")
                    await query.answer(f"❌ 요격 승인 대기 중 에러 발생: {e}", show_alert=True)
            
            elif sub == "MANUAL_FIRE_EXEC":
                try:
                    status_code, _ = await controller._get_market_status()
                    if status_code not in ["PRE", "REG"]:
                        return await query.answer("❌ [격발 차단] 현재 장운영시간(정규장/프리장)이 아닙니다.", show_alert=True)
                        
                    app_data = context.bot_data.get('app_data', {})
                    tracking_cache = app_data.get('sniper_tracking', {})
                    
                    t_h = tracking_cache.get(f"AVWAP_T_H_{ticker}", 0.0)
                    if t_h <= 0.0:
                        if hasattr(self.strategy, 'v_avwap_plugin'):
                            est = ZoneInfo('America/New_York')
                            now_est = datetime.datetime.now(est)
                            state = await asyncio.to_thread(self.strategy.v_avwap_plugin.load_state, ticker, now_est)
                            t_h = float(state.get('T_H', 0.0))
                    
                    if t_h <= 0.0:
                        return await query.answer(f"❌ [{ticker}] 수동 요격 실패\n▫️ T_H(지정가 덫 기준선) 데이터가 존재하지 않습니다. 스캔이 완료될 때 대기하십시오.", show_alert=True)

                    try:
                        curr_p_val = await asyncio.wait_for(asyncio.to_thread(self.broker.get_current_price, ticker), timeout=5.0)
                        curr_p = float(curr_p_val or 0.0)
                    except Exception as e:
                        logging.error(f"🚨 수동 요격 현재가 스캔 에러: {e}")
                        curr_p = 0.0

                    if curr_p <= 0.0:
                        return await query.answer(f"❌ [{ticker}] 수동 요격 실패\n▫️ 현재가를 스캔할 수 없습니다. 통신 상태를 확인하십시오.", show_alert=True)

                    if curr_p >= t_h:
                        return await query.answer(f"🛡️ [{ticker}] 수동 요격 차단 (타점 이탈)\n▫️ 현재가(${curr_p:.2f})가 T_H(${t_h:.2f}) 이상입니다.\n▫️ 떨어지는 칼날(Deep Dip) 조건 미충족.", show_alert=True)

                    async with self.tx_lock:
                        cash, holdings = await asyncio.wait_for(asyncio.to_thread(self.broker.get_account_balance), timeout=10.0)
                        
                    avwap_free_cash = max(0.0, float(cash or 0.0))
                    safe_budget = avwap_free_cash * 0.95
                    buy_qty = int(math.floor(safe_budget / t_h)) if t_h > 0 else 0

                    if buy_qty <= 0:
                        return await query.answer(f"❌ [{ticker}] 수동 요격 실패\n▫️ 예산 부족(0주 산출). 가용 현금: ${avwap_free_cash:.2f}", show_alert=True)

                    await query.answer("🔫 사이보그 요격 시퀀스 정상 가동. KIS 서버로 전송합니다...", show_alert=False)
                    await query.edit_message_text(f"🚀 <b>[{ticker}] 사이보그(Cyborg) 수동 강제 요격(Manual Fire) 격발 중...</b>\n▫️ 팩트 스캔 완료. 딥매수 타점을 검증합니다.", parse_mode='HTML')

                    res = await asyncio.to_thread(self.broker.send_order, ticker, "BUY", buy_qty, t_h, "LIMIT")
                    buy_odno = res.get('odno', '') if isinstance(res, dict) else ''

                    if res and res.get('rt_cd') == '0' and buy_odno:
                        ccld_qty = 0
                        for _ in range(4):
                            await asyncio.sleep(2.0)
                            unfilled_check = await asyncio.to_thread(self.broker.get_unfilled_orders_detail, ticker)
                            safe_unfilled = unfilled_check if isinstance(unfilled_check, list) else []
                            my_order = next((ox for ox in safe_unfilled if ox.get('odno') == buy_odno), None)
                            if my_order:
                                ccld_qty = int(float(my_order.get('tot_ccld_qty') or 0))
                                if ccld_qty >= buy_qty:
                                    break
                            else:
                                ccld_qty = buy_qty
                                break

                        if ccld_qty < buy_qty:
                            try:
                                await asyncio.to_thread(self.broker.cancel_order, ticker, buy_odno)
                                await asyncio.sleep(0.5)
                            except: pass

                        if ccld_qty > 0:
                            # MODIFIED: [결함 1 수술] 수동 요격 시 익절 덫 타점 2.0% 하향 락온 적용
                            trap_price = round(t_h * 1.02, 2)
                            trap_res = await asyncio.to_thread(self.broker.send_order, ticker, "SELL", ccld_qty, trap_price, "LIMIT")
                            trap_odno = trap_res.get('odno', '') if isinstance(trap_res, dict) else ''

                            if trap_res and trap_res.get('rt_cd') == '0' and trap_odno:
                                # MODIFIED: [결함 1 수술] 텔레그램 메시지 텍스트 2.0% 팩트 교정 완료
                                trap_msg = f"▫️ +2.0% 수익 타점(<b>${trap_price:.2f}</b>)에 익절 덫을 즉시 자동 장전했습니다."
                            else:
                                trap_err = html.escape(trap_res.get('msg1', '오류')) if trap_res else '통신 장애'
                                trap_msg = f"⚠️ <b>[익절 덫 장전 실패]</b> KIS 서버 거절: {trap_err}"

                            est = ZoneInfo('America/New_York')
                            now_est = datetime.datetime.now(est)
                            
                            tracking_cache[f"AVWAP_BOUGHT_{ticker}"] = True
                            tracking_cache[f"AVWAP_SHUTDOWN_{ticker}"] = False
                            tracking_cache[f"AVWAP_EXECUTED_BUY_{ticker}"] = True
                            tracking_cache[f"AVWAP_QTY_{ticker}"] = ccld_qty
                            tracking_cache[f"AVWAP_AVG_{ticker}"] = round(t_h, 4)
                            tracking_cache[f"AVWAP_TRAP_ODNO_{ticker}"] = trap_odno
                            # MODIFIED: [Case 31 팩트 수술] 1분봉 시차 패러독스(Time-Shield) 멱등성 동기화를 위해 초 단위 0 초기화 락온
                            tracking_cache[f"AVWAP_TRAP_PLACED_TIME_{ticker}"] = now_est.replace(second=0, microsecond=0).strftime('%H%M%S')
                            
                            daily_b = tracking_cache.get(f"AVWAP_DAILY_BOUGHT_{ticker}", 0) + ccld_qty
                            tracking_cache[f"AVWAP_DAILY_BOUGHT_{ticker}"] = daily_b

                            if hasattr(self.strategy, 'v_avwap_plugin'):
                                state = await asyncio.to_thread(self.strategy.v_avwap_plugin.load_state, ticker, now_est)
                                state.update({
                                    "bought": True,
                                    "shutdown": False,
                                    "executed_buy": True,
                                    "qty": ccld_qty,
                                    "avg_price": round(t_h, 4),
                                    "daily_bought_qty": daily_b,
                                    "trap_odno": trap_odno,
                                    "limit_order_placed": True,
                                    "placed_target_th": t_h,
                                    "trap_placed_time": tracking_cache[f"AVWAP_TRAP_PLACED_TIME_{ticker}"],
                                    "buy_odno": buy_odno
                                })
                                await asyncio.to_thread(self.strategy.v_avwap_plugin.save_state, ticker, now_est, state)

                            final_msg = f"🔫 <b>[{ticker}] 사이보그 수동 강제 요격 성공!</b>\n"
                            final_msg += f"▫️ 타점: <b>${t_h:.2f}</b> (순수 지정가 LIMIT)\n"
                            final_msg += f"▫️ 체결수량: <b>{ccld_qty}주</b> (요청: {buy_qty}주)\n"
                            if ccld_qty < buy_qty:
                                final_msg += f"▫️ 미체결 {buy_qty - ccld_qty}주는 안전을 위해 즉각 취소(Nuke)되었습니다.\n"
                            final_msg += f"\n🎯 <b>[투트랙 엑시트 장전]</b>\n{trap_msg}\n"
                            final_msg += f"\n🛡️ <b>[상태기계 롤백 완료]</b>\n▫️ 09:30 기요틴 셧다운이 해제되었습니다.\n▫️ 봇이 남은 시간 동안 덤핑 및 익절을 정상적으로 100% 감시합니다."

                            await query.edit_message_text(final_msg, parse_mode='HTML')
                            
                        else:
                            await query.edit_message_text(f"❌ <b>[{ticker}] 수동 요격 체결 실패</b>\n▫️ 8초 검증 결과 체결된 물량이 없어 주문을 철회했습니다.", parse_mode='HTML')

                    else:
                        err_msg = html.escape(res.get('msg1', '응답 없음')) if res else '통신 장애'
                        logging.error(f"🚨 [{ticker}] 사이보그 수동 요격 서버 거절: {err_msg}")
                        reject_msg = (
                            f"🚨 <b>[{ticker}] 사이보그 수동 딥매수 서버 거절 (Reject)!</b>\n"
                            f"▫️ 사유: <code>{err_msg}</code>\n"
                        )
                        await query.edit_message_text(reject_msg, parse_mode='HTML')

                except Exception as e:
                    logging.error(f"🚨 사이보그 수동 요격 에러: {e}")
                    safe_err = html.escape(str(e))
                    await query.edit_message_text(f"❌ 수동 요격 중 에러 발생: {safe_err}", parse_mode='HTML')

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
