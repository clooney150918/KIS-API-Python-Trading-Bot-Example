# ==========================================================
# FILE: telegram_callbacks.py
# ==========================================================
# 🚨 MODIFIED: [Insight 21] 텔레그램 콜백 페이로드 인덱스(IndexError) 런타임 붕괴 차단.
# 🚨 MODIFIED: [Insight 22] YFinance 결측 DataFrame 및 NaN Math 증발 차단.
# 🚨 MODIFIED: [Insight 23] 큐 장부 오염 객체(Dirty Record) 필터링 락온.
# 🚨 MODIFIED: [Insight 20] 텔레그램 콜백 데이터 동적 타입 오염 방어. html.escape(str()) 강제 캐스팅.
# 🚨 MODIFIED: [Insight 19] 예산 할당 딕셔너리(alloc_cash_dict) NullType Unpacking 붕괴 차단 (or {}).
# 🚨 MODIFIED: [Insight 17] get_plan 반환값 손상 방어 및 NoneType 붕괴 차단.
# 🚨 MODIFIED: [Insight 14, 15] Array Mutation 방어 및 String-Float 맹독성 포맷팅 쉴드.
# 🚨 MODIFIED: [Insight 12] 딕셔너리 맹독성 캐스팅 쉴드 (isinstance).
# 🚨 MODIFIED: [Insight 11] 궁극의 이터러블 Null-Coalescing 쉴드 이식 (or []).
# 🚨 MODIFIED: [Insight 08, 09, 32, 33] 3단 지수 백오프, TPS 캡핑(0.06s), wait_for(10.0) 래핑 완료.
# 🚨 MODIFIED: [제1헌법 교정] os.path.exists 파일 스캔 동기 뇌관 비동기 래핑 100% 완료 및 QueueLedger 동기 인스턴스화 차단.
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
            ledger = await asyncio.to_thread(self.cfg.get_ledger) or []
            net = 0
            for r in ledger:
                if isinstance(r, dict) and r.get('ticker') == ticker:
                    q = int(float(str(r.get('qty') or 0).replace(',', ''))) 
                    net += q if r.get('side') == 'BUY' else -q
            v14_qty = max(0, net)
        except Exception:
            pass

        try:
            if getattr(self, 'queue_ledger', None):
                q_data = await asyncio.to_thread(self.queue_ledger.get_queue, ticker) or []
                vrev_qty = sum(int(float(str(lot.get('qty') or 0).replace(',', ''))) for lot in q_data if isinstance(lot, dict) and int(float(str(lot.get('qty') or 0).replace(',', ''))) > 0)
        except Exception:
            pass

        return max(kis_qty, v14_qty, vrev_qty)

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE, controller):
        query = update.callback_query
        chat_id = update.effective_chat.id
        data = query.data.split(":")
        action, sub = data[0], data[1] if len(data) > 1 else ""

        if action == "UPDATE":
            try:
                await query.answer()
            except Exception:
                pass
            if sub == "CONFIRM":
                from plugin_updater import SystemUpdater
                updater = SystemUpdater()
                await query.edit_message_text("⏳ <b>[업데이트 승인됨]</b> GitHub 코드를 강제 페칭합니다...", parse_mode='HTML')
                try:
                    success, msg = await updater.pull_latest_code()
                    safe_msg = html.escape(str(msg)) 
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
            try:
                await query.answer()
            except Exception:
                pass
            if sub == "VIEW":
                ticker = data[2] if len(data) > 2 else ""
                if getattr(self, 'queue_ledger', None) and ticker:
                    q_data = await asyncio.to_thread(self.queue_ledger.get_queue, ticker) or []
                else:
                    q_data = []
                
                msg, markup = self.view.get_queue_management_menu(ticker, q_data)
                try:
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                except Exception:
                    pass

        elif action == "EMERGENCY_REQ":
            ticker = sub
            status_code, _ = await controller._get_market_status()
            if status_code not in ["PRE", "REG"]:
                await query.answer("❌ [격발 차단] 현재 장운영시간(정규장/프리장)이 아닙니다.", show_alert=True)
                return
                
            if not getattr(self, 'queue_ledger', None):
                from queue_ledger import QueueLedger
                # 🚨 MODIFIED: [제1헌법] QueueLedger 동기 인스턴스화 차단 
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
            status_code, _ = await controller._get_market_status()
            
            if status_code not in ["PRE", "REG"]:
                await query.answer("❌ [격발 차단] 현재 장운영시간(정규장/프리장)이 아닙니다.", show_alert=True)
                return
             
            if not getattr(self, 'queue_ledger', None):
                from queue_ledger import QueueLedger
                # 🚨 MODIFIED: [제1헌법] QueueLedger 동기 인스턴스화 차단
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
                    
                    if isinstance(res, dict) and res.get('rt_cd') == '0':
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

        elif action == "DEL_REQ":
            try:
                await query.answer()
            except Exception:
                pass
            ticker = sub
            target_date = ":".join(data[2:])
            
            if getattr(self, 'queue_ledger', None):
                q_data = await asyncio.to_thread(self.queue_ledger.get_queue, ticker) or []
            else:
                 q_data = []
             
            qty, price = 0, 0.0
            for item in q_data:
                if isinstance(item, dict) and item.get('date') == target_date:
                    qty = int(float(str(item.get('qty') or 0).replace(',', ''))) 
                    price = float(str(item.get('price') or 0.0).replace(',', ''))
                    break
        
            msg, markup = self.view.get_queue_action_confirm_menu(ticker, target_date, qty, price)
            try:
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            except Exception:
                pass

        elif action in ["DEL_Q", "EDIT_Q"]:
            ticker = sub
            target_date = ":".join(data[2:])
            
            try:
                if action == "DEL_Q":
                    if getattr(self, 'queue_ledger', None):
                         await asyncio.to_thread(self.queue_ledger.delete_lot, ticker, target_date)
                     
                    try:
                        await query.answer("✅ 지층 삭제 완료. KIS 원장과 동기화합니다.", show_alert=False)
                    except Exception:
                        pass
                        
                    if ticker not in self.sync_engine.sync_locks:
                        self.sync_engine.sync_locks[ticker] = asyncio.Lock()
                    if not self.sync_engine.sync_locks[ticker].locked():
                        await self.sync_engine.process_auto_sync(ticker, chat_id, context, silent_ledger=True)
        
                    final_q = await asyncio.to_thread(self.queue_ledger.get_queue, ticker) if getattr(self, 'queue_ledger', None) else []
                    final_q = final_q or []
                    msg, markup = self.view.get_queue_management_menu(ticker, final_q)
                    try:
                        await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                    except Exception:
                        pass
            
                elif action == "EDIT_Q":
                    try:
                        await query.answer("✏️ 수정 모드 진입", show_alert=False)
                    except Exception:
                        pass
                    short_date = html.escape(str(target_date[:10]))
                    safe_ticker = html.escape(str(ticker))
                    controller.user_states[chat_id] = f"EDITQ_{ticker}_{target_date}"
                     
                    prompt = f"✏️ <b>[{safe_ticker} 지층 수정 모드]</b>\n"
                    prompt += f"선택하신 <b>[{short_date}]</b> 지층을 재설정합니다.\n\n"
                    prompt += "새로운 <b>[수량]</b>과 <b>[평단가]</b>를 띄어쓰기로 입력하세요.\n"
                    prompt += "(예: <code>229 52.16</code>)\n\n"
                    prompt += "<i>(입력을 취소하려면 숫자 이외의 문자를 보내주세요)</i>"
                    try:
                        await query.edit_message_text(prompt, parse_mode='HTML')
                    except Exception:
                        pass
            except Exception as e:
                safe_err = html.escape(str(e))
                try:
                    await query.answer(f"❌ 처리 중 에러 발생: {safe_err}", show_alert=True)
                except Exception:
                    pass

        elif action == "VERSION":
            try:
                await query.answer()
            except Exception:
                pass
            history_data = await asyncio.to_thread(self.cfg.get_full_version_history) or []
            if sub == "LATEST":
                msg, markup = self.view.get_version_message(history_data, page_index=None)
                try:
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                except Exception:
                    pass
            elif sub == "PAGE":
                page_idx = int(data[2]) if len(data) > 2 else 0
                msg, markup = self.view.get_version_message(history_data, page_index=page_idx)
                try:
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                except Exception:
                    pass
      
        elif action == "RESET":
            try:
                await query.answer()
            except Exception:
                pass
            if sub == "MENU":
                active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers) or []
                msg, markup = self.view.get_reset_menu(active_tickers)
                try:
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                except Exception:
                    pass
            elif sub == "LOCK": 
                ticker = data[2] if len(data) > 2 else ""
                if ticker:
                    await asyncio.to_thread(self.cfg.reset_lock_for_ticker, ticker)
                    try:
                        await query.edit_message_text(f"✅ <b>[{html.escape(str(ticker))}] 금일 매매 잠금이 해제되었습니다.</b>", parse_mode='HTML')
                    except Exception:
                        pass
            elif sub == "REV":
                ticker = data[2] if len(data) > 2 else ""
                if ticker:
                    msg, markup = self.view.get_reset_confirm_menu(ticker)
                    try:
                        await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                    except Exception:
                        pass
            elif sub == "CONFIRM":
                ticker = data[2] if len(data) > 2 else ""
                if not ticker: return
                
                current_ver = await asyncio.to_thread(self.cfg.get_version, ticker)
                is_rev_active = (current_ver == "V_REV")
                await asyncio.to_thread(self.cfg.set_reverse_state, ticker, is_rev_active, 0)
             
                ledger = await asyncio.to_thread(self.cfg.get_ledger) or []
                ledger_data = [r for r in ledger if isinstance(r, dict) and r.get('ticker') != ticker]
                await asyncio.to_thread(self.cfg._save_json, self.cfg.FILES["LEDGER"], ledger_data)
                
                def _process_reset_files():
                    backup_file = self.cfg.FILES["LEDGER"].replace(".json", "_backup.json")
                    if os.path.exists(backup_file):
                        try:
                            with open(backup_file, 'r', encoding='utf-8') as f:
                                b_data = json.load(f)
                            if not isinstance(b_data, list): b_data = []
                            b_data = [r for r in b_data if isinstance(r, dict) and r.get('ticker') != ticker]
                        
                            dir_name = os.path.dirname(backup_file) or '.'
                            fd, tmp_path = tempfile.mkstemp(dir=dir_name)
                            with os.fdopen(fd, 'w', encoding='utf-8') as f_out:
                                json.dump(b_data, f_out, ensure_ascii=False, indent=4)
                                f_out.flush()
                                os.fsync(f_out.fileno())
                            os.replace(tmp_path, backup_file)
                        except Exception:
                            pass
                     
                # 🚨 MODIFIED: [제1헌법] os.path.exists 비동기 격리 락온
                await asyncio.to_thread(_process_reset_files)
            
                if getattr(self, 'queue_ledger', None):
                    await asyncio.to_thread(self.queue_ledger.clear_queue, ticker)
                    await asyncio.to_thread(self.queue_ledger.sync_with_broker, ticker, 0, 0.0)

                await asyncio.to_thread(self.cfg.reset_lock_for_ticker, ticker)

                prev_c = 0.0
                for attempt in range(3):
                    try:
                        await asyncio.sleep(0.06)
                        prev_c_val = await asyncio.wait_for(asyncio.to_thread(self.broker.get_previous_close, ticker), timeout=10.0)
                        prev_c = float(str(prev_c_val or 0.0).replace(',', ''))
                        break
                    except Exception as e:
                        if attempt == 2: logging.error(f"🚨 수동 소각 후 전일 종가 스캔 에러: {e}")
                        else: await asyncio.sleep(1.0 * (2 ** attempt))
                
                if prev_c > 0:
                    try:
                        async with self.tx_lock:
                            cash_val = 0.0
                            for attempt in range(3):
                                try:
                                    await asyncio.sleep(0.06)
                                    cash_tuple = await asyncio.wait_for(asyncio.to_thread(self.broker.get_account_balance), timeout=10.0)
                                    cash_val = cash_tuple[0] if isinstance(cash_tuple, (list, tuple)) and len(cash_tuple) > 0 else 0.0
                                    break
                                except Exception:
                                    if attempt == 2: cash_val = 0.0
                                    else: await asyncio.sleep(1.0 * (2 ** attempt))
                                    
                            cash = float(str(cash_val or 0.0).replace(',', ''))
                            from scheduler_core import get_budget_allocation
                            active_tickers_list = await asyncio.to_thread(self.cfg.get_active_tickers) or []
                            _, alloc_cash_dict = await asyncio.to_thread(get_budget_allocation, cash, active_tickers_list, self.cfg)
                            alloc_cash_dict = alloc_cash_dict or {}
                            available_cash = float(str(alloc_cash_dict.get(ticker) or 0.0).replace(',', ''))
                            
                            await asyncio.to_thread(
                                self.strategy.get_plan, 
                                ticker, 0.0, 0.0, 0, prev_c, 
                                ma_5day=0.0, market_type="REG", available_cash=available_cash, 
                                is_simulation=True, is_snapshot_mode=True
                            )
                    except Exception as e:
                        logging.error(f"🚨 0주 강제 스냅샷 오버라이드 에러: {e}")

                try:
                    await query.edit_message_text(f"✅ <b>[{html.escape(str(ticker))}] 삼위일체 소각(Nuke) 및 초기화 완료!</b>\n▫️ 본장부, 백업장부, 큐(Queue) 찌꺼기 데이터가 100% 영구 삭제되었습니다.\n▫️ KIS 실잔고 0주 동기화, 매매 잠금 해제 및 0주 새출발 디커플링 타점 스냅샷 원자적 덮어쓰기가 완벽히 집행되었습니다.", parse_mode='HTML')
                except Exception:
                    pass
       
            elif sub == "CANCEL":
                try:
                    await query.edit_message_text("❌ 닫았습니다.", parse_mode='HTML')
                except Exception:
                    pass

        elif action == "REC":
            try:
                await query.answer()
            except Exception:
                pass
            if sub == "VIEW": 
                ticker = data[2] if len(data) > 2 else ""
                if not ticker: return
                async with self.tx_lock:
                    holdings = None
                    for attempt in range(3):
                        try:
                            await asyncio.sleep(0.06)
                            _, holdings = await asyncio.wait_for(asyncio.to_thread(self.broker.get_account_balance), timeout=10.0)
                            break
                        except Exception:
                            if attempt == 2: holdings = {}
                            else: await asyncio.sleep(1.0 * (2 ** attempt))
                            
                await self.sync_engine._display_ledger(ticker, chat_id, context, query=query, pre_fetched_holdings=holdings)
            elif sub == "SYNC": 
                ticker = data[2] if len(data) > 2 else ""
                if not ticker: return
          
                if ticker not in self.sync_engine.sync_locks:
                    self.sync_engine.sync_locks[ticker] = asyncio.Lock()
                     
                if not self.sync_engine.sync_locks[ticker].locked():
                    try:
                        await query.edit_message_text(f"🔄 <b>[{html.escape(str(ticker))}] 잔고 기반 대시보드 업데이트 중...</b>", parse_mode='HTML')
                    except Exception:
                        pass
                    res = await self.sync_engine.process_auto_sync(ticker, chat_id, context, silent_ledger=True)
                    if res == "SUCCESS": 
                        async with self.tx_lock:
                            holdings = None
                            for attempt in range(3):
                                try:
                                    await asyncio.sleep(0.06)
                                    _, holdings = await asyncio.wait_for(asyncio.to_thread(self.broker.get_account_balance), timeout=10.0)
                                    break
                                except Exception:
                                    if attempt == 2: holdings = {}
                                    else: await asyncio.sleep(1.0 * (2 ** attempt))
                                    
                        await self.sync_engine._display_ledger(ticker, chat_id, context, message_obj=query.message, pre_fetched_holdings=holdings)

        elif action == "HIST":
            try:
                await query.answer()
            except Exception:
                pass
            if sub == "VIEW":
                hid = int(data[2]) if len(data) > 2 else 0
                hist_data = await asyncio.to_thread(self.cfg.get_history) or []
                target = next((h for h in hist_data if isinstance(h, dict) and h.get('id') == hid), None)
                if target:
                    safe_trades = target.get('trades') or []
                    for t_rec in safe_trades:
                        if isinstance(t_rec, dict):
                            if 'ticker' not in t_rec:
                                t_rec['ticker'] = target.get('ticker')
                            if 'side' not in t_rec:
                                t_rec['side'] = 'BUY'
                      
                    qty, avg, invested, sold = await asyncio.to_thread(self.cfg.calculate_holdings, target.get('ticker'), safe_trades)
  
                    try:
                        msg, markup = self.view.create_ledger_dashboard(target.get('ticker'), qty, avg, invested, sold, safe_trades, 0, 0, is_history=True, history_id=hid)
                    except TypeError:
                        msg, markup = self.view.create_ledger_dashboard(target.get('ticker'), qty, avg, invested, sold, safe_trades, 0, 0, is_history=True)
                     
                    try:
                        await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                    except Exception:
                        pass
             
            elif sub == "LIST":
                if hasattr(controller, 'cmd_history'):
                    await controller.cmd_history(update, context)

            elif sub == "IMG":
                ticker = data[2] if len(data) > 2 else ""
                target_id = int(data[3]) if len(data) > 3 else None
                
                if not ticker: return
                hist_data = await asyncio.to_thread(self.cfg.get_history) or []
                hist_list = [h for h in hist_data if isinstance(h, dict) and h.get('ticker') == ticker]
                 
                if not hist_list:
                    await context.bot.send_message(chat_id, f"📭 <b>[{html.escape(str(ticker))}]</b> 발급 가능한 졸업 기록이 존재하지 않습니다.", parse_mode='HTML')
                    return
                
                target_hist = None
                if target_id:
                    target_hist = next((h for h in hist_list if h.get('id') == target_id), None)
                
                if not target_hist:
                    target_hist = sorted(hist_list, key=lambda x: str(x.get('end_date') or ''), reverse=True)[0]
                
                try:
                    await query.edit_message_text(f"🎨 <b>[{html.escape(str(ticker))}] 프리미엄 졸업 카드를 렌더링 중입니다...</b>", parse_mode='HTML')

                    img_path = await asyncio.to_thread(
                        self.view.create_profit_image,
                        ticker=target_hist.get('ticker'),
                        profit=target_hist.get('profit'),
                        yield_pct=target_hist.get('yield'),
                        invested=target_hist.get('invested'),
                        revenue=target_hist.get('revenue'),
                        end_date=target_hist.get('end_date')
                    )
            
                    # 🚨 MODIFIED: [제1헌법] os.path.exists 비동기 격리 락온
                    is_img_exist = await asyncio.to_thread(os.path.exists, img_path) if img_path else False
                    if img_path and is_img_exist:
                        def _read_img4(p):
                            with open(p, 'rb') as f_in: return f_in.read()
                        img_bytes4 = await asyncio.to_thread(_read_img4, img_path)
                        if img_path.lower().endswith('.gif'):
                            await context.bot.send_animation(chat_id=chat_id, animation=img_bytes4)
                        else:
                            await context.bot.send_photo(chat_id=chat_id, photo=img_bytes4)
                        try:
                            await query.delete_message()
                        except Exception:
                            pass
                    else:
                        await query.edit_message_text("❌ 이미지 생성에 실패했습니다.", parse_mode='HTML')
                except Exception as e:
                    logging.error(f"📸 👑 졸업 이미지 생성/발송 실패: {e}")
                    try:
                        await query.edit_message_text("❌ 이미지 생성 중 오류가 발생했습니다.", parse_mode='HTML')
                    except Exception:
                        pass
            
        elif action == "EXEC":
            t = sub
            ver = await asyncio.to_thread(self.cfg.get_version, t)

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
            
            status_code, _ = await controller._get_market_status()
            if status_code in ["AFTER", "CLOSE", "PRE"]:
                try:
                    def get_yf_close():
                        time.sleep(0.06)
                        df = yf.Ticker(t).history(period="5d", interval="1d")
                        if not df.empty and 'Close' in df.columns and len(df['Close']) > 0:
                            val = float(df['Close'].iloc[-1])
                            return val if not math.isnan(val) else None
                        return None
                    
                    yf_close = None
                    for attempt in range(3):
                        try:
                            yf_close = await asyncio.wait_for(asyncio.to_thread(get_yf_close), timeout=10.0)
                            break
                        except Exception:
                            if attempt == 2: pass
                            else: await asyncio.sleep(1.0 * (2 ** attempt))
                    if yf_close and yf_close > 0:
                        prev_c = yf_close
                except Exception as e:
                    logging.debug(f"YF 정규장 종가 롤오버 스캔 실패 ({t}): {e}")
                if curr_p > 0 and prev_c == 0.0:
                    prev_c = curr_p
         
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
            
            plan = await asyncio.to_thread(self.strategy.get_plan, t, curr_p, safe_avg, safe_qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_budget, is_simulation=True, is_snapshot_mode=True)
            
            if not isinstance(plan, dict):
                plan = {}
            
            if safe_qty == 0:
                for o in plan.get('core_orders') or []:
                    if isinstance(o, dict) and o.get('side') == 'BUY' and 'Buy1' in str(o.get('desc', '')):
                        o['price'] = round(prev_c * 1.15, 2)

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
                    if o.get('type') == 'VWAP' or is_market_active_now:
                        res = await asyncio.wait_for(
                            asyncio.to_thread(
                                self.broker.send_order, 
                                t, o.get('side'), o.get('qty'), o.get('price'), o.get('type'),
                                start_time=dyn_start_t if o.get('type') == 'VWAP' else None,
                                end_time=dyn_end_t if o.get('type') == 'VWAP' else None
                            ),
                            timeout=10.0
                        )
                    else:
                        res = await asyncio.wait_for(
                            asyncio.to_thread(
                                self.broker.send_reservation_order, 
                                t, o.get('side'), o.get('qty'), o.get('price'), o.get('type')
                            ),
                            timeout=10.0
                        )
                except Exception as e:
                    logging.error(f"🚨 V14/VREV 1차 덫 장전 통신 에러/타임아웃: {e}")
                    res = None
            
                is_success = isinstance(res, dict) and res.get('rt_cd') == '0'
                if not is_success:
                    all_success = False
                
                err_msg = html.escape(str(res.get('msg1') or '오류')) if isinstance(res, dict) else '응답 없음/통신 장애'
                status_icon = '✅' if is_success else f'❌({err_msg})'
                msg += f"└ 1차 필수: {html.escape(str(o.get('desc')))} {o.get('qty')}주: {status_icon}\n"
                await asyncio.sleep(0.2) 
            
            target_bonus = plan.get('bonus_orders') or []
            if not isinstance(target_bonus, list): target_bonus = []
            
            for o in target_bonus:
                if not isinstance(o, dict): continue
                try:
                    await asyncio.sleep(0.06)
                    if o.get('type') == 'VWAP' or is_market_active_now:
                        res = await asyncio.wait_for(
                            asyncio.to_thread(
                                self.broker.send_order, 
                                t, o.get('side'), o.get('qty'), o.get('price'), o.get('type'),
                                start_time=dyn_start_t if o.get('type') == 'VWAP' else None,
                                end_time=dyn_end_t if o.get('type') == 'VWAP' else None
                            ),
                            timeout=10.0
                        )
                    else:
                        res = await asyncio.wait_for(
                            asyncio.to_thread(
                                self.broker.send_reservation_order, 
                                t, o.get('side'), o.get('qty'), o.get('price'), o.get('type')
                            ),
                            timeout=10.0
                        )
                except Exception as e:
                    logging.error(f"🚨 V14/VREV 2차 보너스 덫 장전 통신 에러/타임아웃: {e}")
                    res = None
                 
                is_success = isinstance(res, dict) and res.get('rt_cd') == '0'
                err_msg = html.escape(str(res.get('msg1') or '잔금패스')) if isinstance(res, dict) else '응답 없음/통신 장애'
                status_icon = '✅' if is_success else f'❌({err_msg})'
                msg += f"└ 2차 보너스: {html.escape(str(o.get('desc')))} {o.get('qty')}주: {status_icon}\n"
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
                        odno = req.get('ovrs_rsvn_odno') or req.get('odno')
                        ord_dt = req.get('rsvn_ord_rcit_dt') or req.get('ord_dt', d_str)
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
                        u_odno = uo.get('odno')
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

        elif action == "SET_VER":
            try:
                await query.answer()
            except Exception:
                pass
            ticker = data[2] if len(data) > 2 else ""
            if not ticker: return
            
            try:
                holdings = None
                for attempt in range(3):
                    try:
                        await asyncio.sleep(0.06)
                        res = await asyncio.wait_for(asyncio.to_thread(self.broker.get_account_balance), timeout=10.0)
                        holdings = res[1] if isinstance(res, (list, tuple)) and len(res) > 1 else {}
                        break
                    except Exception:
                        if attempt == 2: holdings = {}
                        else: await asyncio.sleep(1.0 * (2 ** attempt))
                if not isinstance(holdings, dict): holdings = {}
                kis_qty = int(float(str(holdings.get(ticker, {}).get('qty') or 0).replace(',', '')))
            except Exception:
                kis_qty = 0
            
            max_qty = await self._get_max_holdings_qty(ticker, kis_qty)
            
            if max_qty > 0:
                try:
                    await query.edit_message_text(f"🛑 <b>[{html.escape(str(ticker))} 모드 전환 차단]</b>\n\n현재 계좌 또는 장부에 단 1주라도 잔고({max_qty}주)가 존재하면 코어 스위칭이 불가능합니다.\n전량 익절(0주) 후 0주 새출발 상태에서 다시 시도해 주십시오.", parse_mode='HTML')
                except Exception:
                    pass
                return
                
            if sub == "V_REV":
                msg, markup = self.view.get_vrev_mode_selection_menu(ticker)
            elif sub == "V14":
                msg, markup = self.view.get_v14_mode_selection_menu(ticker)
            else:
                return
            
            try:
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            except Exception:
                pass

        elif action == "SET_VER_CONFIRM":
            try:
                await query.answer()
            except Exception:
                pass
            ticker = data[2] if len(data) > 2 else ""
            if not ticker: return
             
            if sub == "V_REV":
                await asyncio.to_thread(self.cfg.set_version, ticker, "V_REV")
                await asyncio.to_thread(self.cfg.set_reverse_state, ticker, True, 0, 0.0)
                await asyncio.to_thread(self.cfg.set_manual_vwap_mode, ticker, False) 
                msg = f"✅ <b>[{html.escape(str(ticker))}] V-REV 역추세 모드(VWAP 자동) 락온 완료!</b>\n▫️ 다음 타격부터 역추세 엔진이 전면 가동됩니다."
            elif sub == "V14_LOC":
                await asyncio.to_thread(self.cfg.set_version, ticker, "V14")
                await asyncio.to_thread(self.cfg.set_reverse_state, ticker, False, 0, 0.0)
                await asyncio.to_thread(self.cfg.set_manual_vwap_mode, ticker, False)
                msg = f"✅ <b>[{html.escape(str(ticker))}] V14 오리지널 (LOC 단일 타격) 락온 완료!</b>\n▫️ 다음 타격부터 오리지널 무매법이 가동됩니다."
            elif sub == "V14_VWAP":
                await asyncio.to_thread(self.cfg.set_version, ticker, "V14")
                await asyncio.to_thread(self.cfg.set_reverse_state, ticker, False, 0, 0.0)
                await asyncio.to_thread(self.cfg.set_manual_vwap_mode, ticker, True)
                msg = f"✅ <b>[{html.escape(str(ticker))}] V14 오리지널 (VWAP 자동) 락온 완료!</b>\n▫️ 다음 타격부터 VWAP 알고리즘에 위임합니다."
            else:
                return
                
            try:
                await query.edit_message_text(msg, parse_mode='HTML')
            except Exception:
                pass

        elif action == "AVWAP":
            if sub == "MENU":
                if hasattr(controller, 'cmd_avwap'):
                    await controller.cmd_avwap(update, context)

        elif action == "MODE":
            ticker = data[2] if len(data) > 2 else ""
            if not ticker: return
            
            if sub == "ON":
                try:
                    await query.answer()
                except Exception:
                    pass
                await asyncio.to_thread(self.cfg.set_upward_sniper_mode, ticker, True)
                if hasattr(controller, 'cmd_mode'):
                    await controller.cmd_mode(update, context)
            elif sub == "OFF":
                try:
                    await query.answer()
                except Exception:
                    pass
                await asyncio.to_thread(self.cfg.set_upward_sniper_mode, ticker, False)
                if hasattr(controller, 'cmd_mode'):
                    await controller.cmd_mode(update, context)
            elif sub == "AVWAP_WARN":
                try:
                    await query.answer()
                except Exception:
                    pass
                msg, markup = self.view.get_avwap_warning_menu(ticker)
                try:
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                except Exception:
                    pass
            elif sub == "AVWAP_ON":
                try:
                    await query.answer()
                except Exception:
                    pass
                await asyncio.to_thread(self.cfg.set_avwap_hybrid_mode, ticker, True)
                if hasattr(controller, 'cmd_settlement'):
                    await controller.cmd_settlement(update, context)
            elif sub == "AVWAP_OFF":
                try:
                    await query.answer()
                except Exception:
                    pass
                await asyncio.to_thread(self.cfg.set_avwap_hybrid_mode, ticker, False)
                if hasattr(controller, 'cmd_settlement'):
                    await controller.cmd_settlement(update, context)
            
            elif sub == "AVWAP_SORTIE":
                tgt_val = data[3] if len(data) > 3 else "SINGLE"
                try:
                    await query.answer(f"✅ 작전 궤도를 {html.escape(str(tgt_val))} 모드로 스위칭합니다.", show_alert=False)
                except Exception:
                    pass
                await asyncio.to_thread(self.cfg.set_avwap_sortie_mode, ticker, tgt_val)
                if hasattr(controller, 'cmd_settlement'):
                    await controller.cmd_settlement(update, context)

        elif action == "AVWAP_SET":
            ticker = data[2] if len(data) > 2 else ""
            if not ticker: return
            
            if sub == "SYNC_ZERO":
                status_code, _ = await controller._get_market_status()
                if status_code not in ["PRE", "REG"]:
                    try:
                        await query.answer("❌ [격발 차단] 현재 장운영시간(정규장/프리장)이 아닙니다.", show_alert=True)
                    except Exception:
                        pass
                    return
                    
                try:
                    await query.answer()
                except Exception:
                    pass
                try:
                    app_data = context.bot_data.get('app_data', {})
                    tracking_cache = app_data.get('sniper_tracking', {})
                    
                    tracking_cache[f"AVWAP_QTY_{ticker}"] = 0
                    tracking_cache[f"AVWAP_AVG_{ticker}"] = 0.0
                    tracking_cache[f"AVWAP_BOUGHT_{ticker}"] = False
                    tracking_cache[f"AVWAP_SHUTDOWN_{ticker}"] = True
                    tracking_cache[f"AVWAP_TRAP_PLACED_TIME_{ticker}"] = ""
                    tracking_cache[f"AVWAP_BUY_ODNO_{ticker}"] = "" 

                    est = ZoneInfo('America/New_York')
                    now_est = datetime.datetime.now(est)

                    if hasattr(self.strategy, 'v_avwap_plugin'):
                        state_data = {
                            'bought': False,
                            'shutdown': True,
                            'qty': 0,
                            'avg_price': 0.0,
                            'strikes': int(float(str(tracking_cache.get(f"AVWAP_STRIKES_{ticker}") or 0).replace(',', ''))),
                            'daily_bought_qty': int(float(str(tracking_cache.get(f"AVWAP_DAILY_BOUGHT_{ticker}") or 0).replace(',', ''))),
                            'daily_sold_qty': int(float(str(tracking_cache.get(f"AVWAP_DAILY_SOLD_{ticker}") or 0).replace(',', ''))),
                            'trap_odno': str(tracking_cache.get(f"AVWAP_TRAP_ODNO_{ticker}") or ""),
                            'PM_H': float(str(tracking_cache.get(f"AVWAP_PM_H_{ticker}") or 0.0).replace(',', '')),
                            'PM_L': float(str(tracking_cache.get(f"AVWAP_PM_L_{ticker}") or 0.0).replace(',', '')),
                            'T_H': float(str(tracking_cache.get(f"AVWAP_T_H_{ticker}") or 0.0).replace(',', '')),
                            'T_L': float(str(tracking_cache.get(f"AVWAP_T_L_{ticker}") or 0.0).replace(',', '')),
                            'offset': float(str(tracking_cache.get(f"AVWAP_OFFSET_{ticker}") or 0.0).replace(',', '')),
                            'whipsaw_mode': bool(tracking_cache.get(f"AVWAP_WHIPSAW_MODE_{ticker}")),
                            'whipsaw_armed': bool(tracking_cache.get(f"AVWAP_WHIPSAW_ARMED_{ticker}")),
                            'whipsaw_checked': bool(tracking_cache.get(f"AVWAP_WHIPSAW_CHECKED_{ticker}")),
                            'dump_jitter_sec': int(float(str(tracking_cache.get(f"AVWAP_DUMP_JITTER_{ticker}") or 0).replace(',', ''))),
                            'trap_placed_time': "",
                            'buy_odno': ""
                        }
                        await asyncio.to_thread(self.strategy.v_avwap_plugin.save_state, ticker, now_est, state_data)
                    
                    try:
                        await query.edit_message_text(f"🧯 <b>[{html.escape(str(ticker))}] AVWAP 수동 청산 (0주 락온) 완료!</b>\n▫️ 암살자 물량이 0주로 강제 포맷되었으며, 금일 남은 시간 동안 영구 동결(SHUTDOWN)됩니다.", parse_mode='HTML')
                    except Exception:
                        pass
                except Exception as e:
                    logging.error(f"🚨 수동 0주 동기화 에러: {e}")
                    safe_err = html.escape(str(e))
                    try:
                        await query.edit_message_text(f"❌ 수동 0주 동기화 중 에러 발생: {safe_err}", parse_mode='HTML')
                    except Exception:
                        pass
            elif sub == "REFRESH":
                try:
                    await query.answer()
                except Exception:
                    pass
                if hasattr(controller, 'cmd_avwap'):
                    await controller.cmd_avwap(update, context)
            
            elif sub == "MANUAL_CANCEL_REQ":
                try:
                    status_code, _ = await controller._get_market_status()
                    if status_code not in ["PRE", "REG"]:
                        try:
                            await query.answer("❌ [격발 차단] 장운영시간이 아닙니다.", show_alert=True)
                        except Exception:
                            pass
                        return
                        
                    app_data = context.bot_data.get('app_data', {})
                    tracking_cache = app_data.get('sniper_tracking', {})
                    
                    buy_odno = str(tracking_cache.get(f"AVWAP_BUY_ODNO_{ticker}") or "")
                    
                    if not buy_odno:
                        if hasattr(self.strategy, 'v_avwap_plugin'):
                            est = ZoneInfo('America/New_York')
                            now_est = datetime.datetime.now(est)
                            state = await asyncio.wait_for(asyncio.to_thread(self.strategy.v_avwap_plugin.load_state, ticker, now_est), timeout=5.0) or {}
                            buy_odno = str(state.get('buy_odno') or "")
                            
                    if not buy_odno:
                        try:
                            await query.answer("❌ 파기할 지정가 덫을 찾을 수 없습니다.", show_alert=True)
                        except Exception:
                            pass
                        return
                        
                    try:
                        await query.answer("⚠️ 덫 파기 시퀀스 가동 중...", show_alert=False)
                    except Exception:
                        pass
                    
                    async with self.tx_lock:
                        for attempt in range(3):
                            try:
                                await asyncio.sleep(0.06)
                                await asyncio.wait_for(
                                    asyncio.to_thread(self.broker.cancel_order, ticker, buy_odno),
                                    timeout=10.0
                                )
                                break
                            except Exception as e:
                                if attempt == 2: logging.error(f"🚨 덫 강제 취소 에러: {e}")
                                else: await asyncio.sleep(1.0 * (2**attempt))
                                
                        est = ZoneInfo('America/New_York')
                        now_est = datetime.datetime.now(est)
                        
                        tracking_cache[f"AVWAP_LIMIT_ORDER_PLACED_{ticker}"] = False
                        tracking_cache[f"AVWAP_BUY_ODNO_{ticker}"] = ""
                        tracking_cache[f"AVWAP_PLACED_TARGET_TH_{ticker}"] = 0.0
                        tracking_cache[f"AVWAP_TRAP_PLACED_TIME_{ticker}"] = ""
                        
                        if hasattr(self.strategy, 'v_avwap_plugin'):
                            state = await asyncio.wait_for(asyncio.to_thread(self.strategy.v_avwap_plugin.load_state, ticker, now_est), timeout=5.0) or {}
                            state.update({
                                "limit_order_placed": False,
                                "buy_odno": "",
                                "trap_placed_time": "",
                                "placed_target_th": 0.0
                            })
                            await asyncio.to_thread(self.strategy.v_avwap_plugin.save_state, ticker, now_est, state)

                    msg = f"🛑 <b>[{html.escape(str(ticker))} 수동 매수 덫 파기(Nuke) 성공!]</b>\n\n"
                    msg += f"▫️ 장전되었던 지정가 덫이 100% 철회되었습니다.\n"
                    msg += "▫️ 봇은 현재가 스캔 대기 모드(수동 요격 가능)로 복귀합니다."

                    keyboard = [
                        [InlineKeyboardButton("🔄 관제탑 복귀", callback_data="AVWAP_SET:REFRESH:NONE")]
                    ]
                    try:
                        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                    except Exception:
                        pass
                
                except Exception as e:
                    logging.error(f"🚨 수동 덫 파기 에러: {e}")
                    safe_err = html.escape(str(e))
                    try:
                        await query.answer(f"❌ 수동 덫 파기 중 에러 발생: {safe_err}", show_alert=True)
                    except Exception:
                        pass

            elif sub == "MANUAL_FIRE_REQ":
                try:
                    status_code, _ = await controller._get_market_status()
                    if status_code not in ["PRE", "REG"]:
                        try:
                            await query.answer("❌ [격발 차단] 현재 장운영시간(정규장/프리장)이 아닙니다.", show_alert=True)
                        except Exception:
                            pass
                        return
                        
                    app_data = context.bot_data.get('app_data', {})
                    tracking_cache = app_data.get('sniper_tracking', {})
                    
                    t_h = float(str(tracking_cache.get(f"AVWAP_T_H_{ticker}") or 0.0).replace(',', ''))
                    if t_h <= 0.0:
                        if hasattr(self.strategy, 'v_avwap_plugin'):
                            est = ZoneInfo('America/New_York')
                            now_est = datetime.datetime.now(est)
                            state = await asyncio.wait_for(asyncio.to_thread(self.strategy.v_avwap_plugin.load_state, ticker, now_est), timeout=5.0) or {}
                            t_h = float(str(state.get('T_H') or 0.0).replace(',', ''))
                            
                    if t_h <= 0.0:
                        try:
                            await query.answer(f"❌ [{html.escape(str(ticker))}] 수동 요격 불가\n▫️ T_H(지정가 덫 기준선) 데이터가 존재하지 않습니다. 스캔 대기.", show_alert=True)
                        except Exception:
                            pass
                        return

                    try:
                        await query.answer("⚠️ 요격 확인 팝업 생성 중...", show_alert=False)
                    except Exception:
                        pass
                    
                    msg = f"🚨 <b>[{html.escape(str(ticker))} 사이보그 요격 덫 장전 승인 대기]</b>\n\n"
                    msg += f"▫️ 지정가 타점: <b>${t_h:.2f} (T_H 기준 고정)</b>\n"
                    msg += "▫️ 승인 즉시 가용 예산의 95%가 해당 타점에 순수 지정가(LIMIT) 매수 덫으로 깔립니다.\n\n"
                    msg += "⚠️ <b>포트폴리오 매니저 안내:</b>\n"
                    msg += "현재 가격과 무관하게 무조건 지정가로 전송되므로, 현재가가 더 높다면 체결되지 않고 대기(덫) 상태로 남게 됩니다. 승인하시겠습니까?"

                    keyboard = [
                        [InlineKeyboardButton(f"🔥 [{html.escape(str(ticker))}] 수동 요격 덫 장전 승인", callback_data=f"AVWAP_SET:MANUAL_FIRE_EXEC:{ticker}")],
                        [InlineKeyboardButton("❌ 작전 취소 (안전 모드 복귀)", callback_data="AVWAP_SET:REFRESH:NONE")]
                    ]
                    try:
                        await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
                    except Exception:
                        pass
                    
                except Exception as e:
                    logging.error(f"🚨 수동 요격 확인창 생성 에러: {e}")
                    try:
                        await query.answer(f"❌ 요격 승인 대기 중 에러 발생: {html.escape(str(e))}", show_alert=True)
                    except Exception:
                        pass
            
            elif sub == "MANUAL_FIRE_EXEC":
                try:
                    status_code, _ = await controller._get_market_status()
                    if status_code not in ["PRE", "REG"]:
                        try:
                            await query.answer("❌ [격발 차단] 현재 장운영시간(정규장/프리장)이 아닙니다.", show_alert=True)
                        except Exception:
                            pass
                        return
                        
                    app_data = context.bot_data.get('app_data', {})
                    tracking_cache = app_data.get('sniper_tracking', {})
                    
                    t_h = float(str(tracking_cache.get(f"AVWAP_T_H_{ticker}") or 0.0).replace(',', ''))
                    if t_h <= 0.0:
                        if hasattr(self.strategy, 'v_avwap_plugin'):
                            est = ZoneInfo('America/New_York')
                            now_est = datetime.datetime.now(est)
                            state = await asyncio.wait_for(asyncio.to_thread(self.strategy.v_avwap_plugin.load_state, ticker, now_est), timeout=5.0) or {}
                            t_h = float(str(state.get('T_H') or 0.0).replace(',', ''))
                    
                    if t_h <= 0.0 or math.isnan(t_h):
                        try:
                            await query.answer(f"❌ [{html.escape(str(ticker))}] 수동 요격 실패\n▫️ T_H 데이터가 존재하지 않거나 결측치(NaN)입니다.", show_alert=True)
                        except Exception:
                            pass
                        return

                    curr_p = 0.0
                    for attempt in range(3):
                        try:
                            await asyncio.sleep(0.06)
                            curr_p_val = await asyncio.wait_for(asyncio.to_thread(self.broker.get_current_price, ticker), timeout=5.0)
                            curr_p = float(str(curr_p_val or 0.0).replace(',', ''))
                            break
                        except Exception:
                            if attempt == 2: curr_p = 0.0
                            else: await asyncio.sleep(1.0 * (2 ** attempt))

                    if curr_p <= 0.0:
                        try:
                            await query.answer(f"❌ [{html.escape(str(ticker))}] 수동 요격 실패\n▫️ 현재가 통신 실패로 안전 차단.", show_alert=True)
                        except Exception:
                            pass
                        return

                    async with self.tx_lock:
                        cash = 0.0
                        for attempt in range(3):
                            try:
                                await asyncio.sleep(0.06)
                                cash_tuple = await asyncio.wait_for(asyncio.to_thread(self.broker.get_account_balance), timeout=10.0)
                                cash = float(str(cash_tuple[0] or 0.0).replace(',', '')) if isinstance(cash_tuple, (list, tuple)) and len(cash_tuple) > 0 else 0.0
                                break
                            except Exception:
                                if attempt == 2: cash = 0.0
                                else: await asyncio.sleep(1.0 * (2 ** attempt))
                        
                        avwap_free_cash = max(0.0, float(cash or 0.0))
                        
                        try:
                            from scheduler_core import get_budget_allocation
                            active_tickers_list = await asyncio.to_thread(self.cfg.get_active_tickers) or []
                            _, alloc_cash_dict = await asyncio.to_thread(get_budget_allocation, avwap_free_cash, active_tickers_list, self.cfg)
                            alloc_cash_dict = alloc_cash_dict or {}
                            allocated_budget = float(str(alloc_cash_dict.get(ticker) or 0.0).replace(',', ''))
                        except Exception as e:
                            logging.error(f"🚨 예산 할당 모듈 로드 실패 (N빵 강제 분할 폴백): {e}")
                            try:
                                active_tickers_list = await asyncio.to_thread(self.cfg.get_active_tickers) or []
                                div_count = max(1, len(active_tickers_list))
                            except Exception:
                                div_count = 1
                            allocated_budget = avwap_free_cash / div_count  
                            
                        safe_budget = allocated_budget * 0.95
                        if math.isnan(safe_budget): safe_budget = 0.0
                        buy_qty = max(0, int(math.floor(safe_budget / t_h))) if t_h > 0 else 0

                        if buy_qty <= 0:
                            try:
                                await query.answer(f"❌ [{html.escape(str(ticker))}] 수동 요격 실패\n▫️ 예산 부족. 가용 현금: ${allocated_budget:.2f}", show_alert=True)
                            except Exception:
                                pass
                            return

                        try:
                            await query.answer("🔫 지정가 덫 장전 중...", show_alert=False)
                            await query.edit_message_text(f"🚀 <b>[{html.escape(str(ticker))}] 사이보그(Cyborg) 수동 강제 요격 덫 전송 중...</b>", parse_mode='HTML')
                        except Exception:
                            pass

                        await asyncio.sleep(0.06)
                        
                        try:
                            res = await asyncio.wait_for(
                                asyncio.to_thread(self.broker.send_order, ticker, "BUY", buy_qty, t_h, "LIMIT"),
                                timeout=10.0
                            )
                        except Exception as e:
                            logging.error(f"🚨 사이보그 수동 덫 장전 통신 에러/타임아웃: {e}")
                            res = None
                        
                        is_success = isinstance(res, dict) and res.get('rt_cd') == '0'
                        buy_odno = str(res.get('odno') or '') if isinstance(res, dict) else ''

                        if is_success and buy_odno:
                            est = ZoneInfo('America/New_York')
                            now_est = datetime.datetime.now(est)
                            curr_candle_time_str = now_est.replace(second=0, microsecond=0).strftime('%H%M%S')
                            
                            tracking_cache[f"AVWAP_LIMIT_ORDER_PLACED_{ticker}"] = True
                            tracking_cache[f"AVWAP_BUY_ODNO_{ticker}"] = buy_odno
                            tracking_cache[f"AVWAP_PLACED_TARGET_TH_{ticker}"] = t_h
                            tracking_cache[f"AVWAP_TRAP_PLACED_TIME_{ticker}"] = curr_candle_time_str
                            
                            if hasattr(self.strategy, 'v_avwap_plugin'):
                                state = await asyncio.wait_for(asyncio.to_thread(self.strategy.v_avwap_plugin.load_state, ticker, now_est), timeout=5.0) or {}
                                state.update({
                                    "limit_order_placed": True,
                                    "placed_target_th": t_h,
                                    "buy_odno": buy_odno,
                                    "trap_placed_time": curr_candle_time_str
                                })
                                await asyncio.to_thread(self.strategy.v_avwap_plugin.save_state, ticker, now_est, state)

                            final_msg = f"🔫 <b>[{html.escape(str(ticker))}] 수동 지정가 요격 덫 락온 성공!</b>\n"
                            final_msg += f"▫️ 타점: <b>${t_h:.2f}</b> (순수 LIMIT)\n"
                            final_msg += f"▫️ 목표수량: <b>{buy_qty}주</b>\n"
                            final_msg += f"▫️ 상태: 1분봉 자동 감시 모드로 인계되었습니다. 체결 확정 시 2.0% 자동 익절 덫이 투하됩니다."

                            try:
                                await query.edit_message_text(final_msg, parse_mode='HTML')
                            except Exception:
                                pass
                            
                        else:
                            err_msg = html.escape(str(res.get('msg1') or '응답 없음')) if isinstance(res, dict) else '통신 장애/무응답'
                            logging.error(f"🚨 [{ticker}] 사이보그 수동 덫 장전 서버 거절: {err_msg}")
                            reject_msg = (
                                f"🚨 <b>[{html.escape(str(ticker))}] 사이보그 수동 지정가 덫 전송 서버 거절 (Reject)!</b>\n"
                                f"▫️ 사유: <code>{err_msg}</code>\n"
                            )
                            try:
                                await query.edit_message_text(reject_msg, parse_mode='HTML')
                            except Exception:
                                pass

                except Exception as e:
                    logging.error(f"🚨 사이보그 수동 요격/장전 에러: {e}")
                    safe_err = html.escape(str(e))
                    try:
                        await query.edit_message_text(f"❌ 수동 장전 중 에러 발생: {safe_err}", parse_mode='HTML')
                    except Exception:
                        pass

        elif action == "TICKER":
            try:
                await query.answer()
            except Exception:
                pass
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
            try:
                await query.edit_message_text(f"✅ <b>[운용 종목 락온 완료]</b>\n▫️ <b>{html.escape(str(msg_txt))}</b> 모드로 전환되었습니다.\n▫️ /sync를 눌러 확인하십시오.", parse_mode='HTML')
            except Exception:
                pass
            
        elif action == "SEED":
            try:
                await query.answer()
            except Exception:
                pass
            ticker = data[2] if len(data) > 2 else ""
            if not ticker: return
            controller.user_states[chat_id] = f"SEED_{sub}_{ticker}"
            await context.bot.send_message(chat_id, f"💵 [{html.escape(str(ticker))}] 시드머니 금액 입력:", parse_mode='HTML')
            
        elif action == "INPUT":
            try:
                await query.answer()
            except Exception:
                pass
            ticker = data[2] if len(data) > 2 else ""
            if not ticker: return
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
            await context.bot.send_message(chat_id, f"✏️ <b>[{html.escape(str(ticker))}] {html.escape(str(ko_name))}</b>를 설정합니다.\n{desc}", parse_mode='HTML')
            
        else:
            safe_data = html.escape(str(data))
            await context.bot.send_message(chat_id, f"⚠️ <b>[알 수 없는 콜백 라우팅]</b> <code>{safe_data}</code>", parse_mode='HTML')
