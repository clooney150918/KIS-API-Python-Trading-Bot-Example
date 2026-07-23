# ==========================================================
# FILE: callback_config_handler.py
# ==========================================================
# 🚨 MODIFIED: [Reset 0주 오인 패러독스 소각] 리셋(장부 소각) 후 새로운 스냅샷을 박제할 때, qty=0 을 강제 주입하던 치명적 하드코딩 버그를 소각하고 KIS 실잔고(kis_qty)와 평단가(kis_avg)를 정밀 추출하여 팩트 주입 락온 완료.
import logging
import datetime
import math
from zoneinfo import ZoneInfo
import os
import json
import asyncio
import tempfile
import html
from telegram import Update
from telegram.ext import ContextTypes

class CallbackConfigHandler:
    def __init__(self, config, broker, strategy, queue_ledger, sync_engine, view, tx_lock):
        self.cfg = config
        self.broker = broker
        self.strategy = strategy
        self.queue_ledger = queue_ledger
        self.sync_engine = sync_engine
        self.view = view
        self.tx_lock = tx_lock

    def _safe_float(self, val):
        try:
            f_val = float(str(val or 0.0).replace(',', ''))
            if math.isnan(f_val) or math.isinf(f_val):
                return 0.0
            return f_val
        except Exception:
            return 0.0

    async def handle(self, update: Update, context: ContextTypes.DEFAULT_TYPE, controller, action: str, sub: str, data: list):
        query = update.callback_query
        chat_id = update.effective_chat.id
        ticker = data[2] if len(data) > 2 else ""

        if action == "UPDATE":
            try: await query.answer()
            except Exception: pass
            if sub == "CONFIRM":
                from plugin_updater import SystemUpdater
                updater = SystemUpdater()
                await query.edit_message_text("⏳ <b>[업데이트 승인됨]</b> GitHub 코드를 강제 페칭합니다...", parse_mode='HTML')
                try:
                    success, msg = await updater.pull_latest_code()
                    safe_msg = html.escape(str(msg)) 
                    if success:
                        await query.edit_message_text(f"✅ <b>[업데이트 완료]</b> {safe_msg}\n\n🔄 시스템 데몬(pipiosbot)을 OS 단에서 재가동합니다. 다운타임 후 봇이 다시 깨어납니다.", parse_mode='HTML')
                        await updater.restart_daemon()
                    else:
                        await query.edit_message_text(f"❌ <b>[동기화 실패]</b>\n▫️ 사유: {safe_msg}", parse_mode='HTML')
                except Exception as e:
                    safe_err = html.escape(str(e))
                    await query.edit_message_text(f"🚨 <b>[치명적 오류]</b> 프로세스 예외 발생: {safe_err}", parse_mode='HTML')

            elif sub == "CANCEL":
                await query.edit_message_text("❌ 자가 업데이트를 취소했습니다.", parse_mode='HTML')

        elif action == "VERSION":
            try: await query.answer()
            except Exception: pass
            history_data = await asyncio.to_thread(self.cfg.get_full_version_history) or []
            if sub == "LATEST":
                msg, markup = self.view.get_version_message(history_data, page_index=None)
                try: await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                except Exception: pass
            elif sub == "PAGE":
                page_idx = int(data[2]) if len(data) > 2 else 0
                msg, markup = self.view.get_version_message(history_data, page_index=page_idx)
                try: await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                except Exception: pass

        elif action == "RESET":
            try: await query.answer()
            except Exception: pass
            if sub == "MENU":
                active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers) or []
                msg, markup = self.view.get_reset_menu(active_tickers)
                try: await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                except Exception: pass
            elif sub == "LOCK": 
                if ticker:
                    def _hijack_vwap_lock():
                        slice_file = f"data/vrev_slice_state_{ticker}.json"
                        try:
                            if os.path.exists(slice_file):
                                with open(slice_file, 'r', encoding='utf-8') as f:
                                    s_state = json.load(f)
                                s_state['hijacked'] = True
                                s_state['orders'] = []
                                with open(slice_file, 'w', encoding='utf-8') as f:
                                    json.dump(s_state, f, ensure_ascii=False, indent=4)
                        except Exception: pass
                        try:
                            est_now = datetime.datetime.now(ZoneInfo('America/New_York'))
                            today_str = est_now.strftime("%Y-%m-%d")
                            snap_file = f"data/daily_snapshot_REV_{today_str}_{ticker}.json"
                            if os.path.exists(snap_file):
                                os.remove(snap_file)
                        except Exception: pass
                        
                    await asyncio.to_thread(_hijack_vwap_lock)
                    await asyncio.to_thread(self.cfg.reset_lock_for_ticker, ticker)
                    try: await query.edit_message_text(f"✅ <b>[{html.escape(str(ticker))}] 금일 매매 잠금이 해제되었으며, 오염된 슬라이싱 엔진도 무효화되었습니다.</b>", parse_mode='HTML')
                    except Exception: pass
            elif sub == "REV":
                if ticker:
                    msg, markup = self.view.get_reset_confirm_menu(ticker)
                    try: await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                    except Exception: pass
            elif sub == "CONFIRM":
                if not ticker: return
                
                current_ver = str(await asyncio.to_thread(self.cfg.get_version, ticker) or "")
                is_rev_active = (current_ver == "V_REV")
                await asyncio.to_thread(self.cfg.set_reverse_state, ticker, is_rev_active, 0)
             
                ledger = await asyncio.to_thread(self.cfg.get_ledger) or []
                ledger_data = [r for r in ledger if isinstance(r, dict) and str(r.get('ticker')) != str(ticker)]
                await asyncio.to_thread(self.cfg._save_json, self.cfg.FILES["LEDGER"], ledger_data)
                
                def _process_reset_files():
                    backup_file = self.cfg.FILES["LEDGER"].replace(".json", "_backup.json")
                    try:
                        with open(backup_file, 'r', encoding='utf-8') as f:
                            b_data = json.load(f)
                        if not isinstance(b_data, list): b_data = []
                        b_data = [r for r in b_data if isinstance(r, dict) and str(r.get('ticker')) != str(ticker)]
                    
                        dir_name = os.path.dirname(backup_file) or '.'
                        fd, tmp_path = tempfile.mkstemp(dir=dir_name)
                        try:
                            with os.fdopen(fd, 'w', encoding='utf-8') as f_out:
                                json.dump(b_data, f_out, ensure_ascii=False, indent=4)
                                f_out.flush()
                                os.fsync(f_out.fileno())
                            os.replace(tmp_path, backup_file)
                        except Exception:
                            try: os.remove(tmp_path)
                            except OSError: pass
                    except OSError: pass
                    except Exception: pass
                     
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
                        prev_c = self._safe_float(prev_c_val)
                        break
                    except Exception as e:
                        if attempt == 2: logging.error(f"🚨 수동 소각 후 전일 종가 스캔 에러: {e}")
                        else: await asyncio.sleep(1.0 * (2 ** attempt))
                
                if prev_c > 0:
                    try:
                        kis_qty = 0
                        kis_avg = 0.0
                        async with self.tx_lock:
                            cash_val = 0.0
                            for attempt in range(3):
                                try:
                                    await asyncio.sleep(0.06)
                                    cash_tuple = await asyncio.wait_for(asyncio.to_thread(self.broker.get_account_balance), timeout=10.0)
                                    cash_val = cash_tuple[0] if isinstance(cash_tuple, (list, tuple)) and len(cash_tuple) > 0 else 0.0
                                    holdings = cash_tuple[1] if isinstance(cash_tuple, (list, tuple)) and len(cash_tuple) > 1 else {}
                                    break
                                except Exception:
                                    if attempt == 2: holdings = {}
                                    else: await asyncio.sleep(1.0 * (2 ** attempt))
                                    
                            cash = self._safe_float(cash_val)
                            
                            # 🚨 MODIFIED: [0주 오인 패러독스 수술] 장부 소각 후 KIS 실잔고 팩트 무조건 추출 및 주입
                            if isinstance(holdings, dict) and ticker in holdings:
                                kis_qty = int(self._safe_float(holdings[ticker].get('qty', 0)))
                                kis_avg = self._safe_float(holdings[ticker].get('avg', 0.0))
                            
                            from scheduler_core import get_budget_allocation
                            active_tickers_list = await asyncio.to_thread(self.cfg.get_active_tickers) or []
                            _, alloc_cash_dict = await asyncio.to_thread(get_budget_allocation, cash, active_tickers_list, self.cfg)
                            alloc_cash_dict = alloc_cash_dict or {}
                            available_cash = self._safe_float(alloc_cash_dict.get(ticker))
                            
                            await asyncio.to_thread(
                                self.strategy.get_plan, 
                                ticker, 0.0, kis_avg, kis_qty, prev_c, # 🚨 [팩트 주입] 0, 0.0 데드코드 소각 및 KIS 실잔고 주입
                                ma_5day=0.0, market_type="REG", available_cash=available_cash, 
                                is_simulation=True, is_snapshot_mode=True
                            )
                    except Exception as e:
                        logging.error(f"🚨 0주 강제 스냅샷 오버라이드 에러: {e}")

                try:
                    await query.edit_message_text(f"✅ <b>[{html.escape(str(ticker))}] 삼위일체 소각(Nuke) 및 초기화 완료!</b>\n▫️ 본장부, 백업장부, 큐(Queue) 찌꺼기 데이터가 100% 영구 삭제되었습니다.\n▫️ KIS 실잔고 동기화, 매매 잠금 해제 및 디커플링 타점 스냅샷 원자적 덮어쓰기가 완벽히 집행되었습니다.", parse_mode='HTML')
                except Exception: pass
       
            elif sub == "CANCEL":
                try: await query.edit_message_text("❌ 닫았습니다.", parse_mode='HTML')
                except Exception: pass

        elif action == "REC":
            try: await query.answer()
            except Exception: pass
            if sub == "VIEW": 
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
                if not ticker: return
                if ticker not in self.sync_engine.sync_locks:
                    self.sync_engine.sync_locks[ticker] = asyncio.Lock()
                    
                if not self.sync_engine.sync_locks[ticker].locked():
                    try: await query.edit_message_text(f"🔄 <b>[{html.escape(str(ticker))}] 잔고 기반 대시보드 업데이트 중...</b>", parse_mode='HTML')
                    except Exception: pass
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
            try: await query.answer()
            except Exception: pass
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
                       
                    try:
                        qty, avg, invested, sold = await asyncio.to_thread(self.cfg.calculate_holdings, target.get('ticker'), safe_trades)
                        msg, markup = self.view.create_ledger_dashboard(target.get('ticker'), qty, avg, invested, sold, safe_trades, 0, 0, is_history=True, history_id=hid)
                    except TypeError:
                        msg, markup = self.view.create_ledger_dashboard(target.get('ticker'), qty, avg, invested, sold, safe_trades, 0, 0, is_history=True)
                     
                    try: await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                    except Exception: pass
             
            elif sub == "LIST":
                if hasattr(controller, 'cmd_history'):
                    await controller.cmd_history(update, context)

            elif sub == "IMG":
                target_id = int(data[3]) if len(data) > 3 else None
                if not ticker: return
                hist_data = await asyncio.to_thread(self.cfg.get_history) or []
                hist_list = [h for h in hist_data if isinstance(h, dict) and str(h.get('ticker')) == str(ticker)]
                  
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

                    profit_val = self._safe_float(target_hist.get('profit'))
                    yield_pct_val = self._safe_float(target_hist.get('yield'))
                    invested_val = self._safe_float(target_hist.get('invested'))
                    revenue_val = self._safe_float(target_hist.get('revenue'))

                    img_path = await asyncio.to_thread(
                        self.view.create_profit_image,
                        ticker=target_hist.get('ticker'),
                        profit=profit_val,
                        yield_pct=yield_pct_val,
                        invested=invested_val,
                        revenue=revenue_val,
                        end_date=target_hist.get('end_date')
                    )
            
                    if img_path:
                        def _read_img4(p):
                            with open(p, 'rb') as f_in: return f_in.read()
                        try:
                            img_bytes4 = await asyncio.to_thread(_read_img4, img_path)
                            if str(img_path).lower().endswith('.gif'):
                                await context.bot.send_animation(chat_id=chat_id, animation=img_bytes4)
                            else:
                                await context.bot.send_photo(chat_id=chat_id, photo=img_bytes4)
                            try: await query.delete_message()
                            except Exception: pass
                        except OSError:
                            await query.edit_message_text("❌ 이미지 파일 읽기에 실패했습니다.", parse_mode='HTML')
                    else:
                        await query.edit_message_text("❌ 이미지 생성에 실패했습니다.", parse_mode='HTML')
                except Exception as e:
                    logging.error(f"📸 👑 졸업 이미지 생성/발송 실패: {e}")
                    try: await query.edit_message_text("❌ 이미지 생성 중 오류가 발생했습니다.", parse_mode='HTML')
                    except Exception: pass

        elif action == "SET_VER":
            try: await query.answer()
            except Exception: pass
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
                kis_qty = int(self._safe_float(holdings.get(ticker, {}).get('qty')))
            except Exception:
                kis_qty = 0
            
            max_qty = kis_qty
            
            try:
                full_ledger = await asyncio.to_thread(self.cfg.get_ledger) or []
                recs = [r for r in full_ledger if isinstance(r, dict) and str(r.get('ticker')) == ticker]
                if recs:
                    ledger_qty, _, _, _ = await asyncio.to_thread(self.cfg.calculate_holdings, ticker, recs)
                    if ledger_qty > max_qty: max_qty = ledger_qty
            except Exception as e:
                logging.error(f"🚨 모드 전환 전 V14 장부 검증 에러: {e}")
                
            try:
                if getattr(self, 'queue_ledger', None):
                    q_data = await asyncio.to_thread(self.queue_ledger.get_queue, ticker) or []
                    if q_data:
                        vrev_qty = sum(int(self._safe_float(item.get('qty'))) for item in q_data if isinstance(item, dict))
                        if vrev_qty > max_qty: max_qty = vrev_qty
            except Exception as e:
                logging.error(f"🚨 모드 전환 전 V-REV 큐 장부 검증 에러: {e}")
            
            if max_qty > 0:
                try:
                    await query.edit_message_text(f"🛑 <b>[{html.escape(str(ticker))} 모드 전환 차단]</b>\n\n현재 계좌 또는 장부에 단 1주라도 잔고({max_qty}주)가 존재하면 코어 스위칭이 불가능합니다.\n전량 익절(0주) 후 0주 새출발 상태에서 다시 시도해 주십시오.", parse_mode='HTML')
                except Exception: pass
                return
                
            if sub == "V_REV":
                msg, markup = self.view.get_vrev_mode_selection_menu(ticker)
            elif sub == "V14":
                msg, markup = self.view.get_v14_mode_selection_menu(ticker)
            else:
                return
            
            try: await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
            except Exception: pass

        elif action == "SET_VER_CONFIRM":
            try: await query.answer()
            except Exception: pass
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
                
            try: await query.edit_message_text(msg, parse_mode='HTML')
            except Exception: pass

        elif action == "TICKER":
            try: await query.answer()
            except Exception: pass
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
            try: await query.edit_message_text(f"✅ <b>[운용 종목 락온 완료]</b>\n▫️ <b>{html.escape(str(msg_txt))}</b> 모드로 전환되었습니다.\n▫️ /sync를 눌러 확인하십시오.", parse_mode='HTML')
            except Exception: pass
            
        elif action == "SEED":
            try: await query.answer()
            except Exception: pass
            if not ticker: return
            controller.user_states[chat_id] = f"SEED_{sub}_{ticker}"
            await context.bot.send_message(chat_id, f"💵 [{html.escape(str(ticker))}] 시드머니 금액 입력:", parse_mode='HTML')
            
        elif action == "INPUT":
            try: await query.answer()
            except Exception: pass
            if not ticker: return
            controller.user_states[chat_id] = f"CONF_{sub}_{ticker}"
           
            if sub == "SPLIT": ko_name = "분할 횟수"
            elif sub == "TARGET": ko_name = "목표 수익률(%)"
            elif sub == "COMPOUND": ko_name = "자동 복리율(%)"
            elif sub == "STOCK_SPLIT": ko_name = "액면 분할/병합 비율 (예: 10분할은 10, 10병합은 0.1)"
            elif sub == "FEE": ko_name = "증권사 수수료율(%)"
            else: ko_name = "값"
            
            desc = "숫자만 입력하세요.\n(예: 액면분할 시 1주가 10주가 되었다면 10 입력, 10주가 1주로 병합되었다면 0.1 입력)" if sub == "STOCK_SPLIT" else "숫자만 입력하세요."
            await context.bot.send_message(chat_id, f"✏️ <b>[{html.escape(str(ticker))}] {html.escape(str(ko_name))}</b>를 설정합니다.\n{desc}", parse_mode='HTML')
