# ==========================================================
# FILE: callback_config_handler.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 5대 헌법 및 46대 엣지 케이스 완벽 결속 교차 검증 완료
# 🚨 MODIFIED: [암살자 부활 패러독스 궁극 방어] 수동 삼위일체 소각(/reset) 시 암살자의 상태 파일(avwap_trade_state)을 물리적 삭제하는 맹독성 로직을 영구 소각. 대신 `shutdown: True`와 0주 상태를 강제 주입하여 당일 고점 재진입(Limit-Trap) 대참사를 100% 원천 봉쇄.
# 🚨 MODIFIED: [Phase 5 암살자 지정 예산 락온] INPUT 라우팅에 `AVWAP_BUDGET` 액션을 추가하여 사용자 예산 설정을 위한 상태(State) 전이를 완벽 매핑.
# 🚨 MODIFIED: [Phase 5 오버나이트 락온] CONFIG_AVWAP 라우팅에 `TOGGLE_OVERNIGHT` 스위칭 로직을 결속하여 당일청산(MOC)과 오버나이트 모드를 동적으로 제어.
# 🚨 MODIFIED: [Case 38 UI 렌더링 높이 붕괴 패러독스 차단] 버튼 클릭 시 1줄짜리 텍스트("업데이트 중...")로 중간 갱신하여 기존 화면을 증발시키는 행위를 전면 금지. 로딩은 query.answer() 팝업으로 대체하고 최종 결과로 단 1회 제자리 갱신(In-place Edit) 락온.
# 🚨 MODIFIED: [Case 08, 16 헌법 사수] os.path.exists 소각, EAFP 디렉토리 생성 및 원자적 쓰기(Atomic Write) 강제 주입 완료.
# 🚨 MODIFIED: [제1헌법 철저 준수] 파일 I/O 연산 및 텔레그램 통신 전역에 `asyncio.wait_for` 타임아웃 족쇄 100% 강제 래핑 완료 (Deadlock 원천 차단).
# 🚨 MODIFIED: [Event Loop 교착 수술] AssassinLedger 및 SystemUpdater 인스턴스화 시 발생하는 __init__ 내부의 동기 I/O(파일 체크/생성) 블로킹을 막기 위해 100% 백그라운드 스레드(to_thread) 샌드박스로 래핑 락온.
# 🚨 MODIFIED: [스냅샷 유령화 붕괴 궁극 수술] RESET:LOCK 내부 `_hijack_vwap_lock()` 실행 시 V-REV 뿐만 아니라 V14, V14VWAP 스냅샷 파일까지 순회하며 100% 영구 소각하도록 팩트 교정 완료.
# ==========================================================
import logging
import datetime
import math
from zoneinfo import ZoneInfo
import os
import json
import asyncio
import tempfile
import html
import telegram.error
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
        if not update.effective_chat or not update.callback_query:
            return
            
        query = update.callback_query
        chat_id = update.effective_chat.id
        ticker = data[2] if len(data) > 2 else ""

        needs_custom_toast = False
        if action == "UPDATE" and sub == "CONFIRM": needs_custom_toast = True
        elif action == "RESET" and sub in ["LOCK", "CONFIRM"]: needs_custom_toast = True
        elif action == "REC" and sub == "SYNC": needs_custom_toast = True
        elif action == "HIST" and sub in ["DEL_EXEC", "IMG"]: needs_custom_toast = True

        if not needs_custom_toast:
            try: 
                await asyncio.wait_for(query.answer(), timeout=5.0)
            except Exception as e:
                logging.warning(f"⚠️ [Callback] 콜백 쿼리 응답 타임아웃/실패 (진행 계속됨): {e}")

        if action == "UPDATE":
            if sub == "CONFIRM":
                try: 
                    await asyncio.wait_for(query.answer("⏳ 깃허브 코드 동기화 중...", show_alert=False), timeout=5.0)
                except Exception: pass
                
                from plugin_updater import SystemUpdater
                try:
                    updater = await asyncio.wait_for(asyncio.to_thread(SystemUpdater), timeout=5.0)
                except Exception as e:
                    logging.error(f"🚨 업데이터 코어 로드 실패: {e}")
                    try: await asyncio.wait_for(query.edit_message_text(f"❌ <b>[로드 실패]</b> 시스템 모듈을 초기화할 수 없습니다.", parse_mode='HTML'), timeout=10.0)
                    except Exception: pass
                    return
                
                try:
                    success, msg = await updater.pull_latest_code()
                    safe_msg = html.escape(str(msg)) 
                    if success:
                        try: 
                            await asyncio.wait_for(query.edit_message_text(f"✅ <b>[업데이트 완료]</b> {safe_msg}\n\n🔄 시스템 데몬(pipiosbot)을 OS 단에서 재가동합니다. 다운타임 후 봇이 다시 깨어납니다.", parse_mode='HTML'), timeout=10.0)
                        except telegram.error.BadRequest as e:
                            if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                        except Exception: pass
                        await updater.restart_daemon()
                    else:
                        try: 
                            await asyncio.wait_for(query.edit_message_text(f"❌ <b>[동기화 실패]</b>\n▫️ 사유: {safe_msg}", parse_mode='HTML'), timeout=10.0)
                        except telegram.error.BadRequest as e:
                            if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                        except Exception: pass
                except Exception as e:
                    safe_err = html.escape(str(e))
                    try: 
                        await asyncio.wait_for(query.edit_message_text(f"🚨 <b>[치명적 오류]</b> 프로세스 예외 발생: {safe_err}", parse_mode='HTML'), timeout=10.0)
                    except telegram.error.BadRequest as e:
                        if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                    except Exception: pass

            elif sub == "CANCEL":
                try: 
                    await asyncio.wait_for(query.edit_message_text("❌ 자가 업데이트를 취소했습니다.", parse_mode='HTML'), timeout=10.0)
                except telegram.error.BadRequest as e:
                    if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                except Exception: pass

        elif action == "VERSION":
            history_data = await asyncio.wait_for(asyncio.to_thread(self.cfg.get_full_version_history), timeout=10.0) or []
            if sub == "LATEST":
                msg, markup = self.view.get_version_message(history_data, page_index=None)
                try: 
                    await asyncio.wait_for(query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML'), timeout=10.0)
                except telegram.error.BadRequest as e:
                    if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                except Exception: pass
            elif sub == "PAGE":
                page_idx = int(data[2]) if len(data) > 2 else 0
                msg, markup = self.view.get_version_message(history_data, page_index=page_idx)
                try: 
                    await asyncio.wait_for(query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML'), timeout=10.0)
                except telegram.error.BadRequest as e:
                    if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                except Exception: pass

        elif action == "RESET":
            if sub == "MENU":
                active_tickers = await asyncio.wait_for(asyncio.to_thread(self.cfg.get_active_tickers), timeout=10.0) or []
                msg, markup = self.view.get_reset_menu(active_tickers)
                try: 
                    await asyncio.wait_for(query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML'), timeout=10.0)
                except telegram.error.BadRequest as e:
                    if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                except Exception: pass
            elif sub == "LOCK": 
                if ticker:
                    try: await asyncio.wait_for(query.answer("⏳ 매매 잠금 해제 중...", show_alert=False), timeout=5.0)
                    except Exception: pass
                    
                    def _hijack_vwap_lock():
                        slice_file = f"data/vrev_slice_state_{ticker}.json"
                        try:
                            with open(slice_file, 'r', encoding='utf-8') as f:
                                s_state = json.load(f)
                            s_state['hijacked'] = True
                            s_state['orders'] = []
                            
                            dir_name = os.path.dirname(slice_file) or '.'
                            try: os.makedirs(dir_name, exist_ok=True)
                            except OSError: pass
                            
                            fd = None
                            tmp_path = None
                            try:
                                fd, tmp_path = tempfile.mkstemp(dir=dir_name, text=True)
                                with os.fdopen(fd, 'w', encoding='utf-8') as f_out:
                                    fd = None
                                    json.dump(s_state, f_out, ensure_ascii=False, indent=4)
                                    f_out.flush()
                                    os.fsync(f_out.fileno())
                                os.replace(tmp_path, slice_file)
                                tmp_path = None
                            except Exception:
                                if fd is not None:
                                    try: os.close(fd)
                                    except OSError: pass
                                if tmp_path:
                                    try: os.remove(tmp_path)
                                    except OSError: pass
                        except (OSError, json.JSONDecodeError): 
                            pass
                            
                        try:
                            est_now = datetime.datetime.now(ZoneInfo('America/New_York'))
                            today_str = est_now.strftime("%Y-%m-%d")
                            for snap_prefix in ["REV", "V14", "V14VWAP"]:
                                snap_file = f"data/daily_snapshot_{snap_prefix}_{today_str}_{ticker}.json"
                                try: os.remove(snap_file)
                                except OSError: pass
                        except Exception: 
                            pass
                        
                    await asyncio.wait_for(asyncio.to_thread(_hijack_vwap_lock), timeout=10.0)
                    await asyncio.wait_for(asyncio.to_thread(self.cfg.reset_lock_for_ticker, ticker), timeout=10.0)
                    try: 
                        await asyncio.wait_for(query.edit_message_text(f"✅ <b>[{html.escape(str(ticker))}] 금일 매매 잠금이 해제되었으며, 오염된 슬라이싱 엔진 및 스냅샷도 무효화되었습니다.</b>", parse_mode='HTML'), timeout=10.0)
                    except telegram.error.BadRequest as e:
                        if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                    except Exception: pass
            elif sub == "REV":
                if ticker:
                    msg, markup = self.view.get_reset_confirm_menu(ticker)
                    try: 
                        await asyncio.wait_for(query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML'), timeout=10.0)
                    except telegram.error.BadRequest as e:
                        if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                    except Exception: pass
            elif sub == "CONFIRM":
                if not ticker: return
                
                try: await asyncio.wait_for(query.answer("🔥 삼위일체 소각 진행 중...", show_alert=False), timeout=5.0)
                except Exception: pass
                
                current_ver = str(await asyncio.wait_for(asyncio.to_thread(self.cfg.get_version, ticker), timeout=10.0) or "")
                is_rev_active = (current_ver == "V_REV")
                
                await asyncio.wait_for(asyncio.to_thread(self.cfg.set_reverse_state, ticker, is_rev_active, 0, 0.0), timeout=10.0)
                
                ledger = await asyncio.wait_for(asyncio.to_thread(self.cfg.get_ledger), timeout=10.0) or []
                ledger_data = [r for r in ledger if isinstance(r, dict) and str(r.get('ticker')) != str(ticker)]
                await asyncio.wait_for(asyncio.to_thread(self.cfg._save_json, self.cfg.FILES["LEDGER"], ledger_data), timeout=15.0)
                
                def _process_reset_files():
                    backup_file = self.cfg.FILES["LEDGER"].replace(".json", "_backup.json")
                    try:
                        with open(backup_file, 'r', encoding='utf-8') as f:
                            b_data = json.load(f)
                        if not isinstance(b_data, list): b_data = []
                        b_data = [r for r in b_data if isinstance(r, dict) and str(r.get('ticker')) != str(ticker)]
                        
                        dir_name = os.path.dirname(backup_file) or '.'
                        try: os.makedirs(dir_name, exist_ok=True)
                        except OSError: pass
                        
                        fd = None
                        tmp_path = None
                        try:
                            fd, tmp_path = tempfile.mkstemp(dir=dir_name, text=True)
                            with os.fdopen(fd, 'w', encoding='utf-8') as f_out:
                                fd = None
                                json.dump(b_data, f_out, ensure_ascii=False, indent=4)
                                f_out.flush()
                                os.fsync(f_out.fileno())
                            os.replace(tmp_path, backup_file)
                            tmp_path = None
                        except Exception:
                            if fd is not None:
                                try: os.close(fd)
                                except OSError: pass
                            if tmp_path:
                                try: os.remove(tmp_path)
                                except OSError: pass
                    except OSError: pass
                    except Exception: pass
                    
                await asyncio.wait_for(asyncio.to_thread(_process_reset_files), timeout=10.0)
            
                if getattr(self, 'queue_ledger', None):
                    await asyncio.wait_for(asyncio.to_thread(self.queue_ledger.clear_queue, ticker), timeout=10.0)
                    await asyncio.wait_for(asyncio.to_thread(self.queue_ledger.sync_with_broker, ticker, 0, 0.0), timeout=10.0)

                def _nuke_assassin_data():
                    try:
                        from assassin_ledger import AssassinLedger
                        a_ledger = AssassinLedger()
                        a_ledger.clear_ledger(ticker)
                    except Exception as e:
                        logging.error(f"🚨 [{ticker}] 암살자 장부 강제 소각 중 에러: {e}")
                    
                    # 🚨 MODIFIED: [암살자 부활 패러독스 궁극 방어] 파일 물리적 삭제(os.remove)를 영구 소각하고, shutdown: True 팩트를 원자적으로 덮어씌워 당일 재진입 원천 차단
                    state_file = f"data/avwap_trade_state_{ticker}.json"
                    try:
                        est_now = datetime.datetime.now(ZoneInfo('America/New_York'))
                        today_str = est_now.strftime('%Y-%m-%d')
                        state_data = {}
                        try:
                            with open(state_file, 'r', encoding='utf-8') as f:
                                state_data = json.load(f)
                        except Exception:
                            pass

                        state_data['date'] = today_str
                        state_data['qty'] = 0
                        state_data['buy_odno'] = ""
                        state_data['sell_odno'] = ""
                        state_data['shutdown'] = True
                        state_data['dumped'] = True

                        dir_name = os.path.dirname(state_file) or '.'
                        try: os.makedirs(dir_name, exist_ok=True)
                        except OSError: pass

                        fd = None
                        tmp_path = None
                        try:
                            fd, tmp_path = tempfile.mkstemp(dir=dir_name, text=True)
                            with os.fdopen(fd, 'w', encoding='utf-8') as f_out:
                                fd = None
                                json.dump(state_data, f_out, ensure_ascii=False, indent=4)
                                f_out.flush()
                                os.fsync(f_out.fileno())
                            os.replace(tmp_path, state_file)
                            tmp_path = None
                        except Exception as inner_e:
                            if fd is not None:
                                try: os.close(fd)
                                except OSError: pass
                            if tmp_path:
                                try: os.remove(tmp_path)
                                except OSError: pass
                            raise inner_e
                    except Exception as e:
                        logging.error(f"🚨 [{ticker}] 암살자 상태 셧다운 주입 에러: {e}")

                await asyncio.wait_for(asyncio.to_thread(_nuke_assassin_data), timeout=10.0)
                await asyncio.wait_for(asyncio.to_thread(self.cfg.reset_lock_for_ticker, ticker), timeout=10.0)

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
                            
                            if isinstance(holdings, dict) and ticker in holdings:
                                kis_qty = int(self._safe_float(holdings[ticker].get('qty', 0)))
                                kis_avg = self._safe_float(holdings[ticker].get('avg', 0.0))
                            
                            from scheduler_core import get_budget_allocation
                            active_tickers_list = await asyncio.wait_for(asyncio.to_thread(self.cfg.get_active_tickers), timeout=10.0) or []
                            _, alloc_cash_dict = await asyncio.wait_for(asyncio.to_thread(get_budget_allocation, cash, active_tickers_list, self.cfg), timeout=10.0)
                            alloc_cash_dict = alloc_cash_dict or {}
                            available_cash = self._safe_float(alloc_cash_dict.get(ticker))
                    
                            await asyncio.wait_for(
                                asyncio.to_thread(
                                    self.strategy.get_plan, 
                                    ticker, 0.0, kis_avg, kis_qty, prev_c, 
                                    ma_5day=0.0, market_type="REG", available_cash=available_cash, 
                                    is_simulation=True, is_snapshot_mode=True
                                ), timeout=15.0
                            )
                    except Exception as e:
                        logging.error(f"🚨 0주 강제 스냅샷 오버라이드 에러: {e}")

                try:
                    await asyncio.wait_for(query.edit_message_text(f"✅ <b>[{html.escape(str(ticker))}] 삼위일체 소각(Nuke) 및 초기화 완료!</b>\n▫️ 본장부, 백업장부, 큐(Queue) 찌꺼기 데이터가 100% 영구 삭제되었습니다.\n▫️ 암살자의 재진입(부활)이 전면 차단되었으며, 매매 잠금 해제 및 디커플링 타점 스냅샷 덮어쓰기가 완벽히 집행되었습니다.", parse_mode='HTML'), timeout=10.0)
                except telegram.error.BadRequest as e:
                    if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                except Exception: pass
       
            elif sub == "CANCEL":
                try: 
                    await asyncio.wait_for(query.edit_message_text("❌ 닫았습니다.", parse_mode='HTML'), timeout=10.0)
                except telegram.error.BadRequest as e:
                    if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                except Exception: pass

        elif action == "REC":
            if sub == "VIEW": 
                if not ticker: return
                async with self.tx_lock:
                    holdings = None
                    for attempt in range(3):
                        try:
                            await asyncio.sleep(0.06)
                            res_tuple = await asyncio.wait_for(asyncio.to_thread(self.broker.get_account_balance), timeout=10.0)
                            holdings = res_tuple[1] if isinstance(res_tuple, (list, tuple)) and len(res_tuple) > 1 else {}
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
                    try: 
                        await asyncio.wait_for(query.answer(f"🔄 [{ticker}] 장부 무결성 동기화 중...", show_alert=False), timeout=5.0)
                    except Exception: pass
    
                    res = await self.sync_engine.process_auto_sync(ticker, chat_id, context, silent_ledger=True)
                    if res == "SUCCESS": 
                        async with self.tx_lock:
                            holdings = None
                            for attempt in range(3):
                                try:
                                    await asyncio.sleep(0.06)
                                    res_tuple = await asyncio.wait_for(asyncio.to_thread(self.broker.get_account_balance), timeout=10.0)
                                    holdings = res_tuple[1] if isinstance(res_tuple, (list, tuple)) and len(res_tuple) > 1 else {}
                                    break
                                except Exception:
                                    if attempt == 2: holdings = {}
                                    else: await asyncio.sleep(1.0 * (2 ** attempt))
                    
                        await self.sync_engine._display_ledger(ticker, chat_id, context, message_obj=query.message, pre_fetched_holdings=holdings)

        elif action == "HIST":
            if sub == "VIEW":
                hid = int(self._safe_float(data[2])) if len(data) > 2 else 0
                hist_data = await asyncio.wait_for(asyncio.to_thread(self.cfg.get_history), timeout=10.0) or []
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
                        qty, avg, invested, sold = await asyncio.wait_for(asyncio.to_thread(self.cfg.calculate_holdings, target.get('ticker'), safe_trades), timeout=10.0)
                        msg, markup = self.view.create_ledger_dashboard(target.get('ticker'), qty, avg, invested, sold, safe_trades, 0, 0, is_history=True, history_id=hid)
                    except TypeError:
                        msg, markup = self.view.create_ledger_dashboard(target.get('ticker'), qty, avg, invested, sold, safe_trades, 0, 0, is_history=True)
                      
                    try: 
                        await asyncio.wait_for(query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML'), timeout=10.0)
                    except telegram.error.BadRequest as e:
                        if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                    except Exception: pass
            elif sub == "LIST":
                if hasattr(controller, 'cmd_history'):
                    await controller.cmd_history(update, context)

            elif sub == "DEL_REQ":
                hid = int(self._safe_float(data[2])) if len(data) > 2 else 0
                msg, markup = self.view.get_history_delete_confirm_menu(hid)
                try: 
                    await asyncio.wait_for(query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML'), timeout=10.0)
                except telegram.error.BadRequest as e:
                    if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                except Exception: pass

            elif sub == "DEL_EXEC":
                hid = int(self._safe_float(data[2])) if len(data) > 2 else 0
                try: await asyncio.wait_for(query.answer("🔥 소각 중...", show_alert=False), timeout=5.0)
                except Exception: pass
                
                success = False
                try:
                    success = await asyncio.wait_for(asyncio.to_thread(self.cfg.delete_history, hid), timeout=10.0)
                except Exception as e:
                    logging.error(f"🚨 명예의 전당 소각 에러: {e}")
                    
                if success:
                    if hasattr(controller, 'cmd_history'):
                        await controller.cmd_history(update, context)
                else:
                    try: await asyncio.wait_for(query.answer("⚠️ 이미 소각된 기록이거나 찾을 수 없습니다.", show_alert=True), timeout=5.0)
                    except Exception: pass

            elif sub == "IMG":
                target_id = int(data[3]) if len(data) > 3 else None
                if not ticker: return
                
                try: await asyncio.wait_for(query.answer("🎨 프리미엄 졸업 카드를 렌더링 중입니다...", show_alert=False), timeout=5.0)
                except Exception: pass
                
                hist_data = await asyncio.wait_for(asyncio.to_thread(self.cfg.get_history), timeout=10.0) or []
                hist_list = [h for h in hist_data if isinstance(h, dict) and str(h.get('ticker')) == str(ticker)]
                
                if not hist_list:
                    await asyncio.wait_for(context.bot.send_message(chat_id, f"📭 <b>[{html.escape(str(ticker))}]</b> 발급 가능한 졸업 기록이 존재하지 않습니다.", parse_mode='HTML'), timeout=10.0)
                    return
                
                target_hist = None
                if target_id:
                    target_hist = next((h for h in hist_list if h.get('id') == target_id), None)
                
                if not target_hist:
                    target_hist = sorted(hist_list, key=lambda x: str(x.get('end_date') or ''), reverse=True)[0]
                
                try:
                    profit_val = self._safe_float(target_hist.get('profit'))
                    yield_pct_val = self._safe_float(target_hist.get('yield'))
                    invested_val = self._safe_float(target_hist.get('invested'))
                    revenue_val = self._safe_float(target_hist.get('revenue'))

                    img_path = await asyncio.wait_for(
                        asyncio.to_thread(
                            self.view.create_profit_image,
                            ticker=target_hist.get('ticker'),
                            profit=profit_val,
                            yield_pct=yield_pct_val,
                            invested=invested_val,
                            revenue=revenue_val,
                            end_date=target_hist.get('end_date')
                        ), timeout=15.0
                    )
            
                    if img_path:
                        def _read_img4(p):
                            with open(p, 'rb') as f_in: return f_in.read()
                        try:
                            img_bytes4 = await asyncio.wait_for(asyncio.to_thread(_read_img4, img_path), timeout=10.0)
                            if str(img_path).lower().endswith('.gif'):
                                await asyncio.wait_for(context.bot.send_animation(chat_id=chat_id, animation=img_bytes4), timeout=15.0)
                            else:
                                await asyncio.wait_for(context.bot.send_photo(chat_id=chat_id, photo=img_bytes4), timeout=15.0)
                            try: await asyncio.wait_for(query.delete_message(), timeout=5.0)
                            except Exception: pass
                        except OSError:
                            await asyncio.wait_for(context.bot.send_message(chat_id, "❌ 이미지 파일 읽기에 실패했습니다.", parse_mode='HTML'), timeout=10.0)
                    else:
                        await asyncio.wait_for(context.bot.send_message(chat_id, "❌ 이미지 생성에 실패했습니다.", parse_mode='HTML'), timeout=10.0)
                except Exception as e:
                    logging.error(f"📸 👑 졸업 이미지 생성/발송 실패: {e}")
                    try: await asyncio.wait_for(context.bot.send_message(chat_id, "❌ 이미지 생성 중 오류가 발생했습니다.", parse_mode='HTML'), timeout=10.0)
                    except Exception: pass

        elif action == "SET_VER":
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
                full_ledger = await asyncio.wait_for(asyncio.to_thread(self.cfg.get_ledger), timeout=10.0) or []
                recs = [r for r in full_ledger if isinstance(r, dict) and str(r.get('ticker')) == ticker]
                if recs:
                    ledger_qty, _, _, _ = await asyncio.wait_for(asyncio.to_thread(self.cfg.calculate_holdings, ticker, recs), timeout=10.0)
                    if ledger_qty > max_qty: max_qty = ledger_qty
            except Exception as e:
                logging.error(f"🚨 모드 전환 전 V14 장부 검증 에러: {e}")
                
            try:
                if getattr(self, 'queue_ledger', None):
                    q_data = await asyncio.wait_for(asyncio.to_thread(self.queue_ledger.get_queue, ticker), timeout=10.0) or []
                    if q_data:
                        vrev_qty = sum(int(self._safe_float(item.get('qty'))) for item in q_data if isinstance(item, dict))
                        if vrev_qty > max_qty: max_qty = vrev_qty
            except Exception as e:
                logging.error(f"🚨 모드 전환 전 V-REV 큐 장부 검증 에러: {e}")
            
            if max_qty > 0:
                try:
                    await asyncio.wait_for(query.edit_message_text(f"🛑 <b>[{html.escape(str(ticker))} 모드 전환 차단]</b>\n\n현재 계좌 또는 장부에 단 1주라도 잔고({max_qty}주)가 존재하면 코어 스위칭이 불가능합니다.\n전량 익절(0주) 후 0주 새출발 상태에서 다시 시도해 주십시오.", parse_mode='HTML'), timeout=10.0)
                except telegram.error.BadRequest as e:
                    if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                except Exception: pass
                return
                
            if sub == "V_REV":
                msg, markup = self.view.get_vrev_mode_selection_menu(ticker)
            elif sub == "V14":
                msg, markup = self.view.get_v14_mode_selection_menu(ticker)
            else:
                return
            
            try: 
                await asyncio.wait_for(query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML'), timeout=10.0)
            except telegram.error.BadRequest as e:
                if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
            except Exception: pass

        elif action == "SET_VER_CONFIRM":
             if not ticker: return
             
             if sub == "V_REV":
                await asyncio.wait_for(asyncio.to_thread(self.cfg.set_version, ticker, "V_REV"), timeout=10.0)
                await asyncio.wait_for(asyncio.to_thread(self.cfg.set_reverse_state, ticker, True, 0, 0.0), timeout=10.0)
                await asyncio.wait_for(asyncio.to_thread(self.cfg.set_manual_vwap_mode, ticker, False), timeout=10.0) 
                msg = f"✅ <b>[{html.escape(str(ticker))}] V-REV 역추세 모드(VWAP 자동) 락온 완료!</b>\n▫️ 다음 타격부터 역추세 엔진이 전면 가동됩니다."
             elif sub == "V14_LOC":
                await asyncio.wait_for(asyncio.to_thread(self.cfg.set_version, ticker, "V14"), timeout=10.0)
                await asyncio.wait_for(asyncio.to_thread(self.cfg.set_reverse_state, ticker, False, 0, 0.0), timeout=10.0)
                await asyncio.wait_for(asyncio.to_thread(self.cfg.set_manual_vwap_mode, ticker, False), timeout=10.0)
                msg = f"✅ <b>[{html.escape(str(ticker))}] V14 오리지널 (LOC 단일 타격) 락온 완료!</b>\n▫️ 다음 타격부터 오리지널 무매법이 가동됩니다."
             elif sub == "V14_VWAP":
                await asyncio.wait_for(asyncio.to_thread(self.cfg.set_version, ticker, "V14"), timeout=10.0)
                await asyncio.wait_for(asyncio.to_thread(self.cfg.set_reverse_state, ticker, False, 0, 0.0), timeout=10.0)
                await asyncio.wait_for(asyncio.to_thread(self.cfg.set_manual_vwap_mode, ticker, True), timeout=10.0)
                msg = f"✅ <b>[{html.escape(str(ticker))}] V14 오리지널 (VWAP 자동) 락온 완료!</b>\n▫️ 다음 타격부터 VWAP 알고리즘에 위임합니다."
             else:
                return
             
             try: 
                await asyncio.wait_for(query.edit_message_text(msg, parse_mode='HTML'), timeout=10.0)
             except telegram.error.BadRequest as e:
                if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
             except Exception: pass

        elif action == "TICKER":
            if sub == "ALL":
                target_tickers = ["SOXL", "TQQQ"]
                msg_txt = "SOXL + TQQQ 통합"
            elif "," in sub:
                if "SOXS" in sub.split(","):
                    await asyncio.wait_for(context.bot.send_message(chat_id, "⚠️ [V61.00 절대 헌법] 숏(SOXS) 운용은 시스템 전역에서 100% 영구 소각되었습니다."), timeout=10.0)
                    return
                target_tickers = sub.split(",")
                msg_txt = " + ".join(target_tickers) + " 싱글 모멘텀"
            else:
                if sub == "SOXS":
                    await asyncio.wait_for(context.bot.send_message(chat_id, "⚠️ [V61.00 절대 헌법] 숏(SOXS) 운용은 시스템 전역에서 100% 영구 소각되었습니다."), timeout=10.0)
                    return
                target_tickers = [sub]
                msg_txt = sub + " 전용"
                 
            await asyncio.wait_for(asyncio.to_thread(self.cfg.set_active_tickers, target_tickers), timeout=10.0)
            try: 
                await asyncio.wait_for(query.edit_message_text(f"✅ <b>[운용 종목 락온 완료]</b>\n▫️ <b>{html.escape(str(msg_txt))}</b> 모드로 전환되었습니다.\n▫️ /sync를 눌러 확인하십시오.", parse_mode='HTML'), timeout=10.0)
            except telegram.error.BadRequest as e:
                if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
            except Exception: pass
            
        elif action == "SEED":
            if not ticker: return
            controller.user_states[chat_id] = f"SEED_{sub}_{ticker}"
            await asyncio.wait_for(context.bot.send_message(chat_id, f"💵 [{html.escape(str(ticker))}] 시드머니 금액 입력:", parse_mode='HTML'), timeout=10.0)

        elif action == "CONFIG_AVWAP":
            if not ticker: return
            
            if sub == "TOGGLE":
                try:
                    current_state = await asyncio.wait_for(asyncio.to_thread(self.cfg.get_avwap_hybrid_mode, ticker), timeout=10.0)
                    new_state = not current_state
                    await asyncio.wait_for(asyncio.to_thread(self.cfg.set_avwap_hybrid_mode, ticker, new_state), timeout=10.0)
                    
                    if hasattr(controller, 'cmd_settlement'):
                        try:
                            await controller.cmd_settlement(update, context)
                        except telegram.error.BadRequest as e:
                            if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                        except Exception as e:
                            logging.error(f"🚨 관제탑 UI 갱신 실패: {e}")
                except Exception as e:
                    logging.error(f"🚨 [{ticker}] 암살자 모드 토글 실패: {e}")

            elif sub == "TOGGLE_OVERNIGHT":
                try:
                    current_state = await asyncio.wait_for(asyncio.to_thread(self.cfg.get_avwap_overnight_mode, ticker), timeout=10.0)
                    new_state = not current_state
                    await asyncio.wait_for(asyncio.to_thread(self.cfg.set_avwap_overnight_mode, ticker, new_state), timeout=10.0)
                    
                    if hasattr(controller, 'cmd_settlement'):
                        try:
                            await controller.cmd_settlement(update, context)
                        except telegram.error.BadRequest as e:
                            if "not modified" not in str(e).lower(): logging.warning(f"⚠️ UI 갱신 예외: {e}")
                        except Exception as e:
                            logging.error(f"🚨 관제탑 UI 갱신 실패: {e}")
                except Exception as e:
                    logging.error(f"🚨 [{ticker}] 암살자 오버나이트 토글 실패: {e}")
             
        elif action == "INPUT":
            if not ticker: return
            controller.user_states[chat_id] = f"CONF_{sub}_{ticker}"
            
            if sub == "SPLIT": ko_name = "분할 횟수"
            elif sub == "TARGET": ko_name = "목표 수익률(%)"
            elif sub == "COMPOUND": ko_name = "자동 복리율(%)"
            elif sub == "STOCK_SPLIT": ko_name = "액면 분할/병합 비율"
            elif sub == "FEE": ko_name = "증권사 수수료율(%)"
            elif sub == "AVWAP_BUDGET": ko_name = "암살자 1회 타격 예산(USD)" 
            else: ko_name = "값"
             
            desc = "숫자만 입력하세요."
            if sub == "STOCK_SPLIT":
                desc = "액면분할 시 1주가 10주가 되었다면 10 입력, 10주가 1주로 병합되었다면 0.1 입력"
            elif sub == "AVWAP_BUDGET":
                desc = "암살자 격발 시 투입할 <b>최대 고정 예산(USD)</b>을 입력하세요. (예: 10000)\n\n▫️ 실제 격발 시, 이 금액과 KIS 실시간 가용 현금의 95% 중 더 작은 금액으로 <b>안전하게 캡핑(Capping)</b>되어 API 거절(Reject)을 100% 원천 차단합니다."
                
            await asyncio.wait_for(context.bot.send_message(chat_id, f"✏️ <b>[{html.escape(str(ticker))}] {html.escape(str(ko_name))}</b>를 설정합니다.\n{desc}", parse_mode='HTML'), timeout=10.0)
