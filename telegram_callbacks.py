# ==========================================================
# FILE: telegram_callbacks.py
# ==========================================================
# 🚨 [AI 에이전트(Copilot/Claude) 절대 주의 - 환각(Hallucination) 방어막]
# 제1헌법: queue_ledger.get_queue 등 모든 파일 I/O 및 락 점유 메서드는 무조건 asyncio.to_thread로 래핑하여 이벤트 루프 교착(Deadlock)을 원천 차단함.
# MODIFIED: [V44.47 이벤트 루프 데드락 영구 소각] 다이렉트 파일 I/O 및 config/ledger 접근 메서 전면 비동기 래핑 완료.
# MODIFIED: [V44.48 수동 조작 데드코드 영구 소각 및 런타임 무결성 확보] 큐 장부에 존재하지 않는 _load 메서 호출 찌꺼기 100% 소각.
# 🚨 MODIFIED: [V54.04 런타임 붕괴(Split-Brain) 근본 원인 팩트 수술]
# 삼위일체 소각(/reset) 시 V_REV 모드라면 is_active를 False로 끄지 않고 True로 보존하여 '0주 새출발' 상태를 100% 팩트 락온.
# 모드 스위칭(SET_VER_CONFIRM) 시에도 version과 is_active 플래그가 완벽히 동기화되도록 디커플링 배선 정밀 교정 완료.
# 🚨 MODIFIED: [V55.00 오퍼레이션 SSOT - 텔레그램 다이렉트 I/O 병목 및 동시성 오염 원천 차단]
# DEL_Q(삭제), CLEAR_Q(초기화), RESET(삼위일체 소각) 격발 시 존재하던 
# 지저분한 다이렉트 파일 I/O(open, json, tempfile) 찌꺼기를 100% 영구 소각하고,
# QueueLedger의 스레드 세이프(Thread-safe) 코어 메서드(delete_lot, clear_queue)로 직결(Lock-on) 완료.
# 🚨 MODIFIED: [V56.00 차세대 AVWAP 실전 암살자 전면 재가동 락온]
# - Phantom Radar 암살자 제어 메뉴 영구 봉인 락다운 전면 해체.
# - MODE 라우터 내 AVWAP_WARN, AVWAP_ON, AVWAP_OFF 팩트 제어 로직 100% 복구 완료.
# 🚨 MODIFIED: [V59.02 잔재 데드코드 영구 소각] 
# 15:25 전량 덤핑 헌법에 따라 의미를 상실한 AVWAP_SET 라우터 내 TARGET_MANUAL, TARGET_AUTO, EARLY, MULTI 
# 제어 콜백 파이프라인(데드코드)을 전면 철거하고 REFRESH 기능만 보존 완료.
# 🚨 MODIFIED: [V59.03 관제탑 진입 배선 복구] 
# settlement 메뉴에서 '관제탑' 버튼 클릭 시 cmd_avwap을 정상 호출하도록 AVWAP:MENU 라우팅 배선 복구 완료.
# NEW: [V59.06] VWAP 런타임 엑스레이(Dry-Run) 진단 엔진 라우터 이식 완료 (순수 Read-Only 섀도우 연산)
# 🚨 MODIFIED: [V60.00 옴니 매트릭스 락다운 데드코드 전면 폐기]
# XRAY 진단 엔진 내부에서 매수 방아쇠를 강제로 잠그던 옴니 매트릭스 스캔 블록 및 시각적 브리핑 요소를 영구 소각함.
# 🚨 MODIFIED: [V61.00 숏(SOXS) 전면 소각 작전 지시서 적용]
# 1) SET_VER 및 SET_VER_CONFIRM 콜백 내 SOXS 락다운 방어막 텍스트를 시스템 영구 폐기 경고로 오버라이드 완료.
# 2) TICKER 액션 내 SOXS 경고문 교정 및 '듀얼 모멘텀' 텍스트를 '싱글 모멘텀'으로 팩트 교정 완료.
# NEW: [AVWAP 수동 개입 엣지 케이스 방어] 수동 매도 후 유령 물량을 0주로 강제 동기화하는 SYNC_ZERO 라우터 신설
# 🚨 MODIFIED: [V61.06 런타임 붕괴 방어] MODE 및 INPUT 라우터 내 IndentationError(들여쓰기) 팩트 완벽 교정
# 🚨 MODIFIED: [V66.07 오퍼레이션 SSOT - 엑스레이 환각 소각 및 VWAP 최초 명중 타전망 이식]
# 엑스레이 진단 시 무조건 낡은 인메모리 상태를 강제 폐기(None)하고 최신 JSON 팩트 파일을 로드하도록 배선 교정 완료.
# 🚨 NEW: [KIS VWAP 알고리즘 대통합 수술] 수동 VWAP 설정(AUTO/MANUAL) 텔레그램 콜백 라우팅을 전면 소각하고 단일 KIS VWAP 예약 장전 모드로 팩트 락온 완료.
# 🚨 MODIFIED: [런타임 즉사 방어] SYNC_ZERO 콜백 라우터 내 IndentationError 팩트 무결점 4배수 교정 완료.
# 🚨 MODIFIED: [V71.02 XRAY 엔진 라우팅 영구 소각]
# KIS 자체 VWAP 알고리즘 위임에 따라 1분 단위 시뮬레이션의 의미가 상실된 런타임 엑스레이(Dry-Run) 진단 콜백 라우터를 전면 적출 완료.
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

        # MODIFIED: [V71.02 XRAY 엔진 렌더링 영구 소각] 엑스레이(XRAY) 콜백 라우터 및 하위 연산 블록 전면 철거 완료
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
            
                    if os.path.exists(img_path):
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

            if ver == "V_REV":
                await query.answer("🛑 [예방 덫 전면 소각] V-REV 모드는 자전거래 의심을 회피하고 AVWAP 암살자 가동을 위해 예방 덫 수동 장전 기능을 영구 소각했습니다.", show_alert=True)
                return

            await query.answer()
            
            await query.edit_message_text(f"🚀 {t} 수동 강제 전송 시작 (교차 분리)...")
            
            async with self.tx_lock:
                cash, holdings = await asyncio.to_thread(self.broker.get_account_balance)
                
            if holdings is None:
                return await query.edit_message_text("❌ API 통신 오류로 주문을 실행할 수জীবী 잔고를 확인할 수 없어 전환을 차단합니다. 잠시 후 다시 시도해 주세요.")
                
            active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
            _, allocated_cash = await asyncio.to_thread(controller._calculate_budget_allocation, cash, active_tickers)
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
            
            logic_qty_v14 = safe_qty
            is_manual_vwap = await asyncio.to_thread(getattr(self.cfg, 'get_manual_vwap_mode', lambda x: False), t)
            if is_manual_vwap:
                cached_snap_v14 = None
                if hasattr(self.strategy, 'v14_vwap_plugin'):
                    cached_snap_v14 = await asyncio.to_thread(self.strategy.v14_vwap_plugin.load_daily_snapshot, t)
                if cached_snap_v14 and "total_q" in cached_snap_v14:
                    logic_qty_v14 = cached_snap_v14["total_q"]

            plan = await asyncio.to_thread(self.strategy.get_plan, t, curr_p, safe_avg, logic_qty_v14, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash[t], is_simulation=True)
            
            if safe_qty == 0:
                for o in plan.get('core_orders', []):
                    if o['side'] == 'BUY' and 'Buy1' in o.get('desc', ''):
                        o['price'] = round(prev_c * 1.15, 2)

            title = f"💎 <b>[{t}] 무매4 정규장 주문 수동 실행</b>\n"
            msg = title
            
            all_success = True
            
            for o in plan.get('core_orders', []):
                res = await asyncio.to_thread(self.broker.send_order, t, o['side'], o['qty'], o['price'], o['type'])
                is_success = res.get('rt_cd') == '0'
                if not is_success:
                    all_success = False
                    
                err_msg = res.get('msg1', '오류')
                status_icon = '✅' if is_success else f'❌({err_msg})'
                msg += f"└ 1차 필수: {o['desc']} {o['qty']}주: {status_icon}\n"
                await asyncio.sleep(0.2) 
                
            for o in plan.get('bonus_orders', []):
                res = await asyncio.to_thread(self.broker.send_order, t, o['side'], o['qty'], o['price'], o['type'])
                is_success = res.get('rt_cd') == '0'
                err_msg = res.get('msg1', '잔금패스')
                status_icon = '✅' if is_success else f'❌({err_msg})'
                msg += f"└ 2차 보너스: {o['desc']} {o['qty']}주: {status_icon}\n"
                await asyncio.sleep(0.2) 
            
            if all_success and len(plan.get('core_orders', [])) > 0:
                await asyncio.to_thread(self.cfg.set_lock, t, "REG")
                msg += "\n🔒 <b>필수 주문 전송 완료 (잠금 설정됨)</b>"
            else:
                msg += "\n⚠️ <b>일부 필수 주문 실패 (매매 잠금 보류)</b>"

            await context.bot.send_message(chat_id, msg, parse_mode='HTML')

        elif action == "AVWAP":
            await query.answer()
            if sub == "MENU":
                await controller.cmd_avwap(update, context)

        elif action == "SET_VER":
            await query.answer()
            new_ver = sub
            ticker = data[2]
            current_ver = await asyncio.to_thread(self.cfg.get_version, ticker)
            
            if ticker == "TQQQ" and new_ver == "V_REV":
                await context.bot.send_message(chat_id, "⚠️ [절대 헌법 위반] TQQQ는 V14 무매4 전용 아키텍처입니다. 전환이 차단되었습니다.")
                return
            if ticker == "SOXS":
                await context.bot.send_message(chat_id, "⚠️ [V61.00 절대 헌법] 숏(SOXS) 운용은 시스템 전역에서 100% 영구 소각되었습니다.")
                return

            async with self.tx_lock:
                _, holdings = await asyncio.to_thread(self.broker.get_account_balance)
                  
            if holdings is None:
                await context.bot.send_message(chat_id, "🚨 API 통신 지연으로 잔고를 확인할 수 없어 전환을 차단합니다. 잠시 후 다시 시도해 주세요.")
                return
                
            kis_qty = int(float(holdings.get(ticker, {}).get('qty', 0)))
            max_qty = await self._get_max_holdings_qty(ticker, kis_qty)
             
            if kis_qty == 0 and max_qty > 0 and current_ver != new_ver:
                msg = f"🚨 <b>[ 퀀트 모드 전환 강제 차단: 수동 매도 감지 ]</b>\n\n"
                msg += f"실잔고는 0주이나 장부에 잔여 수량({max_qty}주)이 남아있어 모드 전환이 차단되었습니다.\n"
                msg += "증권사 앱에서 수동으로 전량 매도하셨다면, 채팅창에 <code>/reset</code>을 입력하여 장부를 초기화한 후 다시 시도해주세요."
                await query.edit_message_text(msg, parse_mode='HTML')
                return
            
            if max_qty > 0 and current_ver != new_ver:
                msg = f"🚨 <b>[ 퀀트 모드 전환 강제 차단 ]</b>\n\n"
                msg += f"현재 <b>[{ticker}] {max_qty}주</b>를 보유 중입니다. (삼중 교차 검증)\n"
                msg += "V14 ↔ V-REV 간의 엔진 스위칭은 장부 평단가 오염을 막기 위해 <b>'0주(100% 현금)'</b> 상태에서만 절대적으로 허용됩니다.\n\n"
                msg += "진행 중인 매매 사이클을 전량 익절(0주)로 마무리하신 후 다시 시도해 주십시오."
                await query.edit_message_text(msg, parse_mode='HTML')
                return
             
            if new_ver == "V_REV":
                if not (os.path.exists("strategy_reversion.py") and os.path.exists("queue_ledger.py")):
                    await context.bot.send_message(chat_id, "🚨 [개봉박두] V-REV 엔진 모듈 파일이 존재하지 않아 전환할 수 없습니다! (업데이트 필요)")
                    return
                msg, markup = self.view.get_vrev_mode_selection_menu(ticker)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                return
            
            elif new_ver == "V14":
                msg, markup = self.view.get_v14_mode_selection_menu(ticker)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                return
                
            await asyncio.to_thread(self.cfg.set_version, ticker, new_ver)
            await asyncio.to_thread(self.cfg.set_upward_sniper_mode, ticker, False)
            if hasattr(self.cfg, 'set_avwap_hybrid_mode'):
                await asyncio.to_thread(self.cfg.set_avwap_hybrid_mode, ticker, False)
            if hasattr(self.cfg, 'set_manual_vwap_mode'):
                 await asyncio.to_thread(self.cfg.set_manual_vwap_mode, ticker, False)
            
            await query.edit_message_text(f"✅ <b>[{ticker}]</b> 퀀트 엔진이 <b>V14 무매4</b> 모드로 전환되었습니다.\n▫️ /sync 명령어에서 변경된 지시서를 확인하세요.", parse_mode='HTML')

        elif action == "SET_VER_CONFIRM":
            await query.answer()
            mode_type = sub 
            ticker = data[2]
            current_ver = await asyncio.to_thread(self.cfg.get_version, ticker)
            
            target_ver = "V_REV" if mode_type == "V_REV" else "V14"

            if ticker == "TQQQ" and target_ver == "V_REV":
                await context.bot.send_message(chat_id, "⚠️ [절대 헌법 위반] TQQQ는 V14 무매4 전용 아키텍처입니다. 전환이 차단되었습니다.")
                return
            if ticker == "SOXS":
                await context.bot.send_message(chat_id, "⚠️ [V61.00 절대 헌법] 숏(SOXS) 운용은 시스템 전역에서 100% 영구 소각되었습니다.")
                return

            async with self.tx_lock:
                _, holdings = await asyncio.to_thread(self.broker.get_account_balance)
                
            if holdings is None:
                await context.bot.send_message(chat_id, "🚨 API 통신 지연으로 잔고를 확인할 수 없어 전환을 차단합니다. 잠시 후 다시 시도해 주세요.")
                return
                
            kis_qty = int(float(holdings.get(ticker, {}).get('qty', 0)))
            max_qty = await self._get_max_holdings_qty(ticker, kis_qty)
            
            if kis_qty == 0 and max_qty > 0 and current_ver != target_ver:
                msg = f"🚨 <b>[ 퀀트 모드 전환 강제 차단: 수동 매도 감지 ]</b>\n\n"
                msg += f"실잔고는 0주이나 장부에 잔여 수량({max_qty}주)이 남아있어 모드 전환이 차단되었습니다.\n"
                msg += "증권사 앱에서 수동으로 전량 매도하셨다면, 채팅창에 <code>/reset</code>을 입력하여 장부를 초기화한 후 다시 시도해주세요."
                await query.edit_message_text(msg, parse_mode='HTML')
                return
            
            if max_qty > 0 and current_ver != target_ver:
                msg = f"🚨 <b>[ 퀀트 모드 전환 강제 차단 ]</b>\n\n"
                msg += f"현재 <b>[{ticker}] {max_qty}주</b>를 보유 중입니다. (삼중 교차 검증)\n"
                msg += "V14 ↔ V-REV 간의 엔진 스위칭은 장부 평단가 오염을 막기 위해 <b>'0주(100% 현금)'</b> 상태에서만 절대적으로 허용됩니다.\n\n"
                msg += "진행 중인 매매 사이클을 전량 익절(0주)로 마무리하신 후 다시 시도해 주십시오."
                await query.edit_message_text(msg, parse_mode='HTML')
                return
            
            if mode_type == "V_REV":
                await asyncio.to_thread(self.cfg.set_version, ticker, "V_REV")
                await asyncio.to_thread(self.cfg.set_reverse_state, ticker, True, 0)
                await asyncio.to_thread(self.cfg.set_upward_sniper_mode, ticker, False)
                if hasattr(self.cfg, 'set_avwap_hybrid_mode'):
                    await asyncio.to_thread(self.cfg.set_avwap_hybrid_mode, ticker, False)
                
                # 🚨 NEW: [KIS VWAP 알고리즘 대통합 수술] 수동/자동 분기 전면 소각 및 자동 예약 락온
                await asyncio.to_thread(self.cfg.set_manual_vwap_mode, ticker, True)
                mode_txt = "🕒 KIS VWAP 알고리즘 예약 주문 자동 장전"
                    
                await query.edit_message_text(f"✅ <b>[{ticker}]</b> 퀀트 엔진이 <b>V_REV 역추세 하이브리드</b>로 전환되었습니다.\n▫️ <b>운용 방식:</b> {mode_txt}\n▫️ /sync 지시서를 확인해 주십시오.", parse_mode='HTML')
            
            elif mode_type in ["V14_LOC", "V14_VWAP"]:
                await asyncio.to_thread(self.cfg.set_version, ticker, "V14")
                await asyncio.to_thread(self.cfg.set_reverse_state, ticker, False, 0)
                await asyncio.to_thread(self.cfg.set_upward_sniper_mode, ticker, False)
                if hasattr(self.cfg, 'set_avwap_hybrid_mode'):
                    await asyncio.to_thread(self.cfg.set_avwap_hybrid_mode, ticker, False)
                    
                if mode_type == "V14_VWAP":
                    # 🚨 NEW: [KIS VWAP 알고리즘 대통합 수술]
                    await asyncio.to_thread(self.cfg.set_manual_vwap_mode, ticker, True)
                    mode_txt = "🕒 KIS VWAP 알고리즘 예약 주문 자동 장전"
                else:
                    await asyncio.to_thread(self.cfg.set_manual_vwap_mode, ticker, False)
                    mode_txt = "📉 LOC 단일 타격 (초안정성)"
                
                await query.edit_message_text(f"✅ <b>[{ticker}]</b> 퀀트 엔진이 <b>V14 무매4</b> 모드로 전환되었습니다.\n▫️ <b>집행 방식:</b> {mode_txt}\n▫️ /sync 명령어에서 변경된 지시서를 확인하세요.", parse_mode='HTML')

        elif action == "AVWAP_SET":
            action_type = sub
            ticker = data[2]
            
            if 'app_data' not in context.bot_data:
                context.bot_data['app_data'] = {}
            render_app_data = context.bot_data['app_data']
            
            if context.job_queue:
                 for job in context.job_queue.jobs():
                     if job.data is not None:
                        render_app_data = job.data
            
            tracking_cache = render_app_data.setdefault('sniper_tracking', {})
            
            if action_type == "REFRESH":
                try:
                    from telegram_avwap_console import AvwapConsolePlugin
                    plugin = AvwapConsolePlugin(self.cfg, self.broker, self.strategy, self.tx_lock)
                    msg, markup = await plugin.get_console_message(render_app_data)
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                    await query.answer("🔄 관제탑 스크린을 최신 팩트로 갱신했습니다.", show_alert=False)
                except Exception as e:
                    if "Message is not modified" in str(e):
                          await query.answer("✅ 시장 변화가 없어 최신 상태가 유지 중입니다.", show_alert=False)
                    else:
                        await query.answer(f"갱신 에러: {e}", show_alert=True)
            
            elif action_type == "SYNC_ZERO":
                try:
                    est = ZoneInfo('America/New_York')
                    now_est = datetime.datetime.now(est)
                    
                    tracking_cache[f"AVWAP_QTY_{ticker}"] = 0
                    tracking_cache[f"AVWAP_BOUGHT_{ticker}"] = False
                    tracking_cache[f"AVWAP_SHUTDOWN_{ticker}"] = True
                    tracking_cache[f"AVWAP_AVG_{ticker}"] = 0.0

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
                        
                        # 🚨 MODIFIED: [런타임 즉사 방어] IndentationError 팩트 무결점 교정 완료
                        await asyncio.to_thread(self.strategy.v_avwap_plugin.save_state, ticker, now_est, state_data)
                    
                    from telegram_avwap_console import AvwapConsolePlugin
                    plugin = AvwapConsolePlugin(self.cfg, self.broker, self.strategy, self.tx_lock)
                    msg, markup = await plugin.get_console_message(render_app_data)
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
        
                    await query.answer(f"🧯 [{ticker}] 암살자 수동 청산 동기화 완료! 당일 영구 동결(Shutdown) 상태로 전환됩니다.", show_alert=True)
                except Exception as e:
                    logging.error(f"🚨 암살자 수동 청산 동기화 에러: {e}")
                    await query.answer(f"동기화 에러: {e}", show_alert=True)

        elif action == "MODE":
            mode_val = sub
            ticker = data[2] if len(data) > 2 else "SOXL"
            
            if mode_val == "AVWAP_WARN":
                msg, markup = self.view.get_avwap_warning_menu(ticker)
                await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                return
            elif mode_val in ["AVWAP_ON", "AVWAP_OFF"]:
                is_on = (mode_val == "AVWAP_ON")
                await asyncio.to_thread(self.cfg.set_avwap_hybrid_mode, ticker, is_on)
                await query.answer(f"✅ AVWAP 암살자 {'가동' if is_on else '해제'} 완료", show_alert=False)
                
                active_tickers = await asyncio.to_thread(self.cfg.get_active_tickers)
                atr_data = {t: (0.0, 0.0) for t in active_tickers}
                app_data = context.bot_data.get('app_data', {})
                tracking_cache = app_data.get('sniper_tracking', {})
                
                msg, markup = self.view.get_settlement_message(active_tickers, self.cfg, atr_data, tracking_cache)
                try:
                    await query.edit_message_text(msg, reply_markup=markup, parse_mode='HTML')
                except Exception:
                    pass
                return

            await query.answer()
            current_ver = await asyncio.to_thread(self.cfg.get_version, ticker)
            if current_ver == "V_REV" and mode_val == "ON":
                await context.bot.send_message(chat_id, f"🚨 {current_ver} 모드에서는 로직 충돌 방지를 위해 상방 스나이퍼를 켤 수 없습니다!")
                return

            await asyncio.to_thread(self.cfg.set_upward_sniper_mode, ticker, mode_val == "ON")
            await query.edit_message_text(f"✅ <b>[{ticker}]</b> 상방 스나이퍼 모드 변경 완료: {'🎯 ON (가동중)' if mode_val == 'ON' else '⚪ OFF (대기중)'}", parse_mode='HTML')
            
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
