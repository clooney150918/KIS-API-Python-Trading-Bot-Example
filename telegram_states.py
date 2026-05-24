# ==========================================================
# FILE: telegram_states.py
# ==========================================================
# 🚨 MODIFIED: [보안 무결성 팩트 교정] 관리자 검증 코루틴 호출 시 await 누락으로 인한 보안망 우회 맹점 완벽 수술
# 🚨 MODIFIED: [V44.30 수동 입력 렌더링 수술] 텔레그램 상태 제어 후 제자리 렌더링 및 응답 무결성 확보
# 🚨 MODIFIED: [V44.44 이벤트 루프 교착 방어] 큐 장부 지층 수동 수정(EDIT_Q) 시 발생하는 직접적인 파일 I/O 작업을 비동기 래핑
# 🚨 MODIFIED: [Case 26 절대 헌법 준수] 텔레그램 HTML 파서 붕괴 방어를 위한 html.escape 쉴드 전역 강제 주입
# 🚨 MODIFIED: [Case 32 & 33 절대 규칙] 팻핑거 스캔 시 TPS 캡핑(0.06s) 및 3단 지수 백오프, 타임아웃(10s) 샌드위치 락온
# 🚨 MODIFIED: [NoneType 붕괴 원천 봉쇄] update.message 다이렉트 참조 소각 및 update.effective_message 단락 평가 락온
# ==========================================================

import logging
import datetime
from zoneinfo import ZoneInfo
import os
import json
import asyncio
import tempfile
import html
from telegram import Update
from telegram.ext import ContextTypes

class TelegramStates:
    def __init__(self, config, broker, queue_ledger, sync_engine):
        self.cfg = config
        self.broker = broker
        self.queue_ledger = queue_ledger
        self.sync_engine = sync_engine

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, controller):
        # 🚨 MODIFIED: [보안 팩트 교정] await 키워드 강제 락온으로 코루틴 경고 소각 및 관리자 인증망 수복
        if not await controller._is_admin(update):
            return
            
        chat_id = update.effective_chat.id
        # 🚨 MODIFIED: 미디어(사진 등) 수신 시 text 속성이 None이 되어 발생하는 TypeError 단락 평가 방어
        text = update.effective_message.text.strip() if update.effective_message and update.effective_message.text else ""
        
        if "통합 지시서" in text or "지시서 조회" in text:
            return await controller.cmd_sync(update, context)
        elif "장부 동기화" in text or "장부 조회" in text:
            return await controller.cmd_record(update, context)
        elif "명예의 전당" in text:
            return await controller.cmd_history(update, context)
        elif "코어 스위칭" in text or "전술 설정" in text or "모드변환" in text or "분할변경" in text:
            return await controller.cmd_settlement(update, context)
        elif "시드머니" in text or "시드 변경" in text or "시드 관리" in text:
            return await controller.cmd_seed(update, context)
        elif "종목 선택" in text:
            return await controller.cmd_ticker(update, context)
        elif "스나이퍼" in text:
            return await controller.cmd_mode(update, context)
        elif "버전" in text or "업데이트 내역" in text:
            return await controller.cmd_version(update, context)
        elif "비상 해제" in text:
            return await controller.cmd_reset(update, context)
        elif "시스템 업데이트" in text or "엔진 업데이트" in text:
            return await controller.cmd_update(update, context)

        state = controller.user_states.get(chat_id)
        
        if not state:
            return

        try:
            if state.startswith("EDITQ_"):
                parts = state.split("_", 2)
                ticker = parts[1]
                target_date = parts[2]
                safe_ticker = html.escape(str(ticker))
                
                input_parts = text.split()
                if len(input_parts) != 2:
                    del controller.user_states[chat_id]
                    return await update.effective_message.reply_text("❌ 입력 형식 오류입니다. 띄어쓰기로 수량과 평단가를 입력해주세요. (수정 취소됨)")
                
                try:
                    qty = int(input_parts[0])
                    price = float(input_parts[1])
                except ValueError:
                    del controller.user_states[chat_id]
                    return await update.effective_message.reply_text("❌ 수량/평단가는 숫자로 입력하세요. (수정 취소됨)")
                
                try:
                    curr_p = 0.0
                    # 🚨 MODIFIED: [Case 32, 33, 14] 팻핑거 스캔 시 TPS 캡핑, 3단 백오프, 타임아웃 10초 샌드위치 락온
                    for attempt in range(3):
                        try:
                            await asyncio.sleep(0.06)
                            curr_p_val = await asyncio.wait_for(
                                asyncio.to_thread(self.broker.get_current_price, ticker), 
                                timeout=10.0
                            )
                            curr_p = float(str(curr_p_val or 0.0).replace(',', ''))
                            break
                        except Exception:
                            if attempt == 2: curr_p = 0.0
                            else: await asyncio.sleep(1.0 * (2 ** attempt))
                            
                    if curr_p and curr_p > 0 and (price < curr_p * 0.7 or price > curr_p * 1.3):
                        del controller.user_states[chat_id]
                        return await update.effective_message.reply_text(f"🚨 <b>팻핑거 방어 가동:</b> 입력가(${price:.2f})가 현재가(${curr_p:.2f}) 대비 ±30%를 초과합니다. 다시 시도해주세요.", parse_mode='HTML')
                except Exception:
                    pass

                # 🚨 MODIFIED: [제1헌법] 큐 장부 파일 I/O 작업 비동기 래핑
                if getattr(self, 'queue_ledger', None):
                    await asyncio.to_thread(self.queue_ledger.edit_lot, ticker, target_date, qty, price)
                
                del controller.user_states[chat_id]
                short_date = html.escape(str(target_date[:10]))
                await update.effective_message.reply_text(f"✅ <b>[{safe_ticker}] 지층 정밀 수정 완료! KIS 원장과 동기화합니다.</b>\n▫️ {short_date} | {qty}주 | ${price:.2f}", parse_mode='HTML')
                
                if ticker not in self.sync_engine.sync_locks:
                    self.sync_engine.sync_locks[ticker] = asyncio.Lock()
                if not self.sync_engine.sync_locks[ticker].locked():
                    await self.sync_engine.process_auto_sync(ticker, chat_id, context, silent_ledger=False)
                
                return

            # 🚨 MODIFIED: [Insight 14] String-Float 콤마 맹독성 쉴드 래핑
            val = float(str(text).replace(',', ''))
            parts = state.split("_")
            
            if state.startswith("SEED"):
                if val < 0:
                    return await update.effective_message.reply_text("❌ 오류: 시드머니는 0 이상이어야 합니다.")
                    
                action, ticker = parts[1], parts[2]
                safe_ticker = html.escape(str(ticker))
                
                # 🚨 MODIFIED: [맹점 4] cfg.get_seed 동기 I/O 블로킹 비동기 래핑
                curr = float(str(await asyncio.to_thread(self.cfg.get_seed, ticker) or 0.0).replace(',', ''))

                new_v = curr + val if action == "ADD" else (max(0.0, curr - val) if action == "SUB" else val)
                await asyncio.to_thread(self.cfg.set_seed, ticker, new_v)
                await update.effective_message.reply_text(f"✅ [{safe_ticker}] 시드 변경: ${new_v:,.0f}")
                
            elif state.startswith("CONF_SPLIT"):
                if val < 1:
                    return await update.effective_message.reply_text("❌ 오류: 분할 횟수는 1 이상이어야 합니다.")
                    
                ticker = parts[2]
                safe_ticker = html.escape(str(ticker))
                
                def _set_split():
                    d = self.cfg._load_json(self.cfg.FILES["SPLIT"], self.cfg.DEFAULT_SPLIT)
                    d[ticker] = val
                    self.cfg._save_json(self.cfg.FILES["SPLIT"], d)
                
                await asyncio.to_thread(_set_split)
                await update.effective_message.reply_text(f"✅ [{safe_ticker}] 분할: {int(val)}회")
                
            elif state.startswith("CONF_TARGET"):
                ticker = parts[2]
                safe_ticker = html.escape(str(ticker))
                
                def _set_target():
                    d = self.cfg._load_json(self.cfg.FILES["PROFIT_CFG"], self.cfg.DEFAULT_TARGET)
                    d[ticker] = val
                    self.cfg._save_json(self.cfg.FILES["PROFIT_CFG"], d)
                
                await asyncio.to_thread(_set_target)
                await update.effective_message.reply_text(f"✅ [{safe_ticker}] 목표 수익률: {val}%")

            elif state.startswith("CONF_COMPOUND"):
                if val < 0:
                    return await update.effective_message.reply_text("❌ 오류: 복리율은 0 이상이어야 합니다.")
                    
                ticker = parts[2]
                safe_ticker = html.escape(str(ticker))
                await asyncio.to_thread(self.cfg.set_compound_rate, ticker, val)
                await update.effective_message.reply_text(f"✅ [{safe_ticker}] 졸업 시 자동 복리율: {val}%")

            elif state.startswith("CONF_FEE"):
                if val < 0.0 or val > 10.0:
                    return await update.effective_message.reply_text("🚨 <b>오입력 차단:</b> 수수료율은 0.0% ~ 10.0% 사이여야 합니다.", parse_mode='HTML')
                    
                ticker = parts[2]
                safe_ticker = html.escape(str(ticker))
                await asyncio.to_thread(self.cfg.set_fee, ticker, val)
                await update.effective_message.reply_text(f"💳 <b>[{safe_ticker}] 증권사 거래 수수료: {val}% 적용 완료!</b>\n▫️ 다음 명예의 전당 정산부터 수익 연산 시 해당 수수료가 적용됩니다.", parse_mode='HTML')
                
            elif state.startswith("CONF_STOCK_SPLIT"):
                if val <= 0:
                    return await update.effective_message.reply_text("❌ 오류: 액면 보정 비율은 0보다 커야 합니다.")
                    
                ticker = parts[2]
                safe_ticker = html.escape(str(ticker))
                await asyncio.to_thread(self.cfg.apply_stock_split, ticker, val)
                
                # 🚨 MODIFIED: [Case 03] ZoneInfo('America/New_York') EST 단일 소스 락온
                est = ZoneInfo('America/New_York')
                today_str = datetime.datetime.now(est).strftime('%Y-%m-%d')
                await asyncio.to_thread(self.cfg.set_last_split_date, ticker, today_str)
                
                await update.effective_message.reply_text(f"✅ [{safe_ticker}] 수동 액면 보정 완료\n▫️ 모든 장부 기록이 {val}배 비율로 정밀하게 소급 조정되었습니다.")

            elif state.startswith("VREV_GAP"):
                ticker = parts[2]
                safe_ticker = html.escape(str(ticker))
                if val > 0: val = -val
                
                if hasattr(self.cfg, 'set_vrev_gap_threshold'):
                    await asyncio.to_thread(self.cfg.set_vrev_gap_threshold, ticker, val)
                    
                await update.effective_message.reply_text(f"📉 <b>[{safe_ticker}] V-REV 장막판 갭 스위칭 임계치 설정 완료!</b>\n▫️ 팩트 타격선: 기초자산 VWAP 대비 <b>{val}%</b>\n▫️ 다음 타임 슬라이싱 스케줄부터 즉시 적용됩니다.", parse_mode='HTML')
                
        except ValueError:
            await update.effective_message.reply_text("❌ 오류: 유효한 숫자를 입력하세요. (입력 대기 상태가 강제 해제되었습니다.)")
        except Exception as e:
            safe_err = html.escape(str(e))
            await update.effective_message.reply_text(f"❌ 알 수 없는 오류 발생: {safe_err}")
        finally:
            if chat_id in controller.user_states:
                del controller.user_states[chat_id]
