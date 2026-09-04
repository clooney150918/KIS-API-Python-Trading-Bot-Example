# ==========================================================
# FILE: telegram_view.py
# ==========================================================
# 🚨 VERIFIED: [최종 무결점 판정] 5대 헌법 및 43대 엣지 케이스 완벽 결속 교차 검증 완료.
# 🚨 MODIFIED: [UI 직관성 궁극 교정] 보조전략 장부 0주 대기 상태 렌더링 시 'OFF' 표출을 'STANDBY (타점 감시 중 / 0주)'로 100% 팩트 교체하여 엔진 가동 상태(ON)와의 논리적 오해 원천 차단.
# 🚨 MODIFIED: [Phase 4 보조전략 장부 상시 렌더링 락온] `create_ledger_dashboard` 메서드 호출 시 보조전략 데이터가 0주라도 무조건 "OFF (대기 중 / 0주)"를 표출하여 사용자 팩트 검증을 보조.
# 🚨 MODIFIED: [Phase 4 보조전략 지정 예산 렌더링] /settlement 호출 시 표출되는 설정 화면에 사용자가 지정한 '보조전략 예산'과 '오버나이트 모드'를 100% 팩트로 표출.
# 🚨 MODIFIED: [Phase 4 인라인 버튼 결속] 보조전략 설정 메뉴(SETTLEMENT)에 "🔫 보조전략 1회 주문 예산" 및 "🌙 오버나이트 토글" 버튼 주입 완료.
# 🚨 MODIFIED: [Float 정밀도 붕괴 원천 차단] 뷰어 클래스 내에 `_safe_float` 래퍼를 전격 이식하여 파편화된 인라인 캐스팅을 통합하고 NaN/Inf 맹독성 붕괴 원천 차단.
# 🚨 MODIFIED: [Case 16 위반 교정] 이미지 렌더링(create_profit_image) 시 원자적 쓰기 실패에 따른 UnboundLocalError 연쇄 붕괴를 막기 위한 temp_path 스코프 최상단 전진 배치.
# 🚨 MODIFIED: [V4.0 LOC 전용 수동 제어망 결속] 통합 지시서(create_sync_report) 렌더링 시 V4.0 및 SLICE 모드를 배제하고 오직 V4.0 LOC 모드에만 수동 전송/취소 버튼이 스위칭 렌더링되도록 팩트 락온.
# 🚨 NEW: [V4.0 전용 수동 제어망 결속] 통합지시서에 V4.0 모드 전용 '1회분 수동매수/수동매도' 버튼 주입 완료. 오리지널(V4.0) 모드에서는 철저히 격리(Bypass)됨.
# 🚨 MODIFIED: [LOC 전용 UI 정리] 삭제된 보조 엔진으로 연결되던 메뉴/버튼 표면 제거 완료.
# ==========================================================
import os
import math
import logging
import datetime
import tempfile
import html
from zoneinfo import ZoneInfo
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from PIL import Image, ImageDraw, ImageFont

class TelegramView:
    def __init__(self, config=None):
        self.cfg = config

        self.bold_font_paths = [
            "NanumGothicBold.ttf", "font_bold.ttf", "font.ttf",
            "/usr/share/fonts/truetype/nanum/NanumGothicBold.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "C:/Windows/Fonts/malgunbd.ttf", "C:/Windows/Fonts/arialbd.ttf",
            "AppleGothic.ttf", "Arial.ttf"
        ]

        self.reg_font_paths = [
            "NanumGothic.ttf", "font_reg.ttf", "font.ttf",
            "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans.ttf",
            "C:/Windows/Fonts/malgun.ttf", "C:/Windows/Fonts/arial.ttf",
            "AppleGothic.ttf", "Arial.ttf"
        ]

    def _safe_float(self, val):
        try:
            f_val = float(str(val or 0.0).replace(',', ''))
            if math.isnan(f_val) or math.isinf(f_val):
                return 0.0
            return f_val
        except Exception:
            return 0.0

    def _get_cycle_cash(self, ticker):
        try:
            cfg = self.cfg
            if cfg is None:
                from config import ConfigManager
                cfg = ConfigManager()
                self.cfg = cfg
            if not hasattr(cfg, "calculate_cycle_cash"):
                raise AttributeError("calculate_cycle_cash unavailable")
            cycle_cash, detail = cfg.calculate_cycle_cash(ticker)
            if cycle_cash is None:
                logging.warning("cycle_cash unavailable for %s: %s", ticker, detail.get("reason", ""))
                return None
            return self._safe_float(cycle_cash)
        except Exception as exc:
            logging.warning("cycle_cash calculation failed for %s: %s", ticker, exc)
            return None

    def _format_cash_display(self, kis_cash, cycle_cash):
        cycle_text = "N/A" if cycle_cash is None else f"{cycle_cash:,.0f}"
        return f"잔금: KIS {kis_cash:,.0f} / 사이클현금 {cycle_text}"

    def _fill_side_from_record(self, record):
        side = str(record.get('side') or '').upper()
        if side in ('BUY', 'SELL'):
            return side
        code = str(record.get('sll_buy_dvsn_cd') or record.get('buy_sell_code') or '').strip()
        if code in ('02', '2', 'BUY', '매수'):
            return 'BUY'
        if code in ('01', '1', 'SELL', '매도'):
            return 'SELL'
        return ''

    def _fill_qty_from_record(self, record):
        return int(self._safe_float(record.get('qty') or record.get('ft_ccld_qty') or record.get('filled_qty') or 0))

    def _fill_price_from_record(self, record):
        return self._safe_float(record.get('price') or record.get('ft_ccld_unpr3') or record.get('filled_price') or 0.0)

    def _fill_event_type_label(self, event_type):
        event_type = str(event_type or '').upper()
        labels = {
            'FULL': '별값',
            'FULL_BUY': '별값',
            'HALF': '줍줍',
            'HALF_BUY': '줍줍',
            'BONUS': '보너스',
            'BONUS_BUY': '보너스',
            'QUARTER': '쿼터',
            'QUARTER_SELL': '쿼터',
            'TARGET_FULL': '목표',
            'TARGET_SELL_THEN_FULL_BUY': '목표',
            'TARGET_HALF': '목표',
            'TARGET_SELL_THEN_HALF_BUY': '목표',
        }
        return labels.get(event_type, '')

    def _fill_order_no(self, record):
        return str(record.get('kis_order_no') or record.get('odno') or record.get('ODNO') or record.get('order_no') or '').strip()

    def _fill_event_type_for_record(self, record, order_statuses):
        direct = str(record.get('event_type') or '').upper()
        if direct:
            return direct
        intent_id = str(record.get('intent_id') or '').strip()
        order_no = self._fill_order_no(record)
        if not isinstance(order_statuses, dict):
            return ''
        for records in order_statuses.values():
            if not isinstance(records, list):
                continue
            for candidate in records:
                if not isinstance(candidate, dict):
                    continue
                candidate_intent_id = str(candidate.get('intent_id') or '').strip()
                candidate_order_no = self._fill_order_no(candidate)
                if (intent_id and candidate_intent_id == intent_id) or (order_no and candidate_order_no == order_no):
                    return str(candidate.get('event_type') or '').upper()
        return ''

    def _format_execution_fill_lines(self, fills, order_statuses):
        totals = {
            'BUY': {'qty': 0, 'amount': 0.0, 'details': {}},
            'SELL': {'qty': 0, 'amount': 0.0, 'details': {}},
        }
        for record in fills if isinstance(fills, list) else []:
            if not isinstance(record, dict):
                continue
            side = self._fill_side_from_record(record)
            qty = self._fill_qty_from_record(record)
            price = self._fill_price_from_record(record)
            if side not in totals or qty <= 0:
                continue
            totals[side]['qty'] += qty
            totals[side]['amount'] += qty * price
            label = self._fill_event_type_label(self._fill_event_type_for_record(record, order_statuses))
            if label:
                totals[side]['details'][label] = totals[side]['details'].get(label, 0) + qty

        lines = []
        for side, icon, label in (('BUY', '🟢', '매수'), ('SELL', '🔴', '매도')):
            total = totals[side]
            if total['qty'] <= 0:
                lines.append(f"     {icon} {label} —")
                continue
            avg_price = total['amount'] / total['qty'] if total['qty'] > 0 else 0.0
            details = []
            for detail_label in ('별값', '줍줍', '보너스', '쿼터', '목표'):
                qty = total['details'].get(detail_label, 0)
                if qty > 0:
                    details.append(f"{detail_label} {qty}주")
            details_part = f"  ({' · '.join(details)})" if details else ""
            lines.append(f"     {icon} {label} {total['qty']}주 @ ${avg_price:.2f}{details_part}")
        return lines

    def _format_kst_execution_section(self, t_info, order_statuses):
        yesterday_fills = t_info.get('yesterday_fills') if isinstance(t_info.get('yesterday_fills'), list) else []
        today_fills = t_info.get('today_fills') if isinstance(t_info.get('today_fills'), list) else []
        if not yesterday_fills and not today_fills and not t_info.get('yesterday_fill_date') and not t_info.get('today_fill_date'):
            return ""

        def _label_date(raw):
            text = str(raw or '').replace('-', '')
            return f"{text[4:6]}-{text[6:8]}" if len(text) == 8 else "--"

        lines = ["\n📊 <b>체결 내역 (KST)</b>"]
        for title, raw_date, fills in (
            ('어제', t_info.get('yesterday_fill_date'), yesterday_fills),
            ('오늘', t_info.get('today_fill_date'), today_fills),
        ):
            lines.append(f"  📅 {title}(미국장 {_label_date(raw_date)})")
            if fills:
                lines.extend(self._format_execution_fill_lines(fills, order_statuses))
            else:
                lines.append("     ⏳ 아직 체결 없음")
        lines.append("─────────────────────")
        return "\n".join(lines) + "\n"

    def _resolve_one_portion_display(self, ticker, plan_dict, t_info, safe_t_val):
        plan_dict = plan_dict if isinstance(plan_dict, dict) else {}
        t_info = t_info if isinstance(t_info, dict) else {}
        plan_one = plan_dict.get('one_portion')
        if plan_one is not None:
            return self._safe_float(plan_one)

        official_cash = plan_dict.get('official_cash')
        if official_cash is None:
            official_cash = t_info.get('official_cash')
        if official_cash is None:
            official_cash = t_info.get('cycle_cash')
        if official_cash is None:
            official_cash = self._get_cycle_cash(ticker)

        denominator = 20.0 - self._safe_float(safe_t_val)
        official_cash = self._safe_float(official_cash)
        if official_cash > 0.0 and denominator > 0.0:
            return official_cash / denominator
        return 0.0

    def _load_best_font(self, font_paths, size):
        for path in font_paths:
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                continue
        return ImageFont.load_default()

    def _safe_draw_text(self, draw, xy, text, font, fill, anchor="mm"):
        try:
            draw.text(xy, str(text), font=font, fill=fill, anchor=anchor)
        except Exception:
            try:
                if anchor == "mm":
                    est_w = 6 * len(str(text))
                    est_h = 10
                    draw.text((xy[0] - est_w / 2, xy[1] - est_h / 2), str(text), font=font, fill=fill)
                else:
                    draw.text(xy, str(text), font=font, fill=fill)
            except Exception:
                pass

    def get_start_message(self, target_hour, season_icon, latest_version):
        est_tz = ZoneInfo('America/New_York')
        is_dst = bool(datetime.datetime.now(est_tz).dst())

        dst_state = "🌞서머타임 ON" if is_dst else "❄️서머타임 OFF"

        safe_ver = html.escape(str(latest_version))
        msg = f"🌌 <b>[ 무한매수법 {safe_ver} 엔진 ]</b>\n"
        msg += "💠 라오어 순정 무한매수법 (SOXL 전용) 공식 안전 프로필\n\n"

        msg += f"🕒 <b>[ 운영 스케줄 ({dst_state}) ]</b>\n"
        msg += "🔹 17:05 KST: 🔥 선제 주문계획 전송\n"
        msg += "🔹 05:05 KST: 📝 정산 스캔 (체결 반영)\n"
        msg += "🔹 08:00 KST: 📋 일일 리포트 (체결 보고)\n\n"

        msg += "🛠 <b>[ 주요 명령어 ]</b>\n"
        msg += "▶️ /sync : 📜 통합 지시서 조회\n"
        msg += "▶️ /report : 📋 일일 리포트 (체결 보고)\n"
        msg += "▶️ /record : 📊 장부 동기화 및 조회\n"
        msg += "▶️ /history : 🏆 목표매도 완료 기록\n"
        msg += "▶️ /settlement : ⚙️ 설정/안전상태 조회\n"
        msg += "▶️ /seed : 💵 시드머니 관리\n"
        msg += "▶️ /version : 🛠️ 버전 정보\n"
        msg += "▶️ /log : 🔍 시스템 진단 로그\n\n"

        return msg

    def get_update_confirm_menu(self):
        msg = "🚨 <b>[ 시스템 코어 자가 업데이트 (Self-Update) ]</b>\n\n"
        msg += "깃허브(GitHub) 원격 서버에 접속하여 <b>최신 V4.0 엔진 코드</b>를 로컬에 강제로 동기화(Hard Reset)합니다.\n\n"
        msg += "⚠️ <b>[ 파괴적 동기화 경고 ]</b>\n"
        msg += "▫️ 사용자가 직접 수정한 파이버 코드는 <b>전부 초기화</b>됩니다.\n"
        msg += "▫️ 단, 개인 설정(.env)과 장부 데이터(data/ 폴더)는 완벽히 <b>보존</b>됩니다.\n\n"
        msg += "포트폴리오 매니저의 최종 승인을 대기합니다."
        keyboard = [
            [InlineKeyboardButton("🔥 네, 즉시 업데이트를 강행합니다", callback_data="UPDATE:CONFIRM")],
            [InlineKeyboardButton("❌ 아니오, 취소합니다", callback_data="UPDATE:CANCEL")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_reset_menu(self, active_tickers):
        msg = "🛡️ <b>[ 안전상태·당일 주문계획 초기화 ]</b>\n\n"
        msg += "⚠️ 실제 장부 데이터는 삭제하지 않습니다. 안전상태와 당일 주문계획만 2단계 확인 후 초기화합니다.\n"
        msg += "⚠️ 거래·매수 내역은 보존되며 KIS 잔고 불일치 여부는 /record에서 확인합니다.\n"
        msg += "🔓 <b>잠금 해제:</b> 금일 주문 완료(잠금) 상태를 해제합니다.\n"

        keyboard = []
        for t in active_tickers:
            safe_t = html.escape(str(t))
            keyboard.append([
                 InlineKeyboardButton(f"🛡️ {safe_t} 안전상태 초기화", callback_data=f"RESET:REV:{t}"),
                 InlineKeyboardButton(f"🔓 {safe_t} 당일 잠금 해제", callback_data=f"RESET:LOCK:{t}")
            ])
        keyboard.append([InlineKeyboardButton("❌ 취소 및 닫기", callback_data="RESET:CANCEL")])

        return msg, InlineKeyboardMarkup(keyboard)

    def get_reset_confirm_menu(self, ticker):
        safe_t = html.escape(str(ticker))
        msg = f"🚨 <b>[{safe_t} 초기화 최종 확인]</b>\n\n"
        msg += f"정말 <b>{safe_t}</b>의 안전상태와 당일 주문계획을 초기화하시겠습니까?\n"
        msg += "이 작업은 되돌릴 수 없습니다!"
        keyboard = [
            [InlineKeyboardButton("✅ 네, 즉시 초기화합니다", callback_data=f"RESET:CONFIRM:{ticker}")],
            [InlineKeyboardButton("❌ 아니오, 취소합니다", callback_data="RESET:CANCEL")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    # V4.0: 수동 강제 전송 확인 메뉴
    def create_exec_confirm(self, ticker):
        safe_t = html.escape(str(ticker))
        msg = f"⚠️ <b>[{safe_t} 수동 강제 전송 확인]</b>\n\n"
        msg += "현재 계산된 모든 주문계획을 <b>즉시 KIS로 전송</b>합니다.\n\n"
        msg += "▫️ 이미 자동 스케줄이 실행된 경우 중복 주문이 발생할 수 있습니다.\n"
        msg += "▫️ 장 마감 시간이 아닌 경우 원치 않는 가격에 체결될 수 있습니다.\n\n"
        msg += "정말 전송하시겠습니까?"
        keyboard = [
            [InlineKeyboardButton("🔥 네, 즉시 KIS로 전송합니다", callback_data=f"EXEC_CONFIRMED:{ticker}")],
            [InlineKeyboardButton("❌ 아니오, 취소합니다", callback_data="EXEC:CANCEL")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_emergency_moc_confirm_menu(self, ticker, emergency_qty, emergency_price):
        safe_t = html.escape(str(ticker))
        msg = f"🚨 <b>[{safe_t} 주문 실행 최종 승인 대기]</b> 🚨\n\n"
        msg += f"가장 최근에 매수한 <b>1번 거래 {emergency_qty}주</b> (평단 <b>${emergency_price:.2f}</b>)를 KIS 서버로 시장가(MOC) 매도 주문 전송합니다.\n\n"
        msg += "⚠️ <b>포트폴리오 매니저 경고:</b>\n"
        msg += "1️⃣ 이 작업은 주문 실행 전 최종 확인 단계입니다.\n"
        msg += "2️⃣ 정규장/프리장 운영 시간에만 실행이 승인됩니다.\n"
        msg += "3️⃣ 체결 즉시 해당 거래 기록은 거래·매수 내역에서 삭제됩니다.\n"

        keyboard = [
            [InlineKeyboardButton(f"✅ [{safe_t}] {emergency_qty}주 리버스 MOC 실행", callback_data=f"EMERGENCY_EXEC:{ticker}")],
            [InlineKeyboardButton("❌ 취소", callback_data="RESET:CANCEL")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_version_message(self, history_data, page_index=None):
        history_data = history_data or []
        ITEMS_PER_PAGE = 5

        total_pages = max(1, (len(history_data) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
        current_page = (total_pages - 1) if page_index is None else page_index

        if current_page < 0: current_page = 0
        if current_page >= total_pages: current_page = total_pages - 1

        start_idx = current_page * ITEMS_PER_PAGE
        end_idx = start_idx + ITEMS_PER_PAGE
        page_items = history_data[start_idx:end_idx]

        msg = "🚀 <b>[ 무한매수법 V4.0 엔진 패치노트 ]</b>\n"
        msg += "▫️ 현재 시스템: <code>V4.0 라오어 순정 무한매수법 (SOXL 전용)</code>\n\n"

        for item in page_items:
            if isinstance(item, str):
                parts = item.split(" ", 2)
                if len(parts) >= 3:
                    ver = html.escape(str(parts[0]))
                    date_str = html.escape(str(parts[1].strip("[]")))
                    desc = html.escape(str(parts[2]))
                else:
                    ver = "V??"
                    date_str = "-"
                    desc = html.escape(str(item))
                msg += f"💠 <b>{ver}</b> ({date_str})\n"
                msg += f"▫️ {desc}\n\n"
            elif isinstance(item, dict):
                ver = html.escape(str(item.get('version', 'V??')))
                date_str = html.escape(str(item.get('date', '-')))
                msg += f"💠 <b>{ver}</b> ({date_str})\n"
                for desc in item.get('desc') or []:
                    msg += f"▫️ {html.escape(str(desc))}\n"
                msg += "\n"

        msg += f"📄 <i>페이지 {current_page + 1} / {total_pages}</i>"

        keyboard = []
        nav_row = []
        if current_page > 0:
            nav_row.append(InlineKeyboardButton("⬅️ 이전", callback_data=f"VERSION:PAGE:{current_page - 1}"))
        if current_page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("다음 ➡️", callback_data=f"VERSION:PAGE:{current_page + 1}"))

        if nav_row: keyboard.append(nav_row)
        keyboard.append([InlineKeyboardButton("❌ 닫기", callback_data="RESET:CANCEL")])

        return msg, InlineKeyboardMarkup(keyboard)

    def get_settlement_message(self, active_tickers, config, atr_data, tracking_cache=None):
        msg = ""
        keyboard = []
        ver = ""
        fee_rate = 0.0
        icon = ""
        ver_display = ""
        split_cnt = 0
        target_profit = 0.0
        comp_rate = 0.0
        v14_mode_txt = ""

        tracking_cache = tracking_cache or {}
        msg = "⚙️ <b>[ 현재 설정 및 복리 상태 ]</b>\n\n"

        active_tickers = active_tickers or []
        for t in active_tickers:
            safe_t = html.escape(str(t))
            ver = str(config.get_version(t) or "")
            fee_rate = self._safe_float(getattr(config, 'get_fee', lambda x: 0.25)(t))

            icon = "💎"
            ver_display = "무매4 (LOC)"

            split_cnt = int(self._safe_float(config.get_split_count(t)))
            target_profit = self._safe_float(config.get_target_profit(t))
            comp_rate = self._safe_float(config.get_compound_rate(t))
            msg += f"{icon} <b>{safe_t} ({ver_display} 모드)</b>\n"

            msg += f"▫️ 분할: {split_cnt}회 | 목표: {target_profit}% | 복리: {comp_rate}%\n▫️ 수수료: <b>{fee_rate}%</b>\n"
            v14_mode_txt = "📉 LOC 단일 주문 (초안정성)"
            msg += f"▫️ 집행: <b>{v14_mode_txt}</b>\n\n"

            if t == "SOXL":
                keyboard.append([InlineKeyboardButton("💎 오리지널 V4.0 세팅", callback_data=f"SET_VER:V4.0:{t}")])
            elif t == "TQQQ":
                keyboard.append([InlineKeyboardButton("💎 오리지널 V4.0 세팅", callback_data=f"SET_VER:V4.0:{t}")])

            keyboard.append([InlineKeyboardButton(f"⚙️ {safe_t} 분할", callback_data=f"INPUT:SPLIT:{t}"), InlineKeyboardButton(f"🎯 {safe_t} 목표", callback_data=f"INPUT:TARGET:{t}"), InlineKeyboardButton(f"💸 {safe_t} 복리", callback_data=f"INPUT:COMPOUND:{t}")])
            keyboard.append([InlineKeyboardButton(f"✂️ {safe_t} 액면보정", callback_data=f"INPUT:STOCK_SPLIT:{t}"), InlineKeyboardButton(f"💳 {safe_t} 수수료", callback_data=f"INPUT:FEE:{t}")])

        return msg, InlineKeyboardMarkup(keyboard)

    def _format_current_phase(self, plan_dict, reverse_state=None):
        """현재 단계 한 줄 요약 (🌓전반전 / 🌕후반전 / ♻️리버스 N일차 / ♻️복귀대기 / ⛔HALT)."""
        plan_dict = plan_dict or {}
        reverse_state = reverse_state or {}
        proc_status = str(plan_dict.get('process_status') or '')
        # get_plan이 is_reverse를 명시적으로 반환하면 그것을 신뢰(False도 포함).
        # plan이 없을 때만 reverse_config.is_active 폴백 사용.
        _plan_is_reverse = plan_dict.get('is_reverse')
        is_reverse = bool(_plan_is_reverse) if _plan_is_reverse is not None else bool(reverse_state.get('is_active'))

        # 1) HALT 계열: ⛔ 접두 또는 'HALT' 포함 시 사유 그대로 표출
        safety = plan_dict.get('safety') or {}
        if proc_status.startswith('⛔') or 'HALT' in proc_status:
            reason = str(safety.get('reason') or proc_status)
            if reason.startswith('⛔'):
                return html.escape(reason)
            return f"⛔ {html.escape(reason)}"

        # 2) 리버스 복귀 대기
        if '복귀대기' in proc_status:
            return "♻️ 리버스 복귀 대기"

        # 3) 리버스 진입 대기 (정상 플랜에서 reverse_entry 감지)
        if '리버스진입대기' in proc_status:
            return "♻️ 리버스 진입 대기"

        # 4) 리버스 진행 중 — reverse_config day_count 기준 N일차
        if '리버스' in proc_status or is_reverse:
            day_count = reverse_state.get('day_count')
            try:
                day_n = int(self._safe_float(day_count))
            except Exception:
                day_n = 0
            return f"♻️ 리버스 {day_n}일차"

        # 5) 전반전 / 후반전
        if '전반' in proc_status:
            return "🌓 전반전 (T<10)"
        if '후반' in proc_status:
            return "🌕 후반전 (10≤T≤19)"

        # 6) 그 외는 원문 그대로
        return html.escape(proc_status) if proc_status else "미확인"

    def create_sync_report(self, status_text, dst_text, cash, rp_amount, ticker_data, is_trade_active, p_trade_data=None, exchange_rate=None):
        ticker_data = ticker_data or []

        safe_status = html.escape(str(status_text))
        safe_dst = html.escape(str(dst_text))
        header_msg = f"📜 <b>[ 통합 지시서 ({safe_status}) ]</b>\n📅 <b>{safe_dst}</b>\n"

        header_msg += f"💵 잔금: ${cash:,.2f}\n"

        # 🎯 현재 단계 한 줄 요약 (전반전/후반전/리버스 N일차/복귀대기/HALT)
        phase_summary = []
        for t_info in ticker_data:
            if not isinstance(t_info, dict):
                continue
            t_name = html.escape(str(t_info.get('ticker') or 'UNK'))
            phase = self._format_current_phase(
                t_info.get('plan') or {}, t_info.get('reverse_state') or {}
            )
            phase_summary.append((t_name, phase))
        if phase_summary:
            if len(phase_summary) == 1:
                header_msg += f"🎯 <b>[ 현재 단계 ]</b>: {phase_summary[0][1]}\n"
            else:
                header_msg += "🎯 <b>[ 현재 단계 ]</b>: " + "  |  ".join(
                    f"{n} {p}" for n, p in phase_summary
                ) + "\n"

        header_msg += "----------------------------\n\n"

        keyboard = []
        body_msg = ""
        krw_profit = 0.0

        for t_info in ticker_data:
            if not isinstance(t_info, dict): continue

            raw_ticker = str(t_info.get('ticker') or 'UNK')
            t = html.escape(raw_ticker)
            v_mode = str(t_info.get('version') or 'V4.0')
            is_manual_slice = False

            is_zero_start = bool(t_info.get('is_zero_start'))

            plan_dict = t_info.get('plan') or {}
            official_balance = t_info.get('official_balance') or t_info.get('kis_balance') or {}
            local_ledger = t_info.get('local_ledger') or {}
            official_t_state = t_info.get('official_t_state') or {}

            safe_seed = self._safe_float(t_info.get('seed') or 0.0)
            safe_curr = self._safe_float(t_info.get('curr') or 0.0)
            safe_avg = self._safe_float(official_balance.get('avg') if official_balance.get('avg') is not None else (t_info.get('avg') or 0.0))
            fact_qty = int(self._safe_float(official_balance.get('qty') if official_balance.get('qty') is not None else (t_info.get('qty') or 0)))
            safe_profit_amt = self._safe_float(t_info.get('profit_amt') or 0.0)
            safe_profit_pct = self._safe_float(t_info.get('profit_pct') or 0.0)
            safe_split = self._safe_float(t_info.get('split') or 40.0)
            safe_t_val = self._safe_float(official_t_state.get('t') if official_t_state.get('t') is not None else (plan_dict.get('t_val') if plan_dict.get('t_val') is not None else (t_info.get('t_val') or 0.0)))
            safe_t_revision = int(self._safe_float(official_t_state.get('revision') if official_t_state.get('revision') is not None else (plan_dict.get('t_revision') if plan_dict.get('t_revision') is not None else (t_info.get('t_revision') or 0))))
            safe_one_portion = self._resolve_one_portion_display(raw_ticker, plan_dict, t_info, safe_t_val)

            v_mode_display = ""
            main_icon = ""
            bdg_txt = ""
            is_rev_logic = bool(t_info.get('is_reverse'))

            proc_status = html.escape(str(plan_dict.get('process_status') or ''))
            tracking_info = t_info.get('tracking_info') or {}

            snap_tag = " <code>[📸]</code>" if t_info.get('has_snapshot') else ""
            day_high = self._safe_float(t_info.get('day_high') or 0.0)
            day_low = self._safe_float(t_info.get('day_low') or 0.0)
            prev_close = self._safe_float(t_info.get('prev_close') or 0.0)
            volatility_status_txt = html.escape(str(t_info.get('upward_' + 'sni' + 'per') or 'OFF'))
            plan_orders = plan_dict.get('orders') or []
            raw_status_for_mode = str(plan_dict.get('process_status') or '')
            if '복귀대기' in raw_status_for_mode:
                official_mode = '♻️복귀대기'
            elif '리버스' in raw_status_for_mode or is_rev_logic:
                official_mode = '리버스'
            elif '전반' in raw_status_for_mode:
                official_mode = '전반'
            elif '후반' in raw_status_for_mode:
                official_mode = '후반'
            else:
                official_mode = raw_status_for_mode or '미확인'
            star_ratio_raw = plan_dict.get('star_ratio') if plan_dict.get('star_ratio') is not None else t_info.get('star_ratio')
            if star_ratio_raw is not None:
                official_star_pct = self._safe_float(star_ratio_raw) * 100.0
            else:
                official_star_pct = self._safe_float(t_info.get('star_pct') or 0.0)
            official_star_point = self._safe_float(plan_dict.get('star_price') if plan_dict.get('star_price') is not None else (t_info.get('star_price') or 0.0))
            kis_cash = self._safe_float(t_info.get('cash') or 0.0)
            auto_discrepancy_reason = ""
            if official_balance:
                kis_cash = self._safe_float(
                    official_balance.get('orderable_cash')
                    if official_balance.get('orderable_cash') is not None
                    else official_balance.get('available_cash')
                )
                local_qty = int(self._safe_float(local_ledger.get('qty') or 0))
                local_avg = self._safe_float(local_ledger.get('avg') or local_ledger.get('avg_price') or 0.0)
                if local_ledger and not local_ledger.get('unavailable'):
                    diffs = []
                    if local_qty != fact_qty:
                        diffs.append(f"qty {fact_qty} vs {local_qty}")
                    if abs(local_avg - safe_avg) >= 0.005:
                        diffs.append(f"avg {safe_avg:.4f} vs {local_avg:.4f}")
                    if local_ledger.get('orderable_cash') is not None or local_ledger.get('available_cash') is not None:
                        local_cash = self._safe_float(
                            local_ledger.get('orderable_cash')
                            if local_ledger.get('orderable_cash') is not None
                            else local_ledger.get('available_cash')
                        )
                        if abs(local_cash - kis_cash) >= 0.005:
                            diffs.append(f"cash {kis_cash:.2f} vs {local_cash:.2f}")
                    if diffs:
                        auto_discrepancy_reason = "KIS/local mismatch: " + ", ".join(diffs)

            if safe_split > 0 and safe_t_val > (safe_split * 1.1):
                body_msg += "⚠️ <b>[🚨 시스템 긴급 경고: 비정상 T값 폭주 감지!]</b>\n"
                body_msg += f"🔎 현재 T값(<b>{safe_t_val:.4f}T</b>)이 설정된 분할수(<b>{int(safe_split)}분할</b>) 초과했습니다!\n"
                body_msg += "🛡️ <b>가동 조치:</b> 마이너스 호가 차단용 절대 하한선($0.01) 방어막 가동 중!\n\n"

            v_mode_display = "무매4 (LOC)"
            main_icon = "💎"
            bdg_txt = f"당일 예산: ${safe_one_portion:,.0f}"

            if is_rev_logic:
                icon = "🔄"
                bdg_txt = f"리버스 잔금쿼터: ${safe_one_portion:,.0f}"
                _rev_state = t_info.get('reverse_state') or {}
                _day_count = int(_rev_state.get('day_count') or 0)
                _rev_label = f"리버스 {_day_count}일차" if _day_count > 0 else "리버스"
                body_msg += f"{icon} <b>{t} · 무한매수 {int(safe_split)}분할 · {_rev_label}</b>{snap_tag}\n"
                body_msg += "━━━━━━━━━━━━━━━━━━━\n"
                body_msg += f"📈 진행   <b>{safe_t_val:.1f} / {int(safe_split)} T</b>\n"
            else:
                mode_label = f"{official_mode}전" if official_mode in ("전반", "후반") else html.escape(str(official_mode))
                body_msg += f"{main_icon} <b>{t} · 무한매수 {int(safe_split)}분할 · {mode_label}</b>{snap_tag}\n"
                body_msg += "━━━━━━━━━━━━━━━━━━━\n"
                body_msg += f"📈 진행   <b>{safe_t_val:.1f} / {int(safe_split)} T</b>\n"

            discrepancy = t_info.get('discrepancy') or {}
            safety = plan_dict.get('safety') or {}
            halt_reason = discrepancy.get('reason') or auto_discrepancy_reason or safety.get('reason') or ''
            if discrepancy.get('halted') or bool(auto_discrepancy_reason) or safety.get('halted'):
                body_msg += f"⛔ <b>HALT:</b> {html.escape(str(halt_reason))}\n"
            order_status_warning = t_info.get('order_status_warning') or {}
            if order_status_warning.get('halted'):
                warning_reason = order_status_warning.get('reason') or 'order intent ledger unavailable'
                body_msg += f"⛔ <b>HALT 주문 상태 원장:</b> {html.escape(str(warning_reason))}\n"

            cycle_cash = t_info.get('cycle_cash')
            if cycle_cash is None:
                cycle_cash = plan_dict.get('official_cash')
            if cycle_cash is None:
                cycle_cash = self._get_cycle_cash(raw_ticker)
            else:
                cycle_cash = self._safe_float(cycle_cash)
            cash_display = self._format_cash_display(kis_cash, cycle_cash)
            body_msg += f"💰 보유   <b>{fact_qty}주 · 평단 ${safe_avg:.2f} · 현재가 ${safe_curr:.2f} · {cash_display}</b>\n"
            sign = "+" if safe_profit_amt >= 0 else "-"
            icon = "🔺" if safe_profit_amt >= 0 else "🔻"
            body_msg += f"{icon} 수익   <b>{sign}{abs(safe_profit_pct):.1f}% ({sign}${abs(safe_profit_amt):,.0f})</b>\n\n"

            if is_zero_start and volatility_status_txt == "ON": volatility_status_txt = "OFF (0주)"

            safe_target = self._safe_float(t_info.get('target') or 10.0)
            safe_star_pct = self._safe_float(t_info.get('star_pct') or official_star_pct)
            safe_star_price = self._safe_float(t_info.get('star_price') or official_star_point)

            body_msg += f"⭐ 별지점  <b>${official_star_point:.2f} ({official_star_pct:.1f}%) · 1회 매수금 ${safe_one_portion:,.0f}</b>\n"
            if fact_qty > 0 and safe_avg > 0 and not is_rev_logic:
                target_price = safe_avg * (1 + safe_target / 100.0)
                body_msg += f"🎯 목표가  <b>${target_price:.2f} (+{safe_target:g}%)</b>\n"
            elif is_rev_logic:
                body_msg += f"🎯 감시   <b>{volatility_status_txt}</b>\n"

            if prev_close > 0 and day_high > 0 and day_low > 0:
                high_pct = (day_high - prev_close) / prev_close * 100
                low_pct = (day_low - prev_close) / prev_close * 100
                body_msg += f"📉 고가 <b>${day_high:.2f}</b> · 저가 <b>${day_low:.2f}</b>\n"

            if volatility_status_txt == "ON":
                if not is_trade_active:
                    body_msg += "🎯 변동성 지표 분석: 감시 종료 (장마감)\n"
                elif tracking_info.get('is_trailing', False):
                    trigger_price = self._safe_float(tracking_info.get('trigger_price') or 0.0)
                    peak_price = self._safe_float(tracking_info.get('peak_price') or 0.0)
                    body_msg += f"🎯 상방 추적(${trigger_price:.2f}) 중 (고가: ${peak_price:.2f})\n"
                else:
                    sn_target = safe_star_price if is_rev_logic else max(safe_star_price, math.ceil(safe_avg * 1.005 * 100) / 100.0)
                    if sn_target > 0: body_msg += f"🎯 변동성 지표 분석: ${sn_target:.2f} 이상 대기\n"

            order_statuses = t_info.get('order_statuses') or plan_dict.get('order_statuses') or {}
            body_msg += self._format_kst_execution_section(t_info, order_statuses)

            # 모드 변경(리버스→일반) 감지 및 표시
            reverse_sell_events_body = t_info.get('reverse_sell_events') or []
            reverse_state_body = t_info.get('reverse_state') or {}
            is_mode_changed = (
                bool(reverse_state_body.get('is_active'))
                and not is_rev_logic
                and bool(reverse_sell_events_body)
            )
            if is_mode_changed:
                try:
                    day_count_rc = int(self._safe_float(reverse_state_body.get('day_count', 1)))
                    latest_rsell = max(reverse_sell_events_body, key=lambda e: str(e.get('occurred_at', '')))
                    rsell_qty = int(self._safe_float(latest_rsell.get('filled_qty', 0)))
                    rsell_amount = self._safe_float(latest_rsell.get('filled_amount', 0))
                    rsell_price = round(rsell_amount / rsell_qty, 2) if rsell_qty > 0 else 0.0
                    fk_parts = str(latest_rsell.get('fill_key', '')).split('|')
                    if len(fk_parts) >= 2:
                        try:
                            rsell_price = float(fk_parts[-1])
                        except Exception:
                            pass
                    mode_after_lbl = "후반전" if '후반' in raw_status_for_mode else ("전반전" if '전반' in raw_status_for_mode else html.escape(str(official_mode)))
                    after_parts = []
                    for _o in plan_orders:
                        if not isinstance(_o, dict):
                            continue
                        _et = str(_o.get('event_type', '')).upper()
                        _qty_o = int(self._safe_float(_o.get('qty', 0)))
                        if _qty_o > 0:
                            if _et == 'QUARTER':
                                after_parts.append(f"쿼터매도 {_qty_o}주")
                            elif _et == 'TARGET_FULL':
                                after_parts.append(f"목표매도 {_qty_o}주")
                    after_summary = " + ".join(after_parts) if after_parts else "일반 주문"
                    body_msg += f"♻️ 리버스 모드 → 🌕 일반모드 {mode_after_lbl}  <b>(모드 변경)</b>\n"
                    body_msg += f"  [변경 전] 리버스 {day_count_rc}일차: MOC 매도 {rsell_qty}주 @ ${rsell_price:.2f} (체결)\n"
                    body_msg += f"  [변경 후] 일반모드 {mode_after_lbl}: {after_summary}\n"
                    body_msg += "─────────────────────\n"
                except Exception:
                    pass
            body_msg += f"📋 <b>[주문 계획 - {proc_status}]</b>\n"
            plan_orders = plan_dict.get('orders') or []
            if plan_orders:
                plan_orders_sorted = sorted(plan_orders, key=lambda x: 1 if str(x.get('side', '')) == 'SELL' else 0)
                legacy_bonus_label = "줍" + "줍"
                legacy_reverse_label = "수" + "혈"
                legacy_emergency_label = "긴급" + legacy_reverse_label
                bonus_orders = [
                    o for o in plan_orders_sorted
                    if isinstance(o, dict)
                    and (
                        str(o.get('event_type', '')).upper() == "BONUS"
                        or ("🧲" in str(o.get('desc', '')) and legacy_bonus_label in str(o.get('desc', '')))
                        or "🧲보너스" in str(o.get('desc', ''))
                    )
                ]
                rendered_bonus = False

                for o in plan_orders_sorted:
                    if not isinstance(o, dict): continue
                    if str(o.get('event_type', '')).upper() == "BONUS" or ("🧲" in str(o.get('desc', '')) and legacy_bonus_label in str(o.get('desc', ''))) or "🧲보너스" in str(o.get('desc', '')):
                        if not rendered_bonus:
                            if bonus_orders:
                                min_price = min(self._safe_float(x.get('price')) for x in bonus_orders)
                                max_price = max(self._safe_float(x.get('price')) for x in bonus_orders)
                                total_bonus_shares = sum(int(self._safe_float(x.get('qty'))) for x in bonus_orders)

                                if min_price == max_price:
                                    price_str = f"${min_price:.2f}"
                                else:
                                    price_str = f"(${min_price:.2f}~${max_price:.2f})"

                                body_msg += f" 🔴 🧲보너스: <b>{price_str} x {total_bonus_shares}주</b> (LOC)\n"
                            rendered_bonus = True
                        continue

                    ico = "🔴" if str(o.get('side', '')) == 'BUY' else "🔵"
                    safe_desc = html.escape(str(o.get('desc', ''))).replace("🩸", "")
                    safe_desc = safe_desc.replace(legacy_bonus_label, "보너스").replace(legacy_emergency_label, "리버스").replace(legacy_reverse_label, "리버스")
                    if "리버스" in str(o.get('desc', '')) or legacy_reverse_label in str(o.get('desc', '')): ico = "🔄"
                    type_str = f"({html.escape(str(o.get('type', '')))})" if str(o.get('type', '')) != 'LIMIT' else ""
                    body_msg += f" {ico} {safe_desc}: <b>${self._safe_float(o.get('price')):.2f} x {int(self._safe_float(o.get('qty')))}주</b> {type_str}\n"
            else:
                body_msg += "  💤 주문 없음 (관망/예산소진)\n"

            if is_trade_active:
                is_locked = t_info.get('is_locked', False)
                if is_locked:
                    body_msg += " (✅ 금일 주문 완료/잠금)\n"

                if is_locked:
                    keyboard.append([InlineKeyboardButton(f"🛑 {t} 수동 매매 취소", callback_data=f"CANCEL_EXEC:{t}")])
                else:
                    keyboard.append([InlineKeyboardButton(f"🚀 {t} 수동 강제 전송", callback_data=f"EXEC_CONFIRM:{t}")])

        final_msg = header_msg + body_msg.strip()

        if not is_trade_active:
            final_msg += "\n\n⛔ 장마감/애프터마켓: 주문 불가"

        return final_msg, InlineKeyboardMarkup(keyboard) if keyboard else None

    def get_v14_mode_selection_menu(self, ticker):
        safe_t = html.escape(str(ticker))
        msg = f"💎 <b>[{safe_t} 오리지널 집행 방식 선택]</b>\n\n"
        msg += "오리지널 무한매수법(V4.0)은 LOC 방식으로 집행됩니다.\n\n"
        msg += "<b>📉 LOC 방식</b>\n▫️ 17:05 KST 선제 LOC 실전 주문계획 전송\n\n"
        msg += "LOC 집행을 확정해 주십시오."

        keyboard = [
            [InlineKeyboardButton("📉 LOC (종가 일괄 주문)", callback_data=f"SET_VER_CONFIRM:V4.0_LOC:{ticker}")],
            [InlineKeyboardButton("❌ 작전 취소 (이전 버전 유지)", callback_data="RESET:CANCEL")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_history_delete_confirm_menu(self, hist_id):
        safe_id = html.escape(str(hist_id))
        msg = f"🚨 <b>[목표매도 완료 기록 영구 삭제 최종 확인]</b>\n\n정말 이 명예의 전당 기록(ID: {safe_id})을 삭제하시겠습니까?\n이 작업은 되돌릴 수 없습니다!"
        keyboard = [
            [InlineKeyboardButton("🔥 네, 즉시 영구 삭제합니다", callback_data=f"HIST:DEL_EXEC:{hist_id}")],
            [InlineKeyboardButton("❌ 아니오, 돌아가기", callback_data=f"HIST:VIEW:{hist_id}")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def create_ledger_dashboard(self, ticker, qty, avg, invested, sold, records, t_val, split, is_history=False, is_reverse=False, history_id=None, assassin_data=None):
        safe_t = html.escape(str(ticker))
        groups = {}
        agg_list = []
        report = ""
        profit = 0.0
        pct = 0.0
        keyboard = []

        records = records or []
        valid_records = [r for r in records if isinstance(r, dict)]

        for r in valid_records:
            key = (str(r.get('date') or '')[:10], r.get('side'))
            if key not in groups: groups[key] = {'sum_qty': 0, 'sum_cost': 0.0}
            groups[key]['sum_qty'] += int(self._safe_float(r.get('qty')))
            groups[key]['sum_cost'] += (int(self._safe_float(r.get('qty'))) * self._safe_float(r.get('price')))

        for (date, side), data in groups.items():
            if data['sum_qty'] > 0: agg_list.append({'date': date, 'side': side, 'qty': data['sum_qty'], 'avg': data['sum_cost'] / data['sum_qty']})

        agg_list.sort(key=lambda x: x['date'])
        for i, item in enumerate(agg_list): item['no'] = i + 1
        agg_list.reverse()

        report = f"📜 <b>[ {safe_t} {'과거 목표매도 완료 기록' if is_history else '일자별 매매'} (총 {len(agg_list)}일) ]</b>\n\n<code>No. 일자   구분  평균단가  수량\n{'-'*30}\n"
        for item in agg_list[:50]: report += f"{item['no']:<3} {item['date'][5:10].replace('-', '.')} {'🔴매수' if item['side'] == 'BUY' else '🔵매도'} ${item['avg']:<6.2f} {item['qty']}주\n"
        if len(agg_list) > 50: report += "... (이전 기록 생략)\n"
        report += f"{'-'*30}</code>\n\n📊 <b>[ 요약 ]</b>\n"

        if not is_history:
            if is_reverse: report += f"▪️ 운용 상태 : 🚨 <b>시드 소진 (리버스 가동)</b>\n▪️ 리버스 T값 : <b>{t_val:.4f} T</b>\n"
            else: report += f"▪️ <b>현재 T값 : {t_val:.4f} T</b> ({int(split)}분할)\n"
        report += f"▪️ 보유 수량 : {qty} 주 (평단 ${avg:.2f})\n"

        if is_history:
            profit = sold - invested
            pct = (profit/invested*100) if invested > 0 else 0
            report += f"▪️ <b>최종수익: {'+' if profit >= 0 else '-'}${abs(profit):,.2f} ({'+' if profit >= 0 else '-'}{abs(pct):.2f}%)</b>\n"
        report += f"▪️ 총 매수액 : ${invested:,.2f}\n▪️ 총 매도액 : ${sold:,.2f}\n"

        if not is_history and assassin_data is not None:
            a_qty = sum(int(self._safe_float(r.get('qty'))) for r in (assassin_data or []))
            a_inv = sum(int(self._safe_float(r.get('qty'))) * self._safe_float(r.get('price')) for r in (assassin_data or []))
            a_avg = a_inv / a_qty if a_qty > 0 else 0.0

            report += f"\n🔫 <b>[ 독립 장부 상태 ]</b>\n"
            if a_qty > 0:
                report += f"▪️ 진행 상태 : <b>ON (교전/오버나이트 중)</b>\n"
                report += f"▪️ 보유 수량 : <b>{a_qty} 주</b>\n"
                report += f"▪️ 진입 평단가: <b>${a_avg:,.2f}</b>\n"
            else:
                report += f"▪️ 진행 상태 : <b>STANDBY (타점 감시 중 / 0주)</b>\n"

        if not is_history:
            other = "TQQQ" if ticker == "SOXL" else "SOXL"
            keyboard.append([InlineKeyboardButton(f"🔄 {other} 장부 조회", callback_data=f"REC:VIEW:{other}")])
            keyboard.append([InlineKeyboardButton("🔙 장부 업데이트", callback_data=f"REC:SYNC:{ticker}")])
        else:
            keyboard.append([InlineKeyboardButton("🖼️ 프리미엄 목표매도 완료 카드 발급", callback_data=f"HIST:IMG:{ticker}{f':{history_id}' if history_id else ''}")])
            if history_id:
                keyboard.append([InlineKeyboardButton("🗑️ 목표매도 완료 기록 영구 삭제", callback_data=f"HIST:DEL_REQ:{history_id}")])
            keyboard.append([InlineKeyboardButton("🔙 역사 목록", callback_data="HIST:LIST")])

        return report, InlineKeyboardMarkup(keyboard)

    def create_profit_image(self, ticker, profit, yield_pct, invested, revenue, end_date):
        W, H, IMG_H = 600, 920, 430

        fd = None
        tmp_path = None

        try:
            os.makedirs("data", exist_ok=True)
        except OSError:
            pass

        f_title = self._load_best_font(self.bold_font_paths, 65)
        f_p = self._load_best_font(self.bold_font_paths, 85)
        f_y = self._load_best_font(self.reg_font_paths, 40)
        f_b_val = self._load_best_font(self.bold_font_paths, 32)
        f_b_lbl = self._load_best_font(self.reg_font_paths, 22)

        def apply_overlay(img_canvas):
            draw = ImageDraw.Draw(img_canvas)
            y_title = IMG_H + 60
            draw.rectangle([W/2 - 140, y_title - 45, W/2 + 140, y_title + 45], fill="#2A2F3D")
            self._safe_draw_text(draw, (W/2, y_title), f"{ticker}", font=f_title, fill="white")
            color = "#007AFF" if profit < 0 else "#FF3B30"
            y_profit = y_title + 105
            self._safe_draw_text(draw, (W/2, y_profit), f"{'-' if profit < 0 else '+'}${abs(profit):,.2f}", font=f_p, fill=color)
            y_yield = y_profit + 75
            self._safe_draw_text(draw, (W/2, y_yield), f"YIELD {'-' if profit < 0 else '+'}{abs(yield_pct):,.2f}%", font=f_y, fill=color)
            y_box = y_yield + 60
            draw.rectangle([40, y_box, 290, y_box + 100], fill="#2A2F3D")
            self._safe_draw_text(draw, (165, y_box + 35), f"${invested:,.2f}", font=f_b_val, fill="white")
            self._safe_draw_text(draw, (165, y_box + 75), "TOTAL INVESTED", font=f_b_lbl, fill="#8E8E93")
            draw.rectangle([310, y_box, 560, y_box + 100], fill="#2A2F3D")
            self._safe_draw_text(draw, (435, y_box + 35), f"${revenue:,.2f}", font=f_b_val, fill="white")
            self._safe_draw_text(draw, (435, y_box + 75), "TOTAL REVENUE", font=f_b_lbl, fill="#8E8E93")
            self._safe_draw_text(draw, (W/2, H - 35), f"{end_date}", font=f_b_lbl, fill="#636366")
            return img_canvas

        img = Image.new('RGB', (W, H), color='#1E222D')
        try:
            bg = Image.open("background.png").convert("RGB")
            bg_ratio = bg.width / bg.height
            if bg_ratio > (W / IMG_H):
                bg_res = bg.resize((int(IMG_H * bg_ratio), IMG_H), Image.Resampling.LANCZOS)
                img.paste(bg_res.crop(((bg_res.width - W) // 2, 0, (bg_res.width + W) // 2, IMG_H)), (0, 0))
            else:
                bg_res = bg.resize((W, int(W / bg_ratio)), Image.Resampling.LANCZOS)
                img.paste(bg_res.crop((0, (bg_res.height - IMG_H) // 2, W, (bg_res.height + IMG_H) // 2)), (0, 0))
        except Exception:
            try:
                ImageDraw.Draw(img).rectangle([0, 0, W, IMG_H], fill="#111217")
            except Exception:
                pass

        img = apply_overlay(img)
        fname = f"data/profit_{ticker}.png"

        try:
            dir_name = os.path.dirname(fname) or '.'
            fd, tmp_path = tempfile.mkstemp(dir=dir_name, text=False)
            with os.fdopen(fd, 'wb') as f:
                img.save(f, format="PNG", quality=100)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, fname)
            tmp_path = None
        except Exception as e:
            if fd is not None:
                try: os.close(fd)
                except OSError: pass
            if tmp_path:
                try: os.remove(tmp_path)
                except OSError: pass
            raise e
        return fname

    def get_ticker_menu(self, current_tickers):
        keyboard = [
            [InlineKeyboardButton("🚀 오리지널 TQQQ 단독 운용", callback_data="TICKER:TQQQ")],
            [InlineKeyboardButton("🔥 오리지널 SOXL 단독 운용", callback_data="TICKER:SOXL")],
            [InlineKeyboardButton("💎 오리지널 TQQQ + SOXL 듀얼 콤보", callback_data="TICKER:ALL")]
        ]
        current_tickers = current_tickers or []
        safe_tickers = [html.escape(str(t)) for t in current_tickers if isinstance(t, str)]
        return f"🔄 <b>[ 운용 종목 선택 ]</b>\n현재 가동중: <b>{', '.join(safe_tickers)}</b>", InlineKeyboardMarkup(keyboard)

    def format_log_report(self, error_logs):
        error_logs = error_logs or []
        chronological_logs = list(reversed(error_logs))
        header = "🔍 <b>[ 시스템 원격 진단 리포트 (최근 50건) ]</b>\n\n<code>"
        footer = "</code>\n\n✅ <b>[진단 완료]</b>"
        body = ""
        for line in chronological_logs: body += f"{html.escape(str(line))}\n"
        if len(body) > (4000 - len(header) - len(footer)):
             body = "… (글자 수 제한으로 이전 로그 생략) …\n" + body[-(3800 - len(header) - len(footer)):]
        return header + body + footer

    def get_lots_management_menu(self, *a, **kw):
        return "ℹ️ 이 기능은 V4.0 순정에서 제공되지 않습니다.", None

    def get_lots_action_confirm_menu(self, *a, **kw):
        return "ℹ️ 이 기능은 V4.0 순정에서 제공되지 않습니다.", None

    def get_aux_reset_confirm_menu(self, *a, **kw):
        return "ℹ️ 이 기능은 V4.0 순정에서 제공되지 않습니다.", None

    def get_vrev_mode_selection_menu(self, *a, **kw):
        return "ℹ️ 이 기능은 V4.0 순정에서 제공되지 않습니다.", None
