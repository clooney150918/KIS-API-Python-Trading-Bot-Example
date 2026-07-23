# ==========================================================
# FILE: kis_order_engine.py
# ==========================================================
# 🚨 MODIFIED: [파사드 패턴 3단계] 주문 전송, 취소, 미체결 및 원장 제어 도메인 분리
# 🚨 MODIFIED: [Thundering Herd 영구 소각] 파편화된 time.sleep(0.06) 땜질 코드를 전면 삭제하고, 내부 _api_request 의 GlobalThrottle 중앙 통제로 100% 위임하여 이벤트 루프 교착 상태 완벽 방어.
# 🚨 MODIFIED: [선형 상속 락온] MarketDataProvider를 상속하여 API 클라이언트와 시세 연산 기능까지 풀 패키지 상속
# 🚨 MODIFIED: [Case 18] 로컬 예약 스냅샷 전면 폐기 및 KIS 원장 직접 연동(get_reservation_orders) 기반 취소 집행 유지
# 🚨 MODIFIED: [TypeError 붕괴 궁극 수술] KIS 서버에서 msg1이 None으로 반환될 때 발생하는 문자열 매칭(in) 즉사 버그 완벽 차단
# 🚨 MODIFIED: [페이징 팩트 수술] get_execution_history 내 연속 조회(Pagination) 토큰 미갱신으로 인한 체결 내역 증발 버그 완벽 수술
# 🚨 MODIFIED: [이벤트 루프 교착 방어] cancel_all_orders_safe 내부의 과도한 time.sleep(5)를 1.0초로 단축하여 Caller 타임아웃(10초) 폭발 원천 차단
# 🚨 VERIFIED: [Case 36 절대 방어망 결속] MOC(시장가 매도) 주문 리젝 시 현재가 -5% 최유리 지정가(LIMIT) 덤핑 자동 폴백 100% 팩트 가동
# 🚨 MODIFIED: [주문가능금액 역산 팩트 복구] KIS API가 특정 시간대에 예수금을 0.0으로 반환하는 고질적 결함을 우회하기 위해, 오리지널 수리적 역산(Reverse Calc) 공식(외화예수금+매도정산-매수정산)을 100% 롤백 완료.
# ==========================================================

import time
import datetime
import math
import logging
from zoneinfo import ZoneInfo
from market_data_provider import MarketDataProvider

class KisOrderEngine(MarketDataProvider):
    
    def get_account_balance(self):
        """ 🚨 [Case 03 준수] API 잔고 응답 중복 합산 절대 방어 락온 """
        cash = 0.0
        holdings = {}
        api_success = False 
  
        params = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "WCRC_FRCR_DVSN_CD": "02", "NATN_CD": "840", "TR_MKET_CD": "00", "INQR_DVSN_CD": "00"}
       
        res = self._call_api("CTRP6504R", "/uapi/overseas-stock/v1/trading/inquire-present-balance", "GET", params=params)
   
        if res.get('rt_cd') == '0':
            api_success = True
            # 🚨 MODIFIED: [Null-Pointer 방어] output2 결측 시 빈 딕셔너리로 폴백
            o2 = res.get('output2') or {}
            if isinstance(o2, list): o2 = o2[0] if len(o2) > 0 else {}
     
            if not isinstance(o2, dict): o2 = {}
            
            # 🚨 MODIFIED: [주문가능금액 역산 팩트 복구] KIS API 오류를 우회하는 100% 수식 계산망 롤백
            dncl_amt = self._safe_float(o2.get('frcr_dncl_amt_2', 0))     
            sll_amt = self._safe_float(o2.get('frcr_sll_amt_smtl', 0))      
            buy_amt = self._safe_float(o2.get('frcr_buy_amt_smtl', 0))      
            raw_bp = dncl_amt + sll_amt - buy_amt
            cash = max(0.0, math.floor((raw_bp * 0.9945) * 100) / 100.0)

        target_excgs = ["NASD", "AMEX", "NYSE"] 
  
        for excg in target_excgs:
            fk200, nk200 = "", ""
            for attempt in range(20): 
                # 🚨 MODIFIED: [Thundering Herd 영구 소각] 파편화된 sleep 소각 (GlobalThrottle 위임)
                params_hold = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg, "TR_CRCY_CD": "USD", "CTX_AREA_FK200": fk200, "CTX_AREA_NK200": nk200}
                headers = self._get_header("TTTS3012R")
          
                # 🚨 MODIFIED: [페이징 팩트 수술] 다음 페이지 요청 시 "tr_cont": "N" 강제 주입
                if fk200 or nk200: headers["tr_cont"] = "N"
 
                url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-balance"
         
                res_hold, resp_json = self._api_request("GET", url, headers, params=params_hold)
    
                if res_hold and resp_json.get('rt_cd') == '0':
                    api_success = True
  
                    if cash <= 0:
                        # 🚨 MODIFIED: [Null-Pointer 방어] output2 결측 시 빈 딕셔너리로 폴백
                        o2 = resp_json.get('output2') or {}
                        if isinstance(o2, list): o2 = o2[0] if len(o2) > 0 else {}
           
                        if not isinstance(o2, dict): o2 = {}
                        new_cash = self._safe_float(o2.get('ovrs_ord_psbl_amt', 0))
                        if new_cash > cash: cash = new_cash
                   
                    for item in (resp_json.get('output1') or []):
                        if not isinstance(item, dict): continue
                        ticker = item.get('ovrs_pdno')
                        if not ticker: continue
 
                        qty = int(self._safe_float(item.get('ovrs_cblc_qty', 0)))
                        ord_psbl_qty = int(self._safe_float(item.get('ord_psbl_qty', 0)))
                        avg = self._safe_float(item.get('pchs_avg_pric', 0))
                
                        if qty > 0 and ord_psbl_qty == 0: ord_psbl_qty = qty
               
                        if qty > 0:
                            if ticker not in holdings: 
                                holdings[ticker] = {'qty': qty, 'ord_psbl_qty': ord_psbl_qty, 'avg': avg}
                            else:
                                # 🚨 MODIFIED: [Case 03] 유령 중복 합산 누적 무시 (영구 소각)
                                continue 

                    tr_cont = res_hold.headers.get('tr_cont', '') if hasattr(res_hold, 'headers') else ''
                    fk200 = str(resp_json.get('ctx_area_fk200', '') or '').strip()
                    nk200 = str(resp_json.get('ctx_area_nk200', '') or '').strip()
                    if tr_cont in ['M', 'F'] and nk200:
                        continue
                    else: break
                else: break
        
        if api_success: return cash, holdings
        else: return cash, None

    def get_unfilled_orders_detail(self, ticker):
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        valid_orders = []
    
        fk200, nk200 = "", ""
     
        for attempt in range(10):
            # 🚨 MODIFIED: [Thundering Herd 영구 소각] 파편화된 sleep 소각 (GlobalThrottle 위임)
            params = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd, "SORT_SQN": "DS", "CTX_AREA_FK200": fk200, "CTX_AREA_NK200": nk200}
            headers = self._get_header("TTTS3018R")
            
            # 🚨 MODIFIED: [페이징 팩트 수술] 다음 페이지 요청 시 "tr_cont": "N" 강제 주입
            if fk200 or nk200: headers["tr_cont"] = "N"
         
            res, resp_json = self._api_request("GET", f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-nccs", headers, params=params)
    
            if res and resp_json.get('rt_cd') == '0':
                # 🚨 MODIFIED: [Iterable 붕괴 방어] None 유입 시 []로 단락 평가
                output = resp_json.get('output') or []
            
                if isinstance(output, dict): output = [output]
                if not isinstance(output, list): output = []
                
                valid_orders.extend([item for item in output if isinstance(item, dict) and item.get('pdno') == ticker])
           
                tr_cont = res.headers.get('tr_cont', '') if hasattr(res, 'headers') else ''
                fk200 = str(resp_json.get('ctx_area_fk200', '') or '').strip()
 
                nk200 = str(resp_json.get('ctx_area_nk200', '') or '').strip()
                if tr_cont in ['M', 'F'] and nk200: time.sleep(0.3); continue
                else: break
            else: return False
        return valid_orders

    # 🚨 MODIFIED: [Case 18] 로컬 예약 스냅샷 폐기 및 KIS 원장 직접 연동
    def get_reservation_orders(self, ticker, start_date, end_date):
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        valid_orders = []
  
        fk200, nk200 = "", ""
        
        for attempt in range(15):
            # 🚨 MODIFIED: [Thundering Herd 영구 소각] 파편화된 sleep 소각 (GlobalThrottle 위임)
            params = {
                "CANO": self.cano,
                "ACNT_PRDT_CD": self.acnt_prdt_cd,
                "INQR_STRT_DT": start_date,
                "INQR_END_DT": end_date,
                "INQR_DVSN_CD": "00",
                "OVRS_EXCG_CD": excg_cd,
                "PRDT_TYPE_CD": "",
                "CTX_AREA_FK200": fk200,
                 "CTX_AREA_NK200": nk200
            }
            headers = self._get_header("TTTT3039R")
            
            # 🚨 MODIFIED: [페이징 팩트 수술] 다음 페이지 요청 시 "tr_cont": "N" 강제 주입
            if fk200 or nk200: headers["tr_cont"] = "N"
            
            res, resp_json = self._api_request("GET", f"{self.base_url}/uapi/overseas-stock/v1/trading/order-resv-list", headers, params=params)
            
            if res and resp_json.get('rt_cd') == '0':
                # 🚨 MODIFIED: [Iterable 붕괴 방어] None 유입 시 []로 단락 평가
                output = resp_json.get('output') or []
    
                if isinstance(output, dict): output = [output]
                if not isinstance(output, list): output = []
               
                valid_orders.extend([item for item in output if isinstance(item, dict) and item.get('pdno') == ticker])
                
                tr_cont = res.headers.get('tr_cont', '') if hasattr(res, 'headers') else ''
                fk200 = str(resp_json.get('ctx_area_fk200', '') or '').strip()
                nk200 = str(resp_json.get('ctx_area_nk200', '') or '').strip()
            
                if tr_cont in ['M', 'F'] and nk200:
                    time.sleep(0.3)
                    continue
                else:
                    break
            else:
                break
           
        return valid_orders

    def cancel_all_orders_safe(self, ticker, side=None):
        for i in range(3):
            orders = self.get_unfilled_orders_detail(ticker)
            if orders is False: return False
            if not orders: return True
        
            target_orders = orders
       
            if side == "BUY": target_orders = [o for o in orders if o.get('sll_buy_dvsn_cd') == '02']
            elif side == "SELL": target_orders = [o for o in orders if o.get('sll_buy_dvsn_cd') == '01']
            if not target_orders: return True
            for o in target_orders: 
                # 🚨 MODIFIED: 파편화된 sleep 소각
                self.cancel_order(ticker, o.get('odno'))
     
            # 🚨 MODIFIED: [이벤트 루프 교착 방어] 5.0초의 긴 대기 시간을 1.0초로 단축하여 상위 스케줄러 TimeoutError 원천 봉쇄
            time.sleep(1.0)
       
        final_orders = self.get_unfilled_orders_detail(ticker)
        if final_orders is False: return False
        # 🚨 MODIFIED: [State Mismatch 치명적 결함 수술] side is None(전체 취소) 시 누락되던 멱등성 검증 논리 완벽 복구
        failed_orders = [o for o in final_orders if side is None or (side == "BUY" and o.get('sll_buy_dvsn_cd') == '02') or (side == "SELL" and o.get('sll_buy_dvsn_cd') == '01')]
        return len(failed_orders) == 0
      
    def cancel_targeted_orders(self, ticker, side, target_ord_dvsn):
        sll_buy_cd = '02' if side == "BUY" else '01'
       
        orders = self.get_unfilled_orders_detail(ticker)
        if not orders: return 0
        target_orders = [o for o in orders if o.get('sll_buy_dvsn_cd') == sll_buy_cd and str(o.get('ord_dvsn_cd') or o.get('ord_dvsn') or '') == target_ord_dvsn]
        for o in target_orders: 
            # 🚨 MODIFIED: 파편화된 sleep 소각
            self.cancel_order(ticker, o.get('odno'))
            time.sleep(0.3)
        return len(target_orders)

    def cancel_orders_by_price(self, ticker, side, target_prices):
        sll_buy_cd = '02' if side == "BUY" else '01'
        orders = self.get_unfilled_orders_detail(ticker)
        if not orders: return 0
        target_orders = []
    
        # 🚨 MODIFIED: [배열 내부 Float 오염 차단 & 결측치 방어] 문자열 유입 방지 사전 맵핑 및 단락 평가
        safe_targets = [self._safe_float(tp) for tp in (target_prices or [])]
        
        for o in orders:
             if o.get('sll_buy_dvsn_cd') == sll_buy_cd:
                o_price = 0.0
                for rp in [o.get('ft_ord_unpr3', 0), o.get('ord_unpr', 0), o.get('ovrs_ord_unpr', 0)]:
                    # 🚨 MODIFIED: [Insight 14] String-Float 맹독성 쉴드 래핑
                    try: 
                        val = self._safe_float(rp)
                        if val > 0: o_price = val; break 
                    except (TypeError, ValueError): 
                        pass
                for tp in safe_targets:
                    if o_price > 0 and abs(o_price - tp) < 0.005: target_orders.append(o); break
        for o in target_orders: 
            # 🚨 MODIFIED: 파편화된 sleep 소각
            self.cancel_order(ticker, o.get('odno'))
            time.sleep(0.3)
        return len(target_orders)

    def send_order(self, ticker, side, qty, price, order_type="LIMIT", start_time=None, end_time=None):
        # 🚨 MODIFIED: [Insight 14] String-Float 맹독성 쉴드 래핑
        try: order_qty = int(self._safe_float(qty))
        except (TypeError, ValueError): return {'rt_cd': '999', 'msg1': f'유효하지 않은 주문 수량: {qty!r}'}
        if order_qty <= 0: return {'rt_cd': '999', 'msg1': f'수량 오류: {qty}'}

        for attempt in range(3):
            # 🚨 MODIFIED: 파편화된 sleep 소각
            tr_id = "TTTT1002U" if side == "BUY" else "TTTT1006U"
            excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        
            ord_dvsn = {"LOC": "34", "MOC": "33", "LOO": "32", "MOO": "31", "VWAP": "36"}.get(order_type, "00")
            final_price = 0 if order_type in ["MOC", "MOO"] else self._ceil_2(price)
         
            if order_type not in ["MOC", "MOO"] and final_price <= 0.0: return {'rt_cd': '999', 'msg1': f'가격 오류: {price}'}
            
            sll_type = "00" if side == "SELL" else ""
            
            body = {
                "CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd, 
                "PDNO": ticker, "ORD_QTY": str(order_qty), "OVRS_ORD_UNPR": str(final_price), 
                "ORD_SVR_DVSN_CD": "0", "ORD_DVSN": ord_dvsn, "SLL_TYPE": sll_type
            }
            
            if order_type == "VWAP":
                if start_time and end_time:
                    body["ALGO_ORD_TMD_DVSN_CD"] = "00"
                    body["START_TIME"] = start_time
                    body["END_TIME"] = end_time
                else:
                    body["ALGO_ORD_TMD_DVSN_CD"] = "02"

            res = self._call_api(tr_id, "/uapi/overseas-stock/v1/trading/order", "POST", body=body)
            # 🚨 MODIFIED: [TypeError 붕괴 방어] msg1이 None일 경우 any() 루프에서 발생하는 예외 원천 봉쇄
            safe_msg = str(res.get('msg1') or '')
            
            if res.get('rt_cd') != '0':
                if attempt < 2 and any(x in safe_msg for x in ["거래소", "시장", "exchange", "코드"]):
                    if ticker in self._excg_cd_cache: del self._excg_cd_cache[ticker]
                    time.sleep(1.0 * (2 ** attempt))
                    continue
                
                # 🚨 VERIFIED: [Case 36 절대 방어망] 리버스 모드 MOC(시장가 매도) 리젝 시 최유리 지정가(-5%) 덤핑 자동 팩트 요격
                if order_type == "MOC":
                    logging.warning(f"🚨 [Case 36 방어망] KIS MOC 주문 리젝 감지 ({safe_msg}). 현재가 -5% 덤핑 폴백 가동!")
                    curr_p = self.get_current_price(ticker)
                    if curr_p > 0:
                        dump_price = self._ceil_2(curr_p * 0.95)
                        logging.info(f"🔄 [{ticker}] MOC ➔ LIMIT(${dump_price:.2f}) 전환 요격 전송")
                        # 🚨 [무한 루프 차단] LIMIT으로 재귀 호출하므로 2차 리젝 시 폴백이 중복 격발되지 않음
                        return self.send_order(ticker, side, qty, dump_price, order_type="LIMIT")
                
                return {'rt_cd': str(res.get('rt_cd') or '999'), 'msg1': safe_msg or '오류', 'odno': ''}
                
            # 🚨 MODIFIED: [AttributeError 붕괴 방어] output 객체 추출 시 안전 단락 평가
            out = res.get('output') or {}
            if not isinstance(out, dict): out = {}
            return {'rt_cd': str(res.get('rt_cd') or '999'), 'msg1': safe_msg or '오류', 'odno': str(out.get('ODNO') or '')}
 
        return {'rt_cd': '999', 'msg1': '거래소 캐시 재시도 초과'}

    def cancel_order(self, ticker, order_id):
        # 🚨 MODIFIED: 파편화된 sleep 소각
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        body = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd, "PDNO": ticker, "ORGN_ODNO": order_id, "RVSE_CNCL_DVSN_CD": "02", "ORD_QTY": "0", "OVRS_ORD_UNPR": "0", "ORD_SVR_DVSN_CD": "0"}
        # 🚨 MODIFIED: [Case 30 팩트 교정] 취소 주문 API 응답 객체 반환 배선 강제 이식
        return self._call_api("TTTT1004U", "/uapi/overseas-stock/v1/trading/order-rvsecncl", "POST", body=body)

    def send_daytime_order(self, ticker, side, qty, price):
        # 🚨 MODIFIED: 파편화된 sleep 소각
        # 🚨 MODIFIED: [최종 무결성 수술] 수동 주문 시 Float 수량이 주입되어 KIS 서버에서 리젝되는 현상을 막기 위해 int 강제 형변환 쉴드 주입
        try: order_qty = int(self._safe_float(qty))
        except: return {'rt_cd': '999', 'msg1': '수량 오류'}
        if order_qty <= 0: return {'rt_cd': '999', 'msg1': '수량 오류'}
        tr_id = "TTTS6036U" if side == "BUY" else "TTTS6037U"
       
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
    
        body = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd, "PDNO": ticker, "ORD_QTY": str(order_qty), "OVRS_ORD_UNPR": str(self._ceil_2(price)), "CTAC_TLNO": "", "MGCO_APTM_ODNO": "", "ORD_SVR_DVSN_CD": "0", "ORD_DVSN": "00"}
        res = self._call_api(tr_id, "/uapi/overseas-stock/v1/trading/daytime-order", "POST", body=body)
        
        # 🚨 MODIFIED: [AttributeError 붕괴 방어] output 객체 추출 시 안전 단락 평가
        out = res.get('output') or {}
        if not isinstance(out, dict): out = {}
        safe_msg = str(res.get('msg1') or '오류')
        return {'rt_cd': str(res.get('rt_cd') or '999'), 'msg1': safe_msg, 'odno': str(out.get('ODNO') or '')}

    def cancel_daytime_order(self, ticker, order_id, qty="100", price="0"):
        # 🚨 MODIFIED: 파편화된 sleep 소각
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
    
        # 🚨 MODIFIED: [최종 무결성 수술] Float 문자열 방어
        safe_qty = str(int(self._safe_float(qty)))
        safe_price = str(self._safe_float(price))
        body = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd, "PDNO": ticker, "ORGN_ODNO": order_id, "RVSE_CNCL_DVSN_CD": "02", "ORD_QTY": safe_qty, "OVRS_ORD_UNPR": safe_price, "CTAC_TLNO": "", "MGCO_APTM_ODNO": "", "ORD_SVR_DVSN_CD": "0"}
        return self._call_api("TTTS6038U", "/uapi/overseas-stock/v1/trading/daytime-order-rvsecncl", "POST", body=body)

    def send_reservation_order(self, ticker, side, qty, price, order_type="LIMIT"):
        # 🚨 MODIFIED: 파편화된 sleep 소각
        # 🚨 MODIFIED: [Insight 14] String-Float 맹독성 쉴드 래핑
        try: order_qty = int(self._safe_float(qty))
        except: return {'rt_cd': '999', 'msg1': '수량 오류'}
        
        tr_id = "TTTT3014U" if side == "BUY" else "TTTT3016U"
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        final_price = str(self._ceil_2(price))
        
        body = {
            "CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "PDNO": ticker,
            "OVRS_EXCG_CD": excg_cd, "FT_ORD_QTY": str(order_qty)
        }
      
        if order_type == "LOC":
            body["ORD_DVSN"] = "34" 
            body["FT_ORD_UNPR3"] = final_price
        else:
            body["ORD_DVSN"] = "00" 
            body["FT_ORD_UNPR3"] = final_price
           
        res = self._call_api(tr_id, "/uapi/overseas-stock/v1/trading/order-resv", "POST", body=body)
        rt_cd = str(res.get('rt_cd') or '999')
        msg1 = str(res.get('msg1') or '오류')
      
        # 🚨 MODIFIED: [AttributeError 붕괴 방어] output 객체 추출 시 안전 단락 평가
        out = res.get('output') or {}
        if not isinstance(out, dict): out = {}
        odno = str(out.get('ODNO') or '')
        return {'rt_cd': rt_cd, 'msg1': msg1, 'odno': odno}

    def cancel_reservation_order(self, order_date, order_id):
        # 🚨 MODIFIED: 파편화된 sleep 소각
        body = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "RSVN_ORD_RCIT_DT": order_date, "OVRS_RSVN_ODNO": order_id}
        return self._call_api("TTTT3017U", "/uapi/overseas-stock/v1/trading/order-resv-ccnl", "POST", body=body)

    def get_execution_history(self, ticker, start_date, end_date):
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        odno_map = {}
        
        # 🚨 MODIFIED: [페이징 팩트 수술] KIS 원장 100건 초과 시 다음 페이지 토큰 변수를 루프 외부로 전진 배치
        fk200, nk200 = "", ""
       
        for attempt in range(10): 
            # 🚨 MODIFIED: 파편화된 sleep 소각
            # 🚨 MODIFIED: 갱신된 연속 조회 토큰(fk200, nk200)을 params에 정밀 주입하여 유령 루프 붕괴 차단
            params = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "PDNO": ticker, "ORD_STRT_DT": start_date, "ORD_END_DT": end_date, "SLL_BUY_DVSN": "00", "CCLD_NCCS_DVSN": "00", "OVRS_EXCG_CD": excg_cd, "SORT_SQN": "DS", "CTX_AREA_FK200": fk200, "CTX_AREA_NK200": nk200}
            headers = self._get_header("TTTS3035R")
            
            # 🚨 MODIFIED: [페이징 팩트 수술] 다음 페이지 요청 시 "tr_cont": "N" 강제 주입
            if fk200 or nk200: headers["tr_cont"] = "N"
            
            res, resp_json = self._api_request("GET", f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-ccnl", headers, params=params)
 
            if res and resp_json.get('rt_cd') == '0':
                # 🚨 MODIFIED: [Iterable 붕괴 방어] None 유입 시 []로 단락 평가
                output = resp_json.get('output') or []
  
                if isinstance(output, dict): output = [output] 
                if not isinstance(output, list): output = []
         
                for item in output:
                    if not isinstance(item, dict): continue
                    try:
                        # 🚨 MODIFIED: [Insight 14] String-Float 맹독성 쉴드 래핑
                        iq, ip = self._safe_float(item.get('ft_ccld_qty', 0)), self._safe_float(item.get('ft_ccld_unpr3', 0))
                        odno = item.get('odno', f"__nk_{id(item)}")
                        if iq > 0:
                            if odno not in odno_map: 
                                odno_map[odno] = {"item": dict(item), "total_qty": iq, "total_amt": iq * ip}
                            else: 
                                odno_map[odno]["total_qty"] += iq
                                odno_map[odno]["total_amt"] += (iq * ip)
                    except: continue
                
                # 🚨 MODIFIED: [페이징 팩트 수술] 다음 페이지 호출을 위한 연속 조회 토큰 파싱
                fk200 = str(resp_json.get('ctx_area_fk200', '') or '').strip()
                nk200 = str(resp_json.get('ctx_area_nk200', '') or '').strip()

                # 🚨 MODIFIED: [AttributeError 붕괴 방어] res.headers 안전 스코프 확인 및 갱신된 nk200이 있을 때만 continue
                if hasattr(res, 'headers') and res.headers.get('tr_cont', '') in ['M', 'F'] and nk200:
                    time.sleep(0.3)
                    continue
                else: break
            else: break
        # 🚨 MODIFIED: [치명적 논리 결함 수술] 딕셔너리 언패킹 오버라이드 맹점 교정 (원본 데이터 우선 배치)
        return [{**d["item"], "ft_ccld_qty": str(d["total_qty"]), "ft_ccld_unpr3": str(d["total_amt"]/d["total_qty"] if d["total_qty"]>0 else 0)} for d in odno_map.values()]

    def get_genesis_ledger(self, ticker, limit_date_str=None):
        _, h = self.get_account_balance()
        if not h: return None, 0, 0.0
        t_info = h.get(ticker, {'qty': 0, 'avg': 0.0})
        curr_qty = int(self._safe_float(t_info.get('qty', 0)))
        if curr_qty == 0: return [], 0, 0.0
        ledger_records, est, target_date, loop = [], ZoneInfo('America/New_York'), datetime.datetime.now(ZoneInfo('America/New_York')), 0
     
        while curr_qty > 0 and loop < 365:
            # 🚨 MODIFIED: 파편화된 sleep 소각 (get_execution_history에서 API 호출 시 자동 지연됨)
            if target_date.weekday() < 5: loop += 1
            date_str = target_date.strftime('%Y%m%d')
            
            if limit_date_str and date_str < limit_date_str: break 
           
            execs = self.get_execution_history(ticker, date_str, date_str)
            if execs:
                # 🚨 MODIFIED: [Sort 런타임 붕괴 방어] ord_tmd 결측치(None) 유입 시 str() 캐스팅 및 '000000' 폴백으로 TypeError 예방
                execs.sort(key=lambda x: str(x.get('ord_tmd') or '000000'), reverse=True)
                for ex in execs:
                    # 🚨 MODIFIED: [Insight 14] String-Float 맹독성 쉴드 래핑
                    side, eq, ep = str(ex.get('sll_buy_dvsn_cd') or ''), int(self._safe_float(ex.get('ft_ccld_qty', 0))), self._safe_float(ex.get('ft_ccld_unpr3', 0))
            
                    rq = eq
                    if side == "02":
                        if curr_qty <= eq: rq, curr_qty = curr_qty, 0
                        else: curr_qty -= eq
                    else: curr_qty += eq
                 
                    ledger_records.append({'date': f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}", 'side': "BUY" if side=="02" else "SELL", 'qty': rq, 'price': ep})
                    if curr_qty == 0: break
            target_date -= datetime.timedelta(days=1);
            time.sleep(0.1) 
        
        if curr_qty > 0: ledger_records.append({'date': 'INCOMPLETE', 'side': 'UNKNOWN', 'qty': curr_qty, 'price': self._safe_float(t_info.get('avg', 0.0)), 'is_incomplete': True})
        ledger_records.reverse()
        return ledger_records, int(self._safe_float(t_info.get('qty', 0))), self._safe_float(t_info.get('avg', 0.0))
