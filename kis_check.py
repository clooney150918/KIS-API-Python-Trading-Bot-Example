import os, sys, json
os.chdir('/app')
sys.path.insert(0, '/app')
from dotenv import load_dotenv
load_dotenv('/app/.env')
from kis.api_client import KisApiClient

app_key = os.getenv('KIS_APP_KEY')
app_secret = os.getenv('KIS_APP_SECRET')
cano = os.getenv('KIS_CANO')
acnt = os.getenv('KIS_ACNT_PRDT_CD', '01')
client = KisApiClient(app_key, app_secret, cano, acnt)

print('=== 미체결 주문 ===')
for excg in ['NASD','NYSE','AMEX']:
    try:
        res = client._api_request(
            'GET',
            '/uapi/overseas-stock/v1/trading/inquire-oder-no-confirm',
            tr_id='CTOS0015R',
            params={
                'CANO': cano,
                'ACNT_PRDT_CD': acnt,
                'OVRS_EXCG_CD': excg,
                'ODNO': '',
            }
        )
        if res.get('rt_cd') == '0':
            outs = res.get('output', [])
            if isinstance(outs, dict):
                outs = [outs] if outs else []
            if outs:
                for o in outs:
                    bd = o.get('sll_buy_dvsn_cd', '')
                    side = '매수' if bd == '02' else '매도'
                    print(f'{o.get("pdno")} {side} {o.get("ord_qty")}주 @ ${o.get("ord_unpr","")} odno={o.get("odno")}')
            else:
                print(f'[{excg}] 미체결 없음')
        else:
            print(f'[{excg}] 오류 rt_cd={res.get("rt_cd")}, msg={res.get("msg1","")}')
    except Exception as e:
        print(f'[{excg}] 예외: {e}')

print()
print('=== 잔고 ===')
bal = client._api_request(
    'GET',
    '/uapi/overseas-stock/v1/trading/inquire-balance',
    tr_id='TTTS3012R',
    params={
        'CANO': cano,
        'ACNT_PRDT_CD': acnt,
        'OVRS_EXCG_CD': 'NASD',
        'TR_CRCY_CD': 'USD',
        'CTX_AREA_FK200': '',
        'CTX_AREA_NK200': '',
    }
)
if bal.get('rt_cd') == '0':
    for item in bal.get('output1', []):
        qty = item.get('ovrs_cblc_qty', '0')
        if int(qty) > 0:
            print(f'{item.get("ovrs_pdno")}: {qty}주, 매수가 ${item.get("pchs_avg_pric","0")}, 현재가 ${item.get("now_pric2","0")}, 평가 ${item.get("ovrs_stck_evlu_amt","0")}')
    o2 = bal.get('output2', {})
    if isinstance(o2, dict):
        for k, v in o2.items():
            print(f'{k}: {v}')
