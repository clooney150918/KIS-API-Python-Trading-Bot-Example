from kis_order_engine import KisOrderEngine


class _HoldingsResponse:
    headers = {}


def test_account_balance_cash_does_not_subtract_pending_buy_amount():
    engine = object.__new__(KisOrderEngine)
    engine.cano = "12345678"
    engine.acnt_prdt_cd = "01"
    engine.base_url = "https://openapi.koreainvestment.com:9443"
    engine._safe_float = lambda value: float(value)
    engine._get_header = lambda tr_id: {"tr_id": tr_id}

    def fake_call_api(tr_id, path, method, params=None):
        assert tr_id == "CTRP6504R"
        return {
            "rt_cd": "0",
            "output2": {
                "frcr_dncl_amt_2": "1491.09",
                "frcr_sll_amt_smtl": "3403.24",
                "frcr_buy_amt_smtl": "1239.98",
            },
        }

    def fake_api_request(method, url, headers, params=None):
        return _HoldingsResponse(), {
            "rt_cd": "0",
            "output1": [],
            "ctx_area_fk200": "",
            "ctx_area_nk200": "",
        }

    engine._call_api = fake_call_api
    engine._api_request = fake_api_request

    cash, holdings = engine.get_account_balance()

    assert cash == 4867.41
    assert cash != 3634.25
    assert holdings == {}
