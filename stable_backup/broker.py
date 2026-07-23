# ==========================================================
# FILE: broker.py
# ==========================================================
# 🚨 MODIFIED: [파사드 패턴 4단계 최종] 기존 시스템과의 완벽한 하위 호환성을 보장하는 진입점(Facade)
# 🚨 MODIFIED: Linear Inheritance(선형 상속)로 조립되어 다중 상속(MRO) 충돌 문제를 원천 봉쇄
# 🚨 MODIFIED: main.py, telegram_bot.py, strategy.py 등 외부 모듈의 수정 없이 즉시 작동 보장
# 🚨 MODIFIED: [MRO 붕괴 수술] 암묵적 상속(pass)으로 인한 TypeError 런타임 즉사 버그를 막기 위해, 명시적 __init__ 선언 및 super() 패싱 100% 락온.
# ==========================================================

from kis_order_engine import KisOrderEngine

class KoreaInvestmentBroker(KisOrderEngine):
    """
    🚨 파사드(Facade) 패턴 랩퍼 클래스
    기존 거대 클래스(God Object)였던 KoreaInvestmentBroker의 모든 인터페이스 시그니처를 
    100% 동일하게 유지하여, 시스템 내 다른 코드가 파괴되지 않고 안전하게 연결되도록 합니다.
    """
    
    # 🚨 MODIFIED: [MRO 붕괴 수술] 4개의 식별 인자를 동적으로 받아 최상단 코어로 정확히 라우팅
    def __init__(self, app_key, app_secret, cano, acnt_prdt_cd="01"):
        super().__init__(app_key, app_secret, cano, acnt_prdt_cd)
