import ipaddress
from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from sqlalchemy import select
from models.message import AllowedIP
from database import AsyncSessionLocal
from config import settings

# IP 체크를 건너뛸 경로 (Google OAuth 콜백 등)
SKIP_IP_CHECK_PATHS = {"/api/auth/callback", "/api/auth/login", "/api/health"}


def _ip_in_cidr(ip: str, cidr: str) -> bool:
    try:
        return ipaddress.ip_address(ip) in ipaddress.ip_network(cidr, strict=False)
    except ValueError:
        return False


class IPFilterMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path in SKIP_IP_CHECK_PATHS:
            return await call_next(request)

        client_ip = request.headers.get("X-Forwarded-For", request.client.host).split(",")[0].strip()

        # 1. 환경변수 IP 목록 확인
        allowed = settings.allowed_ip_list
        if allowed:
            if any(_ip_in_cidr(client_ip, cidr) for cidr in allowed):
                return await call_next(request)

        # 2. DB IP 목록 확인
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(AllowedIP))
            db_ips = result.scalars().all()

        if db_ips:
            if any(_ip_in_cidr(client_ip, row.cidr) for row in db_ips):
                return await call_next(request)

        # 3. 허용된 IP 없으면 통과 (설정 전 개발 모드)
        if not allowed and not db_ips:
            return await call_next(request)

        return JSONResponse(
            status_code=403,
            content={"detail": f"접근이 제한된 IP입니다: {client_ip}. 회사 네트워크에서 접속해 주세요."},
        )
