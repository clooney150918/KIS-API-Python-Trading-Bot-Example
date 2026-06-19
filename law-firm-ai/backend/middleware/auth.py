import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional
from jose import JWTError, jwt
from fastapi import Depends, HTTPException, Cookie
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from models.user import User
from database import get_db
from config import settings

ALGORITHM = "HS256"
TOKEN_EXPIRE_HOURS = 24 * 7  # 7일


def create_access_token(user_id: uuid.UUID) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=TOKEN_EXPIRE_HOURS)
    return jwt.encode({"sub": str(user_id), "exp": expire}, settings.secret_key, algorithm=ALGORITHM)


async def get_current_user(
    access_token: Optional[str] = Cookie(default=None),
    db: AsyncSession = Depends(get_db),
) -> User:
    if not access_token:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    try:
        payload = jwt.decode(access_token, settings.secret_key, algorithms=[ALGORITHM])
        user_id = uuid.UUID(payload["sub"])
    except (JWTError, ValueError):
        raise HTTPException(status_code=401, detail="유효하지 않은 토큰입니다.")

    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=401, detail="사용자를 찾을 수 없습니다.")
    if not user.is_approved:
        raise HTTPException(status_code=403, detail="관리자 승인이 필요합니다.")
    return user


async def get_admin_user(current_user: User = Depends(get_current_user)) -> User:
    if not current_user.is_admin:
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다.")
    return current_user
