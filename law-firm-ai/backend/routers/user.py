from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from models.user import User
from database import get_db
from middleware.auth import get_current_user
import uuid
from jose import jwt, JWTError
from fastapi import Request
from config import settings

router = APIRouter(prefix="/api/user")


async def get_any_user(request: Request, db: AsyncSession = Depends(get_db)) -> User:
    """승인 여부와 무관하게 로그인된 사용자 반환 (프로필 설정용)"""
    token = request.cookies.get("access_token")
    if not token:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=["HS256"])
        user_id = uuid.UUID(payload["sub"])
    except (JWTError, ValueError):
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="유효하지 않은 토큰입니다.")

    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="사용자를 찾을 수 없습니다.")
    return user


class ProfileUpdate(BaseModel):
    name: str
    job_title: str


@router.put("/profile")
async def update_profile(
    body: ProfileUpdate,
    current_user: User = Depends(get_any_user),
    db: AsyncSession = Depends(get_db),
):
    current_user.name = body.name.strip()
    current_user.job_title = body.job_title.strip()
    db.add(current_user)
    await db.commit()
    return {"ok": True, "name": current_user.name, "job_title": current_user.job_title}
