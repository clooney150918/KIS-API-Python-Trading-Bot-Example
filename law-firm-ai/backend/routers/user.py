from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from models.user import User
from database import get_db
from middleware.auth import get_current_user

router = APIRouter(prefix="/api/user")


class ProfileUpdate(BaseModel):
    name: str
    job_title: str


@router.put("/profile")
async def update_profile(
    body: ProfileUpdate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    current_user.name = body.name.strip()
    current_user.job_title = body.job_title.strip()
    db.add(current_user)
    await db.commit()
    return {"ok": True, "name": current_user.name, "job_title": current_user.job_title}
