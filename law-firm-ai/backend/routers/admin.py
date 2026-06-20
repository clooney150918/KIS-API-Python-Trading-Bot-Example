import uuid
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, delete
from models.user import User
from models.message import Message, AllowedIP
from database import get_db
from middleware.auth import get_admin_user

router = APIRouter(prefix="/api/admin")


# ── 직원 관리 ──────────────────────────────────────────────
@router.get("/users")
async def list_users(
    _: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(User).order_by(User.created_at.desc()))
    users = result.scalars().all()
    stats = {}
    for u in users:
        cnt = await db.scalar(select(func.count()).where(Message.user_id == u.id))
        stats[u.id] = cnt or 0

    return [
        {
            "id": str(u.id),
            "email": u.email,
            "name": u.name,
            "job_title": u.job_title,
            "is_admin": u.is_admin,
            "is_approved": u.is_approved,
            "message_count": stats[u.id],
            "created_at": u.created_at.isoformat(),
            "last_seen_at": u.last_seen_at.isoformat() if u.last_seen_at else None,
        }
        for u in users
    ]


@router.patch("/users/{user_id}/approve")
async def approve_user(
    user_id: uuid.UUID,
    _: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    user.is_approved = True
    await db.commit()
    return {"ok": True}


@router.patch("/users/{user_id}/block")
async def block_user(
    user_id: uuid.UUID,
    _: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    user.is_approved = False
    await db.commit()
    return {"ok": True}


@router.delete("/users/{user_id}/memory")
async def reset_memory(
    user_id: uuid.UUID,
    _: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """직원의 Hermes 세션 ID 초기화 + 대화 기록 삭제"""
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    user.hermes_session_id = uuid.uuid4()
    await db.execute(delete(Message).where(Message.user_id == user_id))
    await db.commit()
    return {"ok": True}


# ── IP 관리 ──────────────────────────────────────────────
class IPCreate(BaseModel):
    cidr: str
    label: str = ""


@router.get("/ips")
async def list_ips(
    _: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(AllowedIP).order_by(AllowedIP.created_at))
    return [
        {"id": str(ip.id), "cidr": ip.cidr, "label": ip.label, "created_at": ip.created_at.isoformat()}
        for ip in result.scalars().all()
    ]


@router.post("/ips")
async def add_ip(
    body: IPCreate,
    _: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    import ipaddress
    try:
        ipaddress.ip_network(body.cidr, strict=False)
    except ValueError:
        raise HTTPException(status_code=400, detail="유효하지 않은 CIDR 형식입니다.")

    existing = await db.execute(select(AllowedIP).where(AllowedIP.cidr == body.cidr))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="이미 등록된 IP입니다.")

    db.add(AllowedIP(cidr=body.cidr, label=body.label))
    await db.commit()
    return {"ok": True}


@router.delete("/ips/{ip_id}")
async def delete_ip(
    ip_id: uuid.UUID,
    _: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(select(AllowedIP).where(AllowedIP.id == ip_id))
    ip = result.scalar_one_or_none()
    if not ip:
        raise HTTPException(status_code=404, detail="IP를 찾을 수 없습니다.")
    await db.delete(ip)
    await db.commit()
    return {"ok": True}
