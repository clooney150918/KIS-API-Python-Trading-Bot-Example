import uuid
from datetime import datetime, timezone
from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse, JSONResponse
from authlib.integrations.httpx_client import AsyncOAuth2Client
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from fastapi import Depends
from models.user import User
from database import get_db
from middleware.auth import create_access_token
from config import settings

router = APIRouter(prefix="/api/auth")

GOOGLE_AUTHORIZE_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"


def _oauth_client() -> AsyncOAuth2Client:
    return AsyncOAuth2Client(
        client_id=settings.google_client_id,
        client_secret=settings.google_client_secret,
        redirect_uri=f"{settings.frontend_url}/api/auth/callback",
    )


@router.get("/login")
async def login():
    async with _oauth_client() as client:
        url, state = client.create_authorization_url(
            GOOGLE_AUTHORIZE_URL, scope="openid email profile"
        )
    response = RedirectResponse(url)
    response.set_cookie("oauth_state", state, httponly=True, samesite="lax", max_age=300)
    return response


@router.get("/callback")
async def callback(request: Request, db: AsyncSession = Depends(get_db)):
    code = request.query_params.get("code")
    if not code:
        return RedirectResponse(f"{settings.frontend_url}/?error=oauth_failed")

    async with _oauth_client() as client:
        token = await client.fetch_token(GOOGLE_TOKEN_URL, code=code)
        resp = await client.get(GOOGLE_USERINFO_URL, token=token)

    info = resp.json()
    google_id = info.get("sub")
    email = info.get("email", "")

    result = await db.execute(select(User).where(User.google_id == google_id))
    user = result.scalar_one_or_none()

    if not user:
        from sqlalchemy import func as sqlfunc
        count = await db.scalar(select(sqlfunc.count()).select_from(User))
        is_first = (count == 0)
        user = User(
            google_id=google_id,
            email=email,
            is_admin=is_first,
            is_approved=is_first,
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)

    user.last_seen_at = datetime.now(timezone.utc)
    await db.commit()

    token = create_access_token(user.id)
    redirect = f"{settings.frontend_url}/setup" if not user.name else f"{settings.frontend_url}/dashboard"
    response = RedirectResponse(redirect)
    response.set_cookie("access_token", token, httponly=True, samesite="lax", max_age=60 * 60 * 24 * 7)
    return response


@router.post("/logout")
async def logout():
    response = JSONResponse({"ok": True})
    response.delete_cookie("access_token")
    return response


@router.get("/me")
async def me(request: Request, db: AsyncSession = Depends(get_db)):
    token = request.cookies.get("access_token")
    if not token:
        return JSONResponse({"authenticated": False}, status_code=401)

    try:
        from jose import jwt
        payload = jwt.decode(token, settings.secret_key, algorithms=["HS256"])
        user_id = uuid.UUID(payload["sub"])
    except Exception:
        return JSONResponse({"authenticated": False}, status_code=401)

    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        return JSONResponse({"authenticated": False}, status_code=401)

    return {
        "authenticated": True,
        "id": str(user.id),
        "email": user.email,
        "name": user.name,
        "job_title": user.job_title,
        "is_admin": user.is_admin,
        "is_approved": user.is_approved,
        "setup_complete": bool(user.name and user.job_title),
    }
