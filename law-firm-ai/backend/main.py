from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from database import init_db
from middleware.ip_filter import IPFilterMiddleware
from routers import auth, user, chat, admin
from config import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(title="Law Firm AI", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_url],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(IPFilterMiddleware)

app.include_router(auth.router)
app.include_router(user.router)
app.include_router(chat.router)
app.include_router(admin.router)


@app.get("/api/health")
async def health():
    return {"status": "ok"}
