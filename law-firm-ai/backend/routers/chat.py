import json
import uuid as _uuid
import httpx
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from models.user import User
from models.message import Message
from database import get_db, AsyncSessionLocal
from middleware.auth import get_current_user
from config import settings

router = APIRouter(prefix="/api/chat")

ROLE_PROMPTS: dict[str, str] = {
    "변호사": (
        "당신은 법무법인 소속 변호사를 전문적으로 보조하는 AI 에이전트입니다. "
        "법률 문서 검토, 판례 분석, 법령 해석, 의견서 작성 등을 지원합니다. "
        "항상 전문적이고 정확한 법률 정보를 제공하며, 불확실한 사항은 명확히 표시합니다."
    ),
    "파트너 변호사": (
        "당신은 법무법인 파트너 변호사를 보조하는 AI 에이전트입니다. "
        "고난도 법률 전략 수립, 주요 사건 분석, 클라이언트 대응 전략, "
        "팀 업무 조율 등 파트너급 업무를 지원합니다."
    ),
    "사무직": (
        "당신은 법무법인 사무직 직원을 보조하는 AI 에이전트입니다. "
        "문서 관리, 일정 조율, 보고서 작성, 법원 서류 접수 절차 등 "
        "사무 업무를 효율적으로 지원합니다."
    ),
    "인턴": (
        "당신은 법무법인 인턴을 보조하는 AI 에이전트입니다. "
        "법률 기초 학습, 문서 작성 가이드, 업무 프로세스 안내 등을 지원합니다. "
        "배움의 과정에 있는 인턴이 성장할 수 있도록 친절하게 도움을 드립니다."
    ),
    "default": (
        "당신은 법무법인 직원을 보조하는 AI 에이전트입니다. "
        "법률 업무 전반에 걸쳐 전문적인 지원을 제공합니다."
    ),
}


def _get_system_prompt(job_title: str | None) -> str:
    if not job_title:
        return ROLE_PROMPTS["default"]
    return ROLE_PROMPTS.get(job_title, ROLE_PROMPTS["default"])


class ChatRequest(BaseModel):
    message: str


@router.get("/history")
async def get_history(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Message)
        .where(Message.user_id == current_user.id)
        .order_by(Message.created_at)
    )
    messages = result.scalars().all()
    return [
        {"role": m.role, "content": m.content, "created_at": m.created_at.isoformat()}
        for m in messages
    ]


@router.post("/send")
async def send_message(
    body: ChatRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if not body.message.strip():
        raise HTTPException(status_code=400, detail="메시지를 입력해 주세요.")

    # 사용자 메시지 저장
    db.add(Message(user_id=current_user.id, role="user", content=body.message))
    await db.commit()

    # 최근 대화 히스토리 (최대 50개)
    result = await db.execute(
        select(Message)
        .where(Message.user_id == current_user.id)
        .order_by(Message.created_at.desc())
        .limit(50)
    )
    history = list(reversed(result.scalars().all()))

    payload_messages = [{"role": "system", "content": _get_system_prompt(current_user.job_title)}]
    payload_messages += [{"role": m.role, "content": m.content} for m in history]

    # 스트리밍 중 DB 세션이 닫히므로 필요한 값을 미리 추출
    user_id: _uuid.UUID = current_user.id
    session_id: str = str(current_user.hermes_session_id)

    async def stream_response():
        assistant_parts: list[str] = []
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                async with client.stream(
                    "POST",
                    f"{settings.hermes_base_url}/v1/chat/completions",
                    json={"model": "hermes", "messages": payload_messages, "stream": True},
                    headers={"X-Hermes-Session-Id": session_id},
                ) as response:
                    async for line in response.aiter_lines():
                        if not line.startswith("data: "):
                            continue
                        data = line[6:]
                        if data == "[DONE]":
                            break
                        try:
                            chunk = json.loads(data)
                            delta = chunk["choices"][0]["delta"].get("content", "")
                            if delta:
                                assistant_parts.append(delta)
                                yield f"data: {json.dumps({'content': delta})}\n\n"
                        except (json.JSONDecodeError, KeyError):
                            continue
        finally:
            if assistant_parts:
                full_content = "".join(assistant_parts)
                # 스트리밍 완료 후 새 세션으로 저장
                async with AsyncSessionLocal() as save_db:
                    save_db.add(Message(user_id=user_id, role="assistant", content=full_content))
                    await save_db.commit()

    return StreamingResponse(stream_response(), media_type="text/event-stream")
