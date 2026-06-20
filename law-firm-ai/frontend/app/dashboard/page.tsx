"use client";

import { useEffect, useRef, useState, useCallback } from "react";
import { useRouter } from "next/navigation";
import ReactMarkdown from "react-markdown";

interface Message {
  role: "user" | "assistant";
  content: string;
  created_at?: string;
}

interface User {
  name: string;
  email: string;
  job_title: string;
  is_admin: boolean;
}

export default function DashboardPage() {
  const router = useRouter();
  const [user, setUser] = useState<User | null>(null);
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [streaming, setStreaming] = useState(false);
  const bottomRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    fetch("/api/auth/me", { credentials: "include" })
      .then((r) => r.json())
      .then((data) => {
        if (!data.authenticated) return router.replace("/");
        if (!data.setup_complete) return router.replace("/setup");
        setUser(data);
      });

    fetch("/api/chat/history", { credentials: "include" })
      .then((r) => r.json())
      .then((data) => Array.isArray(data) && setMessages(data));
  }, [router]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const sendMessage = useCallback(async () => {
    const text = input.trim();
    if (!text || streaming) return;

    setInput("");
    setMessages((prev) => [...prev, { role: "user", content: text }]);
    setStreaming(true);

    const assistantMsg: Message = { role: "assistant", content: "" };
    setMessages((prev) => [...prev, assistantMsg]);

    try {
      const res = await fetch("/api/chat/send", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({ message: text }),
      });

      if (!res.body) throw new Error("No body");
      const reader = res.body.getReader();
      const decoder = new TextDecoder();

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        const chunk = decoder.decode(value);
        for (const line of chunk.split("\n")) {
          if (!line.startsWith("data: ")) continue;
          try {
            const { content } = JSON.parse(line.slice(6));
            setMessages((prev) => {
              const next = [...prev];
              next[next.length - 1] = {
                ...next[next.length - 1],
                content: next[next.length - 1].content + content,
              };
              return next;
            });
          } catch {}
        }
      }
    } catch {
      setMessages((prev) => {
        const next = [...prev];
        next[next.length - 1] = {
          ...next[next.length - 1],
          content: "오류가 발생했습니다. 다시 시도해 주세요.",
        };
        return next;
      });
    } finally {
      setStreaming(false);
    }
  }, [input, streaming]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  };

  const handleLogout = async () => {
    await fetch("/api/auth/logout", { method: "POST", credentials: "include" });
    router.replace("/");
  };

  if (!user) {
    return (
      <div className="min-h-screen bg-surface-0 flex items-center justify-center">
        <div className="w-6 h-6 border-2 border-accent-light border-t-transparent rounded-full animate-spin" />
      </div>
    );
  }

  return (
    <div className="flex h-screen bg-surface-0 overflow-hidden">
      {/* 사이드바 */}
      <aside className="w-64 bg-surface-1 border-r border-surface-border flex flex-col shrink-0">
        {/* 로고 */}
        <div className="p-5 border-b border-surface-border">
          <div className="flex items-center gap-2">
            <span className="text-xl">⚖️</span>
            <span className="font-bold gradient-text text-lg">법무법인 AI</span>
          </div>
        </div>

        {/* 새 대화 */}
        <div className="p-3">
          <button
            onClick={() => setMessages([])}
            className="w-full flex items-center gap-2 px-3 py-2.5 rounded-xl text-gray-400 hover:bg-surface-2 hover:text-white transition-colors text-sm"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
            </svg>
            새 대화
          </button>
        </div>

        <div className="flex-1" />

        {/* 하단 사용자 정보 */}
        <div className="p-3 border-t border-surface-border space-y-1">
          {user.is_admin && (
            <button
              onClick={() => router.push("/admin")}
              className="w-full flex items-center gap-2 px-3 py-2 rounded-xl text-gray-400 hover:bg-surface-2 hover:text-white transition-colors text-sm"
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
              </svg>
              관리자
            </button>
          )}
          <div className="px-3 py-2">
            <p className="text-white text-sm font-medium truncate">{user.name}</p>
            <p className="text-gray-500 text-xs truncate">{user.job_title}</p>
          </div>
          <button
            onClick={handleLogout}
            className="w-full flex items-center gap-2 px-3 py-2 rounded-xl text-gray-500 hover:bg-surface-2 hover:text-red-400 transition-colors text-sm"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" />
            </svg>
            로그아웃
          </button>
        </div>
      </aside>

      {/* 메인 채팅 영역 */}
      <main className="flex-1 flex flex-col min-w-0">
        {/* 헤더 */}
        <header className="px-6 py-4 border-b border-surface-border flex items-center justify-between shrink-0">
          <div>
            <h2 className="text-white font-medium">{user.name}님의 AI 에이전트</h2>
            <p className="text-gray-500 text-xs mt-0.5">Hermes Agent • {user.job_title} 특화</p>
          </div>
          <div className="flex items-center gap-2">
            <span className="w-2 h-2 bg-green-500 rounded-full" />
            <span className="text-gray-500 text-xs">연결됨</span>
          </div>
        </header>

        {/* 메시지 목록 */}
        <div className="flex-1 overflow-y-auto px-4 py-6 space-y-6">
          {messages.length === 0 && (
            <div className="flex flex-col items-center justify-center h-full text-center">
              <div className="text-5xl mb-4">⚖️</div>
              <h3 className="text-white text-xl font-semibold mb-2">안녕하세요, {user.name}님!</h3>
              <p className="text-gray-500 text-sm max-w-md">
                {user.job_title} 업무를 위한 전용 AI 에이전트입니다.
                <br />
                법률 문서, 판례 분석, 업무 자동화 등 무엇이든 물어보세요.
              </p>
            </div>
          )}

          {messages.map((msg, i) => (
            <div
              key={i}
              className={`flex gap-4 ${msg.role === "user" ? "flex-row-reverse" : ""}`}
            >
              {/* 아바타 */}
              <div
                className={`w-8 h-8 rounded-full shrink-0 flex items-center justify-center text-sm font-medium ${
                  msg.role === "user"
                    ? "bg-accent text-white"
                    : "bg-surface-2 border border-surface-border text-gray-300"
                }`}
              >
                {msg.role === "user" ? user.name[0] : "H"}
              </div>

              {/* 메시지 버블 */}
              <div
                className={`max-w-[70%] rounded-2xl px-5 py-3 text-sm leading-relaxed ${
                  msg.role === "user"
                    ? "bg-accent text-white rounded-tr-sm"
                    : "bg-surface-1 border border-surface-border text-gray-200 rounded-tl-sm"
                }`}
              >
                {msg.role === "assistant" ? (
                  <ReactMarkdown
                    components={{
                      p: ({ children }) => <p className="mb-2 last:mb-0">{children}</p>,
                      code: ({ children }) => (
                        <code className="bg-surface-3 px-1.5 py-0.5 rounded text-accent-light font-mono text-xs">
                          {children}
                        </code>
                      ),
                      pre: ({ children }) => (
                        <pre className="bg-surface-0 border border-surface-border rounded-xl p-4 overflow-x-auto my-3 text-xs font-mono">
                          {children}
                        </pre>
                      ),
                    }}
                  >
                    {msg.content || (streaming && i === messages.length - 1 ? "▋" : "")}
                  </ReactMarkdown>
                ) : (
                  msg.content
                )}
              </div>
            </div>
          ))}
          <div ref={bottomRef} />
        </div>

        {/* 입력창 */}
        <div className="px-4 pb-6 shrink-0">
          <div className="max-w-3xl mx-auto">
            <div className="bg-surface-1 border border-surface-border rounded-2xl flex items-end gap-3 px-4 py-3 focus-within:border-accent/50 transition-colors">
              <textarea
                ref={textareaRef}
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="메시지를 입력하세요... (Enter로 전송, Shift+Enter로 줄바꿈)"
                rows={1}
                disabled={streaming}
                className="flex-1 bg-transparent text-white placeholder-gray-600 resize-none outline-none text-sm leading-6 max-h-40 disabled:opacity-50"
                style={{ minHeight: "24px" }}
              />
              <button
                onClick={sendMessage}
                disabled={!input.trim() || streaming}
                className="w-9 h-9 bg-accent hover:bg-accent/90 disabled:opacity-30 disabled:cursor-not-allowed rounded-xl flex items-center justify-center transition-all shrink-0"
              >
                {streaming ? (
                  <span className="w-3.5 h-3.5 border-2 border-white border-t-transparent rounded-full animate-spin" />
                ) : (
                  <svg className="w-4 h-4 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 12h14M12 5l7 7-7 7" />
                  </svg>
                )}
              </button>
            </div>
            <p className="text-center text-xs text-gray-700 mt-2">
              AI 에이전트의 답변은 참고용입니다. 중요한 법적 판단은 담당 변호사에게 확인하세요.
            </p>
          </div>
        </div>
      </main>
    </div>
  );
}
