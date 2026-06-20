"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";

const JOB_TITLES = ["변호사", "파트너 변호사", "사무직", "인턴", "기타"];

export default function SetupPage() {
  const router = useRouter();
  const [name, setName] = useState("");
  const [jobTitle, setJobTitle] = useState("");
  const [customTitle, setCustomTitle] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    const finalTitle = jobTitle === "기타" ? customTitle : jobTitle;
    if (!name.trim() || !finalTitle.trim()) {
      setError("이름과 직책을 모두 입력해 주세요.");
      return;
    }

    setLoading(true);
    try {
      const res = await fetch("/api/user/profile", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({ name: name.trim(), job_title: finalTitle.trim() }),
      });
      if (!res.ok) throw new Error();
      router.replace("/dashboard");
    } catch {
      setError("저장에 실패했습니다. 다시 시도해 주세요.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <main className="min-h-screen bg-surface-0 flex items-center justify-center px-4">
      <div className="w-full max-w-md">
        <div className="text-center mb-8">
          <h1 className="text-2xl font-bold gradient-text mb-2">프로필 설정</h1>
          <p className="text-gray-500 text-sm">AI 에이전트를 개인화하기 위해 정보를 입력해 주세요</p>
        </div>

        <form
          onSubmit={handleSubmit}
          className="bg-surface-1 border border-surface-border rounded-2xl p-8 space-y-6"
        >
          {error && (
            <div className="bg-red-900/30 border border-red-800 text-red-400 rounded-lg px-4 py-3 text-sm">
              {error}
            </div>
          )}

          <div className="space-y-2">
            <label className="block text-sm font-medium text-gray-300">이름</label>
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="홍길동"
              className="w-full bg-surface-2 border border-surface-border text-white rounded-xl px-4 py-3 focus:outline-none focus:border-accent placeholder-gray-600 transition-colors"
            />
          </div>

          <div className="space-y-2">
            <label className="block text-sm font-medium text-gray-300">직책</label>
            <div className="grid grid-cols-2 gap-2">
              {JOB_TITLES.map((t) => (
                <button
                  key={t}
                  type="button"
                  onClick={() => setJobTitle(t)}
                  className={`py-2.5 px-3 rounded-xl border text-sm font-medium transition-all ${
                    jobTitle === t
                      ? "bg-accent/20 border-accent text-accent-light"
                      : "bg-surface-2 border-surface-border text-gray-400 hover:border-gray-600"
                  }`}
                >
                  {t}
                </button>
              ))}
            </div>
            {jobTitle === "기타" && (
              <input
                type="text"
                value={customTitle}
                onChange={(e) => setCustomTitle(e.target.value)}
                placeholder="직접 입력"
                className="w-full bg-surface-2 border border-surface-border text-white rounded-xl px-4 py-3 focus:outline-none focus:border-accent placeholder-gray-600 transition-colors mt-2"
              />
            )}
          </div>

          <button
            type="submit"
            disabled={loading}
            className="w-full bg-accent hover:bg-accent/90 disabled:opacity-50 text-white font-medium py-3 rounded-xl transition-colors flex items-center justify-center gap-2"
          >
            {loading ? (
              <span className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
            ) : (
              "시작하기"
            )}
          </button>
        </form>
      </div>
    </main>
  );
}
