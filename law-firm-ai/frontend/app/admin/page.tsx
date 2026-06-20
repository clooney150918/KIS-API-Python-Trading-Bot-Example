"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";

interface Employee {
  id: string;
  email: string;
  name: string | null;
  job_title: string | null;
  is_admin: boolean;
  is_approved: boolean;
  message_count: number;
  created_at: string;
  last_seen_at: string | null;
}

interface AllowedIP {
  id: string;
  cidr: string;
  label: string;
  created_at: string;
}

type Tab = "employees" | "ips";

export default function AdminPage() {
  const router = useRouter();
  const [tab, setTab] = useState<Tab>("employees");
  const [employees, setEmployees] = useState<Employee[]>([]);
  const [ips, setIps] = useState<AllowedIP[]>([]);
  const [newCidr, setNewCidr] = useState("");
  const [newLabel, setNewLabel] = useState("");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/auth/me", { credentials: "include" })
      .then((r) => r.json())
      .then((data) => {
        if (!data.authenticated || !data.is_admin) router.replace("/dashboard");
      });

    loadData();
  }, [router]);

  const loadData = async () => {
    setLoading(true);
    const [empRes, ipRes] = await Promise.all([
      fetch("/api/admin/users", { credentials: "include" }),
      fetch("/api/admin/ips", { credentials: "include" }),
    ]);
    if (empRes.ok) setEmployees(await empRes.json());
    if (ipRes.ok) setIps(await ipRes.json());
    setLoading(false);
  };

  const approve = async (id: string) => {
    await fetch(`/api/admin/users/${id}/approve`, { method: "PATCH", credentials: "include" });
    loadData();
  };

  const block = async (id: string) => {
    await fetch(`/api/admin/users/${id}/block`, { method: "PATCH", credentials: "include" });
    loadData();
  };

  const resetMemory = async (id: string, name: string | null) => {
    if (!confirm(`${name || "이 직원"}의 AI 기억을 초기화하시겠습니까?`)) return;
    await fetch(`/api/admin/users/${id}/memory`, { method: "DELETE", credentials: "include" });
    loadData();
  };

  const addIP = async () => {
    if (!newCidr.trim()) return;
    const res = await fetch("/api/admin/ips", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      body: JSON.stringify({ cidr: newCidr.trim(), label: newLabel.trim() }),
    });
    if (res.ok) {
      setNewCidr("");
      setNewLabel("");
      loadData();
    } else {
      const err = await res.json();
      alert(err.detail || "오류가 발생했습니다.");
    }
  };

  const deleteIP = async (id: string) => {
    await fetch(`/api/admin/ips/${id}`, { method: "DELETE", credentials: "include" });
    loadData();
  };

  const pending = employees.filter((e) => !e.is_approved);
  const approved = employees.filter((e) => e.is_approved);

  return (
    <div className="min-h-screen bg-surface-0">
      {/* 헤더 */}
      <header className="bg-surface-1 border-b border-surface-border px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <span className="text-xl">⚖️</span>
          <h1 className="text-white font-bold">법무법인 AI · 관리자</h1>
        </div>
        <button
          onClick={() => router.push("/dashboard")}
          className="text-gray-400 hover:text-white text-sm transition-colors"
        >
          ← 대시보드로
        </button>
      </header>

      <div className="max-w-5xl mx-auto px-4 py-8">
        {/* 탭 */}
        <div className="flex gap-1 bg-surface-1 border border-surface-border rounded-xl p-1 mb-6 w-fit">
          {(["employees", "ips"] as Tab[]).map((t) => (
            <button
              key={t}
              onClick={() => setTab(t)}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                tab === t ? "bg-accent text-white" : "text-gray-400 hover:text-white"
              }`}
            >
              {t === "employees" ? `직원 관리 (${employees.length})` : `IP 관리 (${ips.length})`}
            </button>
          ))}
        </div>

        {loading ? (
          <div className="flex justify-center py-20">
            <div className="w-6 h-6 border-2 border-accent-light border-t-transparent rounded-full animate-spin" />
          </div>
        ) : tab === "employees" ? (
          <div className="space-y-6">
            {/* 승인 대기 */}
            {pending.length > 0 && (
              <div>
                <h2 className="text-amber-400 text-sm font-medium mb-3 flex items-center gap-2">
                  <span className="w-2 h-2 bg-amber-400 rounded-full" />
                  승인 대기 ({pending.length})
                </h2>
                <div className="space-y-2">
                  {pending.map((emp) => (
                    <EmployeeRow key={emp.id} emp={emp} onApprove={approve} onBlock={block} onReset={resetMemory} />
                  ))}
                </div>
              </div>
            )}

            {/* 활성 직원 */}
            <div>
              <h2 className="text-gray-400 text-sm font-medium mb-3">활성 직원 ({approved.length})</h2>
              <div className="space-y-2">
                {approved.map((emp) => (
                  <EmployeeRow key={emp.id} emp={emp} onApprove={approve} onBlock={block} onReset={resetMemory} />
                ))}
              </div>
            </div>
          </div>
        ) : (
          <div className="space-y-4">
            {/* IP 추가 */}
            <div className="bg-surface-1 border border-surface-border rounded-xl p-4 flex gap-3">
              <input
                value={newCidr}
                onChange={(e) => setNewCidr(e.target.value)}
                placeholder="예: 192.168.1.0/24 또는 203.0.113.5/32"
                className="flex-1 bg-surface-2 border border-surface-border text-white rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-accent placeholder-gray-600"
              />
              <input
                value={newLabel}
                onChange={(e) => setNewLabel(e.target.value)}
                placeholder="설명 (선택)"
                className="w-40 bg-surface-2 border border-surface-border text-white rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-accent placeholder-gray-600"
              />
              <button
                onClick={addIP}
                className="bg-accent hover:bg-accent/90 text-white px-4 py-2 rounded-lg text-sm font-medium transition-colors"
              >
                추가
              </button>
            </div>

            {/* IP 목록 */}
            <div className="space-y-2">
              {ips.length === 0 && (
                <p className="text-gray-600 text-sm text-center py-8">
                  등록된 IP가 없습니다. IP를 추가하면 해당 IP에서만 접근 가능합니다.
                </p>
              )}
              {ips.map((ip) => (
                <div
                  key={ip.id}
                  className="bg-surface-1 border border-surface-border rounded-xl px-4 py-3 flex items-center justify-between"
                >
                  <div>
                    <span className="text-white font-mono text-sm">{ip.cidr}</span>
                    {ip.label && <span className="text-gray-500 text-sm ml-3">{ip.label}</span>}
                  </div>
                  <button
                    onClick={() => deleteIP(ip.id)}
                    className="text-red-500 hover:text-red-400 text-sm transition-colors"
                  >
                    삭제
                  </button>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function EmployeeRow({
  emp,
  onApprove,
  onBlock,
  onReset,
}: {
  emp: Employee;
  onApprove: (id: string) => void;
  onBlock: (id: string) => void;
  onReset: (id: string, name: string | null) => void;
}) {
  return (
    <div className="bg-surface-1 border border-surface-border rounded-xl px-4 py-3 flex items-center gap-4">
      <div className="w-9 h-9 bg-accent/20 border border-accent/30 rounded-full flex items-center justify-center text-accent-light font-medium text-sm shrink-0">
        {emp.name ? emp.name[0] : emp.email[0].toUpperCase()}
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-white text-sm font-medium truncate">
            {emp.name || "(미설정)"}
          </span>
          {emp.is_admin && (
            <span className="bg-amber-500/20 text-amber-400 text-xs px-1.5 py-0.5 rounded">관리자</span>
          )}
          {!emp.is_approved && (
            <span className="bg-red-500/20 text-red-400 text-xs px-1.5 py-0.5 rounded">대기</span>
          )}
        </div>
        <p className="text-gray-500 text-xs truncate">
          {emp.email} {emp.job_title && `• ${emp.job_title}`} • 메시지 {emp.message_count}건
        </p>
      </div>
      <div className="flex gap-2 shrink-0">
        {!emp.is_approved ? (
          <button
            onClick={() => onApprove(emp.id)}
            className="bg-green-600/20 hover:bg-green-600/30 text-green-400 text-xs px-3 py-1.5 rounded-lg transition-colors"
          >
            승인
          </button>
        ) : (
          <button
            onClick={() => onBlock(emp.id)}
            className="bg-red-600/20 hover:bg-red-600/30 text-red-400 text-xs px-3 py-1.5 rounded-lg transition-colors"
          >
            차단
          </button>
        )}
        <button
          onClick={() => onReset(emp.id, emp.name)}
          className="bg-surface-2 hover:bg-surface-3 text-gray-400 hover:text-white text-xs px-3 py-1.5 rounded-lg transition-colors border border-surface-border"
        >
          기억 초기화
        </button>
      </div>
    </div>
  );
}
