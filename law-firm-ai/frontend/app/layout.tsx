import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "법무법인 AI 어시스턴트",
  description: "직원 전용 AI 에이전트 플랫폼",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="ko">
      <body>{children}</body>
    </html>
  );
}
