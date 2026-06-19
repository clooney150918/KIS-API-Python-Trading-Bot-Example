# 법무법인 AI 에이전트 - 설치 가이드

## 사전 준비

1. **Google OAuth 설정**
   - [Google Cloud Console](https://console.cloud.google.com) 접속
   - 새 프로젝트 생성 → APIs & Services → OAuth 2.0 Client ID 생성
   - 앱 유형: Web application
   - 승인된 리디렉션 URI: `https://your-domain.com/api/auth/callback`
   - Client ID, Client Secret 복사

2. **Contabo VPS에 Docker 설치**
   ```bash
   curl -fsSL https://get.docker.com | bash
   apt install docker-compose-plugin -y
   ```

## 설치

```bash
# 1. 프로젝트 클론
git clone <repo-url>
cd law-firm-ai

# 2. 환경변수 설정
cp .env.example .env
nano .env   # 실제 값으로 채우기

# 3. 실행
docker compose up -d

# 4. 첫 실행 확인
docker compose logs -f backend
```

## .env 설정 항목

| 항목 | 설명 | 예시 |
|------|------|------|
| `POSTGRES_PASSWORD` | DB 비밀번호 | `StrongPass123!` |
| `SECRET_KEY` | JWT 서명 키 (32자 이상) | `openssl rand -hex 32` 출력값 |
| `GOOGLE_CLIENT_ID` | Google OAuth Client ID | `xxx.apps.googleusercontent.com` |
| `GOOGLE_CLIENT_SECRET` | Google OAuth Secret | `GOCSPX-xxx` |
| `ALLOWED_IPS` | 허용 회사 IP (CIDR, 콤마 구분) | `203.0.113.0/24` |
| `FRONTEND_URL` | 서버 도메인 | `https://ai.yourfirm.com` |

## 첫 관리자 계정

- 최초로 Google 로그인한 계정이 자동으로 관리자가 됩니다.
- 이후 직원들이 로그인하면 관리자 페이지(`/admin`)에서 승인해야 사용 가능합니다.

## IP 설정

`ALLOWED_IPS` 환경변수 또는 관리자 페이지 → IP 관리에서 추가:
- 단일 IP: `203.0.113.5/32`
- 대역: `192.168.1.0/24`
- 빈값이면 모든 IP 허용 (개발용)

## SSL 설정 (Let's Encrypt)

```bash
apt install certbot
certbot certonly --standalone -d ai.yourfirm.com
# 인증서: /etc/letsencrypt/live/ai.yourfirm.com/
cp /etc/letsencrypt/live/ai.yourfirm.com/fullchain.pem nginx/ssl/
cp /etc/letsencrypt/live/ai.yourfirm.com/privkey.pem nginx/ssl/
# nginx/nginx.conf의 HTTPS 섹션 주석 해제 후 재시작
docker compose restart nginx
```
