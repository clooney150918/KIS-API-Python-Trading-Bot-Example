from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str
    secret_key: str
    google_client_id: str
    google_client_secret: str
    frontend_url: str = "http://localhost:3000"
    hermes_base_url: str = "http://hermes:11434"
    allowed_ips: str = ""  # 콤마 구분 CIDR 목록

    @property
    def allowed_ip_list(self) -> list[str]:
        if not self.allowed_ips:
            return []
        return [ip.strip() for ip in self.allowed_ips.split(",") if ip.strip()]

    class Config:
        env_file = ".env"


settings = Settings()
