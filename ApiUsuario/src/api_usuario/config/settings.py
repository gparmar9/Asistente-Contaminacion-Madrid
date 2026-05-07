from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    app_name: str
    app_env: str
    api_host: str
    api_port: int
    api_version: str


def get_settings() -> Settings:
    return Settings(
        app_name=os.getenv("APP_NAME", "ApiUsuario"),
        app_env=os.getenv("APP_ENV", "development"),
        api_host=os.getenv("API_HOST", "0.0.0.0"),
        api_port=int(os.getenv("API_PORT", "8000")),
        api_version=os.getenv("API_VERSION", "1.0.0"),
    )
