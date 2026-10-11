from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    app_name: str
    app_env: str
    api_host: str
    api_port: int
    api_version: str
    # Conexión a la base de datos del proyecto (RDS/PostgreSQL o Docker local)
    database_url: str
    # URL base del servicio LLMOrchestrator. Vacía = el chat responde con un stub.
    orchestrator_url: str
    orchestrator_timeout_s: float
    # URL base del servicio Agente. Si está definida, /chat y /chat/stream van al agente
    # en lugar de al orquestador. Vacía = comportamiento anterior (orquestador o stub).
    agente_url: str = ""
    # Timeout de lectura por fragmento: en el stream, el heartbeat del agente lo mantiene vivo.
    agente_timeout_s: float = 120.0


def get_settings() -> Settings:
    return Settings(
        app_name=os.getenv("APP_NAME", "ApiUsuario"),
        app_env=os.getenv("APP_ENV", "development"),
        api_host=os.getenv("API_HOST", "0.0.0.0"),
        api_port=int(os.getenv("API_PORT", "8000")),
        api_version=os.getenv("API_VERSION", "1.0.0"),
        database_url=os.getenv("DATABASE_URL", ""),
        orchestrator_url=os.getenv("ORCHESTRATOR_URL", ""),
        orchestrator_timeout_s=float(os.getenv("ORCHESTRATOR_TIMEOUT_S", "120")),
        agente_url=os.getenv("AGENTE_URL", ""),
        agente_timeout_s=float(os.getenv("AGENTE_TIMEOUT_S", "120")),
    )
