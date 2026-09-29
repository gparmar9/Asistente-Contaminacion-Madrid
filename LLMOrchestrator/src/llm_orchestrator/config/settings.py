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
    # Proveedor del LLM: cualquier API OpenAI-compatible (Mistral, Groq, Gemini, Ollama...)
    llm_base_url: str
    llm_api_key: str
    llm_model: str
    llm_timeout_s: float
    # Tope de vueltas del bucle del agente (cada vuelta = 1 llamada al LLM)
    max_iteraciones: int


def get_settings() -> Settings:
    return Settings(
        app_name=os.getenv("APP_NAME", "LLMOrchestrator"),
        app_env=os.getenv("APP_ENV", "development"),
        api_host=os.getenv("API_HOST", "0.0.0.0"),
        api_port=int(os.getenv("API_PORT", "8100")),
        api_version=os.getenv("API_VERSION", "1.0.0"),
        database_url=os.getenv("DATABASE_URL", ""),
        llm_base_url=os.getenv("LLM_BASE_URL", ""),
        llm_api_key=os.getenv("LLM_API_KEY", ""),
        llm_model=os.getenv("LLM_MODEL", ""),
        llm_timeout_s=float(os.getenv("LLM_TIMEOUT_S", "30")),
        max_iteraciones=int(os.getenv("MAX_ITERACIONES", "4")),
    )
