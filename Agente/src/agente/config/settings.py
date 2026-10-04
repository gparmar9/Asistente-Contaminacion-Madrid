"""Configuración del agente, leída de variables de entorno (ver .env.example)."""
from dataclasses import dataclass
import os

PROVEEDORES = ("openai_compatible", "bedrock")


@dataclass(frozen=True)
class Settings:
    app_name: str
    app_env: str
    api_version: str
    api_port: int
    # Proveedor del LLM: "openai_compatible" (Mistral API, Groq, Ollama...) o "bedrock".
    llm_proveedor: str
    llm_modelo: str
    llm_base_url: str          # solo openai_compatible
    llm_api_key: str           # solo openai_compatible
    llm_temperatura: float     # síntesis (el clasificador usa siempre 0)
    llm_timeout_s: float       # por intento
    llm_max_reintentos: int
    aws_region: str            # solo bedrock
    aws_profile: str           # solo bedrock; vacío = credenciales del entorno o rol de instancia
    # Servicio RAG (rag.api). Vacío = la herramienta documental no se ofrece.
    rag_url: str
    rag_timeout_s: float
    # Vueltas máximas del bucle de herramientas; después, síntesis forzada sin herramientas.
    max_vueltas: int
    # Tiempo límite del clasificador de intención; si se agota, el turno sigue como DESCONOCIDA.
    clasificador_timeout_s: float


def get_settings() -> Settings:
    return Settings(
        app_name=os.getenv("APP_NAME", "Agente"),
        app_env=os.getenv("APP_ENV", "development"),
        api_version=os.getenv("API_VERSION", "0.1.0"),
        api_port=int(os.getenv("API_PORT", "8200")),
        llm_proveedor=os.getenv("LLM_PROVEEDOR", "openai_compatible"),
        llm_modelo=os.getenv("LLM_MODELO", ""),
        llm_base_url=os.getenv("LLM_BASE_URL", ""),
        llm_api_key=os.getenv("LLM_API_KEY", ""),
        llm_temperatura=float(os.getenv("LLM_TEMPERATURA", "0.2")),
        llm_timeout_s=float(os.getenv("LLM_TIMEOUT_S", "30")),
        llm_max_reintentos=int(os.getenv("LLM_MAX_REINTENTOS", "2")),
        aws_region=os.getenv("AWS_REGION", "eu-west-1"),
        aws_profile=os.getenv("AWS_PROFILE", ""),
        rag_url=os.getenv("RAG_URL", ""),
        rag_timeout_s=float(os.getenv("RAG_TIMEOUT_S", "20")),
        max_vueltas=int(os.getenv("MAX_VUELTAS", "3")),
        clasificador_timeout_s=float(os.getenv("CLASIFICADOR_TIMEOUT_S", "10")),
    )
