import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from llm_orchestrator.api.routers.health import router as health_router
from llm_orchestrator.api.routers.responder import router as responder_router
from llm_orchestrator.config.settings import get_settings

logger = logging.getLogger("llm_orchestrator")

settings = get_settings()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Avisar en el arranque, no con un 500 por petición, si falta configuración.
    if not settings.database_url:
        logger.warning("DATABASE_URL no está configurada: la tool query_sql fallará")
    if not (settings.llm_base_url and settings.llm_api_key and settings.llm_model):
        logger.warning(
            "LLM_BASE_URL / LLM_API_KEY / LLM_MODEL sin configurar: /responder fallará. "
            "Cualquier proveedor OpenAI-compatible vale (Mistral, Groq, Gemini, Ollama)."
        )
    yield


app = FastAPI(
    title=settings.app_name,
    version=settings.api_version,
    lifespan=lifespan,
)

app.include_router(health_router)
app.include_router(responder_router)
