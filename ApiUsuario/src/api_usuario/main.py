import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from api_usuario.api.routers.chat import router as chat_router
from api_usuario.api.routers.estaciones import router as estaciones_router
from api_usuario.api.routers.health import router as health_router
from api_usuario.config.settings import get_settings

logger = logging.getLogger("api_usuario")

settings = get_settings()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Avisar en el arranque, no con un 500 por petición, si falta configuración.
    if not settings.database_url:
        logger.warning(
            "DATABASE_URL no está configurada: /estaciones y /series fallarán hasta definirla"
        )
    yield


app = FastAPI(
    title=settings.app_name,
    version=settings.api_version,
    lifespan=lifespan,
)

app.include_router(health_router)
app.include_router(estaciones_router)
app.include_router(chat_router)

