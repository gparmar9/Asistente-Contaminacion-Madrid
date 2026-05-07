from fastapi import FastAPI

from api_usuario.api.routers.health import router as health_router
from api_usuario.config.settings import get_settings

settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version=settings.api_version,
)

app.include_router(health_router)

