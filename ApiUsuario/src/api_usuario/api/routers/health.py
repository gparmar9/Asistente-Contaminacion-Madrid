from fastapi import APIRouter

from api_usuario.config.settings import get_settings

router = APIRouter(tags=["health"])


@router.get("/health", summary="Endpoint para comprobar que la api funciona")
def get_health() -> dict[str, str]:
    settings = get_settings()
    return {
        "status": "ok",
        "service": settings.app_name,
        "environment": settings.app_env,
    }

