"""Motor de SQLAlchemy compartido por toda la API (singleton perezoso).

Se expone como dependencia de FastAPI: los routers lo reciben con
`Depends(get_engine)` y los tests lo sustituyen con `dependency_overrides`.
"""
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from api_usuario.config.settings import get_settings


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    settings = get_settings()
    if not settings.database_url:
        raise RuntimeError(
            "DATABASE_URL no está configurada: la API no puede consultar la base de datos"
        )
    # pool_pre_ping evita usar conexiones muertas (RDS cierra las inactivas)
    return create_engine(settings.database_url, pool_pre_ping=True)
