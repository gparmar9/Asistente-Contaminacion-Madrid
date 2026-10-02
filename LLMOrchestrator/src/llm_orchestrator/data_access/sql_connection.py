"""Motor de SQLAlchemy compartido (singleton perezoso), inyectable en tests."""
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from llm_orchestrator.config.settings import get_settings


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    settings = get_settings()
    if not settings.database_url:
        raise RuntimeError(
            "DATABASE_URL no está configurada: la tool query_sql no puede funcionar"
        )
    # pool_pre_ping evita usar conexiones muertas (RDS cierra las inactivas)
    return create_engine(settings.database_url, pool_pre_ping=True)
