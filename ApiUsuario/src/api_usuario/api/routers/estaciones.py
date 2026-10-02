"""Endpoints de estaciones: catálogo y series por bloques horarios."""
from datetime import date, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.engine import Engine

from api_usuario.data_access.estaciones_repository import listar_estaciones, obtener_estacion
from api_usuario.data_access.resumen_repository import serie_bloques
from api_usuario.data_access.sql_connection import get_engine
from api_usuario.entities.estacion import Estacion
from api_usuario.entities.serie import SerieBloques
from api_usuario.shared.constantes import Bloque, Contaminante, MAGNITUD_POR_CONTAMINANTE

router = APIRouter(prefix="/estaciones", tags=["estaciones"])

VENTANA_POR_DEFECTO_DIAS = 7


@router.get("", response_model=list[Estacion], summary="Catálogo de estaciones de control")
def get_estaciones(engine: Engine = Depends(get_engine)) -> list[Estacion]:
    return [Estacion(**e) for e in listar_estaciones(engine)]


@router.get(
    "/{codigo_corto}/series",
    response_model=SerieBloques,
    summary="Serie por bloques horarios de una estación",
    description=(
        "Bloques (madrugada/manana/tarde/noche) con estadísticos y salida del "
        "detector de anomalías, desde `resumen_datos_ml`. Sin fechas, devuelve "
        "los últimos 7 días. Interpretar `is_anomaly` solo con `cobertura >= 0.7`."
    ),
)
def get_serie(
    codigo_corto: int,
    contaminante: Contaminante = Query(..., description="Uno de los 6 contaminantes objetivo"),
    desde: date | None = Query(None, description="Primer día del rango (incluido)"),
    hasta: date | None = Query(None, description="Último día del rango (incluido); por defecto, hoy"),
    bloque: Bloque | None = Query(None, description="Filtrar por un único bloque del día"),
    engine: Engine = Depends(get_engine),
) -> SerieBloques:
    if obtener_estacion(engine, codigo_corto) is None:
        raise HTTPException(status_code=404, detail=f"La estación {codigo_corto} no existe")

    hasta = hasta or date.today()
    desde = desde or hasta - timedelta(days=VENTANA_POR_DEFECTO_DIAS - 1)
    if desde > hasta:
        raise HTTPException(status_code=400, detail="'desde' no puede ser posterior a 'hasta'")

    puntos = serie_bloques(
        engine,
        estacion=codigo_corto,
        magnitud=MAGNITUD_POR_CONTAMINANTE[contaminante.value],
        desde=desde,
        hasta=hasta,
        bloque=bloque.value if bloque is not None else None,
    )
    return SerieBloques(
        estacion=codigo_corto,
        contaminante=contaminante.value,
        desde=desde,
        hasta=hasta,
        puntos=puntos,
    )
