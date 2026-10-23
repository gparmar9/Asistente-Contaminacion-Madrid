"""Reenvía los KPIs del agente. Sin AGENTE_URL, devuelve 404.

No comprueba permisos de administrador: el proyecto no tiene gestión de usuarios."""
import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import ValidationError

from api_usuario.config.settings import Settings, get_settings
from api_usuario.entities.kpis import Dias, Kpis

router = APIRouter(tags=["kpis"])

KPIS_NO_DISPONIBLES = "Los KPIs no están disponibles en este momento"


@router.get("/kpis", response_model=Kpis, summary="KPIs de uso del asistente en los últimos días")
async def get_kpis(
    dias: Dias = Query(Dias.SIETE, description="Días hacia atrás"),
    settings: Settings = Depends(get_settings),
) -> Kpis:
    """Devuelve 503 si el servicio falla y 502 si la respuesta no cumple el contrato."""
    if not settings.agente_url:
        raise HTTPException(status_code=404, detail="Sin agente no hay KPIs")
    url = settings.agente_url.rstrip("/") + "/kpis"
    try:
        async with httpx.AsyncClient(timeout=settings.agente_timeout_s) as cliente:
            respuesta = await cliente.get(url, params={"dias": int(dias)})
        respuesta.raise_for_status()
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail=KPIS_NO_DISPONIBLES) from exc
    try:
        return Kpis.model_validate(respuesta.json())
    except (ValueError, ValidationError) as exc:
        raise HTTPException(status_code=502, detail="El agente devolvió unos KPIs inválidos") from exc
