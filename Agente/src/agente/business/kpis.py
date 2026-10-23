"""Calcula los KPIs de uso a partir de turnos y votos de Phoenix.

ServicioKpis lee Phoenix y guarda el resultado en caché durante 60 segundos."""
from __future__ import annotations

import math
import statistics
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Callable, Iterable
from zoneinfo import ZoneInfo

from agente.business.trazas import PRECIOS_FECHA, Turno, turnos
from agente.datos.phoenix import LectorPhoenix
from agente.entities.kpis import (Calidad, Comprobaciones, Conteo, Coste, Dia, Distribucion, FalloHerramienta,
                                  Herramientas, Kpis, Periodo, PorRuta, Regla, Resumen, Satisfaccion, Sesiones,
                                  Votos)

ZONA = ZoneInfo("Europe/Madrid")
CACHE_S = 60.0

# Las frases fijas cuentan como resueltas: son respuestas previstas por el diseño.
RESULTADOS = ("respondida", "frase_fija", "sin_evidencia", "servicio_no_disponible", "bloqueada", "error")
RESUELTOS = ("respondida", "frase_fija")
RECHAZOS = ("frase_fija", "sin_evidencia", "servicio_no_disponible", "bloqueada")
NO_DISPONIBLE = ("datos_no_disponibles", "documentacion_no_disponible")
RESPUESTAS_ANTES_DEL_AVISO = 2  # como en Web/src/dominio/valoracion.ts


class ServicioKpis:
    """Lee Phoenix y calcula los KPIs por periodo, con caché."""

    def __init__(self, lector: LectorPhoenix, proyecto: str, cache_s: float = CACHE_S,
                 reloj: Callable[[], float] = time.monotonic,
                 ahora: Callable[[], datetime] = lambda: datetime.now(ZONA)):
        self._lector = lector
        self._proyecto = proyecto
        self._cache_s = cache_s
        self._reloj = reloj
        self._ahora = ahora
        self._cache: dict[int, tuple[float, Kpis]] = {}

    async def obtener(self, dias: int) -> Kpis:
        guardado = self._cache.get(dias)
        if guardado and self._reloj() - guardado[0] < self._cache_s:
            return guardado[1]
        hasta = self._ahora()
        desde = hasta - timedelta(days=dias)
        lista, huerfanas = turnos(await self._lector.spans(self._proyecto, desde, hasta))
        votos = await self._lector.votos(self._proyecto, [t.raiz["span_id"] for t in lista]) if lista else {}
        kpis = calcular(lista, huerfanas, votos, Periodo(dias=dias, desde=desde, hasta=hasta, generado=hasta))
        self._cache[dias] = (self._reloj(), kpis)
        return kpis


def calcular(lista: list[Turno], huerfanas: int, votos: dict[str, str], periodo: Periodo) -> Kpis:
    """`votos`: span_id del turno -> `positiva` | `negativa` (los de otros spans se ignoran)."""
    resultados = {t.trace_id: resultado(t) for t in lista}
    resueltos = [t for t in lista if resultados[t.trace_id] in RESUELTOS]
    por_resultado = Counter(resultados.values())
    return Kpis(
        periodo=periodo, precios=PRECIOS_FECHA, huerfanas=huerfanas,
        resumen=_resumen(lista, resueltos),
        por_dia=_por_dia(lista, resultados),
        intenciones=_conteo(t.intencion for t in lista),
        rutas=_conteo(t.ruta for t in lista),
        temas=_conteo(t.atributo("agente.tema") or "?" for t in lista),
        resultados=[Conteo(categoria=r, n=por_resultado[r]) for r in RESULTADOS if por_resultado[r]],
        coste=_coste(lista, resueltos),
        latencia_por_ruta=_por_ruta((t, t.duracion_ms) for t in lista),
        primer_token_por_ruta=_por_ruta((t, v) for t in lista if (v := primer_token_ms(t)) is not None),
        calidad=_calidad(lista),
        comprobaciones=_comprobaciones(lista),
        herramientas=_herramientas(lista),
        sesiones=_sesiones(lista, resultados),
        satisfaccion=_satisfaccion(lista, votos),
    )


def resultado(t: Turno) -> str:
    if t.error or t.cancelado:
        return "error"
    if t.atributo("agente.bloqueada"):
        return "bloqueada"
    if t.ruta == "sin_evidencia" or t.atributo("agente.valida") is False:
        return "sin_evidencia"
    if t.ruta == "fija":
        return "servicio_no_disponible" if any(d in NO_DISPONIBLE for d in _decisiones(t)) else "frase_fija"
    return "respondida"


def primer_token_ms(t: Turno) -> float | None:
    marca = next((e for e in t.raiz["events"] if e["name"] == "primer_token"), None)
    if marca is None:
        return None
    return (datetime.fromisoformat(marca["time"]) - datetime.fromisoformat(t.raiz["start"])).total_seconds() * 1000


def sesiones(lista: list[Turno]) -> dict[str, list[Turno]]:
    """Turnos por `session.id`, en orden de inicio. Vacío si los spans no lo llevan."""
    por_sesion: dict[str, list[Turno]] = defaultdict(list)
    for t in lista:
        if sesion := t.atributo("session.id"):
            por_sesion[sesion].append(t)
    for propios in por_sesion.values():
        propios.sort(key=lambda t: t.raiz["start"])
    return dict(por_sesion)


def percentil(valores: list[float], p: float) -> float:
    """Rango más cercano, como el p95 de `evaluacion/informe_trazas.py`."""
    ordenados = sorted(valores)
    return ordenados[max(1, math.ceil(p / 100 * len(ordenados))) - 1]


def _resumen(lista: list[Turno], resueltos: list[Turno]) -> Resumen:
    return Resumen(
        preguntas=len(lista), dias_con_uso=len({_local(t.raiz["start"]).date() for t in lista}),
        resueltas=len(resueltos),
        latencia_ms=_distribucion([t.duracion_ms for t in lista], 1),
        primer_token_ms=_distribucion([v for t in lista if (v := primer_token_ms(t)) is not None], 1),
        coste_usd=_distribucion([t.coste_usd for t in lista if t.coste_usd is not None]))


def _por_dia(lista: list[Turno], resultados: dict[str, str]) -> list[Dia]:
    por_dia: dict = defaultdict(list)
    for t in lista:
        por_dia[_local(t.raiz["start"]).date()].append(t)
    return [Dia(dia=dia, preguntas=len(propios),
                resueltas=sum(resultados[t.trace_id] in RESUELTOS for t in propios))
            for dia, propios in sorted(por_dia.items())]


def _coste(lista: list[Turno], resueltos: list[Turno]) -> Coste | None:
    valores = [t.coste_usd for t in lista if t.coste_usd is not None]
    if not valores:
        return None
    total = sum(valores)
    resueltos_con_coste = sum(t.coste_usd is not None for t in resueltos)
    return Coste(turnos_con_coste=len(valores), total_usd=total,
                 por_resuelta_usd=total / resueltos_con_coste if resueltos_con_coste else None,
                 modelos=sorted(set().union(*(t.modelos for t in lista))), valores_usd=valores)


def _por_ruta(pares: Iterable[tuple[Turno, float]]) -> list[PorRuta]:
    """De más a menos turnos."""
    por_ruta: dict[str, list[float]] = defaultdict(list)
    for t, valor in pares:
        por_ruta[t.ruta].append(valor)
    return [PorRuta(ruta=ruta, ms=_distribucion(valores, 1))
            for ruta, valores in sorted(por_ruta.items(), key=lambda par: -len(par[1]))]


def _calidad(lista: list[Turno]) -> Calidad:
    documentales = [t for t in lista if t.atributo("agente.valida") is not None]
    reparaciones = [t.atributo("agente.reparaciones", 0) for t in documentales]
    return Calidad(
        sintesis_documentales=len(documentales),
        validas_a_la_primera=sum(t.atributo("agente.valida") is True and t.atributo("agente.reparaciones") == 0
                                 for t in documentales),
        reparaciones_medias=round(statistics.fmean(reparaciones), 2) if reparaciones else None,
        busquedas_forzadas=sum(bool(t.atributo("agente.busqueda_forzada")) for t in lista),
        consultas_forzadas=sum(bool(t.atributo("agente.consulta_forzada")) for t in lista),
        sintesis_forzadas=sum(bool(t.atributo("agente.sintesis_forzada")) for t in lista))


def _comprobaciones(lista: list[Turno]) -> Comprobaciones:
    hallazgos: dict[str, set[str]] = defaultdict(set)
    bloqueos: dict[str, set[str]] = defaultdict(set)
    for t in lista:
        for e in t.raiz["events"]:
            if e["name"] == "comprobacion":
                regla = e["attributes"].get("agente.regla", "?")
                hallazgos[regla].add(t.trace_id)
                if e["attributes"].get("agente.bloquea"):
                    bloqueos[regla].add(t.trace_id)
    return Comprobaciones(
        turnos_bloqueados=sum(bool(t.atributo("agente.bloqueada")) for t in lista),
        reglas=[Regla(regla=r, turnos=len(hallazgos[r]), bloqueados=len(bloqueos[r]))
                for r in sorted(hallazgos, key=lambda r: -len(hallazgos[r]))])


def _herramientas(lista: list[Turno]) -> Herramientas:
    llamadas = [s for t in lista for s in t.spans if s["kind"] == "TOOL" and "agente.ok" in s["attributes"]]
    fallos = Counter((s["name"], _causa(s)) for s in llamadas if s["attributes"]["agente.ok"] is False)
    con_datos = [t for t in lista if "DATOS" in t.intencion]
    return Herramientas(
        llamadas=len(llamadas),
        fallos=[FalloHerramienta(herramienta=nombre, causa=causa, fallos=c) for (nombre, causa), c in fallos.most_common()],
        preguntas_de_datos=len(con_datos),
        datos_no_disponibles=sum("datos_no_disponibles" in _decisiones(t) for t in con_datos))


def _causa(span: dict) -> str:
    a = span["attributes"]
    if a.get("agente.permitida") is False:
        return "vetada"
    return a.get("agente.estado") or "otro"


def _sesiones(lista: list[Turno], resultados: dict[str, str]) -> Sesiones | None:
    grupos = sesiones(lista)
    if not grupos:
        return None
    turnos_sesion = [len(g) for g in grupos.values()]
    duraciones = [(datetime.fromisoformat(g[-1].raiz["end"]) - datetime.fromisoformat(g[0].raiz["start"]))
                  .total_seconds() for g in grupos.values() if len(g) > 1]
    tras_rechazo = [actual for g in grupos.values() for previo, actual in zip(g, g[1:])
                    if resultados[previo.trace_id] in RECHAZOS]
    return Sesiones(
        sesiones=len(grupos), turnos_mediana=statistics.median(turnos_sesion), turnos_maximo=max(turnos_sesion),
        de_un_turno=turnos_sesion.count(1), duracion_s=_distribucion(duraciones, 1),
        turnos_con_previo=sum(len(g) - 1 for g in grupos.values()), tras_rechazo=len(tras_rechazo),
        resueltos_tras_rechazo=sum(resultados[t.trace_id] in RESUELTOS for t in tras_rechazo))


def _satisfaccion(lista: list[Turno], votos: dict[str, str]) -> Satisfaccion | None:
    valorados = [(t, votos[t.raiz["span_id"]]) for t in lista if t.raiz["span_id"] in votos]
    if not valorados:
        return None
    # La web pide el voto tras dos respuestas completas; excluye errores y cancelaciones.
    elegibles = [g for g in sesiones(lista).values()
                 if sum(not (t.error or t.cancelado) for t in g) >= RESPUESTAS_ANTES_DEL_AVISO]
    return Satisfaccion(
        votos=len(valorados), positivos=sum(v == "positiva" for _, v in valorados),
        por_ruta=_votos_por(valorados, lambda t: t.ruta),
        por_tema=_votos_por(valorados, lambda t: t.atributo("agente.tema") or "?"),
        sesiones_elegibles=len(elegibles),
        sesiones_con_voto=sum(any(t.raiz["span_id"] in votos for t in g) for g in elegibles))


def _votos_por(valorados: list[tuple[Turno, str]], clave: Callable[[Turno], str]) -> list[Votos]:
    """De más a menos votos."""
    grupos: dict[str, list[str]] = defaultdict(list)
    for t, v in valorados:
        grupos[clave(t)].append(v)
    return sorted((Votos(grupo=g, positivos=vs.count("positiva"), votos=len(vs)) for g, vs in grupos.items()),
                  key=lambda v: -v.votos)


def _decisiones(t: Turno) -> list[str]:
    return [e["attributes"].get("agente.decision", "") for s in t.spans for e in s["events"] if e["name"] == "decision"]


def _conteo(valores: Iterable[str]) -> list[Conteo]:
    return [Conteo(categoria=v, n=c) for v, c in Counter(valores).most_common()]


def _distribucion(valores: list[float], decimales: int | None = None) -> Distribucion:
    if not valores:
        return Distribucion(n=0)
    redondear = (lambda v: round(v, decimales)) if decimales is not None else (lambda v: v)
    return Distribucion(n=len(valores), **{f"p{p}": redondear(percentil(valores, p)) for p in (50, 90, 95)})


def _local(iso: str) -> datetime:
    return datetime.fromisoformat(iso).astimezone(ZONA)
