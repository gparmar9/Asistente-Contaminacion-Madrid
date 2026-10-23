"""KPIs de uso del asistente desplegado, a partir de los spans de Phoenix (o de JSONL de trazas).

Lee los spans del proyecto con la API REST de Phoenix (`GET /v1/projects/{proyecto}/spans`,
paginada; verificada en el servidor 20.19.0) usando `httpx`, sin el cliente de Phoenix. Cada span
se adapta al formato del JSONL propio para reutilizar `informe_trazas.turnos()`: árbol del turno,
tokens y coste con la misma tabla de precios.

KPIs: preguntas por día, mezcla de intenciones, rutas y temas, resultado del turno y tasa de
resolución, bloqueos por regla, reparaciones y respuestas forzadas por el código, coste por
pregunta (P50, P90 y por pregunta resuelta), latencia por ruta, tiempo hasta el primer token
(evento `primer_token`), fallos de herramienta por causa (`agente.estado`), sesiones (turnos y
duración) y tasa de reformulación tras un rechazo. Las dos últimas necesitan `session.id` en el
span `turno` y la satisfacción, la valoración de los usuarios: sin ellas salen «sin datos».

La salida no lleva texto de usuarios: ni preguntas ni respuestas ni mensajes de error, solo
agregados y, con `--detalle`, ruta, tema, tiempos, tokens y coste por turno. Se puede versionar.

El formato lo da la extensión de `--salida`: `.md` (tablas; las dos figuras se escriben como
`<nombre>_embudo.svg` y `<nombre>_coste.svg` al lado) o `.html` (autocontenido, figuras dentro).

Uso, desde la carpeta Agente/ (la contraseña de la web, en `PHOENIX_CONTRASENA` o la pide):
    .venv/bin/python evaluacion/kpis_phoenix.py --phoenix http://<ip-de-la-ec2>/phoenix \\
        --desde 2026-10-23 --salida evaluacion/resultados/kpis_20261023.md
    .venv/bin/python evaluacion/kpis_phoenix.py --phoenix http://localhost:6006 --usuario ""  # local
    .venv/bin/python evaluacion/kpis_phoenix.py --jsonl trazas/*.jsonl --salida kpis.html
"""
from __future__ import annotations

import argparse
import getpass
import html
import math
import os
import re
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterator
from zoneinfo import ZoneInfo

import httpx

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))  # evaluacion.informe_trazas

from evaluacion import informe_trazas  # noqa: E402
from evaluacion.informe_trazas import Turno, tabla  # noqa: E402

ZONA = ZoneInfo("Europe/Madrid")
TAM_PAGINA = 1000  # máximo que admite el endpoint

# Resultado del turno, en el orden de las tablas. Resuelto = respondida o frase fija del código
# (fuera de alcance por diseño, o un periodo sin mediciones): es la respuesta correcta.
RESULTADOS = ("respondida", "frase_fija", "sin_evidencia", "servicio_no_disponible", "bloqueada", "error")
RESUELTOS = ("respondida", "frase_fija")
RECHAZOS = ("frase_fija", "sin_evidencia", "servicio_no_disponible", "bloqueada")  # para la reformulación
NO_DISPONIBLE = ("datos_no_disponibles", "documentacion_no_disponible")
MAX_CATEGORIAS = 6  # en la figura del embudo; el resto, «otras»


# ---------------------------------------------------------------------- lectura de Phoenix

def descargar(cliente: httpx.Client, proyecto: str, desde: datetime | None = None,
              hasta: datetime | None = None) -> list[dict]:
    """Todos los spans del proyecto que empiezan en [desde, hasta), siguiendo el cursor."""
    params: dict[str, Any] = {"limit": TAM_PAGINA}
    if desde:
        params["start_time"] = desde.isoformat()
    if hasta:
        params["end_time"] = hasta.isoformat()
    spans: list[dict] = []
    cursor = None
    while True:
        r = cliente.get(f"/v1/projects/{proyecto}/spans", params={**params, **({"cursor": cursor} if cursor else {})})
        r.raise_for_status()
        cuerpo = r.json()
        spans.extend(cuerpo["data"])
        cursor = cuerpo.get("next_cursor")
        if not cursor:
            return spans


def a_formato_jsonl(span: dict) -> dict:
    """Span de la API de Phoenix -> el dict de `observabilidad/jsonl.py` que lee `informe_trazas`.
    Phoenix devuelve los atributos del span aplanados (`agente.ruta`) y saca el tipo a `span_kind`."""
    inicio, fin = _fecha(span["start_time"]), _fecha(span["end_time"])
    tipo = span.get("span_kind")
    tipo = None if tipo in (None, "UNKNOWN") else tipo
    return {
        "trace_id": span["context"]["trace_id"], "span_id": span["context"]["span_id"],
        "parent_id": span.get("parent_id"), "name": span["name"], "kind": tipo,
        "start": _iso(inicio), "end": _iso(fin), "duracion_ms": round((fin - inicio).total_seconds() * 1000, 3),
        "status": span.get("status_code") or "UNSET", "status_descripcion": span.get("status_message") or None,
        "attributes": {"openinference.span.kind": tipo, **(span.get("attributes") or {})},
        "events": [{"name": e["name"], "time": _iso(_fecha(e["timestamp"])),
                    "attributes": dict(_aplanar(e.get("attributes") or {}))} for e in span.get("events") or []],
    }


def _aplanar(atributos: dict, prefijo: str = "") -> Iterator[tuple[str, Any]]:
    for clave, valor in atributos.items():
        if isinstance(valor, dict):
            yield from _aplanar(valor, f"{prefijo}{clave}.")
        else:
            yield f"{prefijo}{clave}", valor


def _fecha(texto: str) -> datetime:
    return datetime.fromisoformat(texto)


def _iso(momento: datetime) -> str:
    """Siempre con microsegundos: `turnos()` ordena los spans por el texto del inicio."""
    return momento.isoformat(timespec="microseconds")


# ---------------------------------------------------------------------- KPIs

@dataclass
class Seccion:
    titulo: str
    texto: str = ""
    filas: list[list[str]] | None = None
    figura: tuple[str, str] | None = None  # (nombre, svg)


@dataclass
class Informe:
    titulo: str
    cabecera: list[str]
    secciones: list[Seccion] = field(default_factory=list)


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
    return (_fecha(marca["time"]) - _fecha(t.raiz["start"])).total_seconds() * 1000


def sesiones(lista: list[Turno]) -> dict[str, list[Turno]]:
    """Turnos por `session.id`, en orden de inicio. Vacío si los spans no lo llevan."""
    por_sesion: dict[str, list[Turno]] = defaultdict(list)
    for t in lista:
        if sesion := t.atributo("session.id"):
            por_sesion[sesion].append(t)
    for propios in por_sesion.values():
        propios.sort(key=lambda t: t.raiz["start"])
    return dict(por_sesion)


def construir(lista: list[Turno], huerfanas: int, origen: str, detalle: bool = False) -> Informe:
    n = len(lista)
    if lista:
        primero, ultimo = (_local(lista[0].raiz["start"]), _local(lista[-1].raiz["start"]))
        periodo = f"del {primero:%Y-%m-%d %H:%M} al {ultimo:%Y-%m-%d %H:%M} (hora de Madrid)"
    else:
        periodo = "sin turnos"
    cabecera = [f"Origen: {origen}. {n} turnos, {periodo}.",
                f"Generado el {datetime.now(ZONA):%Y-%m-%d %H:%M}. Precios: {informe_trazas.PRECIOS_FECHA}. "
                "Percentiles por rango más cercano: con pocas muestras, P90 y P95 son casi el máximo. "
                "Sin texto de usuarios."]
    if huerfanas:
        cabecera.append(f"Trazas sin span `turno` (descartadas: turno cortado o fuera del intervalo): {huerfanas}.")
    informe = Informe("KPIs del asistente", cabecera)
    if not lista:
        return informe
    resultados = {t.trace_id: resultado(t) for t in lista}
    resueltos = [t for t in lista if resultados[t.trace_id] in RESUELTOS]
    s = informe.secciones

    s.append(_resumen(lista, resultados, resueltos))
    s.append(_por_dia(lista, resultados))
    s.append(Seccion("Embudo: intención, ruta y resultado",
                     "Cada barra, el nº de turnos de esa categoría. Tablas con las mismas cifras debajo.",
                     figura=("embudo", svg_embudo([
                         ("Intención", _conteo(t.intencion for t in lista)),
                         ("Ruta", _conteo(t.ruta for t in lista)),
                         ("Resultado", [(r, c) for r in RESULTADOS if (c := Counter(resultados.values())[r])]),
                     ], n))))
    s.append(_reparto("Intención", (t.intencion for t in lista), n))
    s.append(_reparto("Ruta", (t.ruta for t in lista), n))
    s.append(_reparto("Tema", (t.atributo("agente.tema") or "?" for t in lista), n))
    s.append(_resultados(resultados, n))
    s.append(_coste(lista, resueltos))
    s.append(_latencia(lista))
    s.append(_primer_token(lista))
    s.append(_calidad(lista))
    s.append(_bloqueos(lista))
    s.append(_herramientas(lista))
    s.append(_sesiones(lista, resultados))
    s.append(Seccion("Satisfacción", "Sin datos: la valoración de los usuarios (👍/👎) aún no existe."))
    if detalle:
        s.append(_detalle(lista, resultados))
    return informe


def _resumen(lista: list[Turno], resultados: dict[str, str], resueltos: list[Turno]) -> Seccion:
    n = len(lista)
    con_coste = [t.coste_usd for t in lista if t.coste_usd is not None]
    primeros = [v for t in lista if (v := primer_token_ms(t)) is not None]
    filas = [["KPI", "Valor", "n"],
             ["Preguntas (turnos)", str(n), str(n)],
             ["Días con uso", str(len({_local(t.raiz['start']).date() for t in lista})), str(n)],
             ["Tasa de resolución (respondida o frase fija)", _pct(len(resueltos), n), str(n)],
             ["Latencia del turno P50 / P95 (ms)",
              f"{_percentil([t.duracion_ms for t in lista], 50)} / {_percentil([t.duracion_ms for t in lista], 95)}",
              str(n)],
             ["Primer token P50 / P95 (ms)", f"{_percentil(primeros, 50)} / {_percentil(primeros, 95)}"
              if primeros else "sin datos", str(len(primeros))],
             ["Coste por pregunta P50 / P90 (USD)",
              f"{_percentil(con_coste, 50, 5)} / {_percentil(con_coste, 90, 5)}" if con_coste else "—",
              str(len(con_coste))]]
    return Seccion("Resumen", filas=filas)


def _por_dia(lista: list[Turno], resultados: dict[str, str]) -> Seccion:
    por_dia: dict[date, list[Turno]] = defaultdict(list)
    for t in lista:
        por_dia[_local(t.raiz["start"]).date()].append(t)
    filas = [["Día", "Preguntas", "Resueltas"]]
    for dia in sorted(por_dia):
        propios = por_dia[dia]
        filas.append([dia.isoformat(), str(len(propios)),
                      _pct(sum(resultados[t.trace_id] in RESUELTOS for t in propios), len(propios))])
    return Seccion("Preguntas por día", filas=filas)


def _reparto(nombre: str, valores: Iterator[str], n: int) -> Seccion:
    filas = [[nombre, "n", "%"]] + [[v, str(c), _pct(c, n)] for v, c in Counter(valores).most_common()]
    return Seccion(f"Turnos por {nombre.lower()}", filas=filas)


def _resultados(resultados: dict[str, str], n: int) -> Seccion:
    conteo = Counter(resultados.values())
    filas = [["Resultado", "n", "%"]] + [[r, str(conteo[r]), _pct(conteo[r], n)] for r in RESULTADOS if conteo[r]]
    return Seccion("Resultado del turno",
                   "`frase_fija`: respuesta fija del código por diseño (intención fuera de alcance o periodo "
                   "sin mediciones). `sin_evidencia`: el RAG no encontró evidencias o la síntesis no superó "
                   "la validación. `servicio_no_disponible`: base de datos o RAG caídos. `error`: excepción "
                   "o cliente desconectado. Resueltos: `respondida` y `frase_fija`.", filas=filas)


def _coste(lista: list[Turno], resueltos: list[Turno]) -> Seccion:
    con_coste = [t for t in lista if t.coste_usd is not None]
    valores = [t.coste_usd for t in con_coste]
    if not valores:
        return Seccion("Coste por pregunta", "Sin datos: ningún turno tiene tokens completos con precio.")
    total = sum(valores)
    resueltos_con_coste = sum(t.coste_usd is not None for t in resueltos)
    filas = [["Indicador", "Valor (USD)"],
             ["Coste total", informe_trazas._num(total, 4)],
             ["Coste por pregunta P50", _percentil(valores, 50, 5)],
             ["Coste por pregunta P90", _percentil(valores, 90, 5)],
             ["Coste medio por 1.000 preguntas", informe_trazas._num(total / len(valores) * 1000, 2)],
             ["Coste por pregunta resuelta (total / resueltas)",
              informe_trazas._num(total / resueltos_con_coste, 5) if resueltos_con_coste else "—"]]
    texto = (f"Turnos con coste: {len(con_coste)} de {len(lista)} (sin coste: tokens incompletos o modelo sin "
             f"precio). Modelos: {', '.join(sorted(set().union(*(t.modelos for t in lista)))) or '—'}.")
    return Seccion("Coste por pregunta", texto, filas, ("coste", svg_histograma(valores)))


def _latencia(lista: list[Turno]) -> Seccion:
    por_ruta: dict[str, list[float]] = defaultdict(list)
    for t in lista:
        por_ruta[t.ruta].append(t.duracion_ms)
    filas = [["Ruta", "n", "P50 (ms)", "P95 (ms)"]]
    for ruta, valores in sorted(por_ruta.items(), key=lambda par: -len(par[1])):
        filas.append([ruta, str(len(valores)), _percentil(valores, 50), _percentil(valores, 95)])
    return Seccion("Latencia del turno por ruta",
                   "Duración del span `turno`: no incluye la espera por otro turno de la misma sesión.", filas)


def _primer_token(lista: list[Turno]) -> Seccion:
    marcados = [(t, v) for t in lista if (v := primer_token_ms(t)) is not None]
    if not marcados:
        return Seccion("Tiempo hasta el primer token",
                       "Sin datos: ningún turno lleva el evento `primer_token` (solo lo emite el chat en "
                       "streaming, a partir de esta versión del agente).")
    por_ruta: dict[str, list[float]] = defaultdict(list)
    for t, v in marcados:
        por_ruta[t.ruta].append(v)
    filas = [["Ruta", "n", "P50 (ms)", "P95 (ms)"],
             ["todas", str(len(marcados)), _percentil([v for _, v in marcados], 50),
              _percentil([v for _, v in marcados], 95)]]
    filas += [[r, str(len(v)), _percentil(v, 50), _percentil(v, 95)] for r, v in sorted(por_ruta.items())]
    return Seccion("Tiempo hasta el primer token",
                   f"Turnos con el primer texto en tokens: {len(marcados)} de {len(lista)}. El resto (rutas "
                   "documental y mixta, frases fijas, peticiones sin stream) llega entero al final: para "
                   "ellos, la latencia percibida es la del turno.", filas)


def _calidad(lista: list[Turno]) -> Seccion:
    n = len(lista)
    documentales = [t for t in lista if t.atributo("agente.valida") is not None]
    reparaciones = [t.atributo("agente.reparaciones", 0) for t in documentales]
    a_la_primera = sum(t.atributo("agente.valida") is True and t.atributo("agente.reparaciones") == 0
                       for t in documentales)
    filas = [["Indicador", "Valor", "n"],
             ["Síntesis documental válida a la primera", _pct(a_la_primera, len(documentales)), str(len(documentales))],
             ["Reparaciones medias por síntesis documental",
              informe_trazas._num(statistics.fmean(reparaciones), 2) if reparaciones else "—", str(len(documentales))]]
    for clave, nombre in (("agente.busqueda_forzada", "Búsqueda lanzada por el código"),
                          ("agente.consulta_forzada", "Consulta de datos lanzada por el código"),
                          ("agente.sintesis_forzada", "Síntesis forzada (límite de vueltas)")):
        filas.append([nombre, _pct(sum(bool(t.atributo(clave)) for t in lista), n), str(n)])
    return Seccion("Calidad del primer intento y dependencia de los guardarraíles",
                   "Las respuestas forzadas son las que cerró el código porque el modelo no hizo lo que debía.",
                   filas)


def _bloqueos(lista: list[Turno]) -> Seccion:
    hallazgos: dict[str, set[str]] = defaultdict(set)
    bloqueos: dict[str, set[str]] = defaultdict(set)
    for t in lista:
        for e in t.raiz["events"]:
            if e["name"] == "comprobacion":
                regla = e["attributes"].get("agente.regla", "?")
                hallazgos[regla].add(t.trace_id)
                if e["attributes"].get("agente.bloquea"):
                    bloqueos[regla].add(t.trace_id)
    if not hallazgos:
        return Seccion("Comprobaciones posteriores", "Ninguna regla saltó en el periodo.")
    filas = [["Regla", "Turnos con hallazgo", "% de turnos", "Turnos bloqueados"]]
    for regla in sorted(hallazgos, key=lambda r: -len(hallazgos[r])):
        filas.append([regla, str(len(hallazgos[regla])), _pct(len(hallazgos[regla]), len(lista)),
                      str(len(bloqueos[regla]))])
    return Seccion("Comprobaciones posteriores",
                   f"Turnos bloqueados en total: {sum(bool(t.atributo('agente.bloqueada')) for t in lista)}.", filas)


def _herramientas(lista: list[Turno]) -> Seccion:
    llamadas = [s for t in lista for s in t.spans if s["kind"] == "TOOL" and "agente.ok" in s["attributes"]]
    fallos = Counter((s["name"], _causa(s)) for s in llamadas if s["attributes"]["agente.ok"] is False)
    con_datos = [t for t in lista if "DATOS" in t.intencion]
    sin_base = sum(any(d == "datos_no_disponibles" for d in _decisiones(t)) for t in con_datos)
    texto = (f"Llamadas a herramientas: {len(llamadas)}; con fallo: {sum(fallos.values())}. "
             f"Preguntas de datos que la base no pudo responder (decisión `datos_no_disponibles`; p. ej. "
             f"la RDS apagada fuera de horario): {sin_base} de {len(con_datos)}.")
    if not fallos:
        return Seccion("Fallos de herramienta por causa", texto)
    filas = [["Herramienta", "Causa (`agente.estado`)", "Fallos"]] + [
        [nombre, causa, str(c)] for (nombre, causa), c in fallos.most_common()]
    return Seccion("Fallos de herramienta por causa", texto, filas)


def _causa(span: dict) -> str:
    a = span["attributes"]
    if a.get("agente.permitida") is False:
        return "vetada"
    return a.get("agente.estado") or "otro"


def _sesiones(lista: list[Turno], resultados: dict[str, str]) -> Seccion:
    grupos = sesiones(lista)
    if not grupos:
        return Seccion("Sesiones y reformulación",
                       "Sin datos: los spans `turno` aún no llevan `session.id`.")
    turnos_sesion = [len(g) for g in grupos.values()]
    duraciones = [(_fecha(g[-1].raiz["end"]) - _fecha(g[0].raiz["start"])).total_seconds()
                  for g in grupos.values() if len(g) > 1]
    tras_rechazo = [(previo, actual) for g in grupos.values() for previo, actual in zip(g, g[1:])
                    if resultados[previo.trace_id] in RECHAZOS]
    con_previo = sum(len(g) - 1 for g in grupos.values())
    resuelta_despues = sum(resultados[actual.trace_id] in RESUELTOS for _, actual in tras_rechazo)
    filas = [["KPI", "Valor", "n"],
             ["Sesiones", str(len(grupos)), str(len(grupos))],
             ["Turnos por sesión: mediana / máximo",
              f"{informe_trazas._num(statistics.median(turnos_sesion), 1)} / {max(turnos_sesion)}", str(len(grupos))],
             ["Sesiones de un solo turno", _pct(turnos_sesion.count(1), len(grupos)), str(len(grupos))],
             ["Duración de la sesión P50 (s), con 2 o más turnos", _percentil(duraciones, 50) if duraciones else "—",
              str(len(duraciones))],
             ["Tasa de reformulación: turnos tras un rechazo", _pct(len(tras_rechazo), con_previo), str(con_previo)],
             ["De ellos, resueltos a la segunda (rechazo evitable)", _pct(resuelta_despues, len(tras_rechazo)),
              str(len(tras_rechazo))]]
    return Seccion("Sesiones y reformulación",
                   "Rechazo: frase fija, sin evidencia, servicio no disponible o bloqueo. Si el turno siguiente "
                   "de la misma sesión sí se resuelve, el rechazo pudo ser un falso negativo.", filas)


def _detalle(lista: list[Turno], resultados: dict[str, str]) -> Seccion:
    numero = {sesion: f"S{i}" for i, sesion in enumerate(sesiones(lista), 1)}
    filas = [["Inicio (Madrid)", "Traza", "Sesión", "Intención", "Ruta", "Tema", "Resultado", "Duración (ms)",
              "Primer token (ms)", "Tokens (e / s)", "Coste (USD)"]]
    for t in lista:
        filas.append([f"{_local(t.raiz['start']):%Y-%m-%d %H:%M:%S}", t.trace_id[:8],
                      numero.get(t.atributo("session.id"), "—"), t.intencion, t.ruta, t.atributo("agente.tema") or "?",
                      resultados[t.trace_id], informe_trazas._num(t.duracion_ms),
                      informe_trazas._num(primer_token_ms(t)),
                      f"{t.tokens_entrada} / {t.tokens_salida}" if t.tokens_entrada is not None else "incompletos",
                      informe_trazas._num(t.coste_usd, 5)])
    return Seccion("Detalle por turno", "Sin pregunta ni respuesta. La traza (8 primeros caracteres) "
                   "permite abrir el turno en Phoenix.", filas)


# ---------------------------------------------------------------------- utilidades de cálculo

def _decisiones(t: Turno) -> list[str]:
    return [e["attributes"].get("agente.decision", "") for s in t.spans for e in s["events"] if e["name"] == "decision"]


def _conteo(valores: Iterator[str]) -> list[tuple[str, int]]:
    """Las categorías más frecuentes y el resto como «otras» (la figura no pasa de MAX_CATEGORIAS)."""
    comunes = Counter(valores).most_common()
    if len(comunes) <= MAX_CATEGORIAS:
        return comunes
    return comunes[:MAX_CATEGORIAS - 1] + [("otras", sum(c for _, c in comunes[MAX_CATEGORIAS - 1:]))]


def percentil(valores: list[float], p: float) -> float:
    """Rango más cercano, como el p95 de `informe_trazas`."""
    ordenados = sorted(valores)
    return ordenados[max(1, math.ceil(p / 100 * len(ordenados))) - 1]


def _percentil(valores: list[float], p: float, decimales: int = 0) -> str:
    return informe_trazas._num(percentil(valores, p), decimales) if valores else "—"


def _pct(parte: int, total: int) -> str:
    return f"{informe_trazas._num(100 * parte / total, 1)} %" if total else "—"


def _local(iso: str) -> datetime:
    return _fecha(iso).astimezone(ZONA)


# ---------------------------------------------------------------------- figuras (SVG sin dependencias)

ESTILO_SVG = """<style>
svg.kpi{--superficie:#fcfcfb;--texto:#0b0b0b;--texto-2:#52514e;--tenue:#898781;--rejilla:#e1e0d9;--eje:#c3c2b7;--serie:#2a78d6;font-family:system-ui,-apple-system,"Segoe UI",sans-serif}
@media (prefers-color-scheme:dark){:root:where(:not([data-theme="light"])) svg.kpi,svg.kpi:root{--superficie:#1a1a19;--texto:#f0efec;--texto-2:#c3c2b7;--rejilla:#2c2c2a;--eje:#383835;--serie:#3987e5}}
:root[data-theme="dark"] svg.kpi{--superficie:#1a1a19;--texto:#f0efec;--texto-2:#c3c2b7;--rejilla:#2c2c2a;--eje:#383835;--serie:#3987e5}
svg.kpi .fondo{fill:var(--superficie)} svg.kpi .marca{fill:var(--serie)} svg.kpi .marca:hover{opacity:.8}
svg.kpi .titulo{fill:var(--texto);font-size:13px;font-weight:600} svg.kpi .etiqueta{fill:var(--texto-2);font-size:12px}
svg.kpi .valor{fill:var(--texto);font-size:12px} svg.kpi .tenue{fill:var(--tenue);font-size:11px}
svg.kpi .rejilla{stroke:var(--rejilla);stroke-width:1} svg.kpi .eje{stroke:var(--eje);stroke-width:1}
svg.kpi .referencia{stroke:var(--texto-2);stroke-width:1.5;stroke-dasharray:4 3}
</style>"""


def _svg(ancho: int, alto: int, titulo: str, cuerpo: list[str]) -> str:
    return (f'<svg class="kpi" xmlns="http://www.w3.org/2000/svg" width="{ancho}" height="{alto}" '
            f'viewBox="0 0 {ancho} {alto}" role="img" aria-label="{html.escape(titulo)}">{ESTILO_SVG}'
            f'<rect class="fondo" width="{ancho}" height="{alto}" rx="8"/>' + "".join(cuerpo) + "</svg>\n")


def _barra_h(x: float, y: float, ancho: float, alto: float, ayuda: str) -> str:
    """Barra horizontal con el extremo de datos redondeado (4 px) y recta en la base."""
    r = min(4.0, ancho / 2, alto / 2)
    d = (f"M{x:.1f},{y:.1f}h{ancho - r:.1f}a{r},{r} 0 0 1 {r},{r}v{alto - 2 * r:.1f}"
         f"a{r},{r} 0 0 1 -{r},{r}h-{ancho - r:.1f}z")
    return f'<path class="marca" d="{d}"><title>{html.escape(ayuda)}</title></path>'


def _columna(x: float, base: float, ancho: float, alto: float, ayuda: str) -> str:
    r = min(4.0, ancho / 2, alto / 2)
    d = (f"M{x:.1f},{base:.1f}v-{alto - r:.1f}a{r},{r} 0 0 1 {r},-{r}h{ancho - 2 * r:.1f}"
         f"a{r},{r} 0 0 1 {r},{r}v{alto - r:.1f}z")
    return f'<path class="marca" d="{d}"><title>{html.escape(ayuda)}</title></path>'


def svg_embudo(niveles: list[tuple[str, list[tuple[str, int]]]], total: int) -> str:
    """Un panel de barras horizontales por nivel (intención, ruta, resultado), misma escala."""
    ancho, x0, max_barra, fila, grosor = 640, 190, 330, 24, 16
    maximo = max((c for _, cats in niveles for _, c in cats), default=1)
    cuerpo, y = [], 28
    for nombre, categorias in niveles:
        cuerpo.append(f'<text class="titulo" x="16" y="{y}">{html.escape(nombre)}</text>')
        y += 12
        for etiqueta, c in categorias:
            largo = max(1.0, max_barra * c / maximo)
            ayuda = f"{nombre} {etiqueta}: {c} turnos ({_pct(c, total)})"
            cuerpo.append(f'<text class="etiqueta" x="{x0 - 8}" y="{y + grosor - 4}" text-anchor="end">'
                          f'{html.escape(_corto(etiqueta, 26))}</text>')
            cuerpo.append(_barra_h(x0, y, largo, grosor, ayuda))
            cuerpo.append(f'<text class="valor" x="{x0 + largo + 6:.1f}" y="{y + grosor - 4}">{c} '
                          f'<tspan class="tenue">({_pct(c, total)})</tspan></text>')
            y += fila
        y += 20
    return _svg(ancho, y - 8, "Embudo de turnos: intención, ruta y resultado", cuerpo)


def svg_histograma(costes: list[float]) -> str:
    """Distribución del coste por pregunta (milésimas de USD) con P50 y P90 marcados."""
    ancho, alto, x0, x1, y0, y1 = 640, 280, 48, 620, 56, 230
    valores = [c * 1000 for c in costes]
    tope = max(valores) or 1.0
    cubos = min(12, max(5, math.ceil(math.sqrt(len(valores)))))
    paso = tope / cubos
    cuentas = [0] * cubos
    for v in valores:
        cuentas[min(cubos - 1, int(v / paso))] += 1
    cmax = max(cuentas)
    escala_x = (x1 - x0) / tope
    cuerpo = ['<text class="titulo" x="16" y="22">Coste por pregunta (n = %d)</text>' % len(valores)]
    for k in range(0, cmax + 1, max(1, math.ceil(cmax / 4))):  # rejilla horizontal y eje de cuentas
        y = y1 - (y1 - y0) * k / cmax
        cuerpo.append(f'<line class="rejilla" x1="{x0}" x2="{x1}" y1="{y:.1f}" y2="{y:.1f}"/>')
        cuerpo.append(f'<text class="tenue" x="{x0 - 6}" y="{y + 4:.1f}" text-anchor="end">{k}</text>')
    hueco = 2
    for i, c in enumerate(cuentas):
        if c:
            izq = x0 + i * paso * escala_x
            cuerpo.append(_columna(izq + hueco / 2, y1, paso * escala_x - hueco, (y1 - y0) * c / cmax,
                                   f"{informe_trazas._num(i * paso, 3)}-{informe_trazas._num((i + 1) * paso, 3)} "
                                   f"milésimas de USD: {c} preguntas"))
    cuerpo.append(f'<line class="eje" x1="{x0}" x2="{x1}" y1="{y1}" y2="{y1}"/>')
    for k in range(5):
        v = tope * k / 4
        cuerpo.append(f'<text class="tenue" x="{x0 + v * escala_x:.1f}" y="{y1 + 16}" text-anchor="middle">'
                      f'{informe_trazas._num(v, 2)}</text>')
    cuerpo.append(f'<text class="etiqueta" x="{(x0 + x1) / 2}" y="{alto - 12}" text-anchor="middle">'
                  'Coste por pregunta (milésimas de USD)</text>')
    for p, alto_texto in ((50, y0 - 6), (90, y0 + 8)):
        v = percentil(valores, p)
        x = x0 + v * escala_x
        cuerpo.append(f'<line class="referencia" x1="{x:.1f}" x2="{x:.1f}" y1="{y0 - 2}" y2="{y1}"/>')
        ancla = "end" if x > x1 - 80 else "start"
        dx = -4 if ancla == "end" else 4
        cuerpo.append(f'<text class="valor" x="{x + dx:.1f}" y="{alto_texto}" text-anchor="{ancla}">'
                      f'P{p} {informe_trazas._num(v, 3)}</text>')
    return _svg(ancho, alto, "Distribución del coste por pregunta con P50 y P90", cuerpo)


def _corto(texto: str, maximo: int) -> str:
    return texto if len(texto) <= maximo else texto[:maximo - 1] + "…"


# ---------------------------------------------------------------------- salida

def a_markdown(informe: Informe, nombre_base: str | None = None) -> tuple[str, dict[str, str]]:
    """Markdown y las figuras como ficheros `<nombre_base>_<figura>.svg` (sin nombre: sin figuras)."""
    partes = [f"# {informe.titulo}", "\n\n".join(informe.cabecera)]
    figuras: dict[str, str] = {}
    for s in informe.secciones:
        bloque = [f"## {s.titulo}"]
        if s.texto:
            bloque.append(s.texto)
        if s.figura and nombre_base:
            fichero = f"{nombre_base}_{s.figura[0]}.svg"
            figuras[fichero] = s.figura[1]
            bloque.append(f"![{s.titulo}]({fichero})")
        if s.filas:
            bloque.append(tabla(s.filas))
        partes.append("\n\n".join(bloque))
    return "\n\n".join(partes) + "\n", figuras


def a_html(informe: Informe) -> str:
    cuerpo = [f"<h1>{html.escape(informe.titulo)}</h1>"] + [f"<p>{_en_linea(c)}</p>" for c in informe.cabecera]
    for s in informe.secciones:
        cuerpo.append(f"<h2>{html.escape(s.titulo)}</h2>")
        if s.texto:
            cuerpo.append(f"<p>{_en_linea(s.texto)}</p>")
        if s.figura:
            cuerpo.append(f'<figure>{s.figura[1]}</figure>')
        if s.filas:
            cabecera, *filas = s.filas
            cuerpo.append('<div class="tabla"><table><thead><tr>' + "".join(f"<th>{_en_linea(c)}</th>" for c in cabecera)
                          + "</tr></thead><tbody>" + "".join(
                              "<tr>" + "".join(f"<td>{_en_linea(c)}</td>" for c in f) + "</tr>" for f in filas)
                          + "</tbody></table></div>")
    return PLANTILLA_HTML.replace("__TITULO__", html.escape(informe.titulo)).replace("__CUERPO__", "\n".join(cuerpo))


def _en_linea(texto: str) -> str:
    """El markdown en línea que usan los textos del informe: `código` y **negrita**."""
    texto = html.escape(texto)
    texto = re.sub(r"`([^`]+)`", r"<code>\1</code>", texto)
    return re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", texto)


PLANTILLA_HTML = """<!doctype html>
<html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITULO__</title>
<style>
:root{color-scheme:light;--superficie:#fcfcfb;--texto:#0b0b0b;--texto-2:#52514e;--borde:#e1e0d9;--zebra:#f4f3ef}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){color-scheme:dark;--superficie:#1a1a19;--texto:#f0efec;--texto-2:#c3c2b7;--borde:#2c2c2a;--zebra:#222220}}
:root[data-theme="dark"]{color-scheme:dark;--superficie:#1a1a19;--texto:#f0efec;--texto-2:#c3c2b7;--borde:#2c2c2a;--zebra:#222220}
body{margin:0 auto;max-width:960px;padding:24px 16px;background:var(--superficie);color:var(--texto);
font:15px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}
h1{font-size:24px;margin:0 0 8px}h2{font-size:18px;margin:32px 0 8px}p{color:var(--texto-2);margin:6px 0}
code{font-size:13px}figure{margin:12px 0;overflow-x:auto}svg{max-width:100%;height:auto}
.tabla{overflow-x:auto}table{border-collapse:collapse;margin:8px 0;font-variant-numeric:tabular-nums}
th,td{border-bottom:1px solid var(--borde);padding:4px 10px;text-align:left;white-space:nowrap}
tbody tr:nth-child(even){background:var(--zebra)}
</style></head><body>
__CUERPO__
</body></html>
"""


# ---------------------------------------------------------------------- línea de órdenes

def _intervalo(texto: str | None, fin: bool) -> datetime | None:
    """`AAAA-MM-DD` (hora de Madrid; `--hasta` incluye el día entero) o fecha y hora ISO."""
    if not texto:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", texto):
        dia = date.fromisoformat(texto) + timedelta(days=1 if fin else 0)
        return datetime.combine(dia, time(), ZONA)
    momento = datetime.fromisoformat(texto)
    return momento if momento.tzinfo else momento.replace(tzinfo=ZONA)


def _cliente(url: str, usuario: str) -> httpx.Client:
    """Con usuario, la contraseña de la web (nginx protege /phoenix/) como *basic auth*."""
    auth = None
    if usuario:
        contrasena = os.getenv("PHOENIX_CONTRASENA") or getpass.getpass(f"Contraseña de {usuario}@{url}: ")
        auth = httpx.BasicAuth(usuario, contrasena)
    return httpx.Client(base_url=url, auth=auth, timeout=60)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    origen = parser.add_mutually_exclusive_group(required=True)
    origen.add_argument("--phoenix", help="URL de Phoenix: http://<ip>/phoenix (nginx) o http://localhost:6006")
    origen.add_argument("--jsonl", nargs="+", type=Path, help="ficheros JSONL de trazas en lugar de Phoenix")
    parser.add_argument("--proyecto", default="agente", help="proyecto de Phoenix (por defecto, agente)")
    parser.add_argument("--usuario", default="demo", help="usuario de la web; vacío = sin contraseña")
    parser.add_argument("--desde", help="AAAA-MM-DD o fecha y hora ISO (hora de Madrid si no lleva zona)")
    parser.add_argument("--hasta", help="AAAA-MM-DD (incluido) o fecha y hora ISO (excluida)")
    parser.add_argument("--detalle", action="store_true", help="añade una fila por turno (sin texto)")
    parser.add_argument("--salida", type=Path, help="fichero .md o .html; sin él, markdown por pantalla")
    args = parser.parse_args()
    formato = args.salida.suffix.lower() if args.salida else ".md"
    if formato not in (".md", ".html"):
        sys.exit(f"Extensión no soportada: {formato!r} (usa .md o .html)")
    desde, hasta = _intervalo(args.desde, fin=False), _intervalo(args.hasta, fin=True)

    if args.phoenix:
        try:
            with _cliente(args.phoenix, args.usuario) as cliente:
                spans = [a_formato_jsonl(s) for s in descargar(cliente, args.proyecto, desde, hasta)]
        except httpx.HTTPStatusError as exc:
            sys.exit(f"Phoenix respondió {exc.response.status_code} en {exc.request.url}"
                     + (" (¿contraseña?)" if exc.response.status_code == 401 else ""))
        except httpx.HTTPError as exc:
            sys.exit(f"No se pudo leer Phoenix en {args.phoenix}: {exc}")
        origen_texto = f"Phoenix, proyecto `{args.proyecto}`"
    else:
        spans = informe_trazas.cargar(args.jsonl)
        origen_texto = f"{len(args.jsonl)} fichero(s) JSONL"
    lista, huerfanas = informe_trazas.turnos(spans)
    if args.jsonl:  # Phoenix ya filtra por fecha en el servidor
        lista = [t for t in lista if (not desde or _fecha(t.raiz["start"]) >= desde)
                 and (not hasta or _fecha(t.raiz["start"]) < hasta)]
    if args.desde or args.hasta:
        origen_texto += f", intervalo {args.desde or '…'} – {args.hasta or '…'}"
    informe = construir(lista, huerfanas, origen_texto, args.detalle)

    if formato == ".html":
        texto, figuras = a_html(informe), {}
    else:
        texto, figuras = a_markdown(informe, args.salida.stem if args.salida else None)
    if not args.salida:
        print(texto)
        return
    args.salida.write_text(texto, encoding="utf-8")
    for nombre, svg in figuras.items():
        (args.salida.parent / nombre).write_text(svg, encoding="utf-8")
    print(f"Informe en {args.salida}" + (f" (+ {len(figuras)} figuras SVG)" if figuras else ""))


if __name__ == "__main__":
    main()
