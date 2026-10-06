"""Evaluación pequeña: turnos completos contra el `Bucle` en proceso (fuera de CI: usa red y claves).

Cada caso de `casos_turno.json` declara lo esperado: intención, ruta, si se busca en el RAG y,
en las preguntas documentales, si la respuesta cita documentos. El script lo comprueba, guarda
las trazas del lote en un JSONL (la etiqueta del lote es también el proyecto de Phoenix) y
escribe en `resultados/` un markdown fechado con tres criterios separados:

- JSON a la primera: la primera salida de la síntesis documental es un objeto JSON (se lee en
  la entrada del primer span `validar`).
- Referencias válidas: `/rag/validar` acepta la salida final, con su número de reparaciones.
- Respuesta correcta: lectura humana (sí / no / parcial). El markdown deja la columna en blanco.

Sin jueces LLM. Al final va el informe de trazas del lote (latencia, tokens y coste).

Uso, desde la carpeta Agente/, con rag.api levantado y el proveedor en el entorno:
    LLM_PROVEEDOR=bedrock LLM_MODELO=mistral.ministral-3-14b-instruct AWS_REGION=eu-west-1 \\
      AWS_PROFILE=pontia RAG_URL=http://localhost:8010 .venv/bin/python evaluacion/evaluar_turnos.py
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path

import httpx

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))
sys.path.insert(0, str(RAIZ))  # evaluacion.informe_trazas

from agente import observabilidad  # noqa: E402
from agente.business.bucle import Bucle, LLMNoDisponible, ResultadoTurno  # noqa: E402
from agente.config.settings import get_settings  # noqa: E402
from agente.tools import rag  # noqa: E402
from evaluacion import informe_trazas  # noqa: E402

CASOS = Path(__file__).with_name("casos_turno.json")
RESULTADOS = Path(__file__).with_name("resultados")


@dataclass
class ResultadoCaso:
    caso: dict
    resultado: ResultadoTurno | None = None
    fallos: list[str] = field(default_factory=list)   # diferencias con lo esperado
    duracion_ms: float = 0.0
    json_a_la_primera: bool | None = None             # None = sin síntesis documental o sin texto en la traza

    @property
    def cumple(self) -> bool:
        return not self.fallos


def comprobar(esperado: dict, r: ResultadoTurno) -> list[str]:
    """Una línea por cada clave de `esperado` que no coincide con lo obtenido."""
    obtenido = {"intencion": r.intencion, "ruta": r.ruta,
                "busqueda": rag.NOMBRE in r.herramientas_usadas,
                "cita": any(f.tipo == "documento" for f in r.fuentes)}
    return [f"{clave}: esperado {_si_no(valor)}, obtenido {_si_no(obtenido[clave])}"
            for clave, valor in esperado.items() if obtenido[clave] != valor]


async def evaluar_caso(bucle: Bucle, caso: dict) -> ResultadoCaso:
    rc = ResultadoCaso(caso=caso)
    inicio = time.perf_counter()
    try:
        rc.resultado = await bucle.responder(caso["pregunta"])
        rc.fallos = comprobar(caso["esperado"], rc.resultado)
    except LLMNoDisponible as exc:
        rc.fallos = [f"error: LLM no disponible ({exc})"]
    rc.duracion_ms = (time.perf_counter() - inicio) * 1000
    return rc


def json_a_la_primera(turno: informe_trazas.Turno) -> bool | None:
    """¿La primera salida de la síntesis fue un objeto JSON? La lee del primer span `validar`."""
    validar = next((s for s in turno.spans if s["name"] == "validar"), None)
    if validar is None:
        return None
    try:
        return isinstance(json.loads(validar["attributes"]["input.value"])["salida"], dict)
    except (KeyError, TypeError, ValueError):  # texto ocultado (__REDACTED__) o entrada sin salida
        return None


# ---------------------------------------------------------------------- informe

def informe_lote(resultados: list[ResultadoCaso], cabecera: str, trazas: str) -> str:
    sintesis = [rc for rc in resultados if rc.resultado and rc.resultado.valida is not None]
    partes = [cabecera, "## Comprobaciones automáticas",
              f"Cumplen lo esperado: {sum(rc.cumple for rc in resultados)}/{len(resultados)}. "
              f"Síntesis documentales: {len(sintesis)}; JSON a la primera "
              f"{sum(rc.json_a_la_primera is True for rc in sintesis)}/{len(sintesis)}; referencias válidas "
              f"{sum(rc.resultado.valida for rc in sintesis)}/{len(sintesis)} "
              f"(reparaciones: {sum(rc.resultado.reparaciones for rc in sintesis)})."]
    filas = [["Caso", "Intención", "Ruta", "Búsqueda", "Cita", "JSON a la primera", "Referencias válidas",
              "Reparaciones", "ms", "Fallos"]]
    for rc in resultados:
        r = rc.resultado
        if r is None:
            filas.append([rc.caso["id"]] + ["—"] * 8 + ["; ".join(rc.fallos)])
            continue
        filas.append([rc.caso["id"], r.intencion, r.ruta, _si_no(rag.NOMBRE in r.herramientas_usadas),
                      _si_no(any(f.tipo == "documento" for f in r.fuentes)), _si_no(rc.json_a_la_primera),
                      _si_no(r.valida), str(r.reparaciones) if r.valida is not None else "—",
                      f"{rc.duracion_ms:.0f}", "; ".join(rc.fallos) or "ok"])
    partes.append(informe_trazas.tabla(filas))

    partes.append("## Revisión manual: respuesta correcta\n\n"
                  "Rellenar con sí / no / parcial después de leer cada respuesta.")
    partes.append(informe_trazas.tabla([["Caso", "Correcta", "Nota"]]
                                        + [[rc.caso["id"], "", ""] for rc in resultados]))
    for rc in resultados:
        r = rc.resultado
        bloque = [f"### {rc.caso['id']} · {rc.caso['pregunta']}"]
        if r is None:
            bloque.append("Sin respuesta: " + "; ".join(rc.fallos))
        else:
            bloque.append(f"Ruta {r.ruta} · traza `{r.traza_id}`"
                          + (f" · fuentes: {', '.join(f.referencia for f in r.fuentes)}" if r.fuentes else ""))
            bloque.append("\n".join("> " + linea for linea in r.respuesta.splitlines()))
        partes.append("\n\n".join(bloque))

    partes.append("## Informe de trazas del lote\n\n" + re.sub(r"^(#+) ", r"##\1 ", trazas, flags=re.M))
    return "\n\n".join(partes) + "\n"


# ---------------------------------------------------------------------- lote

async def ejecutar(args: argparse.Namespace) -> None:
    from agente.main import construir_bucle  # aquí y no arriba: el test no necesita FastAPI

    settings = get_settings()
    etiqueta = args.etiqueta or re.sub(r"[^a-z0-9]+", "-", settings.llm_modelo.lower()).strip("-") or "lote"
    settings = replace(settings, traza_etiqueta=etiqueta, phoenix_proyecto=etiqueta,
                       trazas_ruta=settings.trazas_ruta or str(RAIZ / "trazas"))
    if not settings.rag_url:
        sys.exit("Falta RAG_URL: sin RAG las preguntas documentales no se pueden evaluar")
    try:
        httpx.get(settings.rag_url.rstrip("/") + "/salud", timeout=5).raise_for_status()
    except httpx.HTTPError as exc:
        sys.exit(f"El RAG no responde en {settings.rag_url} ({exc}): espera a que cargue")
    bucle = construir_bucle(settings)
    if bucle is None:
        sys.exit("LLM sin configurar: revisa LLM_PROVEEDOR y LLM_MODELO")
    fichero = observabilidad.configurar(settings)

    casos = json.loads(args.casos.read_text(encoding="utf-8"))
    if args.caso:
        casos = [c for c in casos if c["id"] in args.caso]
    inicio = datetime.now()
    print(f"Lote {etiqueta}: {len(casos)} casos · {settings.llm_proveedor} · {settings.llm_modelo}\n")
    resultados = []
    for caso in casos:
        rc = await evaluar_caso(bucle, caso)
        resultados.append(rc)
        ruta = rc.resultado.ruta if rc.resultado else "error"
        print(f"{'ok ' if rc.cumple else 'MAL'} {rc.duracion_ms:6.0f} ms  {caso['id']:<7} {ruta:<13} "
              f"{'; '.join(rc.fallos)}")
    observabilidad.cerrar()  # vacía los spans pendientes al JSONL

    trazas = "Sin fichero de trazas."
    if fichero is not None and fichero.exists():
        lista, huerfanas = informe_trazas.turnos(informe_trazas.cargar([fichero]))
        por_traza = {t.trace_id: t for t in lista}
        for rc in resultados:
            if rc.resultado and rc.resultado.traza_id in por_traza:
                rc.json_a_la_primera = json_a_la_primera(por_traza[rc.resultado.traza_id])
        trazas = informe_trazas.informe(lista, huerfanas)

    cabecera = (f"# Evaluación de turnos: `{etiqueta}`\n\n"
                f"Fecha: {inicio:%Y-%m-%d %H:%M} · proveedor {settings.llm_proveedor} · modelo "
                f"{settings.llm_modelo} · LLM_MAX_TOKENS {settings.llm_max_tokens} · casos: "
                f"`{args.casos.name}` ({len(casos)}) · trazas: `{fichero.name if fichero else '—'}`")
    RESULTADOS.mkdir(exist_ok=True)
    salida = RESULTADOS / f"turnos_{etiqueta}_{inicio:%Y%m%d_%H%M%S}.md"
    salida.write_text(informe_lote(resultados, cabecera, trazas), encoding="utf-8")
    print(f"\nCumplen lo esperado: {sum(rc.cumple for rc in resultados)}/{len(resultados)}")
    print(f"Resultado en {salida}")


def _si_no(valor) -> str:
    if valor is None:
        return "—"
    if isinstance(valor, bool):
        return "sí" if valor else "no"
    return str(valor)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--casos", type=Path, default=CASOS, help="guion de casos (JSON)")
    parser.add_argument("--etiqueta", help="nombre del lote; por defecto, el modelo")
    parser.add_argument("--caso", action="append", help="solo estos casos por id (se puede repetir)")
    asyncio.run(ejecutar(parser.parse_args()))


if __name__ == "__main__":
    main()
