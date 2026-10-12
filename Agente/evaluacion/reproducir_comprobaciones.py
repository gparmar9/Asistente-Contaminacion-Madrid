"""Comprobaciones posteriores sobre trazas ya guardadas (fuera de CI; sin LLM ni red).

Reconstruye cada turno desde los JSONL con lo que vio y escribió el modelo y le aplica las mismas
reglas que el `Bucle` (`agente.business.comprobaciones`), en modo observación:

- ruta libre: la respuesta (`output.value` del span `turno`) contra la pregunta y las salidas de
  las herramientas del turno;
- ruta documental válida: las afirmaciones y limitaciones de la última salida aceptada por
  `/rag/validar` (entrada del span `validar`), contra las evidencias renumeradas que devolvieron
  los spans `buscar_evidencias` (lo que vio la síntesis).

Las rutas `fija` y `sin_evidencia`, la documental no válida y los turnos sin texto en la traza
(`__REDACTED__`) no se comprueban, como en el turno real.

Sale un markdown con el conteo por lote, ruta y regla y cada hallazgo con una columna en blanco
para revisarlo a mano (acierto o falso positivo). En las cifras de la ruta documental dice además
si la cifra está en otra evidencia del turno (cita mal puesta) o en ninguna.

Uso, desde la carpeta Agente/:
    .venv/bin/python evaluacion/reproducir_comprobaciones.py trazas/*.jsonl
    .venv/bin/python evaluacion/reproducir_comprobaciones.py trazas/x.jsonl --salida hallazgos.md
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))
sys.path.insert(0, str(RAIZ))  # evaluacion.informe_trazas

from agente.business import comprobaciones  # noqa: E402
from agente.entities.comprobaciones import Hallazgo, MaterialTurno  # noqa: E402
from agente.tools import rag  # noqa: E402
from evaluacion import informe_trazas  # noqa: E402
from evaluacion.informe_trazas import Turno, tabla  # noqa: E402

RESULTADOS = Path(__file__).with_name("resultados")
OCULTO = "__REDACTED__"


@dataclass
class TurnoComprobado:
    turno: Turno
    material: MaterialTurno | None
    motivo: str = ""                                   # por qué no se comprobó (material None)
    hallazgos: list[tuple[int, Hallazgo]] = field(default_factory=list)  # (índice del texto, hallazgo)
    evidencias: list[dict] = field(default_factory=list)                 # documental: las del turno
    citas: list[list[str]] = field(default_factory=list)                 # documental: IDs por afirmación


# ---------------------------------------------------------------------- reconstrucción

def reconstruir(turno: Turno) -> TurnoComprobado:
    """El material del turno, como lo arma el `Bucle`. Sin material, el motivo."""
    pregunta = turno.atributo("input.value")
    if not isinstance(pregunta, str) or pregunta == OCULTO:
        return TurnoComprobado(turno, None, "sin texto en la traza")
    herramientas = [s for s in turno.spans if s["kind"] == "TOOL" and s["name"] != "validar"]

    if turno.ruta == "libre":
        respuesta = turno.atributo("output.value")
        if not isinstance(respuesta, str) or respuesta == OCULTO:
            return TurnoComprobado(turno, None, "sin texto en la traza")
        consultas = [f"{s['name']}: {s['attributes'].get('output.value', '')}" for s in herramientas]
        return TurnoComprobado(turno, comprobaciones.material_libre(pregunta, consultas, respuesta))

    if turno.ruta == "documental":
        salida = _salida_valida(turno)
        if salida is None:
            return TurnoComprobado(turno, None, "síntesis no válida o sin texto")
        evidencias = _evidencias(herramientas)
        afirmaciones = list(salida.get("afirmaciones") or [])
        limitaciones = list(salida.get("limitaciones") or [])
        material = comprobaciones.material_documental(pregunta, evidencias, afirmaciones, limitaciones)
        return TurnoComprobado(turno, material, evidencias=evidencias,
                               citas=[list(a.get("evidencias") or []) for a in afirmaciones])

    return TurnoComprobado(turno, None, f"ruta {turno.ruta}: el texto es del código")


def _salida_valida(turno: Turno) -> dict | None:
    """La salida del modelo que aceptó `/rag/validar` (el último `validar` válido)."""
    for s in reversed(turno.spans):
        if s["name"] == "validar" and s["attributes"].get("agente.valida") is True:
            try:
                salida = json.loads(s["attributes"]["input.value"])["salida"]
            except (KeyError, TypeError, ValueError):
                return None
            return salida if isinstance(salida, dict) else None
    return None


def _evidencias(herramientas: list[dict]) -> list[dict]:
    """Evidencias del turno sin repetir ID, en orden de llegada (ya renumeradas en el span)."""
    vistas: dict[str, dict] = {}
    for s in herramientas:
        if s["name"] != rag.NOMBRE:
            continue
        try:
            datos = json.loads(s["attributes"].get("output.value") or "")
        except ValueError:
            continue
        for e in datos.get("evidencias") or [] if isinstance(datos, dict) else []:
            vistas.setdefault(e["id"], e)
    return list(vistas.values())


def comprobar(tc: TurnoComprobado) -> TurnoComprobado:
    """Las reglas texto a texto, para saber qué texto hizo saltar cada hallazgo."""
    if tc.material is None:
        return tc
    for i, texto in enumerate(tc.material.textos):
        unico = MaterialTurno(tc.material.ruta, (texto,))
        tc.hallazgos += [(i, h) for h in comprobaciones.comprobar(unico, [rag.NOMBRE])]
    return tc


# ---------------------------------------------------------------------- informe

def informe(lista: list[TurnoComprobado], ficheros: list[Path]) -> str:
    comprobados = [tc for tc in lista if tc.material is not None]
    partes = [
        "# Comprobaciones posteriores sobre trazas guardadas\n\n"
        f"Generado: {datetime.now():%Y-%m-%d %H:%M} · {len(ficheros)} fichero(s) · {len(lista)} turnos, "
        f"{len(comprobados)} comprobados · reglas en observación (las mismas funciones que el `Bucle`). "
        f"Fuga: n-gramas de {comprobaciones.TAM_NGRAMA} palabras, umbral {comprobaciones.UMBRAL_FUGA}.",
        _sin_comprobar(lista),
        _resumen(comprobados),
        _revision(comprobados),
    ]
    return "\n\n".join(p for p in partes if p) + "\n"


def _sin_comprobar(lista: list[TurnoComprobado]) -> str:
    motivos = Counter(tc.motivo for tc in lista if tc.material is None)
    if not motivos:
        return ""
    return "## Turnos sin comprobar\n\n" + tabla([["Motivo", "Turnos"]]
                                                 + [[m, str(n)] for m, n in motivos.most_common()])


def _resumen(comprobados: list[TurnoComprobado]) -> str:
    """Por lote y ruta: turnos y textos comprobados, y turnos con hallazgo de cada regla."""
    grupos: dict[tuple[str, str], list[TurnoComprobado]] = {}
    for tc in comprobados:
        grupos.setdefault((tc.turno.etiqueta, tc.material.ruta), []).append(tc)
    filas = [["Lote", "Ruta", "Turnos", "Textos", *(f"Turnos con {r}" for r in comprobaciones.REGLAS)]]
    totales = Counter()
    for (lote, ruta), grupo in sorted(grupos.items()):
        conteo = {r: sum(any(h.regla == r for _, h in tc.hallazgos) for tc in grupo) for r in comprobaciones.REGLAS}
        textos = sum(len(tc.material.textos) for tc in grupo)
        totales.update({"turnos": len(grupo), "textos": textos, **conteo})
        filas.append([lote, ruta, str(len(grupo)), str(textos), *(str(conteo[r]) for r in comprobaciones.REGLAS)])
    filas.append(["**Total**", "", str(totales["turnos"]), str(totales["textos"]),
                  *(str(totales[r]) for r in comprobaciones.REGLAS)])
    return "## Hallazgos por lote, ruta y regla\n\n" + tabla(filas)


def _revision(comprobados: list[TurnoComprobado]) -> str:
    filas = [["#", "Lote", "Traza", "Ruta", "Regla", "Detalle", "Diagnóstico", "Revisión"]]
    bloques = []
    n = 0
    for tc in comprobados:
        for i, h in tc.hallazgos:
            n += 1
            texto = tc.material.textos[i]
            diagnostico = _diagnostico(tc, i, h)
            filas.append([str(n), tc.turno.etiqueta, tc.turno.trace_id[:8], tc.material.ruta, h.regla,
                          _celda(h.detalle), _celda(diagnostico), ""])
            bloque = [f"### {n} · {h.regla} · `{tc.turno.trace_id[:8]}` ({tc.turno.etiqueta}, {tc.material.ruta})",
                      f"Pregunta: {tc.turno.atributo('input.value')}",
                      f"Detalle: `{h.detalle}`" + (f" · {diagnostico}" if diagnostico else ""),
                      "\n".join("> " + linea for linea in texto.texto.splitlines() or [""])]
            bloques.append("\n\n".join(bloque))
    if not n:
        return "## Revisión manual\n\nNingún hallazgo."
    return ("## Revisión manual\n\nRellenar «Revisión» con *acierto* o *falso positivo* (y el motivo).\n\n"
            + tabla(filas) + "\n\n## Textos con hallazgo\n\n" + "\n\n".join(bloques))


def _diagnostico(tc: TurnoComprobado, i: int, h: Hallazgo) -> str:
    """Cifras en una afirmación documental: ¿están en otra evidencia del turno o en ninguna?"""
    if h.regla != comprobaciones.CIFRAS or tc.material.ruta != "documental":
        return ""
    if i >= len(tc.citas):
        return "limitación (base: todas las evidencias)"
    todas = "\n".join([tc.turno.atributo("input.value"), *(e.get("texto", "") for e in tc.evidencias)])
    citadas = ", ".join(tc.citas[i]) or "ninguna"
    sin_respaldo = h.detalle.split(", ")
    fuera = set(comprobaciones.cifras(tc.material.textos[i].texto, todas))
    en_otra = [c for c in sin_respaldo if c not in fuera]
    partes = [f"cita {citadas}"]
    if en_otra:
        partes.append(f"en otra evidencia: {', '.join(en_otra)}")
    if fuera:
        partes.append(f"en ninguna: {', '.join(c for c in sin_respaldo if c in fuera)}")
    return "; ".join(partes)


def _celda(texto: str) -> str:
    return texto.replace("|", "/").replace("\n", " ")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("jsonl", nargs="+", type=Path, help="ficheros JSONL de trazas")
    parser.add_argument("--salida", type=Path, help="markdown de salida (por defecto, en resultados/)")
    args = parser.parse_args()

    lista, _ = informe_trazas.turnos(informe_trazas.cargar(args.jsonl))
    resultado = [comprobar(reconstruir(t)) for t in lista]
    salida = args.salida or RESULTADOS / f"comprobaciones_{datetime.now():%Y%m%d_%H%M%S}.md"
    salida.parent.mkdir(exist_ok=True)
    salida.write_text(informe(resultado, args.jsonl), encoding="utf-8")
    hallazgos = Counter(h.regla for tc in resultado for _, h in tc.hallazgos)
    print(f"{sum(tc.material is not None for tc in resultado)}/{len(resultado)} turnos comprobados; "
          f"hallazgos: {dict(hallazgos) or 'ninguno'}")
    print(f"Resultado en {salida}")


if __name__ == "__main__":
    main()
