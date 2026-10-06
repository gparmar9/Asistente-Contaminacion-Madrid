"""Informe agregado de los turnos del agente a partir de los JSONL de trazas (un span por línea).

Solo biblioteca estándar. Agrupa los spans por `trace_id` (un turno), reconstruye el árbol por
`parent_id` y saca tablas markdown por lote (`agente.etiqueta` del span `turno`):

- turnos por ruta e intención;
- latencia del turno y por fase: **mediana con n**; el p95 es solo descriptivo (muestras pequeñas);
- tokens y coste por turno con la tabla de precios de abajo. Si a una llamada al LLM le faltan
  los tokens (o su modelo no tiene precio), el turno queda **incompleto**: no cuenta en las
  medianas de tokens y su coste no se suma. Ausente nunca es cero;
- síntesis válida a la primera (/rag/validar), reparaciones, búsquedas y síntesis forzadas, herramientas vetadas,
  errores y decisiones del código.

El formato de `--salida` lo da la extensión: `.md` (estas tablas), `.html` (informe visual
autocontenido para analizar latencias y costes: filtros, gráficas y cascada de cada turno; sin
dependencias externas, se abre sin red) o `.json` (el conjunto de datos que dibuja el HTML; es el
contrato que podría servir una futura web de análisis). La plantilla es `plantilla_informe.html`.

Uso, desde la carpeta Agente/:
    .venv/bin/python evaluacion/informe_trazas.py trazas/*.jsonl
    .venv/bin/python evaluacion/informe_trazas.py trazas/*.jsonl --etiqueta ministral --salida informe.md
    .venv/bin/python evaluacion/informe_trazas.py trazas/*.jsonl --salida informe.html
    .venv/bin/python evaluacion/informe_trazas.py trazas/x.jsonl --traza-id 5534d0f4   # un turno
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# USD por millón de tokens (entrada, salida). Bedrock bajo demanda, región Irlanda (eu-west-1),
# página de precios consultada el 2026-10-04. Un modelo que no esté aquí deja el coste incompleto.
PRECIOS_FECHA = "2026-10-04, Bedrock eu-west-1"
PRECIOS = {
    "mistral.ministral-3-14b-instruct": (0.24, 0.24),
    "openai.gpt-oss-120b-1:0": (0.18, 0.70),
}

# Orden de las fases en la tabla de latencias; el resto de spans van detrás, por nombre.
FASES = ("clasificar", "bucle", "busqueda_forzada", "sintesis_documental", "sintesis_forzada", "validar")

PLANTILLA_HTML = Path(__file__).with_name("plantilla_informe.html")


@dataclass
class Turno:
    trace_id: str
    spans: list[dict]                 # todos los spans de la traza, en orden de inicio
    raiz: dict                        # span `turno`
    etiqueta: str
    ruta: str
    intencion: str
    duracion_ms: float
    error: bool
    modelos: set[str] = field(default_factory=set)
    tokens_entrada: int | None = None  # None = incompleto
    tokens_salida: int | None = None
    coste_usd: float | None = None     # None = incompleto o modelo sin precio

    def atributo(self, clave: str, defecto=None):
        return self.raiz["attributes"].get(clave, defecto)


# ---------------------------------------------------------------------- lectura

def cargar(rutas: list[Path]) -> list[dict]:
    spans = []
    for ruta in rutas:
        with ruta.open(encoding="utf-8") as f:
            spans.extend(json.loads(linea) for linea in f if linea.strip())
    return spans


def turnos(spans: list[dict]) -> tuple[list[Turno], int]:
    """Turnos completos y número de trazas sin span `turno` (proceso cortado a mitad)."""
    por_traza: dict[str, list[dict]] = defaultdict(list)
    for s in spans:
        por_traza[s["trace_id"]].append(s)
    resultado, huerfanas = [], 0
    for trace_id, propios in por_traza.items():
        propios.sort(key=lambda s: s["start"] or "")
        raiz = next((s for s in propios if s["name"] == "turno" and not s["parent_id"]), None)
        if raiz is None:
            huerfanas += 1
            continue
        a = raiz["attributes"]
        turno = Turno(trace_id=trace_id, spans=propios, raiz=raiz,
                      etiqueta=a.get("agente.etiqueta") or "sin_etiqueta",
                      ruta=a.get("agente.ruta", "error"), intencion=a.get("agente.intencion", "?"),
                      duracion_ms=raiz["duracion_ms"], error=raiz["status"] == "ERROR")
        _tokens_y_coste(turno)
        resultado.append(turno)
    resultado.sort(key=lambda t: t.raiz["start"] or "")
    return resultado, huerfanas


def _tokens_y_coste(turno: Turno) -> None:
    entrada = salida = 0
    coste = 0.0
    completo, con_precio = True, True
    for s in turno.spans:
        if s["kind"] != "LLM":
            continue
        a = s["attributes"]
        modelo = a.get("llm.model_name") or "?"
        turno.modelos.add(modelo)
        e, o = a.get("llm.token_count.prompt"), a.get("llm.token_count.completion")
        if not (isinstance(e, int) and isinstance(o, int)):
            completo = False
            continue
        entrada, salida = entrada + e, salida + o
        coste_llamada = _coste(modelo, e, o)
        if coste_llamada is None:
            con_precio = False
        else:
            coste += coste_llamada
    if completo:
        turno.tokens_entrada, turno.tokens_salida = entrada, salida
        if con_precio:
            turno.coste_usd = coste


def _coste(modelo: str, entrada: int, salida: int) -> float | None:
    if modelo not in PRECIOS:
        return None
    precio_e, precio_s = PRECIOS[modelo]
    return (entrada * precio_e + salida * precio_s) / 1e6


# ---------------------------------------------------------------------- informe

def informe(lista: list[Turno], huerfanas: int = 0) -> str:
    lotes: dict[str, list[Turno]] = defaultdict(list)
    for t in lista:
        lotes[t.etiqueta].append(t)
    partes = [f"# Informe de trazas del agente\n\n{len(lista)} turnos en {len(lotes)} lote(s). "
              f"Medianas con su n; el p95 es solo descriptivo. Precios: {PRECIOS_FECHA}."]
    if huerfanas:
        partes.append(f"Trazas sin span `turno` (descartadas): {huerfanas}.")
    partes.append(_resumen_lotes(lotes))
    for etiqueta, propios in lotes.items():
        partes.append(_detalle_lote(etiqueta, propios))
    return "\n\n".join(partes) + "\n"


def _resumen_lotes(lotes: dict[str, list[Turno]]) -> str:
    filas = [["Lote", "Modelo", "Turnos", "Latencia mediana (ms)", "p95 (ms)",
              "Tokens mediana (entrada / salida)", "Coste total (USD)", "Sin coste (tokens o precio)",
              "Síntesis válida a la primera"]]
    for etiqueta, lista in lotes.items():
        completos = [t for t in lista if t.tokens_entrada is not None]
        con_coste = [t for t in lista if t.coste_usd is not None]
        documentales = [t for t in lista if t.atributo("agente.valida") is not None]
        a_la_primera = sum(t.atributo("agente.valida") is True and t.atributo("agente.reparaciones") == 0
                           for t in documentales)
        filas.append([
            etiqueta, ", ".join(sorted(set().union(*(t.modelos for t in lista)))) or "—", str(len(lista)),
            _mediana([t.duracion_ms for t in lista]), _p95([t.duracion_ms for t in lista]),
            f"{_mediana([t.tokens_entrada for t in completos])} / {_mediana([t.tokens_salida for t in completos])}",
            _num(sum(t.coste_usd for t in con_coste), 4) if con_coste else "—",
            str(len(lista) - len(con_coste)),
            f"{a_la_primera}/{len(documentales)}",
        ])
    return "## Resumen por lote\n\n" + tabla(filas)


def _detalle_lote(etiqueta: str, lista: list[Turno]) -> str:
    partes = [f"## Lote `{etiqueta}`"]

    rutas = Counter((t.ruta, t.intencion) for t in lista)
    filas = [["Ruta", "Intención", "n", "Latencia mediana (ms)", "Tokens mediana (entrada / salida)",
              "Coste mediano (USD)"]]
    for (ruta, intencion), n in sorted(rutas.items()):
        grupo = [t for t in lista if (t.ruta, t.intencion) == (ruta, intencion)]
        completos = [t for t in grupo if t.tokens_entrada is not None]
        filas.append([ruta, intencion, str(n), _mediana([t.duracion_ms for t in grupo]),
                      f"{_mediana([t.tokens_entrada for t in completos])} / "
                      f"{_mediana([t.tokens_salida for t in completos])}",
                      _mediana([t.coste_usd for t in grupo if t.coste_usd is not None], 5)])
    partes.append("### Turnos por ruta e intención\n\n" + tabla(filas))

    duraciones: dict[str, list[float]] = defaultdict(list)
    for t in lista:
        nombres = {s["span_id"]: s["name"] for s in t.spans}
        for s in t.spans:
            if s is t.raiz or s["duracion_ms"] is None:
                continue
            nombre = s["name"]
            if s["kind"] == "LLM":  # la fase de una llamada al LLM la da su padre
                nombre = f"llm ({nombres.get(s['parent_id'], '?')})"
            duraciones[nombre].append(s["duracion_ms"])
    orden = {n: i for i, n in enumerate(FASES)}
    filas = [["Span", "n", "Mediana (ms)", "p95 (ms)"], ["turno", str(len(lista)),
             _mediana([t.duracion_ms for t in lista]), _p95([t.duracion_ms for t in lista])]]
    for nombre in sorted(duraciones, key=lambda n: (orden.get(n, len(orden)), n)):
        valores = duraciones[nombre]
        filas.append([nombre, str(len(valores)), _mediana(valores), _p95(valores)])
    partes.append("### Latencia por fase\n\n"
                  "Las llamadas al LLM, por la fase que las contiene; `llm (bucle)` es una por vuelta.\n\n"
                  + tabla(filas))

    documentales = [t for t in lista if t.atributo("agente.valida") is not None]
    herramientas = [s for t in lista for s in t.spans if s["kind"] == "TOOL" and s["name"] != "validar"]
    filas = [["Indicador", "Valor"],
             ["Síntesis documentales", str(len(documentales))],
             ["Válida a la primera (/rag/validar)", str(sum(t.atributo("agente.valida") is True
                                                  and t.atributo("agente.reparaciones") == 0 for t in documentales))],
             ["Válido tras reparación", str(sum(t.atributo("agente.valida") is True
                                                and t.atributo("agente.reparaciones", 0) > 0 for t in documentales))],
             ["Inválido tras reparar (insuficiencia)", str(sum(t.atributo("agente.valida") is False
                                                               for t in documentales))],
             ["Búsquedas forzadas por el código", str(sum(bool(t.atributo("agente.busqueda_forzada")) for t in lista))],
             ["Síntesis forzadas (límite de vueltas)", str(sum(bool(t.atributo("agente.sintesis_forzada")) for t in lista))],
             ["Llamadas a herramientas", str(len(herramientas))],
             ["Herramientas vetadas pedidas por el modelo",
              str(sum(s["attributes"].get("agente.permitida") is False for s in herramientas))],
             ["Herramientas con error", str(sum(s["status"] == "ERROR" for s in herramientas))],
             ["Turnos con error (excepción)", str(sum(t.error for t in lista))],
             ["Turnos con tokens incompletos", str(sum(t.tokens_entrada is None for t in lista))]]
    partes.append("### Calidad del turno\n\n" + tabla(filas))

    decisiones = Counter(e["attributes"].get("agente.decision", "?")
                         for t in lista for s in t.spans for e in s["events"] if e["name"] == "decision")
    if decisiones:
        filas = [["Decisión del código", "n"]] + [[d, str(n)] for d, n in decisiones.most_common()]
        partes.append("### Decisiones del código\n\n" + tabla(filas))
    return "\n\n".join(partes)


def detalle_traza(turno: Turno) -> str:
    """Un turno como tabla ordenada (alternativa a Phoenix en la terminal)."""
    hijos: dict[str | None, list[dict]] = defaultdict(list)
    for s in turno.spans:
        hijos[s["parent_id"]].append(s)
    inicio = _segundos(turno.raiz["start"])
    filas = [["Span", "Tipo", "Inicio (ms)", "Duración (ms)", "Tokens (e / s)", "Detalle"]]

    def recorrer(s: dict, nivel: int) -> None:
        a = s["attributes"]
        tokens = ""
        if s["kind"] == "LLM":
            tokens = f"{a.get('llm.token_count.prompt', '—')} / {a.get('llm.token_count.completion', '—')}"
        detalle = [f"{k.removeprefix('agente.')}={a[k]}" for k in sorted(a)
                   if k.startswith("agente.") and k != "agente.etiqueta"]
        detalle += [f"decision={e['attributes'].get('agente.decision')}" for e in s["events"] if e["name"] == "decision"]
        if s["status"] == "ERROR":
            detalle.append(f"ERROR: {s.get('status_descripcion') or ''}".strip())
        filas.append(["\u00a0\u00a0" * nivel + s["name"], s["kind"] or "",
                      _num((_segundos(s["start"]) - inicio) * 1000), _num(s["duracion_ms"]), tokens,
                      "; ".join(detalle).replace("|", "/")])
        for h in hijos[s["span_id"]]:
            recorrer(h, nivel + 1)

    recorrer(turno.raiz, 0)
    return (f"# Turno {turno.trace_id}\n\nPregunta: {turno.atributo('input.value', '—')}\n\n"
            f"Lote `{turno.etiqueta}` · ruta {turno.ruta} · intención {turno.intencion} · "
            f"tokens {turno.tokens_entrada if turno.tokens_entrada is not None else 'incompletos'} / "
            f"{turno.tokens_salida if turno.tokens_salida is not None else '—'} · coste "
            f"{_num(turno.coste_usd, 5) if turno.coste_usd is not None else '—'} USD\n\n" + tabla(filas) + "\n")


# ---------------------------------------------------------------------- datos e informe HTML

def datos(lista: list[Turno], huerfanas: int = 0) -> dict:
    """Conjunto de datos serializable que dibuja el HTML: un registro por turno con sus spans.

    Sin los mensajes al LLM ni las salidas de las herramientas (solo pregunta y respuesta del
    turno). Las agregaciones (medianas, p95, coste por fase) las hace la página para poder filtrar.
    """
    return {
        "generado": datetime.now().astimezone().isoformat(timespec="seconds"),
        "precios": {"fecha": PRECIOS_FECHA, "usd_por_millon": {m: list(p) for m, p in PRECIOS.items()}},
        "fases": list(FASES),
        "huerfanas": huerfanas,
        "turnos": [_datos_turno(t) for t in lista],
    }


def _datos_turno(turno: Turno) -> dict:
    por_id = {s["span_id"]: s for s in turno.spans}
    inicio = _segundos(turno.raiz["start"])

    def fase(s: dict) -> str:
        """Nombre del hijo directo del span `turno` que contiene a este span."""
        while s["parent_id"] in por_id and s["parent_id"] != turno.raiz["span_id"]:
            s = por_id[s["parent_id"]]
        return s["name"]

    spans = []
    for s in turno.spans:
        a = s["attributes"]
        padre = por_id.get(s["parent_id"])
        registro = {
            "id": s["span_id"], "padre": s["parent_id"], "nombre": s["name"], "tipo": s["kind"] or "",
            "grupo": f"llm ({padre['name'] if padre else '?'})" if s["kind"] == "LLM" else s["name"],
            "fase": "turno" if s is turno.raiz else fase(s),
            "inicio_ms": round((_segundos(s["start"]) - inicio) * 1000, 1),
            "duracion_ms": s["duracion_ms"], "error": s["status"] == "ERROR",
            "error_descripcion": s.get("status_descripcion"),
            "atributos": {k.removeprefix("agente."): v for k, v in a.items()
                          if k.startswith("agente.") and k != "agente.etiqueta"},
            "decisiones": [e["attributes"].get("agente.decision") for e in s["events"] if e["name"] == "decision"],
        }
        if s["kind"] == "LLM":
            e, o = a.get("llm.token_count.prompt"), a.get("llm.token_count.completion")
            completos = isinstance(e, int) and isinstance(o, int)
            registro.update(modelo=a.get("llm.model_name"), tokens_entrada=e if completos else None,
                            tokens_salida=o if completos else None,
                            coste_usd=_coste(a.get("llm.model_name") or "?", e, o) if completos else None)
        spans.append(registro)
    return {
        "id": turno.trace_id, "lote": turno.etiqueta, "modelos": sorted(turno.modelos),
        "ruta": turno.ruta, "intencion": turno.intencion, "tema": turno.atributo("agente.tema"),
        "pregunta": turno.atributo("input.value"), "respuesta": turno.atributo("output.value"),
        "inicio": turno.raiz["start"], "duracion_ms": turno.duracion_ms, "error": turno.error,
        "tokens_entrada": turno.tokens_entrada, "tokens_salida": turno.tokens_salida, "coste_usd": turno.coste_usd,
        "vueltas": turno.atributo("agente.vueltas"), "valida": turno.atributo("agente.valida"),
        "reparaciones": turno.atributo("agente.reparaciones"),
        "busqueda_forzada": bool(turno.atributo("agente.busqueda_forzada")),
        "sintesis_forzada": bool(turno.atributo("agente.sintesis_forzada")),
        "spans": spans,
    }


def informe_html(lista: list[Turno], huerfanas: int = 0) -> str:
    """La plantilla con los datos incrustados; `<` escapado para que ningún texto cierre el script."""
    contenido = json.dumps(datos(lista, huerfanas), ensure_ascii=False).replace("<", "\\u003c")
    return PLANTILLA_HTML.read_text(encoding="utf-8").replace("__DATOS__", contenido)


# ---------------------------------------------------------------------- utilidades

def tabla(filas: list[list[str]]) -> str:
    cabecera, *cuerpo = filas
    lineas = ["| " + " | ".join(cabecera) + " |", "|" + "-|" * len(cabecera)]
    lineas += ["| " + " | ".join(f) + " |" for f in cuerpo]
    return "\n".join(lineas)


def _mediana(valores: list[float], decimales: int = 0) -> str:
    return _num(statistics.median(valores), decimales) if valores else "—"


def _p95(valores: list[float]) -> str:
    """Rango más cercano: con pocas muestras coincide con el máximo."""
    if not valores:
        return "—"
    ordenados = sorted(valores)
    return _num(ordenados[math.ceil(0.95 * len(ordenados)) - 1])


def _num(valor: float | None, decimales: int = 0) -> str:
    if valor is None:
        return "—"
    return f"{valor:.{decimales}f}".replace(".", ",")


def _segundos(iso: str) -> float:
    return datetime.fromisoformat(iso).timestamp()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("jsonl", nargs="+", type=Path, help="ficheros JSONL de trazas")
    parser.add_argument("--etiqueta", action="append", help="solo estos lotes (se puede repetir)")
    parser.add_argument("--traza-id", help="imprime un turno (basta el principio del id)")
    parser.add_argument("--salida", type=Path,
                        help="fichero de salida; la extensión elige el formato: .md, .html o .json")
    args = parser.parse_args()
    formato = args.salida.suffix.lower() if args.salida else ".md"
    if formato not in (".md", ".html", ".json"):
        sys.exit(f"Extensión no soportada: {formato!r} (usa .md, .html o .json)")

    lista, huerfanas = turnos(cargar(args.jsonl))
    if args.etiqueta:
        lista = [t for t in lista if t.etiqueta in args.etiqueta]
    if args.traza_id:
        lista = [t for t in lista if t.trace_id.startswith(args.traza_id)]
        if len(lista) != 1:
            sys.exit(f"{len(lista)} turnos empiezan por {args.traza_id!r}")
    if formato == ".html":
        texto = informe_html(lista, huerfanas)
    elif formato == ".json":
        texto = json.dumps(datos(lista, huerfanas), ensure_ascii=False, indent=1)
    else:
        texto = detalle_traza(lista[0]) if args.traza_id else informe(lista, huerfanas)
    if args.salida:
        args.salida.write_text(texto, encoding="utf-8")
        print(f"Informe en {args.salida}")
    else:
        print(texto)


if __name__ == "__main__":
    main()
