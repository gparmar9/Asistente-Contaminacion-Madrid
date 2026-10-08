"""Evaluación de la recuperación del RAG sobre un juego de casos fijo.

Mide, sin LLM, si la búsqueda vectorial devuelve las secciones correctas y si el
umbral de evidencia (`RAG_UMBRAL_DISTANCIA`) separa las preguntas con respuesta en
el corpus de las que no la tienen.

Casos: `tests/evals/rag_casos.jsonl`, una línea por caso:

    {"id": "doc-01", "tipo": "documental", "pregunta": "...",
     "esperados": ["ozono_salud:efectos-en-la-salud"]}

- `documental`: tiene respuesta en el corpus. `esperados` lista las secciones que la
  responden como `documento:seccion` (el chunk_id sin el ordinal).
- `ajena`: fuera del dominio (tiempo, tráfico, otra ciudad...). `esperados` vacío.
- `adversaria`: parece del corpus pero no tiene respuesta en él ni en los datos
  (predicciones, normativa de otra ciudad...). `esperados` vacío.

Medidas. Una búsqueda por caso con `k=K_BUSQUEDA`; las de posición miran los
`K_METRICA` primeros, que es lo que recibe el LLM en producción:

| Medida | Definición | Casos |
|-|-|-|
| hit@4 | alguna sección esperada entre los 4 primeros | documentales |
| MRR@10 | media de 1/posición de la primera sección esperada; 0 si no está | documentales |
| hit@4 con umbral | hit@4 y esa sección con distancia <= umbral | documentales |
| ajenas rechazadas | mejor distancia > umbral | ajenas |
| adversarias con evidencias | mejor distancia <= umbral (informativo) | adversarias |

Calibración exploratoria: punto medio del hueco entre la peor mejor-distancia
documental y la mejor mejor-distancia ajena (`punto_medio`).

Las funciones puras (sin torch ni Chroma) se prueban en CI con resultados
fabricados (`tests/test_evaluar.py`). La ejecución con el índice real está en `main()` con sus importaciones dentro, para no cargarlas en los tests.

Uso:
    python -m rag.evaluar                          # umbral de RAG_UMBRAL_DISTANCIA
    python -m rag.evaluar --umbral 0.22 0.1754
    python -m rag.evaluar --figura                 # además, docs/rag/figuras/distancias_por_tipo.png
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from rag.corpus import BASE_DIR

RUTA_CASOS = BASE_DIR / "tests" / "evals" / "rag_casos.jsonl"
TIPOS = ("documental", "ajena", "adversaria")
K_BUSQUEDA = 10  # resultados pedidos por caso (MRR@10)
K_METRICA = 4    # posiciones que cuentan para hit@k; debe coincidir con buscar.K_POR_DEFECTO
CAMPOS_INDICE = ("modelo_embeddings", "commit", "fecha_indexado", "n_fragmentos")


# --------------------------------------------------------------------------- casos

@dataclass(frozen=True)
class Caso:
    id: str
    tipo: str
    pregunta: str
    esperados: tuple[str, ...]


def cargar_casos(ruta: Path = RUTA_CASOS) -> list[Caso]:
    """Lee el JSONL. Comprueba la forma; la validez frente al corpus la prueba
    `tests/test_rag_casos.py`."""
    ruta = Path(ruta)
    casos: list[Caso] = []
    for n, linea in enumerate(ruta.read_text(encoding="utf-8").splitlines(), 1):
        if not linea.strip():
            continue
        try:
            d = json.loads(linea)
        except json.JSONDecodeError as e:
            raise ValueError(f"{ruta.name}:{n}: JSON inválido: {e}") from e
        faltan = {"id", "tipo", "pregunta", "esperados"} - set(d)
        if faltan:
            raise ValueError(f"{ruta.name}:{n}: faltan las claves {sorted(faltan)}")
        casos.append(Caso(str(d["id"]), str(d["tipo"]), str(d["pregunta"]), tuple(d["esperados"])))
    return casos


def seccion_de(chunk_id: str) -> str:
    """'ozono_salud:efectos-en-la-salud:0' -> 'ozono_salud:efectos-en-la-salud'."""
    partes = chunk_id.split(":")
    if len(partes) != 3 or not partes[2].isdigit():
        raise ValueError(f"chunk_id con formato inesperado: {chunk_id!r}")
    return f"{partes[0]}:{partes[1]}"


# --------------------------------------------------------------------------- por caso

@dataclass(frozen=True)
class FilaCaso:
    id: str
    tipo: str
    mejor_distancia: float | None   # None si la búsqueda no devolvió nada
    posicion: int | None            # 1-based de la primera sección esperada; None si no está
    distancia_esperada: float | None
    hit: bool                       # hit@k sin umbral
    hit_umbral: bool                # hit@k y distancia_esperada <= umbral
    rr: float                       # 1/posicion (0 si no está), para el MRR
    rechazada: bool                 # mejor_distancia > umbral o sin resultados


def evaluar_caso(caso: Caso, resultados: list[dict], umbral: float, k: int = K_METRICA) -> FilaCaso:
    """Evalúa un caso con la lista que devuelve `buscar` (dicts con chunk_id y distancia)."""
    orden = sorted(resultados, key=lambda r: r["distancia"])
    mejor = orden[0]["distancia"] if orden else None
    esperadas = set(caso.esperados)
    posicion: int | None = None
    distancia: float | None = None
    for i, r in enumerate(orden, 1):
        if seccion_de(r["chunk_id"]) in esperadas:
            posicion, distancia = i, r["distancia"]
            break
    hit = posicion is not None and posicion <= k
    return FilaCaso(
        id=caso.id,
        tipo=caso.tipo,
        mejor_distancia=mejor,
        posicion=posicion,
        distancia_esperada=distancia,
        hit=hit,
        hit_umbral=hit and distancia <= umbral,
        rr=1 / posicion if posicion else 0.0,
        rechazada=mejor is None or mejor > umbral,
    )


# --------------------------------------------------------------------------- resumen

@dataclass(frozen=True)
class Fraccion:
    aciertos: int
    total: int

    def __str__(self) -> str:
        return f"{self.aciertos}/{self.total}"


@dataclass(frozen=True)
class Resumen:
    hit: Fraccion                        # documentales
    mrr: float                           # documentales; 0.0 si no hay
    hit_umbral: Fraccion                 # documentales
    ajenas_rechazadas: Fraccion          # ajenas
    adversarias_con_evidencias: Fraccion  # adversarias (informativo)


def _fraccion(filas: list[FilaCaso], condicion) -> Fraccion:
    return Fraccion(sum(1 for f in filas if condicion(f)), len(filas))


def resumir(filas: list[FilaCaso]) -> Resumen:
    doc = [f for f in filas if f.tipo == "documental"]
    aje = [f for f in filas if f.tipo == "ajena"]
    adv = [f for f in filas if f.tipo == "adversaria"]
    return Resumen(
        hit=_fraccion(doc, lambda f: f.hit),
        mrr=sum(f.rr for f in doc) / len(doc) if doc else 0.0,
        hit_umbral=_fraccion(doc, lambda f: f.hit_umbral),
        ajenas_rechazadas=_fraccion(aje, lambda f: f.rechazada),
        adversarias_con_evidencias=_fraccion(adv, lambda f: not f.rechazada),
    )


# --------------------------------------------------------------------------- calibración

@dataclass(frozen=True)
class Calibracion:
    peor_documental: float      # mayor mejor-distancia entre las documentales
    id_peor_documental: str
    mejor_ajena: float          # menor mejor-distancia entre las ajenas
    id_mejor_ajena: str
    umbral_propuesto: float     # punto medio entre ambas
    hueco: float                # mejor_ajena - peor_documental; negativo si se solapan
    solapados: tuple[str, ...]  # ids que caen al otro lado del hueco (vacío si hueco > 0)


def punto_medio(filas: list[FilaCaso]) -> Calibracion:
    """Umbral propuesto: punto medio del hueco entre documentales y ajenas."""
    doc = [f for f in filas if f.tipo == "documental" and f.mejor_distancia is not None]
    aje = [f for f in filas if f.tipo == "ajena" and f.mejor_distancia is not None]
    if not doc or not aje:
        raise ValueError("hacen falta documentales y ajenas con resultados para calibrar")
    peor = max(doc, key=lambda f: f.mejor_distancia)
    mejor = min(aje, key=lambda f: f.mejor_distancia)
    solapados = tuple(
        [f.id for f in doc if f.mejor_distancia >= mejor.mejor_distancia]
        + [f.id for f in aje if f.mejor_distancia <= peor.mejor_distancia]
    )
    return Calibracion(
        peor_documental=peor.mejor_distancia,
        id_peor_documental=peor.id,
        mejor_ajena=mejor.mejor_distancia,
        id_mejor_ajena=mejor.id,
        umbral_propuesto=(peor.mejor_distancia + mejor.mejor_distancia) / 2,
        hueco=mejor.mejor_distancia - peor.mejor_distancia,
        solapados=solapados,
    )


# --------------------------------------------------------------------------- informe

def _num(x: float | int | None, decimales: int = 3) -> str:
    return "—" if x is None else f"{x:.{decimales}f}"


def _sino(valor: bool) -> str:
    return "sí" if valor else "no"


def informe_markdown(meta_indice: dict, umbrales: list[float], filas: list[FilaCaso],
                     resumenes: list[Resumen], calibracion: Calibracion | None = None) -> str:
    """Informe en Markdown: índice, resumen por umbral, calibración y tabla por caso.

    `filas` son las del primer umbral (la posición y la mejor distancia no dependen
    del umbral; `hit_umbral` y `rechazada` sí). `resumenes` va alineado con `umbrales`.
    """
    if len(umbrales) != len(resumenes):
        raise ValueError("umbrales y resumenes deben tener la misma longitud")
    n_tipo = {t: sum(1 for f in filas if f.tipo == t) for t in TIPOS}
    lineas = [
        "# Evaluación de la recuperación del RAG",
        "",
        "Índice: " + ", ".join(f"{c}={meta_indice.get(c)}" for c in CAMPOS_INDICE),
        f"Casos: {len(filas)} ({n_tipo['documental']} documentales, {n_tipo['ajena']} ajenas, "
        f"{n_tipo['adversaria']} adversarias). Búsqueda con k={K_BUSQUEDA}; posición medida "
        f"sobre los {K_METRICA} primeros.",
        "Umbrales: " + ", ".join(_num(u, 4) for u in umbrales),
        "",
        "## Resumen",
        "",
        "| Medida | " + " | ".join(_num(u, 4) for u in umbrales) + " |",
        "|-|" + "-|" * len(umbrales),
    ]
    medidas = [
        (f"hit@{K_METRICA} (documentales)", lambda r: str(r.hit)),
        (f"MRR@{K_BUSQUEDA} (documentales)", lambda r: _num(r.mrr)),
        (f"hit@{K_METRICA} con umbral (documentales)", lambda r: str(r.hit_umbral)),
        ("Ajenas rechazadas", lambda r: str(r.ajenas_rechazadas)),
        ("Adversarias con evidencias (informativo)", lambda r: str(r.adversarias_con_evidencias)),
    ]
    for nombre, valor in medidas:
        lineas.append(f"| {nombre} | " + " | ".join(valor(r) for r in resumenes) + " |")

    if calibracion is not None:
        c = calibracion
        lineas += [
            "",
            "## Calibración (punto medio del hueco)",
            "",
            f"Peor documental: {_num(c.peor_documental, 4)} ({c.id_peor_documental}). "
            f"Mejor ajena: {_num(c.mejor_ajena, 4)} ({c.id_mejor_ajena}). "
            f"Hueco: {_num(c.hueco, 4)}. Umbral propuesto: {_num(c.umbral_propuesto, 4)}.",
        ]
        if c.solapados:
            lineas.append("Casos que se solapan: " + ", ".join(c.solapados) + ".")

    lineas += [
        "",
        f"## Casos (umbral {_num(umbrales[0], 4)}, ordenados por mejor distancia)",
        "",
        f"| id | tipo | mejor distancia | posición esperada | distancia esperada | hit@{K_METRICA} "
        f"| hit@{K_METRICA} umbral | rechazada |",
        "|-|-|-|-|-|-|-|-|",
    ]
    sin_resultado = float("inf")
    for f in sorted(filas, key=lambda f: f.mejor_distancia if f.mejor_distancia is not None else sin_resultado):
        documental = f.tipo == "documental"
        lineas.append(
            f"| {f.id} | {f.tipo} | {_num(f.mejor_distancia)} | "
            f"{f.posicion if f.posicion is not None else '—'} | {_num(f.distancia_esperada)} | "
            f"{_sino(f.hit) if documental else '—'} | {_sino(f.hit_umbral) if documental else '—'} | "
            f"{_sino(f.rechazada) if not documental else '—'} |"
        )
    return "\n".join(lineas) + "\n"


# --------------------------------------------------------------------------- figura

RUTA_FIGURA = BASE_DIR / "docs" / "rag" / "figuras" / "distancias_por_tipo.png"


def dibujar_figura(filas: list[FilaCaso], meta_indice: dict, umbrales: list[float],
                   calibracion: Calibracion | None, ruta: Path = RUTA_FIGURA) -> Path:
    """Mejor distancia de cada caso en una franja por tipo, con los umbrales pedidos y
    el punto medio propuesto (si no coincide con ninguno de ellos). Documentales sin
    hit@k con marcador hueco. Llevan su id los casos que
    delimitan el hueco, los solapados y las documentales sin hit@k.

    matplotlib se importa aquí: es un dibujo de desarrollo, no entra en CI.
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    ruta = Path(ruta)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    etiquetados = {f.id for f in filas if f.tipo == "documental" and not f.hit}
    if calibracion is not None:
        etiquetados |= {calibracion.id_peor_documental, calibracion.id_mejor_ajena, *calibracion.solapados}
    colores = {"documental": "#1f77b4", "ajena": "#d62728", "adversaria": "#ff7f0e"}
    fig, ax = plt.subplots(figsize=(8, 3.2))
    for y, tipo in enumerate(reversed(TIPOS)):
        grupo = [f for f in filas if f.tipo == tipo and f.mejor_distancia is not None]
        for i, f in enumerate(sorted(grupo, key=lambda f: f.mejor_distancia)):
            dy = (i % 3 - 1) * 0.12  # separa puntos casi coincidentes
            hueco = tipo == "documental" and not f.hit
            ax.scatter(f.mejor_distancia, y + dy, s=36, zorder=3,
                       facecolors="none" if hueco else colores[tipo], edgecolors=colores[tipo])
            if f.id in etiquetados:
                arriba = dy >= 0  # la etiqueta se aleja de la fila para no tapar puntos
                ax.annotate(f.id, (f.mejor_distancia, y + dy), xytext=(0, 7 if arriba else -7),
                            textcoords="offset points", ha="center",
                            va="bottom" if arriba else "top", fontsize=7)
    for u, estilo in zip(umbrales, ["--", ":", "-."]):
        ax.axvline(u, color="grey", linestyle=estilo, linewidth=1, label=f"umbral {u:.4f}")
    if calibracion is not None and all(abs(calibracion.umbral_propuesto - u) >= 5e-5 for u in umbrales):
        ax.axvline(calibracion.umbral_propuesto, color="black", linewidth=1,
                   label=f"punto medio {calibracion.umbral_propuesto:.4f}")
    ax.set_yticks(range(len(TIPOS)), list(reversed(TIPOS)))
    ax.set_ylim(-0.6, len(TIPOS) - 0.4)
    ax.set_xlabel("mejor distancia coseno del caso (menor = más parecido)")
    fecha = str(meta_indice.get("fecha_indexado") or "")[:10]
    ax.set_title(f"{meta_indice.get('modelo_embeddings')} · índice {fecha}", fontsize=9)
    ax.grid(axis="x", alpha=0.3)
    ax.legend(fontsize=8, loc="lower right")
    fig.tight_layout()
    fig.savefig(ruta, dpi=150)
    plt.close(fig)
    return ruta


# --------------------------------------------------------------------------- ejecución

def _meta_indice() -> dict:
    from rag import embeddings

    coleccion = embeddings.obtener_coleccion()
    meta = dict(coleccion.metadata or {})
    return {
        "modelo_embeddings": meta.get("modelo_embeddings"),
        "commit": meta.get("commit"),
        "fecha_indexado": meta.get("fecha_indexado"),
        "n_fragmentos": coleccion.count(),
    }


def main() -> None:
    import argparse

    # Importaciones con torch y Chroma aquí, para que los tests de las funciones puras no las paguen.
    from rag.buscar import buscar
    from rag.evidencias import umbral_distancia

    parser = argparse.ArgumentParser(description="Evaluación de la recuperación del RAG.")
    parser.add_argument("--casos", type=Path, default=RUTA_CASOS)
    parser.add_argument("--umbral", type=float, nargs="+", action="extend",
                        help="uno o varios; por defecto el de RAG_UMBRAL_DISTANCIA")
    parser.add_argument("--figura", type=Path, nargs="?", const=RUTA_FIGURA, default=None,
                        help=f"guarda la figura de distancias por tipo (por defecto {RUTA_FIGURA.relative_to(BASE_DIR)})")
    args = parser.parse_args()

    umbrales = args.umbral or [umbral_distancia()]
    casos = cargar_casos(args.casos)
    resultados = {c.id: buscar(c.pregunta, k=K_BUSQUEDA, tema=None) for c in casos}
    filas_por_umbral = [[evaluar_caso(c, resultados[c.id], u) for c in casos] for u in umbrales]
    calibracion = punto_medio(filas_por_umbral[0])
    meta = _meta_indice()
    print(informe_markdown(meta, umbrales, filas_por_umbral[0],
                           [resumir(f) for f in filas_por_umbral], calibracion))
    if args.figura is not None:
        print(f"Figura: {dibujar_figura(filas_por_umbral[0], meta, umbrales, calibracion, args.figura)}")


if __name__ == "__main__":
    main()
