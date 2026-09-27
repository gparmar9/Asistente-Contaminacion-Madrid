"""Reconstruye el índice Chroma completo desde `data/rag`.

Uso:
    python -m rag.indexar

Flujo (plan §2.2): leer y validar el corpus → trocear con guardia de tokens →
calcular embeddings → SOLO ENTONCES borrar la colección anterior y escribir la
nueva → imprimir resumen. Si el cálculo de embeddings falla, el índice previo
queda intacto.

La colección guarda como metadatos el modelo de embeddings, el commit del corpus
y la fecha, de modo que `buscar` pueda rechazar un índice construido con otro
modelo y cualquier resultado sea trazable al corpus que lo generó.
"""
from __future__ import annotations

import datetime as dt
import subprocess
from pathlib import Path

from rag.corpus import RUTA_CORPUS, Fragmento, cargar_corpus, trocear_corpus
from rag.embeddings import (
    BASE_DIR,
    MODELO_EMBEDDINGS,
    NOMBRE_COLECCION,
    RUTA_CHROMA,
    contar_tokens,
    embed_pasajes,
    presupuesto_tokens,
    recrear_coleccion,
)
from rag.errores import CorpusInvalido

TAMANO_LOTE = 64


def commit_corpus() -> str:
    """Commit corto de HEAD; añade '-dirty' si data/rag tiene cambios sin commit."""
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], cwd=BASE_DIR,
            capture_output=True, text=True, check=True, timeout=10,
        ).stdout.strip()
        estado = subprocess.run(
            ["git", "status", "--porcelain", "--", "data/rag"], cwd=BASE_DIR,
            capture_output=True, text=True, check=True, timeout=10,
        ).stdout.strip()
        return f"{sha}-dirty" if estado else sha
    except Exception:
        return "desconocido"


def metadatos_a_chroma(metadatos: dict) -> dict:
    """Chroma solo admite str/int/float/bool: la lista `contaminantes` va como CSV."""
    plano = dict(metadatos)
    plano["contaminantes"] = ",".join(metadatos.get("contaminantes") or [])
    return plano


def metadatos_desde_chroma(metadatos: dict) -> dict:
    """Inverso de `metadatos_a_chroma`: recupera `contaminantes` como lista."""
    rico = dict(metadatos)
    csv = rico.get("contaminantes") or ""
    rico["contaminantes"] = [c for c in csv.split(",") if c]
    return rico


def indexar(directorio: Path = RUTA_CORPUS, ruta_chroma: Path | None = None) -> dict:
    """Reconstruye el índice y devuelve un resumen (documentos, fragmentos, commit...)."""
    documentos = cargar_corpus(directorio)
    revisados = [d for d in documentos if d.revisado]
    presupuesto = presupuesto_tokens()
    fragmentos: list[Fragmento] = trocear_corpus(documentos, contar_tokens, presupuesto)
    if not fragmentos:
        raise CorpusInvalido(
            f"El corpus en {directorio} no produjo fragmentos indexables "
            f"({len(documentos)} documentos, {len(revisados)} revisados)."
        )

    # 1) Embeddings ANTES de tocar el índice anterior.
    vectores = embed_pasajes([f.texto for f in fragmentos])

    # 2) Ahora sí: recrear la colección y escribir.
    metadatos_coleccion = {
        "modelo_embeddings": MODELO_EMBEDDINGS,
        "commit": commit_corpus(),
        "fecha_indexado": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "presupuesto_tokens": presupuesto,
        "n_documentos": len(revisados),
        "n_fragmentos": len(fragmentos),
    }
    coleccion = recrear_coleccion(metadatos_coleccion, ruta_chroma)
    for i in range(0, len(fragmentos), TAMANO_LOTE):
        lote = fragmentos[i:i + TAMANO_LOTE]
        coleccion.add(
            ids=[f.chunk_id for f in lote],
            embeddings=vectores[i:i + TAMANO_LOTE],
            documents=[f.texto for f in lote],
            metadatas=[metadatos_a_chroma(f.metadatos) for f in lote],
        )

    return {
        **metadatos_coleccion,
        "n_documentos_total": len(documentos),
        "omitidos": [d.archivo for d in documentos if not d.revisado],
        "max_tokens_fragmento": max(f.metadatos["n_tokens"] for f in fragmentos),
        "ruta": str(ruta_chroma or RUTA_CHROMA),
        "coleccion": NOMBRE_COLECCION,
    }


def main() -> None:
    r = indexar()
    print(f"Índice reconstruido en {r['ruta']} (colección '{r['coleccion']}')")
    print(f"  modelo:      {r['modelo_embeddings']}")
    print(f"  commit:      {r['commit']}")
    print(f"  documentos:  {r['n_documentos']} indexados de {r['n_documentos_total']}"
          + (f" (omitidos: {', '.join(r['omitidos'])})" if r["omitidos"] else ""))
    print(f"  fragmentos:  {r['n_fragmentos']} (máx. {r['max_tokens_fragmento']} tokens; "
          f"presupuesto {r['presupuesto_tokens']})")


if __name__ == "__main__":
    main()
