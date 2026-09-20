"""Trocea el corpus RAG (`data/rag/*.md`) en fragmentos listos para indexar.

Cada documento lleva frontmatter YAML (titulo, tema, contaminantes, fuente) y se
divide por sus encabezados de segundo nivel (`## ...`). Cada fragmento hereda los
metadatos del documento más su encabezado de sección, y se le antepone el título
del documento para darle contexto al embedding.

Es lógica pura (sin torch ni ChromaDB), así que se puede testear en aislamiento.
"""
from pathlib import Path

import yaml

BASE_DIR = Path(__file__).resolve().parents[2]
RUTA_CORPUS = BASE_DIR / "data" / "rag"

# Secciones que no aportan a la búsqueda semántica (ya viven en los metadatos).
SECCIONES_IGNORADAS = {"fuentes"}


def separar_frontmatter(texto: str) -> tuple[dict, str]:
    """Devuelve (metadatos, cuerpo) a partir del texto de un .md con frontmatter."""
    if not texto.startswith("---"):
        return {}, texto
    _, frontmatter, cuerpo = texto.split("---", 2)
    metadatos = yaml.safe_load(frontmatter) or {}
    return metadatos, cuerpo.lstrip("\n")


def _secciones(cuerpo: str) -> list[tuple[str, str]]:
    """Parte el cuerpo en (encabezado, contenido) por cada `## ...`.

    El texto anterior al primer `##` (la introducción bajo el `#`) se devuelve con
    el encabezado 'Introducción'.
    """
    secciones: list[tuple[str, str]] = []
    encabezado = "Introducción"
    buffer: list[str] = []
    for linea in cuerpo.splitlines():
        if linea.startswith("## "):
            if buffer:
                secciones.append((encabezado, "\n".join(buffer).strip()))
            encabezado = linea[3:].strip()
            buffer = []
        elif linea.startswith("# "):
            continue  # el título ya está en el frontmatter
        else:
            buffer.append(linea)
    if buffer:
        secciones.append((encabezado, "\n".join(buffer).strip()))
    return secciones


def _a_metadatos_chroma(meta: dict, encabezado: str, archivo: str) -> dict:
    """Aplana los metadatos a tipos admitidos por ChromaDB (str/int/float/bool).

    ChromaDB no admite listas, así que `contaminantes` se serializa como cadena.
    """
    contaminantes = meta.get("contaminantes") or []
    return {
        "titulo": meta.get("titulo", ""),
        "tema": meta.get("tema", ""),
        "contaminantes": ",".join(contaminantes),
        "fuente": meta.get("fuente", ""),
        "seccion": encabezado,
        "archivo": archivo,
    }


def trocear_documento(ruta: Path) -> list[dict]:
    """Trocea un único .md en fragmentos {id, texto, metadatos}."""
    meta, cuerpo = separar_frontmatter(ruta.read_text(encoding="utf-8"))
    titulo = meta.get("titulo", ruta.stem)

    fragmentos = []
    for i, (encabezado, contenido) in enumerate(_secciones(cuerpo)):
        if not contenido or encabezado.strip().lower() in SECCIONES_IGNORADAS:
            continue
        # Anteponer título + sección da contexto al embedding (mejora el recall).
        texto = f"{titulo} — {encabezado}\n{contenido}"
        fragmentos.append({
            "id": f"{ruta.stem}#{i}",
            "texto": texto,
            "metadatos": _a_metadatos_chroma(meta, encabezado, ruta.name),
        })
    return fragmentos


def trocear_corpus(directorio: Path = RUTA_CORPUS) -> list[dict]:
    """Trocea todos los .md del corpus y devuelve la lista de fragmentos."""
    fragmentos = []
    for ruta in sorted(directorio.glob("*.md")):
        fragmentos.extend(trocear_documento(ruta))
    return fragmentos


if __name__ == "__main__":
    frags = trocear_corpus()
    print(f"{len(frags)} fragmentos de {len(list(RUTA_CORPUS.glob('*.md')))} documentos.")
    for f in frags[:3]:
        print(f"  - {f['id']:32} | {f['metadatos']['seccion']}")
