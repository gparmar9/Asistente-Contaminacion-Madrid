"""Lectura y troceado del corpus RAG (`data/rag/*.md`).

Es lógica pura: no importa torch ni ChromaDB. El recuento de tokens se recibe como
función (`contar_tokens`) para poder probar el troceado sin cargar el modelo.

Formato de documento (ver docsLocal/plan_rag_simplificado_3_fases.md, §2.3):

    ---
    titulo: Ozono troposférico (O3) y salud
    tema: salud                    # salud | normativa | proyecto
    contaminantes: [O3]            # lista; 'NO' siempre entre comillas
    revisado: true                 # false = se lee pero no se indexa
    fecha_revision: 2026-09-10
    fuentes:
      - titulo: WHO global air quality guidelines 2021
        organismo: OMS
        url: https://...
    ---
    # Título
    Introducción (opcional, va al fragmento "Introducción")
    ## Sección
    ...

Reglas:
- El cuerpo se parte por encabezados `##`. Cada sección es un fragmento salvo que
  supere el presupuesto de tokens; entonces se parte por párrafos (y, si un párrafo
  solo ya no cabe, por líneas) conservando título y sección en cada trozo.
- Las secciones "Fuentes", "Cómo lo usa el asistente" e "Indicación para el
  asistente" no se indexan: las fuentes viven en el frontmatter y las
  instrucciones en el prompt.
- chunk_id = <archivo sin extensión>:<slug de la sección>:<ordinal>. No depende de
  la posición de la sección, así que reordenar secciones no cambia los IDs.
"""
from __future__ import annotations

import datetime as dt
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import yaml

from rag.errores import CorpusInvalido

BASE_DIR = Path(__file__).resolve().parents[2]
RUTA_CORPUS = BASE_DIR / "data" / "rag"

TEMAS = ("salud", "normativa", "proyecto")
SECCION_INTRO = "Introducción"
# Secciones excluidas del índice, comparadas por slug (sin tildes ni mayúsculas).
SECCIONES_EXCLUIDAS = frozenset({
    "fuentes",
    "como-lo-usa-el-asistente",
    "indicacion-para-el-asistente",
})
# Ficheros del directorio del corpus que no son documentos.
ARCHIVOS_IGNORADOS = frozenset({"README.md"})

ContarTokens = Callable[[str], int]

_RE_FRONTMATTER = re.compile(r"\A﻿?---\r?\n(.*?)\r?\n---[ \t]*(?:\r?\n|\Z)", re.DOTALL)
_RE_PARRAFOS = re.compile(r"\n[ \t]*\n")


@dataclass(frozen=True)
class Fuente:
    titulo: str
    organismo: str = ""
    url: str = ""


@dataclass
class Documento:
    archivo: str                      # nombre con extensión: ozono_salud.md
    titulo: str
    tema: str
    contaminantes: list[str]
    revisado: bool
    fecha_revision: str               # ISO (YYYY-MM-DD) o "" si no revisado
    fuentes: list[Fuente]
    secciones: list[tuple[str, str]]  # (encabezado, contenido), ya sin excluidas ni vacías

    @property
    def nombre(self) -> str:
        """Nombre sin extensión; es la primera parte del chunk_id."""
        return Path(self.archivo).stem


@dataclass
class Fragmento:
    chunk_id: str
    texto: str
    metadatos: dict = field(default_factory=dict)


# --------------------------------------------------------------------------- utilidades

def slug(texto: str) -> str:
    """'Qué es y su patrón particular' -> 'que-es-y-su-patron-particular'."""
    sin_tildes = unicodedata.normalize("NFKD", texto)
    ascii_ = "".join(c for c in sin_tildes if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "-", ascii_.lower()).strip("-")


def separar_frontmatter(texto: str) -> tuple[dict, str]:
    """Devuelve (metadatos, cuerpo). Si no hay frontmatter, metadatos es {}."""
    m = _RE_FRONTMATTER.match(texto)
    if not m:
        return {}, texto
    try:
        metadatos = yaml.safe_load(m.group(1)) or {}
    except yaml.YAMLError as e:
        raise CorpusInvalido(f"frontmatter YAML inválido: {e}") from e
    if not isinstance(metadatos, dict):
        raise CorpusInvalido("el frontmatter debe ser un mapa clave: valor")
    return metadatos, texto[m.end():].lstrip("\r\n")


def _secciones(cuerpo: str) -> list[tuple[str, str]]:
    """Parte el cuerpo en (encabezado, contenido) por cada `## `.

    El texto anterior al primer `##` (bajo el `#` del título) se devuelve con el
    encabezado "Introducción". Los `###` quedan dentro de su sección `##`.
    """
    secciones: list[tuple[str, str]] = []
    encabezado = SECCION_INTRO
    buffer: list[str] = []
    for linea in cuerpo.splitlines():
        if linea.startswith("## "):
            secciones.append((encabezado, "\n".join(buffer).strip()))
            encabezado = linea[3:].strip()
            buffer = []
        elif linea.startswith("# "):
            continue  # el título ya está en el frontmatter
        else:
            buffer.append(linea)
    secciones.append((encabezado, "\n".join(buffer).strip()))
    return [(enc, cont) for enc, cont in secciones if cont]


# --------------------------------------------------------------------------- lectura

def _validar(meta: dict, archivo: str) -> None:
    def fallo(msg: str) -> CorpusInvalido:
        return CorpusInvalido(f"{archivo}: {msg}")

    for clave in ("titulo", "tema", "contaminantes", "revisado", "fuentes"):
        if clave not in meta:
            raise fallo(f"falta la clave obligatoria '{clave}' en el frontmatter")
    if not isinstance(meta["titulo"], str) or not meta["titulo"].strip():
        raise fallo("'titulo' debe ser un texto no vacío")
    if meta["tema"] not in TEMAS:
        raise fallo(f"'tema' debe ser uno de {list(TEMAS)}, no {meta['tema']!r}")
    contaminantes = meta["contaminantes"]
    if contaminantes is None:
        contaminantes = []
    if not isinstance(contaminantes, list) or not all(isinstance(c, str) for c in contaminantes):
        raise fallo("'contaminantes' debe ser una lista de textos (escribe 'NO' entre comillas)")
    if not isinstance(meta["revisado"], bool):
        raise fallo("'revisado' debe ser true o false")
    fuentes = meta["fuentes"]
    if fuentes is None:
        fuentes = []
    if not isinstance(fuentes, list) or not all(isinstance(f, dict) and f.get("titulo") for f in fuentes):
        raise fallo("'fuentes' debe ser una lista de mapas con al menos 'titulo'")
    if meta["revisado"]:
        if not meta.get("fecha_revision"):
            raise fallo("un documento revisado necesita 'fecha_revision'")
        if not fuentes:
            raise fallo("un documento revisado necesita al menos una fuente")


def _fecha_iso(valor) -> str:
    if valor is None or valor == "":
        return ""
    if isinstance(valor, (dt.date, dt.datetime)):
        return valor.isoformat()[:10]
    return str(valor)


def leer_documento(ruta: Path) -> Documento:
    """Lee y valida un .md del corpus. Lanza CorpusInvalido si no cumple el formato."""
    ruta = Path(ruta)
    meta, cuerpo = separar_frontmatter(ruta.read_text(encoding="utf-8"))
    if not meta:
        raise CorpusInvalido(f"{ruta.name}: el frontmatter YAML es obligatorio")
    _validar(meta, ruta.name)

    secciones: list[tuple[str, str]] = []
    vistos: set[str] = set()
    for encabezado, contenido in _secciones(cuerpo):
        s = slug(encabezado)
        if s in SECCIONES_EXCLUIDAS:
            continue
        if not s:
            raise CorpusInvalido(f"{ruta.name}: la sección {encabezado!r} no produce un slug válido")
        if s in vistos:
            raise CorpusInvalido(f"{ruta.name}: dos secciones comparten el slug '{s}'")
        vistos.add(s)
        secciones.append((encabezado, contenido))

    return Documento(
        archivo=ruta.name,
        titulo=meta["titulo"].strip(),
        tema=meta["tema"],
        contaminantes=[str(c) for c in (meta["contaminantes"] or [])],
        revisado=meta["revisado"],
        fecha_revision=_fecha_iso(meta.get("fecha_revision")),
        fuentes=[
            Fuente(
                titulo=str(f["titulo"]).strip(),
                organismo=str(f.get("organismo") or "").strip(),
                url=str(f.get("url") or "").strip(),
            )
            for f in (meta["fuentes"] or [])
        ],
        secciones=secciones,
    )


def cargar_corpus(directorio: Path = RUTA_CORPUS) -> list[Documento]:
    """Lee todos los .md del directorio (revisados o no), en orden alfabético."""
    directorio = Path(directorio)
    rutas = sorted(p for p in directorio.glob("*.md") if p.name not in ARCHIVOS_IGNORADOS)
    return [leer_documento(p) for p in rutas]


# --------------------------------------------------------------------------- troceado

def _partir(unidades: list[str], separador: str, cabe: Callable[[str], bool],
            partir_unidad: Callable[[str], list[str]]) -> list[str]:
    """Agrupa `unidades` en trozos que quepan; una unidad que no cabe sola se subparte."""
    trozos: list[str] = []
    actual = ""
    for unidad in unidades:
        candidato = f"{actual}{separador}{unidad}" if actual else unidad
        if cabe(candidato):
            actual = candidato
            continue
        if actual:
            trozos.append(actual)
            actual = ""
        if cabe(unidad):
            actual = unidad
        else:
            trozos.extend(partir_unidad(unidad))
    if actual:
        trozos.append(actual)
    return trozos


def _trocear_seccion(doc: Documento, encabezado: str, contenido: str,
                     contar_tokens: ContarTokens, presupuesto: int) -> list[str]:
    """Devuelve los textos (con prefijo título — sección) de una sección."""
    prefijo = f"{doc.titulo} — {encabezado}\n"

    def cabe(cuerpo: str) -> bool:
        return contar_tokens(prefijo + cuerpo) <= presupuesto

    if cabe(contenido):
        return [prefijo + contenido]

    def partir_linea(linea: str) -> list[str]:
        raise CorpusInvalido(
            f"{doc.archivo} / '{encabezado}': una sola línea supera el presupuesto de "
            f"{presupuesto} tokens ({contar_tokens(prefijo + linea)}). Divide el contenido."
        )

    def partir_parrafo(parrafo: str) -> list[str]:
        return _partir(parrafo.split("\n"), "\n", cabe, partir_linea)

    parrafos = [p.strip() for p in _RE_PARRAFOS.split(contenido) if p.strip()]
    return [prefijo + t for t in _partir(parrafos, "\n\n", cabe, partir_parrafo)]


def trocear_documento(doc: Documento, contar_tokens: ContarTokens, presupuesto: int) -> list[Fragmento]:
    """Trocea un documento (revisado o no) en fragmentos con chunk_id estable."""
    if presupuesto <= 0:
        raise ValueError("presupuesto debe ser positivo")
    fragmentos: list[Fragmento] = []
    for encabezado, contenido in doc.secciones:
        textos = _trocear_seccion(doc, encabezado, contenido, contar_tokens, presupuesto)
        for ordinal, texto in enumerate(textos):
            fragmentos.append(Fragmento(
                chunk_id=f"{doc.nombre}:{slug(encabezado)}:{ordinal}",
                texto=texto,
                metadatos={
                    "titulo": doc.titulo,
                    "archivo": doc.archivo,
                    "seccion": encabezado,
                    "tema": doc.tema,
                    "contaminantes": list(doc.contaminantes),
                    "fecha_revision": doc.fecha_revision,
                    "n_tokens": contar_tokens(texto),
                },
            ))
    return fragmentos


def trocear_corpus(documentos: list[Documento], contar_tokens: ContarTokens,
                   presupuesto: int) -> list[Fragmento]:
    """Trocea solo los documentos con `revisado: true` y garantiza IDs únicos."""
    fragmentos: list[Fragmento] = []
    for doc in documentos:
        if doc.revisado:
            fragmentos.extend(trocear_documento(doc, contar_tokens, presupuesto))
    ids = [f.chunk_id for f in fragmentos]
    repetidos = sorted({i for i in ids if ids.count(i) > 1})
    if repetidos:
        raise CorpusInvalido(f"chunk_id repetidos: {repetidos}")
    return fragmentos


def main() -> None:
    """Resumen del corpus sin cargar el modelo (los tokens se aproximan por palabras)."""
    docs = cargar_corpus()
    print(f"{len(docs)} documentos en {RUTA_CORPUS}")
    for d in docs:
        estado = "revisado" if d.revisado else "NO revisado"
        print(f"  - {d.archivo:38} {d.tema:10} {estado:12} {len(d.secciones)} secciones")
    frags = trocear_corpus(docs, lambda t: len(t.split()), presupuesto=10**9)
    print(f"{len(frags)} fragmentos indexables (una sección = un fragmento sin guardia de tokens).")


if __name__ == "__main__":
    main()
