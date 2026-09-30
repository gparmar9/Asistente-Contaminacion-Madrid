"""Pruebas del troceado del corpus (rag.corpus). Lógica pura: sin modelo ni Chroma.

Los tokens se cuentan por palabras a través de `contar_tokens` inyectado; lo que se
prueba es la guardia de longitud y la identidad de fragmentos, no el tokenizer real.
"""
from pathlib import Path

import pytest

from rag.corpus import (
    RUTA_CORPUS,
    Documento,
    cargar_corpus,
    leer_documento,
    separar_frontmatter,
    slug,
    trocear_corpus,
    trocear_documento,
)
from rag.errores import CorpusInvalido

FRONTMATTER = """---
titulo: Documento de prueba
tema: salud
contaminantes: ["NO", NO2]
revisado: true
fecha_revision: 2026-09-10
fuentes:
  - titulo: WHO global air quality guidelines 2021
    organismo: OMS
    url: https://www.who.int/publications/i/item/9789240034228
---
"""

DOC = FRONTMATTER + """
# Documento de prueba

Texto introductorio bajo el título.

## Primera sección

Contenido de la primera sección.

## Cómo lo usa el asistente

Instrucciones que no deben indexarse.

## Fuentes

- OMS. No debe indexarse.
"""


def contar_palabras(texto: str) -> int:
    return len(texto.split())


def escribir(tmp_path: Path, texto: str, nombre: str = "doc.md") -> Path:
    ruta = tmp_path / nombre
    ruta.write_text(texto, encoding="utf-8")
    return ruta


# ------------------------------------------------------------------ frontmatter

def test_separar_frontmatter_extrae_metadatos_y_cuerpo():
    meta, cuerpo = separar_frontmatter(DOC)
    assert meta["titulo"] == "Documento de prueba"
    assert meta["contaminantes"] == ["NO", "NO2"]  # 'NO' entre comillas no es booleano
    assert meta["revisado"] is True
    assert cuerpo.startswith("# Documento de prueba")


def test_separar_frontmatter_admite_crlf_y_bom():
    meta, cuerpo = separar_frontmatter("﻿" + DOC.replace("\n", "\r\n"))
    assert meta["tema"] == "salud"
    assert cuerpo.startswith("# Documento de prueba")


def test_separar_frontmatter_sin_frontmatter():
    meta, cuerpo = separar_frontmatter("# Solo cuerpo\n")
    assert meta == {}
    assert cuerpo == "# Solo cuerpo\n"


def test_leer_documento_exige_frontmatter(tmp_path):
    with pytest.raises(CorpusInvalido, match="frontmatter"):
        leer_documento(escribir(tmp_path, "# Sin cabecera\n\n## A\n\ntexto\n"))


@pytest.mark.parametrize(
    "malo, mensaje",
    [
        ("tema: salud", "tema: otro"),
        ("contaminantes: [\"NO\", NO2]", "contaminantes: [NO, NO2]"),  # NO sin comillas -> bool
        ("revisado: true", "revisado: si"),
        ("fecha_revision: 2026-09-10", "fecha_revision:"),
    ],
)
def test_leer_documento_valida_frontmatter(tmp_path, malo, mensaje):
    texto = DOC.replace(malo, mensaje)
    with pytest.raises(CorpusInvalido):
        leer_documento(escribir(tmp_path, texto))


def test_leer_documento_fuentes_y_fecha_iso(tmp_path):
    doc = leer_documento(escribir(tmp_path, DOC))
    assert doc.fecha_revision == "2026-09-10"
    assert doc.fuentes[0].organismo == "OMS"
    assert doc.fuentes[0].url.startswith("https://")
    assert doc.nombre == "doc"


# ------------------------------------------------------------------ secciones

def test_secciones_excluidas_no_entran(tmp_path):
    doc = leer_documento(escribir(tmp_path, DOC))
    encabezados = [e for e, _ in doc.secciones]
    assert encabezados == ["Introducción", "Primera sección"]


def test_seccion_duplicada_es_error(tmp_path):
    texto = DOC + "\n## Primera sección\n\nOtra vez.\n"
    with pytest.raises(CorpusInvalido, match="slug"):
        leer_documento(escribir(tmp_path, texto))


def test_slug_sin_tildes_ni_mayusculas():
    assert slug("Qué es y su patrón particular") == "que-es-y-su-patron-particular"
    assert slug("Cómo lo usa el asistente") == "como-lo-usa-el-asistente"


# ------------------------------------------------------------------ fragmentos

def test_fragmentos_llevan_titulo_seccion_y_metadatos(tmp_path):
    doc = leer_documento(escribir(tmp_path, DOC))
    frags = trocear_documento(doc, contar_palabras, presupuesto=1000)
    assert [f.chunk_id for f in frags] == ["doc:introduccion:0", "doc:primera-seccion:0"]
    intro = frags[0]
    assert intro.texto.startswith("Documento de prueba — Introducción\n")
    assert intro.metadatos["contaminantes"] == ["NO", "NO2"]  # lista, no cadena
    assert intro.metadatos["tema"] == "salud"
    assert intro.metadatos["archivo"] == "doc.md"
    assert intro.metadatos["fecha_revision"] == "2026-09-10"
    assert intro.metadatos["n_tokens"] == contar_palabras(intro.texto)


def test_ids_estables_al_reordenar_secciones(tmp_path):
    a = FRONTMATTER + "\n## Alfa\n\nuno\n\n## Beta\n\ndos\n\n## Gamma\n\ntres\n"
    b = FRONTMATTER + "\n## Gamma\n\ntres\n\n## Nueva\n\ncuatro\n\n## Alfa\n\nuno\n\n## Beta\n\ndos\n"
    ids_a = {f.chunk_id for f in trocear_documento(leer_documento(escribir(tmp_path, a)), contar_palabras, 100)}
    ids_b = {f.chunk_id for f in trocear_documento(leer_documento(escribir(tmp_path, b)), contar_palabras, 100)}
    assert ids_a <= ids_b
    assert ids_b - ids_a == {"doc:nueva:0"}


def test_seccion_larga_se_subdivide_por_parrafos_sin_superar_presupuesto(tmp_path):
    parrafos = [f"Párrafo {i} " + " ".join(f"p{i}w{j}" for j in range(12)) for i in range(10)]
    texto = FRONTMATTER + "\n## Larga\n\n" + "\n\n".join(parrafos) + "\n"
    presupuesto = 40  # cabe ~2 párrafos por trozo contando el prefijo
    frags = trocear_documento(leer_documento(escribir(tmp_path, texto)), contar_palabras, presupuesto)

    assert len(frags) > 1
    assert all(contar_palabras(f.texto) <= presupuesto for f in frags)
    assert all(f.texto.startswith("Documento de prueba — Larga\n") for f in frags)
    assert [f.chunk_id for f in frags] == [f"doc:larga:{i}" for i in range(len(frags))]
    # No se pierde contenido: el último párrafo está en el último trozo.
    assert "p9w11" in frags[-1].texto
    # Los párrafos no se cortan por dentro cuando caben enteros.
    assert all("Párrafo 0 " not in f.texto or "p0w11" in f.texto for f in frags)


def test_parrafo_mas_largo_que_presupuesto_se_parte_por_lineas(tmp_path):
    lineas = [" ".join(f"l{i}w{j}" for j in range(8)) for i in range(6)]
    texto = FRONTMATTER + "\n## Tabla\n\n" + "\n".join(lineas) + "\n"
    frags = trocear_documento(leer_documento(escribir(tmp_path, texto)), contar_palabras, presupuesto=22)
    assert len(frags) > 1
    assert all(contar_palabras(f.texto) <= 22 for f in frags)
    assert "l5w7" in frags[-1].texto


def test_linea_imposible_de_partir_es_error_explicito(tmp_path):
    texto = FRONTMATTER + "\n## Imposible\n\n" + " ".join(f"w{j}" for j in range(50)) + "\n"
    with pytest.raises(CorpusInvalido, match="supera el presupuesto"):
        trocear_documento(leer_documento(escribir(tmp_path, texto)), contar_palabras, presupuesto=20)


# ------------------------------------------------------------------ corpus

def test_documento_no_revisado_se_lee_pero_no_se_indexa(tmp_path):
    escribir(tmp_path, DOC, "revisado.md")
    escribir(tmp_path, DOC.replace("revisado: true", "revisado: false"), "borrador.md")
    escribir(tmp_path, "# no es un documento\n", "README.md")

    docs = cargar_corpus(tmp_path)
    assert [d.archivo for d in docs] == ["borrador.md", "revisado.md"]
    assert [d.revisado for d in docs] == [False, True]

    frags = trocear_corpus(docs, contar_palabras, presupuesto=1000)
    assert {f.metadatos["archivo"] for f in frags} == {"revisado.md"}


def test_corpus_real_cumple_el_formato():
    docs = cargar_corpus(RUTA_CORPUS)
    assert len(docs) >= 11
    assert all(d.fuentes for d in docs)
    frags = trocear_corpus(docs, contar_palabras, presupuesto=10**6)
    ids = [f.chunk_id for f in frags]
    assert len(ids) == len(set(ids))
    assert all(f.texto.strip() for f in frags)
    excluidas = {"fuentes", "como-lo-usa-el-asistente", "indicacion-para-el-asistente"}
    assert not {f.chunk_id.split(":")[1] for f in frags} & excluidas
