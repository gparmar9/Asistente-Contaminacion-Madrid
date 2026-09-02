import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "rag"))
from trocear_corpus import separar_frontmatter, trocear_documento, trocear_corpus

DOC = """---
titulo: Documento de prueba
tema: salud
contaminantes: ["NO", NO2]
fuente: "OMS 2021"
---

# Documento de prueba

Texto introductorio bajo el titulo.

## Primera sección

Contenido de la primera sección.

## Fuentes

- OMS. No debe indexarse.
"""


def _escribir(tmp_path, texto):
    ruta = tmp_path / "doc.md"
    ruta.write_text(texto, encoding="utf-8")
    return ruta


def test_separar_frontmatter_extrae_metadatos_y_cuerpo():
    meta, cuerpo = separar_frontmatter(DOC)
    assert meta["titulo"] == "Documento de prueba"
    assert meta["contaminantes"] == ["NO", "NO2"]  # 'NO' no se interpreta como booleano
    assert cuerpo.startswith("# Documento de prueba")


def test_separar_frontmatter_sin_frontmatter():
    meta, cuerpo = separar_frontmatter("# Solo cuerpo\n")
    assert meta == {}
    assert cuerpo == "# Solo cuerpo\n"


def test_trocear_documento_secciones_y_metadatos(tmp_path):
    frags = _escribir(tmp_path, DOC)
    fragmentos = trocear_documento(frags)

    secciones = [f["metadatos"]["seccion"] for f in fragmentos]
    assert "Introducción" in secciones
    assert "Primera sección" in secciones
    assert "Fuentes" not in secciones  # sección ignorada

    intro = next(f for f in fragmentos if f["metadatos"]["seccion"] == "Introducción")
    assert intro["texto"].startswith("Documento de prueba — Introducción")  # título antepuesto
    assert intro["metadatos"]["contaminantes"] == "NO,NO2"  # lista serializada a cadena
    assert intro["metadatos"]["tema"] == "salud"


def test_trocear_documento_ids_unicos(tmp_path):
    fragmentos = trocear_documento(_escribir(tmp_path, DOC))
    ids = [f["id"] for f in fragmentos]
    assert len(ids) == len(set(ids))


def test_trocear_corpus_real_no_vacio():
    # El corpus del repo debe producir fragmentos y ninguna sección "Fuentes".
    fragmentos = trocear_corpus()
    assert len(fragmentos) > 0
    assert all(f["texto"].strip() for f in fragmentos)
    assert all(f["metadatos"]["seccion"].lower() != "fuentes" for f in fragmentos)
