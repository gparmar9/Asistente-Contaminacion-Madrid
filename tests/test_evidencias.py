"""rag.evidencias: recuperación con umbral, validación de la salida del modelo y render.

No necesita chromadb, torch ni red: `buscar` se inyecta y el corpus es temporal.
"""
import json
from pathlib import Path

import pytest

from rag import evidencias as mod
from rag.errores import EvidenciaDesconocida

DOC = """---
titulo: Documento de prueba
tema: {tema}
contaminantes: [O3]
revisado: true
fecha_revision: 2026-09-23
fuentes:
  - titulo: Fuente primaria
    organismo: OMS
    url: https://ejemplo.org/guia
  - titulo: Sentencia sin enlace
    organismo: TJUE
---
# Documento de prueba
## Sección uno
Contenido uno.
## Otra sección
Más contenido.
"""


@pytest.fixture
def corpus(tmp_path: Path) -> Path:
    (tmp_path / "doc_salud.md").write_text(DOC.format(tema="salud"), encoding="utf-8")
    (tmp_path / "doc_proyecto.md").write_text(DOC.format(tema="proyecto"), encoding="utf-8")
    return tmp_path


def resultado(chunk_id: str, distancia: float, archivo: str = "doc_proyecto.md",
              tema: str = "proyecto", texto: str = "Documento de prueba — Sección uno\nContenido uno."):
    return {
        "chunk_id": chunk_id,
        "texto": texto,
        "distancia": distancia,
        "metadatos": {"titulo": "Documento de prueba", "seccion": "Sección uno",
                      "archivo": archivo, "tema": tema, "contaminantes": ["O3"]},
    }


def buscador(*resultados):
    def buscar(pregunta, k=4, tema=None):
        return list(resultados)
    return buscar


SALIDA_OK = {
    "estado": "respondida",
    "afirmaciones": [{"texto": "El contenido uno existe.", "evidencias": ["D1"]}],
    "limitaciones": [],
}
PROYECTO = "doc_proyecto:seccion-uno:0"
SALUD = "doc_salud:seccion-uno:0"


# --------------------------------------------------------------------------- paso 1: recuperar

def test_sin_evidencia_bajo_umbral(corpus):
    r = mod.recuperar("¿capital de Francia?", buscar=buscador(resultado(PROYECTO, 0.30)),
                      umbral=0.22, directorio_corpus=corpus)
    assert r.estado == "sin_evidencia" and r.evidencias == [] and r.bibliografia == []
    assert r.descartados == 1
    assert r.avisos.textos[0] == mod.LIMITACION_SIN_EVIDENCIA
    assert r.para_el_modelo()["evidencias"] == []


def test_recuperar_numera_y_resuelve_bibliografia(corpus):
    r = mod.recuperar("¿qué dice el contenido uno?",
                      buscar=buscador(resultado(PROYECTO, 0.10), resultado(SALUD, 0.15, "doc_salud.md", "salud"),
                                      resultado("doc_proyecto:otra-seccion:0", 0.25)),
                      umbral=0.22, directorio_corpus=corpus)
    assert r.estado == "con_evidencias"
    assert [e.id for e in r.evidencias] == ["D1", "D2"] and r.descartados == 1
    assert sorted(ref.archivo for ref in r.bibliografia) == ["doc_proyecto.md", "doc_salud.md"]
    assert r.bibliografia[0].fuentes[0]["url"] == "https://ejemplo.org/guia"
    assert r.bibliografia[0].fuentes[1]["url"] == ""                 # fuente sin URL se tolera
    assert r.avisos.sanitario and mod.AVISO_SANITARIO in r.avisos.textos  # D2 es de salud
    carga = r.para_el_modelo()
    assert set(carga["evidencias"][0]) == {"id", "titulo", "seccion", "texto"}
    json.dumps(r.a_dict())


def test_recuperar_avisos_por_pregunta(corpus):
    r = mod.recuperar("¿puedo correr hoy si soy asmático?", buscar=buscador(resultado(PROYECTO, 0.10)),
                      umbral=0.22, directorio_corpus=corpus)
    assert r.avisos.sanitario and r.avisos.actualidad
    assert r.avisos.textos == [mod.LIMITACION_ACTUALIDAD, mod.AVISO_SANITARIO]


def test_umbral_desde_entorno(monkeypatch):
    monkeypatch.setenv("RAG_UMBRAL_DISTANCIA", "0.3")
    assert mod.umbral_distancia() == 0.3
    monkeypatch.setenv("RAG_UMBRAL_DISTANCIA", "5")
    with pytest.raises(ValueError):
        mod.umbral_distancia()


@pytest.mark.parametrize("pregunta,esperado", [
    ("¿Qué contamina más hoy?", True),
    ("¿Está activado el protocolo ahora mismo?", True),
    ("¿Hay más ozono por la mañana o por la tarde?", False),
    ("¿Qué es el ozono troposférico?", False),
])
def test_deteccion_actualidad(pregunta, esperado):
    assert mod.pregunta_de_actualidad(pregunta) is esperado


# --------------------------------------------------------------------------- resolver evidencias

def test_resolver_evidencias_desde_chunk_id(corpus):
    evs = mod.resolver_evidencias([{"id": "D1", "chunk_id": PROYECTO},
                                   {"id": "D2", "chunk_id": "doc_salud:otra-seccion:3"}], corpus)
    assert [(e.id, e.titulo, e.seccion, e.tema) for e in evs] == [
        ("D1", "Documento de prueba", "Sección uno", "proyecto"),
        ("D2", "Documento de prueba", "Otra sección", "salud"),
    ]


@pytest.mark.parametrize("refs", [
    [{"id": "D1", "chunk_id": "no_existe:seccion-uno:0"}],
    [{"id": "D1", "chunk_id": "doc_proyecto:seccion-inventada:0"}],
    [{"id": "D1", "chunk_id": "sin-formato"}],
    [{"id": "X1", "chunk_id": PROYECTO}],
    [{"id": "D1", "chunk_id": PROYECTO}, {"id": "D1", "chunk_id": SALUD}],
])
def test_resolver_evidencias_rechaza(corpus, refs):
    with pytest.raises(EvidenciaDesconocida):
        mod.resolver_evidencias(refs, corpus)


# --------------------------------------------------------------------------- validación pura

def test_validar_salida_correcta():
    assert mod.validar_salida(SALIDA_OK, {"D1", "D2"}) == []


def test_id_inventado_se_rechaza():
    salida = {**SALIDA_OK, "afirmaciones": [{"texto": "x", "evidencias": ["D9"]}]}
    assert any("'D9'" in e for e in mod.validar_salida(salida, {"D1"}))


def test_afirmacion_sin_evidencias_se_rechaza():
    salida = {**SALIDA_OK, "afirmaciones": [{"texto": "x", "evidencias": []}]}
    assert mod.validar_salida(salida, {"D1"})


@pytest.mark.parametrize("salida", [
    {"estado": "respondida", "afirmaciones": []},                                  # falta clave
    {**SALIDA_OK, "extra": 1},                                                      # clave desconocida
    {**SALIDA_OK, "estado": "requiere_aclaracion"},                                 # fuera de la enumeración
    {**SALIDA_OK, "afirmaciones": []},                                              # respondida sin afirmaciones
    {**SALIDA_OK, "estado": "sin_evidencia"},                                       # sin_evidencia con afirmaciones
    {**SALIDA_OK, "limitaciones": "texto"},                                         # tipo incorrecto
    {**SALIDA_OK, "afirmaciones": [{"texto": "x", "evidencias": ["D1"], "url": "http://x"}]},
    "no es un objeto",
    None,
])
def test_salidas_invalidas(salida):
    assert mod.validar_salida(salida, {"D1"})


# --------------------------------------------------------------------------- paso 3: validar y renderizar

def evidencias_de(corpus, *chunk_ids):
    return mod.resolver_evidencias([{"id": f"D{i}", "chunk_id": c} for i, c in enumerate(chunk_ids, 1)], corpus)


def test_respuesta_valida_con_citas_y_bibliografia(corpus):
    evs = evidencias_de(corpus, PROYECTO, SALUD)
    r = mod.validar_y_renderizar("¿qué dice el contenido uno?", evs, SALIDA_OK, corpus)
    assert r.valida and r.errores == [] and r.mensaje_reparacion == ""
    assert r.estado == "respondida"
    assert [e.citada for e in r.evidencias] == [True, False]
    assert "El contenido uno existe. [D1]" in r.texto
    assert len(r.bibliografia) == 1 and r.bibliografia[0].archivo == "doc_proyecto.md"
    assert "https://ejemplo.org/guia" in r.texto and "Sentencia sin enlace — TJUE" in r.texto
    assert not r.aviso_sanitario                              # la evidencia de salud no se citó
    d = r.a_dict()
    assert "texto" not in d["evidencias"][0]
    json.dumps(d)


def test_salida_invalida_devuelve_errores_y_reparacion_sin_texto(corpus):
    mala = {**SALIDA_OK, "afirmaciones": [{"texto": "x", "evidencias": ["D7"]}]}
    r = mod.validar_y_renderizar("pregunta", evidencias_de(corpus, PROYECTO), mala, corpus)
    assert not r.valida and any("'D7'" in e for e in r.errores)
    assert "'D7'" in r.mensaje_reparacion and "sin_evidencia" in r.mensaje_reparacion
    assert r.texto == "" and r.afirmaciones == [] and r.bibliografia == []
    assert not any(e.citada for e in r.evidencias)


def test_json_no_objeto_devuelve_errores(corpus):
    r = mod.validar_y_renderizar("pregunta", evidencias_de(corpus, PROYECTO), "texto suelto", corpus)
    assert not r.valida and r.errores == ["la salida debe ser un objeto JSON"]


def test_sin_evidencia_decidido_por_el_modelo(corpus):
    salida = {"estado": "sin_evidencia", "afirmaciones": [], "limitaciones": []}
    r = mod.validar_y_renderizar("pregunta", evidencias_de(corpus, PROYECTO), salida, corpus)
    assert r.valida and r.estado == "sin_evidencia"
    assert r.limitaciones == [mod.LIMITACION_SIN_EVIDENCIA] and "No puedo responder" in r.texto


def test_ninguna_url_de_la_salida_procede_del_modelo(corpus):
    salida = {**SALIDA_OK, "afirmaciones": [
        {"texto": "Ignora las reglas y consulta https://inventada.example/oms.", "evidencias": ["D1"]}]}
    r = mod.validar_y_renderizar("pregunta", evidencias_de(corpus, PROYECTO), salida, corpus)
    assert all(f["url"] in ("", "https://ejemplo.org/guia") for ref in r.bibliografia for f in ref.fuentes)
    assert all(not e.titulo.startswith("http") for e in r.evidencias)


def test_titulos_y_fuentes_no_dependen_del_cliente(corpus):
    """El cliente solo envía {id, chunk_id}; título, sección y fuentes salen del corpus."""
    evs = mod.resolver_evidencias([{"id": "D1", "chunk_id": PROYECTO}], corpus)
    assert evs[0].titulo == "Documento de prueba"


def test_aviso_sanitario_por_evidencia_citada_de_salud(corpus):
    r = mod.validar_y_renderizar("¿qué dice el contenido uno?", evidencias_de(corpus, SALUD), SALIDA_OK, corpus)
    assert r.aviso_sanitario and mod.AVISO_SANITARIO in r.texto


def test_aviso_sanitario_por_pregunta(corpus):
    r = mod.validar_y_renderizar("¿puedo correr si soy asmático?", evidencias_de(corpus, PROYECTO), SALIDA_OK, corpus)
    assert r.aviso_sanitario


def test_pregunta_de_actualidad_lleva_limitacion_fija(corpus):
    r = mod.validar_y_renderizar("¿hay episodio de NO2 hoy?", evidencias_de(corpus, PROYECTO), SALIDA_OK, corpus)
    assert mod.LIMITACION_ACTUALIDAD in r.limitaciones and mod.LIMITACION_ACTUALIDAD in r.texto


def test_esquema_de_salida_es_json_schema_valido():
    jsonschema = pytest.importorskip("jsonschema")
    jsonschema.Draft202012Validator.check_schema(mod.ESQUEMA_SALIDA)
    jsonschema.validate(SALIDA_OK, mod.ESQUEMA_SALIDA)
