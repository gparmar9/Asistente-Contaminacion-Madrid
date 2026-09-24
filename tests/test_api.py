"""rag.api con FastAPI TestClient: contrato HTTP y traducción de errores a códigos.

Requiere fastapi y httpx (se salta si faltan). No carga el modelo ni Chroma: la
búsqueda y el resumen del índice se sustituyen.
"""
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient  # noqa: E402

from rag import api  # noqa: E402
from rag import evidencias as mod  # noqa: E402
from rag.errores import ConsultaInvalida, IndiceNoPreparado  # noqa: E402
from tests.test_evidencias import DOC, PROYECTO, SALIDA_OK, SALUD, resultado  # noqa: E402


@pytest.fixture
def cliente(tmp_path, monkeypatch):
    (tmp_path / "doc_salud.md").write_text(DOC.format(tema="salud"), encoding="utf-8")
    (tmp_path / "doc_proyecto.md").write_text(DOC.format(tema="proyecto"), encoding="utf-8")
    monkeypatch.setattr(api, "DIRECTORIO_CORPUS", tmp_path)
    monkeypatch.setattr(api, "_buscar", lambda pregunta, k=4, tema=None: [
        resultado(PROYECTO, 0.10), resultado(SALUD, 0.15, "doc_salud.md", "salud"), resultado(PROYECTO, 0.30)])
    monkeypatch.setattr(api, "_resumen_indice", lambda: {
        "modelo_embeddings": "m", "modelo_configurado": "m", "commit": "abc", "fecha_indexado": "hoy", "n_fragmentos": 50})
    return TestClient(app=api.app, raise_server_exceptions=False)


def test_salud(cliente):
    r = cliente.get("/salud")
    assert r.status_code == 200
    assert r.json()["estado"] == "ok" and r.json()["indice"]["n_fragmentos"] == 50


def test_salud_sin_indice(cliente, monkeypatch):
    def falla():
        raise IndiceNoPreparado("ejecuta rag.indexar")
    monkeypatch.setattr(api, "_resumen_indice", falla)
    r = cliente.get("/salud")
    assert r.status_code == 503 and r.json()["error"] == "IndiceNoPreparado"


def test_herramienta(cliente):
    d = cliente.get("/rag/herramienta").json()
    assert d["herramienta"]["function"]["name"] == "buscar_evidencias"
    assert d["esquema_salida"] == mod.ESQUEMA_SALIDA


def test_evidencias(cliente):
    r = cliente.post("/rag/evidencias", json={"pregunta": "¿qué dice el contenido uno?"})
    assert r.status_code == 200
    d = r.json()
    assert d["estado"] == "con_evidencias" and [e["id"] for e in d["evidencias"]] == ["D1", "D2"]
    assert d["descartados"] == 1
    assert d["para_el_modelo"]["evidencias"][0]["id"] == "D1"
    assert {ref["archivo"] for ref in d["bibliografia"]} == {"doc_proyecto.md", "doc_salud.md"}


def test_evidencias_k_fuera_de_rango_es_422(cliente):
    assert cliente.post("/rag/evidencias", json={"pregunta": "x", "k": 99}).status_code == 422


def test_consulta_invalida_es_422(cliente, monkeypatch):
    def falla(pregunta, k=4, tema=None):
        raise ConsultaInvalida("tema desconocido")
    monkeypatch.setattr(api, "_buscar", falla)
    r = cliente.post("/rag/evidencias", json={"pregunta": "x", "tema": "otro"})
    assert r.status_code == 422 and r.json()["error"] == "ConsultaInvalida"


def test_indice_no_preparado_es_503(cliente, monkeypatch):
    def falla(pregunta, k=4, tema=None):
        raise IndiceNoPreparado("ejecuta rag.indexar")
    monkeypatch.setattr(api, "_buscar", falla)
    r = cliente.post("/rag/evidencias", json={"pregunta": "x"})
    assert r.status_code == 503 and "rag.indexar" in r.json()["detalle"]


def test_validar_ok(cliente):
    r = cliente.post("/rag/validar", json={
        "pregunta": "¿qué dice el contenido uno?",
        "evidencias": [{"id": "D1", "chunk_id": PROYECTO}, {"id": "D2", "chunk_id": SALUD}],
        "salida": SALIDA_OK,
    })
    assert r.status_code == 200
    d = r.json()
    assert d["valida"] and "El contenido uno existe. [D1]" in d["texto"]
    assert [e["citada"] for e in d["evidencias"]] == [True, False]
    assert d["bibliografia"][0]["fuentes"][0]["url"] == "https://ejemplo.org/guia"


def test_validar_salida_invalida_es_200_con_reparacion(cliente):
    r = cliente.post("/rag/validar", json={
        "pregunta": "p",
        "evidencias": [{"id": "D1", "chunk_id": PROYECTO}],
        "salida": {"estado": "respondida", "afirmaciones": [{"texto": "x", "evidencias": ["D5"]}], "limitaciones": []},
    })
    assert r.status_code == 200
    d = r.json()
    assert not d["valida"] and any("'D5'" in e for e in d["errores"]) and d["mensaje_reparacion"]


def test_validar_chunk_desconocido_es_422(cliente):
    r = cliente.post("/rag/validar", json={
        "pregunta": "p", "evidencias": [{"id": "D1", "chunk_id": "no:existe:0"}], "salida": SALIDA_OK})
    assert r.status_code == 422 and r.json()["error"] == "EvidenciaDesconocida"
