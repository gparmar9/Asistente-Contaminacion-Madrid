"""El puente HTTP con el servicio RAG (integrations/rag.py): mapeo y errores.

Sin red: el transporte HTTP se finge con httpx.MockTransport y, para las
funciones públicas, se finge `_post` directamente.
"""
import httpx
import pytest

from llm_orchestrator.integrations import rag


# --------------------------------------------------------------------------- _post

def test_post_sin_rag_url_es_error_claro(monkeypatch):
    monkeypatch.delenv("RAG_URL", raising=False)
    with pytest.raises(RuntimeError, match="RAG_URL"):
        rag._post("/rag/evidencias", {"pregunta": "x"})


def test_post_devuelve_el_json_y_construye_la_url(monkeypatch):
    monkeypatch.setenv("RAG_URL", "http://rag-de-prueba:8010/")  # barra final fuera
    visto = {}

    def _responder(peticion: httpx.Request) -> httpx.Response:
        visto["url"] = str(peticion.url)
        return httpx.Response(200, json={"ok": True})

    r = rag._post("/rag/evidencias", {"pregunta": "x"},
                  transporte=httpx.MockTransport(_responder))
    assert r == {"ok": True}
    assert visto["url"] == "http://rag-de-prueba:8010/rag/evidencias"


def test_post_traduce_el_error_tipado_del_servicio(monkeypatch):
    monkeypatch.setenv("RAG_URL", "http://rag-de-prueba:8010")
    transporte = httpx.MockTransport(lambda _p: httpx.Response(
        503, json={"error": "IndiceNoPreparado", "detalle": "No existe el índice"}))
    with pytest.raises(RuntimeError, match="503.*IndiceNoPreparado.*No existe"):
        rag._post("/rag/evidencias", {"pregunta": "x"}, transporte=transporte)


def test_get_construye_la_url_y_devuelve_el_json(monkeypatch):
    monkeypatch.setenv("RAG_URL", "http://rag-de-prueba:8010")
    visto = {}

    def _responder(peticion: httpx.Request) -> httpx.Response:
        visto.update(url=str(peticion.url), metodo=peticion.method)
        return httpx.Response(200, json={"herramienta": {"type": "function"}})

    r = rag._get("/rag/herramienta", transporte=httpx.MockTransport(_responder))
    assert r == {"herramienta": {"type": "function"}}
    assert visto == {"url": "http://rag-de-prueba:8010/rag/herramienta", "metodo": "GET"}


# ------------------------------------------------------------- funciones públicas

def test_obtener_herramienta_desenvuelve_la_definicion(monkeypatch):
    definicion = {"type": "function", "function": {"name": "buscar_evidencias"}}
    monkeypatch.setattr(rag, "_get",
                        lambda ruta, transporte=None: {"herramienta": definicion,
                                                       "esquema_salida": {}})
    assert rag.obtener_herramienta() == definicion

def test_recuperar_evidencias_mapea_refs_y_carga_del_modelo(monkeypatch):
    respuesta_servicio = {
        "estado": "con_evidencias",
        "evidencias": [
            {"id": "D1", "chunk_id": "ozono_salud:efectos:0", "titulo": "Ozono y salud",
             "seccion": "Efectos", "archivo": "ozono_salud.md", "tema": "salud",
             "distancia": 0.15, "texto": "..."},
        ],
        "bibliografia": [], "avisos": {"textos": []}, "umbral": 0.22, "descartados": 2,
        "para_el_modelo": {"pregunta": "p", "estado": "con_evidencias",
                           "evidencias": [{"id": "D1", "titulo": "Ozono y salud",
                                           "seccion": "Efectos", "texto": "..."}],
                           "avisos": []},
    }
    visto = {}

    def _post_falso(ruta, cuerpo, transporte=None):
        visto.update(ruta=ruta, cuerpo=cuerpo)
        return respuesta_servicio

    monkeypatch.setattr(rag, "_post", _post_falso)
    r = rag.recuperar_evidencias("¿ozono?", tema="salud")
    assert visto["ruta"] == "/rag/evidencias"
    assert visto["cuerpo"] == {"pregunta": "¿ozono?", "tema": "salud"}
    assert r["estado"] == "con_evidencias"
    assert r["para_el_modelo"] == respuesta_servicio["para_el_modelo"]
    # refs: solo id, chunk_id y titulo (lo que necesitan la validación y las fuentes)
    assert r["refs"] == [{"id": "D1", "chunk_id": "ozono_salud:efectos:0",
                          "titulo": "Ozono y salud"}]


def test_validar_evidencias_envia_el_contrato_y_mapea_los_citados(monkeypatch):
    respuesta_servicio = {
        "valida": True, "estado": "respondida", "texto": "El ozono... [D1]",
        "errores": [], "mensaje_reparacion": "",
        "afirmaciones": [], "limitaciones": [], "evidencias": [], "aviso_sanitario": True,
        "bibliografia": [{"archivo": "ozono_salud.md", "titulo": "Ozono y salud",
                          "fecha_revision": "2026-09-23", "ids": ["D1"], "fuentes": []}],
    }
    visto = {}

    def _post_falso(ruta, cuerpo, transporte=None):
        visto.update(ruta=ruta, cuerpo=cuerpo)
        return respuesta_servicio

    monkeypatch.setattr(rag, "_post", _post_falso)
    refs = [{"id": "D1", "chunk_id": "ozono_salud:efectos:0", "titulo": "sobra"}]
    salida = {"estado": "respondida", "afirmaciones": [], "limitaciones": []}
    r = rag.validar_evidencias("¿ozono?", refs, salida)
    assert visto["ruta"] == "/rag/validar"
    # Al servicio solo viajan {id, chunk_id}: el título se resuelve del corpus
    assert visto["cuerpo"] == {"pregunta": "¿ozono?",
                               "evidencias": [{"id": "D1", "chunk_id": "ozono_salud:efectos:0"}],
                               "salida": salida}
    assert r["valida"] is True
    assert r["documentos_citados"] == ["Ozono y salud"]
    assert r["texto"] == "El ozono... [D1]"
