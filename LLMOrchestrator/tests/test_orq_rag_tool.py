import pytest

from llm_orchestrator.integrations import rag
from llm_orchestrator.tools import rag_tool, registro


def _recuperacion(ids=("D1",), estado="con_evidencias"):
    """Lo que devuelve el puente con el servicio de evidencias (paso 1)."""
    return {
        "estado": estado,
        "para_el_modelo": {
            "pregunta": "p", "estado": estado,
            "evidencias": [{"id": i, "titulo": f"Doc {i}", "seccion": "S", "texto": "..."}
                           for i in ids],
            "avisos": [],
        },
        "refs": [{"id": i, "chunk_id": f"doc:{i.lower()}:0", "titulo": f"Doc {i}"}
                 for i in ids],
    }


def test_devuelve_la_recuperacion_tal_cual(monkeypatch):
    recuperacion = _recuperacion(("D1", "D2"))
    monkeypatch.setattr(rag, "recuperar_evidencias",
                        lambda pregunta, tema=None: recuperacion)
    r = rag_tool.buscar_evidencias(pregunta="límites de NO2")
    assert r == recuperacion


def test_recorta_la_pregunta_y_pasa_el_tema(monkeypatch):
    visto = {}

    def _fake(pregunta, tema=None):
        visto.update(pregunta=pregunta, tema=tema)
        return _recuperacion()

    monkeypatch.setattr(rag, "recuperar_evidencias", _fake)
    rag_tool.buscar_evidencias(pregunta="  ozono y asma  ", tema=" salud ")
    assert visto == {"pregunta": "ozono y asma", "tema": "salud"}


def test_pregunta_vacia_es_error():
    assert "pregunta" in rag_tool.buscar_evidencias(pregunta="  ")["error"]


def test_tema_desconocido_es_error():
    r = rag_tool.buscar_evidencias(pregunta="ozono", tema="deportes")
    assert "'tema'" in r["error"]
    assert "salud" in r["error"]  # el error enseña los valores válidos


def test_fallo_del_indice_vuelve_como_error(monkeypatch):
    def _explota(pregunta, tema=None):
        raise RuntimeError("índice no encontrado")

    monkeypatch.setattr(rag, "recuperar_evidencias", _explota)
    r = rag_tool.buscar_evidencias(pregunta="ozono")
    assert "índice no encontrado" in r["error"]


# ------------------------------------------------- esquema_tool (GET /rag/herramienta)

def _herramienta_del_servicio(nombre="buscar_evidencias"):
    return {"type": "function",
            "function": {"name": nombre, "description": "del servicio",
                         "parameters": {"type": "object", "properties": {}}}}


def test_esquema_tool_usa_la_definicion_del_servicio_y_la_cachea(monkeypatch):
    rag_tool._esquema_cacheado = None
    llamadas = []

    def _fake():
        llamadas.append(1)
        return _herramienta_del_servicio()

    monkeypatch.setattr(rag, "obtener_herramienta", _fake)
    assert rag_tool.esquema_tool()["function"]["description"] == "del servicio"
    assert rag_tool.esquema_tool()["function"]["description"] == "del servicio"
    assert len(llamadas) == 1  # el segundo acceso sale de la caché


def test_esquema_tool_lanza_si_el_servicio_falla_y_reintenta_despues(monkeypatch):
    rag_tool._esquema_cacheado = None

    def _explota():
        raise RuntimeError("RAG_URL no está configurada")

    monkeypatch.setattr(rag, "obtener_herramienta", _explota)
    with pytest.raises(RuntimeError, match="RAG_URL"):
        rag_tool.esquema_tool()
    # el fallo no se cachea: cuando el RAG vuelve, la definición se obtiene
    monkeypatch.setattr(rag, "obtener_herramienta", _herramienta_del_servicio)
    assert rag_tool.esquema_tool()["function"]["description"] == "del servicio"


def test_esquema_tool_rechaza_una_tool_con_otro_nombre(monkeypatch):
    rag_tool._esquema_cacheado = None
    monkeypatch.setattr(rag, "obtener_herramienta",
                        lambda: _herramienta_del_servicio(nombre="otra_tool"))
    with pytest.raises(RuntimeError, match="otra_tool"):
        rag_tool.esquema_tool()


def test_esquemas_tools_omite_la_tool_si_el_rag_no_responde(monkeypatch):
    rag_tool._esquema_cacheado = None

    def _explota():
        raise RuntimeError("conexión rechazada")

    monkeypatch.setattr(rag, "obtener_herramienta", _explota)
    nombres = [e["function"]["name"] for e in registro.esquemas_tools()]
    assert nombres == ["query_sql"]  # el agente sigue con la ruta de datos
    monkeypatch.setattr(rag, "obtener_herramienta", _herramienta_del_servicio)
    nombres = [e["function"]["name"] for e in registro.esquemas_tools()]
    assert nombres == ["query_sql", "buscar_evidencias"]
