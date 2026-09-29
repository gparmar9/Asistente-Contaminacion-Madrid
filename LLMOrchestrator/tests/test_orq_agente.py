import json

import pytest
from fakes_llm import ClienteLLMFalso, respuesta_llm, tool_call

from llm_orchestrator.business import agente
from llm_orchestrator.integrations import rag

MODELO = "modelo-de-prueba"


def test_respuesta_directa_sin_tools(motor):
    cliente = ClienteLLMFalso([respuesta_llm(content="El aire está bien.")])
    r = agente.responder("¿Qué tal el aire?", cliente, motor, MODELO)
    assert r.respuesta == "El aire está bien."
    assert r.fuentes == []
    assert r.advertencia is None
    # La llamada llevó las tools y el prompt de sistema en español
    llamada = cliente.llamadas[0]
    assert llamada["model"] == MODELO
    assert llamada["tool_choice"] == "auto"
    assert {t["function"]["name"] for t in llamada["tools"]} == {"query_sql", "search_documents"}
    assert llamada["messages"][0]["role"] == "system"
    assert "español" in llamada["messages"][0]["content"]


def test_bucle_con_query_sql_cita_la_tabla(motor):
    argumentos = json.dumps({"consulta": "serie_bloques", "contaminante": "NO2",
                             "estacion": 49, "fecha_inicio": "2026-09-20",
                             "fecha_fin": "2026-09-21"})
    cliente = ClienteLLMFalso([
        respuesta_llm(tool_calls=[tool_call("t1", "query_sql", argumentos)]),
        respuesta_llm(content="La media de NO2 en Retiro fue de 31,5 µg/m³."),
    ])
    r = agente.responder("¿Cómo estuvo el NO2 en Retiro?", cliente, motor, MODELO)
    assert r.respuesta.startswith("La media")
    assert [f.model_dump() for f in r.fuentes] == [{"tipo": "sql", "referencia": "resumen_datos_ml"}]
    assert r.advertencia is None
    # El resultado de la tool volvió al historial como mensaje 'tool'
    mensajes = cliente.llamadas[1]["messages"]
    del_tool = [m for m in mensajes if m.get("role") == "tool"]
    assert del_tool[0]["tool_call_id"] == "t1"
    assert "filas" in del_tool[0]["content"]


def test_bucle_con_search_documents_pone_el_aviso(motor, monkeypatch):
    monkeypatch.setattr(rag, "buscar_en_corpus",
                        lambda consulta, k=4, tema=None: [
                            {"texto": "El ozono irrita las vías...",
                             "metadatos": {"titulo": "Ozono y salud", "seccion": "Efectos",
                                           "fuente": "OMS (2021)"},
                             "distancia": 0.3}])
    cliente = ClienteLLMFalso([
        respuesta_llm(tool_calls=[tool_call("t1", "search_documents",
                                            json.dumps({"consulta": "ozono asma"}))]),
        respuesta_llm(content="Según la OMS, el ozono puede agravar el asma."),
    ])
    r = agente.responder("¿El ozono afecta al asma?", cliente, motor, MODELO)
    assert r.advertencia == agente.AVISO_MEDICO
    assert [f.model_dump() for f in r.fuentes] == [{"tipo": "documento", "referencia": "Ozono y salud"}]


def test_argumentos_ilegibles_vuelven_como_error_y_el_bucle_sigue(motor):
    cliente = ClienteLLMFalso([
        respuesta_llm(tool_calls=[tool_call("t1", "query_sql", "esto no es json")]),
        respuesta_llm(content="No he podido consultar los datos."),
    ])
    r = agente.responder("¿NO2 en Retiro?", cliente, motor, MODELO)
    assert r.respuesta == "No he podido consultar los datos."
    del_tool = [m for m in cliente.llamadas[1]["messages"] if m.get("role") == "tool"]
    assert "error" in del_tool[0]["content"]
    assert r.fuentes == []  # una tool fallida no se cita


def test_iteraciones_agotadas_fuerza_cierre_sin_tools(motor):
    argumentos = json.dumps({"consulta": "info_estaciones"})
    cliente = ClienteLLMFalso([
        respuesta_llm(tool_calls=[tool_call("t1", "query_sql", argumentos)]),
        respuesta_llm(content="Hay 24 estaciones; 2 en los datos de prueba."),
    ])
    r = agente.responder("¿Qué estaciones hay?", cliente, motor, MODELO, max_iteraciones=1)
    assert r.respuesta.startswith("Hay 24")
    # El cierre no lleva tools NI tool_choice: varios proveedores rechazan
    # tool_choice sin tools
    ultima = cliente.llamadas[-1]
    assert "tools" not in ultima
    assert "tool_choice" not in ultima
    assert [f.model_dump() for f in r.fuentes] == [{"tipo": "sql", "referencia": "estaciones"}]


def test_respuesta_vacia_tiene_texto_de_respaldo(motor):
    cliente = ClienteLLMFalso([respuesta_llm(content="   ")])
    r = agente.responder("¿?", cliente, motor, MODELO)
    assert r.respuesta == agente.RESPUESTA_VACIA


def test_fallo_del_proveedor_lanza_error_llm(motor):
    cliente = ClienteLLMFalso([RuntimeError("429 rate limit")])
    with pytest.raises(agente.ErrorLLM):
        agente.responder("¿Qué tal?", cliente, motor, MODELO)


def test_choices_vacio_lanza_error_llm(motor):
    from types import SimpleNamespace

    # Algunos gateways OpenAI-compatibles devuelven 200 con choices vacío
    cliente = ClienteLLMFalso([SimpleNamespace(choices=[])])
    with pytest.raises(agente.ErrorLLM):
        agente.responder("¿Qué tal?", cliente, motor, MODELO)
