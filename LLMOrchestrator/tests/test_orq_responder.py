import json

from fakes_llm import ClienteLLMFalso, respuesta_llm, tool_call


def test_health_responde_ok(cliente_api):
    cliente = cliente_api(ClienteLLMFalso([]))
    r = cliente.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_responder_cumple_el_contrato(cliente_api):
    argumentos = json.dumps({"consulta": "ultimos_niveles", "contaminante": "NO2"})
    llm = ClienteLLMFalso([
        respuesta_llm(tool_calls=[tool_call("t1", "query_sql", argumentos)]),
        respuesta_llm(content="Los niveles de NO2 de ayer fueron normales."),
    ])
    cliente = cliente_api(llm)
    r = cliente.post("/responder", json={"pregunta": "¿Cómo está el NO2?"})
    assert r.status_code == 200
    cuerpo = r.json()
    # Exactamente el contrato que espera ApiUsuario
    assert set(cuerpo) == {"respuesta", "fuentes", "advertencia"}
    assert cuerpo["fuentes"] == [{"tipo": "sql", "referencia": "resumen_datos_ml"}]
    assert cuerpo["advertencia"] is None


def test_fallo_del_llm_devuelve_503(cliente_api):
    cliente = cliente_api(ClienteLLMFalso([RuntimeError("proveedor caído")]))
    r = cliente.post("/responder", json={"pregunta": "¿Cómo está el aire?"})
    assert r.status_code == 503


def test_pregunta_vacia_devuelve_422(cliente_api):
    cliente = cliente_api(ClienteLLMFalso([]))
    r = cliente.post("/responder", json={"pregunta": ""})
    assert r.status_code == 422
