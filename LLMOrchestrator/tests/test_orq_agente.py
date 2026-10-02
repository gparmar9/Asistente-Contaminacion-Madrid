import json

import pytest
from fakes_llm import ClienteLLMFalso, respuesta_llm, tool_call

from llm_orchestrator.business import agente
from llm_orchestrator.integrations import rag
from test_orq_rag_tool import _recuperacion

MODELO = "modelo-de-prueba"

SALIDA_VALIDA = json.dumps({
    "estado": "respondida",
    "afirmaciones": [{"texto": "El ozono puede agravar el asma.", "evidencias": ["D1"]}],
    "limitaciones": [],
})


class ValidadorFalso:
    """Imita `rag.validar_evidencias` con un guion de veredictos y registra las llamadas."""

    def __init__(self, veredictos: list[dict]):
        self.veredictos = list(veredictos)
        self.llamadas: list[dict] = []

    def __call__(self, pregunta, refs, salida):
        self.llamadas.append({"pregunta": pregunta, "refs": refs, "salida": salida})
        return self.veredictos.pop(0)


def _veredicto_valido(estado="respondida", citados=("Ozono y salud",),
                      texto="El ozono puede agravar el asma. [D1]"):
    return {"valida": True, "estado": estado, "texto": texto, "errores": [],
            "mensaje_reparacion": "", "documentos_citados": list(citados)}


def _veredicto_invalido():
    return {"valida": False, "estado": "sin_evidencia", "texto": "",
            "errores": ["afirmación 1: el ID 'D9' no existe"],
            "mensaje_reparacion": "Tu respuesta anterior no cumple el contrato...",
            "documentos_citados": []}


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
    assert {t["function"]["name"] for t in llamada["tools"]} == {"query_sql", "buscar_evidencias"}
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


def test_ruta_documental_valida_y_renderiza(motor, monkeypatch):
    monkeypatch.setattr(rag, "recuperar_evidencias",
                        lambda pregunta, tema=None: _recuperacion(("D1",)))
    validador = ValidadorFalso([_veredicto_valido()])
    monkeypatch.setattr(rag, "validar_evidencias", validador)
    cliente = ClienteLLMFalso([
        respuesta_llm(tool_calls=[tool_call("t1", "buscar_evidencias",
                                            json.dumps({"pregunta": "ozono asma"}))]),
        respuesta_llm(content=SALIDA_VALIDA),
    ])
    r = agente.responder("¿El ozono afecta al asma?", cliente, motor, MODELO)
    # La respuesta es el texto RENDERIZADO por el servicio, no el JSON del modelo
    assert r.respuesta == "El ozono puede agravar el asma. [D1]"
    assert [f.model_dump() for f in r.fuentes] == [{"tipo": "documento", "referencia": "Ozono y salud"}]
    assert r.advertencia == agente.AVISO_MEDICO
    # El resultado de la tool llevó las evidencias y el recordatorio del contrato
    del_tool = [m for m in cliente.llamadas[1]["messages"] if m.get("role") == "tool"]
    assert "instrucciones" in del_tool[0]["content"]
    assert "D1" in del_tool[0]["content"]
    # La validación recibió la salida parseada y las refs con su chunk_id
    assert validador.llamadas[0]["salida"] == json.loads(SALIDA_VALIDA)
    assert validador.llamadas[0]["refs"][0]["chunk_id"] == "doc:d1:0"


def test_ruta_documental_acepta_json_con_valla(motor, monkeypatch):
    monkeypatch.setattr(rag, "recuperar_evidencias",
                        lambda pregunta, tema=None: _recuperacion(("D1",)))
    validador = ValidadorFalso([_veredicto_valido()])
    monkeypatch.setattr(rag, "validar_evidencias", validador)
    cliente = ClienteLLMFalso([
        respuesta_llm(tool_calls=[tool_call("t1", "buscar_evidencias",
                                            json.dumps({"pregunta": "ozono"}))]),
        respuesta_llm(content=f"```json\n{SALIDA_VALIDA}\n```"),
    ])
    r = agente.responder("¿Ozono?", cliente, motor, MODELO)
    assert validador.llamadas[0]["salida"] == json.loads(SALIDA_VALIDA)
    assert r.respuesta == "El ozono puede agravar el asma. [D1]"


def test_ruta_documental_repara_una_vez(motor, monkeypatch):
    monkeypatch.setattr(rag, "recuperar_evidencias",
                        lambda pregunta, tema=None: _recuperacion(("D1",)))
    validador = ValidadorFalso([_veredicto_invalido(), _veredicto_valido()])
    monkeypatch.setattr(rag, "validar_evidencias", validador)
    cliente = ClienteLLMFalso([
        respuesta_llm(tool_calls=[tool_call("t1", "buscar_evidencias",
                                            json.dumps({"pregunta": "ozono"}))]),
        respuesta_llm(content="esto no es el JSON del contrato"),
        respuesta_llm(content=SALIDA_VALIDA),
    ])
    r = agente.responder("¿Ozono y asma?", cliente, motor, MODELO)
    assert r.respuesta == "El ozono puede agravar el asma. [D1]"
    # La reparación reenvió el mensaje del servicio como turno de usuario, sin tools
    reparacion = cliente.llamadas[2]
    assert "tools" not in reparacion
    assert reparacion["messages"][-1]["role"] == "user"
    assert "no cumple el contrato" in reparacion["messages"][-1]["content"]
    # La primera validación recibió el texto crudo (no parseable como JSON)
    assert validador.llamadas[0]["salida"] == "esto no es el JSON del contrato"


def test_ruta_documental_se_rinde_tras_dos_salidas_invalidas(motor, monkeypatch):
    monkeypatch.setattr(rag, "recuperar_evidencias",
                        lambda pregunta, tema=None: _recuperacion(("D1",)))
    monkeypatch.setattr(rag, "validar_evidencias",
                        ValidadorFalso([_veredicto_invalido(), _veredicto_invalido()]))
    cliente = ClienteLLMFalso([
        respuesta_llm(tool_calls=[tool_call("t1", "buscar_evidencias",
                                            json.dumps({"pregunta": "ozono"}))]),
        respuesta_llm(content="sigo sin ser JSON"),
        respuesta_llm(content="y yo tampoco"),
    ])
    r = agente.responder("¿Ozono?", cliente, motor, MODELO)
    assert r.respuesta == agente.RESPUESTA_SIN_FUNDAMENTO
    assert r.advertencia is None


def test_dos_busquedas_renumeran_las_evidencias(motor, monkeypatch):
    monkeypatch.setattr(rag, "recuperar_evidencias",
                        lambda pregunta, tema=None: _recuperacion(("D1",)))
    validador = ValidadorFalso([_veredicto_valido()])
    monkeypatch.setattr(rag, "validar_evidencias", validador)
    cliente = ClienteLLMFalso([
        respuesta_llm(tool_calls=[tool_call("t1", "buscar_evidencias",
                                            json.dumps({"pregunta": "ozono"}))]),
        respuesta_llm(tool_calls=[tool_call("t2", "buscar_evidencias",
                                            json.dumps({"pregunta": "asma"}))]),
        respuesta_llm(content=SALIDA_VALIDA),
    ])
    agente.responder("¿Ozono y asma?", cliente, motor, MODELO)
    # La segunda recuperación (también D1 local) pasó a D2: IDs únicos en la conversación
    ids = [r["id"] for r in validador.llamadas[0]["refs"]]
    assert ids == ["D1", "D2"]
    del_tool = [m for m in cliente.llamadas[2]["messages"] if m.get("role") == "tool"]
    assert '"D2"' in del_tool[1]["content"]


def test_sin_evidencia_en_la_recuperacion_mantiene_la_ruta_libre(motor, monkeypatch):
    monkeypatch.setattr(rag, "recuperar_evidencias",
                        lambda pregunta, tema=None: _recuperacion((), estado="sin_evidencia"))
    cliente = ClienteLLMFalso([
        respuesta_llm(tool_calls=[tool_call("t1", "buscar_evidencias",
                                            json.dumps({"pregunta": "recetas"}))]),
        respuesta_llm(content="No tengo documentación sobre ese tema."),
    ])
    r = agente.responder("¿Recetas de cocina?", cliente, motor, MODELO)
    # Sin evidencias no hay contrato JSON que validar: texto libre y sin aviso
    assert r.respuesta == "No tengo documentación sobre ese tema."
    assert r.fuentes == []
    assert r.advertencia is None


def test_sin_evidencia_con_datos_sql_cierra_por_la_ruta_de_datos(motor, monkeypatch):
    monkeypatch.setattr(rag, "recuperar_evidencias",
                        lambda pregunta, tema=None: _recuperacion(("D1",)))
    monkeypatch.setattr(rag, "validar_evidencias",
                        ValidadorFalso([_veredicto_valido(estado="sin_evidencia", citados=(),
                                                          texto="No puedo responder...")]))
    sql_args = json.dumps({"consulta": "ultimos_niveles", "contaminante": "NO2"})
    sin_evidencia = json.dumps({"estado": "sin_evidencia", "afirmaciones": [], "limitaciones": []})
    cliente = ClienteLLMFalso([
        respuesta_llm(tool_calls=[tool_call("t1", "query_sql", sql_args),
                                  tool_call("t2", "buscar_evidencias",
                                            json.dumps({"pregunta": "NO2"}))]),
        respuesta_llm(content=sin_evidencia),
        respuesta_llm(content="Ayer el NO2 estuvo en niveles normales."),
    ])
    r = agente.responder("¿Cómo está el NO2?", cliente, motor, MODELO)
    # Los documentos no respaldaban nada pero había datos: responde con ellos
    assert r.respuesta == "Ayer el NO2 estuvo en niveles normales."
    assert [f.tipo for f in r.fuentes] == ["sql"]
    assert r.advertencia is None
    assert cliente.llamadas[2]["messages"][-1]["content"] == agente.CIERRE_SOLO_DATOS


def test_error_de_la_busqueda_no_activa_la_ruta_documental(motor, monkeypatch):
    def _explota(pregunta, tema=None):
        raise RuntimeError("índice no encontrado")

    monkeypatch.setattr(rag, "recuperar_evidencias", _explota)
    cliente = ClienteLLMFalso([
        respuesta_llm(tool_calls=[tool_call("t1", "buscar_evidencias",
                                            json.dumps({"pregunta": "ozono"}))]),
        respuesta_llm(content="No he podido consultar la documentación."),
    ])
    r = agente.responder("¿Ozono?", cliente, motor, MODELO)
    assert r.respuesta == "No he podido consultar la documentación."
    del_tool = [m for m in cliente.llamadas[1]["messages"] if m.get("role") == "tool"]
    assert "error" in del_tool[0]["content"]


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
    assert ultima["messages"][-1]["content"] == agente.CIERRE_LIBRE
    assert [f.model_dump() for f in r.fuentes] == [{"tipo": "sql", "referencia": "estaciones"}]


def test_iteraciones_agotadas_en_ruta_documental_pide_el_json(motor, monkeypatch):
    monkeypatch.setattr(rag, "recuperar_evidencias",
                        lambda pregunta, tema=None: _recuperacion(("D1",)))
    monkeypatch.setattr(rag, "validar_evidencias", ValidadorFalso([_veredicto_valido()]))
    cliente = ClienteLLMFalso([
        respuesta_llm(tool_calls=[tool_call("t1", "buscar_evidencias",
                                            json.dumps({"pregunta": "ozono"}))]),
        respuesta_llm(content=SALIDA_VALIDA),
    ])
    r = agente.responder("¿Ozono?", cliente, motor, MODELO, max_iteraciones=1)
    assert r.respuesta == "El ozono puede agravar el asma. [D1]"
    assert cliente.llamadas[-1]["messages"][-1]["content"] == agente.CIERRE_DOCUMENTAL


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
