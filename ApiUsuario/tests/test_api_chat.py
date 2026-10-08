import asyncio
import json

import httpx

from api_usuario.api.routers import chat as modulo_chat

_AsyncClientReal = httpx.AsyncClient


class _RespuestaFalsa:
    """Imita httpx.Response con un cuerpo configurable."""

    def __init__(self, cuerpo):
        self._cuerpo = cuerpo

    def raise_for_status(self):
        pass

    def json(self):
        if isinstance(self._cuerpo, Exception):
            raise self._cuerpo
        return self._cuerpo


def _cliente_falso(cuerpo, capturado=None):
    """Imita httpx.AsyncClient devolviendo siempre la misma respuesta."""

    class _ClienteFalso:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def post(self, url, **kwargs):
            if capturado is not None:
                capturado["url"] = url
                capturado["json"] = kwargs.get("json")
            return _RespuestaFalsa(cuerpo)

    return _ClienteFalso


def test_sin_orquestador_responde_stub_con_aviso(cliente, con_settings):
    con_settings(orchestrator_url="")
    r = cliente.post("/chat", json={"pregunta": "¿Cómo está el aire en Retiro?"})
    assert r.status_code == 200
    cuerpo = r.json()
    assert cuerpo["respuesta"] == modulo_chat.RESPUESTA_STUB
    assert cuerpo["advertencia"] == modulo_chat.AVISO_MEDICO
    assert cuerpo["fuentes"] == []


def test_orquestador_caido_devuelve_503(cliente, con_settings):
    # Puerto 9 (discard): la conexión falla al instante
    con_settings(orchestrator_url="http://127.0.0.1:9", timeout=0.2)
    r = cliente.post("/chat", json={"pregunta": "¿Cómo está el aire?"})
    assert r.status_code == 503


def test_proxy_devuelve_la_respuesta_del_orquestador(cliente, con_settings, monkeypatch):
    capturado = {}
    cuerpo_valido = {
        "respuesta": "El aire en Retiro está en niveles normales.",
        "fuentes": [{"tipo": "sql", "referencia": "resumen_datos_ml"}],
        "advertencia": None,
    }
    monkeypatch.setattr(modulo_chat.httpx, "AsyncClient", _cliente_falso(cuerpo_valido, capturado))
    con_settings(orchestrator_url="http://orquestador:9000")

    r = cliente.post("/chat", json={"pregunta": "¿Cómo está el aire en Retiro?"})
    assert r.status_code == 200
    assert r.json()["respuesta"] == "El aire en Retiro está en niveles normales."
    assert r.json()["fuentes"] == [{"tipo": "sql", "referencia": "resumen_datos_ml"}]
    assert capturado["url"] == "http://orquestador:9000/responder"
    # Al orquestador solo va la pregunta, pero el contrato devuelve igualmente una sesión
    assert capturado["json"] == {"pregunta": "¿Cómo está el aire en Retiro?"}
    assert r.json()["session_id"]
    assert r.json()["traza_id"] is None


def test_cuerpo_no_json_del_orquestador_devuelve_502(cliente, con_settings, monkeypatch):
    monkeypatch.setattr(
        modulo_chat.httpx, "AsyncClient", _cliente_falso(ValueError("no es JSON"))
    )
    con_settings(orchestrator_url="http://orquestador:9000")
    r = cliente.post("/chat", json={"pregunta": "¿Cómo está el aire?"})
    assert r.status_code == 502


def test_esquema_inesperado_del_orquestador_devuelve_502(cliente, con_settings, monkeypatch):
    monkeypatch.setattr(
        modulo_chat.httpx, "AsyncClient", _cliente_falso({"otra_clave": 1})
    )
    con_settings(orchestrator_url="http://orquestador:9000")
    r = cliente.post("/chat", json={"pregunta": "¿Cómo está el aire?"})
    assert r.status_code == 502


def test_pregunta_vacia_devuelve_422(cliente):
    r = cliente.post("/chat", json={"pregunta": ""})
    assert r.status_code == 422


def _con_agente(monkeypatch, manejador):
    """El cliente HTTP del router habla con `manejador` (httpx.MockTransport) en vez de con la red.
    Devuelve la lista de clientes creados, para comprobar que se cierran."""
    transporte = httpx.MockTransport(manejador)
    creados = []

    def crear(**kw):
        creados.append(_AsyncClientReal(transport=transporte, **kw))
        return creados[-1]

    monkeypatch.setattr(modulo_chat.httpx, "AsyncClient", crear)
    return creados


class _CuerpoSse(httpx.AsyncByteStream):
    """Cuerpo en streaming del agente que recuerda si se cerró."""

    def __init__(self, trozos):
        self.trozos = trozos
        self.cerrado = False

    async def __aiter__(self):
        for trozo in self.trozos:
            yield trozo

    async def aclose(self):
        self.cerrado = True


def test_chat_con_agente_reenvia_session_id_y_devuelve_traza_id(cliente, con_settings, monkeypatch):
    capturado = {}

    def agente(peticion):
        capturado["url"] = str(peticion.url)
        capturado["json"] = json.loads(peticion.content)
        return httpx.Response(200, json={
            "respuesta": "Hola", "fuentes": [], "advertencia": None,
            "traza_id": "abc123", "session_id": "sesion-1",
        })

    _con_agente(monkeypatch, agente)
    con_settings(agente_url="http://agente:8200", orchestrator_url="http://orquestador:9000")
    r = cliente.post("/chat", json={"pregunta": "Hola", "session_id": "sesion-1"})

    assert r.status_code == 200
    assert capturado["url"] == "http://agente:8200/responder"
    assert capturado["json"] == {"pregunta": "Hola", "session_id": "sesion-1"}
    assert r.json()["session_id"] == "sesion-1"
    assert r.json()["traza_id"] == "abc123"


def test_chat_stream_reenvia_el_sse_tal_cual_y_cierra(cliente, con_settings, monkeypatch):
    # Un evento partido entre dos trozos: se reenvía byte a byte, sin reinterpretarlo
    trozos = [b'event: status\ndata: {"fase": "clasificando"}\n\nevent: tok',
              b'en\ndata: {"texto": "Hola"}\n\n',
              b'event: done\ndata: {"session_id": "s1", "traza_id": "t1"}\n\n']
    cuerpos, urls = [], []

    def agente(peticion):
        urls.append(str(peticion.url))
        cuerpos.append(_CuerpoSse(trozos))
        return httpx.Response(200, stream=cuerpos[-1], headers={"content-type": "text/event-stream"})

    clientes = _con_agente(monkeypatch, agente)
    con_settings(agente_url="http://agente:8200")
    r = cliente.post("/chat/stream", json={"pregunta": "Hola", "session_id": "s1"})

    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/event-stream")
    assert urls == ["http://agente:8200/responder/stream"]
    assert r.content == b"".join(trozos)

    # El cliente se va tras el primer trozo: respuesta y cliente HTTP del agente quedan cerrados
    settings = cliente.app.dependency_overrides[modulo_chat.get_settings]()

    async def cortar_a_mitad():
        respuesta = await modulo_chat._proxy_stream(modulo_chat.PreguntaChat(pregunta="Hola"), settings)
        iterador = respuesta.body_iterator
        await iterador.__anext__()
        await iterador.aclose()

    asyncio.run(cortar_a_mitad())
    assert cuerpos[-1].cerrado
    assert clientes[-1].is_closed


def test_chat_stream_con_agente_no_disponible_devuelve_503_sin_abrir(cliente, con_settings, monkeypatch):
    _con_agente(monkeypatch, lambda peticion: httpx.Response(503, json={"detail": "sin LLM"}))
    con_settings(agente_url="http://agente:8200")
    r = cliente.post("/chat/stream", json={"pregunta": "Hola"})

    assert r.status_code == 503
    assert not r.headers["content-type"].startswith("text/event-stream")


def test_chat_stream_sin_agente_emite_passthrough_y_done(cliente, con_settings):
    con_settings(orchestrator_url="")
    r = cliente.post("/chat/stream", json={"pregunta": "Hola", "session_id": "s1"})

    assert r.status_code == 200
    eventos = [bloque.split("\n") for bloque in r.text.strip().split("\n\n")]
    assert [e[0] for e in eventos] == ["event: passthrough", "event: done"]
    passthrough = json.loads(eventos[0][1].removeprefix("data: "))
    done = json.loads(eventos[1][1].removeprefix("data: "))
    assert passthrough["texto"] == modulo_chat.RESPUESTA_STUB
    assert done == {"session_id": "s1", "traza_id": None}
