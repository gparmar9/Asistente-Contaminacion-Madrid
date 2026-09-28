from api_usuario.api.routers import chat as modulo_chat


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
    assert capturado["json"] == {"pregunta": "¿Cómo está el aire en Retiro?"}


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
