import httpx
import pytest

from api_usuario.api.routers import kpis as modulo_kpis

_AsyncClientReal = httpx.AsyncClient

_SIN_DATOS = {"n": 0, "p50": None, "p90": None, "p95": None}
KPIS = {
    "periodo": {"dias": 30, "desde": "2026-09-23T12:00:00+02:00", "hasta": "2026-10-23T12:00:00+02:00",
                "generado": "2026-10-23T12:00:00+02:00"},
    "precios": "2026-10-08, Bedrock eu-west-1",
    "huerfanas": 0,
    "resumen": {"preguntas": 2, "dias_con_uso": 1, "resueltas": 1,
                "latencia_ms": {"n": 2, "p50": 1000.0, "p90": 2500.5, "p95": 2500.5},
                "primer_token_ms": _SIN_DATOS, "coste_usd": {"n": 1, "p50": 2.76e-05, "p90": 2.76e-05, "p95": 2.76e-05}},
    "por_dia": [{"dia": "2026-10-23", "preguntas": 2, "resueltas": 1}],
    "intenciones": [{"categoria": "DATOS", "n": 2}],
    "rutas": [{"categoria": "datos", "n": 1}, {"categoria": "fija", "n": 1}],
    "temas": [{"categoria": "?", "n": 2}],
    "resultados": [{"categoria": "respondida", "n": 1}, {"categoria": "servicio_no_disponible", "n": 1}],
    "coste": {"turnos_con_coste": 1, "total_usd": 2.76e-05, "por_resuelta_usd": 2.76e-05,
              "modelos": ["mistral.ministral-3-14b-instruct"], "valores_usd": [2.76e-05]},
    "latencia_por_ruta": [{"ruta": "datos", "ms": {"n": 1, "p50": 2500.5, "p90": 2500.5, "p95": 2500.5}}],
    "primer_token_por_ruta": [],
    "calidad": {"sintesis_documentales": 0, "validas_a_la_primera": 0, "reparaciones_medias": None,
                "busquedas_forzadas": 0, "consultas_forzadas": 0, "sintesis_forzadas": 0},
    "comprobaciones": {"turnos_bloqueados": 0, "reglas": []},
    "herramientas": {"llamadas": 1, "fallos": [{"herramienta": "consultar_datos", "causa": "no_disponible",
                                                 "fallos": 1}],
                     "preguntas_de_datos": 2, "datos_no_disponibles": 1},
    "sesiones": None,
    "satisfaccion": None,
}


def _con_agente(monkeypatch, manejador):
    """El cliente HTTP del router habla con `manejador` (httpx.MockTransport) en vez de con la red."""
    transporte = httpx.MockTransport(manejador)
    monkeypatch.setattr(modulo_kpis.httpx, "AsyncClient", lambda **kw: _AsyncClientReal(transport=transporte, **kw))


def test_kpis_se_reenvian_del_agente(cliente, con_settings, monkeypatch):
    pedidas = []

    def agente(peticion):
        pedidas.append(str(peticion.url))
        return httpx.Response(200, json=KPIS)

    _con_agente(monkeypatch, agente)
    con_settings(agente_url="http://agente:8200/")
    r = cliente.get("/kpis", params={"dias": 30})

    assert r.status_code == 200
    assert r.json() == KPIS
    assert pedidas == ["http://agente:8200/kpis?dias=30"]


def test_kpis_por_defecto_piden_siete_dias(cliente, con_settings, monkeypatch):
    pedidas = []
    _con_agente(monkeypatch, lambda p: pedidas.append(p.url.params["dias"]) or httpx.Response(200, json=KPIS))
    con_settings(agente_url="http://agente:8200")
    assert cliente.get("/kpis").status_code == 200 and pedidas == ["7"]


def test_kpis_sin_agente_devuelven_404(cliente, con_settings):
    con_settings(agente_url="")
    assert cliente.get("/kpis").status_code == 404


@pytest.mark.parametrize("respuesta", [
    None,  # agente caído
    httpx.Response(503, json={"detail": "Los KPIs no están disponibles: no se pudo leer Phoenix"}),
])
def test_kpis_con_el_agente_o_phoenix_caidos_devuelven_503(cliente, con_settings, monkeypatch, respuesta):
    def agente(peticion):
        if respuesta is None:
            raise httpx.ConnectError("conexión rechazada", request=peticion)
        return respuesta

    _con_agente(monkeypatch, agente)
    con_settings(agente_url="http://agente:8200")
    assert cliente.get("/kpis").status_code == 503


@pytest.mark.parametrize("cuerpo", [{"resumen": {}}, "no es JSON"])
def test_kpis_fuera_de_contrato_devuelven_502(cliente, con_settings, monkeypatch, cuerpo):
    _con_agente(monkeypatch, lambda p: httpx.Response(200, **({"json": cuerpo} if isinstance(cuerpo, dict)
                                                              else {"text": cuerpo})))
    con_settings(agente_url="http://agente:8200")
    assert cliente.get("/kpis").status_code == 502


def test_kpis_con_dias_no_permitidos_devuelven_422(cliente, con_settings):
    con_settings(agente_url="http://agente:8200")
    assert cliente.get("/kpis", params={"dias": 3}).status_code == 422
