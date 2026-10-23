"""KPIs desde Phoenix con spans escritos a mano en el formato de su API REST: sin red ni Phoenix."""
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

import httpx

from evaluacion.informe_trazas import turnos
from evaluacion.kpis_phoenix import (a_formato_jsonl, a_html, a_markdown, construir, descargar, primer_token_ms,
                                     resultado)

MODELO = "mistral.ministral-3-14b-instruct"  # 0,23 / 0,23 USD por millón
INICIO = datetime(2026, 10, 23, 10, tzinfo=timezone.utc)
SECRETOS = ("¿Cuánto NO2 hubo en mi calle?", "En tu calle hubo 41 µg/m³.")  # pregunta y respuesta: nunca en la salida


def _iso(ms: float) -> str:
    return (INICIO + timedelta(milliseconds=ms)).isoformat().replace("+00:00", "Z")


def _span(traza, id_, padre, nombre, tipo, inicio_ms, ms, status="OK", eventos=(), **atributos):
    """Un span como lo devuelve `GET /v1/projects/{p}/spans`: atributos aplanados, tipo aparte."""
    return {"id": f"U3Bhbjo{traza}{id_}", "name": nombre, "context": {"trace_id": traza, "span_id": id_},
            "span_kind": tipo, "parent_id": padre, "start_time": _iso(inicio_ms), "end_time": _iso(inicio_ms + ms),
            "status_code": status, "status_message": "", "attributes": atributos,
            "events": [{"name": n, "timestamp": _iso(inicio_ms + t), "attributes": a} for n, t, a in eventos]}


def _turno(traza, inicio_ms, ms, sesion=None, eventos=(), hijos=(), tokens=(100, 20), **atributos):
    """Span `turno` con una llamada al LLM (sin tokens si `tokens` es None) y los hijos que se pasen."""
    if sesion:
        atributos["session.id"] = sesion
    raiz = _span(traza, "t", None, "turno", "AGENT", inicio_ms, ms, eventos=eventos,
                 **{"input.value": SECRETOS[0], "output.value": SECRETOS[1], "agente.tema": "salud", **atributos})
    llm = {"llm.model_name": MODELO}
    if tokens:
        llm |= {"llm.token_count.prompt": tokens[0], "llm.token_count.completion": tokens[1]}
    return [raiz, _span(traza, "l", "t", "llm", "LLM", inicio_ms, ms / 2, **llm),
            *(_span(traza, f"h{i}", "t", *h[:2], inicio_ms, 10, **h[2]) for i, h in enumerate(hijos))]


def _phoenix() -> list[dict]:
    """Sesión s1: sin evidencia, reformula y se resuelve, y una charla en tokens. Sesión s2: base caída."""
    decision = ("decision", 5, {"agente.decision": "datos_no_disponibles"})
    caida = ("consultar_datos", "TOOL", {"agente.ok": False, "agente.permitida": True, "agente.estado": "no_disponible"})
    return [
        *_turno("a1", 0, 1000, "s1", **{"agente.ruta": "sin_evidencia", "agente.intencion": "DOCUMENTAL"}),
        *_turno("a2", 60_000, 3000, "s1", **{"agente.ruta": "documental", "agente.intencion": "DOCUMENTAL",
                                             "agente.valida": True, "agente.reparaciones": 1}),
        *_turno("a3", 120_000, 2000, "s1", eventos=[("primer_token", 800, {})],
                **{"agente.ruta": "libre", "agente.intencion": "CHARLA"}),
        *_turno("b1", 30_000, 500, "s2", eventos=[decision], hijos=[caida],
                **{"agente.ruta": "fija", "agente.intencion": "DATOS"}),
        *_turno("c1", 200_000, 4000, tokens=None, **{"agente.ruta": "datos", "agente.intencion": "DATOS",
                                                    "agente.bloqueada": True}),
    ]


def _lista():
    return turnos([a_formato_jsonl(s) for s in _phoenix()])


def test_descargar_sigue_el_cursor_con_el_intervalo_y_la_contrasena():
    peticiones = []
    paginas = {None: {"data": _phoenix()[:3], "next_cursor": "c2"}, "c2": {"data": _phoenix()[3:], "next_cursor": None}}

    def transporte(peticion: httpx.Request) -> httpx.Response:
        peticiones.append(peticion)
        return httpx.Response(200, json=paginas[peticion.url.params.get("cursor")])

    cliente = httpx.Client(base_url="http://ec2/phoenix", auth=httpx.BasicAuth("demo", "x"),
                           transport=httpx.MockTransport(transporte))
    spans = descargar(cliente, "agente", INICIO, INICIO + timedelta(days=1))

    assert len(spans) == len(_phoenix()) and len(peticiones) == 2
    assert peticiones[0].url.path == "/phoenix/v1/projects/agente/spans"
    assert peticiones[0].url.params["start_time"] == "2026-10-23T10:00:00+00:00"
    assert peticiones[0].url.params["limit"] == "1000" and peticiones[1].url.params["cursor"] == "c2"
    assert peticiones[0].headers["authorization"].startswith("Basic ")


def test_el_adaptador_produce_el_formato_del_jsonl():
    s = a_formato_jsonl(_span("a", "b", "t", "consultar_datos", "TOOL", 0, 250, "ERROR",
                              eventos=[("sql_invalido", 10, {"agente": {"origen": "validador"}})],
                              **{"agente.ok": False}))
    assert (s["trace_id"], s["span_id"], s["parent_id"], s["kind"], s["status"]) == ("a", "b", "t", "TOOL", "ERROR")
    assert s["duracion_ms"] == 250 and s["attributes"]["openinference.span.kind"] == "TOOL"
    assert s["start"] == "2026-10-23T10:00:00.000000+00:00"  # con microsegundos: se ordena por el texto
    assert s["events"][0]["attributes"] == {"agente.origen": "validador"}  # aplanado si viniera anidado


def test_resultado_del_turno_y_primer_token():
    lista, huerfanas = _lista()
    assert huerfanas == 0
    assert {t.trace_id: resultado(t) for t in lista} == {
        "a1": "sin_evidencia", "a2": "respondida", "a3": "respondida", "b1": "servicio_no_disponible",
        "c1": "bloqueada"}
    assert [primer_token_ms(t) for t in lista if t.trace_id in ("a2", "a3")] == [None, 800]


def test_kpis_del_informe():
    lista, _ = _lista()
    texto, figuras = a_markdown(construir(lista, 0, "prueba"), "kpis")

    assert "| Preguntas (turnos) | 5 | 5 |" in texto
    assert "| Tasa de resolución (respondida o frase fija) | 40,0 % | 5 |" in texto
    assert "| Primer token P50 / P95 (ms) | 800 / 800 | 1 |" in texto
    assert "| respondida | 2 | 40,0 % |" in texto and "| servicio_no_disponible | 1 | 20,0 % |" in texto
    # coste: 4 turnos con tokens (120 * 0,23 / 1e6 cada uno); el bloqueado no tiene tokens
    assert "Turnos con coste: 4 de 5" in texto and "| Coste total | 0,0001 |" in texto
    assert "| Coste por pregunta resuelta (total / resueltas) | 0,00006 |" in texto  # 4 * 0,0000276 / 2
    assert "| Síntesis documental válida a la primera | 0,0 % | 1 |" in texto
    assert "| consultar_datos | no_disponible | 1 |" in texto
    assert "base no pudo responder (decisión `datos_no_disponibles`; p. ej. la RDS apagada fuera de horario): 1 de 2" in texto
    # sesiones: s1 con 3 turnos (uno tras un rechazo, resuelto) y s2 con uno; c1 no tiene sesión
    assert "| Sesiones | 2 | 2 |" in texto and "| Turnos por sesión: mediana / máximo | 2,0 / 3 | 2 |" in texto
    assert "| Duración de la sesión P50 (s), con 2 o más turnos | 122 | 1 |" in texto
    assert "| Tasa de reformulación: turnos tras un rechazo | 50,0 % | 2 |" in texto
    assert "| De ellos, resueltos a la segunda (rechazo evitable) | 100,0 % | 1 |" in texto
    assert sorted(figuras) == ["kpis_coste.svg", "kpis_embudo.svg"] and "![Coste por pregunta](kpis_coste.svg)" in texto
    for svg in figuras.values():
        ET.fromstring(svg)  # SVG bien formado


def test_sin_sesiones_ni_primer_token_salen_sin_datos():
    spans = [s for s in _phoenix() if s["context"]["trace_id"] in ("a1", "a2")]
    for s in spans:
        s["attributes"].pop("session.id", None)
    texto, _ = a_markdown(construir(turnos([a_formato_jsonl(s) for s in spans])[0], 0, "prueba"))
    assert "Sin datos: los spans `turno` aún no llevan `session.id`." in texto
    assert "Sin datos: ningún turno lleva el evento `primer_token`" in texto
    assert "Sin datos: la valoración de los usuarios" in texto


def test_la_salida_no_lleva_texto_de_usuarios():
    lista, _ = _lista()
    informe = construir(lista, 0, "prueba", detalle=True)
    texto, figuras = a_markdown(informe, "kpis")
    pagina = a_html(informe)
    assert "| 2026-10-23 12:00:00 | a1 | S1 | DOCUMENTAL | sin_evidencia |" in texto  # el detalle, en hora de Madrid
    for salida in (texto, pagina, *figuras.values()):
        assert not any(s in salida for s in SECRETOS)
    assert pagina.startswith("<!doctype html>") and pagina.count("<svg") == 2


def test_informe_vacio():
    texto, figuras = a_markdown(construir([], 3, "prueba"), "kpis")
    assert "0 turnos, sin turnos." in texto and "descartadas" in texto and figuras == {}
