"""Pruebas del lector de Phoenix, el cálculo de KPIs y GET /kpis, sin red."""
import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from fastapi.testclient import TestClient

from agente.business.kpis import ServicioKpis, calcular, primer_token_ms, resultado
from agente.business.trazas import turnos
from agente.datos.phoenix import LectorPhoenix, PhoenixNoDisponible, a_formato_jsonl
from agente.entities.kpis import Periodo
from agente.main import app, get_kpis

MODELO = "mistral.ministral-3-14b-instruct"  # 0,23 / 0,23 USD por millón
INICIO = datetime(2026, 10, 23, 10, tzinfo=timezone.utc)
PERIODO = Periodo(dias=1, desde=INICIO, hasta=INICIO + timedelta(days=1), generado=INICIO + timedelta(days=1))
SECRETOS = ("¿Cuánto NO2 hubo en mi calle?", "En tu calle hubo 41 µg/m³.")  # pregunta y respuesta: nunca en la salida


def _iso(ms: float) -> str:
    return (INICIO + timedelta(milliseconds=ms)).isoformat().replace("+00:00", "Z")


def _span(traza, id_, padre, nombre, tipo, inicio_ms, ms, status="OK", eventos=(), **atributos):
    """Un span como lo devuelve `GET /v1/projects/{p}/spans`: atributos aplanados, tipo aparte."""
    return {"id": f"U3Bhbjo{traza}{id_}", "name": nombre, "context": {"trace_id": traza, "span_id": id_},
            "span_kind": tipo, "parent_id": padre, "start_time": _iso(inicio_ms), "end_time": _iso(inicio_ms + ms),
            "status_code": status, "status_message": "", "attributes": atributos,
            "events": [{"name": n, "timestamp": _iso(inicio_ms + t), "attributes": a} for n, t, a in eventos]}


def _turno(traza, inicio_ms, ms, sesion=None, eventos=(), hijos=(), tokens=(100, 20), status="OK", **atributos):
    """Crea un turno con una llamada al LLM y los spans hijos indicados."""
    if sesion:
        atributos["session.id"] = sesion
    raiz_id = f"t{traza}"
    raiz = _span(traza, raiz_id, None, "turno", "AGENT", inicio_ms, ms, status, eventos=eventos,
                 **{"input.value": SECRETOS[0], "output.value": SECRETOS[1], "agente.tema": "salud", **atributos})
    llm = {"llm.model_name": MODELO}
    if tokens:
        llm |= {"llm.token_count.prompt": tokens[0], "llm.token_count.completion": tokens[1]}
    return [raiz, _span(traza, "l", raiz_id, "llm", "LLM", inicio_ms, ms / 2, **llm),
            *(_span(traza, f"h{i}", raiz_id, *h[:2], inicio_ms, 10, **h[2]) for i, h in enumerate(hijos))]


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


def _lista(spans=None):
    return turnos([a_formato_jsonl(s) for s in spans or _phoenix()])


def _anotacion(span_id, label, cuando):
    return {"span_id": span_id, "name": "valoracion_usuario", "annotator_kind": "HUMAN",
            "result": {"label": label, "score": int(label == "positiva")}, "updated_at": cuando}


class PhoenixFingido:
    """Transporte que imita la API REST de Phoenix: spans en dos páginas y anotaciones fijas."""

    def __init__(self, spans: list[dict], anotaciones: list[dict] = (), caido: bool = False):
        self.spans, self.anotaciones, self.caido = spans, list(anotaciones), caido
        self.peticiones: list[httpx.Request] = []

    def __call__(self, peticion: httpx.Request) -> httpx.Response:
        self.peticiones.append(peticion)
        if self.caido:
            raise httpx.ConnectError("conexión rechazada", request=peticion)
        if peticion.url.path.endswith("/spans"):
            if peticion.url.params.get("cursor") == "c2":
                return httpx.Response(200, json={"data": self.spans[3:], "next_cursor": None})
            return httpx.Response(200, json={"data": self.spans[:3], "next_cursor": "c2"})
        ids = set(peticion.url.params.get_list("span_ids"))
        return httpx.Response(200, json={"data": [a for a in self.anotaciones if a["span_id"] in ids],
                                         "next_cursor": None})

    def lector(self) -> LectorPhoenix:
        return LectorPhoenix("http://phoenix:6006/", transport=httpx.MockTransport(self))


# ---------------------------------------------------------------------- lector de Phoenix

@pytest.mark.anyio
async def test_spans_sigue_el_cursor_con_el_intervalo():
    phoenix = PhoenixFingido(_phoenix())
    spans = await phoenix.lector().spans("agente", INICIO, INICIO + timedelta(days=1))

    assert len(spans) == len(_phoenix()) and len(phoenix.peticiones) == 2
    primera, segunda = phoenix.peticiones
    assert str(primera.url).startswith("http://phoenix:6006/v1/projects/agente/spans?")
    assert primera.url.params["start_time"] == "2026-10-23T10:00:00+00:00"
    assert primera.url.params["end_time"] == "2026-10-24T10:00:00+00:00"
    assert primera.url.params["limit"] == "1000" and segunda.url.params["cursor"] == "c2"


def test_el_adaptador_produce_el_formato_del_jsonl():
    s = a_formato_jsonl(_span("a", "b", "t", "consultar_datos", "TOOL", 0, 250, "ERROR",
                              eventos=[("sql_invalido", 10, {"agente": {"origen": "validador"}})],
                              **{"agente.ok": False}))
    assert (s["trace_id"], s["span_id"], s["parent_id"], s["kind"], s["status"]) == ("a", "b", "t", "TOOL", "ERROR")
    assert s["duracion_ms"] == 250 and s["attributes"]["openinference.span.kind"] == "TOOL"
    assert s["start"] == "2026-10-23T10:00:00.000000+00:00"  # con microsegundos: se ordena por el texto
    assert s["events"][0]["attributes"] == {"agente.origen": "validador"}  # aplanado si viniera anidado


@pytest.mark.anyio
async def test_votos_por_lotes_y_el_ultimo_voto_gana():
    peticiones = []

    def transporte(peticion: httpx.Request) -> httpx.Response:
        peticiones.append(peticion)
        ids = peticion.url.params.get_list("span_ids")
        if peticion.url.params.get("cursor") == "c2":
            return httpx.Response(200, json={"data": [_anotacion("ta2", "positiva", "2026-10-23T11:00:00Z")],
                                             "next_cursor": None})
        datos = [_anotacion("ta2", "negativa", "2026-10-23T10:00:00Z")] if "ta2" in ids else []
        return httpx.Response(200, json={"data": datos, "next_cursor": "c2" if datos else None})

    lector = LectorPhoenix("http://phoenix:6006", transport=httpx.MockTransport(transporte))
    votos = await lector.votos("agente", [f"s{i}" for i in range(150)] + ["ta2"])

    assert votos == {"ta2": "positiva"}  # el voto cambiado sustituye al anterior
    assert peticiones[0].url.path == "/v1/projects/agente/span_annotations"
    assert len(peticiones[0].url.params.get_list("span_ids")) == 100  # los ids van en la URL: por lotes
    assert peticiones[0].url.params["include_annotation_names"] == "valoracion_usuario"
    assert len(peticiones) == 3


@pytest.mark.anyio
@pytest.mark.parametrize("respuesta", [
    None,  # conexión rechazada
    httpx.Response(500),
    httpx.Response(200, text="<html>no es JSON</html>"),
    httpx.Response(200, json={"data": [{"name": "turno"}], "next_cursor": None}),  # span sin contexto
])
async def test_phoenix_caido_o_con_formato_inesperado(respuesta):
    def transporte(peticion: httpx.Request) -> httpx.Response:
        if respuesta is None:
            raise httpx.ConnectError("conexión rechazada", request=peticion)
        return respuesta

    lector = LectorPhoenix("http://phoenix:6006", transport=httpx.MockTransport(transporte))
    with pytest.raises(PhoenixNoDisponible):
        await lector.spans("agente", INICIO, INICIO + timedelta(days=1))


# ---------------------------------------------------------------------- cálculo

def test_resultado_del_turno_y_primer_token():
    lista, huerfanas = _lista()
    assert huerfanas == 0
    assert {t.trace_id: resultado(t) for t in lista} == {
        "a1": "sin_evidencia", "a2": "respondida", "a3": "respondida", "b1": "servicio_no_disponible",
        "c1": "bloqueada"}
    assert [primer_token_ms(t) for t in lista if t.trace_id in ("a2", "a3")] == [None, 800]


def test_kpis():
    lista, _ = _lista()
    k = calcular(lista, 0, {}, PERIODO)

    r = k.resumen
    assert (r.preguntas, r.dias_con_uso, r.resueltas) == (5, 1, 2)
    assert (r.primer_token_ms.n, r.primer_token_ms.p50) == (1, 800)
    assert (r.latencia_ms.n, r.latencia_ms.p50, r.latencia_ms.p95) == (5, 2000, 4000)
    assert [(d.dia.isoformat(), d.preguntas, d.resueltas) for d in k.por_dia] == [("2026-10-23", 5, 2)]
    assert {c.categoria: c.n for c in k.resultados} == {
        "respondida": 2, "sin_evidencia": 1, "servicio_no_disponible": 1, "bloqueada": 1}
    assert [(c.categoria, c.n) for c in k.intenciones] == [("DOCUMENTAL", 2), ("DATOS", 2), ("CHARLA", 1)]
    # coste: 4 turnos con tokens (120 * 0,23 / 1e6 cada uno); el bloqueado no tiene tokens
    assert k.coste.turnos_con_coste == 4 and k.coste.valores_usd == pytest.approx([2.76e-5] * 4)
    assert k.coste.total_usd == pytest.approx(1.104e-4)
    assert k.coste.por_resuelta_usd == pytest.approx(5.52e-5)  # total / 2 resueltas
    assert k.coste.modelos == [MODELO] and k.resumen.coste_usd.n == 4
    assert [(p.ruta, p.ms.n) for p in k.primer_token_por_ruta] == [("libre", 1)]
    assert (k.calidad.sintesis_documentales, k.calidad.validas_a_la_primera, k.calidad.reparaciones_medias) == (1, 0, 1)
    assert k.comprobaciones.turnos_bloqueados == 1
    h = k.herramientas
    assert [(f.herramienta, f.causa, f.fallos) for f in h.fallos] == [("consultar_datos", "no_disponible", 1)]
    assert (h.llamadas, h.preguntas_de_datos, h.datos_no_disponibles) == (1, 2, 1)
    # sesiones: s1 con 3 turnos (uno tras un rechazo, resuelto) y s2 con uno; c1 no tiene sesión
    s = k.sesiones
    assert (s.sesiones, s.turnos_mediana, s.turnos_maximo, s.de_un_turno) == (2, 2, 3, 1)
    assert (s.duracion_s.n, s.duracion_s.p50) == (1, 122)
    assert (s.turnos_con_previo, s.tras_rechazo, s.resueltos_tras_rechazo) == (2, 1, 1)
    assert k.satisfaccion is None  # sin votos


def test_comprobaciones_por_regla():
    eventos = [("comprobacion", 5, {"agente.regla": "cifras", "agente.bloquea": True}),
               ("comprobacion", 6, {"agente.regla": "cifras"}), ("comprobacion", 7, {"agente.regla": "fuga"})]
    spans = [*_turno("x1", 0, 100, eventos=eventos, **{"agente.ruta": "datos"}),
             *_turno("x2", 1000, 100, eventos=eventos[1:2], **{"agente.ruta": "datos"})]
    reglas = calcular(_lista(spans)[0], 0, {}, PERIODO).comprobaciones.reglas
    assert [(r.regla, r.turnos, r.bloqueados) for r in reglas] == [("cifras", 2, 1), ("fuga", 1, 0)]


def test_sin_sesiones_ni_primer_token_ni_coste():
    spans = [s for s in _phoenix() if s["context"]["trace_id"] in ("a1", "c1")]
    for s in spans:
        s["attributes"].pop("session.id", None)
    spans = [s for s in spans if s["context"]["trace_id"] == "c1"]  # sin tokens
    k = calcular(_lista(spans)[0], 0, {}, PERIODO)
    assert k.sesiones is None and k.coste is None and k.primer_token_por_ruta == []
    assert k.resumen.primer_token_ms.n == 0 and k.resumen.primer_token_ms.p50 is None


def test_satisfaccion_por_ruta_y_tema():
    lista, _ = _lista()
    votos = {"ta2": "positiva", "ta3": "negativa", "tc1": "positiva", "otro": "negativa"}
    s = calcular(lista, 0, votos, PERIODO).satisfaccion

    assert (s.votos, s.positivos) == (3, 2)  # el voto de un span de fuera del periodo no cuenta
    assert {v.grupo: (v.positivos, v.votos) for v in s.por_ruta} == {
        "documental": (1, 1), "libre": (0, 1), "datos": (1, 1)}
    assert [(v.grupo, v.positivos, v.votos) for v in s.por_tema] == [("salud", 2, 3)]
    assert (s.sesiones_elegibles, s.sesiones_con_voto) == (1, 1)  # s1 es la única de 2 o más turnos


def test_la_participacion_solo_cuenta_sesiones_con_dos_respuestas_completas():
    """Como la web: un turno fallido o cancelado no llega a `done` y no cuenta para el aviso."""
    otras = [*_turno("d1", 300_000, 1000, "s3", **{"agente.ruta": "libre", "agente.intencion": "CHARLA"}),
             *_turno("d2", 310_000, 1000, "s3", status="ERROR"),
             *_turno("e1", 320_000, 1000, "s4", **{"agente.ruta": "libre", "agente.intencion": "CHARLA"}),
             *_turno("e2", 330_000, 1000, "s4", **{"agente.cancelado": True})]
    s = calcular(_lista([*_phoenix(), *otras])[0], 0, {"ta3": "positiva"}, PERIODO).satisfaccion
    assert (s.sesiones_elegibles, s.sesiones_con_voto) == (1, 1)  # s3 y s4 tienen 2 turnos, pero 1 respuesta


def test_kpis_vacios():
    k = calcular([], 3, {}, PERIODO)
    assert k.huerfanas == 3 and k.resumen.preguntas == 0 and k.por_dia == [] and k.resultados == []
    assert k.coste is None and k.sesiones is None and k.satisfaccion is None


# ---------------------------------------------------------------------- servicio y endpoint

@pytest.mark.anyio
async def test_el_servicio_guarda_el_resultado_60_segundos_por_periodo():
    phoenix = PhoenixFingido(_phoenix(), [_anotacion("ta2", "positiva", "2026-10-23T11:00:00Z")])
    reloj = [0.0]
    servicio = ServicioKpis(phoenix.lector(), "agente", reloj=lambda: reloj[0],
                            ahora=lambda: INICIO + timedelta(days=1))

    primera = await servicio.obtener(7)
    assert primera.periodo.desde == INICIO - timedelta(days=6) and primera.satisfaccion.votos == 1
    leidas = len(phoenix.peticiones)  # 2 páginas de spans + 1 de anotaciones
    assert leidas == 3

    reloj[0] = 59
    assert await servicio.obtener(7) is primera and len(phoenix.peticiones) == leidas
    await servicio.obtener(1)  # otro periodo: se lee
    assert len(phoenix.peticiones) == 2 * leidas
    reloj[0] = 61
    assert await servicio.obtener(7) is not primera


@pytest.fixture
def cliente():
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def test_get_kpis_sin_texto_de_usuarios(cliente):
    phoenix = PhoenixFingido(_phoenix(), [_anotacion("ta2", "positiva", "2026-10-23T11:00:00Z")])
    app.dependency_overrides[get_kpis] = lambda: ServicioKpis(phoenix.lector(), "agente")

    r = cliente.get("/kpis", params={"dias": 30})
    assert r.status_code == 200
    cuerpo = r.json()
    assert cuerpo["periodo"]["dias"] == 30 and cuerpo["resumen"]["preguntas"] == 5
    assert cuerpo["satisfaccion"]["votos"] == 1
    texto = json.dumps(cuerpo, ensure_ascii=False)
    assert not any(s in texto for s in SECRETOS)


@pytest.mark.parametrize("servicio", [
    None,  # sin PHOENIX_ENDPOINT
    ServicioKpis(PhoenixFingido([], caido=True).lector(), "agente"),
])
def test_get_kpis_sin_phoenix_devuelve_503(cliente, servicio):
    app.dependency_overrides[get_kpis] = lambda: servicio
    r = cliente.get("/kpis")
    assert r.status_code == 503 and "Phoenix" in r.json()["detail"]


def test_get_kpis_con_dias_no_permitidos_devuelve_422(cliente):
    app.dependency_overrides[get_kpis] = lambda: None
    assert cliente.get("/kpis", params={"dias": 3}).status_code == 422
