"""Informe de trazas sobre un JSONL escrito a mano: conteos, medianas, tokens incompletos y HTML."""
import json
from datetime import datetime, timedelta, timezone

from evaluacion.informe_trazas import cargar, informe, informe_html, turnos

MODELO = "mistral.ministral-3-14b-instruct"  # 0,24 / 0,24 USD por millón
INICIO = datetime(2026, 10, 6, tzinfo=timezone.utc)


def _span(traza, id_, padre, nombre, kind, ms, inicio_ms=0, status="OK", eventos=(), **atributos):
    inicio = INICIO + timedelta(milliseconds=inicio_ms)
    return {"trace_id": traza, "span_id": id_, "parent_id": padre, "name": nombre, "kind": kind,
            "start": inicio.isoformat(), "end": (inicio + timedelta(milliseconds=ms)).isoformat(),
            "duracion_ms": ms, "status": status, "status_descripcion": None,
            "attributes": {"openinference.span.kind": kind, **atributos},
            "events": [{"name": "decision", "time": inicio.isoformat(), "attributes": {"agente.decision": d}}
                       for d in eventos]}


def _llm(traza, id_, padre, ms, entrada=None, salida=None, inicio_ms=0):
    tokens = {} if entrada is None else {"llm.token_count.prompt": entrada, "llm.token_count.completion": salida}
    return _span(traza, id_, padre, "llm", "LLM", ms, inicio_ms, **{"llm.model_name": MODELO}, **tokens)


def _documental(traza, ms, reparaciones, inicio_ms):
    turno = _span(traza, "t", None, "turno", "AGENT", ms, inicio_ms, **{
        "agente.etiqueta": "lote", "agente.ruta": "documental", "agente.intencion": "DOCUMENTAL",
        "agente.valida": True, "agente.reparaciones": reparaciones, "agente.busqueda_forzada": False})
    spans = [turno, _span(traza, "c", "t", "clasificar", "CHAIN", 100, inicio_ms),
             _llm(traza, "c1", "c", 100, 10, 1, inicio_ms),
             _span(traza, "b", "t", "bucle", "CHAIN", 300, inicio_ms + 100),
             _llm(traza, "b1", "b", 200, 100, 10, inicio_ms + 100),
             _span(traza, "h", "b", "buscar_evidencias", "TOOL", 50, inicio_ms + 300, **{"agente.permitida": True}),
             _span(traza, "s", "t", "sintesis_documental", "CHAIN", 400, inicio_ms + 400)]
    for i in range(reparaciones + 1):
        spans.append(_llm(traza, f"s{i}", "s", 300, 200, 20, inicio_ms + 400 + i))
        spans.append(_span(traza, f"v{i}", "s", "validar", "TOOL", 10, inicio_ms + 700 + i,
                           **{"agente.valida": i == reparaciones}))
    return spans


def _escribir(ruta, spans):
    ruta.write_text("".join(json.dumps(s) + "\n" for s in spans), encoding="utf-8")
    return ruta


def test_conteos_y_medianas_de_tres_turnos(tmp_path):
    fija = [_span("f", "t", None, "turno", "AGENT", 2000, 20_000, eventos=["frase_fija:DATOS"],
                  **{"agente.etiqueta": "lote", "agente.ruta": "fija", "agente.intencion": "DATOS"}),
            _span("f", "c", "t", "clasificar", "CHAIN", 100, 20_000),
            _llm("f", "c1", "c", 100, 10, 1, 20_000)]
    ruta = _escribir(tmp_path / "t.jsonl", _documental("a", 1000, 0, 0) + _documental("b", 3000, 1, 10_000) + fija)

    lista, huerfanas = turnos(cargar([ruta]))
    assert huerfanas == 0 and [t.ruta for t in lista] == ["documental", "documental", "fija"]
    assert [(t.tokens_entrada, t.tokens_salida) for t in lista] == [(310, 31), (510, 51), (10, 1)]
    texto = informe(lista)
    assert "| lote | " + MODELO + " | 3 | 2000 | 3000 | 310 / 31 |" in texto  # medianas de los tres
    assert "| 0,0002 | 0 | 1/2 |" in texto  # coste (830 + 83) * 0,24 / 1e6; JSON a la primera
    assert "| documental | DOCUMENTAL | 2 | 2000 |" in texto and "| fija | DATOS | 1 | 2000 |" in texto
    assert "| llm (sintesis_documental) | 3 | 300 |" in texto and "| validar | 3 | 10 |" in texto
    assert "| Válido tras reparación | 1 |" in texto and "| frase_fija:DATOS | 1 |" in texto


def test_turno_sin_tokens_queda_incompleto_y_sin_coste(tmp_path):
    spans = _documental("a", 1000, 0, 0)
    spans[[s["span_id"] for s in spans].index("b1")] = _llm("a", "b1", "b", 200, inicio_ms=100)  # sin tokens
    ruta = _escribir(tmp_path / "t.jsonl", spans + _documental("b", 3000, 0, 10_000))

    lista, _ = turnos(cargar([ruta]))
    incompleto, completo = lista
    assert incompleto.tokens_entrada is None and incompleto.coste_usd is None
    assert completo.tokens_entrada == 310 and completo.coste_usd is not None
    texto = informe(lista)
    assert "| 310 / 31 | 0,0001 | 1 |" in texto  # solo cuenta el turno completo; un incompleto
    assert "| Turnos con tokens incompletos | 1 |" in texto


def test_html_incrusta_los_turnos_sin_romper_la_pagina(tmp_path):
    spans = _documental("a", 1000, 0, 0) + _documental("b", 3000, 1, 10_000)
    spans[0]["attributes"]["input.value"] = "¿</script><b>NO2</b>?"  # el texto no puede cerrar el script
    lista, _ = turnos(cargar([_escribir(tmp_path / "t.jsonl", spans)]))

    html = informe_html(lista)
    incrustado = html.split('<script id="datos" type="application/json">')[1].split("</script>")[0]
    datos_html = json.loads(incrustado)
    assert [t["duracion_ms"] for t in datos_html["turnos"]] == [1000, 3000]
    assert datos_html["turnos"][0]["pregunta"] == "¿</script><b>NO2</b>?"
    assert [s["grupo"] for s in datos_html["turnos"][1]["spans"]].count("llm (sintesis_documental)") == 2
    assert sum(s.get("coste_usd") or 0 for s in datos_html["turnos"][0]["spans"]) == lista[0].coste_usd
