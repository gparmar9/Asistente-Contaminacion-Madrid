"""Pruebas de rag.evaluar con resultados fabricados a mano: sin índice ni modelo."""
import json

import pytest

from rag import evaluar as mod
from rag.buscar import K_POR_DEFECTO
from rag.evaluar import (
    Caso,
    cargar_casos,
    evaluar_caso,
    informe_markdown,
    punto_medio,
    resumir,
    seccion_de,
)

UMBRAL = 0.22
ESPERADA = "ozono_salud:efectos-en-la-salud"
DOC = Caso("doc-01", "documental", "¿Qué síntomas empeora el ozono?", (ESPERADA,))
AJENA = Caso("aje-01", "ajena", "¿Cuál es la capital de Francia?", ())
ADV = Caso("adv-01", "adversaria", "¿Habrá pico de ozono el finde?", ())


def _r(chunk_id: str, distancia: float) -> dict:
    return {"chunk_id": chunk_id, "distancia": distancia}


def _relleno(n: int, desde: float = 0.30) -> list[dict]:
    """n fragmentos irrelevantes con distancias crecientes a partir de `desde`."""
    return [_r(f"otro_doc:seccion-{i}:0", round(desde + i * 0.01, 3)) for i in range(n)]


# --------------------------------------------------------------------------- constantes y utilidades

def test_k_metrica_es_lo_que_recibe_el_llm():
    assert mod.K_METRICA == K_POR_DEFECTO


def test_seccion_de_quita_el_ordinal():
    assert seccion_de("ozono_salud:efectos-en-la-salud:0") == ESPERADA
    assert seccion_de("estaciones_y_zonas:catalogo-de-estaciones:12") == "estaciones_y_zonas:catalogo-de-estaciones"


@pytest.mark.parametrize("malo", ["ozono_salud:efectos", "a:b:c", "a:b:0:1", ""])
def test_seccion_de_rechaza_formatos_raros(malo):
    with pytest.raises(ValueError):
        seccion_de(malo)


def test_cargar_casos_lee_jsonl_y_salta_lineas_vacias(tmp_path):
    ruta = tmp_path / "casos.jsonl"
    ruta.write_text(
        json.dumps({"id": "doc-01", "tipo": "documental", "pregunta": "¿?", "esperados": [ESPERADA]})
        + "\n\n"
        + json.dumps({"id": "aje-01", "tipo": "ajena", "pregunta": "¿?", "esperados": []})
        + "\n",
        encoding="utf-8",
    )
    casos = cargar_casos(ruta)
    assert [c.id for c in casos] == ["doc-01", "aje-01"]
    assert casos[0].esperados == (ESPERADA,)
    assert casos[1].esperados == ()


def test_cargar_casos_rechaza_claves_que_faltan_y_json_invalido(tmp_path):
    ruta = tmp_path / "casos.jsonl"
    ruta.write_text('{"id": "doc-01", "tipo": "documental"}\n', encoding="utf-8")
    with pytest.raises(ValueError, match="casos.jsonl:1"):
        cargar_casos(ruta)
    ruta.write_text("{esto no es json}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="JSON inválido"):
        cargar_casos(ruta)


# --------------------------------------------------------------------------- evaluar_caso

def test_esperada_en_posicion_1():
    fila = evaluar_caso(DOC, [_r(ESPERADA + ":0", 0.10)] + _relleno(9), UMBRAL)
    assert fila.posicion == 1
    assert fila.distancia_esperada == 0.10
    assert fila.mejor_distancia == 0.10
    assert fila.hit and fila.hit_umbral
    assert fila.rr == 1.0


def test_esperada_en_posicion_5_no_es_hit_pero_puntua_en_el_mrr():
    resultados = _relleno(4, desde=0.15) + [_r(ESPERADA + ":0", 0.19)] + _relleno(5, desde=0.30)
    fila = evaluar_caso(DOC, resultados, UMBRAL)
    assert fila.posicion == 5
    assert not fila.hit and not fila.hit_umbral
    assert fila.rr == pytest.approx(0.2)


def test_esperada_ausente():
    fila = evaluar_caso(DOC, _relleno(10, desde=0.15), UMBRAL)
    assert fila.posicion is None
    assert fila.distancia_esperada is None
    assert fila.rr == 0.0
    assert not fila.hit and not fila.hit_umbral
    assert fila.mejor_distancia == 0.15


def test_umbral_es_inclusivo():
    resultados = [_r("otro:cosa:0", 0.20), _r(ESPERADA + ":0", UMBRAL)] + _relleno(8)
    fila = evaluar_caso(DOC, resultados, UMBRAL)
    assert fila.posicion == 2
    assert fila.hit and fila.hit_umbral


def test_hit_sin_umbral_con_irrelevante_por_debajo_y_esperada_por_encima():
    # Lo que la abstención por mejor distancia ocultaba: el LLM recibiría evidencias,
    # pero ninguna de las que responden la pregunta.
    resultados = [_r("otro:cosa:0", 0.20), _r(ESPERADA + ":0", 0.25)] + _relleno(8)
    fila = evaluar_caso(DOC, resultados, UMBRAL)
    assert fila.hit
    assert not fila.hit_umbral
    assert not fila.rechazada


def test_cualquier_fragmento_de_la_seccion_cuenta():
    resultados = [_r(ESPERADA + ":1", 0.12), _r("otro:cosa:0", 0.15), _r(ESPERADA + ":0", 0.16)] + _relleno(7)
    fila = evaluar_caso(DOC, resultados, UMBRAL)
    assert fila.posicion == 1
    assert fila.distancia_esperada == 0.12


def test_varias_secciones_esperadas_cuenta_la_primera_que_aparece():
    caso = Caso("doc-02", "documental", "¿?", ("particulas_salud:que-son", "particulas_salud:efectos-en-la-salud"))
    resultados = [_r("otro:cosa:0", 0.10), _r("particulas_salud:efectos-en-la-salud:0", 0.14),
                  _r("particulas_salud:que-son:0", 0.18)] + _relleno(7)
    fila = evaluar_caso(caso, resultados, UMBRAL)
    assert fila.posicion == 2
    assert fila.distancia_esperada == 0.14


def test_resultados_desordenados_se_ordenan_por_distancia():
    resultados = [_r("otro:cosa:0", 0.30), _r(ESPERADA + ":0", 0.10)]
    fila = evaluar_caso(DOC, resultados, UMBRAL)
    assert fila.posicion == 1
    assert fila.mejor_distancia == 0.10


def test_k_distinto_cambia_el_hit():
    resultados = _relleno(2, desde=0.15) + [_r(ESPERADA + ":0", 0.17)]
    assert evaluar_caso(DOC, resultados, UMBRAL, k=3).hit
    assert not evaluar_caso(DOC, resultados, UMBRAL, k=2).hit


def test_ajena_por_debajo_del_umbral_no_se_rechaza():
    fila = evaluar_caso(AJENA, _relleno(10, desde=0.21), UMBRAL)
    assert not fila.rechazada
    assert fila.posicion is None and not fila.hit and fila.rr == 0.0


def test_ajena_por_encima_del_umbral_se_rechaza():
    assert evaluar_caso(AJENA, _relleno(10, desde=0.23), UMBRAL).rechazada


def test_adversaria_con_evidencias():
    assert not evaluar_caso(ADV, _relleno(10, desde=0.18), UMBRAL).rechazada


def test_sin_resultados():
    fila = evaluar_caso(DOC, [], UMBRAL)
    assert fila.mejor_distancia is None
    assert not fila.hit and fila.rechazada and fila.rr == 0.0


# --------------------------------------------------------------------------- resumir

def _filas_de_muestra():
    doc1 = evaluar_caso(DOC, [_r(ESPERADA + ":0", 0.10)] + _relleno(9), UMBRAL)
    doc2 = evaluar_caso(
        Caso("doc-02", "documental", "¿?", (ESPERADA,)),
        _relleno(4, desde=0.15) + [_r(ESPERADA + ":0", 0.19)] + _relleno(5), UMBRAL,
    )
    aje1 = evaluar_caso(AJENA, _relleno(10, desde=0.25), UMBRAL)
    aje2 = evaluar_caso(Caso("aje-02", "ajena", "¿?", ()), _relleno(10, desde=0.21), UMBRAL)
    adv1 = evaluar_caso(ADV, _relleno(10, desde=0.18), UMBRAL)
    return [doc1, doc2, aje1, aje2, adv1]


def test_resumir_calcula_las_cuatro_medidas_y_la_informativa():
    r = resumir(_filas_de_muestra())
    assert str(r.hit) == "1/2"
    assert str(r.hit_umbral) == "1/2"
    assert r.mrr == pytest.approx((1.0 + 0.2) / 2)
    assert str(r.ajenas_rechazadas) == "1/2"
    assert str(r.adversarias_con_evidencias) == "1/1"


def test_resumir_sin_casos_no_divide_por_cero():
    r = resumir([])
    assert r.mrr == 0.0
    assert str(r.hit) == "0/0"


# --------------------------------------------------------------------------- punto_medio

def _fila_con_mejor(id_: str, tipo: str, mejor: float):
    caso = Caso(id_, tipo, "¿?", (ESPERADA,) if tipo == "documental" else ())
    return evaluar_caso(caso, [_r(ESPERADA + ":0", mejor)] + _relleno(9), UMBRAL)


def test_punto_medio_con_hueco_positivo():
    filas = [
        _fila_con_mejor("doc-01", "documental", 0.15),
        _fila_con_mejor("doc-02", "documental", 0.20),
        _fila_con_mejor("aje-01", "ajena", 0.24),
        _fila_con_mejor("aje-02", "ajena", 0.30),
        _fila_con_mejor("adv-01", "adversaria", 0.10),  # las adversarias no calibran
    ]
    c = punto_medio(filas)
    assert (c.peor_documental, c.id_peor_documental) == (0.20, "doc-02")
    assert (c.mejor_ajena, c.id_mejor_ajena) == (0.24, "aje-01")
    assert c.umbral_propuesto == pytest.approx(0.22)
    assert c.hueco == pytest.approx(0.04)
    assert c.solapados == ()


def test_punto_medio_con_solape():
    filas = [
        _fila_con_mejor("doc-01", "documental", 0.15),
        _fila_con_mejor("doc-02", "documental", 0.25),
        _fila_con_mejor("aje-01", "ajena", 0.23),
        _fila_con_mejor("aje-02", "ajena", 0.30),
    ]
    c = punto_medio(filas)
    assert c.hueco == pytest.approx(-0.02)
    assert c.umbral_propuesto == pytest.approx(0.24)
    assert c.solapados == ("doc-02", "aje-01")


def test_punto_medio_exige_documentales_y_ajenas():
    with pytest.raises(ValueError):
        punto_medio([_fila_con_mejor("doc-01", "documental", 0.15)])


# --------------------------------------------------------------------------- informe

def test_informe_contiene_metadatos_fracciones_y_casos():
    filas = _filas_de_muestra()
    meta = {"modelo_embeddings": "intfloat/multilingual-e5-base", "commit": "abc1234",
            "fecha_indexado": "2026-10-01", "n_fragmentos": 50}
    umbrales = [UMBRAL, 0.25]
    resumenes = [resumir(filas), resumir(filas)]
    texto = informe_markdown(meta, umbrales, filas, resumenes, punto_medio(filas))
    assert "intfloat/multilingual-e5-base" in texto
    assert "abc1234" in texto and "2026-10-01" in texto and "n_fragmentos=50" in texto
    assert "| hit@4 (documentales) | 1/2 | 1/2 |" in texto
    assert "| MRR@10 (documentales) | 0.600 | 0.600 |" in texto
    assert "| Ajenas rechazadas | 1/2 | 1/2 |" in texto
    assert "Umbral propuesto:" in texto
    assert "| doc-01 | documental | 0.100 | 1 | 0.100 | sí | sí | — |" in texto
    assert "| aje-01 | ajena | 0.250 | — | — | — | — | sí |" in texto
    assert "|---" not in texto  # separadores mínimos en las tablas


def test_informe_exige_un_resumen_por_umbral():
    with pytest.raises(ValueError):
        informe_markdown({}, [UMBRAL, 0.25], [], [resumir([])])
