from llm_orchestrator.integrations import rag
from llm_orchestrator.tools import rag_tool


def _fragmento(titulo="Guía OMS 2021", distancia=0.4, texto="Los límites de NO2..."):
    return {"texto": texto,
            "metadatos": {"titulo": titulo, "seccion": "Límites", "fuente": "OMS (2021)"},
            "distancia": distancia}


def test_devuelve_fragmentos_relevantes(monkeypatch):
    monkeypatch.setattr(rag, "buscar_en_corpus",
                        lambda consulta, k=4, tema=None: [_fragmento()])
    r = rag_tool.search_documents(consulta="límites de NO2")
    assert len(r["resultados"]) == 1
    assert r["resultados"][0]["titulo"] == "Guía OMS 2021"
    assert r["resultados"][0]["fuente"] == "OMS (2021)"


def test_filtra_por_umbral_de_distancia(monkeypatch):
    monkeypatch.setattr(rag, "buscar_en_corpus",
                        lambda consulta, k=4, tema=None: [
                            _fragmento(distancia=0.4),
                            _fragmento(titulo="Irrelevante", distancia=0.95),
                        ])
    r = rag_tool.search_documents(consulta="ozono")
    assert [x["titulo"] for x in r["resultados"]] == ["Guía OMS 2021"]


def test_sin_resultados_relevantes_devuelve_mensaje(monkeypatch):
    monkeypatch.setattr(rag, "buscar_en_corpus",
                        lambda consulta, k=4, tema=None: [_fragmento(distancia=0.95)])
    r = rag_tool.search_documents(consulta="recetas de cocina")
    assert r["resultados"] == []
    assert "No se encontraron" in r["mensaje"]


def test_trunca_fragmentos_largos(monkeypatch):
    monkeypatch.setattr(rag, "buscar_en_corpus",
                        lambda consulta, k=4, tema=None: [_fragmento(texto="x" * 5000)])
    r = rag_tool.search_documents(consulta="ozono")
    assert len(r["resultados"][0]["texto"]) == rag_tool.MAX_CARACTERES_FRAGMENTO


def test_consulta_vacia_es_error():
    assert "consulta" in rag_tool.search_documents(consulta="  ")["error"]


def test_k_fuera_de_rango_es_error():
    assert "'k'" in rag_tool.search_documents(consulta="ozono", k=50)["error"]
    assert "'k'" in rag_tool.search_documents(consulta="ozono", k="muchos")["error"]


def test_fallo_del_indice_vuelve_como_error(monkeypatch):
    def _explota(consulta, k=4, tema=None):
        raise RuntimeError("índice no encontrado")

    monkeypatch.setattr(rag, "buscar_en_corpus", _explota)
    r = rag_tool.search_documents(consulta="ozono")
    assert "índice no encontrado" in r["error"]
