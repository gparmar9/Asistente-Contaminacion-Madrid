"""Pruebas de rag.indexar.

- Sin modelo: si el cálculo de embeddings falla, el índice anterior queda intacto.
- Con modelo (RUN_RAG_TESTS=1): índice real sobre un corpus temporal y recuperación
  de un hecho situado al final de una sección más larga que el presupuesto.
"""
import os
import shutil
from pathlib import Path

import pytest

chromadb = pytest.importorskip("chromadb")

from rag import buscar as mod_buscar  # noqa: E402
from rag import embeddings, indexar  # noqa: E402
from rag.corpus import RUTA_CORPUS  # noqa: E402

CON_MODELO = pytest.mark.skipif(
    os.getenv("RUN_RAG_TESTS") != "1",
    reason="carga el modelo de embeddings; activar con RUN_RAG_TESTS=1",
)


def test_metadatos_chroma_ida_y_vuelta():
    m = {"titulo": "t", "contaminantes": ["NO", "NO2"], "n_tokens": 3}
    plano = indexar.metadatos_a_chroma(m)
    assert plano["contaminantes"] == "NO,NO2"
    assert indexar.metadatos_desde_chroma(plano) == m
    assert indexar.metadatos_desde_chroma({"contaminantes": ""})["contaminantes"] == []


def test_si_fallan_los_embeddings_el_indice_anterior_sigue_intacto(tmp_path, monkeypatch):
    # Índice "anterior" con un documento.
    col = embeddings.recrear_coleccion({"modelo_embeddings": embeddings.MODELO_EMBEDDINGS}, ruta=tmp_path)
    col.add(ids=["viejo:intro:0"], embeddings=[[0.0, 1.0]], documents=["viejo"], metadatas=[{"tema": "salud"}])

    # Sin modelo: tokens por palabras, presupuesto fijo y embeddings que fallan.
    monkeypatch.setattr(indexar, "contar_tokens", lambda t, consulta=False: len(t.split()))
    monkeypatch.setattr(indexar, "presupuesto_tokens", lambda: 10_000)

    def falla(_textos):
        raise RuntimeError("sin GPU, sin suerte")
    monkeypatch.setattr(indexar, "embed_pasajes", falla)

    with pytest.raises(RuntimeError):
        indexar.indexar(RUTA_CORPUS, ruta_chroma=tmp_path)

    col = embeddings.obtener_coleccion(tmp_path)
    assert col.count() == 1
    assert col.get(ids=["viejo:intro:0"])["documents"] == ["viejo"]


@CON_MODELO
def test_indexa_corpus_temporal_y_recupera_final_de_seccion_larga(tmp_path):
    corpus = tmp_path / "rag"
    corpus.mkdir()
    shutil.copy(RUTA_CORPUS / "ozono_salud.md", corpus / "ozono_salud.md")

    relleno = "\n\n".join(
        f"Párrafo {i}. El aire de la ciudad cambia con el viento y la temperatura. "
        f"Este texto solo sirve para alargar la sección más allá del presupuesto." for i in range(40)
    )
    (corpus / "largo.md").write_text(
        "---\ntitulo: Documento largo de prueba\ntema: proyecto\ncontaminantes: []\n"
        "revisado: true\nfecha_revision: 2026-09-23\n"
        "fuentes:\n  - titulo: Fuente de prueba\n    organismo: Proyecto\n---\n"
        "# Documento largo de prueba\n\n## Sección larga\n\n" + relleno +
        "\n\nLa contraseña secreta del laboratorio de pruebas es xilófono azul.\n",
        encoding="utf-8",
    )

    ruta_chroma = tmp_path / "chroma"
    resumen = indexar.indexar(corpus, ruta_chroma=ruta_chroma)
    assert resumen["n_documentos"] == 2
    assert resumen["max_tokens_fragmento"] <= resumen["presupuesto_tokens"]

    col = embeddings.obtener_coleccion(ruta_chroma)
    assert col.metadata["modelo_embeddings"] == embeddings.MODELO_EMBEDDINGS
    largos = [i for i in col.get()["ids"] if i.startswith("largo:seccion-larga:")]
    assert len(largos) > 1  # la sección larga se subdividió

    res = mod_buscar.buscar("¿cuál es la contraseña secreta del laboratorio?", k=3, ruta_chroma=ruta_chroma)
    assert any("xilófono azul" in r["texto"] for r in res)
    assert res[0]["chunk_id"].startswith("largo:seccion-larga:")
    assert isinstance(res[0]["metadatos"]["contaminantes"], list)
