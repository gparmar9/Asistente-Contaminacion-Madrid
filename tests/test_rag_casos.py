"""Validez del juego de casos del RAG (tests/evals/rag_casos.jsonl) frente al corpus.

No trocea ni carga el modelo: solo lee el frontmatter y los encabezados de los
documentos revisados. Si el corpus cambia y una etiqueta deja de existir, falla aquí.
"""
import pytest

from rag.corpus import RUTA_CORPUS, cargar_corpus, slug
from rag.evaluar import RUTA_CASOS, TIPOS, cargar_casos

MINIMOS = {"documental": 15, "ajena": 10, "adversaria": 5}


@pytest.fixture(scope="module")
def casos():
    return cargar_casos(RUTA_CASOS)


@pytest.fixture(scope="module")
def documentos_revisados():
    return [d for d in cargar_corpus(RUTA_CORPUS) if d.revisado]


@pytest.fixture(scope="module")
def secciones_revisadas(documentos_revisados):
    return {f"{d.nombre}:{slug(enc)}" for d in documentos_revisados for enc, _ in d.secciones}


def test_ids_unicos(casos):
    ids = [c.id for c in casos]
    repetidos = sorted({i for i in ids if ids.count(i) > 1})
    assert not repetidos, f"ids repetidos: {repetidos}"


def test_tipos_validos(casos):
    malos = [(c.id, c.tipo) for c in casos if c.tipo not in TIPOS]
    assert not malos, f"tipos fuera de {TIPOS}: {malos}"


def test_preguntas_no_vacias(casos):
    vacias = [c.id for c in casos if not c.pregunta.strip()]
    assert not vacias, f"preguntas vacías: {vacias}"


def test_esperados_vacio_si_y_solo_si_no_es_documental(casos):
    doc_sin = [c.id for c in casos if c.tipo == "documental" and not c.esperados]
    otros_con = [c.id for c in casos if c.tipo != "documental" and c.esperados]
    assert not doc_sin, f"documentales sin secciones esperadas: {doc_sin}"
    assert not otros_con, f"ajenas/adversarias con secciones esperadas: {otros_con}"


def test_etiquetas_existen_en_documentos_revisados(casos, secciones_revisadas):
    fallos = [
        f"{c.id}: '{etiqueta}'"
        for c in casos
        for etiqueta in c.esperados
        if etiqueta not in secciones_revisadas
    ]
    assert not fallos, "etiquetas que no son documento:seccion de un documento revisado:\n  " + "\n  ".join(fallos)


def test_etiquetas_sin_repetir_dentro_del_caso(casos):
    repetidas = [c.id for c in casos if len(set(c.esperados)) != len(c.esperados)]
    assert not repetidas, f"casos con una sección esperada repetida: {repetidas}"


@pytest.mark.parametrize("tipo", sorted(MINIMOS))
def test_recuento_minimo_por_tipo(casos, tipo):
    n = sum(1 for c in casos if c.tipo == tipo)
    assert n >= MINIMOS[tipo], f"{tipo}: {n} casos, mínimo {MINIMOS[tipo]}"


def test_una_documental_por_documento_revisado(casos, documentos_revisados):
    cubiertos = {etiqueta.split(":")[0] for c in casos if c.tipo == "documental" for etiqueta in c.esperados}
    sin_caso = sorted(d.nombre for d in documentos_revisados if d.nombre not in cubiertos)
    assert not sin_caso, f"documentos revisados sin ninguna pregunta documental: {sin_caso}"
