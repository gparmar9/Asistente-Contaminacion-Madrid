"""Contrato de rag.buscar: validación de entrada, índice ausente y modelo distinto.

Requiere chromadb (se salta si no está instalado). No carga el modelo de
embeddings: todas las comprobaciones probadas aquí ocurren antes de necesitarlo.
"""
import pytest

chromadb = pytest.importorskip("chromadb")

from rag import buscar as mod_buscar  # noqa: E402
from rag import embeddings  # noqa: E402
from rag.errores import ConsultaInvalida, IndiceNoPreparado, ModeloNoCoincide  # noqa: E402


@pytest.fixture
def sin_modelo(monkeypatch):
    """Si algo intenta cargar el modelo, la prueba falla: aquí no debe hacer falta."""
    def explota():
        raise AssertionError("no debería cargarse el modelo en esta prueba")
    monkeypatch.setattr(embeddings, "cargar_modelo", explota)


@pytest.mark.parametrize("consulta", ["", "   ", None])
def test_consulta_vacia(consulta, sin_modelo, tmp_path):
    with pytest.raises(ConsultaInvalida):
        mod_buscar.buscar(consulta, ruta_chroma=tmp_path)


@pytest.mark.parametrize("k", [0, -1, 21, 2.5, True, "4"])
def test_k_fuera_de_rango(k, sin_modelo, tmp_path):
    with pytest.raises(ConsultaInvalida):
        mod_buscar.buscar("pregunta", k=k, ruta_chroma=tmp_path)


def test_tema_desconocido(sin_modelo, tmp_path):
    with pytest.raises(ConsultaInvalida):
        mod_buscar.buscar("pregunta", tema="disclaimer", ruta_chroma=tmp_path)


def test_indice_ausente(sin_modelo, tmp_path):
    with pytest.raises(IndiceNoPreparado, match="rag.indexar"):
        mod_buscar.buscar("¿qué son los bloques del día?", ruta_chroma=tmp_path / "no_existe")


def test_directorio_sin_coleccion(sin_modelo, tmp_path):
    chromadb.PersistentClient(path=str(tmp_path))  # crea el directorio pero no la colección
    with pytest.raises(IndiceNoPreparado):
        mod_buscar.buscar("pregunta", ruta_chroma=tmp_path)


def test_modelo_no_coincide(sin_modelo, tmp_path):
    embeddings.recrear_coleccion({"modelo_embeddings": "otro/modelo"}, ruta=tmp_path)
    with pytest.raises(ModeloNoCoincide, match="otro/modelo"):
        mod_buscar.buscar("pregunta", ruta_chroma=tmp_path)
