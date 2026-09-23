"""Excepciones tipadas del paquete `rag`.

Se definen aparte para que `corpus.py` (lógica pura) pueda usarlas sin importar
torch ni ChromaDB, y para que la API pueda traducirlas a códigos HTTP claros.
"""


class ErrorRag(Exception):
    """Base de todos los errores del paquete."""


class CorpusInvalido(ErrorRag):
    """Un documento del corpus no cumple el formato (frontmatter, secciones, longitud)."""


class IndiceNoPreparado(ErrorRag):
    """No existe la colección Chroma. Hay que ejecutar `python -m rag.indexar`."""


class ModeloNoCoincide(ErrorRag):
    """El índice se construyó con un modelo de embeddings distinto del configurado."""


class ConsultaInvalida(ErrorRag):
    """La consulta o sus parámetros (k, tema, longitud) no son válidos."""



class EvidenciaDesconocida(ErrorRag):
    """Un chunk_id enviado a validar no existe en el corpus (documento o sección desconocidos)."""
