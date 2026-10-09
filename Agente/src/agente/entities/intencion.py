"""Salida tipada del clasificador de intención."""
from dataclasses import dataclass
from enum import Enum


class Intencion(str, Enum):
    DOCUMENTAL = "DOCUMENTAL"              # salud, normativa, protocolo, el proyecto: busca en el RAG
    DATOS = "DATOS"                        # mediciones actuales o históricas: consulta SQL
    PREDICCION = "PREDICCION"              # qué pasará: no hay predicción
    CHARLA = "CHARLA"                      # saludos, qué sabe hacer el asistente
    FUERA_DE_ALCANCE = "FUERA_DE_ALCANCE"  # nada que ver con la calidad del aire
    DESCONOCIDA = "DESCONOCIDA"            # valor seguro si el clasificador falla; no lo devuelve el modelo


class Tema(str, Enum):
    """Filtro de la búsqueda documental (mismos valores que `tema` en rag.api, más `ninguno`)."""

    SALUD = "salud"
    NORMATIVA = "normativa"
    PROYECTO = "proyecto"
    NINGUNO = "ninguno"


@dataclass(frozen=True)
class Clasificacion:
    """Una o dos intenciones: una pregunta mixta (datos y salud) lleva las dos."""
    intenciones: frozenset[Intencion]
    tema: Tema = Tema.NINGUNO

    @property
    def etiqueta(self) -> str:
        """Las intenciones en orden alfabético, separadas por comas: «DATOS, DOCUMENTAL»."""
        return ", ".join(sorted(i.value for i in self.intenciones))


DESCONOCIDA = Clasificacion(frozenset({Intencion.DESCONOCIDA}))
