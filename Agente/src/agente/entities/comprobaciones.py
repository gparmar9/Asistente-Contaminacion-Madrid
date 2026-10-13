"""Comprobaciones posteriores del turno: lo que se comprueba y lo que se encuentra. Solo datos."""
from dataclasses import dataclass


@dataclass(frozen=True)
class TextoComprobado:
    """Un texto escrito por el modelo y la base que respalda sus cifras (pregunta + evidencias o
    resultados de las herramientas del turno; nunca el historial)."""
    texto: str
    base: str


@dataclass(frozen=True)
class MaterialTurno:
    """Lo que escribió el modelo en el turno. `ruta`: libre | documental."""
    ruta: str
    textos: tuple[TextoComprobado, ...]


@dataclass(frozen=True)
class Hallazgo:
    """Una regla que saltó. `detalle`: las cifras sin respaldo, el n-grama o los términos internos."""
    regla: str
    detalle: str
    bloquea: bool = False
