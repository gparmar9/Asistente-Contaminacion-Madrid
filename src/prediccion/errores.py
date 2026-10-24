"""Errores propios del servicio de predicción; `prediccion.api` los traduce a códigos HTTP."""


class ErrorPrediccion(Exception):
    """Base de los errores del servicio."""


class DatosNoDisponibles(ErrorPrediccion):
    """La vista `mediciones_bloques` no responde, está vacía o no tiene ningún día completo."""


class ModelosNoDisponibles(ErrorPrediccion):
    """Falta un bundle, no se puede cargar o su horizonte no es el esperado."""
