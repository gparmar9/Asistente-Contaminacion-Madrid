"""`periodos.resolver`: una fila por expresión de la tabla del plan, con «hoy» y la última fecha fijos."""
from datetime import date

import pytest

from agente.business.periodos import resolver, resolver_futuro

HOY, PRIMERA, ULTIMA = date(2026, 5, 1), date(2024, 4, 30), date(2026, 4, 30)


def _d(texto: str) -> date:
    return date.fromisoformat(texto)


@pytest.mark.parametrize("pregunta, rangos, bloque", [
    ("¿Dónde se respira mejor ahora mismo?", [("2026-04-30", "2026-04-30")], None),
    ("¿Es seguro salir esta tarde?", [("2026-04-30", "2026-04-30")], "tarde"),
    ("¿Cuál fue el NO2 ayer?", [("2026-04-30", "2026-04-30")], None),
    ("¿Cómo ha estado el NO2 esta semana?", [("2026-04-24", "2026-04-30")], None),
    ("¿Hubo anomalías la semana pasada?", [("2026-04-20", "2026-04-26")], None),
    ("¿Cómo va este mes?", [("2026-05-01", "2026-05-31")], None),
    ("¿Qué distrito fue mejor el mes pasado?", [("2026-04-01", "2026-04-30")], None),
    ("¿Cómo va este año?", [("2026-01-01", "2026-04-30")], None),
    ("¿Cuántos días del año pasado?", [("2025-01-01", "2025-12-31")], None),
    ("en las dos últimas semanas", [("2026-04-17", "2026-04-30")], None),
    ("¿Y en los últimos 3 meses?", [("2026-02-01", "2026-04-30")], None),
    ("la media anual por distrito", [("2025-05-01", "2026-04-30")], None),
    ("¿Cómo será este verano?", [("2026-06-01", "2026-08-31")], None),
    ("¿Y el verano pasado?", [("2025-06-01", "2025-08-31")], None),
    ("¿Ha empeorado el ozono durante los últimos dos veranos?",
     [("2024-06-01", "2024-08-31"), ("2025-06-01", "2025-08-31")], None),
    ("En julio, ¿dónde es menos problemático el ozono?",
     [("2024-07-01", "2024-07-31"), ("2025-07-01", "2025-07-31")], None),
    ("¿Y en abril de 2025?", [("2025-04-01", "2025-04-30")], None),
    ("¿Cuántos días de 2019 hubo episodio?", [("2019-01-01", "2019-12-31")], None),
    ("¿Qué pasó el 15 de marzo?", [("2026-03-15", "2026-03-15")], None),
    ("¿Y hace 3 días?", [("2026-04-28", "2026-04-28")], None),
    ("¿Cómo estaba hace tres semanas?", [("2026-04-06", "2026-04-12")], None),
    ("¿Y hace dos meses?", [("2026-03-01", "2026-03-31")], None),
    ("¿Cómo estaba hace un año?", [("2025-01-01", "2025-12-31")], None),
    ("¿Ha mejorado desde hace 3 semanas?", [("2026-04-10", "2026-04-30")], None),
    ("¿Qué zona me recomiendas para vivir?", [], None),
])
def test_expresion_se_resuelve_con_las_definiciones_del_proyecto(pregunta, rangos, bloque):
    periodos = resolver(pregunta, HOY, PRIMERA, ULTIMA)

    if not rangos:  # sin expresión: el redactor usa todo el histórico y no se comprueban fechas
        assert periodos == []
        return
    assert len(periodos) == 1
    assert periodos[0].rangos == tuple((_d(i), _d(f)) for i, f in rangos) and periodos[0].bloque == bloque


# --------------------------------------------------------------------------- futuro (PREDICCION)

@pytest.mark.parametrize("pregunta, dia, bloque", [
    ("¿Cómo estará el aire mañana?", "2026-05-02", None),
    ("¿Y pasado mañana?", "2026-05-03", None),
    ("¿Habrá pico de ozono esta tarde?", "2026-05-01", "tarde"),
    ("¿Cómo pinta esta noche?", "2026-05-01", "noche"),
    ("¿Y mañana por la mañana?", "2026-05-02", "manana"),         # día siguiente + bloque, no doble día
    ("¿Cómo estará mañana por la noche?", "2026-05-02", "noche"),
    ("¿Cómo va a estar hoy?", "2026-05-01", None),
    ("¿Qué tal en las próximas 24 h?", "2026-05-02", None),
    ("¿Y en las próximas 48 h?", "2026-05-03", None),
    ("¿Cómo estará el aire el 3 de mayo?", None, None),            # «de mayo» no es regla de futuro
    ("¿Y el 2026-05-02?", "2026-05-02", None),                     # fecha ISO explícita
    ("¿Cómo estará este fin de semana?", "2026-05-02", None),      # hoy jueves → sábado
    ("¿Y la semana que viene?", "2026-05-04", None),               # lunes natural siguiente
])
def test_resolver_futuro(pregunta, dia, bloque):
    periodos = resolver_futuro(pregunta, HOY)
    if dia is None:
        assert periodos == []
        return
    assert periodos[-1].rangos == ((_d(dia), _d(dia)),)
    assert periodos[-1].bloque == bloque


def test_resolver_futuro_sin_expresion_vacio():
    """Sin expresión de futuro, vacío: la herramienta aplica el defecto (hoy)."""
    assert resolver_futuro("¿cómo estará el aire en Retiro?", HOY) == []


def test_esta_manana_es_bloque_de_hoy_no_dia_siguiente():
    """«esta mañana» → hoy + bloque mañana; no debe contar como «mañana» (día siguiente)."""
    periodos = resolver_futuro("¿cómo estará esta mañana?", HOY)
    assert periodos[-1].rangos == ((HOY, HOY),) and periodos[-1].bloque == "manana"


def test_resolver_futuro_varias_expresiones_conserva_ambas():
    """«hoy y mañana» → dos periodos; el llamador elige la fecha máxima."""
    periodos = resolver_futuro("¿cómo está hoy y cómo estará mañana?", HOY)
    assert [p.rangos for p in periodos] == [((HOY, HOY),), ((_d("2026-05-02"), _d("2026-05-02")),)]
    assert max(p.rangos[0][0] for p in periodos) == _d("2026-05-02")
