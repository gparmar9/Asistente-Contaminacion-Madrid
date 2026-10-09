"""`periodos.resolver`: una fila por expresión de la tabla del plan, con «hoy» y la última fecha fijos."""
from datetime import date

import pytest

from agente.business.periodos import resolver

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
