"""Integración con PostgreSQL: lo que SQLite no prueba (dialecto, vista real y permisos del rol).

Corre contra el Postgres local ya cargado, conectado con el rol `agente_lectura`. No escribe
nada. Fuera de la suite rápida: solo con RUN_PG_TESTS=1 y DATABASE_URL.

    RUN_PG_TESTS=1 DATABASE_URL=postgresql://agente_lectura:CLAVE@localhost:5432/postgres \\
        .venv/bin/python -m pytest tests/test_integracion_pg.py -v
"""
import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from agente.datos.mediciones import ErrorBaseDatos, Mediciones, crear_motor

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_PG_TESTS") != "1" or not os.environ.get("DATABASE_URL"),
    reason="Integración: define RUN_PG_TESTS=1 y DATABASE_URL con el rol agente_lectura.",
)


@pytest.fixture(scope="module")
def mediciones():
    return Mediciones(crear_motor(os.environ["DATABASE_URL"], timeout_s=2))


def test_media_ponderada_coincide_con_la_calculada_a_mano(mediciones):
    dia = mediciones.ultima_fecha()
    estacion = mediciones.ejecutar(
        "SELECT MIN(estacion) FROM mediciones_bloques WHERE contaminante = 'NO2' AND fecha = :d",
        {"d": dia}).filas[0][0]
    filtro = "contaminante = 'NO2' AND fecha = :d AND estacion = :e AND n_horas > 0"
    params = {"d": dia, "e": estacion}

    bloques = mediciones.ejecutar(f"SELECT media, n_horas FROM mediciones_bloques WHERE {filtro}", params)
    a_mano = sum(m * n for m, n in bloques.filas) / sum(n for _, n in bloques.filas)
    en_sql = mediciones.ejecutar(
        f"SELECT SUM(media * n_horas) / SUM(n_horas) FROM mediciones_bloques WHERE {filtro}", params)

    assert en_sql.filas[0][0] == pytest.approx(a_mano)


def test_ventana_de_dos_anos_y_consulta_temporal(mediciones):
    dentro = mediciones.ejecutar(
        "SELECT MIN(fecha) >= MAX(fecha) - INTERVAL '2 years' FROM mediciones_bloques")
    assert dentro.filas[0][0] is True

    meses = mediciones.ejecutar(
        "SELECT date_trunc('month', fecha)::date AS mes, COUNT(*) FROM mediciones_bloques "
        "WHERE fecha > (SELECT MAX(fecha) FROM mediciones_bloques) - INTERVAL '3 months' "
        "GROUP BY 1 ORDER BY 1")
    assert 3 <= len(meses.filas) <= 4
    assert all(mes.endswith("-01") for mes, _ in meses.filas)


def _sqlstate(exc: BaseException) -> str:
    """Código de error de PostgreSQL (psycopg 3), independiente del idioma del servidor."""
    original = getattr(exc, "orig", None) or getattr(exc.__cause__, "orig", None)
    return getattr(original, "sqlstate", "")


def test_el_rol_solo_lee_la_vista_y_respeta_el_tiempo_limite(mediciones):
    with pytest.raises(ErrorBaseDatos) as tabla:
        mediciones.ejecutar("SELECT 1 FROM resumen_datos_ml LIMIT 1")
    assert _sqlstate(tabla.value) == "42501"          # insufficient_privilege
    with pytest.raises(ErrorBaseDatos) as lenta:
        mediciones.ejecutar("SELECT pg_sleep(3)")
    assert _sqlstate(lenta.value) == "57014"          # query_canceled (statement_timeout)

    # Sesión de lectura y escritura (como si el validador y el motor fallaran): manda el rol
    crudo = create_engine(os.environ["DATABASE_URL"],
                          connect_args={"options": "-c default_transaction_read_only=off"})
    with crudo.connect() as conn, pytest.raises(DBAPIError) as escritura:
        conn.execute(text("UPDATE resumen_datos_ml SET media = media WHERE false"))
    crudo.dispose()
    assert _sqlstate(escritura.value) == "42501"
