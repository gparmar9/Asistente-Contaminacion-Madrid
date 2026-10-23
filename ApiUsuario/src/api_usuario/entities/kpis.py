"""Contrato de GET /kpis, validado y reenviado desde el agente.

Mantener sincronizado con Agente/src/agente/entities/kpis.py.
Los percentiles usan el rango más cercano. null indica falta de datos."""
from datetime import date, datetime
from enum import IntEnum

from pydantic import BaseModel, Field


class Dias(IntEnum):
    """Periodos que se pueden pedir. Phoenix retiene 30 días."""

    UNO = 1
    SIETE = 7
    TREINTA = 30


class Distribucion(BaseModel):
    n: int
    p50: float | None = None
    p90: float | None = None
    p95: float | None = None


class Periodo(BaseModel):
    dias: int
    desde: datetime
    hasta: datetime
    generado: datetime


class Conteo(BaseModel):
    categoria: str
    n: int


class Resumen(BaseModel):
    preguntas: int = Field(description="Turnos del periodo")
    dias_con_uso: int
    resueltas: int = Field(description="Respondidas o con frase fija del código (respuesta correcta por diseño)")
    latencia_ms: Distribucion
    primer_token_ms: Distribucion = Field(description="Solo turnos en streaming con el texto en tokens")
    coste_usd: Distribucion = Field(description="Solo turnos con tokens completos y modelo con precio")


class Dia(BaseModel):
    dia: date = Field(description="Día en hora de Madrid")
    preguntas: int
    resueltas: int


class PorRuta(BaseModel):
    ruta: str
    ms: Distribucion


class Coste(BaseModel):
    turnos_con_coste: int
    total_usd: float
    por_resuelta_usd: float | None = Field(description="Coste total / turnos resueltos con coste")
    modelos: list[str]
    valores_usd: list[float] = Field(description="Coste de cada turno, para el histograma")


class Calidad(BaseModel):
    sintesis_documentales: int
    validas_a_la_primera: int
    reparaciones_medias: float | None
    busquedas_forzadas: int
    consultas_forzadas: int
    sintesis_forzadas: int = Field(description="Cerradas por el código al agotar las vueltas")


class Regla(BaseModel):
    regla: str
    turnos: int = Field(description="Turnos con algún hallazgo de la regla")
    bloqueados: int


class Comprobaciones(BaseModel):
    turnos_bloqueados: int
    reglas: list[Regla]


class FalloHerramienta(BaseModel):
    herramienta: str
    causa: str = Field(description="`agente.estado` del span, o `vetada`")
    fallos: int


class Herramientas(BaseModel):
    llamadas: int
    fallos: list[FalloHerramienta]
    preguntas_de_datos: int
    datos_no_disponibles: int = Field(description="Preguntas de datos sin base (decisión `datos_no_disponibles`)")


class Sesiones(BaseModel):
    sesiones: int
    turnos_mediana: float
    turnos_maximo: int
    de_un_turno: int
    duracion_s: Distribucion = Field(description="Sesiones con 2 o más turnos")
    turnos_con_previo: int = Field(description="Turnos que no abren su sesión")
    tras_rechazo: int = Field(description="Turnos tras un rechazo (reformulación)")
    resueltos_tras_rechazo: int = Field(description="De ellos, resueltos: el rechazo pudo ser evitable")


class Votos(BaseModel):
    grupo: str
    positivos: int
    votos: int


class Satisfaccion(BaseModel):
    votos: int
    positivos: int
    por_ruta: list[Votos]
    por_tema: list[Votos]
    sesiones_elegibles: int = Field(description="Sesiones con 2 o más respuestas completas: pueden ver el aviso")
    sesiones_con_voto: int


class Kpis(BaseModel):
    periodo: Periodo
    precios: str = Field(description="Fecha y origen de la tabla de precios")
    huerfanas: int = Field(description="Trazas sin span `turno` (turno cortado o fuera del intervalo)")
    resumen: Resumen
    por_dia: list[Dia]
    intenciones: list[Conteo]
    rutas: list[Conteo]
    temas: list[Conteo]
    resultados: list[Conteo] = Field(description="respondida, frase_fija, sin_evidencia, servicio_no_disponible, "
                                                 "bloqueada, error")
    coste: Coste | None
    latencia_por_ruta: list[PorRuta]
    primer_token_por_ruta: list[PorRuta]
    calidad: Calidad
    comprobaciones: Comprobaciones
    herramientas: Herramientas
    sesiones: Sesiones | None = Field(description="null si los spans no llevan `session.id`")
    satisfaccion: Satisfaccion | None = Field(description="null si no hay votos en el periodo")
