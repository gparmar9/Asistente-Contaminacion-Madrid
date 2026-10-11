"""Periodos deterministas: Python calcula las fechas que pide la pregunta; el modelo no hace cuentas.

`resolver` busca expresiones de tiempo en la pregunta del usuario (sin tildes ni mayúsculas) y
devuelve sus rangos de días con las definiciones del proyecto. El redactor SQL recibe el periodo ya
calculado (`condicion_sql`) y el validador comprueba que el SQL lo respeta. Lo que no se reconoce
cae al calendario del prompt (`calendario`): el modelo elige una fila, no calcula.

Definiciones (2026-10-08 y 2026-10-09), con `ultima` = último día con datos:
- «ahora mismo», «hoy», «esta tarde»: `ultima` (el pipeline cierra el día a las 23:45), con su bloque;
- «ayer», «este mes», años y meses con año: literales respecto a `hoy`; sin filas → «sin datos»;
- «esta semana»: los 7 últimos días disponibles; «la semana pasada»: semana natural anterior a `hoy`;
- «los últimos N días/semanas/meses/años», «media anual»: hasta `ultima`;
- veranos: junio a agosto completos; un mes sin año: ese mes en cada año con datos;
- un día sin año: el más reciente que no sea posterior a `hoy`;
- «hace N días»: ese día, literal; «hace N semanas/meses/años»: la semana, el mes o el año natural
  de hace N (como «la semana pasada»); «desde hace N …»: desde ese punto hasta `ultima`.

Funciones puras, sin LLM ni base de datos.
"""
from __future__ import annotations

import calendar as _calendario
import re
import unicodedata
from datetime import date, timedelta
from typing import Callable, Sequence

from agente.entities.datos import Periodo

UN_DIA = timedelta(days=1)
MESES = {"enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6, "julio": 7,
         "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12}
NUMEROS = {"un": 1, "uno": 1, "una": 1, "dos": 2, "tres": 3, "cuatro": 4, "cinco": 5, "seis": 6,
           "siete": 7, "ocho": 8, "nueve": 9, "diez": 10, "once": 11, "doce": 12}
# Expresiones del calendario de los prompts: respaldo para lo que `resolver` no reconozca.
EXPRESIONES_CALENDARIO = ("ahora mismo", "esta tarde", "ayer", "esta semana", "la semana pasada",
                          "este mes", "el mes pasado", "este año", "el año pasado", "los últimos 12 meses",
                          "este verano", "el verano pasado", "los últimos dos veranos")

_MES = "(?P<m>" + "|".join(MESES) + ")"
_NUM = r"(?P<n>\d{1,2}|" + "|".join(NUMEROS) + ")"
_UNIDAD = r"(?P<u>dias?|semanas?|mes|meses|anos?|veranos?)"
_UNIDAD_HACE = r"(?P<u>dias?|semanas?|mes|meses|anos?)"

Rangos = tuple[tuple[date, date], ...]


def resolver(texto: str, hoy: date, primera: date, ultima: date) -> list[Periodo]:
    """Periodos de las expresiones de `texto`, en el orden en que aparecen. Vacía si no hay ninguna.

    Las reglas van de la más larga a la más corta y cada coincidencia se borra del texto, para que
    «los últimos dos veranos» no cuente además como «veranos» ni «30 de abril de 2026» como «2026»."""
    original = unicodedata.normalize("NFC", texto)
    normalizado = _normalizar(original)
    encontrados: list[tuple[int, Periodo]] = []
    for patron, regla in _REGLAS:
        for m in patron.finditer(normalizado):
            if rangos := regla(m, hoy, primera, ultima):
                expresion = original[m.start():m.end()].lower()
                encontrados.append((m.start(), Periodo(expresion, rangos, m.groupdict().get("b"))))
        normalizado = patron.sub(lambda m: " " * len(m.group(0)), normalizado)
    periodos: list[Periodo] = []
    for _, periodo in sorted(encontrados, key=lambda e: e[0]):
        if periodo not in periodos:
            periodos.append(periodo)
    return periodos


def condicion_sql(periodos: Sequence[Periodo]) -> str:
    """Para el redactor y los errores del validador: «la semana pasada» → fecha BETWEEN '…' AND '…'."""
    partes = []
    for p in periodos:
        rangos = [f"fecha = '{i}'" if i == f else f"fecha BETWEEN '{i}' AND '{f}'" for i, f in p.rangos]
        condicion = rangos[0] if len(rangos) == 1 else "(" + " OR ".join(rangos) + ")"
        if p.bloque:
            condicion += f" AND bloque = '{p.bloque}'"
        partes.append(f"«{p.expresion}» → {condicion}")
    return "; ".join(partes)


def texto(periodos: Sequence[Periodo]) -> str:
    """Para la conversación y la síntesis, sin SQL: «la semana pasada»: del 2026-04-20 al 2026-04-26."""
    partes = []
    for p in periodos:
        rangos = " y ".join(f"el {i}" if i == f else f"del {i} al {f}" for i, f in p.rangos)
        partes.append(f"«{p.expresion}»: {rangos}" + (f", bloque {p.bloque}" if p.bloque else ""))
    return "; ".join(partes)


def calendario(hoy: date, primera: date, ultima: date,
               formato: Callable[[Sequence[Periodo]], str] = texto) -> str:
    """Las expresiones habituales ya calculadas, una por línea."""
    return "\n".join(f"- {formato(resolver(e, hoy, primera, ultima))}" for e in EXPRESIONES_CALENDARIO)


# ---------------------------------------------------------------------- reglas

def _normalizar(texto: str) -> str:
    """Minúsculas, sin tildes y sin signos, carácter a carácter: las posiciones valen en el original."""
    salida = []
    for c in texto:
        base = "".join(x for x in unicodedata.normalize("NFD", c) if not unicodedata.combining(x)).lower()[:1]
        salida.append(base if base and (base.isalnum() or base in "/-") else " ")
    return "".join(salida)


def _mes(anio: int, mes: int) -> tuple[date, date]:
    return date(anio, mes, 1), date(anio, mes, _calendario.monthrange(anio, mes)[1])


def _restar_meses(dia: date, n: int) -> date:
    anio, mes = divmod(dia.year * 12 + dia.month - 1 - n, 12)
    return date(anio, mes + 1, min(dia.day, _calendario.monthrange(anio, mes + 1)[1]))


def _verano(anio: int) -> tuple[date, date]:
    return date(anio, 6, 1), date(anio, 8, 31)


def _ultimo_verano_completo(dia: date) -> int:
    return dia.year if dia >= date(dia.year, 8, 31) else dia.year - 1


def _numero(valor: str | None) -> int:
    if not valor:
        return 1
    return int(valor) if valor.isdigit() else NUMEROS[valor]


def _ultimos(m: re.Match, hoy: date, primera: date, ultima: date) -> Rangos:
    n, unidad = _numero(m.group("n")), m.group("u")
    if unidad.startswith("verano"):
        anio = _ultimo_verano_completo(ultima)
        return tuple(_verano(a) for a in range(anio - n + 1, anio + 1))
    if unidad.startswith("dia"):
        inicio = ultima - timedelta(days=n - 1)
    elif unidad.startswith("semana"):
        inicio = ultima - timedelta(days=7 * n - 1)
    else:
        inicio = _restar_meses(ultima + UN_DIA, n * (12 if unidad.startswith("ano") else 1))
    return ((inicio, ultima),)


def _hace(m: re.Match, hoy: date, primera: date, ultima: date) -> Rangos:
    n, unidad = _numero(m.group("n")), m.group("u")
    if unidad.startswith("dia"):
        dia = hoy - timedelta(days=n)
        return ((dia, dia),)
    if unidad.startswith("semana"):
        lunes = hoy - timedelta(days=hoy.weekday() + 7 * n)
        return ((lunes, lunes + timedelta(days=6)),)
    if unidad.startswith("mes"):
        dia = _restar_meses(hoy, n)
        return (_mes(dia.year, dia.month),)
    return ((date(hoy.year - n, 1, 1), date(hoy.year - n, 12, 31)),)


def _desde_hace(m: re.Match, hoy: date, primera: date, ultima: date) -> Rangos:
    n, unidad = _numero(m.group("n")), m.group("u")
    if unidad.startswith("dia"):
        inicio = hoy - timedelta(days=n)
    elif unidad.startswith("semana"):
        inicio = hoy - timedelta(days=7 * n)
    else:
        inicio = _restar_meses(hoy, n * (12 if unidad.startswith("ano") else 1))
    return ((inicio, max(inicio, ultima)),)


def _semana_pasada(m, hoy, primera, ultima) -> Rangos:
    lunes = hoy - timedelta(days=hoy.weekday())
    return ((lunes - timedelta(days=7), lunes - UN_DIA),)


def _mes_pasado(m, hoy, primera, ultima) -> Rangos:
    dia = hoy.replace(day=1) - UN_DIA
    return (_mes(dia.year, dia.month),)


def _este_ano(m, hoy, primera, ultima) -> Rangos:
    return ((date(hoy.year, 1, 1), ultima if ultima.year == hoy.year else hoy),)


def _verano_pasado(m, hoy, primera, ultima) -> Rangos:
    return (_verano(hoy.year if hoy > date(hoy.year, 8, 31) else hoy.year - 1),)


def _dia(m, hoy, primera, ultima) -> Rangos | None:
    mes, dia = MESES[m.group("m")], int(m.group("d"))
    try:
        if m.group("a"):
            fecha = date(int(m.group("a")), mes, dia)
        else:
            fecha = date(hoy.year, mes, dia)
            if fecha > hoy:
                fecha = fecha.replace(year=hoy.year - 1)
    except ValueError:  # 31 de febrero
        return None
    return ((fecha, fecha),)


def _fecha_numerica(m, hoy, primera, ultima) -> Rangos | None:
    try:
        fecha = date(int(m.group("a")), int(m.group("mm")), int(m.group("d")))
    except ValueError:
        return None
    return ((fecha, fecha),)


def _mes_sin_ano(m, hoy, primera, ultima) -> Rangos:
    meses = (_mes(a, MESES[m.group("m")]) for a in range(primera.year, ultima.year + 1))
    return tuple((i, f) for i, f in meses if i <= ultima and f >= primera)


def _veranos(m, hoy, primera, ultima) -> Rangos:
    return tuple(_verano(a) for a in range(primera.year, _ultimo_verano_completo(ultima) + 1)
                 if _verano(a)[0] >= primera)


def _ultimo_dia(m, hoy, primera, ultima) -> Rangos:
    return ((ultima, ultima),)


def _fijo(calcular: Callable[[date], tuple[date, date]]):
    def regla(m, hoy, primera, ultima):
        return (calcular(hoy),)
    return regla


# De la expresión más larga a la más corta: cada coincidencia se borra antes de la regla siguiente.
_REGLAS = [(re.compile(p), r) for p, r in (
    (r"\b(?P<a>\d{4})-(?P<mm>\d{1,2})-(?P<d>\d{1,2})\b", _fecha_numerica),
    (r"\b(?P<d>\d{1,2})/(?P<mm>\d{1,2})/(?P<a>\d{4})\b", _fecha_numerica),
    (rf"\bdesde hace {_NUM} {_UNIDAD_HACE}\b", _desde_hace),
    (rf"\bhace {_NUM} {_UNIDAD_HACE}\b", _hace),
    (rf"\b{_NUM} ultim(?:os|as) {_UNIDAD}\b", _ultimos),
    (rf"\bultim(?:o|a|os|as) (?:{_NUM} )?{_UNIDAD}\b", _ultimos),
    (r"\b(?:media|promedio) anual\b", lambda m, hoy, primera, ultima: ((_restar_meses(ultima + UN_DIA, 12), ultima),)),
    (r"\besta (?P<b>madrugada|manana|tarde|noche)\b", _ultimo_dia),  # grupo b: el bloque
    (r"\bahora(?: mismo)?\b|\bhoy\b|\bactualmente\b|\ben este momento\b", _ultimo_dia),
    (r"\bayer\b", _fijo(lambda hoy: (hoy - UN_DIA, hoy - UN_DIA))),
    (r"\besta semana\b", lambda m, hoy, primera, ultima: ((ultima - timedelta(days=6), ultima),)),
    (r"\bsemana (?:pasada|anterior)\b", _semana_pasada),
    (r"\beste mes\b", _fijo(lambda hoy: _mes(hoy.year, hoy.month))),
    (r"\bmes (?:pasado|anterior)\b", _mes_pasado),
    (r"\beste ano\b", _este_ano),
    (r"\bano (?:pasado|anterior)\b", _fijo(lambda hoy: (date(hoy.year - 1, 1, 1), date(hoy.year - 1, 12, 31)))),
    (r"\beste verano\b", _fijo(lambda hoy: _verano(hoy.year))),
    (r"\bverano (?:pasado|anterior)\b", _verano_pasado),
    (rf"\b(?P<d>\d{{1,2}}) de {_MES}(?: de(?:l)? (?P<a>\d{{4}}))?\b", _dia),
    (rf"\b{_MES}(?: de(?:l)?)? (?P<a>(?:19|20)\d{{2}})\b",
     lambda m, hoy, primera, ultima: (_mes(int(m.group("a")), MESES[m.group("m")]),)),
    (r"\b(?P<a>(?:19|20)\d{2})\b",
     lambda m, hoy, primera, ultima: ((date(int(m.group("a")), 1, 1), date(int(m.group("a")), 12, 31)),)),
    (rf"\b{_MES}\b", _mes_sin_ano),
    (r"\bveranos?\b", _veranos),
)]
