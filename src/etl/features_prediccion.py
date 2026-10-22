"""Features del modelo de predicción t+1 por bloques: misma lógica que el nb04,
extraída a funciones puras para que notebook e inferencia las construyan igual.

Entrada: el resumen por bloques de `a_bloques` (una fila por ESTACION, MAGNITUD,
FECHA, BLOQUE con MEDIA y COBERTURA). Cada fila describe un día t cuyo TARGET es la
MEDIA del MISMO bloque en t+horizonte. La climatología (MEDIA_ESPERADA) debe venir
calculada SOLO con el train: usar el histórico completo sería fuga.

Invariante clave: `completar_calendario` deja una fila por día natural y serie, así
shift(k) ES el día t-k (un hueco queda NaN y no arrastra el valor de hace dos días).
"""
import pandas as pd

from features_bloques import ORDEN_BLOQUES

# Una serie diaria por tupla: el mismo bloque horario de una estación y magnitud
CLAVES_SERIE = ["ESTACION", "MAGNITUD", "BLOQUE"]
LAGS = (1, 2, 3, 4, 5, 6)
VENTANAS = (3, 7)
_ORDINAL_BLOQUE = {b: i for i, b in enumerate(ORDEN_BLOQUES)}

FEATURES_PREDICCION = (
    ["MEDIA"]
    + [f"MEDIA_LAG_{k}" for k in LAGS]
    + [f"MEDIA_MOVIL_{v}" for v in VENTANAS]
    + ["MEDIA_ESPERADA", "COBERTURA", "BLOQUE_ORD", "ESTACION",
       "DIA_SEMANA_OBJ", "MES_OBJ", "ES_FIN_SEMANA_OBJ"]
)


def completar_calendario(bloques: pd.DataFrame) -> pd.DataFrame:
    """Rejilla diaria completa por serie: una fila por día natural entre la primera
    y la última fecha del conjunto. Los días sin medición quedan con MEDIA NaN."""
    df = bloques.copy()
    df["FECHA"] = pd.to_datetime(df["FECHA"])
    if df.empty:
        return df   # sin datos no hay rejilla que completar (date_range(NaT, NaT) fallaría)
    fechas = pd.DataFrame({"FECHA": pd.date_range(df["FECHA"].min(), df["FECHA"].max(), freq="D")})
    series = df[CLAVES_SERIE].drop_duplicates()
    rejilla = series.merge(fechas, how="cross")
    return rejilla.merge(df, on=CLAVES_SERIE + ["FECHA"], how="left")


def construir_lags(df: pd.DataFrame, lags=LAGS, ventanas=VENTANAS) -> pd.DataFrame:
    """MEDIA_LAG_k (día t-k) y MEDIA_MOVIL_v (media de los últimos v días, t incluido)
    por serie. Exige la rejilla de `completar_calendario`; nunca cruza series."""
    df = df.sort_values(CLAVES_SERIE + ["FECHA"], kind="stable").reset_index(drop=True)
    grupos = df.groupby(CLAVES_SERIE, observed=True)["MEDIA"]
    for k in lags:
        df[f"MEDIA_LAG_{k}"] = grupos.shift(k)
    for v in ventanas:
        # min_periods=v: una móvil con huecos dentro de la ventana queda NaN (honesta)
        df[f"MEDIA_MOVIL_{v}"] = grupos.transform(lambda s: s.rolling(v, min_periods=v).mean())
    return df


def anadir_objetivo(df: pd.DataFrame, horizonte: int = 1) -> pd.DataFrame:
    """TARGET (MEDIA en t+horizonte), COBERTURA_OBJ y el calendario de la fecha
    objetivo. Con la rejilla completa, shift(-horizonte) es exactamente FECHA+horizonte."""
    df = df.sort_values(CLAVES_SERIE + ["FECHA"], kind="stable").reset_index(drop=True)
    grupos = df.groupby(CLAVES_SERIE, observed=True)
    df["TARGET"] = grupos["MEDIA"].shift(-horizonte)
    df["COBERTURA_OBJ"] = grupos["COBERTURA"].shift(-horizonte)
    df["FECHA_OBJETIVO"] = df["FECHA"] + pd.Timedelta(days=horizonte)
    df["DIA_SEMANA_OBJ"] = df["FECHA_OBJETIVO"].dt.dayofweek
    df["MES_OBJ"] = df["FECHA_OBJETIVO"].dt.month
    df["ES_FIN_SEMANA_OBJ"] = df["DIA_SEMANA_OBJ"].isin([5, 6])
    return df


def preparar_dataset(bloques: pd.DataFrame, baseline: pd.DataFrame,
                     horizonte: int = 1, cobertura_min: float = 0.7,
                     exigir_objetivo: bool = True) -> pd.DataFrame:
    """Pipeline completo: rejilla -> lags -> objetivo -> climatología del mes objetivo.

    `baseline` es la salida de `features_bloques.calcular_baseline` SOBRE EL TRAIN
    (por estación, magnitud, bloque y MES); aquí se cruza por el mes de la fecha
    objetivo.

    `exigir_objetivo=True` (entrenar/evaluar): conserva solo filas con TARGET medido
    de forma fiable (COBERTURA_OBJ >= cobertura_min) y con MEDIA del día t presente
    (sin ella no hay persistencia con la que comparar en igualdad de condiciones).
    `exigir_objetivo=False` (predecir el futuro): el objetivo aún no existe, así que
    se devuelven todas las filas con sus features; el llamador elige el día.
    """
    df = completar_calendario(bloques.drop(columns=["MEDIA_ESPERADA", "STD_ESPERADA"], errors="ignore"))
    df = construir_lags(df)
    df = anadir_objetivo(df, horizonte)
    df["BLOQUE_ORD"] = df["BLOQUE"].map(_ORDINAL_BLOQUE)
    base = baseline.rename(columns={"MES": "MES_OBJ"})
    df = df.merge(base[["ESTACION", "MAGNITUD", "BLOQUE", "MES_OBJ", "MEDIA_ESPERADA"]],
                  on=["ESTACION", "MAGNITUD", "BLOQUE", "MES_OBJ"], how="left")
    if not exigir_objetivo:
        return df.reset_index(drop=True)
    filtro = (df["TARGET"].notna()
              & (df["COBERTURA_OBJ"] >= cobertura_min)
              & df["MEDIA"].notna())
    return df.loc[filtro].reset_index(drop=True)
