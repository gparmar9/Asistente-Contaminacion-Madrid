# TFM – Sistema Inteligente de Monitorización de Calidad del Aire (Madrid)

Sistema basado en Machine Learning que analiza los datos de calidad del aire de Madrid, **detecta
anomalías automáticamente** y (fase futura) permite consultarlos mediante informes y un asistente
conversacional con LLM.

> 📐 **Diseño completo y decisiones**: [`docs/plan_arquitectura_v3.html`](docs/plan_arquitectura_v3.html)
> (ábrelo en el navegador). Es la referencia viva de la arquitectura.

---

## Estado del proyecto

La **Fase 1 (datos + detección de anomalías)** está funcionando de punta a punta en local sobre
PostgreSQL. Las fases 2–4 (Vector DB, chatbot LLM, dashboard) están por empezar.

| Bloque | Estado |
|---|---|
| EDA del histórico (nb01) | ✅ Hecho |
| Features por bloques del día → `ResumenDatosML` (nb02) | ✅ Hecho |
| Detector de anomalías: Isolation Forest + baseline z-score (nb03) | ✅ Hecho |
| PostgreSQL en Docker (volumen persistente) | ✅ Hecho |
| Pipeline de tiempo real (API → Postgres → inferencia) | ✅ Hecho |
| CI/CD (tests unitarios + integración) | ✅ Hecho |
| Tabla de estaciones (distrito, tipo, coordenadas) | ✅ Hecho |
| Vector DB + LLM + dashboard | 🔜 Fases 2–4 |

---

## MVP — Producto mínimo viable

El corte más pequeño que entrega valor real: **ver el estado de la calidad del aire de Madrid,
saber si hay algo anómalo y poder preguntarlo en lenguaje natural**, combinando los datos con
conocimiento de salud.

**Incluye:**

- ✅ Ingesta histórico + tiempo real a PostgreSQL *(hecho)*
- ✅ Detección de anomalías (Isolation Forest + baseline z-score) en `resumen_datos_ml` *(hecho)*
- ⏳ Vector DB (ChromaDB) con documentos de salud y normativa + pipeline de embeddings
- ⏳ Chatbot con *tool use*: consulta SQL (`query_sql`) + búsqueda documental / RAG (`search_documents`)
- ⏳ Interfaz mínima (Streamlit) para chatear, con disclaimer médico

**Fuera del MVP** (mejoras posteriores): informes automáticos rotativos, dashboard completo, LSTM
y despliegue cloud 24/7.

> Con la Fase 1 ya hecha, el camino al MVP es la **Fase 2 (Vector DB con documentos)** + la
> **Fase 3 (LLM con tool use)**: un chatbot que combina los datos de contaminación (SQL) con el
> contexto de salud (RAG), sobre una interfaz mínima.

---

## Arquitectura (resumen)

```
API Madrid (tiempo real) ──▶ pipeline_tiempo_real.py ──▶ PostgreSQL: calidad_aire_horas_live (horario crudo)
                                        │
                                        ▼  agrega a bloques + features
CSV histórico 2018-2026 ──▶ Notebooks 01/02/03 ──▶ Isolation Forest (.joblib) ──▶ PostgreSQL: resumen_datos_ml (+ anomalías)
                                                                                          │
                                                                                          ▼  (Fase 2-4)
                                                                         Vector DB + LLM local + Dashboard
```

Los **datos de contaminación viven estructurados en PostgreSQL**. La Vector DB (futura) solo
contendrá documentos externos (salud, normativa), no datos de estaciones.

---

## 🚀 Puesta en marcha en local (para compañeros)

Todo lo necesario para arrancar lo que hay hecho (Fase 1) en tu máquina.

### Requisitos

- **Python 3.12**
- **Docker Desktop** (para la base de datos)
- **git**
- El **dataset histórico** `data/raw/datos_completos_2018_2026.csv` — ⚠️ **no está en el repo**
  (es grande y está en `.gitignore`). Hay que descargarlo del Drive que tenemos en común, cocnretamente en la carpeta Data. Colócalo en `data/raw/`.

### 1. Clonar y preparar el entorno

```bash
git clone git@github.com:gparmar9/Asistente-Contaminaci-n-Madrid.git
cd Asistente-Contaminaci-n-Madrid

python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

pip install -r requirements.txt
```

### 2. Configurar variables de entorno

```bash
# Windows:
copy .env.example .env
# Linux/Mac:
cp .env.example .env
```

Edita `.env` y pon una contraseña. Las credenciales de `POSTGRES_*` deben **coincidir** con las de
`DATABASE_URL`. El fichero `.env` está en `.gitignore` (no se sube nunca).

### 3. Levantar PostgreSQL en Docker

```bash
docker compose up -d
```

Esto arranca un contenedor `jupiter_postgres` (PostgreSQL 18) en `localhost:5432`, con los datos en
un **volumen nombrado** (persisten aunque pares el contenedor).

> ⚠️ **Si ya tienes un PostgreSQL nativo** ocupando el puerto 5432, tienes dos opciones: parar el
> servicio nativo, o cambiar `POSTGRES_PORT` en `.env` (p. ej. a `5433`) y actualizar el puerto en
> `DATABASE_URL`.

Parar / arrancar: `docker compose down` / `docker compose up -d` (los datos siguen en el volumen).
`docker compose down -v` **borra** los datos.

### 4. Generar los artefactos y cargar la base de datos

Con el histórico ya en `data/raw/`, ejecuta los notebooks en orden (en Jupyter o VS Code):

1. [`notebooks/02_preprocesado_y_features.ipynb`](notebooks/02_preprocesado_y_features.ipynb) → genera `data/processed/resumen_bloques.parquet`.
2. [`notebooks/03_entrenamiento_anomalias.ipynb`](notebooks/03_entrenamiento_anomalias.ipynb) → genera `data/processed/resumen_datos_ml.parquet` y `models/isolation_forest.joblib`.

Luego carga la tabla en Postgres:

```bash
python src/etl/cargar_resumen_ml.py
```

> Alternativa rápida: si descargas el `resumen_datos_ml.parquet` de la carpeta Data/processed del Drive que tenemos en común, puedes
> saltarte los notebooks e ir directo al `cargar_resumen_ml.py`.

Y carga el catálogo de estaciones (nombres, distritos, coordenadas, tipo):

```bash
python src/etl/cargar_estaciones.py
```

### 5. Ejecutar el pipeline de tiempo real

```bash
python src/etl/pipeline_tiempo_real.py
```

Baja datos de la API, los guarda en `calidad_aire_horas_live`, corre la inferencia y hace *upsert*
en `resumen_datos_ml`. Es **idempotente**: reejecutarlo no duplica nada.

### 6. Comprobar los tests

```bash
# Tests unitarios (rápidos, sin base de datos):
pytest tests/ --ignore=tests/test_integracion_db.py -v
```

---

## Estructura del repositorio

```
├── data/
│   ├── raw/
│   │   ├── datos_completos_2018_2026.csv   # histórico (NO en git — conseguir aparte)
│   │   └── estaciones-de-control.csv       # catálogo de estaciones
│   └── processed/
│       ├── calidad_aire_live.csv           # backup diario (GitHub Action)
│       └── *.parquet                        # artefactos de notebooks (NO en git)
├── docs/
│   └── plan_arquitectura_v3.html            # diseño y decisiones (referencia viva)
├── models/
│   └── isolation_forest.joblib             # modelos entrenados (6, uno por contaminante)
├── notebooks/
│   ├── 01_eda_calidad_aire.ipynb           # análisis exploratorio
│   ├── 02_preprocesado_y_features.ipynb    # bloques del día + features → ResumenDatosML
│   └── 03_entrenamiento_anomalias.ipynb    # Isolation Forest + baseline
├── src/etl/
│   ├── limpiar_datos.py                    # parser único ancho→largo (wide_a_largo)
│   ├── features_bloques.py                 # agregación a bloques + features (z-score, cobertura, CV)
│   ├── inferencia.py                       # aplica el Isolation Forest
│   ├── pipeline_tiempo_real.py             # orquestador API → Postgres → anomalías
│   ├── cargar_resumen_ml.py                # carga inicial masiva (COPY) de ResumenDatosML
│   ├── cargar_estaciones.py                # carga la tabla de estaciones (con distrito)
│   └── ingesta_datos_live_csv.py           # backup diario a CSV (GitHub Action)
├── tests/                                   # tests unitarios y de integración
├── docker-compose.yml                       # PostgreSQL 18 + volumen
├── .env.example                             # plantilla de variables de entorno
└── requirements.txt
```

---

## Modelo de datos (PostgreSQL)

| Tabla | Qué contiene |
|---|---|
| `calidad_aire_horas_live` | Horario crudo en formato largo (una fila por medición horaria). Clave única `(estacion, magnitud, fecha)`. |
| `resumen_datos_ml` | **Tabla principal**: una fila por (estación, magnitud, día, bloque) con estadísticos, baseline y salida del modelo (`anomaly_score`, `is_anomaly`, `expected_value`). ~1,27M filas. |
| `baseline_historico` | Valor esperado (`media_esperada`, `std_esperada`) por (estación, magnitud, bloque, mes). Lo usa la inferencia. |
| `estaciones` | Dimensión de las 24 estaciones: `nombre`, `distrito`, `tipo` (tráfico/fondo/suburbana), coordenadas y qué contaminantes mide. Se une a las tablas de mediciones por `estacion = codigo_corto`; da el contexto geográfico al chatbot. |

### Bloques del día

| Bloque | Horas | Relevancia |
|---|---|---|
| Madrugada | 00–06 | Nivel de fondo |
| Mañana | 07–12 | Hora punta → picos de NO₂ |
| Tarde | 13–19 | Picos de O₃ (sol + calor) |
| Noche | 20–23 | Tráfico vespertino |

---

## Detección de anomalías

Dos familias de anomalías, cada una con su señal en las features:

| Familia | Señal (feature) |
|---|---|
| **Ambiental** (nivel inusual) | `z_score` — desviación respecto al valor esperado |
| **Operativa** — sensor caído | `cobertura` baja — faltan horas del bloque |
| **Operativa** — sensor congelado | `cv ≈ 0` — variabilidad relativa nula |

**Modelo actual**: un **Isolation Forest por contaminante** (scikit-learn) sobre esas 3 features
comparables entre estaciones, más el **baseline z-score** como referencia. Un detector temporal
(autoencoder) queda como mejora opcional — ver la sección **Extras / futuras mejoras** más abajo.

---

## Datos

- **Fuentes**: histórico 2018–2026 + API de Ciudades Abiertas de Madrid (~cada 20 min) + catálogo de estaciones.
- **6 magnitudes objetivo**: NO (7), NO2 (8), PM2.5 (9), PM10 (10), NOx (12), O3 (14).
- **Formato**: la fuente viene en formato *ancho* (`H01..H24` / `V01..V24`). `wide_a_largo` lo pasa a
  *largo* (una fila por hora). Las horas con flag `N` son placeholders y se descartan para el modelado.

---

## CI/CD y ramas

- **`tests.yml`**: dos jobs — *unit* (lógica pura, sin DB) e *integración* (Postgres de servicio efímero).
  Los tests de integración solo corren con `RUN_DB_TESTS=1` (lo pone el CI), nunca contra tu DB local por accidente.
- **`ingesta_diaria.yml`**: cron que guarda los datos crudos diarios en un CSV versionado (backup gratuito, independiente del pipeline).
- **`proteger_main.yml`**: los PR a `main` solo pueden venir de `development`.

**Flujo de ramas**: trabaja en ramas `feature/...` → PR a `development` → PR de `development` a `main`.

---

## Extras / futuras mejoras

Fuera del alcance actual; opcionales o para una segunda versión. El modelo de anomalías es una pieza
enchufable, así que añadir un detector nuevo no toca el resto del sistema.

| Mejora | Qué aporta | Coste |
|---|---|---|
| **Autoencoder (PCA)** | Anomalías de *forma* del perfil horario, solo con scikit-learn (un autoencoder lineal equivale a PCA). | Bajo |
| **LSTM Autoencoder** | Lo mismo con no-linealidades temporales; componente de *deep learning* para la memoria. | Alto (PyTorch) |
| Baseline con recencia | Ponderar más los años recientes (la contaminación baja con los años). | Bajo |
| Scheduler en la nube | Ingesta 24/7 sin depender de un PC encendido. | Medio |

> **Sobre el LSTM**: se valoró como modelo principal en v2, pero el detector actual ya cubre las
> familias de anomalías relevantes (nivel, sensor caído, sensor congelado) y, sin datos etiquetados,
> su mejora no es medible. Queda como mejora opcional, no como bloqueo.

---

## Tecnologías

Python · pandas / NumPy · scikit-learn · PostgreSQL 18 (Docker) · SQLAlchemy + psycopg2 ·
pyarrow · pytest · GitHub Actions · (futuro: ChromaDB, FastAPI, LLM local)
