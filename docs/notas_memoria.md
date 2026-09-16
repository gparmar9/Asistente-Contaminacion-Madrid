# Notas para la memoria del TFM

Material de trabajo para redactar la memoria final (**máximo 25 páginas**). No es la memoria:
es el cuaderno de decisiones, resultados, hallazgos y aprendizajes del proyecto, ordenado por
capítulos para que redactar sea seleccionar y no reconstruir.

## 0. Cómo usar este documento

**Convenciones**
- Las secciones de cada capítulo describen el **estado actual** del sistema. Cuando algo cambia,
  se sobrescribe.
- Los cambios relevantes no se pierden: quedan en la **§2 Evolución de decisiones** con fecha,
  motivo y alternativa descartada. Es el hilo conductor de la memoria.
- `[por confirmar]` marca lo no verificado. Las cifras de coste son **estimaciones**, no facturas.
- Las cifras de resultados salen de las salidas de los notebooks y del código del repositorio.
- No se anotan secretos ni identificadores sensibles: el repositorio está en GitHub.

**Reparto orientativo de las 25 páginas**

| Capítulo de la memoria | Fuente en estas notas | Páginas |
|---|---|---|
| Introducción, problema y objetivos | §1 | 2 |
| Datos y análisis exploratorio | §3 | 3 |
| Arquitectura y su evolución | §2, §4 | 3 |
| Ingeniería de datos y pipeline | §5 | 2–3 |
| Detección de anomalías | §6 | 4 |
| Asistente: RAG y LLM | §7 | 3–4 |
| Despliegue en la nube (AWS) | §8 | 3 |
| Metodología y calidad del software | §9 | 1 |
| Resultados, limitaciones y trabajo futuro | §10, §11 | 2 |

---

## 1. Contexto, problema y objetivos

**Problema.** Madrid publica los datos de su red de estaciones de calidad del aire como datos
abiertos (histórico en CSV y API en tiempo real), pero se consumen en bruto: es difícil detectar
fallos de sensores, interpretar si un nivel es normal o saber qué implica para la salud.

**Objetivo del sistema.**
1. **Detectar anomalías automáticamente**, tanto ambientales (niveles inusuales) como operativas
   (sensores caídos o congelados).
2. **Permitir consultar los datos en lenguaje natural** mediante un asistente que combine los datos
   de contaminación con conocimiento de salud y normativa.
3. Generar **informes automáticos** (fuera del MVP).

**MVP definido (2026-07-22).** Ver el estado del aire, saber si hay algo anómalo y preguntarlo en
lenguaje natural. Incluye ingesta histórica y en tiempo real, detector de anomalías, Vector DB con
documentos de salud y normativa, chatbot con *tool use* (SQL + búsqueda documental) e interfaz
mínima con aviso médico. Fuera: informes rotativos, dashboard completo y LSTM.

**Preguntas de usuario como especificación.** `Preguntas.txt` (2026-05-03) recoge unas 50 preguntas
reales en 6 categorías: condiciones actuales, planificación del mismo día o del siguiente,
planificación estacional y de eventos, análisis histórico, decisiones residenciales y preguntas por
contaminante. Sirvieron para decidir qué documentos necesita el RAG.

> **Límite de alcance a explicar en la memoria:** varias preguntas exigen **predicción** ("¿habrá
> pico de ozono este fin de semana?", "probabilidad de episodio en 48 h") o **datos que el sistema no
> tiene** (calidad del aire interior, polen, meteorología). El sistema detecta anomalías y describe
> el presente y el pasado; no hace *forecasting*.

**Equipo** (según autores de git) `[por confirmar si hay más miembros]`: Guillermo Parés
(ingesta, ETL, notebooks 01–03, base de datos, pipeline, CI/CD, RAG, cloud) y Carlos Fernández
(banco de preguntas, EDA de PM10, esqueleto de `ApiUsuario`).

---

## 2. Evolución de decisiones

Hilo conductor de la memoria: cómo y por qué cambió el diseño.

| Fecha | Ámbito | De → A | Motivo |
|---|---|---|---|
| antes de 2026-03-26 | Vector DB | Datos de estaciones embebidos como texto (plan v1) → datos en SQL + Vector DB solo para documentos externos (plan v2) | Los datos numéricos se consultan mejor con SQL exacto que con búsqueda semántica |
| 2026-03-26 → 04-23 | Base de datos | SQLite (plan v2) → **PostgreSQL local** nativo | Primer prototipo de ingesta en tiempo real con clave única e inserción idempotente |
| 2026-05-07 | Alcance de datos | Todas las magnitudes (14 en el dataset) → **6 objetivo**: NO, NO2, NOx, O3, PM10, PM2.5 | Contaminantes con relevancia sanitaria y cobertura suficiente |
| 2026-05-07 | Ingesta programada | Ejecución manual → **GitHub Actions** diario que guarda un CSV en el repo `[por confirmar: la rama se llamaba feature/azure-functions; ¿se valoró Azure Functions?]` | Programación gratuita sin infraestructura propia |
| 2026-06-22 | Robustez de ingesta | Una petición → **reintentos** si la API responde sin `records` | La API a veces devuelve respuestas vacías |
| 2026-07-16 | Código | Limpieza duplicada entre scripts y notebooks → **parser único** `wide_a_largo` | Garantizar que histórico y tiempo real se transforman igual |
| 2026-07-20 | Modelo de anomalías | **LSTM Autoencoder** como modelo principal (plan v2) → **Isolation Forest por contaminante + baseline z-score** | Cubre las familias de anomalías relevantes sin GPU ni PyTorch, es interpretable y, sin datos etiquetados, la mejora del LSTM no sería medible. El LSTM pasa a mejora opcional |
| 2026-07-20 | Base de datos | PostgreSQL nativo → **PostgreSQL 18 en Docker** con volumen nombrado | Entorno reproducible para todo el equipo; el mismo `docker-compose` sirve de base para la nube |
| 2026-07-20 | Tiempo real | Ingesta de crudos sin modelo → **pipeline API → Postgres → inferencia → upsert** | Aplicar el detector a los datos entrantes |
| 2026-07-20 | Carga masiva | Inserción fila a fila → **COPY** de PostgreSQL | Cargar ~1,27 M filas en tiempos razonables |
| 2026-07-20 | Credenciales | Contraseña escrita en el código → **`.env` + `.env.example`** | Seguridad (ver §10) |
| 2026-07-22 | Interfaz | Chatbot por WhatsApp/Telegram (README de abril) → **web con Streamlit + FastAPI** | Menos dependencias externas para un demo |
| 2026-07-23 | RAG | Documentos por recopilar → **corpus propio de 11 documentos Markdown con metadatos** | Controlar calidad, fuentes y troceado del conocimiento |
| 2026-09-02 | Embeddings | `all-MiniLM-L6-v2` (plan v2) → **`paraphrase-multilingual-MiniLM-L12-v2`** | El corpus y las preguntas están en español; el modelo original es solo inglés |
| 2026-09-12 | Infraestructura | Todo en local → **AWS**: Lambda + S3 + RDS (en curso) | Requisito del TFM y ejecución 24/7 sin depender de un PC |
| 2026-09-16 | Backup de datos | GitHub Action que commitea `calidad_aire_live.csv` al repositorio (~13 MB/dia) → **desactivada** | Los datos ya viven en RDS con backups automaticos; el workflow inflaba el historial de git y provocaba conflictos de merge en todas las ramas |
| 2026-09-16 | Base de datos | PostgreSQL 18 en Docker → **Amazon RDS for PostgreSQL 18.3** (`db.t4g.micro`, Single-AZ, 20 GB gp2) | Servicio gestionado: backups, parches y snapshots automáticos. Migración sin cambios de código: solo cambia `DATABASE_URL` (ver §8) |

**Decisiones abiertas que alimentarán esta tabla:** LLM local vs. servicio gestionado, SQL generado
por el LLM vs. consultas predefinidas, y opción de red en AWS (§12).

---

## 3. Datos

### 3.1 Fuentes

| Fuente | Contenido | Uso |
|---|---|---|
| Histórico del Ayuntamiento (CSV, 78 MB) | 2018-01-01 → 2026-04-30, formato ancho | Entrenamiento y baseline |
| API Ciudades Abiertas (`calair_tiemporeal`) | Mediciones del día, actualización ~20 min | Pipeline en tiempo real |
| Catálogo de estaciones (CSV) | 24 estaciones: tipo, dirección, coordenadas, qué miden | Dimensión geográfica |

**Formato ancho:** una fila por (estación, magnitud, día) con 24 columnas de valor `H01..H24` y
24 flags de validación `V01..V24`.

### 3.2 Resultados del análisis exploratorio (nb01 y EDA de PM10)

- **417.629** filas estación-magnitud-día, **24 estaciones**, **14 magnitudes**; con las 6 objetivo,
  **320.129** filas y **7.574.463 mediciones horarias válidas**.
- **Hallazgo clave — el flag `N`:** las horas marcadas `N` no son mediciones sospechosas, sino
  **placeholders**: ~45 % son ceros y el resto incluye valores físicamente imposibles (decenas de
  miles de µg/m³; en PM10, máximo 89.337 y mínimo −42). Decisión: `N` se trata como **dato ausente**,
  nunca como etiqueta de anomalía.
- **Cobertura heterogénea:** NO, NO2 y NOx se miden en las 24 estaciones; PM2.5 solo en 11 y PM10 en
  15. Consecuencia: modelos **por contaminante** y robustez al nivel base de cada estación.
- **Ciclo diario:** los óxidos de nitrógeno tienen dos picos (horas punta) y el O3 alcanza su máximo
  por la tarde (origen fotoquímico). Justifica la agregación en **bloques del día**.
- **Estacionalidad opuesta:** O3 máximo en verano, NOx máximo en invierno. El valor esperado debe
  condicionarse al menos a (estación, magnitud, bloque, mes).
- **Episodios reales visibles:** el día con mayor media de PM10 fue el **15-03-2022** (337,5 µg/m³
  de media y picos horarios de 721 µg/m³ en Vallecas), coherente con el episodio de polvo sahariano
  de marzo de 2022 `[contrastar con fuente]`.
- **Distribución de PM10** con cola derecha larga: p50 = 14, p99 = 76, p99,9 = 157 µg/m³.

### 3.3 Calidad del dato

- Limpieza de PM10 (EDA de Carlos): 25.907 horas inválidas de origen, 59 negativos, 27 extremos y
  4.584 huecos (0,5 %); se imputaron 227 huecos de ≤ 3 h.
- **Duplicados:** una versión anterior del CSV tenía 51.522 filas duplicadas. El CSV actual se
  verificó el 2026-09-15: **0 duplicados** por (estación, magnitud, día).

---

## 4. Arquitectura (estado actual)

```
API Madrid (tiempo real) ─▶ pipeline_tiempo_real.py ─▶ PostgreSQL: calidad_aire_horas_live
                                     │ agrega a bloques + features + Isolation Forest
                                     ▼
CSV histórico ─▶ notebooks 01/02/03 ─▶ modelo .joblib ─▶ PostgreSQL: resumen_datos_ml (+ anomalías)

Corpus RAG (.md) ─▶ troceado + embeddings ─▶ ChromaDB            (Fase 2, en development)
Usuario ─▶ LLM con tools: query_sql + search_documents ─▶ web   (Fases 3–4, pendiente)
```

**Principio de diseño central:** los **datos de contaminación viven estructurados en SQL** y la
Vector DB solo guarda **conocimiento externo** (salud, normativa). El LLM decide qué fuente consultar
mediante *tool use*.

**Estado por fases**

| Fase | Contenido | Estado |
|---|---|---|
| 1 | Datos, detector de anomalías, pipeline en tiempo real | Hecha en local; migración a AWS en curso |
| 2 | Vector DB con corpus de salud y normativa | Implementada y mergeada en `development` (PR #37); pendiente de pasar a `main` |
| 3 | LLM con *tool use* | Pendiente |
| 4 | Informes y dashboard | Pendiente (solo esqueleto de `ApiUsuario` con `/health`) |

**Stack actual:** Python 3.12 · pandas · NumPy · scikit-learn · PostgreSQL 18 (Docker) · SQLAlchemy ·
pyarrow · ChromaDB · sentence-transformers · pytest · GitHub Actions · FastAPI (esqueleto) · AWS (en curso).

---

## 5. Ingeniería de datos y pipeline

### 5.1 Transformación

- **Parser único** `wide_a_largo`: despivota `H01..H24` a una fila por hora (`H01` → hora 0) y lo
  usan tanto los notebooks como la ingesta en tiempo real.
- **Mismo módulo de features en entrenamiento e inferencia** (`features_bloques.py`). Evita la
  deriva entre cómo se entrena el modelo y cómo se usa en producción (*training-serving skew*).
  Es un punto de ingeniería de ML que merece destacarse.

### 5.2 Bloques del día

| Bloque | Horas | Justificación (EDA) |
|---|---|---|
| Madrugada | 00–06 | Nivel de fondo |
| Mañana | 07–12 | Hora punta, picos de NO2 |
| Tarde | 13–19 | Picos de O3 |
| Noche | 20–23 | Tráfico vespertino |

Cada día pasa de 24 mediciones a 4 filas. Un bloque captura el régimen de una franja **sin diluir
los picos**, como haría una media diaria. Resultado: **1.274.644 filas de bloque**.

### 5.3 Modelo de datos (PostgreSQL)

| Tabla | Contenido |
|---|---|
| `calidad_aire_horas_live` | Horario crudo en formato largo; clave única (estación, magnitud, fecha) |
| `resumen_datos_ml` | **Tabla principal para el chatbot**: una fila por (estación, magnitud, día, bloque) con 27 columnas de identificación, calendario, estadísticos, baseline y salida del modelo |
| `baseline_historico` | Valor y dispersión esperados por (estación, magnitud, bloque, mes): **5.376** combinaciones |
| `estaciones` | 24 estaciones con nombre, tipo, coordenadas, contaminantes medidos y **distrito** |

- El **distrito** no viene en el catálogo oficial: se asignó a mano a partir de la dirección, con 4
  estaciones en frontera entre distritos marcadas para revisar.

### 5.4 Pipeline en tiempo real

1. Descarga de la API con reintentos.
2. Limpieza con el parser único.
3. Inserción del horario crudo en tabla de *staging* + `INSERT ... ON CONFLICT DO NOTHING`.
4. Agregación a bloques, features e Isolation Forest.
5. *Upsert* de `resumen_datos_ml` con `ON CONFLICT DO UPDATE`.

**Propiedad clave: idempotencia.** Reejecutarlo no duplica datos, lo que permite programarlo con
frecuencia sin coordinación.

---

## 6. Detección de anomalías

### 6.1 Familias de anomalías y su señal

| Familia | Ejemplo | Feature |
|---|---|---|
| Ambiental | O3 inusualmente alto para un julio | `Z_SCORE` respecto al valor esperado |
| Operativa: sensor caído | Faltan horas del bloque | `COBERTURA` = horas válidas / horas del bloque |
| Operativa: sensor congelado | Valor plano toda la franja | `CV` = STD / (\|MEDIA\| + 1) ≈ 0 |

Las tres features son **comparables entre estaciones**: no dependen de la escala, que es distinta en
una estación de tráfico y en una de fondo.

### 6.2 Enfoques valorados

| Enfoque | Rol | Estado |
|---|---|---|
| Z-score (\|z\| > 3) | Baseline y feature | Implementado |
| Isolation Forest por contaminante | Detector principal | Implementado |
| LSTM Autoencoder | Anomalías de forma temporal | Descartado como principal; mejora opcional |
| Prophet + residuos | Alternativa con estacionalidad | Descartado |
| Autoencoder lineal (PCA) | Anomalías de forma con scikit-learn | Mejora opcional de bajo coste |

### 6.3 Configuración

- Un `IsolationForest` por contaminante (6 modelos): `n_estimators=200`, `contamination=0.01`,
  `random_state=0`, con `StandardScaler` propio por contaminante.
- Salidas: `ANOMALY_SCORE` (score negado, **alto = más anómalo**), `IS_ANOMALY` y `EXPECTED_VALUE`
  (media histórica del grupo). Modelos, scalers y features se persisten juntos en un `.joblib`.

### 6.4 Resultados

- **Z-score:** marca el **1,35 %** de los bloques. Por contaminante: NO 2,06 %, NOx 1,66 %,
  PM10 1,52 %, PM2.5 1,51 %, NO2 0,87 % y O3 0,17 %.
- **Isolation Forest:** marca el **1,00 %**. Esa cifra la **fija el hiperparámetro**
  `contamination=0.01`: no es un hallazgo del modelo y conviene aclararlo en la memoria.

**Cruce entre detectores (1.274.644 bloques)**

| | z-score: no | z-score: sí |
|---|---|---|
| **IF: no** | 1.246.843 | 15.053 |
| **IF: sí** | **10.550** | 2.198 |

- **10.550 bloques solo los detecta el Isolation Forest**: caídas y congelaciones de sensor que el
  z-score no ve por mirar únicamente el nivel. Es el principal argumento para usar el modelo.
- **Validación cualitativa con eventos conocidos:** entre los bloques más anómalos aparece PM10 en
  la estación 18 la madrugada del **01-01-2019** (129,5 µg/m³ frente a 14,9 esperados), atribuible a
  la pirotecnia de Nochevieja.

### 6.5 Limitaciones del detector

- **Sin etiquetas:** no se pueden calcular precisión ni *recall*; la evaluación es cualitativa
  (casos extremos, solapamiento con el baseline y eventos conocidos).
- **Fuga de información en el baseline:** el valor esperado se calcula con **todo** el histórico,
  incluido el periodo evaluado. En producción debería ajustarse solo con datos de entrenamiento.
- El baseline no pondera la recencia, aunque la contaminación baja con los años.

---

## 7. Asistente: RAG y LLM

### 7.1 Vector DB y corpus (Fase 2) — estado actual

**Estado:** implementado (2026-09-02) y mergeado en `development` (PR #37); **pendiente de pasar a `main`**.

- **Corpus:** 11 documentos Markdown escritos para el proyecto como **síntesis divulgativas** de
  fuentes públicas, no copias: guías OMS 2021, límites legales UE, partículas, óxidos de nitrógeno,
  ozono, alergias, recomendaciones por perfil, protocolo de episodios de NO2 de Madrid, estaciones y
  zonas, glosario de magnitudes y aviso médico.
- **Fuentes citadas:** OMS (2021), Directiva 2008/50/CE, Directiva (UE) 2024/2881, RD 102/2011,
  EEA, US EPA, SEAIC, AEMET y Ayuntamiento de Madrid (Madrid 360).
- **Metadatos YAML** por documento (`titulo`, `tema`, `contaminantes`, `fuente`) para **filtrar**
  búsquedas y **citar la fuente** en las respuestas.
- **Troceado por secciones** `##`, anteponiendo título y sección a cada fragmento para dar contexto
  al embedding; la sección de fuentes se excluye de la búsqueda.
- **Embeddings locales:** `paraphrase-multilingual-MiniLM-L12-v2`, con vectores normalizados y
  distancia coseno. Alternativa de más calidad: `multilingual-e5-base` (exige prefijos
  `query:`/`passage:`).
- **ChromaDB persistente**, reconstruida entera en cada ingesta (idempotente). `buscar_documentos`
  devuelve los k = 4 fragmentos más cercanos con filtro opcional por tema: es la base de la futura
  tool `search_documents`.
- Coste práctico: `sentence-transformers` arrastra PyTorch y el modelo pesa ~470 MB.

### 7.2 LLM con tool use (Fase 3) — pendiente

- Tools previstas: `query_sql` (solo lectura sobre las tablas de §5.3) y `search_documents`.
- Plan original: LLM local (Llama 3 / Mistral con Ollama o vLLM) por **privacidad**, condicionado a
  disponer de GPU.

---

## 8. Despliegue en la nube (AWS)

### 8.1 Motivación y alcance

- El TFM exige una plataforma cloud; se eligió **AWS**. La cuenta la proporciona el máster.
- La Fase 1 dependía de un PC encendido y de compartir artefactos por Drive. El plan v3 dejaba
  abierta la pregunta *"¿dónde corre el pipeline 24/7?"*; la migración la responde.

| Pieza | Destino | Motivo |
|---|---|---|
| `pipeline_tiempo_real.py` | **Lambda** (imagen de contenedor) + **EventBridge Scheduler** cada 20 min | Ingesta 24/7 |
| Modelo, histórico y parquet | **S3** con versionado | Fuente única para el equipo; sustituye a compartirlos por Drive |

Los objetos se organizan con los mismos prefijos que el repositorio: `models/`, `data/raw/` y `data/processed/`.
| PostgreSQL en Docker | **RDS for PostgreSQL** `db.t4g.micro` | Servicio gestionado. **Hecho** (2026-09-16) |

**Fuera de alcance por ahora:** notebooks (siguen en local), Vector DB, LLM, API y dashboard.

### 8.2 Alternativas descartadas

- **Lambda frente a EC2 con cron:** ~2.190 ejecuciones/mes de ~30 s encajan en pago por uso y en la
  capa gratuita permanente de Lambda; un servidor encendido para eso es desproporcionado.
- **Imagen de contenedor frente a zip:** pandas + scikit-learn superan los 250 MB de las capas.
- **RDS frente a Aurora Serverless v2:** con tráfico cada 20 min nunca se pausa y sale más caro
  (> 40 $/mes).
- **RDS frente a PostgreSQL en EC2:** ahorraría ~10 $/mes a cambio de perder backups, parches y
  snapshots gestionados.

### 8.3 Estimación de costes (orientativa, `eu-west-1`)

| Servicio | Configuración | Con free tier | Sin free tier |
|---|---|---|---|
| RDS | `db.t4g.micro`, Single-AZ + 20 GB gp2 | 0 $ | ~15,6 $ |
| Lambda | ~66.000 GB-s/mes | 0 $ | 0 $ |
| EventBridge, SSM, CloudWatch, SNS | Volumen testimonial | 0 $ | 0 $ |
| S3 + ECR | ~150 MB + imagen de ~700 MB | 0 $ | ~0,1 $ |
| **Total** | | **≈ 0 $/mes** | **≈ 15,5 $/mes** |

- **~95 % del coste es RDS.** Bajar la frecuencia del pipeline no ahorra nada.
- Coste a evitar: un **NAT Gateway** (~32 $/mes) duplicaría la factura.
- La instancia se creó con la **plantilla de capa gratuita** (750 h/mes de `db.t4g.micro` Single-AZ,
  20 GB de SSD de uso general y 20 GB de backups). La configuración elegida cabe entera en esos
  límites; se eligió **gp2** en lugar de gp3 precisamente porque es lo que cubre la capa gratuita.
  `[por confirmar: si la cuenta del máster sigue dentro de los 12 meses de capa gratuita]`
- Ahorro durante el desarrollo: la instancia puede pararse (máximo 7 días seguidos) y solo se paga
  el disco. Deja de ser posible cuando la Lambda ingiera cada 20 minutos.

### 8.4 Decisión de red

Una Lambda dentro de una VPC llega a RDS en privado, pero pierde la salida a internet que necesita
para llamar a la API de Madrid.

| Opción | Descripción | Coste extra | Valoración |
|---|---|---|---|
| A | RDS público + Lambda fuera de VPC, TLS forzado y credenciales en SSM | 0 $ | Recomendada para el TFM |
| B | Lambda en VPC + NAT Gateway | ~32 $/mes | Desproporcionada para datos públicos |
| C | Lambda sin VPC descarga a S3; Lambda en VPC lee vía Gateway Endpoint y escribe en RDS privado | 0 $ | La más elegante; mejora futura |

**Estado:** opción A recomendada, **pendiente de confirmar**.

### 8.5 Seguridad

- **Acceso humano por SSO** con credenciales temporales; no se crean usuarios IAM con claves
  permanentes.
- **Mínimo privilegio:** la Lambda tendrá un rol que solo puede leer `models/*` del bucket, los
  parámetros `/jupiter/*` de SSM y escribir sus logs.
- **S3:** acceso público bloqueado, ACL deshabilitadas, cifrado SSE-S3 y versionado.
- **Secretos** en SSM Parameter Store (`SecureString`), no en ficheros desplegados: la cadena de
  conexión vive en `/jupiter/database_url` y la Lambda la leerá en ejecución con su rol. Efecto
  secundario útil: el equipo deja de compartirse la contraseña, cada persona la obtiene con sus
  propias credenciales de AWS.
- **Presupuesto con alertas** antes de crear recursos. Un Budget **avisa, no bloquea**.
- **Roles previstos:** ejecución de la Lambda (obligatorio), EventBridge Scheduler (obligatorio) y
  OIDC para GitHub Actions (opcional). Un **usuario** tiene claves permanentes; un **rol** se asume
  temporalmente. El propio acceso por SSO ya es un rol asumido.

### 8.6 Adaptaciones del código detectadas para la nube

- **Fijar scikit-learn 1.9.0:** cargar el `.joblib` con otra versión puede fallar o cargar mal.
- **Dependencias mínimas** para Lambda (sin pyarrow, matplotlib ni Jupyter): imagen de ~2 GB a ~700 MB.
- **Cachear `baseline_historico`:** hoy se lee entera en cada ejecución.
- **Concurrencia reservada = 1:** las tablas de *staging* usan `if_exists="replace"` y se pisarían.
- **Crear el índice único antes de programar** el scheduler: sobre ~1,27 M filas tarda.
- **Arranques en frío medidos:** 3,6 s de inicialización; la ejecución completa tarda 9,5 s en frío
  y 3,0 s en caliente, muy lejos del timeout de 300 s. Irrelevante para una tarea por lotes.

---

## 9. Metodología y calidad del software

- **Flujo de ramas:** `feature/*` → PR a `development` → PR a `main`. Un workflow bloquea cualquier
  PR a `main` que no venga de `development`. Más de 30 pull requests hasta julio de 2026.
- **Tests:** 16 en `main` (limpieza, features, inferencia, estaciones, ingesta, pipeline e
  integración) más 5 del troceado del corpus (en `development`).
- **CI con dos jobs:** unitarios sin base de datos e **integración contra un PostgreSQL efímero**
  como servicio. Los de integración solo corren con `RUN_DB_TESTS=1`, para no tocar nunca la base
  local por accidente.
- **Reproducibilidad:** PostgreSQL en Docker con volumen nombrado, `.env.example` y parser y features
  compartidos entre notebooks y producción.
- **Documentación viva:** planes de arquitectura v2 y v3, y README con puesta en marcha.

---

## 10. Hallazgos y lecciones aprendidas

**Datos y ML**
- Un flag de validación **no** es una etiqueta de anomalía: las horas `N` eran huecos disfrazados
  de ceros. Tratarlas como datos habría enseñado al modelo que los ceros son normales.
- Un detector que solo mira el nivel (z-score) es **ciego a los fallos de sensor**; hacen falta
  features de cobertura y variabilidad.
- El porcentaje de anomalías de un Isolation Forest lo decide `contamination`, no los datos.

**Ingeniería**
- Compartir el código de features entre entrenamiento e inferencia evita divergencias silenciosas.
- La idempotencia (`ON CONFLICT`) simplifica mucho programar el pipeline.
- La carga fila a fila no escala a millones de filas; `COPY` sí.
- **Un fallo de tipos que solo aparece con datos reales.** La API devuelve `PROVINCIA` y
  `MUNICIPIO` como texto, pero `calidad_aire_horas_live` los declara `INTEGER`: como la tabla
  de staging la crea pandas deduciendo tipos, el `INSERT` fallaba con `DatatypeMismatch`. Los
  tests no lo detectaban porque construían los datos a mano ya como enteros. Salió a la luz al
  ejecutar el pipeline dentro del entorno de Lambda contra RDS, y se corrigió en la limpieza
  (que es quien debe entregar el esquema que espera la BBDD) con un test de regresión.
- **GitHub Actions no puede escribir en un PostgreSQL local:** están en redes distintas. Por eso la
  ingesta en tiempo real no podía programarse ahí y hace falta la nube.
- Commitear un CSV de ~13 MB cada día al repositorio (60 commits automáticos en la rama `main` local) infla el
  historial de git. Con los datos en RDS deja de tener sentido.
- **Los workflows con `schedule` se ejecutan siempre desde la rama por defecto.** Comentar el
  cron en una rama de trabajo no detiene nada hasta que el cambio llega a `main`; para pararlo
  de inmediato hay que desactivarlo desde la interfaz de GitHub.
- En CI el PostgreSQL de servicio es la versión 16 y en local la 18 `[pendiente de alinear]`.

**Seguridad**
- En abril la contraseña de la base de datos quedó **escrita en el código y commiteada**. Se corrigió
  moviéndola a `.env`, pero **sigue en el historial de git**: lo correcto es rotarla y no reutilizarla.

**Cloud (AWS)**
- Hay dos modelos de free tier: el clásico de 12 meses y el de créditos para cuentas nuevas desde 2025.
- Las credenciales del portal SSO son temporales; los servicios desplegados usan sus propios roles.
- En S3 no hay carpetas reales: `models/isolation_forest.joblib` es una única clave.
- El NAT Gateway es el coste inesperado más común al meter una Lambda en una VPC.
- En una cuenta de una organización, las políticas del máster (SCP) pueden restringir acciones
  aunque se tenga `AdministratorAccess`.

---

## 11. Limitaciones y trabajo futuro

**Limitaciones**
- Sin predicción: el sistema no responde preguntas sobre el futuro (§1).
- Evaluación del detector sin etiquetas y baseline con fuga de información (§6.5).
- Distritos asignados manualmente; 4 estaciones fronterizas por revisar.
- Sin datos meteorológicos, de polen ni de aire interior.

**Trabajo futuro**
- LSTM Autoencoder o PCA para anomalías de forma del perfil horario.
- Baseline ponderado por recencia y ajustado solo con el periodo de entrenamiento.
- Módulo de *forecasting* para las preguntas de planificación.
- Opción de red C en AWS (RDS sin exposición pública a coste cero).
- CI/CD con despliegue automático a AWS mediante OIDC.

---

## 12. Bitácora

| Fecha | Hito |
|---|---|
| 2026-03-26 | Inicio: plan de arquitectura v2, documentación de la API y primer parser de limpieza |
| 2026-04-10 | Primer EDA y script de ingesta en tiempo real |
| 2026-04-23 | Ingesta a PostgreSQL local con clave única e inserción sin duplicados |
| 2026-05-03 | Banco de preguntas de usuario (`Preguntas.txt`) |
| 2026-05-07 | GitHub Action de ingesta diaria a CSV; reducción a 6 contaminantes; primeros tests y CI; esqueleto de `ApiUsuario` |
| 2026-06-04 | EDA y limpieza de PM10 |
| 2026-06-22 | Reintentos en la ingesta cuando la API responde vacía |
| 2026-06-26 | Notebooks de NO y O3 |
| 2026-07-16 | Nuevos notebooks de EDA y preprocesado; parser único |
| 2026-07-20 | Detector Isolation Forest (nb03); PostgreSQL en Docker con carga por COPY; pipeline en tiempo real; tests de features, inferencia e integración; protección de `main` |
| 2026-07-21 | Plan de arquitectura v3 |
| 2026-07-22 | Definición del MVP; tabla de estaciones con distritos |
| 2026-07-23 | Corpus RAG en Markdown |
| 2026-09-02 | Pipeline RAG: troceado, embeddings multilingües y búsqueda en ChromaDB, mergeado en `development` (PR #37) |
| 2026-09-12 | Análisis y plan de migración de la Fase 1 a AWS con costes y decisión de red |
| 2026-09-14 | Acceso a la cuenta AWS del máster (SSO, `eu-west-1`) y AWS CLI configurada |
| 2026-09-15 | Presupuesto mensual `jupiter-mensual` (10 $) y bucket S3 `jupiter-calidad-aire-madrid` (sin acceso público, versionado, SSE-S3) |
| 2026-09-15 | Verificado que el CSV histórico actual no tiene filas duplicadas |
| 2026-09-16 | Artefactos subidos a S3 (Guillermo): modelo, histórico, catálogo de estaciones y los dos parquet — 5 objetos, 166 MiB |
| 2026-09-16 | Instancia RDS `jupiter-postgres` creada (Guillermo): PostgreSQL 18.3, `db.t4g.micro`, Single-AZ, 20 GB gp2, acceso público restringido por grupo de seguridad y TLS obligatorio |
| 2026-09-16 | Tablas cargadas en RDS desde local con los scripts existentes: `resumen_datos_ml` (1.274.644 filas) y `estaciones` (24). La Fase 1 deja de depender del PostgreSQL en Docker |
| 2026-09-16 | Cadena de conexión guardada cifrada en SSM Parameter Store (`/jupiter/database_url`, SecureString) |
| 2026-09-16 | Desactivada la ingesta diaria a CSV (`ingesta_diaria.yml`): se comenta el cron y se conserva el disparo manual |
| 2026-09-16 | Imagen de Lambda construida y probada en local (1,26 GB, `x86_64`). Detectado y corregido un fallo de tipos en la limpieza (`provincia`/`municipio` como texto) con test de regresión |
| 2026-09-16 | Pipeline validado de punta a punta dentro del entorno de Lambda contra RDS: 2.568 mediciones horarias nuevas, 313 bloques y 41 anomalías en datos reales; la segunda invocación devuelve 0 filas nuevas (idempotencia confirmada en la nube) |

---

## 13. Preguntas abiertas

- [ ] ¿Qué límite de gasto o créditos tiene la cuenta AWS del máster?
- [ ] Confirmar la opción de red en AWS (A recomendada).
- [ ] ¿Permite la organización crear roles IAM? (necesario para la Lambda)
- [ ] LLM de la Fase 3: ¿local con GPU o servicio gestionado? ¿SQL generado o consultas predefinidas?
- [ ] ¿Se valoró Azure Functions para la ingesta programada? (§2)
- [ ] Pasar la Fase 2 (RAG) de `development` a `main`.
- [ ] Contrastar la atribución del pico de PM10 del 15-03-2022 a polvo sahariano.
