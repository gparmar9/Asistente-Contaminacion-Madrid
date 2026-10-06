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
(ingesta, ETL, notebooks 01–03, base de datos, pipeline, CI/CD, primera versión del RAG, cloud)
y Carlos Fernández (banco de preguntas, EDA de PM10, esqueleto de `ApiUsuario`, reescritura del
RAG como servicio de evidencias).

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
| 2026-09-23 | Embeddings | `paraphrase-multilingual-MiniLM-L12-v2` → **`intfloat/multilingual-e5-base`** | Ventana de 512 tokens y 768 dimensiones: las secciones largas del corpus dejan de truncarse. Coste: el modelo pasa de ~470 MB a ~1,1 GB y exige prefijos `query:`/`passage:` |
| 2026-09-23 | Arquitectura del RAG | Biblioteca importable (`buscar_documentos`) → **servicio HTTP de evidencias, con el LLM fuera** | El LLM vive en la API de chat y usa el RAG como *function tool*. El RAG recupera, valida las citas que devuelve el modelo y construye la bibliografía desde el corpus (§7.2) |
| 2026-09-23 | Metadatos del corpus | `fuente` como texto libre → **`fuentes` estructuradas (título, organismo, URL) + `revisado` / `fecha_revision`** | Permite citar cada fuente con su URL y dejar borradores versionados sin que entren en el índice |
| 2026-09-23 | Identificador de fragmento | ID posicional por documento (`archivo#n`) → **`chunk_id` estable** (`archivo:slug-sección:ordinal`) | Reordenar o añadir secciones ya no cambia los IDs: una cita sigue apuntando al mismo texto |
| 2026-09-27 | LLM del asistente | LLM local con Ollama, condicionado a GPU (plan v2/v3) → **API de LLM hospedada con tier gratuito**, cliente contra la interfaz OpenAI-compatible (proveedor = 3 variables de entorno; sin fijar aún) | La GPU del máster no se concedió (riesgo nº 1 del roadmap). Los free tiers de 2026 (Mistral, Groq, Gemini) ofrecen modelos de clase ≥70B con *function calling*. Anula el DoD «nada de APIs externas» de T3.1: aceptable porque los datos enviados son públicos y el chat no guarda datos personales. Volver a un LLM propio = cambiar la URL base |
| 2026-09-27 | Arquitectura de la API (Fase 4) | Esqueleto único sin diseño cerrado → **2 servicios**: `ApiUsuario` (ligero: lecturas SQL + proxy de chat) y `LLMOrchestrator` (agente + tools + deps pesadas de RAG; pendiente). **Sin estado**: ni usuarios ni historial (se descarta el scaffold usuario/conversación). El dashboard consumirá solo la API | Separar las dependencias pesadas (torch/chromadb) del servicio de usuario, permitir el trabajo en paralelo del equipo y dejar la costura lista para un futuro LLM autoalojado. API primero, agente después: 3 de los 4 grupos de endpoints solo necesitan SQL |
| 2026-09-28 | Despliegue | Build + push + `update-function-code` a mano desde un PC con credenciales SSO → **workflow de GitHub Actions (`workflow_dispatch`) con OIDC** | Cualquiera del equipo puede desplegar sin credenciales de AWS; no se despliega si fallan los tests; el tag por SHA dice qué commit corre en producción y permite volver atrás; la verificación del digest elimina el fallo silencioso de subir la imagen sin actualizar la función. Descartado: claves de acceso en GitHub Secrets (permanentes y compartidas) y despliegue en cada push a `main` (se optó por lanzarlo a mano para decidir cuándo se toca producción) |
| 2026-09-28 | Vuelta atrás | `update-function-code` a mano con la imagen buena → **workflow de rollback** con modo consulta, vuelta por digest a una versión publicada y verificación | Deshacer un despliegue fallido en segundos, sin credenciales ni reconstruir la imagen, y dejando rastro (cada rollback publica una versión con su motivo). Descartado: "volver a la versión anterior" automático, porque tras un rollback la anterior es justo la versión que se deshizo; primero se consulta la tabla y luego se elige |
| 2026-10-01 | Disponibilidad de la base de datos | RDS encendida 24 h → **encendida solo de 22:00 a 01:30 (hora de Madrid) y a demanda** con un workflow de GitHub (§8.8) | La carga es una vez al día y la API aún no está desplegada: pagar la instancia 24 h no aporta nada. Ahorro estimado de ~15,5 a ~4–5 $/mes. Descartado: DynamoDB (el acceso es analítico: rangos, agregaciones y `JOIN`, justo lo que no hace bien una base clave-valor), Aurora Serverless v2 con pausa automática (ahorro parecido pero exige migrar; queda como alternativa), programarlo con `schedule` de GitHub Actions (puede retrasarse y se desactiva tras 60 días sin actividad) y que el programador llame directamente a `StartDBInstance` (da error si la base ya está encendida) |
| 2026-10-01 | Workflows de despliegue | «Desplegar Lambda» y «Rollback Lambda» → **«Desplegar» y «Rollback» con un desplegable de componente** (hoy solo `lambda`) | Preparar el despliegue de la API, el orquestador, el RAG y la web sin multiplicar workflows: cada pieza será una opción del desplegable y un job propio. Bloqueo por componente: piezas distintas pueden desplegarse a la vez, la misma no. Descartado: un workflow por pieza (duplica pasos y botones) y pedir la imagen a desplegar (el despliegue siempre construye el código de la rama elegida; volver a una versión concreta es tarea del rollback) |
| 2026-10-02 | Empaquetado de la API | Servicios arrancados a mano con `uvicorn` y `python -m rag.api` → **una imagen Docker por servicio** (`api-usuario`, `orquestador`, `rag`), levantadas juntas con el perfil `api` de `docker-compose` | Es como irán en la EC2 y permite desplegar cada pieza por separado. La imagen del RAG lleva **dentro el modelo y el índice** construido en el propio build, con el commit del corpus en sus metadatos: arranca sin descargar nada y cada imagen corresponde a un corpus concreto. Descartado: una sola imagen con todo (obliga a redesplegar el RAG, de ~3 GB, por cualquier cambio en la API) y construir el índice al arrancar el contenedor (arranque lento y sin garantía de que todas las réplicas usen el mismo índice) |

| 2026-10-04 | Agente LLM | `LLMOrchestrator` como único agente → **nuevo servicio `Agente`** (puerto 8200), diseñado desde cero con un **bucle de herramientas escrito a mano**; **LlamaIndex solo como cliente del LLM** (`OpenAILike` para APIs OpenAI-compatibles y `BedrockConverse` para Amazon Bedrock, elegibles por variable de entorno) y, más adelante, como almacén del chat. Ambos servicios conviven sin código ni dependencias compartidas; `ApiUsuario` elegirá uno por configuración | Principios tomados de las clases del máster y de un diseño de agente empresarial: pocas piezas, el modelo clasifica y el código decide, fases pequeñas con salida tipada y valor seguro, síntesis final sin herramientas, lo determinista lo entrega el código. **Descartado un framework de agentes completo** (ReAct/`AgentWorkflow` de LlamaIndex, LangGraph): esconde el bucle que precisamente se quiere controlar (vetar u obligar herramientas, cerrar el turno sin modelo, validar la salida con el RAG) y arrastra abstracciones que no se usan. Descartado también ampliar `LLMOrchestrator`: su bucle no tiene puntos de intervención en código y rehacerlo dentro equivalía a reescribirlo. Bedrock entra como proveedor de producción porque la cuenta del máster da acceso; el modelo concreto en `eu-west-1` con *tool use* y *streaming* queda `[por confirmar]` |

**Decisiones abiertas que alimentarán esta tabla:** proveedor concreto del LLM (se decidirá al
conectar el bucle real: crear cuenta y verificar límites en consola) y dónde se despliega el
servicio RAG en producción (§13). La opción de red en AWS ya está decidida y aplicada (opción A, §8.4).

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

Corpus RAG (.md) ─▶ troceado + embeddings e5 ─▶ ChromaDB ─▶ servicio RAG (FastAPI)   (Fase 2)

Usuario ─▶ ApiUsuario (FastAPI) ──▶ PostgreSQL (lecturas: estaciones, series)   (Fase 4, en curso)
                    └─ /chat ──┬─▶ LLMOrchestrator ─▶ LLM hospedado (OpenAI-compat)
                               │       ├─ query_sql ─────────▶ PostgreSQL       (Fase 3, primer agente;
                               │       └─ buscar_evidencias ─▶ rag.api (HTTP)    proveedor pendiente)
                               └─▶ Agente ─▶ LLM vía LlamaIndex (OpenAI-compat | Bedrock)
                                       └─ buscar_evidencias ─▶ rag.api (HTTP)   (Fase 3 bis, en construcción;
                                                                                  ApiUsuario elegirá por configuración)
```

**Principio de diseño central:** los **datos de contaminación viven estructurados en SQL** y la
Vector DB solo guarda **conocimiento externo** (salud, normativa). El LLM decide qué fuente consultar
mediante *tool use*.

**Estado por fases**

| Fase | Contenido | Estado |
|---|---|---|
| 1 | Datos, detector de anomalías, pipeline en tiempo real | Hecha en local; migración a AWS en curso |
| 2 | Vector DB y servicio de evidencias | **Reescrita** (2026-09-23) como servicio de evidencias y mergeada en `development`; pendiente de pasar a `main` |
| 3 | LLM con *tool use* | **Dos servicios** (§7.3): `LLMOrchestrator`, implementado (2026-09-28; ruta documental por contrato HTTP el 2026-09-30) con `query_sql` y `buscar_evidencias`, 51 tests, sin proveedor conectado; y `Agente` (2026-10-04), rediseño por fases con bucle propio y LlamaIndex como cliente: fases 1 a 3 de 7 hechas (esqueleto, herramienta documental, ruta documental validada, clasificador de intención, 20 tests). Falta conectar un proveedor real |
| 4 | Informes y dashboard | En curso: `ApiUsuario` con `/estaciones`, `/estaciones/{codigo}/series` y `/chat` (proxy con stub), tests propios y job de CI. Informes y dashboard pendientes |

**Stack actual:** Python 3.12 · pandas · NumPy · scikit-learn · PostgreSQL 18 (RDS) · SQLAlchemy ·
pyarrow · ChromaDB · sentence-transformers (`multilingual-e5-base`) · FastAPI · pytest ·
GitHub Actions · AWS (Lambda, S3, RDS, EventBridge, CloudWatch).

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

### 6.5 Qué detecta el modelo en un día real (2026-09-16)

Primera ejecución del detector sobre datos en vivo en AWS. Desglose de los bloques del día,
separando los marcados como anómalos de los normales:

| Bloque | Marcado | n | Cobertura media | Cobertura mín. | \|z\| medio | \|z\| máx |
|---|---|---|---|---|---|---|
| madrugada | no | 103 | 1,00 | 1,00 | 0,60 | 1,69 |
| mañana | **sí** | 13 | 0,55 | 0,33 | 1,14 | 1,68 |
| mañana | no | 90 | 1,00 | 0,67 | 0,98 | 2,21 |
| tarde | **sí** | 2 | 0,50 | 0,29 | 0,98 | 1,61 |
| tarde | no | 105 | 0,85 | 0,71 | 0,75 | 1,78 |

**Resultado clave: las 15 anomalías del día son todas operativas, ninguna ambiental.** Los bloques
marcados tienen la mitad de las horas sin medir (cobertura 0,29-0,55) mientras su nivel es normal.

**El baseline z-score no habría detectado nada:** el máximo del día es 2,21 y el umbral es 3. Es la
comprobación empírica, sobre datos reales, de que el z-score es ciego a los fallos de sensor y de
que el Isolation Forest aporta valor precisamente ahí. Sirve como caso de estudio en la memoria.

**Efecto del momento de ejecución.** La misma jornada dio 41 anomalías a las 17:06 y 15 a las 19:40:
el bloque de tarde en curso tenía entonces ~0,5 de cobertura y se marcaba; al completarse hasta 0,85
dejó de marcarse. Es decir, ejecutar a media jornada genera falsos positivos transitorios en la
franja abierta. De ahí que la ingesta se programe a las **23:45**, con las cuatro franjas cerradas:
el recuento es estable y comparable entre días.

**Confirmado en producción, no solo en el test.** Tras desplegar el arreglo, la ejecución del
2026-09-19 corrigió 742 horas que habían quedado como placeholder, y las anomalías del día
volvieron al rango normal (13, frente a las 97 falsas registradas a media tarde antes del arreglo).
Las 126 filas `N` que quedan son íntegramente la hora 23 (una por cada serie activa), la
limitación permanente ya documentada — no un resto del bug.

### 6.6 Limitaciones del detector

- **Sin etiquetas:** no se pueden calcular precisión ni *recall*; la evaluación es cualitativa
  (casos extremos, solapamiento con el baseline y eventos conocidos).
- **Fuga de información en el baseline:** el valor esperado se calcula con **todo** el histórico,
  incluido el periodo evaluado. En producción debería ajustarse solo con datos de entrenamiento.
- El baseline no pondera la recencia, aunque la contaminación baja con los años.

---

## 7. Asistente: RAG y LLM

### 7.1 Corpus documental — estado actual

- **Corpus:** 11 documentos Markdown escritos para el proyecto como **síntesis divulgativas** de
  fuentes públicas, no copias: guías OMS 2021, límites legales UE, partículas, óxidos de nitrógeno,
  ozono, alergias, recomendaciones por perfil, protocolo de episodios de NO2 de Madrid, estaciones y
  zonas, glosario de magnitudes y aviso médico.
- **Fuentes citadas:** OMS (2021), Directiva 2008/50/CE, Directiva (UE) 2024/2881, RD 102/2011,
  EEA, US EPA, SEAIC, AEMET y Ayuntamiento de Madrid (Madrid 360).
- **Frontmatter YAML** por documento: `titulo`, `tema` (salud | normativa | proyecto),
  `contaminantes`, `revisado`, `fecha_revision` y `fuentes` estructuradas (título, organismo, URL).
  Solo se indexa lo que tiene `revisado: true`, así que un borrador puede vivir en git sin
  contaminar el índice. El formato se valida al leer: un documento mal formado es un error, no un
  fragmento silenciosamente raro.
- **Troceado por secciones** `##`, anteponiendo `título — sección` a cada fragmento para dar
  contexto al embedding. Las secciones `Fuentes`, `Cómo lo usa el asistente` e `Indicación para el
  asistente` se excluyen: las fuentes viven en el frontmatter y las instrucciones, en el prompt.
- **`chunk_id` estable** (`ozono_salud:efectos-en-la-salud:0`): no depende de la posición de la
  sección, de modo que reordenar el documento no invalida las citas ya emitidas.
- **Guardia de tokens**: una sección que supera el presupuesto del modelo se parte por párrafos
  conservando título y sección. Se cuenta con el tokenizer real del modelo, no por palabras.

### 7.2 Servicio de evidencias con citas verificables (Fase 2) — estado actual

**Estado:** reescrito el 2026-09-23 por Carlos Fernández en `feature/rag-herramienta-llm` (fases A y
B de un plan propio de tres). Sustituye a `trocear_corpus.py` e `ingesta_vector.py`. Pendiente de PR
y de la fase C (evaluación reproducible).

**Principio de diseño: el LLM vive fuera.** El paquete `rag` no llama a ningún modelo de lenguaje.
Se expone como servicio HTTP (FastAPI) y el LLM, que vive en la API de chat, lo usa como
*function tool*. El flujo es de tres pasos:

1. **Recuperar** — `POST /rag/evidencias` devuelve los fragmentos bajo un umbral de distancia,
   numerados `D1..Dn`, con avisos fijos y bibliografía.
2. **Redactar** — el modelo devuelve un JSON `{estado, afirmaciones[{texto, evidencias}],
   limitaciones}` conforme a un esquema que sirve tal cual como `response_format` del proveedor.
3. **Validar y renderizar** — `POST /rag/validar` comprueba el contrato y produce el texto final, o
   devuelve los errores y un `mensaje_reparacion` listo para reenviar al modelo.

**La garantía principal: las citas no se creen, se comprueban.** Título, sección, tema y fuentes se
resuelven **desde el corpus a partir del `chunk_id`**, nunca de lo que envíe el cliente ni el
modelo. Un ID inventado se rechaza; una URL que el modelo escriba dentro de una afirmación no llega
nunca a la bibliografía; y la bibliografía final solo lista los documentos realmente citados. Es el
argumento central del capítulo del asistente: la trazabilidad no depende de la buena conducta del
LLM, sino de una comprobación determinista en el backend.

**Endpoints:** `GET /salud` (modelo, fragmentos, commit y umbral vigentes), `GET /rag/herramienta`
(definición de la tool en formato *function calling* de OpenAI, que aceptan también Ollama, vLLM y
Mistral, más el JSON Schema de salida), `POST /rag/evidencias` y `POST /rag/validar`.

**Qué se valida de la salida del modelo:** claves exactas, estado dentro de la enumeración, máximo 8
afirmaciones de 700 caracteres, cada una con al menos un ID **de esa petición**, máximo 6
limitaciones, coherencia entre estado y número de afirmaciones, y existencia real de cada
`chunk_id` en el corpus.

**Umbral de evidencia: 0,22 de distancia coseno, provisional.** Medido con e5-base, `k=4` y el
corpus actual (11 documentos, 50 fragmentos): las preguntas documentales dan 0,11–0,20 en su mejor
fragmento y las ajenas al corpus («capital de Francia») 0,24–0,25. El margen es estrecho y la
calibración con casos de evaluación es la fase C. Si nada baja del umbral, el estado es
`sin_evidencia` y la API de chat puede responder insuficiencia **sin llamar al modelo**.

**Avisos fijos en código, no en el corpus.** El aviso sanitario se añade si la pregunta contiene
términos de salud o si alguna evidencia citada es de `tema: salud`; la limitación de actualidad, si
la pregunta habla de «hoy», «ahora» o «está activado». Confirma la decisión de julio: el aviso
médico no puede quedar a merced de la recuperación semántica.

**Robustez de la indexación:** `python -m rag.indexar` calcula los embeddings **antes** de borrar la
colección anterior, así que un fallo a mitad no destruye el índice. La colección guarda el modelo
con el que se construyó, el commit del corpus y la fecha; la búsqueda se niega a responder si el
modelo configurado no coincide con el del índice. Cualquier respuesta es trazable al corpus exacto
que la generó.

**Defensa frente a inyección de prompt:** el prompt de sistema propuesto
(`docs/rag/prompt_respuesta_fundamentada_v1.txt`) instruye tratar la pregunta y las evidencias como
datos, nunca como instrucciones.

**Coste práctico:** el modelo e5-base pesa ~1,1 GB y se carga en la primera petición, no al
arrancar, así que esa primera llamada tarda. El servicio no lleva autenticación ni CORS: asume red
interna entre la API de chat y él.

**Fuera de alcance declarado:** ingesta incremental, filtro por contaminante, *reranking*, historial
de conversación, *streaming* y consulta de mediciones.

### 7.3 LLM con tool use (Fase 3) — dos servicios, proveedor pendiente

Hay dos implementaciones del agente, independientes entre sí (sin código, dependencias ni imágenes
compartidas). `ApiUsuario` reenvía `/chat` a una u otra por configuración (hoy `ORCHESTRATOR_URL`;
el agente nuevo añadirá `AGENTE_URL`). El agente nuevo ya se ha probado contra Amazon Bedrock
(clasificador y uso de herramientas, 2026-10-04); el orquestador sigue sin proveedor conectado.

#### 7.3.1 `Agente` — rediseño por fases (en construcción)

**Estado (2026-10-04): fases 1 a 3 de 7 hechas** (rama `feature/Agente`). Servicio FastAPI en `Agente/`
(paquete `agente`, puerto 8200, `GET /salud`, `POST /responder` con el mismo contrato que reenvía
`ApiUsuario`), misma convención por capas que `ApiUsuario`.

**Diseño.** Un solo agente con el **bucle de herramientas escrito a mano**; LlamaIndex
(`llama-index-core` 0.14) aporta solo el cliente del LLM: `OpenAILike` (Mistral API, Groq,
Ollama…) o `BedrockConverse` (Amazon Bedrock, credenciales por rol de instancia o perfil), elegidos
con `LLM_PROVEEDOR`. El resto del código ve una única interfaz (`FunctionCallingLLM`). Principios:
el modelo clasifica y el código decide; fases pequeñas con salida tipada y valor seguro; la síntesis
final no tiene herramientas; lo determinista (texto con citas y bibliografía) lo entrega el código,
no lo reescribe el modelo; el estado entre turnos se guarda como datos tipados; las comprobaciones
nuevas empiezan en modo observación. Alternativas descartadas en §2 (2026-10-04); el detalle de
cada decisión y de la elección de LlamaIndex está en `docs/agente/decisiones_agente.md`.

**Lo que hace hoy (fases 1 a 3).**
- **Herramientas**: contrato propio (`definicion()` en formato OpenAI y `ejecutar()` que **nunca
  lanza**: devuelve `{ok, datos | error}` y el error llega al modelo como resultado). Un adaptador
  mínimo las presenta a LlamaIndex con el esquema JSON crudo; LlamaIndex nunca las ejecuta.
- **`buscar_evidencias`**: cliente HTTP de `rag.api`. La definición la publica el RAG
  (`GET /rag/herramienta`, cacheada por proceso); si el GET falla, la herramienta **no se ofrece en
  ese turno** y se reintenta en el siguiente. `POST /rag/evidencias` → el modelo recibe
  `para_el_modelo` (sin `chunk_id` ni internos); el estado y los `chunk_id` se guardan aparte para
  el código.
- **Bucle**: máx. `MAX_VUELTAS=3` llamadas al LLM con herramientas; si el modelo sigue pidiendo
  herramientas, **síntesis forzada sin herramientas** (peor caso: 4 llamadas). Herramienta
  desconocida → mensaje `tool` de error y se sigue. Cualquier fallo del proveedor → 503 controlado.
  El stream del LLM ya se drena (`astream_chat_with_tools`): el streaming de la fase 4 no cambiará
  la forma del bucle.
- **Ruta documental determinista (fase 2)**: si el turno tuvo evidencias, cuando el modelo deja de
  pedir herramientas su texto libre se descarta y una llamada aparte, **sin herramientas** y con
  mensajes nuevos (pregunta + evidencias), devuelve el JSON `{estado, afirmaciones[{texto,
  evidencias}], limitaciones}` cuyo esquema publica el RAG. `POST /rag/validar` lo comprueba: si
  falla, **una reparación** con el mensaje del RAG; si vuelve a fallar, frase fija de insuficiencia.
  Si es válido, **passthrough íntegro** del texto que renderiza el RAG; `fuentes` = solo documentos
  citados; `advertencia` sanitaria solo si el RAG la activa (hay afirmaciones de salud). Se eligió
  dejar seguir el bucle en vez de cortarlo al llegar evidencias porque el RAG será una herramienta
  entre varias (SQL, ML); coste: una llamada más (3 por pregunta documental, 4 con reparación).
  - **Intervención**: si una búsqueda devuelve `sin_evidencia` y el turno no tiene evidencias, el
    código cierra el turno con frase fija **sin volver a llamar al modelo** (1 sola llamada).
  - **Renumeración**: cada búsqueda del RAG numera desde D1; el agente reasigna D1..Dn por orden de
    llegada, sin repetir fragmentos, antes de que el modelo vea el resultado.
  - Probado con el `rag.api` real y el LLM falso: válida, reparada, doble fallo y `sin_evidencia`
    se comportan como se espera.
- **Clasificador de intención y decisión en código (fase 3)**: antes del bucle, una llamada corta
  (mismo modelo, temperatura 0, tiempo límite propio de 10 s) responde dos líneas: intención
  (`DOCUMENTAL`, `DATOS`, `PREDICCION`, `CHARLA`, `FUERA_DE_ALCANCE`) y tema (salud, normativa,
  proyecto, ninguno). Parser estricto (dos líneas, valores conocidos; solo quita adornos de
  markdown como `**`): error, tiempo agotado o formato inválido dan el valor seguro
  `DESCONOCIDA`, que ofrece todas las herramientas y no usa frases fijas (un clasificador caído no
  debe quitar herramientas). Una tabla en código decide:
  - `DATOS`, `PREDICCION` y `FUERA_DE_ALCANCE` → frase fija, sin llamar más al modelo (el agente aún
    no consulta mediciones ni predice).
  - `CHARLA` → sin herramientas, prompt corto con lo que sabe hacer el asistente y sus límites.
  - `DOCUMENTAL` → solo `buscar_evidencias`, obligatoria. Si el modelo no la pide, la lanza el código
    con la pregunta literal y el tema del clasificador y pasa directo a la ruta documental. Si el RAG
    está caído → frase fija sin modelo, para no responder sobre salud sin evidencias.
  - Una herramienta no permitida no se ofrece; si el modelo la pide igualmente, recibe un error y no
    se ejecuta.
  - Descartado: buscar siempre antes del modelo (ahorra una llamada, pero pierde la reformulación de
    la búsqueda) y responder en libre con el RAG caído. La intención `REPETIR` se deja para la
    fase 5, cuando haya historial.
  - Coste en llamadas al LLM: frase fija 1; charla 2; documental 4 (5 con reparación; 3 si busca el
    código).
  - Guion de medición fuera de CI (`Agente/evaluacion/`): 20 preguntas etiquetadas a mano, 15 de
    `Preguntas.txt` y 5 nuevas para charla y fuera de alcance. **Resultado en Bedrock
    (2026-10-04): 20/20 en intención y 5/5 en tema con los dos modelos candidatos**. Latencia típica
    del clasificador: ~0,3 s con Ministral 14B y ~0,5–1 s con gpt-oss-120b (un caso aislado de 7 s).
  - **Hallazgo de la primera medición: 4/20 con Ministral 14B por formato, no por comprensión.** El
    modelo escribía `intencion: **DOCUMENTAL**`; el parser estricto rechazaba la respuesta y el
    turno caía a `DESCONOCIDA`. Las 16 respuestas rechazadas tenían intención y tema correctos.
    gpt-oss-120b sacó 19/20: devolvió `tema: conceptos generales`, una expresión que el prompt usaba
    para describir `DOCUMENTAL`. Arreglo en dos capas: el prompt pide texto plano sin negritas, deja
    claro que el tema es uno de cuatro y ya no menciona «conceptos generales»; además, el parser
    quita `*` y `` ` `` antes de comparar. Con el prompt nuevo Ministral ya no usa negritas (0/20),
    así que la tolerancia del parser queda como red de seguridad. Descartado un parser tolerante que
    busque la palabra en el texto: un formato dudoso no debe disparar frases fijas.
- **Tests**: 21, sin red, sin claves y sin torch: LLM falso con guion que hereda de
  `FunctionCallingLLM` (mismo camino que un proveedor real) y RAG fingido con
  `httpx.MockTransport`. **Política de tests mínima** (decisión del 2026-10-04): solo los casos
  que fija el plan de cada fase (bucle con 0, 1 y 2 llamadas, límite de vueltas, RAG caído,
  contrato HTTP; en la fase 2, JSON válido, reparación, doble fallo y `sin_evidencia`; en la fase 3,
  cada intención, clasificador caído, herramienta vetada, RAG caído con intención documental y
  clasificación en negrita), sin tests de detalle interno; se partió de 25 y se recortaron. Verificado además, sin red, que `OpenAILike` serializa la herramienta cruda
  y los mensajes `assistant(tool_calls)`/`tool` como espera la API. Quinto job del workflow de CI.
- **Probado contra Bedrock (2026-10-04)**, desde local con credenciales SSO temporales: los dos
  candidatos están bajo demanda en `eu-west-1` con streaming, y los dos **piden la herramienta por
  Converse con streaming** (`astream_chat_with_tools`), con la consulta reformulada y `tema=salud`.
  Ruta documental de punta a punta con `rag.api`: medida el 2026-10-05 (abajo). Queda por probar la
  síntesis forzada.
- **Modelo en Bedrock: dos candidatos a comparar** (decisión del 2026-10-04): **Ministral 14B 3.0**
  ($0,24 / $0,24 por 1M de tokens de entrada / salida en Irlanda) y **gpt-oss-120b** ($0,18 / $0,70).
  Ministral 14B es el Mistral actual más parecido a Mistral Small 3.2 24B, que era la referencia del
  equipo. gpt-oss-120b tiene una arquitectura parecida a la de Mistral Small 4.
  - Descartados: Small 3.2 (no está en Bedrock ni se puede importar); Magistral Small 1.2 (razonamiento:
    unas 3 veces más caro y más lento); Mistral Large 3 (no está en regiones de la UE).
  - Coste estimado de los dos: unos $0,003 por pregunta documental. Decidirá la calidad medida.
    Clasificador y uso de herramientas: empate (ver arriba). Calidad del español `[por medir]`.
  - **Ruta documental (2026-10-05)**: 18 turnos por modelo (las 6 preguntas documentales del guion,
    2 veces cada una, más la del NO2 repetida). En todos los turnos el modelo pidió la búsqueda por
    sí mismo, sin que la lanzara el código.

    | Modelo | JSON válido a la primera | Tras una reparación | Fallidos (frase de insuficiencia) | Latencia del turno |
    |-|-|-|-|-|
    | Ministral 14B | 18 | 0 | 0 | 3,0–7,3 s |
    | gpt-oss-120b | 10 | 4 | 4 | 4,5–10,2 s |

  - **Hallazgo: gpt-oss-120b se queda sin tokens de salida.** El agente no fija `max_tokens` y
    `BedrockConverse` usa 512 por defecto. gpt-oss es un modelo de razonamiento: lo que razona antes
    de responder cuenta dentro de ese límite, y como razona más o menos cada vez, el JSON de la
    síntesis sale cortado en un punto distinto (238, 393 o 632 caracteres en tres turnos). El RAG
    lo rechaza («la salida debe ser un objeto JSON»), la reparación vuelve a cortarse y el usuario
    recibe la frase de insuficiencia. Falla sobre todo en las respuestas largas (NO2, PM10 frente a
    PM2.5). Con 2048 tokens, la misma síntesis salió válida 6 de 6 veces (con 512, 3 de 6); gastó
    540–637 tokens de salida para un JSON visible de 500–1000 caracteres. Bedrock respondió
    `stopReason=end_turn` en llamadas que gastaron 497 y 505 de 512 tokens, así que el corte no se
    detecta por el motivo de parada `[por confirmar en una llamada cortada]`. El razonamiento no se
    cuela en el texto: todas las salidas empiezan por el JSON. Ministral 14B no razona y cabe en 512.
    Arreglo propuesto, sin aplicar: `LLM_MAX_TOKENS` configurable (2048) y repetir la comparación en
    igualdad de condiciones.
  - Hallazgo: `BedrockConverse` 0.15.3 rechaza los IDs de modelo que no conoce, como el de Ministral
    14B (`ValueError: Unknown model`). Resuelto con una subclase en la fábrica que declara a mano los
    metadatos de los modelos fuera de su lista.

**Fases pendientes** (4–7): streaming SSE y sesión (`session_id` opaco, un turno a
la vez); memoria con `turn_state` tipado; comprobaciones posteriores en modo observación con
evaluación sobre 20 preguntas etiquetadas; Bedrock y despliegue junto al orquestador.

#### 7.3.2 `LLMOrchestrator` — primer agente (implementado el 2026-09-28; convive)

Servicio FastAPI, puerto 8100, `POST /responder`. Ligero: el RAG se consume por HTTP (sin
torch/chromadb). Se mantiene tal cual mientras el agente nuevo no lo sustituya.

- **Bucle**: máx. `MAX_ITERACIONES=4` vueltas; al agotarse, cierre forzado sin tools. Las tools
  nunca lanzan: sus errores vuelven como `{"error": ...}`.
- **`query_sql`**: 5 consultas predefinidas (`ultimos_niveles`, `serie_bloques`, `anomalias`,
  `comparar_estaciones`, `info_estaciones`) con parámetros validados y ligados; `anomalias` exige
  `cobertura >= 0,7` en el SQL. Sin SQL libre (D3 en §2).
- **`buscar_evidencias`** (2026-09-30): cliente HTTP de `rag.api`; definición pedida al RAG y
  cacheada; si falla, degrada a solo `query_sql`. Renumera `Dn` si hay varias llamadas.
- **Ruta documental con citas verificadas** (2026-09-30): JSON `{estado, afirmaciones, limitaciones}`
  validado con `POST /rag/validar`, una reparación, texto renderizado con `[Dn]`; `fuentes` solo con
  documentos citados y advertencia médica solo con afirmaciones documentales. La ruta de solo datos
  responde en texto libre. Peor caso: `MAX_ITERACIONES` + 3 llamadas.
- **Cliente LLM**: SDK `openai` (chat/completions + `tools`) con reintentos del propio SDK;
  proveedor = `LLM_BASE_URL` + `LLM_API_KEY` + `LLM_MODEL`.
- **Tests**: 51 (2026-10-01), ~1 s, sin red. Cuarto job de CI.
- **Revisión adversarial** (2026-09-28, 4 revisores + refutación): aviso al LLM cuando un resultado
  se trunca a 60 filas; filtro de distrito por coincidencia parcial; presupuesto de timeouts
  coherente entre servicios (`LLM_TIMEOUT_S=30` × 2 reintentos ≤ `ORCHESTRATOR_TIMEOUT_S=120`); el
  cierre forzado no envía `tool_choice` sin `tools`; `choices` vacío → 503; prompt ajustado a las
  limitaciones reales de los datos. Limitación aceptada: consultas sin estación recorren la tabla.
- **Integración con la Fase 2** (rota al mergear `development` el 2026-09-30, resuelta el mismo
  día): el orquestador adopta el contrato de evidencias por HTTP en lugar de llamar al RAG en
  proceso (duplicaba responsabilidades y metía las deps pesadas). El hueco «una afirmación basada
  en SQL no tiene `Dn`» se resuelve separando rutas; descartado extender el esquema con IDs `S1..Sn`.
  Verificado de punta a punta contra `rag.api` real.

**Proveedor del LLM (común a ambos, sin decidir).** Finalistas evaluados el 2026-09-27 con la
documentación oficial: **Mistral** plan Experiment (toda la gama gratis, límites reportados
~1 req/s y 500K tokens/min `[por confirmar en consola]`, mejor español), **Groq** (30 RPM,
1.000 req/día; riesgo: 8K tokens/min con contexto RAG) y **Gemini** (límites del free tier no
publicados). Descartados: Cerebras (free tier eliminado en 2026), Cohere (1.000 llamadas/mes),
Hugging Face (créditos insuficientes); OpenRouter solo como reserva. Desde el 2026-10-04 se suma
**Amazon Bedrock** para producción (acceso con la cuenta del máster). Para el agente nuevo se
compararán Ministral 14B 3.0 y gpt-oss-120b en `eu-west-1` (§7.3.1). Implicación operativa: los free tiers limitan
peticiones/minuto y cada pregunta cuesta 2–4 llamadas → pocas vueltas y reintentos con *backoff*.

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
- **Encendido programado (desde el 2026-10-01, §8.8):** la instancia solo está encendida de 22:00 a
  01:30 y cuando el equipo la enciende a mano; parada solo se paga el disco y las copias. Estimación
  sin capa gratuita: ~3,5 h/día ≈ 106 h/mes de instancia + disco ≈ **4–5 $/mes** en lugar de 15,5
  `[estimación: confirmar con la factura de octubre]`. Es posible porque la carga pasó a ser diaria
  (23:45); con la ingesta cada 20 minutos prevista al principio no lo habría sido.

### 8.4 Decisión de red

Una Lambda dentro de una VPC llega a RDS en privado, pero pierde la salida a internet que necesita
para llamar a la API de Madrid.

| Opción | Descripción | Coste extra | Valoración |
|---|---|---|---|
| A | RDS público + Lambda fuera de VPC, TLS forzado y credenciales en SSM | 0 $ | Recomendada para el TFM |
| B | Lambda en VPC + NAT Gateway | ~32 $/mes | Desproporcionada para datos públicos |
| C | Lambda sin VPC descarga a S3; Lambda en VPC lee vía Gateway Endpoint y escribe en RDS privado | 0 $ | La más elegante; mejora futura |

**Estado: opción A aplicada** (2026-09-16). El grupo de seguridad admite el puerto 5432 desde
`0.0.0.0/0` porque una Lambda fuera de VPC no tiene IP fija que autorizar. Mitigaciones: TLS
obligatorio (`rds.force_ssl = 1`, verificado el 2026-09-16: RDS rechaza cualquier conexión
sin cifrar), contraseña larga y aleatoria
cifrada en SSM, y datos públicos sin información personal. Es una decisión consciente y
documentada, no una configuración por defecto; la opción C queda como mejora futura.

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
- **Roles:** ejecución de la Lambda (`jupiter-lambda-pipeline`), EventBridge Scheduler
  (`jupiter-scheduler-pipeline`), desde el 2026-09-27 **`jupiter-github-actions-deploy`** para el
  despliegue automático (§13) y, desde el 2026-10-01, **`jupiter-automation-rds`** (los runbooks que
  encienden y apagan la RDS) y **`jupiter-github-actions-entorno`** (workflows «Encender entorno» y
  «Apagar entorno»),
  ambos limitados a `jupiter-postgres` y comprobados con el simulador de IAM (§8.8). Un **usuario** tiene claves permanentes; un **rol** se asume
  temporalmente. El propio acceso por SSO ya es un rol asumido.
- **OIDC de GitHub Actions** (2026-09-27): proveedor de identidad
  `token.actions.githubusercontent.com` + rol `jupiter-github-actions-deploy`, sin claves de acceso
  guardadas en GitHub (el único secreto es el ID de cuenta, para que no salga en los logs públicos).
  La *trust policy* exige audiencia `sts.amazonaws.com` y `sub` de este repositorio, desde
  **cualquier rama**: el equipo quiere poder desplegar una rama concreta con `workflow_dispatch`.
  Permisos inline `despliegue-ecr-lambda`: subir imágenes solo al repositorio ECR
  `jupiter-pipeline` y, solo sobre esa función, `UpdateFunctionCode`, `PublishVersion` y
  `ListVersionsByFunction` (las dos últimas añadidas el 2026-09-28 para el rollback). Comprobado
  con el simulador de IAM que no puede borrar la función ni cambiar su configuración. Políticas
  versionadas en `deploy/iam/*-github-actions.json`.
  - *Descartado:* restringir a la rama `main` (lo más seguro, pero impide desplegar otras ramas) y
    personalizar el `sub` del token con la API de GitHub para incluir el workflow (una pieza más que
    mantener y cambia el token de todos los workflows del repo).
  - *Riesgo asumido:* cualquier workflow del repo con `id-token: write` podría asumir el rol, así
    que los cambios en `.github/workflows/` se revisan en el PR como código con acceso a producción.

### 8.6 Adaptaciones del código detectadas para la nube

- **Fijar scikit-learn 1.9.0:** cargar el `.joblib` con otra versión puede fallar o cargar mal.
- **Dependencias mínimas** para Lambda (sin pyarrow, matplotlib ni Jupyter): imagen de ~2 GB a ~700 MB.
- **Cachear `baseline_historico`:** hoy se lee entera en cada ejecución.
- **Concurrencia reservada = 1:** las tablas de *staging* usan `if_exists="replace"` y se pisarían.
- **Crear el índice único antes de programar** el scheduler: sobre ~1,27 M filas tarda.
- **Arranques en frío medidos:** 3,6 s de inicialización; la ejecución completa tarda 9,5 s en frío
  y 3,0 s en caliente, muy lejos del timeout de 300 s. Irrelevante para una tarea por lotes.

### 8.7 Observabilidad

Dos alarmas de CloudWatch que avisan por correo (SNS), pensadas para dos fallos distintos:

| Alarma | Métrica | Dispara cuando |
|---|---|---|
| `jupiter-pipeline-errores` | `Errors` (suma, 5 min) | La función lanza una excepción |
| `jupiter-pipeline-sin-ejecuciones` | `Invocations` (suma, 24 h) | No ha habido ninguna ejecución |

La segunda es la que más aporta y depende de un detalle de configuración: `treatMissingData:
breaching`, es decir, **la ausencia de datos se interpreta como fallo**. Cubre el fallo silencioso
de que el programador deje de disparar o alguien desactive la función: no hay errores que contar,
no salta nada y el hueco en los datos se descubriría semanas después.

La retención de los logs se baja a 14 días; por defecto no caducan nunca.

### 8.8 Encendido programado de la base de datos — estado actual

La RDS solo está encendida cuando hace falta: cada noche alrededor de la carga, y cuando el equipo
la enciende a mano para trabajar o hacer una demo.

| Hora de Madrid | Qué pasa | UTC en verano / invierno |
|---|---|---|
| 22:00 | `jupiter-rds-encender` enciende la RDS | 20:00 / 21:00 |
| — | Ventana de copias de seguridad | 21:10–21:40 / 21:10–21:40 |
| 23:45 | `jupiter-pipeline-diario` lanza la Lambda (sin cambios) | 21:45 / 22:45 |
| — | Ventana de mantenimiento (domingos) | 22:00–22:30 / 22:00–22:30 |
| 01:30 | `jupiter-rds-apagar` la apaga, también si alguien la encendió a mano | 23:30 / 00:30 |

- **El cambio de hora condiciona el horario.** Las programaciones van en `Europe/Madrid`, pero las
  ventanas de copias y mantenimiento de RDS se fijan en UTC. Con una sola hora encendida, la franja
  de verano y la de invierno no se solapan en UTC y no habría dónde colocar las copias; con 3,5 h
  queda un tramo común y ninguna ventana coincide con la Lambda. Se evita además la franja de 02:00
  a 03:00, que no existe el día del cambio de hora de marzo.
- **Idempotente.** El programador no llama a RDS directamente, sino a los runbooks gestionados por
  AWS `AWS-StartRdsInstance` / `AWS-StopRdsInstance` (Systems Manager Automation), que primero
  consultan el estado y no hacen nada si la base ya está encendida o apagada. Llamar directamente a
  `StartDBInstance` habría devuelto `InvalidDBInstanceState` cada vez que alguien la hubiera
  encendido antes. Contenido de los runbooks revisado el 2026-10-01.
- **Pocos reintentos** (3, durante como mucho 30 min): un fallo nocturno no debe acabar encendiendo
  la base por la mañana. La Lambda conserva su política anterior.
- **Encendido y apagado a demanda:** workflows `encender_entorno.yml` («▶️ Encender entorno») y
  `apagar_entorno.yml` («⏹️ Apagar entorno»), con un rol propio que solo puede consultar, encender
  y apagar `jupiter-postgres` (no borrarla ni modificarla). Los dos terminan bien si la base ya está
  en el estado pedido y comparten bloqueo para no solaparse. El de apagar **se niega entre las 22:00
  y las 00:00** (hora de Madrid) para no dejar sin base de datos la carga de las 23:45; apagar a
  mano es opcional, porque la programación de la 01:30 la apaga igualmente. Cuando exista el
  servidor de la API, ambos gestionarán también la EC2.
- **Riesgo aceptado:** el día que haya mantenimiento pendiente, si se alarga en invierno puede
  coincidir con la Lambda (22:45 UTC) y hacerla fallar esa noche. Lo detecta la alarma de errores y
  la carga es idempotente, así que basta con relanzarla. Ocurre pocas veces al año.

---

## 9. Metodología y calidad del software

- **Flujo de ramas:** `feature/*` → PR a `development` → PR a `main`. Un workflow bloquea cualquier
  PR a `main` que no venga de `development`. Más de 30 pull requests hasta julio de 2026.
- **Tests:** 16 en `main` (limpieza, features, inferencia, estaciones, ingesta, pipeline e
  integración). Con el RAG reescrito la suite unitaria llega a **96 pruebas** (verificado el
  2026-09-27 en local con todas las dependencias del RAG: 95 pasan por defecto y 1 se salta —
  la que carga el modelo real e indexa y busca de extremo a extremo, solo activa con
  `RUN_RAG_TESTS=1` por el coste de cargar el modelo—; con esa variable activada pasan las 96,
  en 42,55 s).
- **Cobertura del RAG en CI.** El job unitario instala además `fastapi`, `httpx` (mismas versiones
  que `requirements-rag.txt`) y `jsonschema`, que son ligeros y no arrastran PyTorch: así se prueban
  el contrato HTTP y el esquema de salida del modelo, además del troceado y la validación de citas.
  `chromadb` y `sentence-transformers` no se instalan a propósito (descarga grande), y
  `test_buscar` y `test_indexar` se saltan solos con `importorskip`. Simulado en local sin esas
  dos librerías: 80 pasan y 2 ficheros se saltan `[por confirmar en la primera ejecución de CI]`.
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
- La idempotencia (`ON CONFLICT`) simplifica mucho programar el pipeline, pero **`DO NOTHING` no
  es lo mismo que idempotencia**. La API entrega siempre las 24 horas del día y las aún no
  medidas llegan como ceros con flag `N`: al ignorar los conflictos, la primera ejecución del día
  fijaba esos ceros y ninguna posterior los corregía. El 2026-09-16, con varias ejecuciones
  manuales durante la tarde, las horas 18 a 23 quedaron guardadas como inválidas de forma
  permanente (740 filas `N` frente a 135-153 en los días de ejecución única). Corregido pasando a
  `DO UPDATE` condicionado a que la medición entrante sea válida y la guardada no.
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
- **A un LLM no se le pide que cite bien: se le comprueba.** En el servicio de evidencias el modelo
  solo elige identificadores `D1..Dn`; el backend resuelve título, sección y fuentes desde el
  `chunk_id` leyendo el corpus. Una URL que invente el modelo no puede llegar a la bibliografía.
  Convertir una promesa de comportamiento en una comprobación determinista es lo que hace la
  trazabilidad defendible.
- **Al medir un LLM, mirar las respuestas crudas antes de juzgar al modelo.** La primera medición
  del clasificador dio a Ministral 14B un 4/20, pero las 16 respuestas «malas» eran correctas en
  negrita (`**DOCUMENTAL**`) y el parser estricto las descartaba. Una línea en el prompt («texto
  plano, sin negritas») lo llevó a 20/20. Un parser estricto mide el formato a la vez que la
  comprensión: las dos cosas deben salir por separado en el informe de evaluación.
- **Un índice se reconstruye sin ventana de indisponibilidad** calculando los embeddings antes de
  borrar la colección anterior. El orden ingenuo (borrar y luego calcular) deja el sistema sin
  índice si el cálculo falla.
- **Un identificador derivado de la posición es frágil.** Pasar a `chunk_id` estables
  (`archivo:slug-sección:ordinal`) permite reordenar el corpus sin invalidar las citas emitidas.
- **Dependencias sin fijar en CI rompen sin tocar código.** El job de integración instalaba
  `sqlalchemy` sin versión; una versión nueva pasó a elegir el driver `psycopg` (v3) para las URL
  `postgresql://` y el job falló con `No module named 'psycopg'` en un PR que no tocaba la base de
  datos. En local no pasaba porque `requirements.txt` fija `SQLAlchemy==2.0.49`. Corregido fijando
  en CI las mismas versiones (2026-09-27).

**LLM**
- **Un modelo de razonamiento gasta en pensar los tokens de salida** (2026-10-05). Con el límite
  por defecto de `BedrockConverse` (512), gpt-oss-120b cortaba el JSON de la síntesis en un punto
  distinto cada vez y 4 de 18 preguntas documentales acabaron en la frase de insuficiencia; con
  2048, 6 de 6 bien. El síntoma engaña: parece que el modelo «no sabe» devolver JSON, pero lo
  empieza bien y se queda sin espacio. Lecciones: fijar siempre `max_tokens` en vez de heredar el
  valor por defecto de la librería, y registrar a nivel visible por qué falla una validación (el
  aviso iba a nivel INFO y uvicorn no lo mostraba; hizo falta un script de diagnóstico). §7.3.1.

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
- **El simulador de políticas de IAM** (`simulate-principal-policy`) permite comprobar permisos
  sin ejecutar nada. Con varios recursos en la misma llamada, el resultado resumido marcó como
  denegadas acciones que el rol sí tenía; hay que leer el detalle por recurso o simular recurso a
  recurso.
- **IAM solo evalúa `aud` y `sub` de un token OIDC de GitHub.** La primera versión del rol de
  despliegue se restringía por `job_workflow_ref` (el fichero de workflow), que viaja en el token
  pero IAM no expone como condición: la condición nunca se cumplía y el rol era inasumible.
  AWS aceptó la política sin avisar; se detectó revisando la documentación antes del primer
  despliegue y se corrigió a `sub` (2026-09-27). Lección: una *trust policy* válida
  sintácticamente no es una *trust policy* que funcione.
- **Hora local y UTC en la misma planificación** (2026-10-01). El primer horario propuesto para
  encender la RDS (una hora por la noche) era correcto en hora de Madrid, pero las ventanas de copias
  y mantenimiento de RDS solo admiten UTC: con el cambio de hora de octubre la franja encendida se
  desplaza una hora en UTC y no quedaba ningún tramo común en el que colocar las copias. Lección:
  cuando una tarea en hora local convive con otra en UTC, hay que comprobar las dos épocas del año.
- **El simulador de IAM solo responde a la pregunta que se le hace** (2026-10-01). La política del
  programador permitía lanzar los runbooks sobre el ARN `automation-definition/...:$DEFAULT`, y el
  simulador lo dio por bueno porque se le preguntó por ese mismo ARN. En la ejecución real, AWS
  comprobó el permiso contra otro recurso, el documento (`document/AWS-StartRdsInstance`), y la
  primera noche la base no se encendió. Tras corregirlo, la noche siguiente falló por un tercer
  recurso: `ssm:StartAutomationExecution` se autoriza a la vez contra el documento **y** contra la
  ejecución que crea (`automation-execution/*`), y cada error de CloudTrail solo muestra el primer
  recurso denegado. La prueba manual había funcionado porque se lanzó con un usuario administrador,
  no con el rol del programador. Lecciones: probar con la **identidad real** que usará la
  automatización (por ejemplo, con una programación de un solo uso dentro de unos minutos) en vez
  de esperar a la noche, y leer el `errorMessage` de CloudTrail, que dice exactamente qué acción y
  qué recurso se denegaron.

---

## 11. Limitaciones y trabajo futuro

**Limitaciones**
- Sin predicción: el sistema no responde preguntas sobre el futuro (§1).
- Evaluación del detector sin etiquetas y baseline con fuga de información (§6.5).
- Distritos asignados manualmente; 4 estaciones fronterizas por revisar.
- Sin datos meteorológicos, de polen ni de aire interior.
- **La hora 23 nunca se captura.** El endpoint en tiempo real solo sirve el día en curso y publica
  con ~1 hora de retraso (comprobado: a las 14:51 la última hora válida era la 13). A las 23:45 lo
  último disponible es la hora 22, y pasada la medianoche la API ya solo devuelve el día nuevo.
  Consecuencia sistemática: el bloque de noche (20-23) tiene cobertura 0,75 todos los días. Para
  cerrarlo haría falta una segunda fuente (el fichero diario del Ayuntamiento), no otro horario.
- **El asistente documental no consulta mediciones.** El RAG responde sobre documentación revisada,
  no sobre la situación de hoy; por eso añade automáticamente una limitación de actualidad cuando
  la pregunta habla del presente. Unir ambas fuentes es la Fase 3 (§7.3).
- **El umbral de evidencia está calibrado a ojo** (0,22) con un margen estrecho entre las preguntas
  del corpus y las ajenas, y sin casos de evaluación todavía.

**Trabajo futuro**
- **¿Hace falta un modelo de razonamiento para el agente?** En la ruta documental no: el modelo sin
  razonamiento (Ministral 14B) acertó 18 de 18 turnos y es más rápido (§7.3.1). Queda por analizar
  con las preguntas de datos, cuando exista la herramienta SQL: elegir consulta y parámetros, o
  combinar varias mediciones, puede beneficiarse del razonamiento. Depende de si la herramienta usa
  consultas predefinidas (como `LLMOrchestrator`, §2 2026-09-28) o SQL generado. Si se usa un modelo
  razonador, hay que darle margen de tokens de salida (§10).
- LSTM Autoencoder o PCA para anomalías de forma del perfil horario.
- Baseline ponderado por recencia y ajustado solo con el periodo de entrenamiento.
- Módulo de *forecasting* para las preguntas de planificación.
- Opción de red C en AWS (RDS sin exposición pública a coste cero).
- **Evaluación del RAG (fase C):** 20–30 casos (documentales, sin evidencia y adversarios) con
  recall@k, validez de citas y acierto de abstención, y recalibrado del umbral con esos datos.
- **Decidir dónde se despliega el servicio RAG.** El modelo de embeddings (~1,1 GB) y su carga en
  la primera petición no encajan bien en Lambda; habrá que valorar otra opción de cómputo.

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
| 2026-09-16 | Imagen subida a ECR (304 MB) y función Lambda `jupiter-pipeline` creada (imagen, x86_64, 1024 MB, timeout 300 s, concurrencia reservada 1) con rol de ejecución de mínimo privilegio. Primera invocación en AWS correcta |
| 2026-09-16 | Ejecución diaria programada a las 23:45 (`Europe/Madrid`) con EventBridge Scheduler y rol propio; verificado `rds.force_ssl = 1`; alarmas de CloudWatch de errores y de ausencia de ejecuciones con aviso por SNS |
| 2026-09-17 a 09-19 | **El pipeline se ejecuta solo**: tres noches consecutivas a las 23:45 sin intervención, 2.568 mediciones por día y 23 estaciones, sin errores ni alarmas. Queda respondida la pregunta abierta del plan v3 sobre dónde corre el pipeline en producción |
| 2026-09-19 | Revisando esos datos se detectan dos problemas: el `DO NOTHING` congelaba las horas aún no medidas (corregido) y la hora 23 es inalcanzable con el endpoint en tiempo real (limitación documentada) |
| 2026-09-19 a 20 | Arreglo desplegado y **validado en producción**: la ejecución de la noche del 19
  corrigió 742 horas placeholder y las anomalías volvieron a 13 (frente a las 97 falsas de antes
  del arreglo). Las 126 filas `N` restantes son exactamente la hora 23 en todas las series |
| 2026-09-23 | **RAG reescrito como servicio de evidencias** (Carlos Fernández, rama
  `feature/rag-herramienta-llm`): paquete `rag` instalable, modelo e5-base, `chunk_id` estables,
  umbral de evidencia y API FastAPI que valida las citas del modelo. Sustituye a `trocear_corpus.py`
  e `ingesta_vector.py` |
| 2026-09-24 | Revisión de esa rama (Guillermo): la suite unitaria pasa (80 pruebas en local). Se
  detectan el README principal desactualizado (documenta ficheros ya borrados), la cobertura parcial
  del RAG en CI y el hueco de contrato entre `query_sql` y el esquema de citas (§7.3) |
| 2026-09-27 | Preparación del PR de la rama del RAG (Guillermo): README principal actualizado al paquete
  `rag` (instalación, `python -m rag.indexar`, servicio de evidencias, nuevo formato del
  frontmatter) y job de CI ampliado para probar la API del RAG. Suite local: 95 pasan, 1 saltada |
| 2026-09-27 | En el PR salta el job `integracion`: `sqlalchemy` sin fijar versión en el workflow
  eligió el driver `psycopg` (v3) para `postgresql://` y rompió la conexión (`requirements.txt` fija
  `SQLAlchemy==2.0.49`, que resuelve a `psycopg2`). Corregido fijando las mismas versiones en CI.
  RAG mergeado a `development` (PR #42) y de ahí a `main` (Guillermo) |
| 2026-09-27 | **OIDC de GitHub Actions creado** (Guillermo, cuenta AWS del máster): proveedor de
  identidad + rol `jupiter-github-actions-deploy` con permisos mínimos sobre ECR y la Lambda
  `jupiter-pipeline`. La primera *trust policy* (por `job_workflow_ref`) era inasumible y se
  corrigió a `sub` del repositorio (§10). Workflow `deploy_lambda.yml` escrito: tests → build →
  push a ECR con tag por SHA → `update-function-code` → verificación del digest |
| 2026-09-27 | Diseño de la capa de API cerrado: 2 servicios (`ApiUsuario` + `LLMOrchestrator`), sin estado, LLM hospedado gratuito con cliente OpenAI-compatible en vez de Ollama/GPU. `ApiUsuario` implementado: `/estaciones`, `/estaciones/{codigo}/series` (bloques + anomalías) y `/chat` (proxy al orquestador con stub), con 18 tests sobre SQLite en memoria y job propio en CI (Sheila) |
| 2026-09-28 | **Primer despliegue automático correcto** (Guillermo, desde `main`, commit `2778132`):
  2 min 26 s en total (tests unitarios 41 s e integración 57 s en paralelo; build 24 s, push 26 s,
  actualización 20 s). Verificado fuera del workflow: la Lambda ejecuta la imagen con tag
  `2778132`, su `CodeSha256` coincide con el digest de ECR, se publicó la versión 2 y CloudTrail
  registra el `AssumeRoleWithWebIdentity` con `sub` de la rama `main`. Primera ejecución
  programada con la imagen nueva: noche del 28-09 `[por confirmar en CloudWatch]` |
| 2026-09-28 | **Workflow de rollback** (Guillermo, `rollback_lambda.yml`): modo consulta con la tabla
  de versiones y vuelta a una versión por digest, sin reconstruir, con verificación. El despliegue
  pasa a publicar versiones con descripción (`deploy <commit> desde <rama>`). Rol ampliado con
  `PublishVersion` y `ListVersionsByFunction`. Lógica de la tabla probada en local con los datos
  reales de la función (v1 y v2); **pendiente de la primera ejecución real** |
| 2026-10-01 | **Encendido programado de la RDS** (Guillermo, cuenta AWS del máster): rol
  `jupiter-automation-rds`, permisos del programador ampliados a los runbooks
  `AWS-Start/StopRdsInstance`, programaciones `jupiter-rds-encender` (22:00) y `jupiter-rds-apagar`
  (01:30), ventanas de copias (21:10–21:40 UTC) y mantenimiento (dom 22:00–22:30 UTC) movidas, y rol
  `jupiter-github-actions-entorno` para el nuevo workflow «Encender entorno». Permisos comprobados
  con el simulador de IAM: cada rol puede justo lo previsto y nada más. Prueba real del apagado con
  el runbook el mismo día (`Success`). **La primera noche no encendió:** a las 22:00 el
  programador recibió `AccessDenied` en `ssm:StartAutomationExecution`, porque AWS autoriza la
  llamada contra el documento (`arn:aws:ssm:eu-west-1::document/AWS-StartRdsInstance`) y la
  política solo permitía el recurso `automation-definition` (§10). Diagnosticado con CloudTrail y
  corregido añadiendo los ARN de documento. **El apagado de la 01:30 volvió a fallar**, ya con otro
  mensaje: la misma acción exige también permiso sobre la ejecución que crea
  (`automation-execution/*`). Corregido el 02-10 y **probado ese mismo día** con una programación
  de un solo uso a las 19:47 con el rol real del programador: el runbook de apagado se lanzó y
  terminó en `Success`. **Ciclo nocturno confirmado** las noches del 02-10 y del 03-10: los runbooks
  de encendido (22:00:27) y apagado (01:30:08) terminaron en `Success` las dos noches.
  **Workflow «Encender entorno» probado** esa misma noche con la base parada:
  correcto en 4 min 44 s de principio a fin (incluye el arranque del runner y la autenticación
  OIDC; la mayor parte es el arranque de la instancia). **Workflow «Apagar entorno» probado** el
  02-10 con la base encendida: correcto. Los cambios en IAM y RDS los ejecutó Guillermo con un script: el modo
  automático del asistente bloquea por diseño conceder permisos y modificar recursos compartidos.
  Mismo día: workflow «Apagar entorno» y `rds:StopDBInstance` añadido al rol de entorno |
| 2026-10-01 | **Despliegue por componente** (Guillermo): `deploy_lambda.yml` y `rollback_lambda.yml`
  pasan a `desplegar.yml` («🚀 Desplegar») y `rollback.yml` («⏮️ Rollback»), con un desplegable de
  componente (hoy solo `lambda`), un job por componente y bloqueo por componente. Sin cambios en
  AWS: la confianza del rol de despliegue no depende del nombre del fichero del workflow (§10).
  Primera ejecución tras el merge a `main` (04-10): «Rollback» en modo consulta, correcto |
| 2026-10-02 | **API en contenedores** (Guillermo, rama `feature/contenedores-api`): Dockerfile por
  servicio (`ApiUsuario/`, `LLMOrchestrator/`, `deploy/Dockerfile.rag`), perfil `api` en
  `docker-compose.yml` y `RAG_COMMIT` en `rag.indexar` para conservar el commit del corpus sin git
  dentro de la imagen (2 tests nuevos). Compose validado; 97 tests del proyecto, 18 de ApiUsuario
  y 51 del orquestador en verde. **Primer build en local** (Guillermo): los cuatro contenedores
  sanos; el RAG construyó su índice dentro de la imagen (50 fragmentos, modelo e5 coincidente) y
  responde en ~2 s (pregunta sobre NO2 y asma → 4 evidencias con distancias 0,12–0,16; «capital de
  Francia» → `sin_evidencia`). **La prueba destapó un fallo que los tests no veían:** `/estaciones`
  daba 500 porque ApiUsuario y el orquestador no fijaban `sqlalchemy` y la versión nueva elige
  psycopg 3 (el mismo problema que en CI el 27-09, §10); los tests usan SQLite y no lo detectan.
  Corregido fijando `SQLAlchemy==2.0.49` y `psycopg2-binary==2.9.12`; después `/estaciones` y
  `/series` responden con datos reales. `/chat` devuelve 503 controlado: aún no hay proveedor de
  LLM elegido. Prueba de principio a fin con LLM: `[por confirmar]` |
| 2026-10-04 | **Tests de la API en CI** (Guillermo): `tests.yml` suma los jobs `api-usuario` (18 tests)
  y `orquestador` (51), cada uno con su `requirements-dev.txt`. Hasta ahora solo corrían en local
  aunque el README decía lo contrario. Reproducidos antes en entornos virtuales limpios. Como
  `desplegar.yml` reutiliza `tests.yml`, ningún despliegue sale ya sin que pasen también estos |
| 2026-10-04 | **Agente LLM, fase 1 de 7** (Carlos, rama `feature/Agente`): nuevo servicio `Agente/` independiente de `LLMOrchestrator`: FastAPI (`/salud`, `/responder`, puerto 8200), fábrica de LLM con LlamaIndex (`OpenAILike` | `BedrockConverse`), contrato de herramienta y `buscar_evidencias` como cliente HTTP de `rag.api`, bucle a mano con 3 vueltas y síntesis forzada, LLM falso con guion. 8 tests sin red (política mínima: solo los casos del plan), Dockerfile, `.env.example` y quinto job de CI «Tests del agente». Diseño completo (7 fases) anotado en §7.3.1 y §2. Sin proveedor real probado todavía |
| 2026-10-04 | **Agente LLM, fase 2 de 7** (Carlos, rama `feature/Agente`): ruta documental determinista. Síntesis JSON sin herramientas, validación con `POST /rag/validar`, una reparación y frase de insuficiencia; passthrough del texto del RAG; `fuentes` solo citadas y advertencia sanitaria solo con afirmaciones de salud; `sin_evidencia` cierra el turno sin modelo; renumeración de evidencias entre búsquedas. 11 tests. Probada contra el `rag.api` real con el LLM falso |
| 2026-10-04 | **Agente LLM, fase 3 de 7** (Carlos, rama `feature/Agente`): clasificador de intención (temperatura 0, tiempo límite propio, parser estricto, valor seguro `DESCONOCIDA`) y tabla de decisión en código: frase fija para datos, predicción y fuera de alcance; charla sin herramientas; búsqueda documental obligatoria que lanza el código si el modelo no la pide; frase fija si el RAG está caído; herramientas vetadas rechazadas. Guion de 20 preguntas etiquetadas para medir el clasificador (sin ejecutar: falta proveedor). 20 tests |
| 2026-10-04 | **Agente LLM: candidatos de Bedrock** (Carlos): se compararán Ministral 14B 3.0 y gpt-oss-120b en `eu-west-1`. Descartados Mistral Small 3.2 (no está en Bedrock), Magistral Small 1.2 (caro y lento por el razonamiento) y Mistral Large 3 (no está en la UE). Detectado que `BedrockConverse` 0.15.3 no admite el ID de Ministral 14B sin ajustar la fábrica. Sin credenciales ni pruebas contra AWS todavía |
| 2026-10-04 | **Agente LLM: primera prueba contra Bedrock** (Carlos): perfil SSO local y fábrica ajustada para Ministral 14B (metadatos declarados a mano). Los dos candidatos usan herramientas por Converse con streaming. Clasificador: primera medición 4/20 (Ministral) y 19/20 (gpt-oss) por formato (negritas, tema fuera de lista); tras ajustar el prompt y quitar adornos de markdown en el parser, **20/20 en intención y 5/5 en tema con los dos**. 21 tests. Pendiente: ruta documental de punta a punta y elección del modelo |
| 2026-10-05 | **Agente LLM: ruta documental de punta a punta en Bedrock** (Carlos): `rag.api` real y los dos candidatos, 18 turnos cada uno. Ministral 14B 18/18 con JSON válido a la primera (3,0–7,3 s). gpt-oss-120b 10 a la primera, 4 tras reparación y 4 fallidos (4,5–10,2 s): su razonamiento agota los 512 tokens de salida que `BedrockConverse` usa por defecto y corta el JSON; con 2048, 6/6. Sin cambios de código: arreglo (`LLM_MAX_TOKENS`) propuesto y comparación en igualdad pendiente. Queda abierto si las preguntas de datos (SQL) necesitarán un modelo de razonamiento |


---

## 13. Preguntas abiertas

- [ ] ¿Qué límite de gasto o créditos tiene la cuenta AWS del máster?
- [ ] Agente nuevo: ¿Ministral 14B 3.0 o gpt-oss-120b en Bedrock? Empatan en clasificador y uso de
  herramientas. Ruta documental (2026-10-05): Ministral 18/18 a la primera; gpt-oss 4 fallos de 18
  por el límite de 512 tokens de salida. Falta repetirla con `LLM_MAX_TOKENS` y medir el español.
  (§7.3.1)
- [ ] Agente nuevo: ¿necesitan las preguntas de datos (herramienta SQL) un modelo de razonamiento?
  Para la ruta documental no hace falta. Analizarlo cuando exista la herramienta SQL. (§11)
- [x] Agente nuevo: precisión del clasificador con un proveedor real → 20/20 en intención y 5/5 en
  tema con los dos candidatos de Bedrock (2026-10-04), tras corregir el formato. (§7.3.1)
- [x] Confirmar la opción de red en AWS → **opción A aplicada** desde el 2026-09-16 (RDS público +
  TLS forzado), ver §8.4.
- [x] ¿Permite la organización crear roles IAM? → sí: se creó el rol de ejecución de la Lambda
  `jupiter-pipeline` (bitácora 2026-09-16). Queda por comprobar si también se puede crear el
  **proveedor de identidad OIDC** que necesita GitHub Actions, que es un recurso distinto (ver el
  punto de CI/CD más abajo).
- [x] LLM de la Fase 3: ¿local con GPU o servicio gestionado? ¿SQL generado o consultas
  predefinidas? **Resueltas** (ver §2): LLM hospedado OpenAI-compatible (2026-09-27) y 5 consultas
  predefinidas parametrizadas (2026-09-28). Queda solo elegir el proveedor concreto.
- [x] **¿Cómo se citan los datos de SQL?** → resuelto el 2026-09-30 separando rutas: los datos
  responden en texto libre (deterministas por construcción) y lo documental pasa por el flujo
  evidencias/validar; en respuestas mixtas la cifra va dentro de la afirmación (§7.3, §2).
- [x] Reconciliar `LLMOrchestrator` con la Fase 2 reescrita → hecho el 2026-09-30: el orquestador
  adopta el contrato de evidencias en proceso (tool `buscar_evidencias` + validación en el
  cierre); el umbral y las fuentes son ya los del servicio (§7.3).
- [ ] Reindexar `data/chroma/` con e5 en cada entorno de trabajo (`python -m rag.indexar`): el
  índice construido antes de la reescritura no tiene metadatos de modelo y la búsqueda lo rechaza.
- [ ] ¿Dónde se despliega el servicio RAG en producción? (§7.2: modelo de ~1,1 GB)
- [ ] ¿Se valoró Azure Functions para la ingesta programada? La rama `feature/azure-functions`
  existe en el repositorio (ingesta a CSV), así que se llegó a probar algo, pero no queda anotado
  por qué se optó por GitHub Actions en su lugar `[por confirmar con quien la creó]` (§2).
- [x] Integrar la Fase 2: PR `feature/rag-herramienta-llm` mergeado a `development` (PR #42) y de
  ahí a `main` (hecho el 2026-09-27). La versión anterior (PR #37) queda sustituida.
- [x] Actualizar el README principal al paquete `rag` (hecho el 2026-09-27).
- [ ] Contrastar la atribución del pico de PM10 del 15-03-2022 a polvo sahariano.
- [x] **Despliegue automático desde GitHub Actions (CI/CD) — hecho el 2026-09-28** (primer
  despliegue verificado, ver bitácora; queda confirmar la ejecución programada de esa noche).
  `workflow_dispatch` con selector de rama → tests → build → push a ECR con tag por SHA →
  `update-function-code --publish` → verificación del digest. Rol `jupiter-github-actions-deploy`
  por OIDC (§8.5). Detalle operativo en `deploy/README.md`.
