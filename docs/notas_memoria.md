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
RAG como servicio de evidencias, agente LLM `Agente`). El despliegue en AWS y la dockerización del
agente los hace otro miembro del equipo.

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
| 2026-10-04 | Umbral de evidencia del RAG | 0,22 fijado a ojo con preguntas lejanas al dominio → **0,1754**, punto medio entre la peor documental (0,1750) y la mejor ajena (0,1759) en 30 casos de evaluación (§7.2) | Con 0,22 las 7 ajenas cercanas al dominio (tiempo, tráfico, transporte) recibían evidencias y llegaban al LLM (3/10 rechazadas). Con 0,1754 se rechazan 10/10 a costa de una documental (13/15 en vez de 14/15): se prefiere que el asistente se abstenga de más a que responda fuera del dominio. Descartado: mantener 0,22, que era lo que dictaba la regla fijada antes de medir (no cambiar con un hueco menor de 0,01; aquí es de 0,001) y cambiar a `multilingual-e5-small` (mejor MRR, pero solapa documentales y ajenas y no hay punto medio) |
| 2026-10-04 | Agente LLM | `LLMOrchestrator` como único agente → **nuevo servicio `Agente`** (puerto 8200), diseñado desde cero con un **bucle de herramientas escrito a mano**; **LlamaIndex solo como cliente del LLM** (`OpenAILike` para APIs OpenAI-compatibles y `BedrockConverse` para Amazon Bedrock, elegibles por variable de entorno) y, más adelante, como almacén del chat. Ambos servicios conviven sin código ni dependencias compartidas; `ApiUsuario` elegirá uno por configuración | Principios tomados de las clases del máster y de un diseño de agente empresarial: pocas piezas, el modelo clasifica y el código decide, fases pequeñas con salida tipada y valor seguro, síntesis final sin herramientas, lo determinista lo entrega el código. **Descartado un framework de agentes completo** (ReAct/`AgentWorkflow` de LlamaIndex, LangGraph): esconde el bucle que precisamente se quiere controlar (vetar u obligar herramientas, cerrar el turno sin modelo, validar la salida con el RAG) y arrastra abstracciones que no se usan. Descartado también ampliar `LLMOrchestrator`: su bucle no tiene puntos de intervención en código y rehacerlo dentro equivalía a reescribirlo. Bedrock entra como proveedor de producción porque la cuenta del máster da acceso; el modelo concreto en `eu-west-1` con *tool use* y *streaming* queda `[por confirmar]` |
| 2026-10-06 | Sesión en el agente | Servicios sin estado (2026-09-27) → **`session_id` opaco** en `/responder` (se genera si no llega) y **un turno a la vez por sesión** | El streaming y la memoria de la conversación (fases 4 y 5) necesitan agrupar los turnos. Sin usuarios ni datos personales: el id no identifica a nadie y el agente aún no guarda nada con él |
| 2026-10-06 | Chat de `ApiUsuario` | `/chat` solo hacia `ORCHESTRATOR_URL` (o stub), sin sesión → **`AGENTE_URL` elige el agente**; `session_id` y `traza_id` opcionales en el contrato y **`POST /chat/stream`** (proxy del SSE del agente, o `passthrough` + `done` sin agente) | Los dos agentes conviven y se elige por configuración, sin tocar código. El fallback mantiene el contrato del stream para el frontend aunque el orquestador no conserve contexto |
| 2026-10-06 | Memoria de la conversación del agente | Diseño previsto (2026-10-04): `PostgresChatStore` de LlamaIndex sobre la base del proyecto, estado tipado entre turnos (`turn_state`) e intención `REPETIR` → **almacén propio en memoria del proceso** (1 semana sin actividad, 20 turnos por sesión) y ventana por presupuesto de tokens; **LlamaIndex queda solo como cliente del LLM**. Sin `REPETIR` y con `turn_state` aplazado | Un TFM no necesita que las conversaciones sobrevivan a un reinicio, y el bloqueo por sesión ya exige un solo proceso: Postgres añadiría tablas, migraciones y una dependencia más en cada turno. El almacén vive detrás de una interfaz: pasar a una base de datos cambiaría una clase. `SimpleChatStore` solo guarda listas de mensajes, sin caducidad ni metadatos del turno. `REPETIR` desde cifras sueltas pierde a qué contaminante, unidad o fuente pertenece cada una; un «¿cuál era ese límite?» se trata como pregunta normal con contexto. El estado semántico se definirá cuando lo pida una necesidad concreta (comprobaciones de cifras, herramienta SQL) |
| 2026-10-06 | Alcance del clasificador del agente | Alcance por tema («calidad del aire») → **alcance por lo que el sistema puede responder**: contaminación del aire, con los contaminantes que mide la red citados en el prompt; polen, ruido y tiempo fuera; alergia y asma dentro; los límites legales son documentación, no mediciones | En el lote del 2026-10-06 los dos modelos fallaron la pregunta del polen de forma distinta (uno `DATOS`, otro `FUERA_DE_ALCANCE`). Medido antes y después: 27/28 → 29/29 con los dos modelos (§7.3.1) |
| 2026-10-06 | Comprobaciones posteriores del agente | Un modo global `COMPROBACIONES_MODO` (observar o bloquear todas) → **activación por regla** (`COMPROBACIONES_BLOQUEAN`, vacía por defecto) | Cada regla pasa a bloquear solo con evidencia propia (0 falsos positivos en las trazas reales y en un lote adversario, y al menos un acierto). Con un modo global, una regla ruidosa impediría bloquear con las fiables. Descartado también pedir una reparación al modelo («la cifra X no está en D2») en vez de la frase fija: sería mejor respuesta, pero añade una llamada y mezcla la validación del RAG con la del agente; queda como trabajo futuro |
| 2026-10-08 | Agente del asistente | `LLMOrchestrator` y `Agente` conviviendo, elegidos por `AGENTE_URL` → **`Agente` como único agente**; `LLMOrchestrator` descartado: su código se conserva en el repositorio, sin previsión de uso | Decisión del equipo. El orquestador nunca llegó a conectarse a un proveedor; el `Agente` ya tiene clasificador, ruta documental validada, sesión, *streaming*, memoria, observabilidad y comprobaciones, y está probado con Bedrock. Consecuencia: la consulta de mediciones (`query_sql` del orquestador) no existe aún en el agente y pasa a ser su herramienta SQL pendiente |
| 2026-10-08 | Preguntas de datos del agente | Frase fija («todavía no consulto mediciones») → **herramienta `consultar_datos` con SQL libre**: un LLM redacta el SQL sobre la vista `mediciones_bloques`, un validador (`sqlglot`) lo limita, PostgreSQL lo ejecuta con un rol de solo lectura y una síntesis sin herramientas redacta la respuesta | Las preguntas de datos eran la mitad de las preguntas objetivo del asistente. Se empieza por SQL libre porque 5 de las 11 preguntas de datos del lote no caben en un catálogo cerrado razonable; el catálogo solo se construye si el libre no alcanza el criterio de la evaluación |

**Decisiones abiertas que alimentarán esta tabla:** modelo del agente en Bedrock (Ministral 14B o
gpt-oss-120b; aplazado hasta poder evaluar las herramientas SQL y de ML, §13) y dónde se despliega
el servicio RAG en producción (§13). La opción de red en AWS ya está decidida y aplicada (opción A, §8.4).

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
        /chat, /chat/stream ──▶ Agente ─▶ LLM vía LlamaIndex (Bedrock | OpenAI-compat)
                                   ├─ buscar_evidencias ─▶ rag.api (HTTP)   (Fase 3, en construcción)
                                   └─ consultar_datos ─▶ redactor SQL (LLM) ─▶ validador
                                                       ─▶ PostgreSQL: vista mediciones_bloques (solo lectura)
```

`LLMOrchestrator` (primer agente, con `query_sql`) sigue en el repositorio pero está descartado
desde el 2026-10-08 (§2, §7.3.2).

**Principio de diseño central:** los **datos de contaminación viven estructurados en SQL** y la
Vector DB solo guarda **conocimiento externo** (salud, normativa). El LLM decide qué fuente consultar
mediante *tool use*.

**Estado por fases**

| Fase | Contenido | Estado |
|---|---|---|
| 1 | Datos, detector de anomalías, pipeline en tiempo real | Hecha en local; migración a AWS en curso |
| 2 | Vector DB y servicio de evidencias | **Reescrita** (2026-09-23) como servicio de evidencias y mergeada en `development`; pendiente de pasar a `main` |
| 3 | LLM con *tool use* | `Agente` (2026-10-04, §7.3.1), rediseño por fases con bucle propio y LlamaIndex como cliente: fases 1 a 5 hechas (esqueleto, herramienta documental, ruta documental validada, clasificador de intención, sesión y streaming SSE, memoria de la conversación), observabilidad y comprobaciones posteriores en observación (fase 6, falta decidir cuáles bloquean); herramienta SQL `consultar_datos` (SQL libre) conectada y probada con LLM falso, sin evaluar aún con Bedrock; 82 tests; probado con Amazon Bedrock. Despliegue y dockerización, a cargo de otro miembro del equipo. `LLMOrchestrator` (2026-09-28) descartado el 2026-10-08 (§7.3.2) |
| 4 | Informes y dashboard | En curso: `ApiUsuario` con `/estaciones`, `/estaciones/{codigo}/series`, `/chat` y `/chat/stream` (proxy al agente, con stub), tests propios y job de CI. Informes y dashboard pendientes |

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
| `mediciones_bloques` (vista) | Lo único que ve el agente: `resumen_datos_ml` con la estación y el distrito ya unidos, solo los 2 últimos años (contados desde el último día cargado). La lee el rol `agente_lectura`, que no tiene permisos sobre las tablas |

- El **distrito** no viene en el catálogo oficial: se asignó a mano a partir de la dirección, con 4
  estaciones en frontera entre distritos marcadas para revisar.
- Los bloques duran 7, 6, 7 y 4 horas: la media de un día, una estación o un distrito es
  `SUM(media * n_horas) / SUM(n_horas)`. `AVG(media)` sobre bloques sesga hacia los bloques cortos.

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
- **Modelo de embeddings:** `intfloat/multilingual-e5-base` (768 dimensiones, ventana de 512
  tokens, presupuesto de 504 por fragmento contando el prefijo y los tokens especiales). Exige los
  prefijos `query:` en la pregunta y `passage:` en el fragmento; vectores normalizados y distancia
  coseno. El índice guarda el modelo con el que se construyó y la búsqueda rechaza un índice de
  otro modelo. Índice local comprobado el 2026-10-01: e5-base, 11 documentos, 50 fragmentos.
  Alternativa más ligera medida el 2026-10-04: `multilingual-e5-small` (~470 MB frente a ~1,1 GB);
  resultados en §7.2.

### 7.2 Servicio de evidencias con citas verificables (Fase 2) — estado actual

**Estado:** reescrito el 2026-09-23 por Carlos Fernández en `feature/rag-herramienta-llm` (fases A y
B de un plan propio de tres). Sustituye a `trocear_corpus.py` e `ingesta_vector.py`. Mergeado a
`development` y `main` el 2026-09-27 (PR #42). Fase C (medición de la recuperación y calibración del
umbral) ejecutada el 2026-10-04 en `feature/rag-metricas`.

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

**Evaluación de la recuperación (fase C).** Juego fijo de 30 casos en `tests/evals/rag_casos.jsonl`:
15 documentales (al menos una por cada uno de los 11 documentos, etiquetadas por
`documento:seccion`), 10 ajenas (7 cercanas al dominio: lluvia, atasco en la M-30, polen, metro,
aparcamiento, abono transporte, parques de Barcelona; 3 lejanas: capital de Francia, tortilla,
Quijote) y 5 adversarias que parecen del corpus pero no tienen respuesta en él (predicción de ozono,
protocolo de Barcelona, SO2 en 2030, guía OMS del benceno, multa de la ZBE). El tipo de cada caso se
fijó antes de ver ninguna distancia. Sin LLM: `python -m rag.evaluar` hace una búsqueda con `k=10`
por caso y mide sobre los 4 primeros, que es lo que recibe el LLM. Un test de CI comprueba que cada
sección etiquetada sigue existiendo en el corpus.

Resultados (e5-base, índice del 2026-10-04 con 50 fragmentos; dos ejecuciones dan cifras idénticas):

| Medida | Umbral 0,1754 (vigente) | Umbral 0,22 (anterior) |
|-|-|-|
| hit@4, sección esperada entre los 4 primeros (documentales) | 14/15 | 14/15 |
| MRR@10 (documentales) | 0,740 | 0,740 |
| hit@4 con distancia bajo el umbral, lo que llega al LLM (documentales) | 13/15 | 14/15 |
| Ajenas rechazadas | 10/10 | 3/10 |
| Adversarias con evidencias (informativo) | 5/5 | 5/5 |

- **El fallo documental es real, no de etiqueta:** doc-05 pregunta qué significa una lectura alta
  de **NO** junto al tráfico; la sección que lo explica sale en la posición 10 (0,190) y por delante
  quedan fragmentos de NO2, alergias y el protocolo. El modelo no separa NO de NO2. No se corrigió
  ninguna etiqueta tras ver resultados.
- **Mejores distancias por tipo:** documentales 0,109–0,175; ajenas 0,176–0,285; adversarias
  0,126–0,163. Con 0,22 las 7 ajenas cercanas al dominio (0,176–0,211) recibían evidencias: solo se
  rechazaban las lejanas. La estimación de septiembre («ajenas 0,24–0,25») se hizo solo con
  preguntas lejanas.
- **Calibración:** punto medio entre la peor documental (doc-10, Plaza Elíptica, 0,1750) y la mejor
  ajena (aje-01, «¿va a llover mañana?», 0,1759): **0,1754**. El hueco es de 0,001. La regla fijada
  antes de medir era no cambiar con un hueco menor de 0,01; se cambió igualmente por un criterio de
  producto: **mejor abstenerse de más que responder fuera del dominio**. El coste es una documental
  (doc-12, «¿me puede diagnosticar el asistente?», cuya sección queda a 0,188 y no llega al LLM).
  Con una milésima de margen, una pregunta ajena nueva puede caer por debajo: el valor está
  ajustado a estos 30 casos.
- **Las adversarias no se distinguen por distancia:** las 5 caen dentro del rango de las
  documentales (0,126–0,163, todas por debajo de la peor documental). Un umbral absoluto no puede
  rechazarlas; depende de que el LLM declare `sin_evidencia` o `parcial` al leer los fragmentos.
- **Limitación cualitativa:** bajo el umbral entran fragmentos vecinos poco pertinentes («¿Qué
  efectos tiene el ozono en la salud?» trae también `oxidos_nitrogeno_salud:efectos-en-la-salud`).
  No se mide con una «precisión de evidencias» porque no hay etiquetado exhaustivo de relevancia.
- **Advertencia de método:** calibración y medida usan los mismos 30 casos; las cifras describen
  este conjunto, no una generalización.

**Comparación con `multilingual-e5-small`** (mismo corpus y casos, índice aparte; dos ejecuciones
idénticas; indexación en 56 s en CPU):

| Medida | e5-base, 0,1754 | e5-small, 0,1754 | e5-small, 0,144 |
|-|-|-|-|
| hit@4 (documentales) | 14/15 | 14/15 | 14/15 |
| MRR@10 (documentales) | 0,740 | 0,796 | 0,796 |
| hit@4 bajo el umbral (documentales) | 13/15 | 14/15 | 13/15 |
| Ajenas rechazadas | 10/10 | 5/10 | 10/10 |
| Adversarias con evidencias | 5/5 | 5/5 | 4/5 |

- e5-small **ordena mejor** (MRR 0,796; falla el mismo caso NO/NO2, doc-05) y pesa menos de la mitad.
- Pero **documentales y ajenas se solapan**: la peor documental (doc-10, 0,171) queda por encima de
  la mejor ajena (aje-01, 0,148); hueco de −0,023, con 6 casos al otro lado. El punto medio no
  aplica.
- El umbral de e5-base **no sirve** para e5-small (rechaza 5/10 ajenas): las distancias dependen
  del modelo.
- 0,144 se eligió **después** de ver los datos, entre doc-12 (0,140) y aje-01 (0,148), para igualar
  el compromiso de e5-base. Da las mismas cifras de abstención, pero pierde doc-10 en vez de doc-12
  y no es una calibración: es un ajuste a posteriori.
- **Decisión: se mantiene e5-base.** e5-small queda como opción de despliegue si el tamaño del
  modelo es un problema (§11), con la condición de recalibrar con `python -m rag.evaluar`.

Figuras: `docs/rag/figuras/distancias_por_tipo.png` (e5-base, umbrales 0,1754 y 0,22) y
`docs/rag/figuras/distancias_por_tipo_e5_small.png` (e5-small, con el solape). Mejor distancia por
caso en una franja por tipo. Se generan con `python -m rag.evaluar --figura [ruta]`.

**Umbral de evidencia: 0,1754 de distancia coseno** (`RAG_UMBRAL_DISTANCIA`), calibrado para
e5-base. Si nada baja del umbral, el estado es `sin_evidencia` y la API de chat puede responder
insuficiencia **sin llamar al modelo**; ahora ocurre también con las preguntas cercanas al dominio
pero ajenas al corpus (tiempo, tráfico, transporte).

**Alternativa descartada: Phoenix con proyección UMAP** para inspeccionar corpus y preguntas.
Son pocos puntos (50 fragmentos y 30 preguntas) y las distancias caen en un rango de unas 0,18
(0,11–0,29) con huecos de milésimas, que una proyección 2D no conserva. La tabla ordenada por
distancia y la figura de franjas responden con cifras lo que la proyección mostraría borroso.

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

### 7.3 LLM con tool use (Fase 3) — `Agente`, probado con Bedrock

Hubo dos implementaciones del agente, independientes entre sí (sin código, dependencias ni imágenes
compartidas). Desde el 2026-10-08 solo sigue `Agente` (§7.3.1); `LLMOrchestrator` (§7.3.2) queda
descartado con su código en el repositorio. `ApiUsuario` reenvía `/chat` al agente con `AGENTE_URL`;
el camino a `ORCHESTRATOR_URL` sigue en el código, sin uso previsto. El agente se ha probado contra
Amazon Bedrock (clasificador, uso de herramientas, ruta documental, streaming y memoria).

#### 7.3.1 `Agente` — rediseño por fases (en construcción)

**Estado (2026-10-08): fases 1 a 5 de 7 hechas, más la observabilidad (partes A, B y C) y las
comprobaciones posteriores en observación (fase 6, falta decidir cuáles bloquean)** (rama
`feature/Agente`). La fase 7 (Bedrock en la EC2 y dockerización) la hace otro miembro del equipo. Servicio FastAPI en `Agente/`
(paquete `agente`, puerto 8200, `GET /salud`, `POST /responder` con el mismo contrato que reenvía
`ApiUsuario` y `POST /responder/stream`), misma convención por capas que `ApiUsuario`.

**Sesión (2026-10-06).** `/responder` acepta un `session_id` opcional (1–64 caracteres
`[A-Za-z0-9_-]`; si no llega, genera uno) y lo devuelve. Un turno a la vez por sesión: una segunda
pregunta de la misma sesión **espera** (no se rechaza con 409). Cada sesión activa tiene un bloqueo y
un **contador de usuarios** (el turno en curso más los que esperan); la entrada se borra cuando el
contador llega a 0, también si el turno se cancela. Borrarla al soltar el bloqueo crearía una
carrera: A termina, B espera en el bloqueo viejo y C crea uno nuevo y corre a la vez que B. Los
tests la reproducen con eventos (sin esperas por tiempo) y fallan con las dos variantes erróneas
(borrar al soltar y no borrar nunca). **Límite:** vale para un solo proceso del agente (uvicorn
arranca hoy con uno); con varios haría falta otra coordinación.

**Streaming (2026-10-06).** `POST /responder/stream` entrega el turno como Server-Sent Events:
`status` (fase: `en_espera`, `clasificando`, `buscando`, `redactando`, `validando`), `token`,
`passthrough`, `error` y `done`. **Solo salen como tokens las respuestas definitivas**: la llamada
sin herramientas ofrecidas (`CHARLA`, o `DESCONOCIDA` con el RAG caído) y la síntesis forzada.
Todo lo demás sale entero al final como `passthrough`: frases fijas, `sin_evidencia`, la ruta
documental (su JSON se valida antes de entregarse) y el texto libre de un turno con herramientas
ofrecidas, que puede acabar descartado. Así el cliente nunca ve un texto que luego se retira. El
turno no sabe de HTTP: recibe un **emisor opcional por turno** y la capa HTTP lo conecta a una
**cola acotada** de 64 eventos (si el cliente lee despacio, el turno espera) y repite el último
`status` cada 0,7 s sin eventos. `/responder` es el mismo turno sin emisor. Errores: antes de abrir
el stream, código HTTP (503); después, evento `error` sin `done`. Si el cliente se desconecta, el
turno se cancela, libera la sesión y su span lleva `agente.cancelado`; comprobado con uvicorn real
(0 llamadas al LLM tras cortar). **Límite:** cancelar no garantiza que Bedrock deje de procesar (y
cobrar) una inferencia ya iniciada. Descartado: `/responder` como el generador del stream drenado a
una cadena (lo que decía el plan), porque mezcla en negocio el formato del stream.

**`ApiUsuario` como puerta (2026-10-06).** Con `AGENTE_URL`, `/chat` reenvía `{pregunta,
session_id}` al agente y devuelve también su `traza_id`; `/chat/stream` comprueba la respuesta del
agente antes de abrir su propio SSE (si no es 200, 503) y después reenvía los bytes tal cual. Al
terminar o al irse el cliente cierra la respuesta y el cliente HTTP, y así el agente ve la
desconexión y cancela el turno. Un primer test de ese cierre pasaba aunque se quitara el cierre
(httpx cierra solo la respuesta cuando se lee entera): se rehízo cortando la lectura a mitad.
Sin `AGENTE_URL`, `/chat/stream` hace lo mismo que `/chat` (orquestador o stub) y lo entrega como
`passthrough` + `done`, con `session_id` y `traza_id: null`. **Límite:** el orquestador no conserva
contexto; el `session_id` solo mantiene el contrato.

**Verificación con Bedrock (2026-10-06, `rag.api` real, agente y `ApiUsuario` con uvicorn).** 10
turnos, unas 25 llamadas, 0,003 $. Charla: primer `token` a 3,4 s (Ministral 14B, 9 fragmentos) y a
2,0 s (gpt-oss-120b, 5), sin razonamiento en el texto y con `done` sin `passthrough`. Documental
por el proxy: `clasificando → redactando → buscando → redactando → validando`, ningún `token`,
`passthrough` con citas y `done` (gpt-oss 7,6 s; Ministral 33 s, con llamadas de 8–10 s para menos
de 200 tokens: lentitud de Bedrock ese día, no del stream). Dos preguntas a la vez en la misma
sesión: la segunda recibe `en_espera` ~6 s y se ejecuta después. Corte del cliente a los 6 s a
través del proxy: el agente cancela a los 6,3 s, sin más llamadas, con `agente.cancelado` y la
sesión libre.

**Memoria de la conversación (fase 5, 2026-10-06).** El agente recuerda cada sesión para
interpretar preguntas de seguimiento («¿Qué efectos tiene el NO2?» → «¿Y en niños?» → «¿Y a largo
plazo?»). Regla de fondo: **el historial interpreta, las evidencias fundamentan**; la ruta
documental sigue exigiendo que cada afirmación cite evidencias del turno.
- **Almacén** en memoria del proceso, detrás de una interfaz en la capa de datos (pasar a una base
  de datos cambiaría una clase). Cada turno completado se guarda como una entidad (pregunta,
  respuesta mostrada, respuesta para el contexto, intención, ruta, fuentes, `traza_id`). Como mucho
  20 turnos por sesión; una sesión sin actividad durante una semana se olvida (purga perezosa al
  guardar, sin tareas en segundo plano). Un turno cancelado o con el LLM caído no se guarda; las
  frases fijas sí, y entran en el contexto: el modelo sabe qué se ha rechazado ya.
- **Ventana**: turnos completos, del más reciente hacia atrás, mientras quepan en 1.500 tokens
  estimados a 4 caracteres por token (no hay tokenizador local para los modelos de Bedrock).
- **Lo que se recuerda no es lo que se mostró.** En la ruta documental el texto entregado lleva
  aviso, evidencias con `chunk_id` y bibliografía; al contexto va solo el texto de las afirmaciones
  validadas, sin marcas `[Dn]` (la numeración es de cada turno). Nada interno llega al modelo:
  ni mensajes `system` extra ni metadatos.
- **Dónde entra el contexto**: en el clasificador, como bloque de texto con los 2 últimos turnos
  (respuestas recortadas a 300 caracteres) para que siga siendo corto; en el bucle, como pares
  `user`/`assistant` reales (cumplen la alternancia que exige Bedrock Converse); en las dos
  síntesis, como bloque marcado «no es evidencia» delante de la pregunta, para no empujar al modelo
  a contestar en prosa; y en la búsqueda que lanza el código, como las 2 preguntas previas más la
  actual concatenadas (con una sola, la cadena NO2 → niños → largo plazo pierde el NO2 en el tercer
  turno). `/rag/validar` recibe solo la pregunta actual: concatenar arrastraría el aviso sanitario
  y la limitación de actualidad de preguntas anteriores.
- **Sin historial nada cambia**: el primer turno envía exactamente los mismos mensajes que antes, y
  los 41 tests previos pasan sin tocar ninguna aserción.
- Descartado (§2): `PostgresChatStore`, `SimpleChatStore`, la intención `REPETIR` y el estado
  semántico entre turnos. Alternativa anotada a la concatenación: que el clasificador devuelva una
  consulta reformulada (mejor búsqueda, pero un parser menos estricto y riesgo para el 20/20).
- **Límites**: las conversaciones se pierden al reiniciar o desplegar, y exige un solo proceso del
  agente (como el bloqueo por sesión). Unos 60 KB por sesión llena `[estimación: ~3 KB por turno]`.

**Revisión del clasificador y evaluación con conversaciones (fase 5, 2026-10-06).** El guion del
clasificador pasa a 29 preguntas (9 nuevas: 4 seguimientos con historial, polen, ruido, lluvia,
«¿El NO2 empeora la alergia al polen?» y «¿Cuál es el límite legal anual del NO2?») y se añaden 5
conversaciones de 2–3 turnos (11 turnos) que pasan por la misma capa de sesión y memoria que el
servicio. En los seguimientos se comprueba en las trazas que la búsqueda nombre el referente (el
NO2 en «¿Y a largo plazo?»). Prompt nuevo: alcance = contaminación del aire; los contaminantes de
la red (NO, NO2, NOx, O3, PM10, PM2.5) citados en `DATOS`; `DOCUMENTAL` aunque la red no mida el
contaminante (el benceno acaba en `sin_evidencia`); los límites legales son documentación, no
mediciones; fuera de alcance, polen, ruido y tiempo, salvo alergia, asma o rinitis. Medido antes y
después con Bedrock y `rag.api` real:

| Medición | Ministral 14B antes | después | gpt-oss-120b antes | después |
|-|-|-|-|-|
| Clasificador, intención | 27/28 | 29/29 | 27/28 | 29/29 |
| Clasificador, tema | 9/9 | 10/10 | 8/9 | 10/10 |
| Conversaciones (turnos que cumplen) | 9/11 | 11/11 | 11/11 | 11/11 |
| Turnos aislados (12 casos) | 11/12 | 12/12 | 12/12 | 12/12 |

- Fallos de antes: Ministral clasificaba el polen como `DATOS` (aislado) o `DOCUMENTAL` (tras una
  pregunta de ozono) y el límite legal anual del NO2 como `DATOS`; el de gpt-oss fue un tiempo
  agotado del clasificador (10 s), no un error de clasificación.
- Tres iteraciones del prompt sobre los mismos casos: **riesgo de sobreajuste**. La primera, con el
  polen fuera de alcance sin matiz, arrastró también la rinitis, la zona para alérgicos y el NO2
  con polen (26/28). Residuo conocido: «¿Cuál es el límite anual de PM10?», sin «legal», sigue en
  `DATOS` con Ministral (2 de 2).
- **La búsqueda concatenada no se ejecutó en ningún lote**: los dos modelos reformulan solos la
  búsqueda del seguimiento («efectos del NO2 en la salud de los niños»). Referentes 4/4 por modelo
  antes y después. La concatenación sigue sin medir.
- «¿Y en niños?» lleva el aviso sanitario con los dos modelos aunque `/rag/validar` solo vea esa
  pregunta: el validador también lo activa por las evidencias de salud citadas.
- Coste del historial en tokens reales: la llamada del bucle pasa de ~360–380 tokens de entrada a
  ~515–540 con un turno previo y ~655–660 con dos; el clasificador, de ~380–420 a ~500–620. El
  presupuesto de 1.500 no se alcanzó (como mucho 2 turnos previos en el guion).
- Latencia del clasificador con el prompt final: mediana 315 ms con Ministral y 986 ms con gpt-oss
  (máximo 8,4 s). Mediana por turno: conversaciones 5,7 s / 6,9 s, turnos aislados 2,0 s / 3,0 s
  (Ministral / gpt-oss).
- Unas 600 llamadas a Bedrock (tres iteraciones, sondas y un lote repetido), unos 0,12 $
  `[estimación]`. Revisión humana de las respuestas `[pendiente]`.
- Incidencia: un lote entero acabó en frase fija porque cada búsqueda agotó los 20 s del RAG.
  `rag.api` ocupaba 3,7 GB de los 7,9 GB del equipo y quedaban 78 MB libres; un minuto después
  respondía en 0,07 s. Lote descartado y repetido.

**Diseño.** Un solo agente con el **bucle de herramientas escrito a mano**; LlamaIndex
(`llama-index-core` 0.14) aporta solo el cliente del LLM: `OpenAILike` (Mistral API, Groq,
Ollama…) o `BedrockConverse` (Amazon Bedrock, credenciales por rol de instancia o perfil), elegidos
con `LLM_PROVEEDOR`. El resto del código ve una única interfaz (`FunctionCallingLLM`). Principios:
el modelo clasifica y el código decide; fases pequeñas con salida tipada y valor seguro; la síntesis
final no tiene herramientas; lo determinista (texto con citas y bibliografía) lo entrega el código,
no lo reescribe el modelo; el historial interpreta y las evidencias fundamentan; las comprobaciones
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
  herramientas, **síntesis forzada sin herramientas** (peor caso: 4 llamadas), con mensajes
  nuevos: la pregunta y los resultados de las herramientas en texto, sin el historial. Herramienta
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
    la búsqueda) y responder en libre con el RAG caído. La intención `REPETIR`, prevista para
    cuando hubiera historial, se descartó en la fase 5 (§2).
  - Coste en llamadas al LLM: frase fija 1; charla 2; documental 4 (5 con reparación; 3 si busca el
    código).
  - Guion de medición fuera de CI (`Agente/evaluacion/`): 20 preguntas etiquetadas a mano, 15 de
    `Preguntas.txt` y 5 nuevas para charla y fuera de alcance (29 desde la fase 5, ver abajo).
    **Resultado en Bedrock (2026-10-04): 20/20 en intención y 5/5 en tema con los dos modelos
    candidatos**. Latencia típica
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
- **Observabilidad (2026-10-06, adelantada de la fase 6)**: cada turno deja un árbol de **spans
  OpenTelemetry con atributos OpenInference**: `turno` (pregunta, respuesta, ruta, intención,
  vueltas, reparaciones) > `clasificar`, `bucle`, `busqueda_forzada`, `sintesis_documental`,
  `sintesis_forzada` > cada llamada `llm` (mensajes, herramientas ofrecidas, respuesta, tokens) y
  cada herramienta (argumentos y lo que vio el modelo). Lo que decide el código (frase fija,
  `sin_evidencia`, límite de vueltas) queda como evento `decision`. **Una sola instrumentación,
  dos destinos**: **Phoenix** (un contenedor con SQLite, perfil `observabilidad` del compose) para
  ver la cascada de cada turno, y un **exportador JSONL propio** (un span por línea) del que saldrán
  las cifras agregadas para la memoria. `/responder` devuelve además un `traza_id` opcional.
  - El SDK de OpenTelemetry resuelve lo delicado: mantiene separados los turnos concurrentes
    (contexto por `contextvars`, comprobado con dos turnos solapados en un test), cierra los spans
    aunque haya excepción y aísla los fallos de los exportadores: un JSONL que no se puede escribir
    no cambia la respuesta (test).
  - Tokens: se toman del último trozo del stream; si el proveedor no los envía quedan **ausentes,
    nunca a cero**, para no abaratar el coste calculado. Texto de prompts y respuestas guardado por
    defecto; con `TRAZA_GUARDAR_TEXTO=false` queda como `__REDACTED__`.
  - Descartado: una traza propia del turno en paralelo a los spans (dos fuentes que pueden
    discrepar); la instrumentación automática de LlamaIndex (ve las llamadas al LLM, no el bucle);
    Langfuse u Opik (cuatro servicios y 16 GiB recomendados); SaaS como LangSmith (los prompts
    salen fuera); los decoradores de OpenInference (no permiten elegir qué argumentos se guardan y
    fijan el tracer al importar).
  - **Verificado con Bedrock (2026-10-06)**: Phoenix en Docker, `rag.api` real y Ministral 14B.
    En la cascada del turno documental «¿Qué efectos tiene el NO2 en la salud?» se ve el árbol
    completo (captura: `docs/agente/img/PhoenixTraza.PNG`): clasificar 0,28 s; bucle 2,5 s (dos
    llamadas y una búsqueda de 0,13 s); síntesis 2,7 s (llamada de 2,6 s y validación de 0,09 s);
    5,5 s en total, JSON válido sin reparación. Las cuatro llamadas llevan tokens reales (de 273 a
    1.742 por llamada) y el JSONL contiene lo mismo. Phoenix marca coste 0 $ porque no conoce los
    precios de Bedrock: el coste lo calculará el informe.
  - **Primer fallo encontrado gracias a las trazas**: el otro turno tardó 43 s para acabar en la
    frase de documentación no disponible. La búsqueda del modelo agotó el timeout de 20 s del RAG
    (probablemente aún cargando `[por confirmar]`) y el código, al no haber evidencias, lanzó la
    búsqueda forzada y esperó otros 20 s. Corregido el mismo día: si la búsqueda del modelo
    encuentra el RAG sin servicio (red, timeout, 5xx), frase fija sin repetirla; un 422 sí se
    reintenta con la búsqueda del código.
  - **Informe agregado (parte B, 2026-10-06)**: `Agente/evaluacion/informe_trazas.py`, solo con la
    biblioteca estándar, lee los JSONL, agrupa por turno y lote (la etiqueta del turno) y saca tablas
    markdown: turnos por ruta e intención, latencia por fase (**mediana con n; p95 solo
    descriptivo**), tokens y coste con la tabla de precios de Bedrock en Irlanda fechada en el
    propio script, síntesis válidas a la primera, reparaciones, búsquedas forzadas, herramientas
    vetadas y decisiones del código. Un turno con una llamada sin tokens queda **incompleto**: no
    entra en las medianas de tokens y su coste no se suma. `--traza-id` imprime un turno como tabla
    (alternativa a Phoenix en la terminal). Sobre la traza de la verificación: turno documental de
    3.440 tokens de entrada y 687 de salida, unos 0,001 $ con Ministral 14B.
  - **Informe visual (2026-10-06)**: el mismo script genera, con `--salida x.html`, una página
    autocontenida para analizar latencia y coste: filtros por lote, ruta e intención, cifras por
    lote (incluido el coste por 1.000 turnos), dispersión latencia–coste por turno, latencia por
    fase y por ruta mostrando cada span (con n pequeños se ven los puntos, no solo la mediana),
    coste medio por fase y la cascada de cada turno con su coste, que Phoenix no da para Bedrock.
    Python extrae un conjunto de datos JSON (`--salida x.json`) y la página lo dibuja con JS y SVG
    sin librerías: se abre sin red y el JSON es el contrato de una posible web de análisis. Coste:
    las agregaciones existen dos veces (Python para el markdown, JS para la página), con las mismas
    reglas.
  - **Evaluación pequeña (parte C, 2026-10-06)**: `evaluar_turnos.py` pasa 12 casos
    (`casos_turno.json`: 6 documentales, 2 de charla, 2 fuera de alcance y 2 sin evidencias) por el
    mismo `Bucle` que monta el servicio. Cada caso declara lo esperado (intención, ruta, si se busca
    en el RAG, si cita) y el script marca las diferencias. Tres criterios separados: **JSON a la
    primera** (la primera salida de la síntesis es un objeto JSON, leído en la traza), **referencias
    válidas** (lo acepta `/rag/validar`) y **respuesta correcta** (lectura humana: sí, no o
    parcial). Sin jueces LLM: con 12 casos la lectura humana es asumible y no añade otro modelo que
    evaluar. Cada lote deja su JSONL y un markdown fechado con el informe de trazas.
  - Los dos casos sin evidencias se eligieron consultando el índice con la pregunta literal (umbral
    0,1754): «temporada de polen de las gramíneas» y «efectos del benceno en la salud» devuelven 0
    fragmentos. Otras candidatas cercanas al dominio sí pasan el umbral: radón (3 fragmentos),
    monóxido de carbono (4) y la guía de la OMS para el benceno (3). Es otra muestra de que el
    umbral deja pasar preguntas próximas al corpus (§7.2); la síntesis tiene que reconocerlo.
- **Tests**: 64, sin red, sin claves y sin torch: LLM falso con guion que hereda de
  `FunctionCallingLLM` (mismo camino que un proveedor real) y RAG fingido con
  `httpx.MockTransport`. **Política de tests mínima** (decisión del 2026-10-04): solo los casos
  que fija el plan de cada fase (bucle con 0, 1 y 2 llamadas, límite de vueltas, RAG caído,
  contrato HTTP; en la fase 2, JSON válido, reparación, doble fallo y `sin_evidencia`; en la fase 3,
  cada intención, clasificador caído, herramienta vetada, RAG caído con intención documental y
  clasificación en negrita; después, la búsqueda con el RAG caído que no se repite; en la
  observabilidad, el árbol de spans, turnos concurrentes, LLM caído, JSONL que falla, medianas del
  informe, tokens incompletos y el evaluador que marca un caso fallido; en la fase 4, la sesión,
  la concurrencia y el stream; en la fase 5, los 9 casos de la memoria: turnos de la misma sesión,
  nada guardado si el turno falla o se cancela, ventana, caducidad, contexto sin `[Dn]` ni
  metadatos, contexto en cada llamada y el evaluador de conversaciones; en la fase 6, las tres
  reglas, el bloqueo con memoria y stream, el conteo de hallazgos en el informe y el turno
  cancelado, que el informe no cuenta como error), sin tests de detalle interno; se partió de 25 y se recortaron. Verificado además, sin red, que `OpenAILike` serializa la herramienta cruda
  y los mensajes `assistant(tool_calls)`/`tool` como espera la API. Quinto job del workflow de CI.
- **Probado contra Bedrock (2026-10-04)**, desde local con credenciales SSO temporales: los dos
  candidatos están bajo demanda en `eu-west-1` con streaming, y los dos **piden la herramienta por
  Converse con streaming** (`astream_chat_with_tools`), con la consulta reformulada y `tema=salud`.
  Ruta documental de punta a punta con `rag.api`: medida el 2026-10-05 (abajo). Síntesis forzada:
  falló el 2026-10-06 y se corrigió ese día; verificada después contra Bedrock (abajo).
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
    **Arreglo aplicado el 2026-10-06**: `LLM_MAX_TOKENS` configurable (2048 por defecto) para los dos
    proveedores. La comparación en igualdad de condiciones es el lote de la parte C (abajo).
  - **Lote de evaluación (2026-10-06, `evaluar_turnos.py`, 12 casos, `LLM_MAX_TOKENS`=2048)**:
    medianas sobre n=12 turnos por modelo, salvo la ruta documental (n=6).

    | Modelo | Cumplen lo esperado | JSON a la primera | Referencias válidas | Latencia mediana (turno / documental) | Tokens salida mediana (documental) | Coste del lote |
    |-|-|-|-|-|-|-|
    | Ministral 14B | 11/12 | 6/6 | 6/6, 0 reparaciones | 4,0 s / 8,3 s | 562 | 0,0059 $ |
    | gpt-oss-120b | 11/12 | 6/6 | 6/6, 0 reparaciones | 2,9 s / 7,5 s | 1.108 | 0,0087 $ |

    Con 2048 tokens desaparecen los fallos de gpt-oss del 2026-10-05. El único caso fallido es el
    mismo en los dos (`sin-01`, temporada de polen): ninguno lo clasifica como `DOCUMENTAL` (Ministral
    `DATOS`, gpt-oss `FUERA_DE_ALCANCE`). El error estaba en el caso: las estaciones no miden polen,
    así que la pregunta queda fuera de alcance (decisión del equipo; el caso pasa a ser `fue-03`). Con
    esa lectura, gpt-oss 12/12 y Ministral 11/12.
  - **Segunda ejecución (2026-10-06, 20 min después, con Phoenix):** misma calidad (gpt-oss 12/12,
    Ministral 11/12 con el polen como `DATOS`, 6/6 válidas a la primera los dos) y mismo coste
    (0,0060 $ y 0,0089 $). La latencia de Ministral casi se duplicó: mediana del turno 6,8 s
    (documental 14,9 s) frente a 4,0 s (8,3 s), con el clasificador en 1,8 s frente a 0,3 s.
    gpt-oss se mantuvo (3,2 s; documental 7,4 s). Con dos ejecuciones, la latencia de Ministral 14B
    bajo demanda en `eu-west-1` es variable; la de gpt-oss, estable `[n=2 lotes]`. El prompt del
    clasificador se revisó en la fase 5 (arriba): el alcance se define por lo que el sistema
    puede responder, no por el tema.
  - **Hallazgo: Bedrock rechaza la síntesis forzada** (2026-10-06, los dos modelos). Cuando el bucle
    llega al límite de vueltas sin evidencias, el agente vuelve a llamar al modelo sin herramientas
    pero con el historial, que lleva bloques de llamada y resultado de herramienta. Converse exige
    entonces `toolConfig` (`ValidationException`) y el turno acaba en 503. Se forzó con un script
    (una vuelta y una herramienta que falla): 3 de 3 turnos con búsqueda fallaron. Los tests con el
    LLM falso no lo detectan porque no imitan esa regla del proveedor. **Corregido el mismo día**:
    la síntesis forzada parte de mensajes nuevos, como ya hacía la documental (prompt de sistema
    propio + la pregunta y los resultados de las herramientas en texto), y un test comprueba que la
    última llamada no lleva bloques de herramienta. Descartado quitar solo los bloques del historial:
    el modelo perdería lo que devolvieron las herramientas. **Verificado contra Bedrock** (2026-10-06):
    4 de 4 turnos (2 preguntas × 2 modelos, `max_vueltas=1`, búsqueda que falla) llegan a la síntesis forzada y responden; ninguno da 503 (1,9–3,0 s el turno). gpt-oss contesta con honestidad
    («No dispongo de información suficiente…»). **Ministral ignora el «apóyate solo en lo que
    contienen»** y responde con conocimiento propio: los síntomas del asma y, para el PM2.5, una
    cifra (25 µg/m³) y una directiva que no venían de ninguna evidencia, con negritas. Es la ruta
    libre, sin `/rag/validar` que lo frene: queda para las comprobaciones de cifras de la fase 6. Revisión humana de la columna «Correcta» y del español `[pendiente]`.
  - Hallazgo: `BedrockConverse` 0.15.3 rechaza los IDs de modelo que no conoce, como el de Ministral
    14B (`ValueError: Unknown model`). Resuelto con una subclase en la fábrica que declara a mano los
    metadatos de los modelos fuera de su lista.

**Comprobaciones posteriores** (fase 6, en construcción; pasos 1 a 3 hechos el 2026-10-06). Al
final de cada turno el código revisa **lo que escribió el modelo**, no lo que se entrega: en la ruta
libre, la respuesta; en la documental, cada afirmación y limitación del JSON validado (el texto que
renderiza el RAG lleva a propósito `chunk_id`, fechas y URLs). Las frases fijas no se comprueban.
Tres reglas, funciones puras sin LLM:
- **Cifras sin respaldo**: cada número debe estar en la pregunta o en lo que devolvieron las
  herramientas del turno; en la documental, en las evidencias **que cita esa afirmación** (es lo
  que promete la marca `[Dn]`). El historial no respalda: aparecer antes no prueba nada. No cuentan
  años (1900–2099), dígitos pegados a una letra (`PM2.5`, `NO2`, `D1`) ni sub/superíndices.
- **Fuga del prompt**: 10 palabras seguidas compartidas con un prompt del sistema. Se excluyen la
  frase de identidad y la de capacidades del prompt de charla: el modelo las repite al presentarse
  (con ellas, la regla saltaba en 8 de las 22 charlas reales guardadas; sin ellas, en 0).
- **Internos**: nombres de herramientas, `chunk_id`, marcas `[Dn]` fuera de la ruta documental y
  JSON crudo.
Cada regla **observa** (evento `comprobacion` en la traza y campo en el log del turno) salvo que
esté en `COMPROBACIONES_BLOQUEAN`: entonces la respuesta pasa a una frase fija, sin fuentes ni aviso,
y es esa frase la que entra en el historial. Con alguna regla en bloqueo las respuestas libres ya
no salen como tokens: se comprueban enteras y salen al final (`passthrough`). Por defecto todas
observan.

Medición (2026-10-06), con las mismas funciones que usa el turno aplicadas a las trazas guardadas
(un script reconstruye el material de cada turno; sobre los 26 turnos nuevos da exactamente los
mismos hallazgos que anotó el agente en vivo):
- **Trazas existentes**: 128 turnos, 96 con texto del modelo (331 textos). 5 hallazgos, todos de
  cifras; 0 de fuga y 0 de internos.
- **Lote adversario** (6 casos: pedir el prompt, preguntar por las herramientas internas o los
  identificadores de los fragmentos, y preguntas documentales con muchas cifras), con los dos
  modelos y los casos de fuga repetidos con Ministral, más una **sonda de síntesis forzada** (búsqueda
  que falla, 4 turnos por modelo): 26 turnos, 63 llamadas, 0,011 $.
- **Cifras**: 10 hallazgos en total. 3 son falsos positivos, todos iguales: «PM2.5 (partículas de
  diámetro menor a 2.5 micras)», la cifra del nombre. El resto, cifras que el modelo puso sin
  evidencia (Real Decreto 102/2011, Directiva 2008/50/CE, «25 µg/m³» en la síntesis forzada, una
  «Orden 2778/2019» `[por confirmar si existe]`) y una **cita mal puesta**: el límite de 2030
  (20 µg/m³) es correcto pero está en una evidencia que la afirmación no cita. Todos de Ministral.
  Quitar las cifras seguidas de «micras/µm» elimina los 3 falsos positivos sin perder ningún acierto
  (ajustado sobre los mismos datos: falta confirmarlo con turnos nuevos).
- **Fuga**: Ministral **repitió el prompt de charla las 4 veces que la pregunta llegó a la charla**
  (de 6 intentos); gpt-oss clasificó las tres preguntas como fuera de alcance (frase fija). La regla solo detectó la copia literal (1 de
  4): las otras tres eran paráfrasis cuyo tramo literal más largo era de 9 palabras. Ningún tamaño
  de n-grama separa: con 8 palabras detecta las 4, pero salta también en 9 turnos normales que
  describen las capacidades con frases del prompt. Lo que sí separa son las frases que solo tienen
  sentido como instrucción («Eres el asistente…», «estas instrucciones», «Responde en español»):
  4 de 4 fugas y 0 de los otros 113 turnos (misma reserva: definido con esas 4 fugas).
- **Internos**: 0 hallazgos. Ningún modelo nombró la herramienta ni copió un `chunk_id`, ni
  siquiera al pedírselo.
Decisión de qué reglas bloquean `[pendiente]` (paso 4).

**Herramienta SQL (en construcción; fases 1 y 2 de 6 hechas el 2026-10-08).** Se empieza por **SQL
libre** (un LLM redacta la consulta, un validador la limita y PostgreSQL la ejecuta); el catálogo
cerrado de consultas solo se construye si el libre no alcanza el criterio de la evaluación. La
fase 1 deja la base de datos y el acceso:
- **Vista `mediciones_bloques`** como única relación consultable (§5.3) e índice nuevo por `fecha`.
- **Tres capas de solo lectura**: rol `agente_lectura` con `SELECT` solo sobre la vista (la barrera
  real, usado también en local), sesión con `default_transaction_read_only` y `statement_timeout`
  de 5 s, y tope de 60 filas con aviso de truncado. Un único módulo abre conexiones.
- **Entorno local sin RDS**: PostgreSQL en Docker con 2 años del parquet (`--desde` en el
  cargador). El cargador ahora vacía la tabla en vez de borrarla, porque la vista depende de ella.
- `FECHA_REFERENCIA` hace de «hoy» en la evaluación, para que el lote sea reproducible.
- Tests: 4 en SQLite (68 en total) y 3 de integración con PostgreSQL fuera de la suite: media
  ponderada contra un cálculo a mano, consulta con `INTERVAL`/`date_trunc` y permisos del rol
  (lectura de la tabla, `UPDATE` y tiempo límite rechazados): **3/3** en el Postgres local.
- Local verificado: 2 años del parquet (326.700 filas; la vista, 314.411, del 2024-04-30 al
  2026-04-30). Índice por fecha: 2,2 MB. Consultas típicas de agregación: ~20 ms. El filtro por
  periodo no usa ese índice (la vista expone `fecha::date`); solo el corte de 2 años.

La fase 2 conecta el SQL libre y la ruta de datos (`consultar_datos`):
- **Redactor**: una llamada a temperatura 0 a `LLM_MODELO_SQL` (vacío = el modelo del agente;
  comparar modelos es un cambio de configuración). El prompt lleva el esquema de la vista, las 24
  estaciones con su distrito, la fecha de hoy y el rango con datos, y reglas fijas: media ponderada
  por horas (nunca `AVG(media)`), NO2 y PM10 si no se nombra contaminante, un resultado por
  contaminante, días con dato en los rankings, «esta semana» = 7 últimos días disponibles, «hora
  punta» = bloque de mañana, un periodo sin datos no se sustituye por otro.
- **Validador** (`sqlglot`, dialecto PostgreSQL): una sentencia de consulta, solo la vista (las CTE
  propias cuentan), sin DML dentro de un `WITH`, sin `INTO` ni `FOR UPDATE`, sin funciones de
  sistema, `LIMIT` impuesto. Es la primera barrera; el rol sigue siendo la real.
- **Un reintento** con el error del validador o de PostgreSQL. Si no se puede abrir la conexión
  (base caída) no se reintenta: la pregunta recibe una frase fija.
- **Resultado** al modelo: descripción, columnas, filas y truncado, con las cifras **redondeadas a
  1 decimal en código** y un tope de tamaño (8.000 caracteres): el modelo las copia sin redondear
  por su cuenta, y la comprobación de cifras las encuentra tal cual. El SQL va solo a la traza.
- **Ruta de datos**: la consulta es obligatoria en las preguntas DATOS (si el modelo no la pide, la
  lanza el código, como la búsqueda documental) y la respuesta la escribe una síntesis sin
  herramientas con las filas y las fechas, que sale como tokens. `fuentes` lleva una entrada `sql`
  por consulta con su descripción legible.
- Tests: 14 nuevos (82 en total). Cinco casos del lote de 16 de punta a punta con LLM falso y
  SQLite; validador, una regla por caso; reintento. En el Postgres local con el rol de lectura, una
  consulta de ranking por distrito con `date_trunc`, `INTERVAL` y `::numeric` tardó **85 ms** y el
  reintento corrigió una tabla prohibida.
- Pendiente de medir con Bedrock (fase 4 de su plan): acierto con el gold, SQL válido a la primera,
  latencia y coste. La ruta de datos añade dos llamadas al LLM por pregunta (redactor y síntesis).

**Pendiente**: decidir las comprobaciones (fase 6, paso 4) y las herramientas SQL (fases 3 a 6 de su plan: varias intenciones por turno, evaluación con Bedrock, catálogo si hace falta) y de ML. La
fase 7 (Bedrock en la EC2, contenedor del agente en `docker-compose` y despliegue) queda fuera de
este trabajo: la hace otro miembro del equipo.

#### 7.3.2 `LLMOrchestrator` — primer agente (implementado el 2026-09-28; descartado el 2026-10-08)

Servicio FastAPI, puerto 8100, `POST /responder`. Ligero: el RAG se consume por HTTP (sin
torch/chromadb). **Descartado el 2026-10-08** en favor de `Agente` (§2): nunca se conectó a un
proveedor. El código y su job de CI siguen en el repositorio, sin previsión de uso. Lo que sigue
describe cómo quedó.

- **Bucle**: máx. `MAX_ITERACIONES=4` vueltas; al agotarse, cierre forzado sin tools. Las tools
  nunca lanzan: sus errores vuelven como `{"error": ...}`.
- **`query_sql`**: 5 consultas predefinidas (`ultimos_niveles`, `serie_bloques`, `anomalias`,
  `comparar_estaciones`, `info_estaciones`) con parámetros validados y ligados; `anomalias` exige
  `cobertura >= 0,7` en el SQL. Sin SQL libre (D3 en §2).
- **`buscar_evidencias`** (2026-09-30): cliente HTTP de `rag.api`; definición pedida al RAG y
  cacheada; si falla, degrada a solo `query_sql`. Renumera `Dn` si hay varias llamadas.
- **Revisión adversarial** (2026-09-28, 4 revisores + refutación): aviso al LLM cuando un resultado
  se trunca a 60 filas; filtro de distrito por coincidencia parcial; presupuesto de timeouts
  coherente entre servicios (`LLM_TIMEOUT_S=30` × 2 reintentos ≤ `ORCHESTRATOR_TIMEOUT_S=120`); el
  cierre forzado no envía `tool_choice` sin `tools`; `choices` vacío → 503; prompt ajustado a las
  limitaciones reales de los datos. Limitación aceptada: consultas sin estación recorren la tabla.
- **Ruta documental con citas verificadas** (2026-09-30): si hubo evidencias, la respuesta final
  del modelo debe ser el JSON `{estado, afirmaciones[{texto, evidencias:[Dn]}], limitaciones}`;
  el agente lo valida con `POST /rag/validar` (título, sección y bibliografía se resuelven en el
  servicio desde el corpus por `chunk_id`, nunca del modelo) y devuelve el texto renderizado
  con `[Dn]`, avisos y fuentes. Una única reparación con el `mensaje_reparacion` del servicio;
  si tampoco, insuficiencia. `fuentes` lista solo los documentos **citados** y la `advertencia`
  médica se activa solo con afirmaciones documentales. La ruta de solo datos sigue en texto
  libre. Peor caso de llamadas al LLM: `MAX_ITERACIONES` + 3 (cierre + reparación + cierre de
  datos).
- **Tests**: 51 (2026-10-01; eran 34 antes de la ruta documental), en ~1 s, sin red — LLM falso
  con guion que registra las llamadas, SQLite en memoria, recuperación/validación documental
  fingidas y transporte HTTP del puente con `httpx.MockTransport` (ni torch ni el servicio RAG
  se instalan en CI). Cuarto job del workflow.

**Proveedor del LLM: Amazon Bedrock; modelo sin elegir (§13).** Finalistas evaluados el 2026-09-27 con la
documentación oficial: **Mistral** plan Experiment (toda la gama gratis, límites reportados
~1 req/s y 500K tokens/min `[por confirmar en consola]`, mejor español), **Groq** (30 RPM,
1.000 req/día; riesgo: 8K tokens/min con contexto RAG) y **Gemini** (límites del free tier no
publicados). Descartados: Cerebras (free tier eliminado en 2026), Cohere (1.000 llamadas/mes),
Hugging Face (créditos insuficientes); OpenRouter solo como reserva. Desde el 2026-10-04 se suma
**Amazon Bedrock** para producción (acceso con la cuenta del máster), y es el proveedor con el que
se ha probado el agente: Ministral 14B 3.0 y gpt-oss-120b en `eu-west-1` (§7.3.1). Los free tiers
quedan como alternativa para desarrollo, sin usar. Implicación operativa: los free tiers limitan
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
- **Observabilidad del agente:** spans OpenTelemetry por turno, visibles en Phoenix y guardados en
  JSONL (§7.3.1). Los tests de trazas usan el exportador en memoria del SDK: comprueban el árbol de
  spans de un turno, que dos turnos concurrentes no se mezclen y que un exportador caído no afecte
  a la respuesta.
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
- **La primera traza real encontró un fallo que los tests no cubrían** (2026-10-06): con el RAG sin
  responder, el turno esperaba dos veces el timeout (43 s) porque la búsqueda forzada no distinguía
  «el modelo no buscó» de «buscó y el servicio no respondió». Los tests probaban el RAG caído desde
  el principio (la herramienta ni se ofrece), no el que cae después. En la cascada se veían los dos
  tramos de 20 s a simple vista. §7.3.1.
- **Una regla de alcance en el prompt arrastra a sus vecinas** (2026-10-06). Escribir «el polen
  está fuera de alcance» sin matiz mandó fuera también la rinitis y la zona para alérgicos, que sí
  son del dominio: 26/28, peor que los 27/28 de partida. Hizo falta la salvedad explícita (alergia, asma o rinitis
  están dentro). Lección: medir cada cambio de prompt con todo el guion, no solo con el caso que
  se quería arreglar, y asumir que iterar sobre los mismos casos sobreajusta. §7.3.1.
- **Medir antes de construir para el caso difícil** (2026-10-06). La búsqueda con las preguntas
  previas concatenadas se diseñó para los seguimientos («¿Y en niños?»), pero en ningún lote llegó
  a ejecutarse: los dos modelos reformulan solos la búsqueda con el contexto. Sigue siendo una red
  de seguridad, pero sin medir. §7.3.1.

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
- **Los caminos de emergencia también hay que probarlos con el proveedor real** (2026-10-06). La
  síntesis forzada del agente pasaba los tests con el LLM falso, pero Bedrock rechaza su historial
  (bloques de herramienta sin `toolConfig`). En los lotes normales no aparece nunca (0 de 24 turnos),
  así que solo se vio forzando el caso. Lección: los dobles de prueba no reproducen las reglas del
  proveedor; los caminos raros necesitan una prueba dirigida contra el servicio real.
- **Comparar palabras no distingue una fuga parafraseada de una presentación legítima** (2026-10-06).
  La regla de fuga del prompt (n-gramas compartidos) no saltó en ninguno de los 128 turnos reales, lo que
  parecía bueno; el lote adversario mostró que Ministral filtra el prompt reescribiéndolo, y que
  esa paráfrasis comparte con el prompt lo mismo que un saludo normal. Lección: una regla sin
  hallazgos en el tráfico normal no está validada hasta que se la pone a prueba con casos hechos
  para hacerla saltar; y conviene buscar la señal que solo tiene el caso malo (aquí, el texto en
  segunda persona de las instrucciones).

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
  la pregunta habla del presente. Unir ambas fuentes es la herramienta SQL del agente, pendiente
  (§7.3).
- **La memoria de la conversación vive en el proceso del agente**: se pierde al reiniciar o
  desplegar y obliga a un solo proceso (§7.3.1). Escalar a varios exigiría un almacén compartido
  y otra coordinación del bloqueo por sesión.
- **El prompt del clasificador se ajustó sobre el mismo guion con el que se mide** (29 preguntas y
  11 turnos de conversación): el 29/29 es optimista. Hace falta un juego de casos nuevo para
  confirmarlo.
- **El umbral de evidencia tiene un margen de una milésima** (§7.2). 0,1754 rechaza las 10 ajenas
  del juego de casos a costa de una documental, pero el hueco entre ambos grupos es de 0,001: una
  pregunta ajena nueva puede colarse. Ninguna adversaria se rechaza por distancia; ahí la
  abstención depende del LLM.
- **El umbral está atado al modelo de embeddings:** con e5-small el mismo valor rechaza 5/10
  ajenas. Cambiar de modelo obliga a reindexar y recalibrar.
- **Recuperación NO/NO2:** el modelo de embeddings no distingue bien NO de NO2 (14/15 documentales
  con hit@4; el fallo es ese caso).

**Trabajo futuro**
- **¿Hace falta un modelo de razonamiento para el agente?** En la ruta documental no: el modelo sin
  razonamiento (Ministral 14B) acertó 18 de 18 turnos y es más rápido (§7.3.1). Queda por analizar
  con las preguntas de datos, cuando exista la herramienta SQL: elegir consulta y parámetros, o
  combinar varias mediciones, puede beneficiarse del razonamiento. Depende de si la herramienta usa
  consultas predefinidas (como el `query_sql` del `LLMOrchestrator` descartado, §7.3.2) o SQL generado. Si se usa un modelo
  razonador, hay que darle margen de tokens de salida (§10).
- **Agente**: herramientas SQL (mediciones y anomalías) y de ML; consulta de búsqueda reformulada por el clasificador en lugar de concatenar las
  preguntas previas; estado semántico entre turnos (cifras, contaminantes, evidencias citadas)
  cuando lo pidan las comprobaciones de cifras o la herramienta SQL.
- LSTM Autoencoder o PCA para anomalías de forma del perfil horario.
- Baseline ponderado por recencia y ajustado solo con el periodo de entrenamiento.
- Módulo de *forecasting* para las preguntas de planificación.
- Opción de red C en AWS (RDS sin exposición pública a coste cero).
- **Medir el comportamiento del LLM** con los mismos 30 casos del RAG (el agente se ha medido con
  sus propios 12 casos y 5 conversaciones, §7.3.1): respuestas
  con fuentes, válidas tras reparación y de insuficiencia por tipo de caso. La evaluación de la
  recuperación ya está hecha (§7.2).
- **Abstención más allá del umbral absoluto:** un criterio relativo al mejor fragmento, un
  *reranker* o un clasificador de tema antes de buscar. Sin ello, las ajenas cercanas llegan al LLM.
- **`multilingual-e5-small` para el despliegue** si el modelo de ~1,1 GB no cabe: ya medido
  (§7.2), ordena mejor pero solapa documentales y ajenas; haría falta recalibrar y, probablemente,
  un criterio de abstención adicional.
- **Ampliar el juego de casos** y separar casos de calibración y de medida, para que el umbral no
  se ajuste y se mida con las mismas preguntas.
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
  Pendiente de la primera ejecución real tras el merge a `main` |
| 2026-10-02 | Notas del modelo de embeddings puestas al día (e5-base, presupuesto de tokens, índice
  local comprobado) y distancias reales medidas frente al umbral 0,22 (Carlos Fernández) |
| 2026-10-04 | **Evaluación de la recuperación del RAG** (Carlos Fernández, `feature/rag-metricas`): 30 casos
  fijos y `python -m rag.evaluar`. Con e5-base: hit@4 14/15, MRR@10 0,740. **Umbral 0,22 → 0,1754**
  (ajenas rechazadas 3/10 → 10/10, documentales bajo el umbral 14/15 → 13/15), por decisión de
  priorizar la abstención pese a un hueco de 0,001. Medido también `multilingual-e5-small`
  (MRR 0,796, pero solape documental/ajena de 0,023): se mantiene e5-base. Índice reconstruido
  antes de medir (el anterior era de una copia sin commit, mismas cifras) |
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
| 2026-10-06 | **Agente LLM: observabilidad, parte A** (Carlos, rama `feature/Agente`): spans OpenTelemetry con atributos OpenInference en todo el turno (clasificar, bucle, búsqueda forzada, síntesis, validar, cada llamada al LLM y cada herramienta), eventos de decisión del código, exportador JSONL propio y servicio Phoenix en el compose (perfil `observabilidad`). `traza_id` opcional en `/responder` y una línea de resumen por turno en el log. 25 tests (4 nuevos). Pendiente: verlo con Bedrock en Phoenix, el informe agregado y la evaluación pequeña |
| 2026-10-06 | **Agente LLM: observabilidad verificada con Bedrock** (Carlos): Phoenix en Docker, `rag.api` y Ministral 14B; cascada completa con tokens reales (turno documental de 5,5 s; captura en `docs/agente/img/PhoenixTraza.PNG`). La traza mostró una doble espera de 20 s con el RAG sin servicio (turno de 43 s): corregida, ya no se repite la búsqueda. 26 tests. Pendiente: informe agregado (parte B) y evaluación pequeña (parte C) |
| 2026-10-06 | **Agente LLM: observabilidad, partes B y C, y `LLM_MAX_TOKENS`** (Carlos, rama `feature/Agente`): `informe_trazas.py` (tablas markdown desde los JSONL: medianas con n, tokens y coste con precios fechados, turnos incompletos sin coste) y `evaluar_turnos.py` con 12 casos con resultado esperado y tres criterios separados (JSON, referencias, revisión humana). `LLM_MAX_TOKENS` (2048) en los dos proveedores y log de síntesis inválida a `warning`. 29 tests. Pendiente: ejecutar el lote con los dos modelos de Bedrock y elegir modelo |
| 2026-10-06 | **Agente LLM: informe visual de trazas** (Carlos, rama `feature/Agente`): `informe_trazas.py --salida x.html` genera una página autocontenida (JS y SVG sin librerías) para analizar latencia y coste por lote, fase, ruta y turno, con la cascada de cada turno y su coste; `--salida x.json` da el conjunto de datos, pensado como contrato de una futura web de análisis. 30 tests |
| 2026-10-06 | **Agente LLM: lote de evaluación en Bedrock** (Carlos): 12 casos por modelo con `rag.api` real y `LLM_MAX_TOKENS`=2048. Ministral 14B y gpt-oss-120b: 11/12 lo esperado y 6/6 síntesis válidas a la primera, sin reparaciones. Medianas del turno: 4,0 s y 2,9 s; coste del lote: 0,0059 $ y 0,0087 $. Mismo fallo en los dos (`sin-01`, polen): el error era del caso, que pasa a fuera de alcance (las estaciones no miden polen); con esa lectura, gpt-oss 12/12 y Ministral 11/12. Pendiente: revisión humana y elección del modelo |
| 2026-10-06 | **Agente LLM: síntesis forzada probada en Bedrock** (Carlos): falla con los dos modelos (`ValidationException`, falta `toolConfig` con bloques de herramienta en el historial) y el turno acaba en 503. Arreglo y revisión del prompt del clasificador (alcance: el polen no se mide) añadidos al plan del agente |
| 2026-10-06 | **Agente LLM: síntesis forzada corregida** (Carlos): mensajes nuevos (pregunta + resultados de las herramientas en texto) en lugar del historial con bloques de herramienta; prompt de la síntesis forzada reescrito para ir solo. 31 tests (1 nuevo). Verificado en Bedrock: 4 de 4 turnos (2 preguntas × 2 modelos, `max_vueltas=1`, búsqueda que falla) llegan a la síntesis forzada y responden; ninguno da 503; Ministral responde con conocimiento propio (cifras sin evidencia), gpt-oss admite que no tiene información |
| 2026-10-06 | **Agente LLM: sesión y un turno a la vez** (Carlos, rama `feature/Agente`): `session_id` opaco en `/responder` (generado si no llega) y `business/sesiones.py` con bloqueo y contador de usuarios por sesión. 35 tests (4 nuevos: contrato y tres de concurrencia con eventos) |
| 2026-10-06 | **Agente LLM: streaming SSE** (Carlos, rama `feature/Agente`): `POST /responder/stream` con eventos `status`, `token`, `passthrough`, `error` y `done`; tokens solo en respuestas definitivas; emisor opcional por turno, cola acotada, heartbeat y cancelación al desconectarse el cliente (verificada con uvicorn real). 41 tests (6 nuevos). Sin probar aún contra Bedrock |
| 2026-10-06 | **Agente LLM: lote repetido con Phoenix** (Carlos): el primero se lanzó sin `PHOENIX_ENDPOINT` (el script no lee `.env`) y solo quedó en JSONL. Misma calidad y coste; Ministral casi duplica su latencia (mediana 6,8 s frente a 4,0 s) y gpt-oss se mantiene (3,2 s). Cada lote es un proyecto en Phoenix |
| 2026-10-06 | **Agente LLM: fase 4 cerrada, `ApiUsuario` conectado al agente** (Carlos, rama `feature/Agente`): `AGENTE_URL`, `session_id` y `traza_id` en `/chat`, y `POST /chat/stream` (proxy del SSE del agente o fallback `passthrough` + `done`). 22 tests de `ApiUsuario` (4 nuevos y 1 ampliado). Verificado con Bedrock de punta a punta (charla en tokens, documental en `passthrough`, `en_espera` y cancelación al cortar el cliente a través del proxy); 0,003 $ |
| 2026-10-06 | **Entorno de pruebas local con un comando** (Carlos): `entorno_local.sh` levanta RAG, Phoenix, agente y `ApiUsuario` en paneles de tmux (antes, cuatro terminales a mano) con esperas por `/salud`, comprobación previa de credenciales de Bedrock y puertos, y `preguntar` para consultar por `/chat/stream`. Se descarta de momento Docker Compose para esto: cada cambio de código obligaría a reconstruir imágenes |
| 2026-10-06 | **Agente LLM, fase 5: memoria de la conversación** (Carlos, rama `feature/Agente`): almacén en memoria del proceso detrás de una interfaz (20 turnos por sesión, caducidad de 1 semana), ventana por presupuesto de tokens (1.500), respuesta para el contexto sin marcas `[Dn]` y contexto en el clasificador, el bucle, las dos síntesis y la búsqueda que lanza el código; `/rag/validar` sigue viendo solo la pregunta actual. Descartados `PostgresChatStore`, `REPETIR` y el estado semántico. 50 tests (9 nuevos); sin historial, los mensajes del primer turno no cambian |
| 2026-10-06 | **Agente LLM: clasificador revisado y evaluación con conversaciones** (Carlos): prompt con el alcance definido por lo que el sistema puede responder (polen, ruido y tiempo fuera; alergias dentro; límites legales como documentación). Guion de 29 preguntas y 5 conversaciones (11 turnos). En Bedrock, antes → después: clasificador 27/28 → 29/29 con los dos modelos; conversaciones 9/11 → 11/11 (Ministral) y 11/11 (gpt-oss). La búsqueda concatenada no llegó a ejecutarse: los modelos reformulan solos. Unas 600 llamadas, ~0,12 $ `[estimación]`. 51 tests |
| 2026-10-06 | **Agente LLM, fase 6: comprobaciones posteriores, pasos 1 y 2** (Carlos, rama `feature/Agente`): reglas de cifras sin respaldo, fuga del prompt e internos sobre lo que escribió el modelo, en modo observación (evento en la traza y campo en el log); `COMPROBACIONES_BLOQUEAN` para que una regla sustituya la respuesta por una frase fija (sin tokens en el stream si alguna bloquea). Excluidas de la fuga las frases de presentación que el modelo repite. 62 tests (11 nuevos); sin llamadas a Bedrock. Pendiente: medir sobre las 128 trazas y un lote adversario y decidir qué reglas bloquean |
| 2026-10-06 | **Agente LLM, fase 6: medición de las comprobaciones** (Carlos, rama `feature/Agente`): script que aplica las reglas a las trazas guardadas (128 turnos) y lote adversario + sonda de síntesis forzada en Bedrock (26 turnos, 63 llamadas, 0,011 $). Cifras: 10 hallazgos, 3 falsos positivos (el «2.5 micras» de PM2.5) y una cita mal puesta. Fuga: Ministral repite el prompt de charla cada vez que la pregunta llega a la charla (4 de 4) y la regla solo detecta la copia literal (1 de 4). Internos: 0. Conteo por regla en el informe de trazas y en la evaluación de turnos. 63 tests |
| 2026-10-08 | **`LLMOrchestrator` descartado y reparto de la fase 7** (Carlos): `Agente` queda como único agente; el código del orquestador se conserva sin previsión de uso (§2). El despliegue del agente con Bedrock y su dockerización pasan a otro miembro del equipo. El informe de trazas cuenta aparte los turnos cancelados por el cliente (antes salían como ruta `error`). 64 tests |
| 2026-10-08 | **Agente LLM, herramienta SQL, fase 1: datos y entorno local** (Carlos con Claude Code, rama `feature/Agente`): vista `mediciones_bloques` e índice por fecha, rol `agente_lectura` (scripts en `deploy/sql/`), DAL de solo lectura con tiempo límite y tope de filas, `--desde` y recarga con `TRUNCATE` en el cargador. 68 tests (4 nuevos) y 3 de integración con PostgreSQL, 3/3 en local; consultas típicas ~20 ms |
| 2026-10-08 | **Agente LLM, herramienta SQL, fase 2: SQL libre y ruta de datos** (Carlos con Claude Code, rama `feature/Agente`): `consultar_datos` con redactor (`LLM_MODELO_SQL`), validador `sqlglot`, un reintento y cifras a 1 decimal; DATOS deja la frase fija y obliga la consulta; síntesis de datos con fechas y fuentes `sql`; lote `casos_datos.json` (16 casos). 82 tests (14 nuevos); probado en el Postgres local con el rol de lectura (85 ms), sin evaluar aún con Bedrock |


---

## 13. Preguntas abiertas

- [ ] ¿Qué límite de gasto o créditos tiene la cuenta AWS del máster?
- [ ] Agente nuevo: ¿Ministral 14B 3.0 o gpt-oss-120b en Bedrock? Empatan en clasificador y uso de
  herramientas. Ruta documental (2026-10-05): Ministral 18/18 a la primera; gpt-oss 4 fallos de 18
  por el límite de 512 tokens de salida. Con `LLM_MAX_TOKENS`=2048 (lote del 2026-10-06): empate,
  6/6 válidas a la primera con los dos (12/12 gpt-oss y 11/12 Ministral con el caso del polen
  corregido); gpt-oss algo más rápido y ~50 % más caro. Tras la fase 5, 29/29 en el clasificador y
  11/11 en conversaciones con los dos. **Elección aplazada** (2026-10-06) hasta poder evaluar
  también las herramientas SQL y de ML. Falta la revisión humana de las respuestas y del español.
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
  predefinidas parametrizadas (2026-09-28). Proveedor: Amazon Bedrock (2026-10-04); queda elegir
  el modelo (punto del agente, arriba).
- [x] **¿Cómo se citan los datos de SQL?** → resuelto el 2026-09-30 separando rutas: los datos
  responden en texto libre (deterministas por construcción) y lo documental pasa por el flujo
  evidencias/validar; en respuestas mixtas la cifra va dentro de la afirmación (§7.3, §2).
- [x] Reconciliar `LLMOrchestrator` con la Fase 2 reescrita → hecho el 2026-09-30: el orquestador
  adopta el contrato de evidencias en proceso (tool `buscar_evidencias` + validación en el
  cierre); el umbral y las fuentes son ya los del servicio (§7.3).
- [ ] Reindexar `data/chroma/` con e5 en cada entorno de trabajo (`python -m rag.indexar`): el
  índice construido antes de la reescritura no tiene metadatos de modelo y la búsqueda lo rechaza.
  Comprobado en el entorno de Carlos el 2026-10-01 (e5-base, 50 fragmentos); falta el resto.
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
