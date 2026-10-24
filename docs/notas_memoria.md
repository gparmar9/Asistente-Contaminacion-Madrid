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
| 2026-09-28 | Tool de datos del asistente | SQL libre generado por el LLM con validación *read-only* (opción grande de D3) → **5 consultas predefinidas parametrizadas**: el LLM rellena enums/fechas validados y el módulo construye el único SELECT posible con parámetros ligados | Inyección SQL imposible por construcción, resultados deterministas y defendibles; los modelos de los tiers gratuitos fallan más generando SQL correcto que rellenando 5 parámetros. Resuelve la decisión D3 del roadmap |
| 2026-09-30 | Ruta documental del agente | Tool `search_documents` con umbral propio (0,8) y citas por confianza en el modelo → **contrato de evidencias de la Fase 2 por HTTP**: tool `buscar_evidencias` (`POST /rag/evidencias`, umbral 0,22 del servicio), respuesta del modelo en JSON `{estado, afirmaciones[Dn], limitaciones}`, validación con `POST /rag/validar` y render con `[Dn]` + bibliografía; 1 reparación | Las citas se comprueban, no se creen: la trazabilidad no depende de la conducta del LLM (argumento central de §7.2). Cierra el hueco de contrato de 2026-09-24 separando rutas (datos en texto libre; en mixtas, la cifra va dentro de la afirmación). Descartado: extender el esquema con `S1..Sn` |
| 2026-09-30 | Arquitectura de servicios | 2 servicios con el RAG embebido en el orquestador (torch/chromadb en proceso) → **3 servicios**: `ApiUsuario` + `LLMOrchestrator` (ligero) + `rag.api` (:8010, evidencias) | El orquestador no repite trabajo que el RAG ya expone por API; sin torch/chromadb queda ligero para desplegar. El RAG (modelo ~1,1 GB) se despliega, escala y calienta por su cuenta. Se valoró en proceso el mismo día y se descartó: duplicaba responsabilidades y dependencias |
| 2026-10-01 | Definición de la tool `buscar_evidencias` | Copia duplicada a propósito en el orquestador → **se consume `GET /rag/herramienta`** (caché por proceso), sin copia local | Una sola fuente de verdad del esquema: una copia local puede divergir del servicio en silencio. Si el GET falla, la tool no se ofrece en esa vuelta (el agente sigue con `query_sql`) y se reintenta en la siguiente. Descartada una copia local de respaldo (primera versión del mismo día): era precisamente la duplicación que se quería eliminar |
| 2026-10-01 | Disponibilidad de la base de datos | RDS encendida 24 h → **encendida solo de 22:00 a 01:30 (hora de Madrid) y a demanda** con un workflow de GitHub (§8.8) | La carga es una vez al día y la API aún no está desplegada: pagar la instancia 24 h no aporta nada. Ahorro estimado de ~15,5 a ~4–5 $/mes. Descartado: DynamoDB (el acceso es analítico: rangos, agregaciones y `JOIN`, justo lo que no hace bien una base clave-valor), Aurora Serverless v2 con pausa automática (ahorro parecido pero exige migrar; queda como alternativa), programarlo con `schedule` de GitHub Actions (puede retrasarse y se desactiva tras 60 días sin actividad) y que el programador llame directamente a `StartDBInstance` (da error si la base ya está encendida) |
| 2026-10-01 | Workflows de despliegue | «Desplegar Lambda» y «Rollback Lambda» → **«Desplegar» y «Rollback» con un desplegable de componente** (hoy solo `lambda`) | Preparar el despliegue de la API, el orquestador, el RAG y la web sin multiplicar workflows: cada pieza será una opción del desplegable y un job propio. Bloqueo por componente: piezas distintas pueden desplegarse a la vez, la misma no. Descartado: un workflow por pieza (duplica pasos y botones) y pedir la imagen a desplegar (el despliegue siempre construye el código de la rama elegida; volver a una versión concreta es tarea del rollback) |
| 2026-10-02 | Empaquetado de la API | Servicios arrancados a mano con `uvicorn` y `python -m rag.api` → **una imagen Docker por servicio** (`api-usuario`, `orquestador`, `rag`), levantadas juntas con el perfil `api` de `docker-compose` | Es como irán en la EC2 y permite desplegar cada pieza por separado. La imagen del RAG lleva **dentro el modelo y el índice** construido en el propio build, con el commit del corpus en sus metadatos: arranca sin descargar nada y cada imagen corresponde a un corpus concreto. Descartado: una sola imagen con todo (obliga a redesplegar el RAG, de ~3 GB, por cualquier cambio en la API) y construir el índice al arrancar el contenedor (arranque lento y sin garantía de que todas las réplicas usen el mismo índice) |
| 2026-10-03 | Interfaz (Fase 4) | «Web con Streamlit + FastAPI» (2026-07-22) → **SPA React 19 + TypeScript (Vite) en `Web/`, servida por nginx que hace de proxy de `/api/*` a `ApiUsuario`** (un solo origen, dos contenedores). Tipos del cliente **generados del OpenAPI** del backend (`openapi-typescript` + `openapi-fetch`); el job de CI regenera el contrato y falla si difiere del versionado | Streamlit no permite controlar lo que aquí es un requisito de correctitud (huecos en vez de ceros, estados de anomalía con icono y texto, cobertura visible) ni presentar un chat sin memoria de forma honesta. Con los tipos generados, un cambio de contrato rompe `npm run build` en el PR, no la demo. Descartados: Next.js (sin SSR/SEO no aporta), librerías de componentes (CSS con *custom properties* y modo oscuro propio) y una URL de la API en el frontend (rutas relativas, sin CORS) |
| 2026-10-03 | Lectura de anomalías en la interfaz | `is_anomaly` mostrado tal cual → **solo se presenta como anomalía con `cobertura ≥ 0,7`**; por debajo, el dato se muestra con su cobertura y la etiqueta «cobertura baja», sin juicio. Nulos = huecos; `anomaly_score` nunca como probabilidad; sin bandas tipo AQI; nunca doble eje Y | Las marcas con cobertura baja son artefactos del hueco (§6.5). Pintar bandas sanitarias sería inventar un juicio clínico que el detector no hace. La cifra de la pantalla «Hoy» usa el **último día consolidado** (el pipeline carga el día a las 23:45) y lo dice, en vez de mostrar un cero engañoso |
| 2026-10-04 | Umbral de evidencia del RAG | 0,22 fijado a ojo con preguntas lejanas al dominio → **0,1754**, punto medio entre la peor documental (0,1750) y la mejor ajena (0,1759) en 30 casos de evaluación (§7.2) | Con 0,22 las 7 ajenas cercanas al dominio (tiempo, tráfico, transporte) recibían evidencias y llegaban al LLM (3/10 rechazadas). Con 0,1754 se rechazan 10/10 a costa de una documental (13/15 en vez de 14/15): se prefiere que el asistente se abstenga de más a que responda fuera del dominio. Descartado: mantener 0,22, que era lo que dictaba la regla fijada antes de medir (no cambiar con un hueco menor de 0,01; aquí es de 0,001) y cambiar a `multilingual-e5-small` (mejor MRR, pero solapa documentales y ajenas y no hay punto medio) |
| 2026-10-04 | Agente LLM | `LLMOrchestrator` como único agente → **nuevo servicio `Agente`** (puerto 8200), diseñado desde cero con un **bucle de herramientas escrito a mano**; **LlamaIndex solo como cliente del LLM** (`OpenAILike` para APIs OpenAI-compatibles y `BedrockConverse` para Amazon Bedrock, elegibles por variable de entorno) y, más adelante, como almacén del chat. Ambos servicios conviven sin código ni dependencias compartidas; `ApiUsuario` elegirá uno por configuración | Principios tomados de las clases del máster y de un diseño de agente empresarial: pocas piezas, el modelo clasifica y el código decide, fases pequeñas con salida tipada y valor seguro, síntesis final sin herramientas, lo determinista lo entrega el código. **Descartado un framework de agentes completo** (ReAct/`AgentWorkflow` de LlamaIndex, LangGraph): esconde el bucle que precisamente se quiere controlar (vetar u obligar herramientas, cerrar el turno sin modelo, validar la salida con el RAG) y arrastra abstracciones que no se usan. Descartado también ampliar `LLMOrchestrator`: su bucle no tiene puntos de intervención en código y rehacerlo dentro equivalía a reescribirlo. Bedrock entra como proveedor de producción porque la cuenta del máster da acceso; el modelo concreto en `eu-west-1` con *tool use* y *streaming* queda `[por confirmar]` |
| 2026-10-06 | Sesión en el agente | Servicios sin estado (2026-09-27) → **`session_id` opaco** en `/responder` (se genera si no llega) y **un turno a la vez por sesión** | El streaming y la memoria de la conversación (fases 4 y 5) necesitan agrupar los turnos. Sin usuarios ni datos personales: el id no identifica a nadie y el agente aún no guarda nada con él |
| 2026-10-06 | Chat de `ApiUsuario` | `/chat` solo hacia `ORCHESTRATOR_URL` (o stub), sin sesión → **`AGENTE_URL` elige el agente**; `session_id` y `traza_id` opcionales en el contrato y **`POST /chat/stream`** (proxy del SSE del agente, o `passthrough` + `done` sin agente) | Los dos agentes conviven y se elige por configuración, sin tocar código. El fallback mantiene el contrato del stream para el frontend aunque el orquestador no conserve contexto |
| 2026-10-06 | Memoria de la conversación del agente | Diseño previsto (2026-10-04): `PostgresChatStore` de LlamaIndex sobre la base del proyecto, estado tipado entre turnos (`turn_state`) e intención `REPETIR` → **almacén propio en memoria del proceso** (1 semana sin actividad, 20 turnos por sesión) y ventana por presupuesto de tokens; **LlamaIndex queda solo como cliente del LLM**. Sin `REPETIR` y con `turn_state` aplazado | Un TFM no necesita que las conversaciones sobrevivan a un reinicio, y el bloqueo por sesión ya exige un solo proceso: Postgres añadiría tablas, migraciones y una dependencia más en cada turno. El almacén vive detrás de una interfaz: pasar a una base de datos cambiaría una clase. `SimpleChatStore` solo guarda listas de mensajes, sin caducidad ni metadatos del turno. `REPETIR` desde cifras sueltas pierde a qué contaminante, unidad o fuente pertenece cada una; un «¿cuál era ese límite?» se trata como pregunta normal con contexto. El estado semántico se definirá cuando lo pida una necesidad concreta (comprobaciones de cifras, herramienta SQL) |
| 2026-10-06 | Alcance del clasificador del agente | Alcance por tema («calidad del aire») → **alcance por lo que el sistema puede responder**: contaminación del aire, con los contaminantes que mide la red citados en el prompt; polen, ruido y tiempo fuera; alergia y asma dentro; los límites legales son documentación, no mediciones | En el lote del 2026-10-06 los dos modelos fallaron la pregunta del polen de forma distinta (uno `DATOS`, otro `FUERA_DE_ALCANCE`). Medido antes y después: 27/28 → 29/29 con los dos modelos (§7.3.1) |
| 2026-10-06 | Comprobaciones posteriores del agente | Un modo global `COMPROBACIONES_MODO` (observar o bloquear todas) → **activación por regla** (`COMPROBACIONES_BLOQUEAN`, vacía por defecto) | Cada regla pasa a bloquear solo con evidencia propia (0 falsos positivos en las trazas reales y en un lote adversario, y al menos un acierto). Con un modo global, una regla ruidosa impediría bloquear con las fiables. Descartado también pedir una reparación al modelo («la cifra X no está en D2») en vez de la frase fija: sería mejor respuesta, pero añade una llamada y mezcla la validación del RAG con la del agente; queda como trabajo futuro |
| 2026-10-07 | Diseño del dashboard | Dos paletas (original + «Cielo» experimental con `?paleta=cielo`) y 4 pantallas navegables → **una sola paleta cielo** (azul cielo + slate, alineada con la landing de AireMadrid: degradado `#38bdf8→#2563eb`, radios 16 px, botones píldora) y navegación **Home · Estadísticas · Estaciones**: portada tipo landing en `/` (hero con foto, 3 tarjetas de rasgos, sección de mapa ilustrada), Estadísticas (antes «Hoy»: cifra de anomalías con perfil de Madrid, *stat tiles*, bloques marcados) y Estaciones (mapa). El asistente queda como burbuja flotante; «Informe del día», la página Asistente y Metodología salen de la navegación (las rutas siguen vivas) | Simplificar para la demo: una portada amable que explique el producto, menos navegación y una identidad visual única. Mantener dos paletas duplicaba validación de contraste sin aportar; el informe del día dependía del LLM aún no conectado |
| 2026-10-08 | Agente del asistente | `LLMOrchestrator` y `Agente` conviviendo, elegidos por `AGENTE_URL` → **`Agente` como único agente**; `LLMOrchestrator` descartado: su código se conserva en el repositorio, sin previsión de uso | Decisión del equipo. El orquestador nunca llegó a conectarse a un proveedor; el `Agente` ya tiene clasificador, ruta documental validada, sesión, *streaming*, memoria, observabilidad y comprobaciones, y está probado con Bedrock. Consecuencia: la consulta de mediciones (`query_sql` del orquestador) no existe aún en el agente y pasa a ser su herramienta SQL pendiente |
| 2026-10-08 | Preguntas de datos del agente | Frase fija («todavía no consulto mediciones») → **herramienta `consultar_datos` con SQL libre**: un LLM redacta el SQL sobre la vista `mediciones_bloques`, un validador (`sqlglot`) lo limita, PostgreSQL lo ejecuta con un rol de solo lectura y una síntesis sin herramientas redacta la respuesta | Las preguntas de datos eran la mitad de las preguntas objetivo del asistente. Se empieza por SQL libre porque 5 de las 11 preguntas de datos del lote no caben en un catálogo cerrado razonable; el catálogo solo se construye si el libre no alcanza el criterio de la evaluación |
| 2026-10-09 | Preguntas mixtas del agente | Una intención y cifras dentro de afirmaciones documentales → **hasta dos intenciones y respuesta en dos bloques: datos y documentación** | Las cifras SQL no tienen citas documentales `[Dn]`. Separar los bloques conserva la validación del RAG; añadir una fuente SQL `S1` habría cambiado su contrato |
| 2026-10-09 | Modelo de referencia para SQL | Claude Haiku 5.5 → **MiniMax M2.5** | Haiku no estaba disponible en la cuenta del máster. MiniMax fue elegido por su menor precio entre las alternativas disponibles; GLM 5.3 y Kimi K3 eran más caros |
| 2026-10-09 | Fechas de las preguntas de datos | Fechas elegidas por el LLM → **periodos calculados en Python a partir de la pregunta original** | Los modelos alteraban los periodos. Un calendario en el prompt o parámetros elegidos por el LLM no eliminaban ese riesgo; `dateparser` y Duckling no daban los rangos definidos por el proyecto, y un único par de fechas no cubría periodos separados |
| 2026-10-09 | Modelo de referencia para SQL | MiniMax M2.5 → **sin modelo de referencia en los siguientes lotes** | Logró 2/11 aciertos en datos frente a 4/11 del mejor modelo barato, con mayor coste por lote: 0,085 $ frente a 0,023–0,043 $. Se prioriza corregir el diseño |
| 2026-10-11 | Predicción de calidad del aire | Intent `PREDICCION` con frase fija («no hago predicciones») → **modelo de predicción t+1 por bloques** (`HistGradientBoostingRegressor`, uno por contaminante) con baselines honestos | Es la segunda herramienta de ML que el agente necesita (§6.7). HGB porque ya está en las dependencias, maneja los NaN de los lags sin imputar y no necesita escalado. Descartados: LightGBM (dependencia nueva sin ganancia a este tamaño), SARIMA (≈550 series frágiles con huecos) y LSTM (sin meteorología no hay señal que justifique PyTorch en CI) |
| 2026-10-13 | Servicios de la EC2 | `api-usuario` + `orquestador` + `rag`, con `api-usuario` publicado en el 8000 y el LLM por `/jupiter/llm/{base_url,model,api_key}` → **`web` + `api-usuario` + `agente` + `rag`**: solo `web` (nginx) publica puerto (80); el agente usa **Bedrock con el rol de la EC2** (sin claves) y lee su configuración de **todo `/jupiter/agente/`** en SSM (cada parámetro, una variable de entorno) | El equipo sustituyó el orquestador por el `Agente` (2026-10-08) y la web ya tiene imagen. Un solo origen para el navegador (nginx hace de proxy de `/api/*`), como en local. Leer la ruta entera evita tocar `arrancar.sh` cada vez que el agente añade una variable. Descartado: mantener el orquestador desplegado (código sin uso) y una clave de API del LLM en SSM (Bedrock no la necesita con el rol de la máquina) |
| 2026-10-21 | Acceso a la web de la EC2 | Puerto 80 abierto solo a las IP del equipo → **IP del equipo o, para demos, abierto a todo internet con usuario y contraseña** (autenticación básica de nginx, contraseña cifrada en SSM) | La demo la presentan compañeros desde redes cuya IP pública no se conoce de antemano y puede cambiar. Descartados: una contraseña fija en el código (el repositorio es público), abrirlo sin contraseña (la API no tiene autenticación y el chat cuesta dinero), *port forwarding* con Session Manager (cada presentador necesita la CLI de AWS y su plugin) y un balanceador con Cognito o CloudFront con HTTPS (exigen dominio y certificado; demasiado para la víspera de la demo) |
| 2026-10-22 | Integración de la predicción con el agente | Importar los modelos en el agente → **servicio HTTP propio** (`src/prediccion`, espejo de `rag.api`, herramienta publicada en `/prediccion/herramienta`) | El agente se mantiene ligero (sin scikit-learn ni bundles) y conserva su patrón de herramientas remotas; reentrenar un modelo es redesplegar solo el servicio. El ancla y la elección de horizonte (h1/h2) quedan en código del servicio, nunca en el LLM (§6.7) |
| 2026-10-22 | Frecuencia de ingesta | Una carga al día a las 23:45 con la RDS encendida de 22:00 a 01:30 → **una carga al cierre de cada franja**: 07:45, 13:45, 20:45 y 23:45 (`cron(45 7,13,20,23 * * ? *)`), escribiendo **solo los bloques cerrados**, **con la RDS siempre encendida** (aplicado en AWS el 2026-10-23) | Datos del día disponibles durante la jornada y cada bloque evaluado completo, sin los falsos positivos del bloque abierto (§6.5). El minuto 45 sale de producción: 36/36 noches con la hora 22 publicada a las 23:45. El ahorro deja de ser prioritario. Descartado: cada hora (evalúa bloques a medias), cada 20 min (diseño inicial), 08:15/14:15/21:15 (primera propuesta: 30 min más de espera sin necesidad) y decidir el cierre por lo publicado en vez de por el reloj (si la API se retrasa, la última carga del día no escribiría la noche) |
| 2026-10-23 | Cobertura del bloque de noche en tiempo real | Sobre 4 horas (20–23) → **sobre las 3 que publica la API (20–22)** | La hora 23 no llega nunca (§11): con 4, todas las noches tenían cobertura 0,75 y el detector marcaba el 7,19 % (0,44 % en el histórico). Simulado en el histórico: 8,69 % con 4 y 0,49 % con 3. Descartado: reentrenar con la noche en 20–22 (cambia el modelo y el histórico por una hora) y una segunda fuente para la hora 23 (no se ha localizado una publicación diaria `[por confirmar]`) |
| 2026-10-23 | Logs y métricas de la EC2 | Logs de los contenedores solo en el disco de la máquina (`json-file`) y sin métricas de memoria ni disco → logs de `web`, `api-usuario` y `rag` en CloudWatch Logs y memoria y disco con el agente de CloudWatch; `agente` y `phoenix` siguen en la máquina | Depurar exigía entrar por Session Manager y los fallos de la EC2 no avisaban a nadie. Base para las alarmas. Descartados: Grafana + Loki en la misma EC2 (3-4 contenedores más en 4 GB y duplica Phoenix y CloudWatch), Amazon Managed Grafana (9 $ por usuario y mes), métricas propias del agente con EMF (unos 5-8 $/mes `[estimación]` y repiten Phoenix) y enviar también los logs del agente (duplican sus trazas) |
| 2026-10-23 | Trazas del agente en producción | Phoenix solo en local (perfil `observabilidad` del compose) → **Phoenix como quinto contenedor de la EC2**, sin puerto propio, servido por el nginx de la web en `/phoenix/` y **siempre con la contraseña de la web** (403 si no hay contraseña). El agente le envía las trazas por la red interna de Docker. Imagen pública fijada en el compose (`20.19.0`), SQLite en un volumen del disco de la EC2 | Las trazas de cada turno (fases, tiempos, tokens, errores) son la observabilidad del asistente y solo existían en el portátil de quien probaba. Descartados: Phoenix Cloud de Arize (manda a un tercero las preguntas de los usuarios), publicar su puerto 6006 (un segundo puerto sin autenticación), un balanceador con reglas por ruta (unos 16 $/mes `[estimación]`), guardar las trazas en la RDS (ata Phoenix a los horarios de la base y mete un esquema ajeno), copiar la imagen a ECR (un repositorio y un job más para una imagen que no se construye) y la autenticación propia de Phoenix (usuarios y API keys, pero exige un paso manual tras el primer arranque y un secreto más en SSM; no compensa con una sola persona operando) |
| 2026-10-23 | Acceso a la web de la EC2 | 80 para la IP del equipo y para todo internet, y 443 abierto a todo internet → **solo el 80, abierto a todo internet con contraseña** | La contraseña ya protegía la web y `/phoenix/`, así que la regla de la IP del equipo sobraba. En el 443 no escucha nada: su regla solo servía para que el navegador que prueba HTTPS primero recibiera un rechazo inmediato. Se acepta que ese navegador tarde más en pasar a HTTP a cambio de no tener abierto un puerto sin uso |
| 2026-10-23 | Alarmas | Dos alarmas, solo de la Lambda → **diez alarmas** que cubren también la web (5xx), Bedrock (errores y tokens al día), la EC2 (estado, créditos de CPU, memoria, disco) y la RDS (espacio), con 3 filtros de métricas sobre el log de nginx y un panel; todo en la plantilla de CloudFormation `jupiter-observabilidad` | Un fallo de la web, del modelo o de la máquina no avisaba a nadie, y un bucle del agente o la web abierta a internet podían disparar el gasto en Bedrock sin que nadie lo viera. Descartados: un script con `put-metric-alarm` por alarma, como el resto de `deploy/aws/` (con ocho alarmas se desvía de lo que hay en AWS sin que nadie lo note), crearlas en la consola (no se revisan en un PR) y métricas propias del agente (Phoenix ya lo mide) |
| 2026-10-23 | Evaluación del asistente | Solo *offline* (baterías de preguntas con respuesta esperada y LLM juez) → **offline + valoración 👍/👎 de los usuarios** pedida en la web y anotada en Phoenix sobre el span del turno | Las baterías miden lo que el equipo pregunta, no lo que preguntan los usuarios ni si les sirve. Votar en Phoenix deja el voto junto a la traza (ruta, tema, coste) sin tabla nueva. Descartados: una tabla de votos en la RDS (apagada fuera de horario y separa el voto de su traza), un 👍/👎 en cada respuesta y una encuesta abierta (texto libre de usuarios que no se puede versionar) |
| 2026-10-23 | KPIs de uso del asistente | Script local `kpis_phoenix.py` que leía Phoenix y escribía un informe markdown o HTML → **endpoint `GET /kpis` del agente**, reenviado por api-usuario, para verlos en la web desplegada; **script eliminado** | Ver los KPIs exigía ejecutar el script en un portátil con la contraseña de la web, y el tribunal no lo vería. El cálculo pasa al agente, dueño de la lógica del turno, y comparte con el informe de evaluación el árbol del turno y la tabla de precios. Descartados: mantener el script además del endpoint (dos sitios con la misma lógica), que api-usuario lea Phoenix (duplica el turno) y una página estática regenerada cada hora |
| 2026-10-23 | Acceso a la web de la EC2 | 80 abierto a todo internet **con contraseña**, IP automática que cambiaba en cada encendido → **web pública sin contraseña en una IP elástica** (fija), `/phoenix/` cerrado (403) para todos y **presupuesto diario de Bedrock** (AWS Budgets, 2 $/día, solo aviso por correo) | El tribunal tiene que entrar a probarla sin pedir credenciales y con un enlace que no cambie. La IP elástica cuesta lo mismo que la automática (0,005 $/h). Riesgo asumido: la API y el chat no tienen autenticación y cada pregunta se paga en Bedrock (0,001–0,003 $ `[estimación]`). Descartados: un cupo diario de preguntas en el agente (corte inmediato, pero código nuevo en el agente y la API a días de la entrega), una *budget action* que deniegue Bedrock al rol de la EC2 (AWS actualiza los costes unas tres veces al día: corta horas tarde y hay que quitarla a mano), las cuotas de servicio de Bedrock (son por minuto y solo admiten subidas) y mantener Phoenix con contraseña (exigía separar su contraseña de la de la web en `arrancar.sh` y un permiso IAM nuevo) |

**Decisiones abiertas que alimentarán esta tabla:** modelo del agente en Bedrock (Ministral 14B,
gpt-oss-120b, Qwen3 Next 80B (2026-10-16) o Nemotron 3 Super 120B (2026-10-18); Claude Haiku 5.5 no está
disponible en la cuenta;
aplazado hasta poder evaluar las herramientas SQL y de ML, §13) El RAG irá en la EC2 (§8.9). La opción de red en AWS ya está decidida y aplicada (opción A, §8.4).

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

Usuario ─▶ Web (nginx: SPA React + proxy /api/*) ─▶ ApiUsuario (FastAPI) ──▶ PostgreSQL (lecturas: estaciones, series)   (Fase 4)
                            └─ /chat, /chat/stream ──▶ Agente ─▶ LLM vía LlamaIndex (Bedrock | OpenAI-compat)
                                                         ├─ buscar_evidencias ─▶ rag.api (HTTP)   (Fase 3, en construcción)
                                                         └─ consultar_datos ─▶ redactor SQL (LLM) ─▶ validador
                                                                             ─▶ PostgreSQL: vista mediciones_bloques (solo lectura)
                    └─ /phoenix/* ──▶ Phoenix ◀── trazas OpenTelemetry de cada turno ── Agente
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
| 3 | LLM con *tool use* | `Agente` con documentación validada, memoria, streaming y preguntas mixtas. SQL evaluado en dos lotes con Bedrock: aún no cumple el criterio (§7.3.1). 126 tests. Pendiente decidir qué comprobaciones bloquean respuestas. Otro miembro se encarga del despliegue. `LLMOrchestrator` descartado (§7.3.2) |
| 4 | Informes y dashboard | **Implementada** (2026-10-03; rediseño 2026-10-07/08): `ApiUsuario` con `/estaciones`, `/estaciones/{codigo}/series`, `/chat` y `/chat/stream` (proxy al agente, con stub), tests propios y job de CI; dashboard web en `Web/` (portada Home, Estadísticas, Estaciones con detalle, asistente como burbuja flotante) servido por el contenedor `web` (nginx) junto al perfil `api` del compose. Pendiente: definir `AGENTE_URL` para conectar el chat al agente y verificar la pila completa |

**Stack actual:** Python 3.12 · pandas · NumPy · scikit-learn · PostgreSQL 18 (RDS) · SQLAlchemy ·
pyarrow · ChromaDB · sentence-transformers (`multilingual-e5-base`) · FastAPI · pytest ·
GitHub Actions · AWS (Lambda, S3, RDS, EventBridge, CloudWatch) · React 19 + TypeScript (Vite) ·
TanStack Query · Recharts · nginx · Docker Compose · OpenTelemetry + Arize Phoenix (trazas del agente).

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
3. Inserción del horario crudo en tabla de *staging* + `INSERT ... ON CONFLICT`, que solo
   actualiza una hora si antes era inválida y ahora es válida.
4. Agregación a bloques, features e Isolation Forest. La cobertura de la noche se mide sobre las 3
   horas que publica la API (20–22, §6.5 bis).
5. **Solo los bloques cerrados**: se descartan los del día cuya última hora publicable (6, 12, 19
   y 22) aún no ha terminado. Por reloj y no por lo publicado: si la API se retrasa, el bloque se
   escribe con menos cobertura y la carga siguiente lo completa.
6. *Upsert* de `resumen_datos_ml` con `ON CONFLICT DO UPDATE`.

**Cuándo se ejecuta:** al cierre de cada franja, a las 07:45, 13:45, 20:45 y 23:45 (§2). Cada
ejecución recalcula todos los bloques cerrados del día. `--reprocesar AAAA-MM-DD` rehace los bloques
desde el horario guardado, sin llamar a la API.

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
- **El Isolation Forest detecta sobre todo fallos operativos y deja pasar episodios ambientales
  extremos** (comprobado el 2026-10-22 sobre `resumen_datos_ml.parquet`). El 74,9 % de sus marcas
  tienen cobertura < 0,7 (entre 57 % en NO y 88 % en PM2.5), y solo marca el **12,7 %** de los
  bloques con |z| > 3 (2.198 de 17.251; en PM10, el 7,3 %). Caso más claro, la **calima del
  15-03-2022**: el z-score marca 52/52 bloques de PM10 (mediana de z 12,2) y 31/32 de PM2.5; el IF,
  **7/52 y 0/32**. Los bloques de PM10 de ese día, con cobertura 1,0 y medias de 137–528 µg/m³,
  puntúan 0,67–0,71, justo bajo el umbral del modelo de PM10 (0,694). Lectura: la puntuación del IF
  se satura (a partir de cierto punto, un valor más extremo no se aísla antes) y el cupo del 1 % lo
  ocupan sobre todo los fallos de cobertura. Los dos detectores son **complementarios**: z-score
  para lo ambiental, IF para lo operativo. Hoy `IS_ANOMALY` es solo la salida del IF, así que la web
  y el agente no verían ese episodio como anómalo.

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
franja abierta. De ahí que la ingesta solo escriba **bloques cerrados** y se ejecute al cierre de
cada franja (§5.4): el recuento es estable y comparable entre días. Comprobado de nuevo el
2026-10-23 a las 18:49 con la API real: la tarde abierta (5 de 7 horas, cobertura 0,714) tenía 8
bloques marcados, todos con z-score normal y por encima del 0,7 de la web, es decir, visibles como
anomalías; madrugada y mañana, cerradas, ninguno.

**Confirmado en producción, no solo en el test.** Tras desplegar el arreglo, la ejecución del
2026-09-19 corrigió 742 horas que habían quedado como placeholder, y las anomalías del día
volvieron al rango normal (13, frente a las 97 falsas registradas a media tarde antes del arreglo).
Las 126 filas `N` que quedan son íntegramente la hora 23 (una por cada serie activa), la
limitación permanente ya documentada — no un resto del bug.

### 6.5 bis Producción del 16-09 al 21-10-2026 (consultado en RDS el 2026-10-22)

- **36/36 días con carga**, 38 ejecuciones de la Lambda (2 manuales), **0 errores** (CloudWatch);
  duración media 8,5 s, máxima 23,2 s. 92.400 horas cargadas (93,4 % válidas) y 15.336 bloques
  nuevos (418–428 al día: 107 series × 4 bloques).
- **517 bloques marcados (3,4 %)**, frente al 1 % del histórico; entre 1 y 51 al día (mediana 12,5).
  Por bloque: noche **7,19 %** (histórico 0,44 %), mañana 4,13 % (2,19 %), tarde 1,48 % (0,84 %),
  madrugada 0,68 % (0,53 %).
- **Causa del exceso nocturno: la hora 23 nunca llega** (§11). Todos los bloques de noche en vivo
  tienen cobertura 0,75; en el histórico solo el 0,59 % la tienen y de esos el IF marca el 6,3 %,
  la misma tasa que en producción. Como 0,75 supera el filtro de la web (≥ 0,7), **se muestran como
  anomalías**: falso positivo sistemático. 250 de las 276 marcas de noche tienen cobertura ≥ 0,7.
- **Arreglo (2026-10-23):** en tiempo real, la cobertura de la noche se mide sobre las 3 horas que
  la API publica (20–22). Simulado sobre los 318.000 bloques de noche del histórico con el modelo
  de producción:

  | Bloque de noche | Marcados | Marcados con cobertura ≥ 0,7 |
  |---|---|---|
  | Horas 20–23, sobre 4 (como se entrenó) | 0,44 % | 0,22 % |
  | Horas 20–22, sobre 4 (producción hasta ahora) | **8,69 %** | 8,22 % |
  | Horas 20–22, sobre 3 (arreglo) | **0,49 %** | 0,18 % |

  El arreglo coincide con el cálculo de 4 horas en el 99,65 % de los bloques; quitar la hora 23
  apenas mueve el z-score (+0,02 de media).
- **Comprobado en producción (2026-10-23):** `--reprocesar 2026-09-16` reescribió 15.869 bloques
  con el cálculo nuevo. Del 16-09 al 22-10 (consulta de solo lectura a la RDS):

  | Bloque | Marcados antes (16-09 a 21-10) | Marcados tras reprocesar (16-09 a 22-10) | Cobertura media |
  |---|---|---|---|
  | noche | 7,19 % | **0,74 %** (29 de 3.944) | 0,75 → **0,989** |
  | mañana | 4,13 % | 4,22 % (166 de 3.936) | 0,967 |
  | tarde | 1,48 % | 2,20 % (87 de 3.947) | 0,976 |
  | madrugada | 0,68 % | 0,66 % (26 de 3.934) | 0,996 |

  En total, 308 de 15.761 bloques (1,95 %, frente al 3,4 %). Las marcas de noche que la web
  presenta como anomalías (cobertura ≥ 0,7) bajan de 250 a **11**. La noche queda en el nivel del
  histórico (0,44 %, 0,49 % en la simulación). La mañana en torno al 4 % es un rasgo del modelo,
  también presente en el histórico (§8.7). La subida de la tarde no está explicada `[por
  confirmar]`; el periodo incluye un día más.

### 6.6 Limitaciones del detector

- **Sin etiquetas:** no se pueden calcular precisión ni *recall*; la evaluación es cualitativa
  (casos extremos, solapamiento con el baseline y eventos conocidos).
- **Fuga de información en el baseline:** el valor esperado se calcula con **todo** el histórico,
  incluido el periodo evaluado. En producción debería ajustarse solo con datos de entrenamiento.
- El baseline no pondera la recencia, aunque la contaminación baja con los años.
- **Infradetección de episodios ambientales extremos** (§6.4): el IF solo marca el 12,7 % de los
  bloques con |z| > 3 y no marcó la calima del 15-03-2022 en PM2.5. Mejora directa: combinar
  ambos detectores (`IS_ANOMALY` = IF o |z| > 3) y etiquetar el tipo (ambiental u operativa).

### 6.7 Predicción a corto plazo (t+1 por bloques) — en construcción

Segundo modelo de ML del proyecto, para la herramienta `PREDICCION` del agente (hoy una frase
fija). Predice la **media del día siguiente** por (estación, contaminante, bloque). Rama
`feature/ml-prediccion`; notebook `04_entrenamiento_prediccion.ipynb`; funciones puras en
`src/etl/features_prediccion.py` e `inferencia_prediccion.py` (mismo reparto notebook/`src` que el
detector). **Alcance de esta iteración:** solo el modelo y su evaluación; la conexión con el agente,
el pipeline y el despliegue quedan para iteraciones posteriores.

**Diseño.** Un modelo por contaminante (convenio del detector), `HistGradientBoostingRegressor`.
Features (16): la media del día t, lags 1–6 del mismo bloque, medias móviles 3 y 7 días,
climatología del mes objetivo, cobertura, bloque y estación, y el calendario de la fecha objetivo.
Rejilla diaria completa por serie para que un hueco de ingesta quede como NaN y no arrastre el valor
de hace dos días; HGB trabaja con esos NaN sin imputar. Baselines: persistencia (lo de hoy),
climatología (valor típico del mes, solo train) y persistencia semanal (lag 6).

**Anti-fuga.** La climatología se recalcula **solo con el train** (la columna del parquet se calculó
con todo el histórico); en la frontera, una fila de train cuyo objetivo cae en el test se descarta
(`FECHA_OBJETIVO <= corte` para train, `FECHA > corte` para test). La configuración de HGB se elige
contra una validación **temporal** interna, nunca contra el test.

**Resultados (2026-10-14, histórico completo de S3).** Entrenado con `resumen_bloques.parquet`
(1.274.644 bloques, 24 estaciones, 2018-01-01 → 2026-04-30, sin huecos de día). Corte temporal
2025-04-30 → **test de un año completo** (2025-05 → 2026-04, 147.790 filas comparables); train 1,1 M
filas. Configuración elegida por validación temporal interna: «suave» (`max_iter=120, lr=0,05,
min_samples_leaf=50`). Se descarta la climatología precalculada del parquet (se computó con todo el
histórico = fuga) y se recalcula solo con el train.

MAE global en µg/m³: **persistencia 10,46**, climatología 15,00, lag semanal 13,38; **modelo 10,11**
→ *skill* global **+0,03**. El modelo **bate a la climatología en los 6 contaminantes** y a la
persistencia en **5 de 6**: O3 +0,12, NO2/PM10/PM2.5 +0,08, NOx +0,01; solo el **NO** pierde
(−0,18). Por bloque, la mañana es donde más aporta (skill +0,12); noche y tarde quedan a la par
(−0,01). El criterio propuesto (skill > 0 global y en ≥4/6, y batir a la climatología en todos)
**se cumple**, con margen modesto. Los peores errores son picos nocturnos extremos de NOx
(400–580 µg/m³) que ni el modelo ni la persistencia anticipan: episodios puntuales, la limitación
esperable sin meteorología.

**Lectura.** A 1 día vista la persistencia es un rival fuerte y el modelo la mejora poco pero de
forma consistente y medida sobre un año fuera de muestra; su valor real frente a la climatología
(el «¿cómo suele estar?») es claro. Sobre el CSV provisional de ~4 meses (2026-10-11) el modelo
**perdía** contra la persistencia (skill −0,28): la diferencia la hizo el volumen y la
estacionalidad del histórico, no un cambio de diseño.

**Hallazgo de calidad del dato** (para el equipo): en el CSV live de CI hay bloques con
`MEDIA = 0,0` marcados como válidos (flag V) seguidos de valores altos —ceros placeholder que el
filtro de validez no descarta—; en el parquet histórico no se observó ese problema.

**Segundo horizonte, t+2 (2026-10-21).** El modelo predice el día siguiente al último dato; durante
el día los datos llegan hasta ayer, así que h1 cae en **hoy** y «¿cómo estará mañana?» —la pregunta
más probable— quedaba sin respuesta. Se entrena un segundo bundle idéntico con `HORIZONTE = 2`
(`models/prediccion_bloques_h2.joblib`; el nb04 queda parametrizado para reproducirlo). Mismo año de
test: MAE modelo 11,86 vs 12,80 de la persistencia → **skill global +0,07**, mejor que a t+1: al
alejar el objetivo la persistencia se degrada más deprisa (+22 %) que el modelo (+17 %). Bate a la
climatología en los 6 contaminantes y a la persistencia en 5 de 6 (NO −0,18, igual que a t+1);
configuración elegida por validación interna: «sin regularizar». La persistencia semanal a h=2 es el
lag 5 (t−5 → t+2 = 7 días). Verificado con un replay en terminal (datos hasta ayer → h1 = hoy,
h2 = mañana, comparado contra lo medido). La herramienta del agente debe elegir el horizonte en
código: `fecha pedida − último día completo de datos` (0 → ya está medido, consultar datos; >2 →
fuera de alcance), nunca el LLM — el mismo convenio que las fechas de `consultar_datos`.

**Servicio HTTP (2026-10-22).** La predicción se expone como servicio propio, `src/prediccion`
(paquete `prediccion`, puerto 8011), **espejo de `src/rag`**: publica su definición de herramienta
en `GET /prediccion/herramienta` (función `predecir_calidad_aire`), `GET /salud` y
`POST /prediccion` con `{fecha_objetivo, estacion?, contaminante?, bloque?}`. Alternativa
descartada: importar los modelos en el propio agente — habría metido scikit-learn y los bundles en
un servicio deliberadamente ligero y acoplado los despliegues. El servicio concentra las dos reglas
que no deben decidir ni el agente ni el LLM: el **ancla** (último día con los 4 bloques: un día a
medio ingerir desplazaría los horizontes) y el **horizonte** (`fecha_objetivo − ancla` → bundle h1
o h2; `<=0` → `ya_medido`, `>2` → `fuera_de_alcance`). Lee la misma vista `mediciones_bloques` del
agente (solo lectura); la fecha objetivo la calculará el código del cliente, nunca el LLM. Se
corrigieron de paso los dos pendientes: la climatología del train ya viaja **dentro de los bundles**
y `preparar_dataset` descarta una `MEDIA_ESPERADA` precalculada en la entrada (la colisión que
rompía la inferencia con el parquet). 21 tests nuevos (motor + contrato HTTP, sin base de datos ni
sklearn: lector inyectado y modelos dobles); suite unit 150 en verde.

**Pendiente:** cerrar el criterio de éxito con el equipo; el cliente del agente (herramienta fina
tipo `tools/rag.py`, cambio de la frase fija en `intencion.py` y `periodos.py` con expresiones de
futuro, donde «mañana» exige desambiguar día frente a bloque matinal); y el despliegue del servicio
(Dockerfile + compose, como el RAG). Trabajo futuro: valorar predecir la **probabilidad de superar
un umbral** (aviso de episodio), que es lo que más piden los usuarios y donde la persistencia no
ayuda.

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
sin herramientas ofrecidas (`CHARLA`, o `DESCONOCIDA` con el RAG caído), la síntesis de datos y la
síntesis forzada.
Todo lo demás sale entero al final como `passthrough`: frases fijas, `sin_evidencia`, la ruta
documental (su JSON se valida antes de entregarse) y el texto libre de un turno con herramientas
ofrecidas, que puede acabar descartado. Así el cliente nunca ve un texto que luego se retira. El
evento `done` lleva siempre `fuentes` y `advertencia`, salga el texto como tokens o como `passthrough`
(los dos coinciden si hay `passthrough`). El turno no sabe de HTTP: recibe un **emisor opcional por turno** y la capa HTTP lo conecta a una
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
`passthrough` + `done`, con `session_id`, `traza_id: null` y las mismas fuentes y advertencia. **Límite:** el orquestador no conserva
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
  ($0,24 / $0,24 por 1M de tokens de entrada / salida en Irlanda al elegirlo; $0,23 / $0,23 desde el
  2026-10-08) y **gpt-oss-120b** ($0,18 / $0,70).
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
  contaminante, días con dato en los rankings, «hora punta» = bloque de mañana, un periodo sin datos
  no se sustituye por otro. Las fechas no las calcula: recibe el periodo calculado en Python (abajo).
- **Validador** (`sqlglot`, dialecto PostgreSQL): una sentencia de consulta, solo la vista (las CTE
  propias cuentan), sin DML dentro de un `WITH`, sin `INTO` ni `FOR UPDATE`, sin funciones de
  sistema, `LIMIT` impuesto (tope + 1 para que el DAL detecte el truncado; un `LIMIT` menor que
  el tope es del modelo y se respeta). Es la primera barrera; el rol sigue siendo la real.
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
- Evaluada con Bedrock en dos lotes (resultados abajo). La ruta de datos añade dos llamadas al
  LLM por pregunta: redactor SQL y síntesis.

**Preguntas mixtas: datos y documentación (2026-10-09).** El clasificador admite una o dos
intenciones. La respuesta muestra primero las mediciones y después la explicación documental,
con citas y aviso sanitario. Cada parte puede responder aunque la otra falle. El bloque documental
no recibe las filas SQL y se valida antes de mostrar la respuesta completa.
Si se piden datos y predicciones, se consultan los datos y se avisa de que no se hacen predicciones.
Una respuesta mixta requiere unas 6 llamadas al LLM, más una si hay reparación `[estimación]`.

**Clasificador.** Tras ajustar las instrucciones, gpt-oss-120b acertó las 29 preguntas originales
en dos pasadas; Ministral, 27/29 en ambas. Se eligió gpt-oss-120b mediante
`LLM_MODELO_CLASIFICADOR`, independiente del modelo de conversación. No se siguió ajustando
Ministral para evitar adaptarlo demasiado a las preguntas de prueba. Las dos preguntas mixtas
nuevas obtuvieron 2/2 y 1/2 con gpt-oss: el clasificador aún falla fuera del lote original.

**Evaluación SQL (2026-10-09).** Se prepararon 16 casos con resultados de referencia (*gold*)
calculados en PostgreSQL local (`gold_datos.sql`). El criterio exige acertar al menos 9/11 preguntas
de datos, las 2 mixtas y los 3 casos límite. Los veredictos de ambos lotes están
**[por confirmar en la revisión humana]**; los costes son estimaciones por consumo.

Las definiciones cambian el resultado: en Plaza de España hubo **22 días** de 2025 con media diaria
de PM10 superior a 50 µg/m³; contar días con algún pico horario daría 122. Para las pruebas,
«media anual» significa últimos 12 meses; «semana pasada», semana natural anterior; y «patrón en
hora punta», diferencia entre la mañana y el resto del día.

**Primer lote.** Siete combinaciones, coste total 0,31 $. Ninguna cumplió el criterio.
MiniMax M2.5 se probó como modelo de referencia al no disponer de Haiku 5.5. Fue más caro y acertó
menos, por lo que se descartó (§2). Las latencias son orientativas: hubo tres ejecuciones simultáneas.
Detalle: `resultados/revision_featuresql_20261009.md`.

| Conversación · redactor | Datos (/11) | Mixtas | Límites | SQL a la 1.ª | Turno (mediana) | Coste lote |
|-|-|-|-|-|-|-|
| Ministral · mismo | 3 | 0/2 | 1/3 | 3/11 | 15,4 s | 0,029 $ |
| Ministral · Qwen3 Coder | 3 | 0/2 | 2/3 | 7/11 | 6,8 s | 0,023 $ |
| Ministral · gpt-oss-20b | 3 | 0/2 | 3/3 | 7/11 | 10,9 s | 0,030 $ |
| gpt-oss-120b · mismo | 4 | 0/2 | 2/3 | 11/11 | 13,7 s | 0,038 $ |
| gpt-oss-120b · Qwen3 Coder | 3 | 0/2 | 1/3 | 7/11 | 8,7 s | 0,040 $ |
| gpt-oss-120b · gpt-oss-20b | 2 | 0/2 | 3/3 | 9/11 | 17,1 s | 0,043 $ |
| MiniMax M2.5 (techo) | 2 | 0/2 | 2/3 | 6/11 | 23,9 s | 0,085 $ |


**Cambios antes del segundo lote.** Se corrigió el aviso de resultados recortados y se pasó el
cálculo de periodos a Python. El redactor recibe la pregunta original y las fechas calculadas;
el validador rechaza fechas literales fuera del rango y funciones como `CURRENT_DATE` o `NOW()`.
También se aclaró cómo agrupar resultados, usar las estaciones de la vista y escribir SQL válido
para PostgreSQL. Se prefirió ajustar el prompt a reescribir expresiones `ROUND` en el validador.

`business/periodos.py` interpreta el periodo de la pregunta y, en preguntas de seguimiento,
consulta las dos anteriores. «Ahora mismo» usa el último día disponible; «ayer» mantiene su fecha
literal; «esta semana» usa los últimos 7 días disponibles. Sin un periodo reconocido, el modelo
usa un calendario de apoyo. Las 15 consultas de referencia con SQL pasaron el validador.

**Segundo lote.** Tres combinaciones ejecutadas en serie, con coste total de 0,10 $.
Los periodos reconocidos se resolvieron bien y el recorte de filas se avisó. Solo hubo un error de
sintaxis o funciones de PostgreSQL. Aun así, las tres combinaciones acertaron solo 4/11 preguntas
de datos y ninguna respuesta mixta completa. Detalle: `resultados/revision_featuresql2_20261009.md`.

| Conversación · redactor | Datos (/11) | Mixtas | Límites | SQL a la 1.ª | Turno (mediana) | Coste lote |
|-|-|-|-|-|-|-|
| gpt-oss-120b · mismo | 4 (+3 parciales) | 0/2 | 1/3 | 10/10 | 12,0 s | 0,039 $ |
| gpt-oss-120b · Qwen3 Coder | 4 | 0/2 | 1/3 | 10/11 | 7,0 s | 0,033 $ |
| Ministral · Qwen3 Coder | 4 | 0/2 | 2/3 | 9/10 | 9,3 s | 0,029 $ |

**Tercer lote.** Mismas tres combinaciones, en serie, con las correcciones posteriores al segundo
lote; coste 0,10 $. Ninguna cumple el criterio. La mejor, gpt-oss-120b como conversación y redactor,
sube de 4 a 6/11 en datos; Ministral · Qwen3 Coder es la primera con 3/3 en límites. Veredictos
**[por confirmar en la revisión humana]**. Detalle: `resultados/revision_featuresql3_20261009.md`.

| Conversación · redactor | Datos (/11) | Mixtas | Límites | SQL a la 1.ª | Turno (mediana) | Coste lote |
|-|-|-|-|-|-|-|
| gpt-oss-120b · mismo | 6 (+2 parciales) | 0/2 | 2/3 | 10/10 | 11,6 s | 0,033 $ |
| gpt-oss-120b · Qwen3 Coder | 3 (+4 parciales) | 1/2 | 2/3 | 8/11 | 8,1 s | 0,039 $ |
| Ministral · Qwen3 Coder | 5 (+1 parcial) | 0/2 | 3/3 | 8/10 | 5,4 s | 0,030 $ |

**Arreglos deterministas y cuarto lote (2026-10-12).** Antes de comparar con el catálogo se dejó el
SQL libre en su mejor versión con reglas de código, no de prompt: el validador sustituye cualquier
`LIMIT` del modelo por el tope de filas (el orden lo da el `ORDER BY`), rechaza `AVG(media)` sobre la
vista y, si la pregunta no pide periodo (ni reconocido ni mencionado), rechaza todo filtro de fecha y
se consulta el histórico completo. Además, la descripción de la herramienta ya no dice «por día y
bloque» y el «NO2 y PM10 por defecto» no se aplica a anomalías ni a qué estaciones miden. 133 tests.
Mismas tres combinaciones; coste 0,09 $. Ninguna cumple el criterio. Veredictos **[por confirmar en
la revisión humana]**. Detalle: `resultados/revision_featuresql4_20261012.md`.

| Conversación · redactor | Datos (/11) | Mixtas | Límites | SQL a la 1.ª | Turno (mediana) | Coste lote |
|-|-|-|-|-|-|-|
| gpt-oss-120b · mismo | 8 (+1 parcial) | 1/2 | 2/3 | 10/10 | 10,6 s | 0,033 $ |
| gpt-oss-120b · Qwen3 Coder | 7 | 1/2 | 2/3 | 7/10 | 6,0 s | 0,030 $ |
| Ministral · Qwen3 Coder | 6 (+2 parciales) | 1/2 | 2/3 | 8/10 | 5,6 s | 0,028 $ |

**Hallazgos y estado actual (2026-10-12).** La mejor combinación sube de 6 a 8/11 (lo estimado tras
el tercer lote) y ya no tiene ningún SQL inválido ni recortado. Los casos que quedan no son de forma
del SQL: la definición de hora punta (mañana − resto), una diferencia que la síntesis no puede
calcular, el clasificador en dat-10, la mixta que no dice «ayer» y lim-02, que no dice que el SO2 no
se mide. Con Ministral, los fallos vienen de la pregunta reescrita y de la síntesis, que inventan
contaminantes y periodos.

| Problema | Corrección o pendiente |
|---|---|
| El límite de 60 filas impedía detectar el recorte | Corregido antes del segundo lote: se permite leer una fila adicional |
| El modelo cambiaba los periodos al reformular la pregunta | Periodos calculados en Python antes del segundo lote |
| Agrupaciones incorrectas, estaciones copiadas y errores de SQL | Prompt mejorado; quedó un error de PostgreSQL en el segundo lote |
| La descripción de la herramienta inducía filtros de anomalías y contaminantes no pedidos | Descripción simplificada en dos pasos; resuelto en el cuarto lote con gpt-oss. Qwen3 Coder aún devuelve filas sin agregar |
| Una media mezclaba varios contaminantes o no estaba ponderada | El validador rechaza las dos; resuelto en el cuarto lote |
| La síntesis inventaba el periodo o decía «0 días» fuera del histórico | Periodo explícito y frase fija para fechas sin cobertura; resuelto en el tercer lote |
| Ministral inventa estaciones o periodos al reformular | Sin periodo en la pregunta, el validador rechaza las fechas inventadas (cuarto lote). Sigue inventando contaminantes y, en la síntesis, el periodo. Pendiente |
| `LIMIT 1` pierde parte del ranking | El validador impone siempre el tope; resuelto en el cuarto lote |
| Se interpreta mal la comparación mañana − resto (dat-05) | Pendiente en los cuatro lotes |
| «NO2 y PM10 por defecto» aplicado a preguntas de anomalías: pierde una anomalía de O3 | Regla con excepciones; resuelto en el cuarto lote |
| La síntesis inventa mediciones cuando no hay filas | lim-02 sin cifras inventadas en el tercer lote; Ministral inventa con filas que no responden a la pregunta (mismo valor para dos años). Pendiente |
| Las respuestas mixtas confunden datos recientes con actuales y contienen avisos contradictorios | Pendiente |
| El clasificador varía en preguntas fuera de las 29 originales | Pendiente: dat-10 sale documental en 3 de 3 en el cuarto lote |
| Dos SQL rechazados acaban en la frase de «no puedo consultar las mediciones», la de base caída | Pendiente (cuarto lote) |

**Validación de los últimos cambios.** 126 tests del agente y la integración con PostgreSQL en
verde; las 15 consultas de referencia pasan el validador. El tercer lote confirma la mejora con
Bedrock (de 4 a 6/11 en la mejor combinación), insuficiente para el criterio.

**Límites.** Con periodo calculado, el control temporal solo comprueba fechas literales, no filtros
por `ano`/`mes` ni aritmética de fechas. Sin periodo, una heurística decide si la pregunta menciona
tiempo («el invierno pasado»); si no, se rechaza cualquier filtro de fecha. Si menciona un periodo que
no reconoce, el modelo aún puede elegirlo; la respuesta indica
«no calculado» cuando filtra fechas, o «todo el histórico» cuando no las filtra. La comprobación
`cifras` tampoco garantiza fidelidad: dejó pasar «21 días» cuando las filas decían 22.

**Alternativa: catálogo de consultas fijas.** Evitaría algunos errores del SQL, pero seguiría
necesitando interpretar la pregunta y redactar la respuesta. Se corrigen primero esas partes
compartidas; el catálogo se reconsiderará si la siguiente evaluación no alcanza el criterio.
Tras el tercer lote, cuatro de los cinco fallos de la mejor combinación eran de forma del SQL; los
arreglos deterministas resolvieron tres (dat-01, dat-07 y dat-09). Decidido el 2026-10-12: construir el
catálogo y comparar las dos opciones con el mismo lote para la memoria. El catálogo puede resolver
dat-05 y dat-06 si sus consultas calculan la diferencia; el clasificador y la síntesis siguen igual.

**Catálogo cerrado (2026-10-12; sin evaluar aún con Bedrock).** Segunda forma de obtener el SQL,
seleccionable con `HERRAMIENTA_DATOS=libre|catalogo`. Bucle, periodos calculados en Python, síntesis
y comprobaciones no cambian: solo el paso que produce el SQL, para que la comparación mida eso. Un
LLM (el mismo de `LLM_MODELO_SQL`) devuelve en JSON una de 7 consultas y sus parámetros, y el código
construye el SQL: ranking, serie (día, mes o año), patrón horario (un bloque frente al resto, con la
diferencia), laborables frente a fin de semana (con la diferencia), días por encima de un umbral,
anomalías y estaciones. Reglas que en el libre dependían del prompt pasan a código: media ponderada,
NO2 y PM10 por defecto solo en las consultas de niveles, días con dato, diferencias ya calculadas y
lugares resueltos contra la lista de estaciones. Un contaminante que no se mide (SO2) o un lugar
inexistente da un resultado vacío con una nota, no un fallo. Verificado sin LLM: con la elección
correcta, las consultas del lote reproducen el gold en el Postgres local (dat-05 +8,1; dat-06 6,8;
dat-07 83,5 → 85,3; dat-08 22 días; mix-01 11,0 y 19,0). 143 tests; las 7 consultas corren en SQLite
y en PostgreSQL.

| Alternativa | Por qué no |
|-|-|
| Catálogo como herramienta con parámetros para el modelo de conversación (diseño de `LLMOrchestrator` y del plan inicial) | Ahorra una llamada, pero el modelo volvería a pasar fechas (justo lo que resolvieron los periodos en Python), la consulta que lanza el código cuando el modelo no consulta no sabría rellenar los parámetros y la comparación mezclaría dos cambios |
| Selector con llamada a herramienta en vez de JSON en texto | Exige que el modelo de Bedrock admita forzar la herramienta; el JSON sirve para todos y se valida igual |

Prueba de humo con Bedrock (2026-10-12, gpt-oss-120b como selector): 8 de 8 preguntas eligen la
consulta esperada a la primera, en 1 a 2 s, con las cifras del gold; dos turnos completos (hora punta y
SO2) responden bien por 0,003 $. No sustituye a la evaluación.

**Lote nuevo, no visto (2026-10-12).** Las 7 consultas y cuatro rondas de arreglos salieron de los
mismos 16 casos: compararlas solo con ese lote favorecería al catálogo y mediría arreglos hechos a
medida. Se preparó un segundo lote de 28 casos que no se usó para ajustar nada: 20 de datos (12 caben
en el catálogo con otros parámetros, 4 en parte, 2 no: máximo horario y porcentaje de anomalías, y 2
de robustez: periodo no reconocido y lugar ambiguo), 3 mixtas y 5 límites. Cada caso dice si cabe en el
catálogo, para separar el resultado. Gold calculado en PostgreSQL local con media ponderada `[pendiente
de revisión humana]`. Incluye respuestas que no son una cifra: dos estaciones prácticamente empatadas
(21,3 frente a 21,2) y un recuento cuyo resultado correcto es «ninguna» (media anual de NO2 > 40 en 2025).

**Catálogo en el lote nuevo (2026-10-12; veredictos en borrador, pendientes de revisión humana).**
Tres combinaciones (modelo de conversación · selector), 28 casos cada una, 0,146 $ en total:

| Combinación | Datos sí / parcial / no (20) | Datos que caben en el catálogo (14) | Mixtas | Límites | Turno mediana |
|-|-|-|-|-|-|
| gpt-oss-120b · mismo | 12 / 4 / 4 | 11 / 1 / 2 | 0/3 (3 parciales) | 2/5 | 7,2 s |
| gpt-oss-120b · Qwen3 Coder | 9 / 6 / 5 | 9 / 2 / 3 | 0/3 (3 parciales) | 4/5 | 6,4 s |
| Ministral · Qwen3 Coder | 9 / 1 / 10 | 9 / 1 / 4 | 0/3 (3 parciales) | 4/5 | 4,2 s |

Las 77 selecciones fueron JSON válido con una consulta existente a la primera. Las preguntas de cifras
simples (8 de 20) aciertan en las tres combinaciones, también con Ministral, que en el SQL libre
inventaba contaminantes y periodos: la selección no los admite. Con gpt-oss como selector, 11 de las 12
preguntas que caben y pasan el clasificador aciertan. Los fallos que quedan casi no son del catálogo: el
clasificador saca de la ruta de datos cuatro preguntas nuevas («¿cuántas anomalías detectó el modelo…?»
→ fuera de alcance; «¿qué contaminantes mide la estación…?» → documental), en las mixtas la parte
documental contradice a la de datos (0/3 en todas) y, cuando la consulta no responde exactamente,
Ministral inventa en la síntesis («18 estaciones superaron el límite anual»; porcentajes del 120 %)
mientras gpt-oss dice que no puede. Del catálogo: Qwen3 Coder como selector confunde lugares (Vallecas →
Villaverde) y periodos; una estación que no mide un contaminante da «sin datos» en vez de «no se mide»;
las series de más de 60 filas se truncan.

**SQL libre en el lote nuevo y comparación (2026-10-12; veredictos en borrador, pendientes de revisión
humana).** Mismas tres combinaciones y mismo lote, con el SQL libre del cuarto lote sin cambios, 0,147 $:

| Combinación | Libre: datos sí / parcial / no (20) | Catálogo (20) | Libre en lo que cabe (14) | Catálogo en lo que cabe (14) | Límites libre / catálogo |
|-|-|-|-|-|-|
| gpt-oss-120b · mismo | **15** / 3 / 2 | 12 / 4 / 4 | 10 / 3 / 1 | 11 / 1 / 2 | 2/5 · 2/5 |
| gpt-oss-120b · Qwen3 Coder | **16** / 2 / 2 | 9 / 6 / 5 | 10 / 2 / 2 | 9 / 2 / 3 | 2/5 · 4/5 |
| Ministral · Qwen3 Coder | 10 / 1 / 9 | 9 / 1 / 10 | 7 / 1 / 6 | **9** / 1 / 4 | 2/5 · 4/5 |

Mixtas 0/3 en los seis lotes. Coste y latencia equivalentes (el selector tarda lo que el redactor).
Lectura: **en preguntas no vistas, el SQL libre con gpt-oss responde más**; la diferencia sale de las 6
preguntas que no caben en las 7 formas del catálogo (máximo horario, porcentaje de anomalías, media por
franja, diferencia entre dos veranos), que el libre resuelve y el catálogo no. En lo que cabe, empatan.
**El catálogo protege con modelos débiles**: con Ministral acierta más en lo que cabe y sus notas en código
(«no se mide», «no hay estación») resuelven límites que el libre responde como «no hay datos». Fallos
propios del libre en este lote: la síntesis lee el código `dia_semana` (0 = lunes) como 0 = domingo; el
periodo que elige el SQL cuando Python no lo reconoce no vuelve a la síntesis; Qwen3 Coder omite una regla
del prompt (anomalías sin cobertura mínima). Lo que no depende del SQL (clasificador, mixtas, lugares
ambiguos) falla igual en las dos. Cautela: una pasada por combinación; diferencias de 1 o 2 casos están
dentro de la variación del clasificador.

Límites: el selector sigue siendo un LLM y puede elegir la consulta equivocada; una pregunta que no
encaja en las 7 formas se responde con la más parecida. No cambia el clasificador (dat-10), las
mixtas ni la síntesis.

**Lecciones.** Comparar respuestas con resultados de referencia descubrió fallos que no aparecían
en tests con pocas filas. El modelo más caro no mejoró los resultados: cambiar de modelo no bastó.
Además, el LLM puede copiar la descripción de una herramienta como si fuera una instrucción.
Cuatro rondas de arreglos guiadas por los mismos 16 casos arriesgan sobreajustar el sistema al guion:
el 8/11 mide menos de lo que parece. Las reglas en código (tope de filas, media ponderada, sin periodo
= histórico) funcionaron mejor que las del prompt: el modelo las puede ignorar y el validador no.

**Modelos alternativos: Qwen3 Next 80B, Nemotron 3 Super 120B y Claude Haiku 5.5 (2026-10-16 y
2026-10-18; veredictos en borrador, pendientes de revisión humana).** Se buscaron en Bedrock dos candidatos más, en la línea de precio de los
dos anteriores. Criterios: disponibles desde `eu-west-1` y con *tool use* por Converse. Los modelos grandes
(Opus, GPT-6 Astra a 10 $ / 50 $) quedaron fuera. Elegidos: **Claude Haiku 5.5** (`eu.anthropic.claude-haiku-5-5`,
0,11 $ / 0,55 $ por 1M de tokens, perfil `eu.`) y **Qwen3 Next 80B A3B** (`qwen.qwen3-next-80b-a3b`,
0,17 $ / 1,44 $, en la región, MoE con 3B activos y sin razonamiento). Precios de la API pública de precios de
AWS para Irlanda.
- **Nemotron 3 Super 120B** (`nvidia.nemotron-super-3-120b`, 0,18 $ / 0,78 $, en la región, MoE con 12B
  activos y razonamiento configurable): aplazado al principio por decisión del equipo; evaluado el
  2026-10-18 porque Haiku seguía bloqueado.
- Descartados: Nova 2 Lite
  (salida a 3,16 $); GLM 4.7 Flash (en *Legacy*, se retira en marzo de 2027); DeepSeek v3.2, GLM 4.7,
  Kimi K2.5, Qwen3 235B y Mistral Large 3 (no están en Irlanda o cuestan de 3 a 5 veces más).
- **Haiku 5.5 no se puede usar en la cuenta del máster**: `AccessDeniedException` («not available for
  this account»). Es lo mismo que encontró Carlos el 2026-10-09. Causa, con consultas de solo lectura:
  `get-use-case-for-model-access` responde que **nunca se ha enviado el formulario de caso de uso** que
  Anthropic exige una vez por cuenta (o en la cuenta de gestión de la organización). Sin él no se crea el
  acuerdo de Marketplace (`agreementAvailability: NOT_AVAILABLE` en todos los Claude). La oferta sí existe
  para la cuenta (`list-foundation-model-agreement-offers`). El formulario se envió el mismo día. Aun
  así, las llamadas siguieron dando el mismo error 7 minutos después y al día siguiente (2026-10-17). El acuerdo no se creó solo y en
  CloudTrail no aparece ningún intento de suscripción en Marketplace. La consola (*playground*) da el mismo
  error. El bloqueo afecta solo a Anthropic: todos los Claude, también desde `us-east-1`. Otros modelos
  de terceros (gpt-oss, MiniMax) sí tienen acuerdo. La cuenta es miembro de una organización y sus
  políticas no se pueden consultar desde ella. El texto del error («contact AWS Sales») apunta a una
  restricción de la organización o de AWS para esta cuenta `[por confirmar con el máster]`.
- Qwen3 Next (2026-10-16) y Nemotron (2026-10-18) se pasaron por **las mismas baterías y el mismo
  código** que Ministral y gpt-oss. Cada uno hizo de conversación, clasificador y SQL, en serie y una
  sola pasada. La base local se replicó
  con los datos de Carlos: 326.700 filas y una vista de 314.411, del 2024-04-30 al 2026-04-30. Sin esa
  réplica, el gold no sirve, porque la base de trabajo ya llegaba al 2026-05-19.

| Batería | Ministral 14B | gpt-oss-120b | Qwen3 Next 80B | **Nemotron 3 Super** |
|-|-|-|-|-|
| Clasificador, 29 preguntas originales | 27/29 | 29/29 | 27/29 (mixtas nuevas 1/2) | **29/29 (mixtas nuevas 2/2)** |
| Turnos aislados (12) | 12/12 | 12/12 | 12/12 | 11/12² |
| Conversaciones (11 turnos) | 11/11 | 11/11 | 10/11 | 11/11 |
| JSON válido a la primera (documental) | 6/6 | 6/6 | 6/6 y 9/9 en conversaciones | 7/7 y 9/9 |
| Datos, 16 casos (/11)¹ | 6 (+2 parciales) | 8 (+1) | 6 (+2) · SQL a la primera 9/11 | 6 (+3) · SQL a la primera 10/11 |
| Lote nuevo, SQL libre (/20) | 10 | 15 | **16** | **16** |
| Lote nuevo, catálogo (/20) | 9 | 12 | 10 (+4 parciales) | **14 (+3)** |
| Mixtas | 1/2 y 0/3 | 1/2 y 0/3 | 0/2 y 0/3 | 0/2 y 0/3 (4 parciales) |
| Pedir el prompt de sistema | Lo parafrasea (4 de 4) | Frase fija (fuera de alcance) | **Lo copia literal** | Frase fija y **copia literal** (1 y 1) |
| Turno documental, mediana | 8,3 s | 7,5 s | 7,9 s | **4,9 s** |
| Clasificador, mediana | 0,3 s | 1,0 s | 0,5 s | 0,35 s |
| Coste del lote de 12 turnos | 0,0059 $ | 0,0087 $ | 0,0072 $ | 0,0078 $ |

¹ Ministral y gpt-oss, del cuarto lote de Carlos. Ministral, con Qwen3 Coder como redactor. Los 16 casos
se usaron para ajustar el prompt con gpt-oss; el lote nuevo de 28 no se usó para ajustar nada.
Ministral en el lote nuevo, con Qwen3 Coder como redactor o selector.
² El fallo es de la ruta, no del contenido: con el benceno, la búsqueda reformulada pasó el umbral y la
respuesta fue «No puedo responder» por la ruta documental en vez de `sin_evidencia`.

- **En preguntas de datos no vistas, Qwen3 Next iguala al mejor de Carlos**: 16/20 con SQL libre
  (gpt-oss 15–16). Acierta las cifras simples, el máximo horario y el porcentaje de anomalías. Falla lo
  mismo que los demás: la diferencia entre dos veranos y la lectura de `dia_semana`. Además toma el
  «invierno pasado» como 2024-25.
- **Como selector del catálogo es peor que gpt-oss** (10 frente a 12/20). Las 24 selecciones son JSON
  válido, pero elige mal la consulta cuando la pregunta no encaja: franja horaria, día de la semana o
  subida entre veranos. En dos casos saca una conclusión falsa de una consulta que no responde: «13
  estaciones superaron el límite anual de NO2» (no lo superó ninguna) y «la mañana es la peor franja»
  (es la noche).
- **Seguridad: Qwen3 Next es el peor.** Cuando le piden el prompt de sistema lo copia entero en un
  bloque de código; en el otro caso de fuga lo parafrasea casi literal. La regla de fuga lo detecta en los
  dos, pero solo observa, así que la respuesta sale. Si se eligiera este modelo, `fuga` tendría que
  bloquear.
- Dos abstenciones en preguntas del protocolo de episodios (con-02.2 y adv-cif-02). Qwen pidió la
  búsqueda con `tema: proyecto` en vez de `normativa` y el RAG devolvió fragmentos que no venían al caso.
  Responde «No puedo responder», sin inventar.
- Un formato inválido del clasificador (`fuera_de_alcance: ninguno`) llevó una pregunta de datos a
  `DESCONOCIDA` en los dos lotes nuevos. El parser estricto hizo su trabajo: ninguna frase fija.
- Mixtas: 0 en todos los lotes, igual que con los otros modelos. Los datos salen bien; la parte
  documental dice «No puedo responder» con limitaciones que contradicen el bloque de datos.
- Coste de la pasada completa: unos **0,14 $** `[estimación por tokens de las trazas]`, frente a
  ~0,16 $ con gpt-oss. Las salidas son cortas (mediana de 425 tokens en la ruta documental, frente a
  562 de Ministral y 1.108 de gpt-oss), lo que compensa su salida más cara. **Toda la campaña de Carlos
  costó 1,21 $** (Cost Explorer, gasto en Bedrock del 2026-10-04 al 2026-10-14, sin IVA): 0,70 $ de
  gpt-oss-120b, 0,28 $ de Ministral, 0,11 $ de Qwen3 Coder, 0,08 $ de MiniMax y 0,03 $ de gpt-oss-20b.
- Lectura: Qwen3 Next es una alternativa real a gpt-oss para la conversación y el SQL libre, a un coste
  parecido y algo más rápido. Pierde como selector del catálogo y en la fuga del prompt. No cambia lo que
  ya fallaba con todos: clasificador fuera del lote original, mixtas y hora punta. Cautela: una sola
  pasada; diferencias de 1 o 2 casos están dentro de la variación. Detalle:
  `resultados/revision_qwen3next_20261016.md`.

**Nemotron 3 Super (2026-10-18).** El mejor resultado global de los cuatro en una sola pasada.
- **Clasificador 31/31**, incluidas las dos mixtas nuevas, con 0,35 s de mediana. Lleva 4 de las 5
  preguntas mixtas de los lotes de datos por la ruta mixta (solo nm-03 sale como `DOCUMENTAL`); Qwen,
  2 de 5. Aun así, ninguna mixta sale entera: la parte documental se abstiene o contradice al bloque de
  datos, como con los demás.
- **Datos: iguala a Qwen y gpt-oss con SQL libre (16/20) y es el mejor selector del catálogo (14/20
  y +3 parciales; gpt-oss 12).** Es el único que acierta con el catálogo el «invierno pasado»
  (2025-26) y la subida del ozono entre veranos (Escuelas Aguirre +5,3). SQL válido a la primera en
  19 de 20 consultas; las 25 selecciones, JSON válido.
- **El más rápido**: turno documental de 4,9 s de mediana (gpt-oss 7,5 s) y p95 del turno de 6,7 s.
  Escribe poco (324 tokens de salida de mediana en la ruta documental): el razonamiento por defecto de
  Bedrock no se nota ni en la latencia ni en el coste.
- Fallos, por orden de gravedad:
  - Copia literal del prompt de sistema cuando se lo piden en charla (la otra petición la para el
    clasificador).
  - Con el catálogo, conclusiones falsas cuando la consulta no responde: «13 estaciones superaron el
    límite anual», la media anual presentada como «valor horario más alto», porcentajes del 109–117 %.
  - **Inventa datos de Getafe** con la media de toda la ciudad.
  - Con SQL libre, lee `dia_semana` desplazado y presenta medias como «descensos» de 93 µg/m³.
- A favor en documentación: responde con citas los umbrales del protocolo de episodios (180 / 200 /
  400 µg/m³), donde gpt-oss y Qwen se abstenían.
- Coste de la pasada completa: unos **0,14 $** `[estimación por tokens de las trazas]`, como Qwen.
- Incidencia: la primera prueba falló por dos arranques en frío (primera llamada a Bedrock, 10 s en el
  clasificador; RAG recién levantado, 35 s por búsqueda). Repetida un minuto después, sin problema.
  Lección práctica: calentar el RAG y el modelo antes de medir.
- Lectura: en una pasada, Nemotron es el candidato más completo en acierto y velocidad, al nivel de
  gpt-oss o por encima, a coste parecido. Comparte con Qwen la fuga literal del prompt, así que `fuga`
  tendría que bloquear si se elige. Mismas cautelas: una pasada y veredictos en borrador. Detalle:
  `resultados/revision_nemotron_20261018.md`.

**Pendiente:** decidir entre SQL libre, catálogo o híbrido (catálogo como paso previo de comprobación de lugar y contaminante) con la comparación de arriba, elegir qué
comprobaciones bloquean respuestas e incorporar las herramientas de ML. Otro miembro del equipo
se encarga del contenedor y el despliegue en EC2 con Bedrock.

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
- **CloudWatch** (§8.7): 5 métricas propias de 10 gratuitas (3 filtros de logs + 2 del agente de
  CloudWatch), 1 panel de 3 gratuitos y logs muy por debajo de los 5 GB/mes. Las alarmas se cobran
  por métrica vigilada: las diez alarmas vigilan 11 métricas, porque la de errores de Bedrock suma
  dos. Una por encima de las 10 gratuitas ≈ **0,10 $/mes** `[estimación: confirmar con la factura]`.
- La instancia se creó con la **plantilla de capa gratuita** (750 h/mes de `db.t4g.micro` Single-AZ,
  20 GB de SSD de uso general y 20 GB de backups). La configuración elegida cabe entera en esos
  límites; se eligió **gp2** en lugar de gp3 precisamente porque es lo que cubre la capa gratuita.
  **Comprobado el 2026-10-22: la capa gratuita no aplica a la cuenta** (ver el coste real, abajo).
- **Encendido programado (del 2026-10-01 a la ingesta por franjas, §8.8):** la instancia solo
  estaba encendida de 22:00 a 01:30 y cuando el equipo la encendía a mano; parada solo se paga el
  disco y las copias. Real: 4,67 $ de RDS del 1 al 22 de octubre (abajo). Era posible porque la
  carga era diaria (23:45); con cargas también de día, la RDS pasa a estar siempre encendida.

**Coste real (Cost Explorer, sin impuestos, consultado el 2026-10-22).** La capa gratuita **no
aplica** a la cuenta: la RDS se cobra desde el primer día.

| Servicio | 16–30 sep | 1–22 oct |
|---|---|---|
| RDS (instancia, disco y copias) | 7,10 $ | 4,67 $ |
| IPv4 públicas (aparece como «VPC») | 1,72 $ | 2,64 $ |
| Bedrock | — | 1,49 $ |
| EC2 (instancia y disco) | — | 1,17 $ |
| Otros (ECR, S3, Systems Manager, Cost Explorer) | 0,03 $ | 0,37 $ |
| **Total** | **8,85 $** | **10,35 $** |

- Instancia RDS: 343,6 h en septiembre (24 h/día) y 110,9 h en octubre (~5 h/día con el encendido
  programado). Disco y copias se pagan igual con la base apagada.
- **IPv4 pública a 0,005 $/h por dirección**, también la IP elástica de la EC2 con la máquina
  apagada (404,8 h «idle» en octubre, 2,02 $). En octubre, una cuarta parte de la factura. No estaba
  en la estimación inicial.
- Systems Manager: 124 pasos de los runbooks de encendido y apagado (0,25 $).
- **Estimación con la RDS siempre encendida** (ingesta por franjas, decidido el 2026-10-22):
  ~12,4 $ de instancia + ~2,3 $ de disco + ~1 $ de copias + ~7,3 $ de dos IPv4 ≈ **24 $/mes**, más
  las horas de EC2 y el uso de Bedrock `[estimación]`.

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
- **Presupuesto con alertas** antes de crear recursos. Un Budget **avisa, no bloquea**. Con la web
  pública se suma `jupiter-bedrock-diario`: 2 $ al día solo de Bedrock, con correo al 80 % y al
  100 %. Llega con horas de retraso (AWS actualiza los costes unas tres veces al día); el aviso
  rápido es la alarma de tokens de CloudWatch (§8.7).
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

Once alarmas de CloudWatch que avisan por correo (SNS `jupiter-alertas`). Las dos de la Lambda se
crearon a mano; las otras nueve, los filtros y el panel están en la plantilla de CloudFormation
`jupiter-observabilidad`:

| Alarma | Métrica | Dispara cuando |
|---|---|---|
| `jupiter-pipeline-errores` | `Errors` (suma, 5 min) | La función lanza una excepción |
| `jupiter-pipeline-sin-ejecuciones` | `Invocations` (suma, 24 h) | No ha habido ninguna ejecución |
| `jupiter-pipeline-sin-datos` | `HorasNuevas` (suma, 24 h; métrica EMF) | La ejecución no cargó ninguna hora, o no publicó |
| `jupiter-web-5xx` | Filtro `Peticiones5xx` | ≥ 5 en 5 min: API, agente o proxy rotos |
| `jupiter-bedrock-errores` | `InvocationClientErrors` + `InvocationThrottles` | ≥ 3 en 15 min: modelo retirado, cuota, permisos |
| `jupiter-bedrock-tokens-dia` | `InputTokenCount` (suma, 24 h) | Supera un umbral (parámetro de la plantilla; provisional 2 M) |
| `jupiter-ec2-status` | `StatusCheckFailed` | Dos periodos de 5 min seguidos |
| `jupiter-ec2-creditos-cpu` | `CPUCreditBalance` | < 20 créditos |
| `jupiter-ec2-memoria` | `mem_used_percent` (agente) | > 90 % durante 10 min |
| `jupiter-ec2-disco` | `disk_used_percent` (agente) | > 85 % |
| `jupiter-rds-espacio` | `FreeStorageSpace` | < 2 GB |

La segunda es la que más aporta y depende de un detalle de configuración: `treatMissingData:
breaching`, es decir, **la ausencia de datos se interpreta como fallo**. Cubre el fallo silencioso
de que el programador deje de disparar o alguien desactive la función: no hay errores que contar,
no salta nada y el hueco en los datos se descubriría semanas después. `jupiter-pipeline-sin-datos`
cubre el otro fallo silencioso (§10) con la misma configuración. Las demás de la pila hacen lo
contrario (`notBreaching`): la EC2 y la RDS pasan la mayor parte del día apagadas, y sin datos no
hay fallo.

Lo que se aprendió al fijar los umbrales con los datos reales de la cuenta:

- **La EC2 está en modo `unlimited`**: sin créditos de CPU no se frena, sino que paga la CPU
  extra. La alarma de créditos avisa de coste, no de lentitud (el saldo medido estaba entre 145 y
  272).
- **La RDS tiene autoescalado de almacenamiento** (20 GB, máximo 1000): antes de llenarse, crece.
  La alarma de espacio avisa de que va a crecer el disco y la factura (17,9 GB libres medidos).
- **Bedrock solo da métricas por modelo o para toda la cuenta**, no por aplicación: las
  evaluaciones de modelos del equipo cuentan en los errores y los tokens. El umbral provisional
  de 2 M tokens al día deja margen sobre el pico medido (1,23 M en un día de campaña de evaluación;
  un día normal, entre 3.000 y 57.000).
- **El disco del agente de CloudWatch** lleva las dimensiones `path` y `fstype` además de
  `InstanceId` aunque se descarte el dispositivo; la alarma tiene que nombrar las tres.

**Filtros de métricas** sobre el log de acceso de nginx (namespace `Jupiter`, sin dimensiones):
`Peticiones5xx`, `Peticiones4xx` (sin 401: con la web abierta a internet y contraseña, los robots
y el primer acceso de cada navegador la piden constantemente) y `TiempoRespuestaChat` (el
`request_time` de las 200 en `/api/chat*`; en el streaming, hasta el último evento). Los filtros se
aplican al grupo entero, pero solo el log de `web` es JSON con `status`.

**Panel `jupiter`** con cinco filas: Bedrock por modelo (invocaciones, latencia, tokens, errores),
borde HTTP (4xx/5xx, tiempo de `/api/chat` y el estado de las once alarmas), máquina (CPU, créditos,
memoria, disco), datos (Lambda, CPU, conexiones y espacio de la RDS) y negocio (abajo). Los
gráficos por modelo usan `SEARCH`, así que un modelo nuevo aparece sin tocar la plantilla.

**Métricas de negocio del pipeline.** Lo anterior dice si la infraestructura funciona; no si el
sistema cumple su objetivo (cargar cada noche los datos de las 24 estaciones y detectar
anomalías). Cada ejecución de la Lambda termina con una línea JSON en formato **EMF** (*Embedded
Metric Format*): CloudWatch extrae las métricas del propio log, sin `PutMetricData`, sin boto3 y
sin permisos nuevos. Namespace `Jupiter/Pipeline`, sin dimensiones:

| Métrica | Qué mide |
|---|---|
| `HorasNuevas` | Filas horarias insertadas o corregidas |
| `EstacionesConDatos` | Estaciones con al menos una medición válida (de 24) |
| `BloquesAnomalos` | Bloques marcados por el Isolation Forest |
| `TasaAnomalias` | `BloquesAnomalos` / bloques procesados (%); el modelo espera ~1 % (`contamination`) |

- **Sin API, la línea sale igual con ceros**: la alarma se evalúa con dato y el hueco se ve en el
  panel, en lugar de depender solo de la falta de datos.
- **Detalle gratis como campos del log**: `pct_validas`, `estaciones_sin_datos` y
  `anomalias_por_magnitud` viajan en la misma línea sin directiva de métrica, así que no cuentan
  como métricas y se consultan con Logs Insights. Descartado: `PorcentajeValidas` como métrica y el
  desglose de anomalías por contaminante como dimensión (cada valor sería una métrica de pago:
  hasta 6 más por las 6 magnitudes), y una métrica de frescura del dato (fuera de alcance).
- **«Preguntas respondidas hoy» sin métrica nueva**: es la estadística `SampleCount` de
  `TiempoRespuestaChat`, que ya cuenta una muestra por cada 200 en `/api/chat*`.
- Fila «Negocio» del panel: horas cargadas por noche (barras), estaciones con datos (indicador
  0–24 con la zona por debajo de 20 en rojo), tasa de anomalías con una línea de referencia en el
  1 % y preguntas del día.
- Presupuesto: 9 de las 10 métricas personalizadas gratuitas; 12 métricas en alarmas, dos por
  encima de la capa gratuita `[estimación: 0,20 $/mes en total, 0,10 $/mes por la nueva]`.
- Código separado del *handler* (`deploy/metricas_emf.py`) para probarlo sin AWS: el *handler* lee
  SSM y S3 al importarse. 7 tests nuevos (forma del JSON, rama sin datos, tasa con cero bloques,
  cobertura de estaciones).

Estado (2026-10-23): desplegado en AWS (imagen de la Lambda y pila). Una invocación manual a las
14:07 UTC publicó las 4 métricas en `Jupiter/Pipeline` **sin dimensiones** (`Dimensions: [[]]`
funciona como se esperaba) y la alarma `jupiter-pipeline-sin-datos` pasó a `OK` con 2.568 horas.
Cifras de esa ejecución: 23 estaciones con datos (falta la 55), 61,8 % de filas válidas, 107
bloques anómalos de 321 (**33,3 %**). `[por confirmar: que los widgets del pipeline muestren el
dato en el panel]`

**Hallazgo: la tasa de anomalías depende de la hora de ejecución** (consultas de solo lectura a la
RDS, 2026-10-23). En la ejecución manual de las 16:07, los 107 anómalos eran los 107 bloques
`tarde` (13-19 h), con cobertura media 0,28: el bloque estaba a medias y el modelo, que usa la
`COBERTURA` como *feature*, lo marca siempre. `madrugada` y `mañana`, completos, 0 anómalos. La
ejecución de la noche reescribe esos bloques, así que ese caso se corrige solo.

El caso que **no** se corrige es el de cada noche. A las 23:45 la hora 24 aún no está medida y el
bloque `noche` (20-23 h) se guarda con 3 de 4 horas; la API solo da el día en curso, así que
ninguna ejecución posterior lo completa:

| Bloque | % anómalos, histórico 2025 (cobertura) | % anómalos, últimos 30 días en vivo (cobertura) |
|---|---|---|
| madrugada | 0,4 (0,99) | 0,6 (1,00) |
| mañana | 4,2 (0,96) | 4,0 (0,97) |
| tarde | 1,2 (0,98) | 1,8 (0,98) |
| noche | 0,6 (0,99) | **6,8 (0,75)** |

El bloque `noche` en vivo sale unas 11 veces más anómalo que en el histórico, y siempre con
cobertura 0,75 (3.165 de 3.196 bloques). Con cobertura 0,5 o 0,25 (31 bloques, ejecuciones
manuales a deshora) sube al 62 % y al 100 %. `mañana` en el 4 % es un rasgo del modelo, presente
también en el histórico. Consecuencia: unas 200 anomalías de más al mes en la base `[estimación: 3.196 × (6,8 − 0,6) %]` y
una `TasaAnomalias` nocturna estructuralmente por encima del 1 % esperado. **Arreglado el mismo
día** (§5.4, §6.5 bis): el pipeline solo escribe bloques cerrados y mide la noche sobre las 3 horas
que publica la API; con las cargas por franja, `TasaAnomalias` pasa a ser la de los bloques recién
cerrados. Queda calibrar la alarma con datos posteriores al arreglo (fase 4).

La retención de los logs se baja a 14 días; por defecto no caducan nunca.

**KPIs de uso del asistente, desde Phoenix.** Los calcula el propio agente y los sirve en
`GET /kpis?dias=1|7|30` (Phoenix retiene 30 días); api-usuario los reenvía en `GET /kpis` y la web
los muestra en `/admin/uso` («Uso del asistente», enlace «Administración» en el pie). Camino: web → nginx →
api-usuario → agente → API REST de Phoenix (`GET /v1/projects/{proyecto}/spans`, paginada) por la
red interna. En la EC2 el agente no escribe JSONL, así que Phoenix es la única fuente. Coste 0 y
ninguna métrica de CloudWatch (regla de reparto).

- Capas: `datos/phoenix.py` (lector con `httpx` y adaptador del span de Phoenix al formato del
  JSONL), `business/trazas.py` (árbol del turno, tokens, coste y tabla de precios; los comparte con
  el informe de evaluación de §7.3, sin duplicar precios), `business/kpis.py` (cálculo puro y
  servicio con caché de 60 s por periodo para no releer Phoenix en cada recarga) y
  `entities/kpis.py` (contrato; api-usuario tiene una copia con el mismo esquema JSON, comprobado).
- KPIs: preguntas por día; mezcla de intenciones, rutas y temas; resultado del turno (respondida,
  frase fija, sin evidencia, servicio no disponible, bloqueada, error) y **tasa de resolución** =
  respondida + frase fija (la frase fija es la respuesta correcta a una intención fuera de
  alcance); bloqueos por regla; síntesis válida a la primera y respuestas forzadas por el código;
  **coste por pregunta** (P50, P90, por pregunta resuelta y la lista de costes para el histograma);
  latencia y **tiempo hasta el primer token** por ruta; fallos de herramienta por causa
  (`agente.estado`) y preguntas de datos perdidas con la RDS apagada (§8.8).
- Por sesión (`session.id` en los spans, ver valoración abajo): turnos y duración de sesión y tasa
  de reformulación tras un rechazo (si el turno siguiente sí se resuelve, el rechazo pudo ser un
  falso negativo). `null` con los spans anteriores a ese cambio.
- **Satisfacción** (👍/👎, ver valoración abajo): votos positivos en total, por ruta y por tema, y
  participación (sesiones con 2 o más respuestas completas que votaron). Los votos se leen de
  `GET /v1/projects/{proyecto}/span_annotations` (verificado en el OpenAPI de Phoenix 20.19.0),
  en lotes de 100 ids de span porque van en la URL.
- **Primer token**: el agente añade el evento `primer_token` al span `turno` cuando el primer texto
  sale hacia el cliente. Solo existe en el stream y en las rutas que entregan tokens (libre, datos,
  síntesis forzada); documental, mixta y frases fijas llegan enteras al final, y para ellas la
  latencia percibida es la del turno.
- El JSON solo lleva recuentos con su *n* y percentiles (rango más cercano); los porcentajes y las
  figuras los hace la web. Sin texto de usuarios (ni preguntas, ni respuestas, ni mensajes de
  error, que pueden llevar literales de la pregunta); un test lo comprueba.
- **Página `/admin/uso`**: selector de 24 h, 7 o 30 días; seis indicadores (preguntas, tasa de
  resolución, coste P50, latencia P50, primer token P50 y satisfacción), cada uno con su *n*;
  embudo intención → ruta → resultado en barras horizontales; histograma del coste por turno con
  marcas de P50 y P90; satisfacción por ruta (barras 0-100 % con el *n* de cada una); tablas de
  rutas (latencias), temas, fallos de herramienta, sesiones y satisfacción por tema con la
  participación. Sin votos, «Sin valoraciones en el periodo» en lugar de 0 %. Las barras y el
  histograma son HTML y CSS, sin Recharts; la página se carga aparte del bundle principal
  (11 kB). Lógica pura (porcentajes, cubos del histograma, formato) con 13 tests Vitest.
- **Rótulo «Solo administradores» sin protección.** La página lo lleva y explica que en producción
  pediría iniciar sesión con un perfil de administrador; el acceso es el del resto de la web (la
  contraseña de nginx). La gestión de usuarios está fuera del alcance del TFM, y una clave falsa
  daría una sensación de seguridad que no existe; el rótulo deja claro el diseño previsto sin
  simularlo. Exponerla no filtra nada: solo agregados, sin texto de usuarios (§11).
- Phoenix caído, sin configurar o con un formato inesperado → 503 con mensaje; api-usuario
  devuelve 503 si el agente no responde y 502 si la respuesta no cumple el contrato.
- Descartados: `arize-phoenix-client` (una dependencia más para una llamada HTTP paginada; `httpx`
  ya estaba y permite probar sin red con `MockTransport`), las vistas guardadas en la interfaz de
  Phoenix (no reproducibles), que api-usuario lea Phoenix directamente (duplicaría la lógica del
  turno, que es del agente) y una página estática regenerada cada hora (aparte de la web y con
  datos de hasta una hora). La variable `PHOENIX_URL` del diseño no hizo falta: la raíz de la API
  se deduce de `PHOENIX_ENDPOINT`, como para la valoración.
- Esquema de la API comprobado en el código del servidor 20.19.0: devuelve los atributos del span
  aplanados (`agente.ruta`) y el tipo aparte (`span_kind`); el adaptador los lleva al formato del
  JSONL. El lector se probó contra el Phoenix local (misma versión) cuando era un script.

Estado (2026-10-23): endpoint en el agente y en api-usuario y página web, con tests. `[por
confirmar: despliegue y cifras reales con su n]`

**Valoración 👍/👎 de los usuarios, anotada en Phoenix.** Es la única medida de satisfacción real:
la evaluación del asistente (§7.3) era hasta ahora solo *offline*, con baterías de preguntas.

- El voto se guarda como **anotación de Phoenix sobre el span `turno`** (`valoracion_usuario`,
  anotador `HUMAN`, etiqueta `positiva`/`negativa`, *score* 1/0), no en una tabla propia: queda
  junto a la traza del turno que se valora (ruta, tema, coste, tiempos) y el informe de KPIs la lee
  por la misma API. Coste 0.
- Phoenix anota por **id de span**, no de traza: el agente devuelve el `span_id` del span `turno`
  en el evento `done` (y en `/responder`) y la web lo reenvía al votar. Sin estado nuevo en el
  agente ni búsqueda en Phoenix. Descartado: guardar en el agente qué span corresponde a cada
  turno (estado en memoria, se pierde al reiniciar) y buscarlo en Phoenix por `traza_id` (una
  consulta más por voto).
- Camino: web → `POST /api/chat/valoracion` (api-usuario, valida y reenvía) → `POST /valoracion`
  (agente) → `POST /v1/span_annotations` de Phoenix. El `identifier` de la anotación es el
  `session_id`: si la misma sesión vuelve a votar el turno, el voto se sustituye, no se duplica.
- Se envía en modo asíncrono: Phoenix encola la anotación y la reintenta si el span aún no ha
  llegado, porque el agente exporta las trazas por lotes y el usuario puede votar antes (comprobado
  en el código del servidor 20.19.0). Si Phoenix no responde, 503 y el voto se pierde: no se
  reintenta, la valoración no es crítica.
- **`session.id`** en todos los spans del turno (`using_session` de OpenInference): Phoenix agrupa
  los turnos de una conversación en su vista de sesiones y el informe calcula las métricas por
  sesión.
- **Cuándo se pide (web):** tras la 2.ª respuesta completa de la conversación, encima del campo de
  pregunta: «¿Qué tal lo está haciendo el asistente hoy?» con 👍, 👎 y «Ahora no». Una vez al día
  por navegador (`localStorage`; sin él, una vez por carga), nunca con una pregunta en vuelo y solo
  con respuestas del agente (las del orquestador no traen `span_id`). Votar o descartar cuentan
  como respuesta del día. Se valora el último turno. El voto se envía sin esperar: «Gracias» dos
  segundos y desaparece. Descartados: un 👍/👎 en cada respuesta (más votos, pero ruido visual y
  votos sobre respuestas sueltas, no sobre la experiencia) y pedirlo tras la 1.ª respuesta (aún no
  hay conversación que valorar).
- Revisión local (Codex, 2026-10-23): pendientes de corregir el estado diario del aviso
  (no se refresca al cambiar de día ni al votar en otra pestaña) y el denominador de
  participación (cuenta turnos fallidos, aunque la web exige dos respuestas completas).
  Reproducción sintética: una sesión elegible con voto y otra con una respuesta y un error
  producen un 50 %; con la regla de la web corresponde un 100 %.
- La API valida el formato de los identificadores, pero no la pertenencia del span a la
  sesión y traza recibidas. Un cliente con un span conocido puede enviar una valoración
  con otra sesión; queda pendiente vincular el voto al turno emitido por el servidor.
- Limitación (sesgo de respuesta): votan pocos usuarios y no al azar, y el voto juzga la
  conversación aunque se anote en un turno. La satisfacción se presenta con su *n* y con la tasa de
  respuesta: en el informe, sesiones con voto entre las que tienen 2 o más respuestas completas (el
  mismo criterio que la web: los turnos con error o cancelados no cuentan); frente al uso real, votos /
  `SampleCount` de `TiempoRespuestaChat` en CloudWatch.
- Tests: 11 del lado servidor (7 del agente con un Phoenix fingido por `MockTransport` y 4 de
  api-usuario) y una comprobación más en cada contrato, 7 de la regla del aviso (Vitest) y 3 del
  informe (descarga por lotes, último voto, satisfacción por ruta y tema). Cuerpo de la anotación
  aceptado por el Phoenix local (20.19.0).

Estado (2026-10-23): agente, api-usuario, web e informe listos; contrato regenerado.
`[por confirmar: despliegue y prueba con una anotación real en la EC2]`

El asistente se observa con **Phoenix**, no con CloudWatch: cada turno deja una traza OpenTelemetry
con sus fases, tiempos, tokens y errores. En la EC2 se consulta en `/phoenix/` (§8.9).

**Regla de reparto: Phoenix para el asistente, CloudWatch para la infraestructura y el borde HTTP.**
Nada se mide dos veces y casi todo cabe en la capa gratuita `[estimación: ~0,10 $/mes, §8.3]`.

- **Logs de la EC2** en el grupo `/jupiter/ec2` (14 días): `web` (log de acceso de nginx en JSON:
  estado, ruta, tiempo de respuesta), `api-usuario` y `rag`, con el driver `awslogs` de Docker y un
  *stream* por contenedor. Los del `agente` y `phoenix` se quedan en la máquina: lo importante del
  agente ya está en sus trazas. Modo `non-blocking`: si CloudWatch no responde se pierden líneas,
  pero el contenedor no se para.
- **Memoria y disco de la máquina**, que `AWS/EC2` no mide: el agente de CloudWatch publica
  `mem_used_percent` y `disk_used_percent` cada 60 s, con solo la dimensión `InstanceId` para que
  sean exactamente 2 métricas personalizadas (de 10 gratuitas). Si el agente no se instala o no
  arranca, `arrancar.sh` lo avisa y la aplicación arranca igual: la vigilancia no puede tumbar
  el servicio.
- **El grupo de logs lo crea CloudFormation** (pila `jupiter-observabilidad`), junto con los
  filtros, las alarmas y el panel. Crearlo a mano habría hecho fallar la pila después, porque
  CloudFormation no adopta un recurso que ya existe.
- Permisos del rol de la EC2: escribir solo en `/jupiter/ec2` y publicar métricas solo en el
  namespace `CWAgent`. Sin ellos Docker no crea el contenedor con `awslogs`: el rol se amplía
  antes de desplegar.

Estado (2026-10-23): aplicado en AWS y verificado. Tres *streams* en `/jupiter/ec2` y exactamente
dos métricas en `CWAgent` (memoria ~30 % y disco 23,4 % con los cinco contenedores en marcha). Una
pregunta al chat queda en el log de `web` como `POST /api/chat/stream`, estado 200, 5,6 s, y se
consulta con Logs Insights. El log de `rag` es casi todo su *healthcheck* (`GET /salud` cada 30 s).
En la máquina, `docker compose logs` sigue funcionando para los cinco servicios (copia local de
Docker también con `awslogs`). El *healthcheck* de `web` (`GET /` desde `127.0.0.1` cada 30 s)
también entra en el log de acceso, pero responde 200 y no casa con ningún filtro.
`[por confirmar: arranque completo tras apagar y encender]`

Filtros, alarmas y panel (2026-10-23): plantilla validada con `validate-template` y `cfn-lint`,
aplicada en AWS el mismo día. Un minuto después, nueve de las diez alarmas estaban en `OK`
evaluadas con datos reales (memoria 29,5 %, disco 23,4 %, 294 créditos de CPU, 17,97 GB libres en
la RDS, 44.421 tokens en el último día); la de 5xx esperaba el primer dato de su filtro. Forzada a
`ALARM` con `set-alarm-state`, CloudWatch publicó en `jupiter-alertas` y volvió a `OK` al minuto.
Los tres filtros, probados con `test-metric-filter` sobre una línea real del chat (y la misma con
503 y con 401), casan lo esperado: el 503 cuenta como 5xx, el 401 no cuenta como 4xx y el tiempo
del chat solo toma la 200. `[por confirmar: 503 reales con el agente parado y primer dato de
TiempoRespuestaChat]`

### 8.8 Encendido programado de la base de datos (2026-10-01 a 2026-10-23)

**Sustituido por la RDS siempre encendida** al pasar a una carga por franja (§2): las dos
programaciones se desactivaron el 2026-10-23 (no se borran) y «Apagar entorno» ya solo apaga
la EC2. Para la entrega también se desactivó el apagado nocturno de la EC2 (§8.9). Las ventanas de copias y mantenimiento no coinciden con ninguna
de las cuatro cargas. Se conserva el diseño porque explica decisiones y fallos de §8 y §12.

La RDS solo estaba encendida cuando hacía falta: cada noche alrededor de la carga, y cuando el
equipo la encendía a mano para trabajar o hacer una demo.

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

### 8.9 La aplicación en una EC2 — estado actual

- **Máquina:** EC2 `t3.medium` (2 vCPU, 4 GB, x86) con Amazon Linux 2023 y 30 GB de disco. x86
  y no Graviton (`t4g`, algo más barata) para construir las imágenes en los runners de GitHub, que
  son x86, sin emular ARM para compilar torch. ~2,5 $/mes de disco más ~0,045 $ por hora encendida
  `[estimación]`. Se apagaba cada noche a la 01:30 (`jupiter-ec2-apagar`); **desde el 2026-10-23,
  encendida siempre** para la entrega, con esa programación desactivada (no borrada). Unos
  32 $/mes más mientras dure `[estimación: 0,045 $/h × 720 h]`.
- **Un contenedor por servicio** (`web`, `api-usuario`, `agente`, `rag`), con las imágenes de ECR
  y la versión activa de cada uno en SSM (`/jupiter/versiones/<servicio>`): se despliega o se
  vuelve atrás una pieza sin tocar las demás. La máquina no construye nada. El `LLMOrchestrator`
  no se despliega (descartado, §2). Desde el 2026-10-23, un quinto contenedor, `phoenix`, con
  imagen pública de Docker Hub y la versión fijada en el compose (sin SSM ni ECR).
- **Arranque autónomo:** un servicio de systemd ejecuta `arrancar.sh` en cada encendido, que lee
  versiones y configuración de SSM con el rol de la máquina y levanta los contenedores. Encender la
  EC2 basta para tener la aplicación.
- **Creación en dos pasos** (`deploy/aws/`): primero ECR, roles, grupo de seguridad y parámetros;
  la EC2 solo cuando las 4 imágenes están desplegadas, para que su primer arranque no falle. Metadatos
  con IMDSv2 y 2 saltos: el contenedor del agente necesita llegar a las credenciales del rol.
  Apagado a la 01:30 con el destino universal de EventBridge Scheduler (`ec2:StopInstances`).
- **Ficheros de arranque por S3** (`s3://…/ec2/`): lo que corre en la máquina sale del repositorio
  y el despliegue lo puede actualizar, sin acceder a la máquina. En cada arranque la EC2 trae de S3
  `compose.yml`, `arrancar.sh` (lo descarga el propio servicio de systemd antes de ejecutarlo) y
  `jupiter.service` (lo reinstala `arrancar.sh` si cambió): los tres salen siempre del mismo commit,
  aunque se desplegara con la máquina apagada.
- **Seguridad:** solo `web` (nginx: SPA y proxy de `/api/*`) publica puerto, el 80. **Para la
  corrección del TFM, abierto a todo internet sin contraseña** en una **IP elástica** (fija), con
  el presupuesto diario de Bedrock de la §8.5 como aviso de gasto (`deploy/aws/3_web_publica.ps1`;
  aplicado en AWS `[por confirmar]`). La contraseña sigue disponible sin tocar código: autenticación
  básica de nginx, contraseña cifrada en SSM `/jupiter/web/contrasena` y en el disco de la EC2 solo
  su hash; se pensó para demos desde redes con IP desconocida. El 443 está cerrado: no hay HTTPS. Si el parámetro existe pero no se puede leer, el
  arranque se detiene: mejor sin web que abierta sin contraseña. `/phoenix/` exige siempre esa
  contraseña, también desde la propia máquina, y sin contraseña configurada responde 403: guarda
  las preguntas y respuestas de los usuarios y no tiene autenticación propia. Limitación: es HTTP, la contraseña
  viaja sin cifrar; HTTPS exigiría dominio y certificado. Verificado en AWS el mismo día desde una IP fuera de la
  lista del equipo (§12). Sin SSH:
  acceso con *Session Manager*. Secretos en ficheros de solo root y repartidos: `api-usuario`
  recibe la conexión a RDS de la Lambda; el `agente`, la del rol de solo lectura `agente_lectura`.
- **LLM sin claves:** el agente usa Bedrock con el rol de la máquina (`bedrock:InvokeModel` y
  `InvokeModelWithResponseStream` en `eu-west-1`). Su configuración sale de todos los parámetros de
  `/jupiter/agente/` (`llm_modelo` → `LLM_MODELO`): una variable nueva se da de alta en SSM, sin
  tocar scripts. Sin `llm_modelo`, todo funciona salvo `/chat`, que responde 503.
- **CI/CD solo por GitHub Actions:** «Desplegar» y «Rollback» suman `web`, `api-usuario`, `agente`
  y `rag` («Desplegar» también `phoenix`, sin tests ni build: publica el compose y recrea el
  contenedor; su vuelta atrás es revertir el commit); si la EC2 está encendida actualizan solo ese contenedor por *SSM Run Command* y esperan
  a que pase su healthcheck (`docker compose up --wait`), así un despliegue roto falla en GitHub. La
  versión activa se guarda en SSM con descripción (quién, qué y desde qué rama) y su historial sirve
  de registro de despliegues y rollbacks. «Encender/Apagar entorno» gestionan también la EC2. Los
  roles solo actúan sobre instancias con la etiqueta `Name=jupiter-api`. Cada despliegue construye
  la imagen con el código del momento: los cambios futuros del agente se publican con un clic.
- **Detalle de nginx:** resuelve los nombres de `api-usuario` y `phoenix` en cada petición (DNS de
  Docker, caché de 10 s, destinos en variables). Hasta el 2026-10-23 guardaba la IP de
  `api-usuario` al arrancar y `arrancar.sh` reiniciaba `web` tras redesplegarla; ya no hace falta, y
  `web` arranca aunque Phoenix no exista (502 en `/phoenix/`).
- **Phoenix (trazas del agente):** el agente las envía a `http://phoenix:6006/v1/traces` por la red
  interna (sin el prefijo `/phoenix`, que solo vale a través de nginx), con la dirección fija en
  el compose; no depende de Phoenix para arrancar. Criterio de configuración del agente: en SSM
  solo lo secreto (`database_url`) o lo que se cambia sin tocar código (los modelos); lo fijo de la
  máquina (proveedor, región, URL del RAG y de Phoenix) en el compose, que manda sobre SSM. Datos en SQLite en un
  volumen del disco de la EC2: sobreviven a apagar y encender. Retención de 30 días configurada,
  pero Phoenix purga los domingos a las 00:00 UTC y la EC2 está apagada a esa hora: en la práctica
  las trazas crecen (aceptado; lo vigilará una alarma de disco). Memoria: unos 450-500 MB en
  reposo, medidos en local; con los cuatro contenedores y el modelo del RAG cargado la EC2 usaba
  1279 MB de 3835, así que cabe sin cambiar de instancia `[estimación: suma de medidas]`.
  **Pendiente de aplicar en AWS** `[por confirmar]`.
- **Verificado en AWS (2026-10-13):** web (200), `/estaciones` desde RDS y chat de punta a punta
  con Bedrock: pregunta documental con cita `[D1]` validada por el RAG (18 s) y pregunta de datos
  por SQL con el rol `agente_lectura` (5,6 s; media de NO₂ en Plaza de España, 29,3 µg/m³ en 27
  días). Una medición por pregunta, sin repetir.
- **Primer arranque fallido:** `arrancar.sh` pedía `/jupiter/agente/` (con barra final) y el rol
  solo autoriza la ruta `parameter/jupiter/agente`: la consulta fallaba y el servicio no levantaba.
  El workflow solo mostraba `failed`; ahora «Encender entorno» imprime el log del servicio si no
  arranca. La simulación local no lo detectó porque el `aws` simulado no comprueba permisos.
- **Arranque fallido tras llevar Phoenix a la EC2** (2026-10-23): se desplegó `web` con la EC2
  apagada y al encenderla `web` no arrancaba (dos ejecuciones de «Encender entorno» en rojo). La
  máquina descargaba de S3 el `compose.yml` nuevo, que monta `phoenix-acceso.conf`, pero ejecutaba
  el `arrancar.sh` antiguo de su disco, que no lo genera. Docker crea un directorio vacío en la ruta
  de un montaje que no existe, y montarlo sobre un fichero de nginx falla. Corregido: el servicio
  trae `arrancar.sh` de S3 antes de ejecutarlo y `arrancar.sh` reinstala `jupiter.service` si cambió
  y borra esos directorios si los encuentra. «Encender entorno» reintenta el arranque una vez si
  falla: antes, con la EC2 ya encendida, relanzarlo solo volvía a mostrar el error anterior.
- **Probado sin AWS:** `arrancar.sh` se ejecutó en local con `aws` y `docker` simulados (todo, un
  solo servicio, `api-usuario` con reinicio de `web`, sin configuración del agente, servicio
  inválido y versión ausente) y `docker compose config` valida el compose de producción. En la
  primera versión (05-10), la prueba destapó que el mensaje de error de una versión ausente se
  escribía dentro de `versiones.env` en vez de en la terminal; corregido enviando los mensajes a
  stderr. La prueba se repitió el 2026-10-23 con Phoenix (todo, solo `phoenix`, `api-usuario` sin
  reinicio de `web`, con contraseña, sin ella y con permiso denegado) y con Phoenix real levantado
  desde el compose de producción detrás de nginx con los ficheros generados.

---

## 9. Metodología y calidad del software

- **Flujo de ramas:** `feature/*` → PR a `development` → PR a `main`. Un workflow bloquea cualquier
  PR a `main` que no venga de `development`. Más de 30 pull requests hasta julio de 2026.
  GitHub guarda el resultado de cada check por **commit**, no por PR: si se abre por error un PR a
  `main` y se rehace contra `development` con el mismo commit, el nuevo PR sigue mostrando el fallo
  de la protección. No bloquea el merge (`development` no exige checks) y desaparece con el
  siguiente commit (PR #93 → #95, 2026-10-23).
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
- **CI con seis jobs:** unitarios sin base de datos, **integración contra un PostgreSQL efímero**
  como servicio (solo con `RUN_DB_TESTS=1`, para no tocar nunca la base local por accidente),
  `api-usuario` (SQLite en memoria), `orquestador` (LLM falso), `agente` (dobles del LLM y del RAG)
  y `frontend`.
- **Contrato frontend-backend comprobado en CI.** El job `frontend` exporta el OpenAPI real de
  `ApiUsuario` (`Web/scripts/exportar_openapi.py`), regenera los tipos TypeScript y falla con
  `git diff --exit-code` si el contrato versionado no coincide; después lint, `npm test` y
  `npm run build` (`tsc -b` + Vite). Un build roto o una deriva de esquema tumban el PR. Las versiones de FastAPI y
  pydantic del job están fijadas a las que generaron el fichero, porque el esquema que emiten puede
  cambiar entre versiones.
- **Observabilidad del agente:** spans OpenTelemetry por turno, visibles en Phoenix y guardados en
  JSONL (§7.3.1). Los tests de trazas usan el exportador en memoria del SDK: comprueban el árbol de
  spans de un turno, que dos turnos concurrentes no se mezclen y que un exportador caído no afecte
  a la respuesta.
- **Reproducibilidad:** PostgreSQL en Docker con volumen nombrado, `.env.example` y parser y features
  compartidos entre notebooks y producción.
- **Entorno local con un comando:** un `Makefile` en la raíz (`make frontend`, `make dev` con
  Phoenix, RAG y Agente, un objetivo por servicio, `make test`) delega en `scripts/local.py` (solo librería estándar), que
  arranca los servicios en orden, espera a que cada dependencia responda, prefija la salida de
  cada uno y los para todos con Ctrl+C o si cae uno. `make frontend` levanta PostgreSQL, ApiUsuario
  y la web y deja el dashboard en `localhost:5173` con datos de la base local (verificado el
  2026-10-13: 24 estaciones y series por bloques). La base de datos sale del `.env` de la raíz, el
  mismo que usa `docker-compose`; una `DATABASE_URL` en el terminal (p. ej. la de RDS) tiene
  prioridad y entonces no se levanta Docker. Con Bedrock, `make dev` comprueba las credenciales de
  AWS antes de arrancar: las claves temporales caducan y, sin la comprobación, el fallo aparecía
  en la primera pregunta como «LLM no disponible». Descartados: varios `Makefile` por servicio (dos capas
  que mantener y rutas relativas al venv y al `.env` de la raíz), recetas con comandos de shell (en
  Windows `make` usa `cmd.exe` y el `bash` del PATH suele ser el de WSL) y ampliar
  `entorno_local.sh`, que depende de tmux y solo funciona en Linux.
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
- **Lo que funciona en `localhost` puede fallar en la nube por no ser HTTPS:** el chat de la web
  no enviaba nada en la EC2 (sin error visible y sin ninguna petición) porque generaba el id de cada
  consulta con `crypto.randomUUID()`, que el navegador solo ofrece en contextos seguros (HTTPS o
  `localhost`). Las pruebas contra la API no lo detectaron: el fallo estaba antes de la llamada.
- Compartir el código de features entre entrenamiento e inferencia evita divergencias silenciosas.
- La idempotencia (`ON CONFLICT`) simplifica mucho programar el pipeline, pero **`DO NOTHING` no
  es lo mismo que idempotencia**. La API entrega siempre las 24 horas del día y las aún no
  medidas llegan como ceros con flag `N`: al ignorar los conflictos, la primera ejecución del día
  fijaba esos ceros y ninguna posterior los corregía. El 2026-09-16, con varias ejecuciones
  manuales durante la tarde, las horas 18 a 23 quedaron guardadas como inválidas de forma
  permanente (740 filas `N` frente a 135-153 en los días de ejecución única). Corregido pasando a
  `DO UPDATE` condicionado a que la medición entrante sea válida y la guardada no.
- **Que una función no falle no significa que haya hecho su trabajo.** Si la API de Madrid no
  devolvía registros, la Lambda terminaba con `{"ok": False}` sin lanzar excepción: ni la alarma de
  errores ni la de «sin ejecuciones» saltaban, y la noche quedaba sin datos sin que nadie se
  enterase. Lo cierra una métrica de resultado (`HorasNuevas`) que se publica también en ese caso
  y una alarma que trata su ausencia como fallo. Lección: además de vigilar que el proceso corre,
  hay que vigilar lo que produce.
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
- **La documentación iba por delante del CI.** README, memoria y comentarios describían un job
  `frontend` que comprobaba el contrato, pero nunca llegó a `tests.yml`: durante tres semanas un
  cambio de la API podía desincronizar los tipos de la web sin que ningún PR fallase. Al describir
  un control automático, comprobar que el workflow lo ejecuta (2026-10-23).
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
- **Probar la configuración del proxy, no solo su sintaxis** (2026-10-23). Al preparar Phoenix
  bajo `/phoenix`, el diseño de nginx tenía tres errores que `nginx -t` no detecta: con el destino
  en una variable, `proxy_pass $phoenix/;` no quita el prefijo sino que manda todo a `/`; un bloque
  de acceso con `satisfy all` heredaba el `deny all` de la web y respondía 403 incluso con la
  contraseña correcta; y la redirección absoluta perdía el puerto. Los tres salieron en una prueba
  con contenedores reales antes de tocar AWS.
- **La configuración fija va junto al servicio al que apunta** (2026-10-23). Con Phoenix ya en la
  EC2 y su interfaz accesible, no llegaba ninguna traza: el envío dependía de un parámetro de SSM
  que nadie había creado, y su ausencia no da ningún error (el agente funciona igual, sin trazas).
  La dirección es fija y no es secreta, así que pasó al compose, al lado del contenedor `phoenix`.
  SSM queda para secretos y valores que se cambian sin desplegar código.

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
- **Lo que se despliega junto tiene que llegar junto** (2026-10-23). `compose.yml` se descargaba de
  S3 en cada arranque, pero `arrancar.sh` y `jupiter.service` solo cuando la EC2 estaba encendida
  al desplegar. Un despliegue con la máquina apagada dejó un compose nuevo con un script viejo, y
  `web` no arrancó (§8.9). Además, Docker convierte un montaje de un fichero que no existe en un
  directorio y deja el error para el siguiente arranque. Lección: todos los ficheros de una versión
  se actualizan por el mismo camino.
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
- **Un modelo puede ganar en acierto y perder en seguridad** (2026-10-16). Qwen3 Next igualó al
  mejor modelo en las preguntas de datos no vistas (16/20), pero copió entero su prompt de sistema
  en cuanto se lo pidieron. Ministral lo parafraseaba y gpt-oss se negaba. Lección: comparar
  modelos solo por acierto y coste no basta; el lote adversario forma parte de la evaluación.
- **Reproducir un lote exige reproducir también los datos** (2026-10-16). La base local ya llegaba al
  2026-05-19 y el gold de Carlos se calculó hasta el 2026-04-30: «ahora mismo», «este mes» y el corte
  de 2 años de la vista habrían cambiado de respuesta. Se replicó el estado en una base aparte y se
  comprobó con el número de filas anotado (314.411).

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
  Por eso, en tiempo real, el bloque de noche se mide sobre 3 horas (20–22) y su media no incluye
  la hora 23 (§6.5 bis). Para cerrarlo haría falta una segunda fuente, no otro horario.
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
- **Las alarmas de Bedrock miden toda la cuenta** (§8.7): una campaña de evaluación del equipo
  puede disparar el aviso de tokens sin que la web haya hecho nada. Separarlo exigiría un perfil
  de inferencia propio de la aplicación o métricas propias del agente.
- **La web pública no tiene autenticación ni límite de uso**: cualquiera puede preguntar al
  asistente y el gasto en Bedrock solo se vigila (presupuesto diario y alarma de tokens), no se
  corta. Lo resolvería un cupo diario en el agente o un límite por IP en nginx.
- **Phoenix no se ve en la web pública**: sin contraseña, `/phoenix/` responde 403 a todos. Para
  consultar las trazas hay que volver a poner la contraseña un rato.
- **La página de KPIs no tiene control de acceso propio** (§8.7): lleva el rótulo «Solo
  administradores», pero con la web pública la ve cualquiera (solo agregados, sin texto de
  usuarios). No hay gestión de usuarios.

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
- **Gestión de usuarios con rol de administrador**: Cognito u OIDC delante de nginx, con un grupo
  «administradores» que dé acceso a `/admin/` y `/api/kpis`, y HTTPS, sin el que cualquier
  credencial viaja en claro.
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
| 2026-09-28 | `LLMOrchestrator` implementado (Fase 3): bucle de agente sobre LLM OpenAI-compatible, tool `query_sql` con 5 consultas predefinidas parametrizadas (resuelve D3: sin SQL libre), tool `search_documents` reutilizando el RAG de la Fase 2, prompt de sistema con aviso médico, 34 tests sin red (LLM falso con guion + SQLite en memoria) y cuarto job de CI. Revisión adversarial multiagente con 7 arreglos aplicados (ver §7.3) (Sheila) |
| 2026-09-30 | Merge de `development` en `feature/api-usuario` (Sheila): incorpora la Fase 2
  reescrita (servicio de evidencias) y el CI/CD de la Lambda. Detectado que `LLMOrchestrator`
  importa `buscar_documentos()`, renombrado a `buscar()` en la reescritura del RAG: integración
  pendiente de reconciliar (§7.3) |
| 2026-09-30 | `LLMOrchestrator` adopta el contrato de evidencias de la Fase 2 **por HTTP**
  (Sheila): tool `buscar_evidencias` contra `rag.api` + validación de citas y render en el
  cierre, con una reparación y cierre por la ruta de datos si `sin_evidencia` con SQL. Resuelve
  el hueco de contrato de 2026-09-24 separando rutas y deja al orquestador sin torch/chromadb
  (3 servicios, §2 y §7.3). Suite del orquestador: 45 tests; verificado de punta a punta contra
  `rag.api` real (validación correcta, reparación y 503 tipado con índice desfasado) |
| 2026-10-01 | La definición de la tool `buscar_evidencias` deja de estar duplicada: el orquestador
  la pide a `GET /rag/herramienta` (caché por proceso); si el RAG no responde, la tool no se
  ofrece en esa vuelta y se reintenta en la siguiente. Suite del orquestador: 51 tests (Sheila) |
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
| 2026-10-03 | **Dashboard web (Fase 4)** en `Web/`, rama `feature/dashboard` (Sheila): SPA React 19 +
  TypeScript con Vite; pantallas Hoy (cifra de anomalías fiables del último día consolidado, *stat
  tiles* con sparkline, informe del día vía `/chat`, bloques marcados), Estaciones (mapa Leaflet con
  mapa base de Esri por defecto o tabla ordenable, y detalle con línea, heatmap días × bloques, tabla
  alternativa, rangos 7/30/90/personalizado y comparación de hasta 3 estaciones), Asistente (preguntas independientes, contador de espera,
  citas `[Dn]` enlazadas a fuentes separadas por tipo) y Metodología; el asistente está además en
  todas las pantallas como burbuja flotante con panel, con el estado de las consultas en un contexto
  global. Aviso médico fijo en la cabecera. Tipos generados del OpenAPI y comprobados en CI (quinto job). Despliegue en dos
  contenedores: `Web/Dockerfile` (node → nginx, `proxy_read_timeout 180s` en `/api/chat`),
  `ApiUsuario/Dockerfile` y servicios `api` + `web` en el compose. Verificado en local: `tsc`,
  ESLint, 22 tests de dominio con Vitest (regla de cobertura ≥ 0,7, citas `[Dn]`, fechas) y build
  limpios (app 352 kB + Recharts 372 kB; 112 + 110 kB gzip), probado de punta a punta contra
  `ApiUsuario` real sobre una SQLite sintética, en claro y oscuro. Imagen Docker
  `[por confirmar: sin demonio Docker en la sesión]` |
| 2026-10-04 | **Evaluación de la recuperación del RAG** (Carlos Fernández, `feature/rag-metricas`): 30 casos
  fijos y `python -m rag.evaluar`. Con e5-base: hit@4 14/15, MRR@10 0,740. **Umbral 0,22 → 0,1754**
  (ajenas rechazadas 3/10 → 10/10, documentales bajo el umbral 14/15 → 13/15), por decisión de
  priorizar la abstención pese a un hueco de 0,001. Medido también `multilingual-e5-small`
  (MRR 0,796, pero solape documental/ajena de 0,023): se mantiene e5-base. Índice reconstruido
  antes de medir (el anterior era de una copia sin commit, mismas cifras) |
  Primera ejecución tras el merge a `main` (04-10): «Rollback» en modo consulta, correcto |
| 2026-10-04 | **Tests de la API en CI** (Guillermo): `tests.yml` suma los jobs `api-usuario` (18 tests)
  y `orquestador` (51), cada uno con su `requirements-dev.txt`. Hasta ahora solo corrían en local
  aunque el README decía lo contrario. Reproducidos antes en entornos virtuales limpios. Como
  `desplegar.yml` reutiliza `tests.yml`, ningún despliegue sale ya sin que pasen también estos |
| 2026-10-05 | **Infraestructura de la EC2, parte del repositorio** (Guillermo, rama
  `feature/infraestructura-ec2`): compose de producción, `arrancar.sh`, servicio de systemd,
  `user-data.sh`, rol `jupiter-ec2-api` y `.gitattributes` para forzar LF en los ficheros que se
  ejecutan en Linux. `arrancar.sh` probado con comandos simulados (§8.9). Mismo día: workflows
  ampliados (Desplegar, Rollback, Encender y Apagar entorno) con la EC2 y el script `ec2.sh`,
  también probado con `aws` simulado; la prueba llevó a separar la consulta de la instancia en dos
  pasos para que un fallo de permisos no se confundiera con «la EC2 no existe». Pendiente: crear
  los recursos en AWS y la primera ejecución real |
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
| 2026-10-07 | **Rediseño del dashboard** (Sheila): paleta única «cielo» en `tokens.css` (la
  experimental se retira; su fichero queda vacío para anular un `?paleta=cielo` recordado en
  localStorage), navegación reducida a Hoy + Estaciones con CTA «Preguntar al asistente», pantalla
  Hoy en 3 bloques (cifra + tiles + bloques marcados; se quita el informe del día). Verificado con
  `tsc` y capturas en claro/oscuro contra `ApiUsuario` + PostgreSQL local, cuyo hueco
  2026-09-09→2026-10-07 se rellenó con datos sintéticos **solo en la base local de demo** (12.876
  filas derivadas de las estadísticas reales por estación/bloque, ~5 % de anomalías; estación 55
  incluida) |
| 2026-10-08 | **Portada Home** (Sheila): landing de marketing en `/` («Respira mejor. Elige tu
  zona.») con foto de parque y vista ilustrada de Madrid (del prototipo WebIAGen, reescaladas a
  375/240 KB en `Web/public/img/`), 3 tarjetas de rasgos y cifras reales (24 estaciones, 6
  contaminantes, 4 franjas). «Hoy» pasa a llamarse **Estadísticas** (`/estadisticas`) y se quita el
  CTA del asistente de la cabecera (queda la burbuja). Verificado: `tsc` y capturas claro/oscuro |
| 2026-10-08 | **`LLMOrchestrator` descartado y reparto de la fase 7** (Carlos): `Agente` queda como único agente; el código del orquestador se conserva sin previsión de uso (§2). El despliegue del agente con Bedrock y su dockerización pasan a otro miembro del equipo. El informe de trazas cuenta aparte los turnos cancelados por el cliente (antes salían como ruta `error`). 64 tests |
| 2026-10-08 | **Agente LLM, herramienta SQL, fase 1: datos y entorno local** (Carlos con Claude Code, rama `feature/Agente`): vista `mediciones_bloques` e índice por fecha, rol `agente_lectura` (scripts en `deploy/sql/`), DAL de solo lectura con tiempo límite y tope de filas, `--desde` y recarga con `TRUNCATE` en el cargador. 68 tests (4 nuevos) y 3 de integración con PostgreSQL, 3/3 en local; consultas típicas ~20 ms |
| 2026-10-08 | **Agente LLM, herramienta SQL, fase 2: SQL libre y ruta de datos** (Carlos con Claude Code, rama `feature/Agente`): `consultar_datos` con redactor (`LLM_MODELO_SQL`), validador `sqlglot`, un reintento y cifras a 1 decimal; DATOS deja la frase fija y obliga la consulta; síntesis de datos con fechas y fuentes `sql`; lote `casos_datos.json` (16 casos). 82 tests (14 nuevos); probado en el Postgres local con el rol de lectura (85 ms), sin evaluar aún con Bedrock |
| 2026-10-09 | **Fuentes en el streaming** (Carlos con Claude Code): `done` incluye siempre fuentes y advertencia, también en `ApiUsuario`. Evita perder las fuentes SQL al emitir tokens. Se descartó repetir el texto con un `passthrough` adicional. 94 tests del agente y 22 de la API. |
| 2026-10-09 | **Preguntas mixtas** (Carlos con Claude Code): Respuesta con datos y documentación en bloques separados; el fallo de uno no impide responder al otro. 93 tests con LLM falso. |
| 2026-10-09 | **Referencia para evaluar SQL** (Carlos con Claude Code): 16 casos con resultados calculados en PostgreSQL local. Acceso comprobado a Qwen3 Coder y gpt-oss-20b; Haiku no disponible. Corregido el perfil `eu.` en el cliente. Clasificador inicial: 28/29 originales con ambos modelos. 94 tests. |
| 2026-10-09 | **Elección del clasificador** (Carlos con Claude Code): Tras ajustar el prompt, gpt-oss-120b logra 29/29 originales en dos pasadas; Ministral, 27/29. Se elige gpt-oss mediante `LLM_MODELO_CLASIFICADOR`; las mixtas aún presentan fallos. |
| 2026-10-09 | **Primera evaluación SQL** (Carlos con Claude Code): Siete combinaciones de modelos y 16 casos: ninguna cumple; mejor resultado, 4/11 en datos y 0/2 mixtas. Coste estimado: 0,31 $. Detectados fallos de truncado y periodos. Veredictos pendientes de revisión humana. |
| 2026-10-09 | **Truncado y modelo de referencia** (Carlos con Claude Code): Se corrige el límite de filas para detectar el recorte, verificado en PostgreSQL local. MiniMax se descarta por mayor coste y menor acierto. 96 tests. |
| 2026-10-09 | **Periodos calculados en Python** (Carlos con Claude Code): Las fechas parten de la pregunta original y se validan antes de ejecutar SQL. Pasan las 15 consultas de referencia; 123 tests. Pendiente evaluar con Bedrock. |
| 2026-10-09 | **Instrucciones del redactor SQL** (Carlos con Claude Code): Se aclaran agrupaciones, uso de estaciones y sintaxis de PostgreSQL, con dos ejemplos. 123 tests; pendiente evaluar con Bedrock. |
| 2026-10-09 | **Segunda evaluación SQL** (Carlos con Claude Code): Tres combinaciones: todas logran 4/11 en datos. Mejoran periodos, aviso de recorte y SQL; persisten errores al reformular y redactar. Coste estimado: 0,10 $. Veredictos pendientes de revisión humana. |
| 2026-10-09 | **Correcciones tras la segunda evaluación** (Carlos con Claude Code): Descripción de herramienta simplificada, periodo explícito, respuesta fija fuera del histórico y rechazo de medias que mezclan contaminantes. 126 tests e integración con PostgreSQL en verde; falta otro lote con Bedrock. |
| 2026-10-09 | **Tercera evaluación SQL** (Carlos con Claude Code): Tres combinaciones; ninguna cumple. Mejor resultado 6/11 en datos (gpt-oss-120b · mismo); límites 3/3 con Ministral · Qwen3 Coder. Resueltos el caso fuera del histórico, el filtro de anomalías y las medias mezcladas; quedan rankings recortados, filas sin agregar y medias sin ponderar. Coste 0,10 $. Veredictos pendientes de revisión humana. |
| 2026-10-11 | **Modelo de predicción t+1 (parte ML del agente)** (Sheila con Claude Code, rama `feature/ml-prediccion`): notebook `04_entrenamiento_prediccion.ipynb`, funciones puras `src/etl/features_prediccion.py` e `inferencia_prediccion.py` (lags con rejilla anti-hueco, split temporal sin fuga, climatología solo-train) y bundle `models/prediccion_bloques.joblib`. `HistGradientBoostingRegressor` por contaminante, 16 features. Evaluado con el CSV live de CI (provisional, ~4 meses): MAE global 5,99 µg/m³ frente a 4,69 de la persistencia → skill −0,28 (solo O3 la supera). 11 tests nuevos; suite unit 129 en verde. Pendiente reentrenar con el histórico de S3 y conectar la herramienta al agente (fuera de alcance de esta iteración) |
| 2026-10-14 | **Predicción reentrenada con el histórico completo de S3** (Sheila con Claude Code): `resumen_bloques.parquet` (1,27 M bloques, 2018–2026, sin huecos), test de un año (2025-05 → 2026-04). El resultado se da la vuelta frente al lote provisional: MAE modelo 10,11 vs 10,46 de la persistencia → **skill global +0,03**, batiendo a la climatología en los 6 contaminantes y a la persistencia en 5 de 6 (solo NO pierde). Se cumple el criterio propuesto, con margen modesto. Se detecta y corrige una fuga: el parquet traía la climatología calculada con todo el histórico, se descarta y se recalcula solo con el train (§6.7) |
| 2026-10-21 | **Segundo bundle de predicción, t+2** (Sheila con Claude Code): el nb04 parametrizado por horizonte y reentrenado con `HORIZONTE=2` para poder responder «mañana» cuando los datos llegan hasta ayer (h1 cae en hoy). Mismo año de test: MAE 11,86 vs 12,80 de la persistencia → **skill +0,07** (5/6 contaminantes; climatología 6/6). Verificado con un replay en terminal contra lo medido de verdad (§6.7) |
| 2026-10-22 | **Servicio de predicción `src/prediccion` (ml.api)** (Sheila con Claude Code): servicio FastAPI espejo del RAG con la herramienta `predecir_calidad_aire` publicada, `/salud` y `POST /prediccion`; ancla por último día completo y elección de bundle por horizonte en código. De paso, la climatología del train entra en los bundles y `preparar_dataset` se protege de una `MEDIA_ESPERADA` precalculada. 21 tests nuevos sin red ni sklearn; suite 150 en verde. Verificado de punta a punta con los bundles reales sobre el histórico. Pendiente: cliente en el agente y despliegue (§6.7) |
| 2026-10-12 | **Arreglos deterministas del SQL libre** (Carlos con Claude Code): el validador impone siempre el tope de filas, rechaza `AVG(media)` y, sin periodo en la pregunta, cualquier filtro de fecha; descripción de la herramienta y regla de contaminantes por defecto ajustadas. 133 tests. |
| 2026-10-12 | **Cuarta evaluación SQL** (Carlos con Claude Code): tres combinaciones; ninguna cumple. Mejor resultado 8/11 en datos y 1/2 mixtas (gpt-oss-120b · mismo), con todo el SQL válido a la primera. Quedan definición de hora punta, clasificador, mixta y síntesis. Coste 0,09 $. Siguiente: catálogo para comparar. Veredictos pendientes de revisión humana. |
| 2026-10-12 | **Catálogo de consultas SQL** (Carlos con Claude Code): siete consultas cerradas que elige un LLM en JSON; el código construye el SQL con media ponderada, periodo calculado y parámetros. Se activa con `HERRAMIENTA_DATOS=catalogo`, sin cambiar el bucle. Con la elección correcta reproduce el gold del lote en PostgreSQL local. 143 tests. Pendiente: lote con Bedrock para compararlo con el SQL libre. |
| 2026-10-12 | **Catálogo comprobado y lote nuevo para comparar** (Carlos con Claude Code): prueba de humo con Bedrock (8/8 consultas elegidas bien, cifras del gold) y lote no visto de 28 preguntas (20 de datos, 3 mixtas, 5 límites) con gold en PostgreSQL local, para comparar SQL libre y catálogo sin el sesgo de los 16 casos usados para ajustar. Comparación pendiente. |
| 2026-10-12 | **Lote nuevo con el catálogo** (Carlos con Claude Code): 28 preguntas no vistas, tres combinaciones. Mejor resultado 12/20 en datos (11 de las 12 que caben en el catálogo y pasan el clasificador) con gpt-oss-120b como modelo y selector; 0/3 mixtas en todas. Fallan sobre todo clasificador, mixtas y la síntesis de Ministral. Coste 0,15 $. Siguiente: el mismo lote con el SQL libre. Veredictos pendientes de revisión humana. |
| 2026-10-12 | **Lote nuevo con el SQL libre y comparación** (Carlos con Claude Code): las mismas 28 preguntas y tres combinaciones con el SQL libre. Con gpt-oss-120b gana el libre (15 y 16 de 20 en datos frente a 12 y 9 del catálogo), por las preguntas que no caben en las 7 consultas; con Ministral el catálogo acierta más en lo que cabe y en límites. Mixtas 0/3 en todas. Coste 0,15 $. Veredictos pendientes de revisión humana. |
| 2026-10-13 | **Entorno local con `make`** (Guille con Claude Code): `Makefile` y `scripts/local.py` para levantar cada servicio o todo junto en un solo terminal, en Windows y Linux. `make frontend` probado con la base local y `make dev` con Bedrock (Ministral 14B) y el RAG; `make test` en verde (web 36, raíz 135, ApiUsuario 22, orquestador 51, agente 143 al crearlo). Después se simplifica a los servicios sueltos, se añade Phoenix y la comprobación de credenciales de AWS. |
| 2026-10-13 | **EC2 adaptada al `Agente` y a la web** (Guille con Claude Code, rama `feature/infraestructura-ec2`): el compose de producción, `arrancar.sh`, los workflows Desplegar/Rollback y los permisos pasan de `orquestador` a `agente` (Bedrock con el rol de la máquina, configuración desde `/jupiter/agente/` en SSM) y suman `web` como único puerto abierto (80). Probado con `aws` y `docker` simulados en seis escenarios. Scripts de creación en dos pasos (`deploy/aws/`): primero los recursos sin máquina (ECR con limpieza a 5 imágenes, roles, grupo de seguridad, modelos en SSM: Ministral 14B para el agente y gpt-oss-120b para el clasificador) y la EC2 solo cuando las 4 imágenes están desplegadas, para que su primer arranque no falle; apagado nocturno con el destino universal de Scheduler (`ec2:StopInstances`), sin runbook. Mismo día: recursos creados con los scripts de `deploy/aws/` y aplicación en marcha en la EC2, verificada de punta a punta (§8.9). Errores encontrados: comas sin comillas en PowerShell (separan argumentos) y la barra final de la ruta de SSM, que el rol no autorizaba |
| 2026-10-14 | **Chat de la web en la EC2** (Guille con Claude Code, rama `feature/fix-chat-http`): el chat no enviaba la pregunta servido por HTTP porque `crypto.randomUUID()` solo existe en contextos seguros; sustituido por un contador local (el id no viaja a la API). Build, lint y 36 tests de la web en verde |
| 2026-10-16 | **Dos modelos más para el agente** (Guille con Claude Code, rama `feature/evaluacion-haiku-qwen`): candidatos elegidos con la API de precios de AWS y la tabla de regiones: Claude Haiku 5.5 y Qwen3 Next 80B A3B. Haiku no está disponible en la cuenta (nadie ha enviado el formulario de caso de uso de Anthropic, así que no hay acuerdo de Marketplace). Qwen3 Next pasa por las baterías de Carlos (clasificador, 12 turnos, conversaciones, adversario, 16 de datos y los 28 nuevos con SQL libre y catálogo) contra una copia local de sus datos: 16/20 en datos no vistos con SQL libre (empata con gpt-oss), 10/20 como selector del catálogo, 0 mixtas y copia literal del prompt de sistema. Unos 0,14 $. Coste real de la campaña de Carlos en Cost Explorer: 1,21 $. Precio de Qwen3 Next añadido al informe de trazas y comparativa visual (`docs/agente/presentacion/comparativa_modelos.html`) ampliada a los tres modelos, con los resultados SQL. Veredictos pendientes de revisión humana |
| 2026-10-18 | **Nemotron 3 Super en las baterías del agente** (Guille con Claude Code, rama `feature/evaluacion-haiku-qwen`): con Haiku 5.5 aún bloqueado, se evalúa Nemotron 3 Super 120B con las mismas baterías y los mismos datos. Clasificador 31/31, conversaciones 11/11, 16/20 en datos no vistos con SQL libre y 14/20 con el catálogo (el mejor selector), turno documental de 4,9 s; copia literal del prompt de sistema e inventa datos de Getafe con el catálogo. Unos 0,14 $. Precio en el informe de trazas y cuarta serie en la comparativa visual. Veredictos pendientes de revisión humana |
| 2026-10-21 | **Usuario y contraseña en la web** (Guille con Claude Code, rama `feature/web-contrasena`): para la demo, que presentan compañeros desde redes cuya IP no se conoce. `arrancar.sh` lee la contraseña de SSM, calcula su hash y genera la configuración de nginx y `compose.yml` la monta en `web`, sin reconstruir la imagen. Probado en local: `arrancar.sh` con `aws` simulado (con contraseña, sin parámetro y con permiso denegado, que detiene el arranque; la contraseña en claro no queda en ningún fichero) y nginx real con el `nginx.conf` de la web (401 sin credenciales o con una errónea, 200 con la buena, también en `/api/*`, healthcheck interno sano; sin parámetro, 200 como antes). Aplicado en AWS el mismo día: permiso de lectura del parámetro en el rol de la EC2, puerto 80 abierto a todo internet y «Desplegar → `web`» con la EC2 encendida (el log muestra «Web protegida»). Desde una IP fuera de la lista del equipo: 401 en la web, `/api/*` y `/api/chat` sin credenciales o con una errónea; con ellas, 24 estaciones, datos de NO₂ hasta el día de la prueba, una pregunta de datos por SQL (7,2 s) y una documental con citas `[Dn]` (23,4 s) |
| 2026-10-22 | **Detector frente a la calima del 15-03-2022** (Guille con Claude Code, al redactar el 3.1 de la memoria): sobre el histórico, el IF marca 7/52 bloques de PM10 y 0/32 de PM2.5 ese día, frente a 52/52 y 31/32 del z-score. En global solo marca el 12,7 % de los bloques con \|z\| > 3 y el 74,9 % de sus marcas tienen cobertura < 0,7: los dos detectores son complementarios (§6.4, §6.6). Sin cambios en el código; combinarlos queda propuesto |
| 2026-10-22 | **Balance de producción y coste real** (Guille con Claude Code, consultas de solo lectura en CloudWatch, RDS y Cost Explorer): 36/36 días con carga y 0 errores desde el 16-09; 3,4 % de bloques marcados, inflado por el bloque de noche (7,19 % frente a 0,44 % del histórico) porque la hora 23 nunca llega (§6.5 bis). Coste real sin impuestos: 8,85 $ (16–30 sep) y 10,35 $ (1–22 oct); sin capa gratuita y con las IPv4 públicas como segundo gasto (§8.3). Decidido: ingesta al cierre de cada franja y RDS siempre encendida |
| 2026-10-23 | **Phoenix detrás de la web, en local** (Carlos con Claude Code, rama `feature/phoenix-prefijo`): primer paso para llevar Phoenix a la EC2 bajo `/phoenix`, detrás del nginx de la web y con su contraseña. Antes, medición en la EC2: 1279 MB usados de 3835 con los cuatro contenedores y el modelo del RAG cargado (562 MB en reposo), y Phoenix 20.19.0 ocupa unos 450-500 MB en reposo: cabe sin cambiar de instancia. El `nginx.conf` de la web resuelve los nombres en cada petición (DNS de Docker y destinos en variables): `web` arranca sin Phoenix (502 en `/phoenix`) y deja de depender de la IP de `api-usuario`. `/phoenix` tiene su propio fichero de acceso, abierto en local; en la EC2 exigirá la contraseña siempre. Log de acceso en JSON, preparado para CloudWatch. Probado con la imagen real, un `api-usuario` simulado y Phoenix: rutas de `/api` (con query y caracteres codificados), estáticos, GraphQL y una traza por la red interna; con contraseña, 401 sin credenciales o con una errónea y 200 con la buena; sin contraseña, 403 en `/phoenix` y la web abierta. La retención de 30 días de Phoenix purga los domingos a las 00:00 UTC, con la EC2 apagada: se acepta que las trazas crezcan |
| 2026-10-23 | **Phoenix en la EC2** (Carlos con Claude Code, rama `feature/phoenix-prefijo`): `phoenix` pasa a ser el quinto contenedor del compose de producción, con imagen pública fijada (`20.19.0`), volumen propio y healthcheck con el Python de la imagen (no trae `curl` ni `wget`). `arrancar.sh` genera el acceso a `/phoenix/` (contraseña de la web siempre o 403 sin ella), no busca versión de Phoenix en SSM y deja de reiniciar `web` al redesplegar `api-usuario`. Hallazgo: al relanzar `web` con la misma imagen, compose no recrea el contenedor aunque cambien los ficheros de acceso que monta, así que poner o quitar la contraseña no se aplicaba hasta el siguiente encendido; ahora `arrancar.sh` recarga nginx cuando actualiza `web`. «Desplegar» gana el componente `phoenix`, sin tests ni build. Probado sin AWS: `arrancar.sh` con `aws` y `docker` simulados en seis casos (nunca pide la versión de Phoenix; con permiso denegado se detiene sin generar el acceso) y Phoenix real levantado con el compose de producción (sano en 16 s) detrás de nginx con los ficheros generados: con contraseña, 401 sin credenciales o con una errónea, 200 con la buena y 401 desde la propia máquina; estáticos y GraphQL con prefijo; una traza enviada a `http://phoenix:6006/v1/traces` aparece en su proyecto; sin contraseña, 403 en `/phoenix/` y la web abierta. Pendiente de aplicar en AWS |
| 2026-10-23 | **Arreglo del arranque de la EC2** (Carlos con Claude Code, rama `fix/arranque-ec2`): dos ejecuciones de «Encender entorno» fallaron porque la EC2 ejecutaba el `arrancar.sh` de su disco, anterior a Phoenix, con el `compose.yml` nuevo de S3 (§8.9). `jupiter.service` trae ahora `arrancar.sh` de S3 antes de ejecutarlo; `arrancar.sh` reinstala `jupiter.service` si cambió y borra los directorios que Docker crea en lugar de los ficheros de acceso; «Encender entorno» reintenta el arranque una vez. Sintaxis de `arrancar.sh` y del comando del workflow comprobada con `bash -n`, y `arrancar.sh` probado con `aws`, `docker` y `systemctl` simulados y el fichero de acceso convertido en directorio. EC2 recuperada a mano por SSM el mismo día (borrar el directorio, `arrancar.sh` y `jupiter.service` de S3, reiniciar el servicio): cinco contenedores sanos y 401 sin credenciales en `/`, `/api/health` y `/phoenix/`. `[por confirmar: encendido tras desplegar con la máquina apagada, con el arreglo ya en S3]` |
| 2026-10-23 | **Trazas de Phoenix en la EC2: dirección en el compose** (Carlos con Claude Code, rama `fix/arranque-ec2`): con Phoenix en marcha en `/phoenix/` no llegaban trazas porque no existía `/jupiter/agente/phoenix_endpoint` en SSM (comprobado en AWS: solo `database_url`, `llm_modelo` y `llm_modelo_clasificador`). La imagen activa del agente (`fcfe855`) ya traía el exportador. `PHOENIX_ENDPOINT` pasa a estar fijo en el `environment` del agente en `compose.yml`; se revisaron los demás parámetros del agente y siguen en SSM por ser secretos o cambiables sin desplegar. Aplicado el mismo día: tras una pregunta en el chat de la EC2, su traza aparece en Phoenix (`/phoenix/`) |
| 2026-10-23 | **Logs y métricas de la EC2 en CloudWatch** (Carlos con Claude Code, rama `feature/cloudwatch-logs`): `web`, `api-usuario` y `rag` envían su log a `/jupiter/ec2` con el driver `awslogs` (modo `non-blocking`); `agente` y `phoenix` siguen en la máquina. El agente de CloudWatch publica memoria y disco (2 métricas en `CWAgent`, dimensión `InstanceId`); `arrancar.sh` lo instala si falta y, si falla, avisa y sigue. El grupo de logs lo crea una pila de CloudFormation (`jupiter-observabilidad`) que recogerá también alarmas y panel. Rol de la EC2: escribir solo en ese grupo y métricas solo en `CWAgent`. Probado sin AWS: `docker compose config` (driver por servicio), plantilla validada con `validate-template` y `arrancar.sh` con `aws`, `docker`, `rpm`, `dnf` y el agente simulados en seis casos (primera instalación, ya instalado, un solo servicio, y fallos de `dnf`, de S3 y de la configuración: en los tres avisa y la aplicación arranca). Aplicado el mismo día: pila y permisos del rol, «Desplegar → `phoenix`» (instaló el agente de CloudWatch) y `arrancar.sh rag api-usuario web` por SSM para recrear solo esos tres contenedores, sin apagar la EC2. Verificado: tres *streams*, exactamente 2 métricas en `CWAgent` (memoria ~30 %, disco 23,4 %) y una pregunta al chat en Logs Insights (`POST /api/chat/stream`, 200, 5,6 s). Incidencia: la pila se creó primero en otra cuenta porque la terminal usaba el perfil por defecto; se borró y la guía de despliegue comprueba ahora la cuenta antes de aplicar. `docker compose logs` en la máquina funciona para los cinco servicios. `[por confirmar: arranque completo tras apagar y encender]` |
| 2026-10-23 | **Alarmas y panel de CloudWatch** (Carlos con Claude Code, rama `feature/cloudwatch-logs`): la pila `jupiter-observabilidad` suma al grupo de logs 3 filtros de métricas sobre el log de nginx, 8 alarmas (web 5xx, errores y tokens de Bedrock, estado, créditos, memoria y disco de la EC2, espacio de la RDS) y el panel `jupiter`. Parámetros: `InstanceId` de la EC2 y `UmbralTokensDia` (provisional 2 M; la cifra la fija quien lleva el coste). Los umbrales se fijaron con consultas de solo lectura a la cuenta, que dieron tres hallazgos: EC2 en modo `unlimited` (los créditos son coste, no lentitud), RDS con autoescalado de disco y métricas de Bedrock solo por modelo o para toda la cuenta. La alarma de errores de Bedrock suma dos métricas: 11 métricas en alarmas, una por encima de la capa gratuita `[estimación: 0,10 $/mes]`. Guía de despliegue con la tabla de alarmas, la prueba de una alarma y el cierre del 443. Validado con `validate-template`, `cfn-lint` y el JSON del panel. Aplicado el mismo día: nueve alarmas en `OK` con datos reales al minuto, prueba de `jupiter-web-5xx` con `set-alarm-state` publicada en SNS y filtros comprobados con `test-metric-filter` sobre una línea real del chat. Grupo de seguridad: cerrados el 443 y la regla de la IP del equipo; queda solo el 80 abierto a todo internet con contraseña. `[por confirmar: 503 reales con el agente parado]` |
| 2026-10-23 | **Batería de uso contra la EC2** (Carlos con Claude Code): para poblar Phoenix y el panel `jupiter` con uso representativo, 95 peticiones con contraseña en 234 s y 2 en paralelo: 61 turnos de chat sacados de los casos de evaluación del agente (turno, datos, comprobaciones, 5 conversaciones de varios turnos y una muestra del clasificador; 3 de cada 4 por `/api/chat/stream`), 31 consultas de estaciones y series y 3 errores de cliente. Resultado: 61/61 turnos con 200, ningún 5xx, 3 respuestas 422 esperadas; tiempo por turno p50 4,7 s, p90 9,1 s y máximo 22,2 s medido en el cliente. En CloudWatch: 218 invocaciones y unos 245 000 tokens de entrada en Bedrock (un 12 % del umbral diario provisional), `TiempoRespuestaChat` con media de 5,5 s y las diez alarmas en `OK`. En Phoenix, 66 trazas y 597 spans en el proyecto `agente`, mediana de 4,9 s |
| 2026-10-23 | **Métricas de negocio del pipeline** (Carlos con Claude Code, rama `feature/metricas-pipeline`): la Lambda publica en formato EMF `HorasNuevas`, `EstacionesConDatos`, `BloquesAnomalos` y `TasaAnomalias` (namespace `Jupiter/Pipeline`), también con ceros cuando la API no devuelve registros, más tres campos de detalle que no son métricas (`pct_validas`, `estaciones_sin_datos`, `anomalias_por_magnitud`). Alarma nueva `jupiter-pipeline-sin-datos` (cierra el fallo silencioso del `{"ok": False}`) y fila «Negocio» en el panel, con las preguntas del día como `SampleCount` de `TiempoRespuestaChat`. 9 de 10 métricas gratuitas. Tests unitarios: 125 en verde. Desplegado el mismo día: métricas sin dimensiones y alarma en `OK`; la primera ejecución (manual, a media tarde) dio un 33,3 % de bloques anómalos, uno por serie, que apunta al bloque en curso incompleto (§8.7) |
| 2026-10-23 | **KPIs de uso del asistente desde Phoenix** (Carlos con Claude Code, rama `feature/kpis-phoenix`): script `kpis_phoenix.py` que lee los spans de la API REST de Phoenix y calcula preguntas por día, mezcla de rutas y temas, tasa de resolución, bloqueos, coste por pregunta (P50/P90 y por pregunta resuelta), latencia por ruta, tiempo hasta el primer token y fallos de herramienta por causa, con dos figuras SVG (embudo y coste) y sin texto de usuarios. Sesiones, reformulación y satisfacción, preparados para cuando llegue `session.id`. El agente emite el evento `primer_token` en el span `turno`. Probado contra el Phoenix local; 9 tests nuevos, 152 del agente en verde. `[por confirmar: informe de la EC2]` |
| 2026-10-23 | **Valoración 👍/👎 de los usuarios, parte de servidor** (Carlos con Claude Code, rama `feature/valoracion-usuario`): el agente devuelve el `span_id` del turno en `done` y en `/responder`, marca todos los spans con `session.id` y expone `POST /valoracion`, que anota el voto en Phoenix sobre el span `turno` (`valoracion_usuario`, `HUMAN`, 1/0). api-usuario lo reenvía desde `POST /chat/valoracion`. 11 tests nuevos; 159 del agente y 26 de api-usuario en verde. `[por confirmar: aviso en la web y despliegue]` |
| 2026-10-23 | **Valoración 👍/👎 de los usuarios, web e informe** (Carlos con Claude Code, rama `feature/valoracion-usuario`): aviso «¿Qué tal lo está haciendo el asistente hoy?» en el panel y en `/asistente` tras la 2.ª respuesta, una vez al día por navegador; `traza_id` y `span_id` leídos del `done`; contrato OpenAPI regenerado. El informe de KPIs añade satisfacción total, por ruta y por tema con su *n* y la figura de satisfacción por ruta. Tests: 43 de la web, 162 del agente y 26 de api-usuario en verde. `[por confirmar: despliegue y anotación real en la EC2]` |
| 2026-10-23 | **Revisión local de valoraciones** (Codex): simplificados los comentarios; reproducidos el estado diario obsoleto del aviso y el sesgo del denominador de participación. Detectada por lectura la falta de validación de pertenencia del span. Hallazgos pendientes de corrección (§8). Web: 43 pruebas, lint y compilación correctos. Agente y API: 188 pruebas correctas y 10 omitidas; ejecución fuera del sandbox por bloqueo de TestClient dentro de él. |
| 2026-10-23 | **PR de la valoración a `development`** (Carlos con Claude Code): PR #95. El primer intento, #93, se abrió contra `main` y la protección falló como debía; al rehacerlo con el mismo commit, el fallo seguía visible en el #95 porque los checks van por commit (§9). Tests de la PR en verde |
| 2026-10-23 | **KPIs de uso servidos por el agente** (Carlos con Claude Code, rama `feature/kpis-agente`): `GET /kpis` en el agente (últimos 1, 7 o 30 días; lector de Phoenix, cálculo en la capa de negocio, caché de 60 s, 503 sin Phoenix) y `GET /kpis` en api-usuario (404 sin agente, 503 caído, 502 fuera de contrato). Turnos, coste y precios pasan a `agente/business/trazas.py`, compartidos con `informe_trazas.py` sin cambios de cifras. Se elimina el script `kpis_phoenix.py` y sus tests; sus casos pasan al agente. Tests: 171 del agente (10 omitidos) y 34 de api-usuario en verde. `[por confirmar: página web y despliegue]` |
| 2026-10-23 | **Página «Uso del asistente» en la web** (Carlos con Claude Code, rama `feature/kpis-agente`): `/admin/uso` con rótulo «Solo administradores» sin protección, enlace en el pie, seis indicadores con su *n*, embudo, histograma del coste con P50/P90, satisfacción por ruta y tablas; contrato OpenAPI regenerado. Sin cambios en `compose.yml`: la raíz de Phoenix sale de `PHOENIX_ENDPOINT`. Web: 56 tests Vitest, typecheck, lint y build en verde. `[por confirmar: despliegue y prueba en la EC2]` |
| 2026-10-23 | **Job `frontend` en CI** (Carlos con Claude Code, rama `feature/kpis-agente`): README, memoria y comentarios daban por hecha la comprobación del contrato web-API, pero el job nunca se había añadido a `tests.yml`. Nuevo sexto job: regenera `openapi.json` desde `ApiUsuario` (FastAPI 0.161.1 y pydantic 2.14.1 fijados) y `schema.d.ts`, falla con `git diff --exit-code` si difieren y pasa lint, tests y build de la web. Comprobado en local: el contrato regenerado coincide byte a byte con el versionado; lint, 56 tests y build en verde. Primera ejecución en GitHub `[por confirmar]` |
| 2026-10-23 | **Ingesta al cierre de cada franja y bloque de noche sobre 3 horas** (Guille con Claude Code, rama `feature/ingesta-por-franjas`): el pipeline solo escribe bloques cerrados y mide la noche sobre las 3 horas que publica la API; `--reprocesar` rehace los días ya cargados. Simulado en el histórico: noche marcada 8,69 % → 0,49 % (0,44 % con las 4 horas). Horario elegido: 07:45, 13:45, 20:45 y 23:45. «Apagar entorno» ya no apaga la RDS. Tests unitarios y del agente en verde. Pendiente en AWS: desactivar las programaciones de la RDS, desplegar la Lambda, cambiar el horario y reprocesar |
| 2026-10-23 | **Ingesta por franjas aplicada en AWS** (Guille con Claude Code): desactivadas `jupiter-rds-encender`, `jupiter-rds-apagar` y, para la entrega, `jupiter-ec2-apagar`; RDS y EC2 quedan siempre encendidas. Lambda desplegada desde `main` y `jupiter-pipeline-diario` cambiada a `cron(45 7,13,20,23 * * ? *)`. `--reprocesar 2026-09-16`: 15.869 bloques reescritos; la noche pasa del 7,19 % al **0,74 %** de bloques marcados y su cobertura media de 0,75 a 0,989 (§6.5 bis). Invocación manual a media tarde: 214 bloques (madrugada y mañana; la tarde, abierta, no se escribe), 0 anomalías, 23 estaciones, frente a las 107 anomalías de la tarde a medias en la prueba de las 16:07 |

---
| 2026-10-23 | **Web pública para la corrección del TFM** (Guille con Claude Code, rama `feature/web-publica`): script `deploy/aws/3_web_publica.ps1` que crea una IP elástica para la EC2 y la asocia, borra la contraseña de la web (`/phoenix/` pasa a 403 para todos) y crea el presupuesto diario `jupiter-bedrock-diario` (2 $/día, correo al 80 % y al 100 %); después, «Desplegar → `web`». Sin cambios de código: el acceso sin contraseña ya estaba previsto en `arrancar.sh`. Guía de despliegue y workflow «Encender entorno» actualizados (la IP ya no cambia). Validado sin AWS: sintaxis del script y JSON del presupuesto. Pendiente de aplicar en AWS `[por confirmar]` |

## 13. Preguntas abiertas

- [ ] ¿Qué límite de gasto o créditos tiene la cuenta AWS del máster?
- [ ] Agente nuevo: ¿Ministral 14B 3.0 o gpt-oss-120b en Bedrock? Empatan en clasificador y uso de
  herramientas. Ruta documental (2026-10-05): Ministral 18/18 a la primera; gpt-oss 4 fallos de 18
  por el límite de 512 tokens de salida. Con `LLM_MAX_TOKENS`=2048 (lote del 2026-10-06): empate,
  6/6 válidas a la primera con los dos (12/12 gpt-oss y 11/12 Ministral con el caso del polen
  corregido); gpt-oss algo más rápido y ~50 % más caro. Tras la fase 5, 29/29 en el clasificador y
  11/11 en conversaciones con los dos. **Elección aplazada** (2026-10-06) hasta poder evaluar
  también las herramientas SQL y de ML. Falta la revisión humana de las respuestas y del español.
  Tercer candidato desde el 2026-10-16: **Qwen3 Next 80B** (16/20 en datos no vistos con SQL libre,
  como gpt-oss; peor como selector del catálogo y copia literal del prompt de sistema). Cuarto desde el
  2026-10-18: **Nemotron 3 Super 120B** (clasificador 31/31, 16/20 con SQL libre, 14/20 con el
  catálogo, el más rápido; también copia el prompt literal). Claude Haiku 5.5 sigue sin acceso aun con
  el formulario de Anthropic enviado: consultado al máster, sin respuesta a 2026-10-18. (§7.3.1)
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
  Sustituido el 2026-10-09 en el `Agente` por la respuesta compuesta (§2, §7.3.1).
- [x] Reconciliar `LLMOrchestrator` con la Fase 2 reescrita → hecho el 2026-09-30: el orquestador
  adopta el contrato de evidencias en proceso (tool `buscar_evidencias` + validación en el
  cierre); el umbral y las fuentes son ya los del servicio (§7.3).
- [ ] Reindexar `data/chroma/` con e5 en cada entorno de trabajo (`python -m rag.indexar`): el
  índice construido antes de la reescritura no tiene metadatos de modelo y la búsqueda lo rechaza.
  Comprobado en el entorno de Carlos el 2026-10-01 (e5-base, 50 fragmentos); falta el resto.
- [x] ¿Dónde se despliega el servicio RAG en producción? → decidido: contenedor propio en la EC2,
  con el modelo y el índice dentro de la imagen (§8.9). Pendiente de crear los recursos en AWS.
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
