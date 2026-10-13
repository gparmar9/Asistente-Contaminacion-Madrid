# Decisiones y memoria del agente (`Agente/`)

Registro propio del servicio `Agente`: qué se decidió, qué se descartó y por qué, y los datos
que conviene recordar. Complementa `docs/notas_memoria.md` (§2 y §7.3), que resume estas
decisiones para la memoria del TFM; aquí está el detalle.

**Cómo mantenerlo:** las secciones de estado (§4 a §6) describen el servicio tal como es hoy y se
sobrescriben. Las decisiones (§2) se añaden con fecha y no se borran; si una se revierte, se añade
otra fila que lo diga. Sin secretos ni identificadores de la cuenta de AWS.

---

## 1. Qué es el agente

Un solo agente conversacional con un **bucle de herramientas escrito a mano**. El modelo pide
herramientas; el código las ejecuta, decide y puede cerrar el turno sin el modelo. Vive en el
servicio `Agente/` (FastAPI, puerto 8200), hermano de `ApiUsuario/` e independiente de
`LLMOrchestrator/` (ni código, ni dependencias, ni imagen compartidas).

Principios de diseño (de las clases del máster y de un agente empresarial de referencia):

| Principio | Qué significa aquí |
|-|-|
| Pocas piezas, herramientas estrechas | Un servicio, una herramienta al principio, prompts cortos |
| El modelo clasifica, el código decide | Un clasificador de intención (fase 3); el código veta u obliga herramientas |
| Fases pequeñas con salida tipada y valor seguro | Cada llamada al LLM responde una sola pregunta; si falla, hay un valor por defecto que no rompe el turno |
| La síntesis no tiene herramientas | La llamada final no puede llamar a nada |
| Lo determinista lo entrega el código | El texto con citas lo renderiza el RAG (`/rag/validar`); el modelo no lo reescribe |
| Estado tipado entre turnos | Cifras, temas y referentes se guardan como datos, no como prosa (fase 5) |
| Una fase nueva debe retirar una comprobación | Las comprobaciones se añaden en modo observación y solo bloquean con evidencia (fase 6) |

---

## 2. Decisiones

| Fecha | Decisión | Alternativas descartadas | Motivo |
|-|-|-|-|
| 2026-10-04 | **Nuevo servicio `Agente/`** en vez de ampliar `LLMOrchestrator` | Rehacer el bucle dentro del orquestador; meter el agente en `ApiUsuario` | El bucle del orquestador no tiene puntos de intervención en código (vetar/obligar herramientas, cerrar sin modelo, validar con el RAG); rehacerlo dentro equivalía a reescribirlo. `ApiUsuario` se diseñó ligero y así lo describe la memoria. Los dos agentes conviven y `ApiUsuario` elige por configuración (`AGENTE_URL`, fase 4) |
| 2026-10-04 | **Bucle de herramientas escrito a mano** | Framework de agentes completo: ReAct o `AgentWorkflow` de LlamaIndex, LangGraph | El framework esconde justo el bucle que se quiere controlar y arrastra abstracciones que no se usan. El bucle propio cabe en un fichero y cada fase le añade un paso visible |
| 2026-10-04 | **LlamaIndex solo como cliente del LLM** (y, en la fase 5, almacén del chat). Detalle en §3 | SDKs directos (`openai` + `boto3`); LangChain; LiteLLM | Una interfaz (`FunctionCallingLLM`) para OpenAI-compatible y Bedrock Converse, con los dos formatos de *tool use*, streaming y reintentos resueltos; almacén de chat incluido. Ver §3 |
| 2026-10-04 | **Dos proveedores desde la fase 1**: `OpenAILike` (Mistral API, Groq, Ollama…) y `BedrockConverse`, elegidos con `LLM_PROVEEDOR` | Fijar un único proveedor ahora | Bedrock es el destino en producción (hay acceso con la cuenta del máster) pero el modelo concreto en `eu-west-1` con *tool use* y *streaming* está `[por confirmar]`; el proveedor OpenAI-compatible cubre el desarrollo y es la salida si Bedrock falla (fase 7) |
| 2026-10-04 | **La definición de `buscar_evidencias` la publica el RAG** (`GET /rag/herramienta`), cacheada por proceso; si el GET falla, la herramienta **no se ofrece en ese turno** | Copiar el esquema en el agente | Única fuente de verdad del esquema; el agente degrada en vez de fallar y se reintenta en el turno siguiente |
| 2026-10-04 | **Las herramientas nunca lanzan**: devuelven `{ok, datos \| error}` y el error llega al modelo como resultado | Propagar excepciones | El modelo puede corregirse en la siguiente vuelta y el usuario siempre recibe respuesta |
| 2026-10-04 | **Máximo 3 vueltas** con herramientas; después, **síntesis forzada sin herramientas** (peor caso: 4 llamadas al LLM) | Sin límite; límite más alto | Los tiers gratuitos limitan peticiones por minuto y cada vuelta es una llamada; con una herramienta no hacen falta más |
| 2026-10-04 | **El stream del LLM se drena ya en la fase 1** (`astream_chat_with_tools`) | Llamadas sin streaming hasta la fase 4 | Que el streaming SSE (fase 4) no cambie la forma del bucle |
| 2026-10-04 | **Sesiones aceptadas** (`session_id` opaco, generado si no llega), a implementar en la fase 4 | Mantener «sin estado» (decisión del 2026-09-27 para la API) | Las preguntas de seguimiento no se responden sin historial. Sin usuarios ni datos personales: solo un identificador opaco |
| 2026-10-04 | **La ruta documental entrega el texto al final**, con eventos `status` mientras tanto | Streaming del texto documental | No se puede validar un JSON a medias contra `/rag/validar` |
| 2026-10-04 | **Política de tests mínima**: un test por caso que enumera el plan de cada fase, sobre comportamiento visible | Tests de detalle interno (contadores de peticiones, mensajes de error concretos, 422, `/salud`) | No aportan valor para el TFM y encarecen cada cambio. Se partió de 25 tests y se recortaron a 8 |
| 2026-10-04 | **Fase 1: `fuentes` lista todos los documentos devueltos por el RAG y `advertencia` queda vacía** | — | Provisional: la fase 2 restringe `fuentes` a los documentos citados y activa la advertencia sanitaria solo con afirmaciones documentales |
| 2026-10-04 | **Ruta documental tras el bucle**: el modelo puede seguir pidiendo herramientas después de buscar; cuando para (o se agotan las vueltas) y hubo evidencias, su texto libre se descarta y se hace una llamada aparte, sin herramientas, que devuelve el JSON | Cortar el bucle en cuanto llegan evidencias (2 llamadas en vez de 3) | El RAG será una herramienta entre varias (SQL, modelos de ML): el modelo debe poder seguir consultando. Coste: una llamada más al LLM por pregunta documental |
| 2026-10-04 | **Passthrough íntegro** del texto que renderiza `/rag/validar` (citas, aviso, evidencias citadas con `chunk_id`, bibliografía). `fuentes` (documentos citados) y `advertencia` se rellenan igualmente | Que el agente componga el texto con los campos validados | El texto final no lo toca nadie más que el RAG. Se acepta repetir aviso y documentos; la comprobación de internos (fase 6) tendrá que ignorar el bloque de evidencias citadas |
| 2026-10-04 | **La síntesis documental parte de mensajes nuevos**: pregunta + evidencias renumeradas, sin el historial de herramientas | Reutilizar la conversación del bucle | Prompt más corto y Bedrock Converse rechaza bloques de herramienta sin `toolConfig` |
| 2026-10-04 | **Renumeración en el agente**: cada búsqueda del RAG numera desde D1; el agente reasigna D1..Dn por orden de llegada, sin repetir `chunk_id`, antes de que el modelo vea el resultado | Pedir al RAG una numeración global | El RAG no conoce el turno; el modelo ve IDs únicos desde la primera vez |
| 2026-10-04 | **`sin_evidencia` cierra el turno sin modelo solo si el turno aún no tiene evidencias** | Cerrar siempre que una búsqueda salga vacía | Una segunda búsqueda vacía no debe tirar las evidencias de la primera |
| 2026-10-04 | **Salida que no es JSON → se manda igual a `/rag/validar`**, que la rechaza con su mensaje de reparación. Si `/rag/validar` no responde → frase de insuficiencia | Validar el JSON en el agente | Un solo sitio valida el contrato (el RAG) |
| 2026-10-04 | **Clasificador = mismo modelo con temperatura 0**, segunda instancia del cliente LLM, con tiempo límite propio (`CLASIFICADOR_TIMEOUT_S=10`) | Pasar la temperatura en cada llamada; un modelo más pequeño solo para clasificar | Funciona igual con los dos proveedores. Un modelo propio para el clasificador se puede añadir si la latencia lo pide |
| 2026-10-04 | **Parser estricto**: dos líneas `intencion:` y `tema:` con valores conocidos; cualquier desviación da `DESCONOCIDA` | Parser tolerante que busque la palabra en el texto | Un formato dudoso no debe disparar frases fijas. El guion de evaluación dirá si el modelo cumple el formato |
| 2026-10-04 | **`REPETIR` fuera del clasificador hasta la fase 5** (decisión del usuario) | Clasificarla ya y tratarla como `DESCONOCIDA` | Sin historial no hay nada que repetir; se añade con la memoria |
| 2026-10-04 | **`DOCUMENTAL` con el RAG caído → frase fija sin modelo** (decisión del usuario) | Dejar que el modelo responda en libre | Evita afirmaciones de salud sin evidencias |
| 2026-10-04 | **Búsqueda obligada solo si el modelo no la pide**: el código busca con la pregunta literal y el tema del clasificador y pasa directo a la ruta documental (decisión del usuario) | Buscar siempre antes del modelo | Se conserva la reformulación de la búsqueda que hace el modelo. Coste: una llamada más cuando el modelo sí busca |
| 2026-10-04 | **El tema del clasificador solo se usa en la búsqueda que lanza el código** | Sobrescribir el tema que elige el modelo | No pisar al modelo sin datos; revisar tras medir el clasificador |
| 2026-10-04 | **Herramienta vetada**: no se ofrece; si el modelo la pide, recibe un error («no está disponible para esta pregunta») y no se ejecuta | Cerrar el turno | El modelo puede corregirse en la vuelta siguiente |
| 2026-10-04 | **Guion de evaluación: 15 preguntas de `Preguntas.txt` + 5 nuevas** para `CHARLA` y `FUERA_DE_ALCANCE` (decisión del usuario) | Solo preguntas de `Preguntas.txt` | El fichero no tiene ejemplos de esas dos intenciones |
| 2026-10-04 | **Comparar dos modelos de Bedrock en `eu-west-1`** antes de elegir: **Ministral 14B 3.0** (`mistral.ministral-3-14b-instruct`, $0,24 / $0,24 por 1M de tokens de entrada / salida) y **gpt-oss-120b** (`openai.gpt-oss-120b-1:0`, $0,18 / $0,70) (decisión del usuario) | Mistral Small 3.2 (24B, la referencia del equipo): no está en Bedrock y no se puede importar (*Custom Model Import* no admite su arquitectura multimodal ni está en `eu-west-1`). Magistral Small 1.2 (Small 3.2 con razonamiento): $0,59 / $1,76 y más lento, no cabe en 4 llamadas por turno con el clasificador a 10 s. Mistral Large 3: no está en regiones de la UE. Pixtral Large: solo por perfil `eu.` y fecha de retirada cercana | Ministral 14B es el Mistral actual más parecido a Small 3.2 (sin razonamiento, en la propia región). gpt-oss-120b es el equivalente más próximo a Mistral Small 4 (MoE ~120B con ~5B activos). Coste estimado de los dos: ~$0,003 por turno documental, así que decide la calidad. Precios de la página de Bedrock para Irlanda, consultada el 2026-10-04 |

---

## 3. LlamaIndex: función y motivación

### Qué hace en el agente

Solo es el **cliente del LLM**. Toca tres sitios: la fábrica `llm/cliente.py`, el adaptador de
`tools/base.py` y los tipos de mensaje (`ChatMessage`, `ChatResponse`) que usa `business/bucle.py`.
No hay agente, índice, *retriever* ni *workflow* de LlamaIndex: el bucle es nuestro.

Lo que resuelve:

- **Una interfaz para dos proveedores.** `OpenAILike` y `BedrockConverse` heredan de
  `FunctionCallingLLM`; el bucle llama siempre a lo mismo y el proveedor se elige por variable de
  entorno.
- **Los dos formatos de *tool use*.** OpenAI envía `tools` y recibe `tool_calls`, con el resultado en
  un mensaje `role: tool` y `tool_call_id`. Bedrock Converse envía `toolConfig` y usa bloques
  `toolUse` y `toolResult` dentro de un mensaje de usuario. LlamaIndex traduce la definición de la
  herramienta, la petición del modelo y nuestro resultado a cada formato. Sin él habría que mantener
  dos serializadores y dos analizadores de llamadas.
- **Streaming y reintentos** de cada proveedor con la misma forma de consumo.
- **En la fase 5, el almacén del chat** (`SimpleChatStore` en memoria/JSON, `PostgresChatStore`
  sobre la base del proyecto), sin escribir la persistencia a mano.

### Por qué se eligió frente a las alternativas

| Alternativa | Por qué no |
|-|-|
| SDKs directos (`openai` + `boto3`) | Es lo que hace `LLMOrchestrator`, solo con OpenAI. Añadir Bedrock duplicaría la capa de mensajes y herramientas dentro del bucle, que es justo el código que debe seguir pequeño |
| LangChain / LangGraph | Resuelven lo mismo pero empujan hacia sus abstracciones de agente y grafo; para usar solo el cliente se arrastra el mismo peso y no se gana el almacén de chat |
| LiteLLM | Alternativa seria: expone Bedrock con formato OpenAI. No aporta el almacén de chat y su modo robusto es un proxy aparte, una pieza más que desplegar |
| Framework de agentes de LlamaIndex (ReAct, `AgentWorkflow`) | Esconde el bucle que el plan quiere controlar en código |

### Coste y salida

- Coste: peso de la dependencia (`llama-index-core` arrastra bastantes paquetes) y ritmo de cambios
  alto. Por eso las versiones están **fijadas** en `requirements.txt`.
- Exposición pequeña: si hubiera que sustituirlo, cambian la fábrica, el adaptador y los tipos de
  mensaje del bucle. La lógica del turno no.
- Detalle técnico que conviene recordar: `OpenAI._prepare_chat_with_tools` modifica en sitio el
  esquema de la herramienta (añade `additionalProperties: false`); el adaptador devuelve una copia
  para no ensuciar la definición cacheada. Para `OpenAILike` hay que marcar `is_chat_model` e
  `is_function_calling_model`, si no LlamaIndex trata el modelo como de completado sin herramientas.

---

## 4. Datos del servicio (estado actual)

| Dato | Valor |
|-|-|
| Puerto | 8200 |
| Endpoints | `GET /salud`, `POST /responder` |
| Llamadas al LLM por turno | Incluyen el clasificador. Frase fija: 1. Charla: 2. Documental: 4 (clasificar, pedir búsqueda, texto descartado, JSON); 5 con reparación; 3 si busca el código. `DESCONOCIDA`: 2–5 |
| Python | 3.12 (Docker y CI); 3.12 en el entorno local `Agente/.venv` |
| Dependencias fijadas | `llama-index-core` 0.14.25, `llama-index-llms-openai-like` 0.8.1, `llama-index-llms-bedrock-converse` 0.15.3, `fastapi` 0.142.2, `httpx` 0.28.1 |
| Vueltas máximas | 3 (`MAX_VUELTAS`) |
| Temperatura | Síntesis 0,2 (`LLM_TEMPERATURA`); clasificador 0 |
| Timeouts | LLM 30 s por intento con 2 reintentos; clasificador 10 s en total; RAG 20 s |
| Tests | 20, sin red ni claves; job de CI «Tests del agente» |
| Imagen Docker | `jupiter-agente` (sin construir aún) |
| Proveedor real probado | Ninguno `[pendiente]` |

---

## 5. Contratos

**`POST /responder`** `{pregunta}` → `{respuesta, fuentes[{tipo: sql|documento, referencia}], advertencia}`.
Es el contrato que `ApiUsuario` ya reenvía desde `/chat`. Las fases 4 y 5 añaden `session_id`
opcional y `POST /responder/stream` (SSE con eventos `status`, `token`, `passthrough`, `error`, `done`).

**Con el RAG** (`rag.api`): `GET /rag/herramienta` (definición), `POST /rag/evidencias`
(herramienta), `POST /rag/validar` `{pregunta, evidencias:[{id, chunk_id}], salida}` (ruta
documental; lo llama el código). Al modelo solo se le entrega `para_el_modelo`: sin `chunk_id` ni
internos. El esquema del JSON documental también lo publica el RAG (`esquema_salida`).

---

## 6. Estado por fases

| Fase | Contenido | Estado |
|-|-|-|
| 1 | Esqueleto, fábrica de LLM, LLM falso, `buscar_evidencias`, bucle mínimo, CI | **Hecha** (2026-10-04, rama `feature/Agente`) |
| 2 | Ruta documental determinista: JSON validado con `/rag/validar`, una reparación, *passthrough*, `sin_evidencia` cierra sin modelo | **Hecha** (2026-10-04, sin commit) |
| 3 | Clasificador de intención con valor seguro `DESCONOCIDA` y tabla de decisión en código | **Hecha** (2026-10-04, sin commit). Clasificador sin medir: falta proveedor |
| 4 | Streaming SSE, sesión, `AGENTE_URL` en `ApiUsuario` | Pendiente |
| 5 | `PostgresChatStore`, ventana por tokens, `turn_state`, intención `REPETIR` | Pendiente |
| 6 | Comprobaciones posteriores en modo observación, log por turno, evaluación con 20 preguntas | Pendiente |
| 7 | Bedrock, `docker-compose`, despliegue junto al orquestador | Pendiente |

---

## 7. Pendiente y por confirmar

- **Comparar Ministral 14B 3.0 y gpt-oss-120b en Bedrock** (§2, 2026-10-04) con
  `evaluar_clasificador.py` y preguntas documentales: herramientas pedidas cuando toca, JSON válido
  a la primera, calidad del español, latencia. Por confirmar en la cuenta:
  - Ministral 14B: *tool use* por Converse. La ficha de AWS solo lo confirma por Chat Completions.
  - gpt-oss-120b: cómo llega su razonamiento por Converse.
- `BedrockConverse` 0.15.3 no conoce el ID de Ministral 14B: la primera llamada falla con
  `ValueError: Unknown model` (comprobado sin red). Hace falta una subclase en `llm/cliente.py` que
  declare los metadatos del modelo. gpt-oss-120b ya está en la lista de LlamaIndex.
- Proveedor OpenAI-compatible para desarrollo (Mistral API, Groq u Ollama). El servicio arranca sin
  él y responde 503 en `/responder` hasta rellenar `.env`.
- Prueba de punta a punta con un proveedor real y `rag.api` levantado. Con el LLM falso y el `rag.api`
  real ya se probaron los cuatro caminos de la ruta documental (2026-10-04): válida, reparada, doble
  fallo y `sin_evidencia`.
- Precisión del clasificador: ejecutar `Agente/evaluacion/evaluar_clasificador.py` con un proveedor real.
