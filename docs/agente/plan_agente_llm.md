# Plan: agente LLM por fases (esqueleto con la herramienta RAG)

Fecha: 2026-10-04. Documento de trabajo, no versionado (`docs/**/plan_*.md` está en `.gitignore`).
Se ignora `LLMOrchestrator`: el agente se diseña desde cero.

## 0. Resumen

Un solo agente, con un bucle de herramientas escrito a mano. LlamaIndex aporta solo dos piezas:
el cliente del LLM (Bedrock o cualquier API OpenAI-compatible) y el almacén del chat.

Principios, tomados de los dos diseños de referencia:

| Principio | De dónde viene | Qué significa aquí |
|-|-|-|
| Pocas piezas, instrucciones operativas, herramientas estrechas | Clases del máster (sesión 5) | Un servicio, una herramienta al principio, prompts cortos |
| El modelo clasifica, el código decide | Agente empresarial | Un clasificador de intención; el código veta u obliga herramientas |
| Fases pequeñas con salida tipada y valor seguro | Agente empresarial | Cada llamada al LLM responde una sola pregunta y, si falla, hay un valor por defecto que no rompe el turno |
| La síntesis no tiene herramientas | Agente empresarial | La llamada final no puede llamar a nada ni filtrar razonamiento |
| Lo determinista lo entrega el código | Agente empresarial (passthrough, restate) | El texto con citas lo renderiza el servicio RAG; el modelo no lo reescribe |
| Estado tipado entre turnos, escrito por el código | Agente empresarial (turn_state) | Cifras, temas y referentes se guardan como datos, no como prosa |
| Una fase nueva debe retirar una comprobación | `PHASES.md` del agente empresarial | Las comprobaciones posteriores se añaden en modo observación y solo bloquean con evidencia |

## 1. Dónde vive

**Nuevo servicio `Agente/`**, hermano de `ApiUsuario/`. Motivos:

- `ApiUsuario` se diseñó ligero (lecturas SQL + proxy de chat) y así lo describe la memoria. Meter el
  agente dentro cambiaría esa decisión y mezclaría dependencias.
- El agente necesita su propio ciclo de vida: prompts, modelo, memoria, despliegue.
- Mantiene la costura actual: `ApiUsuario` ya reenvía `/chat` a `POST {URL}/responder`. El nuevo
  servicio respeta ese contrato y lo amplía (sesión y streaming).
- Totalmente independiente de `LLMOrchestrator`: código, requirements, imagen, puerto (8200) y job de
  CI propios. Ninguno importa del otro.

Estructura propuesta (misma convención por capas que `ApiUsuario`):

```
Agente/
  src/agente/
    main.py                 FastAPI: /salud, /responder, /responder/stream
    config/settings.py      variables de entorno
    entities/               Pregunta, Respuesta, Fuente, Intencion, TurnState, eventos SSE
    business/
      bucle.py              el turno: clasificar -> decidir -> ejecutar -> sintetizar -> comprobar -> guardar
      intencion.py          fase: clasificador (una pregunta, salida tipada, valor seguro)
      sintesis.py           llamada final sin herramientas
      passthrough.py        respuestas que entrega el código tal cual
      comprobaciones.py     cifras inventadas, fuga de prompt, internos
      memoria.py            ventana de historial + turn_state
      frases.py             textos fijos en español (rechazos, insuficiencia, estados)
    tools/
      base.py               contrato de herramienta (nombre, esquema, ejecutar) y resultado tipado
      rag.py                buscar_evidencias: cliente HTTP de rag.api
    llm/
      cliente.py            fábrica LlamaIndex: BedrockConverse | OpenAILike
      falso.py              LLM con guion para tests
  tests/
  Dockerfile, requirements.txt, requirements-dev.txt, .env.example, README.md
```

## 2. El turno, de principio a fin

```
pregunta (+ session_id)
  -> bloqueo por sesión (un turno a la vez)
  -> cargar ventana de historial + turn_state previo
  -> CLASIFICAR (LLM, 1 llamada corta)        -> Intencion tipada; si falla: DESCONOCIDA
  -> DECIDIR (código)                           -> herramientas permitidas/obligadas o respuesta fija
  -> EJECUTAR (bucle a mano, máx. 3 vueltas)    -> el modelo pide herramientas; el código las ejecuta
       intervención: si el RAG devuelve sin_evidencia, el código cierra el turno sin modelo
       reparación: si la herramienta era obligatoria y el modelo no la pidió, la llama el código
  -> SINTETIZAR (LLM, sin herramientas)        -> texto libre (streaming) o JSON tipado (ruta documental)
  -> ENTREGAR
       ruta documental: /rag/validar renderiza el texto con [Dn] y bibliografía -> passthrough
       ruta libre: tokens al cliente según llegan
  -> COMPROBAR (código)                         -> cifras, fuga de prompt, internos (observar/bloquear)
  -> GUARDAR                                    -> pregunta, respuesta final y turn_state (escrito por el código)
```

### 2.1 Clasificador de intención

Una llamada, temperatura 0, respuesta en dos líneas, parser estricto, valor seguro.

| Intención | Ejemplo | Decisión del código |
|-|-|-|
| `DOCUMENTAL` | «¿Qué efectos tiene el NO2 en el asma?» | `buscar_evidencias` obligatoria. Ruta documental |
| `DATOS` | «¿Qué estación está peor ahora?» | Sin herramienta todavía: frase fija (el agente aún no consulta mediciones) |
| `PREDICCION` | «¿Habrá pico de ozono el finde?» | Frase fija: no hay predicción; ofrece lo documental |
| `REPETIR` | «¿Cuál era ese límite?» | Fase 5: respuesta desde `turn_state`, sin modelo |
| `CHARLA` | «Hola, ¿qué sabes hacer?» | Sin herramientas. Síntesis libre con streaming |
| `FUERA_DE_ALCANCE` | «Recomiéndame un restaurante» | Frase fija de rechazo |
| `DESCONOCIDA` | fallo del clasificador | Todas las herramientas disponibles, ninguna obligada, sin frases fijas |

Segundo eje: `tema` ∈ {salud, normativa, proyecto, ninguno}. Alimenta el parámetro `tema` de la
herramienta, es decir, se usa para responder, no para vetar.

El valor seguro es `DESCONOCIDA` con todas las herramientas, no `DOCUMENTAL`: un clasificador caído no
debe quitar herramientas ni forzar frases fijas (lección medida en el agente empresarial).

### 2.2 Bucle de herramientas a mano

- `llm.astream_chat_with_tools(tools, messages)` de LlamaIndex; el código lee los `tool_calls`,
  ejecuta, añade el mensaje `tool` y repite. Máximo 3 vueltas; después, síntesis forzada.
- Las herramientas nunca lanzan: devuelven un resultado tipado `{ok, datos | error}`.
- Entre el resultado de la herramienta y la síntesis el código puede: cerrar el turno
  (`sin_evidencia`), renumerar evidencias si hubo varias llamadas, o descartar una herramienta vetada.
- Solo se ofrecen al modelo las herramientas que la decisión permitió. Si el modelo pide una
  vetada, el código la rechaza con un mensaje `tool` de error y sigue.

### 2.3 Síntesis sin herramientas

- Ruta documental: el modelo devuelve el JSON `{estado, afirmaciones[{texto, evidencias}], limitaciones}`
  que ya define `rag.api` (`ESQUEMA_SALIDA`). El código lo valida con `POST /rag/validar`, permite una
  reparación, y entrega el texto renderizado por el RAG tal cual (passthrough). El modelo nunca
  escribe el texto final con citas.
- Ruta libre (`CHARLA`, `DESCONOCIDA` sin herramientas): texto en streaming, prompt corto y fijo.
- Compromiso del streaming: la ruta documental entrega el texto al final (no se puede validar un
  JSON a medias). Durante la espera el cliente recibe eventos `status` con la fase en curso.

### 2.4 Comprobaciones posteriores (código)

| Comprobación | Regla | Acción |
|-|-|-|
| Cifras inventadas | Dígitos de la respuesta ⊄ dígitos de evidencias ∪ pregunta ∪ turn_state (años y fechas excluidos) | Observar: log. Bloquear: sustituir por frase fija |
| Fuga del prompt | n-gramas de 10 palabras compartidos con los prompts del sistema por encima de un umbral | Igual |
| Internos | Nombres de herramientas, `chunk_id`, JSON crudo en la respuesta | Igual |
| Desvío de estación o fecha | Solo tiene sentido con la herramienta SQL (futuro) | Fuera de este plan |

Empiezan en modo `observar` (variable `COMPROBACIONES_MODO`). Se pasa a `bloquear` una regla
solo cuando los logs muestren que acierta y no daña respuestas correctas.

### 2.5 Memoria y estado tipado

- Almacén de chat de LlamaIndex: `SimpleChatStore` (memoria/JSON) en las primeras fases;
  `PostgresChatStore` sobre la base del proyecto en la fase 5.
- Se guardan solo la pregunta del usuario y la respuesta final. Ni planes ni mensajes `tool`.
- `turn_state` por turno, escrito por el código, como mensaje `system` con marca y versión. Los
  lectores del historial filtran a `user`/`assistant`, así que es invisible para el modelo y el usuario.
- Campos iniciales: `intencion`, `tema`, `citadas` (chunk_ids), `fuentes` (títulos), `cifras`
  (dígitos que aparecieron en la respuesta), `contaminantes` (referente para la futura tool SQL).
- Ventana: los últimos turnos que entran en un presupuesto de tokens; mínimo una pareja.
- Un turno a la vez por sesión: `asyncio.Lock` por `session_id` en proceso.

### 2.6 Modelos

| Entorno | Cliente LlamaIndex | Modelo |
|-|-|-|
| Producción (EC2) | `BedrockConverse` (región `eu-west-1`, credenciales por rol de instancia) | A comparar: Ministral 14B 3.0 (`mistral.ministral-3-14b-instruct`) y gpt-oss-120b (`openai.gpt-oss-120b-1:0`). Decisión del 2026-10-04 |
| Desarrollo / CI | `OpenAILike` (Mistral API, Groq, Ollama) y LLM falso con guion | según `.env` |

Una sola fábrica `llm/cliente.py` elige por `LLM_PROVEEDOR=bedrock|openai_compatible`. El resto del
código ve la misma interfaz. El clasificador usa temperatura 0; la síntesis 0,2.

## 3. Contratos

**`POST /responder`** `{pregunta, session_id?}` → `{respuesta, fuentes[{tipo, referencia}], advertencia, session_id}`.
Compatible con el contrato actual de `ApiUsuario` (campos nuevos opcionales).

**`POST /responder/stream`** (SSE). Eventos:

| Evento | Contenido |
|-|-|
| `status` | `{fase: clasificando|buscando|redactando|validando, herramienta?}` cada 0,7 s mientras no hay tokens |
| `token` | `{texto}` fragmento de la respuesta libre |
| `passthrough` | `{texto, fuentes, advertencia}` respuesta entregada por el código, íntegra |
| `error` | `{detalle}` |
| `done` | `{session_id}` |

**Cambios en `ApiUsuario`** (fase 4): `PreguntaChat.session_id` opcional, `RespuestaChat.session_id`,
y `POST /chat/stream` que reenvía el SSE. Nueva variable `AGENTE_URL`: si está definida, `/chat` va al
agente; si no, sigue yendo a `ORCHESTRATOR_URL`. Así los dos servicios conviven y se elige por
configuración, sin tocar código.

## 4. Fases de implementación

Cada fase cabe en media ventana de contexto, termina con tests en verde y con su anotación en
`docs/notas_memoria.md` (sin citar este plan).

### Fase 1. Esqueleto del servicio y bucle mínimo

- Crear `Agente/` con la estructura de §1, `settings`, `/salud`, Dockerfile, requirements fijados.
- Fábrica de LLM con `OpenAILike` y `BedrockConverse` (este último sin probar aún contra AWS).
- LLM falso con guion para tests (hereda de `FunctionCallingLLM`, registra las llamadas).
- Contrato de herramienta y `buscar_evidencias` como cliente HTTP de `rag.api` (`GET /rag/herramienta`
  cacheado, `POST /rag/evidencias`). Si el RAG no responde, la herramienta no se ofrece esa vuelta.
- Bucle a mano (§2.2) con síntesis libre sin herramientas. `POST /responder` sin sesión.
- Tests: bucle con 0, 1 y 2 llamadas a herramienta; límite de vueltas; RAG caído; contrato HTTP.
- Job de CI `Tests del agente`.
- Memoria: fila en §2 (nuevo servicio `Agente`, LlamaIndex solo como cliente y almacén; descartado
  un framework de agentes completo, con el motivo), §7.3 reescrito, bitácora.

### Fase 2. Ruta documental determinista

- Síntesis JSON con `ESQUEMA_SALIDA`, validación con `POST /rag/validar`, una reparación, passthrough.
- Intervención: `sin_evidencia` cierra el turno con frase fija sin llamar al modelo.
- Renumeración de evidencias si hubo varias llamadas.
- `fuentes` solo con documentos citados; `advertencia` sanitaria solo si hay afirmaciones documentales.
- Tests: JSON válido, inválido + reparación, doble fallo, `sin_evidencia`.

### Fase 3. Clasificador y decisión en código

- `intencion.py` (§2.1): prompt, parser, valor seguro `DESCONOCIDA`, tiempo límite propio.
- Tabla de decisión en código: herramientas permitidas/obligadas y frases fijas por intención.
- Reparación por llamada propia: si `DOCUMENTAL` y el modelo no pidió la herramienta, la llama el código.
- Prompt de síntesis libre para `CHARLA` (qué sabe hacer el asistente, límites).
- Tests: cada intención con el LLM falso; clasificador caído; herramienta vetada pedida por el modelo.
- Guion de 20 preguntas de `Preguntas.txt` etiquetadas a mano para medir el clasificador con un
  proveedor real (script fuera de CI).

### Fase 4. Streaming y sesión

- `POST /responder/stream` con cola, heartbeat `status`, eventos de §3. `/responder` es el mismo
  generador drenado a una cadena, para que ambos caminos no diverjan.
- Bloqueo por sesión. `session_id` generado si no llega.
- `ApiUsuario`: `session_id` en el contrato y `POST /chat/stream` como proxy SSE. Tests.

### Fase 5. Memoria y estado tipado

- `SimpleChatStore` → `PostgresChatStore` (tabla propia en la base del proyecto; la crea la librería).
- Ventana por presupuesto de tokens. `turn_state` (§2.5) escrito tras cada turno.
- Fase `REPETIR`: la respuesta sale de `turn_state.cifras` y `fuentes`, sin modelo.
- El clasificador recibe la pregunta anterior como contexto (las preguntas de seguimiento no se
  clasifican bien solas).
- Tests sobre el almacén: lo guardado tras el turno, no solo la respuesta.

### Fase 6. Comprobaciones posteriores y observabilidad

- `comprobaciones.py` (§2.4) en modo `observar`. Log JSON por turno: intención, herramientas,
  vueltas, latencias por fase, comprobaciones disparadas, tokens.
- Script de evaluación con las 20 preguntas etiquetadas: intención correcta, herramienta usada,
  citas válidas, latencia. Se guarda el resultado con fecha para comparar cambios.
- Pasar a `bloquear` solo las reglas con evidencia. Anotar cifras en la memoria.

### Fase 7. Bedrock y despliegue

- La persona habilita el acceso al modelo en Bedrock y crea el permiso IAM
  (`bedrock:InvokeModel`, `bedrock:InvokeModelWithResponseStream`) para el rol de la EC2 y para su
  perfil SSO local. El agente no toca AWS; solo documenta los comandos.
- Probar `BedrockConverse` con tool use y streaming. Si Mistral no está en `eu-west-1`: perfil de
  inferencia europeo o Mistral API por `OpenAILike` como alternativa documentada.
- `docker-compose`: servicio `agente` (puerto 8200) junto a `orquestador` (8100), ambos en el perfil
  `api`. `ApiUsuario` elige con `AGENTE_URL`. Opción `agente` en el desplegable del workflow `Desplegar`.
- `LLMOrchestrator` sigue tal cual: sin dependencias compartidas, sin imports cruzados, su propio job
  de CI. El agente no importa nada de él ni al revés. Fila en §2 (convivencia y motivo).

## 5. Riesgos

| Riesgo | Mitigación |
|-|-|
| Mistral no disponible o sin tool use en Bedrock `eu-west-1` | Fábrica con dos proveedores desde la fase 1; decidir en la fase 7 con datos |
| Modelos pequeños devuelven JSON inválido en la ruta documental | Una reparación con el mensaje del RAG; luego insuficiencia honesta. Medir en la fase 6 |
| Comprobaciones que dañan respuestas correctas | Modo observación primero; bloquear solo con evidencia |
| Latencia: clasificador + bucle + síntesis + validación | Clasificador corto a temperatura 0; límite de 3 vueltas; heartbeat para el cliente |
| Límites de peticiones de los proveedores gratuitos en desarrollo | Reintentos con backoff del propio cliente; tests sin red |
| Sesiones rompen la decisión «sin estado» de 2026-09-27 | Sin usuarios ni datos personales: solo `session_id` opaco. Anotar el cambio en §2 |
| Dependencias de LlamaIndex pesadas o inestables | Solo `llama-index-core` + un paquete de LLM + el almacén; versiones fijadas |

## 6. Validación

- Unitarios por fase con el LLM falso (sin red, sin torch, sin base externa), en CI.
- Fase 3 y 6: guion de preguntas reales etiquetadas, ejecutado contra un proveedor real, con
  resultado fechado.
- Fase 2: prueba manual de punta a punta contra `rag.api` real (cita válida, reparación, insuficiencia).
- Fase 7: prueba de humo en la EC2 con Bedrock y el RAG en contenedores.

## 7. Decisiones tomadas (2026-10-04)

| Pregunta | Decisión |
|-|-|
| Nombre del servicio | `Agente` (carpeta `Agente/`, paquete `agente`, imagen `jupiter-agente`) |
| Acceso a Bedrock | Hay acceso con la cuenta del máster. Se compararán Ministral 14B 3.0 y gpt-oss-120b en `eu-west-1` (ver `decisiones_agente.md` §2) |
| Sesiones | Se acepta `session_id` (anónimo, opaco). Anotar en §2 de la memoria el cambio frente a «sin estado» |
| Streaming de la ruta documental | Aceptable entregar el texto al final, con eventos `status` mientras tanto |
| `LLMOrchestrator` | Convive. Debe ser totalmente independiente del agente (§1 y fase 7) |

Pendiente: proveedor para desarrollo mientras no se prueba Bedrock (Mistral API, Groq u Ollama). Si no
se decide, la fase 1 arranca con el LLM falso y `OpenAILike` configurado por `.env`.
