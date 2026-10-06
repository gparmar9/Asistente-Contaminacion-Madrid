# Agente

Servicio FastAPI con el agente conversacional del asistente. Un solo agente con un
**bucle de herramientas escrito a mano**; LlamaIndex aporta únicamente el cliente del
LLM (`OpenAILike` o `BedrockConverse`). El RAG se consume por HTTP, así que el servicio
es ligero (sin torch ni chromadb). Independiente de `LLMOrchestrator`: ni código ni
dependencias compartidas.

Decisiones de diseño, alternativas descartadas y estado por fases:
[`docs/agente/decisiones_agente.md`](../docs/agente/decisiones_agente.md).

## Endpoints

| Método | Ruta | Qué devuelve |
|-|-|-|
| GET | `/salud` | Estado del servicio, proveedor y modelo configurados |
| POST | `/responder` | `{pregunta, session_id?}` → `{respuesta, fuentes[{tipo, referencia}], advertencia, traza_id, session_id}` (mismo contrato que reenvía `ApiUsuario` desde `/chat`; `traza_id` sirve para buscar el turno en las trazas) |
| POST | `/responder/stream` | La misma entrada; respuesta en Server-Sent Events (ver abajo). `ApiUsuario` la reenvía desde `/chat/stream` |

Documentación interactiva en `/docs`.

**Sesión.** `session_id` es opaco (1–64 caracteres `[A-Za-z0-9_-]`); si no llega, se genera.
Un turno a la vez por sesión: una segunda pregunta de la misma sesión espera a que termine la
primera (en el stream, recibiendo `status` con `en_espera`). El bloqueo vive en memoria del
proceso: con varios procesos del agente haría falta otra coordinación.

**Memoria de la conversación.** Dos turnos con el mismo `session_id` comparten contexto, para
entender seguimientos como «¿Y en niños?». El historial sirve para interpretar la pregunta; las
respuestas documentales siguen citando solo evidencias del turno.

- Se guardan solo los turnos completados (no los cancelados ni los que fallan por el LLM). Las
  frases fijas también se guardan.
- En la ruta documental se recuerda el texto de las afirmaciones, sin `[Dn]`, aviso ni bibliografía.
- El historial llega al clasificador (2 últimos turnos), al bucle, a las síntesis y a la búsqueda
  que lanza el código (2 preguntas previas + la actual). `/rag/validar` ve solo la pregunta actual.
- **Límites:** vive en el proceso, así que se pierde al reiniciar o desplegar, y exige arrancar
  **un solo proceso** de uvicorn (sin `--workers`), igual que el bloqueo por sesión.

| Variable | Por defecto | Qué hace |
|-|-|-|
| `MEMORIA_PRESUPUESTO_TOKENS` | 1500 | Tope del historial que se manda (turnos completos, estimado a 4 caracteres por token) |
| `MEMORIA_MAX_TURNOS` | 20 | Turnos guardados por sesión |
| `MEMORIA_TTL_H` | 168 | Horas sin actividad tras las que se olvida una sesión |

### Streaming (`/responder/stream`)

Cada evento es `event: <tipo>\ndata: <json>\n\n`, con `Cache-Control: no-cache` y
`X-Accel-Buffering: no`.

| Evento | `data` | Cuándo |
|-|-|-|
| `status` | `{fase, herramienta?}`: `en_espera`, `clasificando`, `buscando`, `redactando`, `validando` | Al cambiar de fase, y repetido cada `STREAM_HEARTBEAT_S` si no sale nada |
| `token` | `{texto}` | Solo respuestas definitivas: la llamada sin herramientas ofrecidas (charla) y la síntesis forzada |
| `passthrough` | `{texto, fuentes, advertencia, traza_id}` | El resto, íntegro al final: ruta documental (el JSON se valida antes de mostrarse), frases fijas, `sin_evidencia` |
| `error` | `{detalle}` | Fallo con el stream ya abierto; cierra sin `done`. Antes de abrirlo, código HTTP (503) |
| `done` | `{session_id, traza_id}` | Turno completado |

Si el cliente corta la conexión, el turno se cancela (sin más llamadas al LLM ni al RAG, y el
span del turno lleva `agente.cancelado=true`). Eso no garantiza que Bedrock deje de procesar y
cobrar una inferencia ya iniciada.

```bash
curl -N -X POST localhost:8200/responder/stream -H 'Content-Type: application/json' \
  -d '{"pregunta": "Hola, ¿qué sabes hacer?", "session_id": "prueba-1"}'
```

## Cómo responde un turno

0. **Clasificar y decidir.** Una llamada corta al LLM (temperatura 0) da la intención y el tema.
   Si falla o tarda más de `CLASIFICADOR_TIMEOUT_S`, el turno sigue como `DESCONOCIDA`
   (todas las herramientas, ninguna obligada). Después decide el código:

   | Intención | Qué hace el agente |
   |-|-|
   | `DOCUMENTAL` | Solo `buscar_evidencias`, obligatoria: si el modelo no la pide, la lanza el código. Con el RAG caído, frase fija |
   | `DATOS`, `PREDICCION`, `FUERA_DE_ALCANCE` | Frase fija, sin más llamadas al modelo |
   | `CHARLA` | Sin herramientas, prompt corto |
   | `DESCONOCIDA` | Todas las herramientas, ninguna obligada |

1. Se piden al RAG las definiciones de las herramientas (`GET /rag/herramienta`, cacheado).
   Si el RAG no responde, la herramienta no se ofrece en ese turno.
2. El modelo recibe la pregunta y las herramientas disponibles. Si pide `buscar_evidencias`,
   el código llama a `POST /rag/evidencias` y devuelve el resultado como mensaje `tool`.
   Si la búsqueda sale vacía (`sin_evidencia`) y no hay evidencias previas, el turno se cierra
   con una frase fija sin volver a llamar al modelo.
3. Máximo `MAX_VUELTAS` vueltas. Al terminar:
   - **sin evidencias**: el texto del modelo es la respuesta (o síntesis forzada sin herramientas);
   - **con evidencias** (ruta documental): una llamada sin herramientas devuelve el JSON
     `{estado, afirmaciones, limitaciones}`, `POST /rag/validar` lo comprueba (una reparación como
     máximo) y se entrega tal cual el texto con citas que renderiza el RAG. `fuentes` lista solo
     los documentos citados y `advertencia` lleva el aviso sanitario si el RAG lo activa.

Las herramientas nunca lanzan: sus errores vuelven al modelo como `{"error": ...}`.

## Puesta en marcha

```bash
pip install -r requirements.txt
cp .env.example .env       # y rellenar el proveedor del LLM y RAG_URL
uvicorn agente.main:app --reload --app-dir src --port 8200 --env-file .env
```

## Observabilidad: trazas de cada turno

Cada turno deja un árbol de spans OpenTelemetry con atributos OpenInference
(`src/agente/observabilidad/`). Hay dos destinos y se pueden usar a la vez:

- **Phoenix**: la cascada interactiva del turno (UI web).
- **JSONL**: un fichero por arranque en `TRAZAS_RUTA` (`trazas_<etiqueta>_<fecha>.jsonl`, un span por
  línea), que lee el informe agregado. La carpeta `trazas/` no se versiona.

```text
turno (agent)                       pregunta -> respuesta; ruta, intención, vueltas, reparaciones...
    clasificar (chain)              salida cruda del clasificador; si fue válida
        llm
    bucle (chain)                   una llamada llm por vuelta y cada herramienta pedida
        llm                         mensajes, herramientas ofrecidas, respuesta, tokens
        buscar_evidencias (tool)    argumentos y lo que vio el modelo (evidencias ya renumeradas)
    busqueda_forzada (chain)        solo si el modelo no buscó y la intención lo exige
    sintesis_documental (chain)     llm, validar (y llm, validar otra vez si hubo reparación)
    sintesis_forzada (chain)        solo si se agotaron las vueltas
```

Las decisiones del código (frase fija, `sin_evidencia`, límite de vueltas, herramienta no
disponible) quedan como eventos `decision` en el span en curso. Los tokens que no envía el
proveedor no aparecen (nunca valen cero).

| Variable | Por defecto | Para qué |
|-|-|-|
| `PHOENIX_ENDPOINT` | vacía (sin Phoenix) | `http://localhost:6006/v1/traces` |
| `PHOENIX_PROYECTO` | `agente` | Proyecto en Phoenix; un lote de evaluación puede usar su propio proyecto |
| `TRAZAS_RUTA` | vacía (sin fichero) | Carpeta de los JSONL, p. ej. `trazas` |
| `TRAZA_GUARDAR_TEXTO` | `true` | `false` = prompts, preguntas y respuestas quedan como `__REDACTED__` |
| `TRAZA_RAZONAMIENTO` | `false` | Guarda el razonamiento del modelo si el proveedor lo devuelve |
| `TRAZA_ETIQUETA` | `agente` | Nombre del fichero JSONL y atributo `agente.etiqueta` del turno |

Phoenix en local (imagen fijada, datos en el volumen `phoenix_data`):

```bash
docker compose --profile observabilidad up -d phoenix     # desde la raíz del repo
# sin compose: docker run -p 127.0.0.1:6006:6006 -e PHOENIX_WORKING_DIR=/mnt/data \
#   -v phoenix_data:/mnt/data arizephoenix/phoenix:version-20.19.0
```

Con `PHOENIX_ENDPOINT=http://localhost:6006/v1/traces` en `.env`, abre `http://localhost:6006`,
elige el proyecto y entra en una traza: a la izquierda está el árbol de spans y a la derecha,
para cada span, la entrada, la salida, los atributos `agente.*` y los eventos. El `traza_id` de la
respuesta HTTP y la línea `turno traza=...` de los logs llevan al mismo turno. Si Phoenix no
responde, el SDK lo avisa en el log y el turno sigue igual: los spans se envían por lotes en
segundo plano.

Informe agregado desde los JSONL (solo biblioteca estándar): turnos por ruta e intención,
latencia por fase (mediana con n; p95 solo descriptivo), tokens y coste con precios fechados en
el script, síntesis válidas a la primera, reparaciones, búsquedas forzadas y decisiones. Un turno
al que le faltan tokens queda como incompleto y su coste no se suma.

```bash
.venv/bin/python evaluacion/informe_trazas.py trazas/*.jsonl                       # por lote (etiqueta)
.venv/bin/python evaluacion/informe_trazas.py trazas/*.jsonl --salida informe.md
.venv/bin/python evaluacion/informe_trazas.py trazas/x.jsonl --traza-id 5534d0f4  # un turno, como tabla
.venv/bin/python evaluacion/informe_trazas.py trazas/*.jsonl --salida informe.html  # informe visual
.venv/bin/python evaluacion/informe_trazas.py trazas/*.jsonl --salida datos.json    # solo los datos
```

La extensión de `--salida` elige el formato. El HTML es un único fichero que se abre en el
navegador sin red (plantilla en `evaluacion/plantilla_informe.html`, sin librerías externas):
filtros por lote, ruta e intención; cifras por lote (latencia mediana y p95, coste total, por
1.000 turnos y mediano, tokens); dispersión latencia–coste por turno; latencia por fase y por ruta
con cada span visible; coste medio por fase; tabla de turnos ordenable y cascada de cada turno con
su coste. Lleva la pregunta y la respuesta de cada turno (no los mensajes al LLM): tratarlo como
las trazas, fuera de git. El `.json` es el conjunto de datos que dibuja la página; una futura web
de análisis podría servirlo desde un endpoint y reutilizar la plantilla.

## Tests

Sin red, sin claves y sin servicio RAG (LLM falso con guion y transporte HTTP fingido):

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

## Medir el clasificador (fuera de CI)

Con un proveedor real en `.env`: 29 preguntas etiquetadas a mano en
`evaluacion/preguntas_clasificador.json`; algunas llevan `historial` (seguimientos) y el acierto
de los casos de la fase 5 sale aparte.

```bash
set -a && . ./.env && set +a
python evaluacion/evaluar_clasificador.py
```

## Evaluar turnos completos (fuera de CI)

12 casos en `evaluacion/casos_turno.json` (6 documentales, 2 de charla, 2 fuera de alcance y 2
sin evidencias), cada uno con lo esperado: intención, ruta, si se busca en el RAG y si cita.
Necesita `rag.api` levantado. Cada lote deja su JSONL en `trazas/` (etiqueta = modelo, también
proyecto de Phoenix si hay `PHOENIX_ENDPOINT`) y un markdown en `evaluacion/resultados/` con las
comprobaciones automáticas, las respuestas para revisarlas a mano (correcta: sí / no / parcial)
y el informe de trazas. Sin jueces LLM.

```bash
LLM_PROVEEDOR=bedrock LLM_MODELO=mistral.ministral-3-14b-instruct AWS_REGION=eu-west-1 \
  AWS_PROFILE=pontia RAG_URL=http://localhost:8010 .venv/bin/python evaluacion/evaluar_turnos.py
# --caso doc-01 --caso sin-02 para repetir solo algunos
# --casos evaluacion/casos_conversacion.json para las conversaciones
```

Conversaciones: `evaluacion/casos_conversacion.json` tiene 5 conversaciones de 2–3 turnos. Cada
turno pasa por la misma capa de sesión y memoria que el servicio, y en los seguimientos se
comprueba en las trazas que la búsqueda nombre el `referente` (por ejemplo, el NO2 en «¿Y a largo
plazo?»). El markdown añade la columna «Historial» (turnos previos enviados).

La carpeta `resultados/` no se versiona; los resultados revisados se añaden con `git add -f`.
