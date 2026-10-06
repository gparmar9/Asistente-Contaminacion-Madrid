# ApiUsuario

Servicio FastAPI que expone los datos del proyecto al dashboard y a cualquier
otro cliente. Es **ligero a propósito**: lee PostgreSQL (`estaciones`,
`resumen_datos_ml`) y delega el chat en el servicio [`Agente`](../Agente/README.md)
o, si `AGENTE_URL` está vacía, en [`LLMOrchestrator`](../LLMOrchestrator/README.md).
No tiene estado: sin usuarios ni historial. El `session_id` del chat es opaco y
solo se reenvía.

## Endpoints

| Método | Ruta | Qué devuelve |
|---|---|---|
| GET | `/health` | Estado del servicio |
| GET | `/estaciones` | Catálogo de estaciones con distrito y `contaminantes_medidos` (los consultables en `/series`; `mide_no2` implica NO/NO2/NOx) |
| GET | `/estaciones/{codigo}/series` | Bloques horarios con estadísticos y anomalías (`resumen_datos_ml`). Parámetros: `contaminante` (obligatorio), `desde`, `hasta`, `bloque` |
| POST | `/chat` | Respuesta del asistente. Sin `AGENTE_URL` ni `ORCHESTRATOR_URL`, responde un stub con el mismo contrato |
| POST | `/chat/stream` | La misma respuesta como Server-Sent Events (ver abajo) |

Documentación interactiva en `/docs` (OpenAPI autogenerada).

## Puesta en marcha

```bash
pip install -r requirements.txt
cp .env.example .env       # y rellenar DATABASE_URL
uvicorn api_usuario.main:app --reload --app-dir src --env-file .env
```

## Tests

No necesitan base de datos (SQLite en memoria):

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

## Contrato del chat

`POST /chat` con `{"pregunta": "...", "session_id": "...?"}` →
`{"respuesta": "...", "fuentes": [{"tipo": "sql|documento", "referencia": "..."}], "advertencia": "...|null", "session_id": "...", "traza_id": "...|null"}`.

- `session_id` (opcional, 1–64 caracteres `[A-Za-z0-9_-]`): agrupa los turnos de una
  conversación. Si no llega, se genera uno; el cliente lo reenvía en la pregunta siguiente.
- `traza_id`: el turno en las trazas del agente. `null` con el orquestador o el stub.

A quién se pregunta:

| `AGENTE_URL` | `ORCHESTRATOR_URL` | Destino |
|-|-|-|
| definida | cualquiera | `POST {AGENTE_URL}/responder` con `{pregunta, session_id}` |
| vacía | definida | `POST {ORCHESTRATOR_URL}/responder` con `{pregunta}` |
| vacía | vacía | stub |

El orquestador **no conserva el contexto**: el `session_id` se devuelve para mantener el
contrato, pero cada pregunta se responde sola. Fallos del destino: 503 si no responde,
502 si la respuesta no cumple el contrato.

### `POST /chat/stream`

Misma entrada; respuesta `text/event-stream`. Cada evento es `event: <tipo>\ndata: <json>\n\n`:

| Evento | `data` | Cuándo |
|-|-|-|
| `status` | `{fase, herramienta?}`: `en_espera`, `clasificando`, `buscando`, `redactando`, `validando` | Al cambiar de fase y cada 0,7 s si no sale nada |
| `token` | `{texto}` | Fragmentos de las respuestas libres (charla) |
| `passthrough` | `{texto, fuentes, advertencia, traza_id}` | Respuesta completa entregada al final (ruta documental, frases fijas) |
| `error` | `{detalle}` | Fallo con el stream abierto; cierra sin `done` |
| `done` | `{session_id, traza_id}` | Turno completado |

- Con `AGENTE_URL`: reenvía byte a byte el SSE de `{AGENTE_URL}/responder/stream`. Si el
  agente no contesta 200, responde 503 sin abrir el stream. Si el cliente se va, cierra la
  conexión con el agente (que cancela el turno).
- Sin `AGENTE_URL`: llama al mismo camino que `/chat` y emite `passthrough` + `done`
  (`traza_id: null`).

```bash
curl -N -X POST localhost:8000/chat/stream -H 'Content-Type: application/json' \
  -d '{"pregunta": "Hola, ¿qué sabes hacer?"}'
```
