# ApiUsuario

Servicio FastAPI que expone los datos del proyecto al dashboard y a cualquier
otro cliente. Es **ligero a propósito**: lee PostgreSQL (`estaciones`,
`resumen_datos_ml`) y delega el chat en el servicio
[`LLMOrchestrator`](../LLMOrchestrator/README.md). No tiene estado: sin usuarios
ni historial de conversaciones.

## Endpoints

| Método | Ruta | Qué devuelve |
|---|---|---|
| GET | `/health` | Estado del servicio |
| GET | `/estaciones` | Catálogo de estaciones con distrito y `contaminantes_medidos` (los consultables en `/series`; `mide_no2` implica NO/NO2/NOx) |
| GET | `/estaciones/{codigo}/series` | Bloques horarios con estadísticos y anomalías (`resumen_datos_ml`). Parámetros: `contaminante` (obligatorio), `desde`, `hasta`, `bloque` |
| POST | `/chat` | Respuesta del asistente. Sin `ORCHESTRATOR_URL`, responde un stub con el mismo contrato |

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

## Contrato con LLMOrchestrator

`POST {ORCHESTRATOR_URL}/responder` con `{"pregunta": "..."}` →
`{"respuesta": "...", "fuentes": [{"tipo": "sql|documento", "referencia": "..."}], "advertencia": "...|null"}`.
`ApiUsuario` reenvía la respuesta tal cual, sin añadir nada.
