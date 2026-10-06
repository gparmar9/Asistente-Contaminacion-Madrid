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
| POST | `/responder` | `{pregunta}` → `{respuesta, fuentes[{tipo, referencia}], advertencia, traza_id}` (mismo contrato que reenvía `ApiUsuario` desde `/chat`; `traza_id` es opcional y sirve para buscar el turno en las trazas) |

Documentación interactiva en `/docs`.

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

## Tests

Sin red, sin claves y sin servicio RAG (LLM falso con guion y transporte HTTP fingido):

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

## Medir el clasificador (fuera de CI)

Con un proveedor real en `.env`: 20 preguntas etiquetadas a mano en `evaluacion/`.

```bash
set -a && . ./.env && set +a
python evaluacion/evaluar_clasificador.py
```
