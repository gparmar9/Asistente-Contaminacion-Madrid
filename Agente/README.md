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
| POST | `/responder` | `{pregunta}` → `{respuesta, fuentes[{tipo, referencia}], advertencia}` (mismo contrato que reenvía `ApiUsuario` desde `/chat`) |

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
