# LLMOrchestrator

El cerebro del asistente (Fase 3): un servicio FastAPI que recibe una pregunta
en español, ejecuta el **bucle del agente** sobre un LLM con *tool use* y
devuelve la respuesta con sus fuentes. Es el servicio al que `ApiUsuario`
reenvía su `/chat`.

## Cómo funciona

1. La pregunta entra por `POST /responder`.
2. El LLM (cualquier proveedor **OpenAI-compatible**: Mistral, Groq, Gemini,
   Ollama — se elige con `LLM_BASE_URL`/`LLM_API_KEY`/`LLM_MODEL`) decide en
   cada vuelta si llama a una tool o responde:
   - **`query_sql`** — 5 consultas predefinidas y parametrizadas sobre
     PostgreSQL (`ultimos_niveles`, `serie_bloques`, `anomalias`,
     `comparar_estaciones`, `info_estaciones`). El LLM **no** genera SQL libre.
   - **`buscar_evidencias`** — paso 1 del contrato del servicio de evidencias
     de la Fase 2 (`src/rag/README.md`): `POST {RAG_URL}/rag/evidencias`
     devuelve evidencias numeradas `D1..Dn` bajo el umbral del servicio. El RAG
     corre como **servicio aparte** (`python -m rag.api`, :8010): este
     orquestador es solo su cliente HTTP y no arrastra torch/chromadb. La
     definición de la tool también la publica el RAG (`GET /rag/herramienta`,
     única fuente de verdad del esquema): se pide una vez y se cachea; si el
     servicio no responde, la tool no se ofrece en esa vuelta (el agente sigue
     solo con `query_sql`) y el GET se reintenta en la siguiente.
3. Los resultados vuelven al historial y el bucle sigue (máx. `MAX_ITERACIONES`
   vueltas; después se fuerza un cierre sin tools).
4. **Ruta documental.** Si hubo evidencias, la respuesta final del modelo debe
   ser el JSON `{estado, afirmaciones[{texto, evidencias:[Dn]}], limitaciones}`.
   El agente la valida con `POST {RAG_URL}/rag/validar`: las citas se
   comprueban contra el corpus (un ID inventado se rechaza) y el texto final
   lleva `[Dn]`, avisos y la bibliografía del frontmatter. Si la salida no
   cumple el contrato se reenvía **una** reparación; si tampoco, se responde
   insuficiencia. Si el modelo declara `sin_evidencia` pero hay datos de
   `query_sql`, se cierra por la ruta de datos en texto plano.
5. La respuesta sale con `fuentes` (tabla SQL o títulos de los documentos
   **realmente citados**) y, si hay afirmaciones documentales, la
   `advertencia` de demo académica.

## Contrato

`POST /responder` con `{"pregunta": "..."}` →
`{"respuesta": "...", "fuentes": [{"tipo": "sql|documento", "referencia": "..."}], "advertencia": "...|null"}`

Errores: 503 si el proveedor del LLM falla (tras reintentos con backoff),
422 si la pregunta está vacía o supera 2000 caracteres.

## Puesta en marcha

```bash
pip install -r requirements.txt   # ligero: el RAG es otro servicio
cp .env.example .env              # rellenar DATABASE_URL, RAG_URL y las LLM_*
uvicorn llm_orchestrator.main:app --reload --app-dir src --env-file .env --port 8100
```

Necesita el **servicio RAG** accesible en `RAG_URL`. Desde la raíz del
repositorio, con las deps de `requirements-rag.txt` y el índice construido:

```bash
pip install -r requirements-rag.txt
python -m rag.indexar      # una vez por entorno (o tras cambiar el corpus)
python -m rag.api          # servicio de evidencias en :8010
```

La primera petición al RAG recién arrancado carga el modelo (~1,1 GB, se
cachea): conviene calentarlo o dejar margen en `RAG_TIMEOUT_S`. El umbral de
evidencia se configura en el servicio (`RAG_UMBRAL_DISTANCIA`, 0,1754).

## Tests

Sin red, sin torch y sin base de datos externa (LLM falso con guion + SQLite
en memoria + búsqueda documental fingida):

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```
