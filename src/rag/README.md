# RAG documental: herramienta de evidencias con citas verificables

Este paquete (`src/rag`, importable como `rag`) convierte el corpus de [`data/rag/`](../../data/rag/)
en un índice vectorial y lo expone como una **herramienta (*function tool*) para un LLM que vive en
otro servicio** (la API de chat). El RAG **no llama a ningún modelo de lenguaje**: recupera
fragmentos numerados `D1..Dn`, deja que el modelo redacte citándolos y después **valida las citas y
construye la bibliografía desde el corpus**, nunca desde lo que diga el modelo.

Estado: fases A (índice fiable) y B (evidencias, validación y servicio HTTP) implementadas y
probadas. Pendiente la fase C (evaluación reproducible y calibración del umbral).

## Principios

- **El corpus vive en git.** `data/rag/*.md` es la única fuente de verdad. Añadir o corregir
  conocimiento es editar un fichero y hacer commit.
- **El índice es un artefacto derivado.** `data/chroma/` se reconstruye entero con un comando, no se
  versiona y guarda el commit del corpus que lo generó.
- **El LLM vive fuera.** Este servicio devuelve evidencias y valida la salida del modelo. La
  generación, el bucle de reparación y el historial son de la API de chat.
- **La validación de citas la hace el backend.** El modelo elige IDs `D1..Dn`; aquí se comprueba que
  existen, y título, sección y fuentes se resuelven a partir del `chunk_id` leyendo el corpus.
- **Un motor, un modelo de embeddings.** ChromaDB embebido y `intfloat/multilingual-e5-base`, sin
  interfaces con una sola implementación.

## Arquitectura

```
data/rag/*.md ──▶ rag.corpus ──▶ rag.embeddings ──▶ ChromaDB (data/chroma/, colección `corpus_rag`)
 (revisado: true)  (fragmentos)    (vectores e5)              │
                                                              ▼
                            rag.buscar · buscar(consulta, k, tema) → chunk_id, texto, metadatos, distancia
                                                              │
                                                              ▼
              rag.evidencias · umbral → D1..Dn + bibliografía + avisos  ·  validar salida del modelo → texto con [Dn]
                                                              │
                                                              ▼
              rag.api (FastAPI, :8010) · POST /rag/evidencias · POST /rag/validar · GET /rag/herramienta · GET /salud
                                                              ▲  function tool
                                                      LLM de la API de chat (otro servicio)
```

| Módulo | Qué hace |
|-|-|
| [`corpus.py`](corpus.py) | Lee y valida el frontmatter, trocea por `##`, aplica la guardia de tokens y genera `chunk_id` estables. Lógica pura: sin torch ni ChromaDB. |
| [`embeddings.py`](embeddings.py) | Carga el modelo e5-base (512 tokens), cuenta tokens con el tokenizer real, añade los prefijos `query:`/`passage:` y da acceso a la colección Chroma (métrica coseno). |
| [`indexar.py`](indexar.py) | `python -m rag.indexar`: reconstruye el índice completo. Calcula los embeddings **antes** de borrar la colección anterior y guarda modelo, commit y fecha como metadatos. |
| [`buscar.py`](buscar.py) | `buscar(consulta, k, tema)`: los `k` fragmentos más cercanos con `chunk_id`, texto, metadatos y distancia. Rechaza índice ausente o construido con otro modelo. |
| [`evidencias.py`](evidencias.py) | Paso 1: `recuperar` (umbral, numeración `D1..Dn`, bibliografía, avisos). Paso 3: `resolver_evidencias`, `validar_salida`, `validar_y_renderizar`. Sin LLM. |
| [`api.py`](api.py) | Servicio FastAPI con los cuatro endpoints y la traducción de errores tipados a códigos HTTP. |
| [`errores.py`](errores.py) | `CorpusInvalido`, `IndiceNoPreparado`, `ModeloNoCoincide`, `ConsultaInvalida`, `EvidenciaDesconocida`. |

## Instalación

Desde la raíz del repositorio, con el entorno virtual activado. No necesita PostgreSQL ni Docker.

```bash
pip install torch --index-url https://download.pytorch.org/whl/cpu   # opcional: rueda de CPU, más ligera
pip install -r requirements-rag.txt   # versiones fijadas; instala también el paquete `rag` (-e .)
```

La primera ejecución descarga el modelo `intfloat/multilingual-e5-base` (~1,1 GB, se cachea en
`~/.cache/huggingface`). Copia `.env.example` a `.env` si quieres cambiar el umbral o el puerto.

| Variable | Por defecto | Para qué |
|-|-|-|
| `RAG_MODELO_EMBEDDINGS` | `intfloat/multilingual-e5-base` | Modelo de embeddings. Cambiarlo obliga a reindexar. |
| `RAG_RUTA_CHROMA` | `data/chroma` | Directorio del índice. |
| `RAG_UMBRAL_DISTANCIA` | `0.22` | Distancia coseno máxima para que un fragmento cuente como evidencia. Provisional. |
| `RAG_API_PUERTO` | `8010` | Puerto del servicio HTTP. |

## Indexar el corpus

```bash
python -m rag.corpus     # resumen del corpus sin cargar el modelo (documentos, secciones, revisados)
python -m rag.indexar    # reconstruye data/chroma/ desde data/rag/*.md (≈1,5 min en CPU)
```

Salida esperada: número de documentos indexados, fragmentos, tokens máximos por fragmento y commit
del corpus. Solo se indexan los documentos con `revisado: true`. Si el cálculo de embeddings falla,
el índice anterior queda intacto.

## Preguntar por consola

```bash
python -m rag.buscar "¿qué son los bloques del día?"                       # búsqueda cruda
python -m rag.buscar --k 3 --tema salud --texto "¿puedo correr si soy asmático?"
python -m rag.evidencias "¿qué son los bloques del día?"                   # paso 1: D1..Dn bajo el umbral
python -m rag.evidencias --json "¿puedo correr si soy asmático?"           # misma respuesta que POST /rag/evidencias
```

## Arrancar el servicio

```bash
python -m rag.api                # uvicorn en 0.0.0.0:8010; documentación interactiva en /docs
uvicorn rag.api:app --reload     # desarrollo
curl localhost:8010/salud
curl -X POST localhost:8010/rag/evidencias -H 'content-type: application/json' \
     -d '{"pregunta": "¿qué son los bloques del día?"}'
```

El modelo de embeddings se carga en la primera petición a `/rag/evidencias`, no al arrancar; esa
primera llamada puede tardar unos minutos. El servicio no lleva autenticación ni CORS: se asume red
interna entre la API de chat y este servicio.

## Contrato para la API de chat

| Método y ruta | Entrada | Salida |
|-|-|-|
| `GET /salud` | | Estado del índice (modelo, número de fragmentos, commit, fecha) y umbral vigente. |
| `GET /rag/herramienta` | | `herramienta` (definición de la *function tool* en formato OpenAI) y `esquema_salida` (JSON Schema). |
| `POST /rag/evidencias` | `{"pregunta": str, "k"?: 1..20, "tema"?: "salud" \| "normativa" \| "proyecto"}` | Evidencias `D1..Dn`, bibliografía, avisos y `para_el_modelo`. |
| `POST /rag/validar` | `{"pregunta": str, "evidencias": [{"id": "D1", "chunk_id": "..."}], "salida": <JSON del modelo>}` | Validación y texto final, o errores y mensaje de reparación. |

### Flujo en tres pasos

1. **Recuperar.** El modelo pide la tool `buscar_evidencias(pregunta, tema?)`. La API de chat hace
   `POST /rag/evidencias` y entrega al modelo el campo `para_el_modelo`: pregunta, `estado`
   (`con_evidencias` o `sin_evidencia`), evidencias `{id, titulo, seccion, texto}` y avisos. Si el
   estado es `sin_evidencia`, no hay nada que citar y la API de chat puede responder insuficiencia
   sin llamar al modelo.
2. **Redactar.** El modelo devuelve un JSON conforme a `esquema_salida`, que sirve tal cual como
   `response_format` del proveedor:

   ```json
   {
     "estado": "respondida | parcial | sin_evidencia",
     "afirmaciones": [{"texto": "...", "evidencias": ["D1", "D3"]}],
     "limitaciones": ["..."]
   }
   ```

3. **Validar y renderizar.** La API de chat hace `POST /rag/validar` con esa salida y los pares
   `{id, chunk_id}` del paso 1. La respuesta es 200 siempre que la petición sea válida:
   - `valida: true`: `texto` trae la respuesta final (afirmaciones con `[Dn]`, limitaciones, aviso
     sanitario, «Evidencias citadas» y «Fuentes de los documentos citados» con las URLs del
     frontmatter). También vienen `afirmaciones`, `limitaciones`, `evidencias` (con `citada`) y
     `bibliografia` por separado.
   - `valida: false`: `errores` describe qué incumple la salida y `mensaje_reparacion` es un texto
     listo para reenviar al modelo como turno de usuario. El servicio no reintenta: la API de chat
     decide cuántas reparaciones permite (el plan propone una).

El prompt de sistema propuesto para el modelo está en
[`docs/rag/prompt_respuesta_fundamentada_v1.txt`](../../docs/rag/prompt_respuesta_fundamentada_v1.txt).
Contiene las reglas de fundamentación e interpretación del dominio; el marcador `{esquema}` se
sustituye por `esquema_salida`.

### Qué valida `POST /rag/validar`

- Claves exactamente `estado`, `afirmaciones` y `limitaciones`; `estado` dentro de la enumeración.
- Como máximo 8 afirmaciones, cada una con `texto` no vacío de hasta 700 caracteres y una lista no
  vacía de IDs **de esta petición**. Un ID inventado se rechaza con la lista de IDs válidos.
- Como máximo 6 limitaciones de texto.
- `respondida` y `parcial` exigen al menos una afirmación; `sin_evidencia` exige la lista vacía.
- Cada `chunk_id` debe existir en el corpus (documento y sección). Si no, 422 `EvidenciaDesconocida`.

### Garantías

- Título, sección, tema y fuentes se resuelven **desde el corpus a partir del `chunk_id`**, nunca
  desde lo que envíe el cliente ni el modelo.
- Las URLs de la bibliografía salen solo del frontmatter de los documentos citados. Una URL que
  escriba el modelo dentro de una afirmación no llega a la bibliografía.
- La bibliografía final solo incluye los documentos realmente citados en las afirmaciones.

### Avisos fijos

Viven en código, no en el corpus, y se devuelven también en `avisos.textos` del paso 1 para que la
API de chat pueda mostrarlos aunque no llegue al paso 3.

| Aviso | Cuándo se añade |
|-|-|
| Aviso sanitario | La pregunta contiene términos de salud (asma, alergia, niños, correr, mascarilla...) o alguna evidencia tiene `tema: salud`. En el paso 3 solo cuentan las evidencias citadas. |
| Limitación de actualidad | La pregunta habla de «hoy», «ahora», «actualmente», «está activado», «hay episodio»... El asistente aún no consulta mediciones en tiempo real. |
| Limitación sin evidencia | Ningún fragmento queda bajo el umbral, o el modelo devuelve `sin_evidencia` sin limitaciones. |

### Errores HTTP

Cuerpo: `{"error": "<Clase>", "detalle": "..."}`.

| Código | Clase | Causa |
|-|-|-|
| 422 | `ConsultaInvalida` | Pregunta vacía, `k` fuera de 1..20, tema desconocido o consulta más larga que el presupuesto de tokens. |
| 422 | `EvidenciaDesconocida` | Un `chunk_id` de `/rag/validar` no existe en el corpus o tiene formato inválido. |
| 503 | `IndiceNoPreparado` | No existe `data/chroma/`. Ejecutar `python -m rag.indexar`. |
| 503 | `ModeloNoCoincide` | El índice se construyó con otro modelo de embeddings. Reindexar o ajustar `RAG_MODELO_EMBEDDINGS`. |
| 500 | `CorpusInvalido` | Un documento del corpus no cumple el formato. |

## Formato de los documentos del corpus

```yaml
---
titulo: Ozono troposférico (O3) y salud
tema: salud                 # salud | normativa | proyecto
contaminantes: [O3]         # lista; 'NO' siempre entre comillas (YAML lo lee como false)
revisado: true              # false = se lee y valida, pero no se indexa
fecha_revision: 2026-09-23  # obligatoria si revisado
fuentes:                    # obligatoria y no vacía si revisado
  - titulo: WHO global air quality guidelines 2021
    organismo: OMS
    url: https://www.who.int/publications/i/item/9789240034228
---
# Título
Introducción opcional (va al fragmento «Introducción»).
## Sección
...
```

Reglas que aplica `corpus.py`:

- El cuerpo se parte por encabezados `##`. Cada sección es un fragmento salvo que supere el
  presupuesto de tokens (máximo del modelo menos un margen); entonces se parte por párrafos
  conservando título y sección en cada trozo.
- Texto indexado de cada fragmento: `"{titulo} — {seccion}\n{contenido}"`.
- `chunk_id = <archivo sin extensión>:<slug de la sección>:<ordinal>`, por ejemplo
  `ozono_salud:efectos-en-la-salud:0`. No depende de la posición de la sección: reordenar no cambia
  los IDs. Dos secciones con el mismo slug en un documento es un error.
- Las secciones `Fuentes`, `Cómo lo usa el asistente` e `Indicación para el asistente` no se
  indexan: las fuentes viven en el frontmatter y las instrucciones en el prompt.
- Cada documento es una síntesis del proyecto. La respuesta enlaza a la síntesis y lista sus
  fuentes primarias; no atribuye frases literales al organismo original.

## Añadir un documento nuevo

1. Escribir el contenido en Markdown en `data/rag/` con el frontmatter anterior. Revisar fuentes y
   cifras; si el original es PDF o web, la conversión es un paso previo con revisión humana.
2. Poner `revisado: true` y la `fecha_revision`. Un borrador puede quedarse en git con
   `revisado: false` sin entrar en el índice.
3. `python -m rag.corpus` para comprobar que valida, y commit.
4. `python -m rag.indexar`.
5. Comprobar con `python -m rag.evidencias "<pregunta que debería responder>"` que aparece el
   `chunk_id` esperado bajo el umbral.

## Pruebas

```bash
pytest tests/test_corpus.py tests/test_buscar.py tests/test_indexar.py tests/test_evidencias.py tests/test_api.py -q
RUN_RAG_TESTS=1 pytest tests/test_indexar.py -q      # añade una prueba con el modelo real (≈1 min)
```

Ninguna prueba carga el modelo ni Chroma salvo la marcada con `RUN_RAG_TESTS`. `test_buscar`,
`test_indexar` y `test_api` se saltan si faltan chromadb o fastapi, así que el job unitario de CI
(que solo instala PyYAML) sigue pasando. Cubren, entre otros casos: una sección larga se subdivide
sin superar el presupuesto; los IDs no cambian al reordenar secciones; un documento no revisado no
se indexa; un fallo de embeddings no destruye el índice; un ID inventado, una afirmación sin
evidencias o una salida que no es un objeto se rechazan con reparación; ninguna URL escrita por el
modelo llega a la bibliografía; los avisos sanitario y de actualidad se añaden cuando toca.

## Umbral de evidencia (provisional)

Con e5-base, `k=4` y el corpus actual (11 documentos, 50 fragmentos), las preguntas documentales
dan una distancia de 0,11 a 0,20 en su mejor fragmento y las ajenas al corpus («¿cuál es la capital
de Francia?») 0,24 a 0,25. El umbral 0,22 separa ambos grupos con un margen estrecho. Bajo el umbral
entran también fragmentos poco pertinentes; no es un problema porque el modelo elige qué citar. Se
recalibrará en la fase C con 20 a 30 casos de evaluación.

## Decisiones y límites conocidos

- `/rag/validar` devuelve 200 con `valida: false` cuando la salida del modelo no cumple el contrato:
  es el caso normal que dispara una reparación, no un error del cliente.
- `/rag/validar` es sin estado: la API de chat reenvía los pares `{id, chunk_id}` del paso 1.
- Del `chunk_id` se comprueban documento y sección; el ordinal no se verifica porque exigiría
  trocear con el tokenizer del modelo.
- Las afirmaciones se muestran tal cual las escribe el modelo, con sus `[Dn]`. No se filtran
  cifras ni URLs dentro del texto: la garantía es que la bibliografía no sale del modelo.
- El aviso sanitario es deliberadamente generoso (regex sobre la pregunta y `tema: salud`).
- La definición de la tool usa el formato de *function calling* de OpenAI, que aceptan también
  Ollama, vLLM, Mistral y Qwen. Si la API de chat usa otro formato, es un diccionario que se adapta
  allí.
- Fuera de alcance por ahora: ingesta incremental, filtro por contaminante, reranking, historial de
  conversación, streaming y consulta de mediciones.

## Pendiente (fase C)

- `tests/evals/rag_casos.jsonl` con 20 a 30 casos (documentales, sin evidencia, adversarios) y un
  script de evaluación con recall@k, validez de citas y acierto de abstención.
- Recalibrar `RAG_UMBRAL_DISTANCIA` con esos casos; probar un criterio relativo al mejor fragmento
  además del absoluto.
- Conectar la API de chat con `GET /rag/herramienta`, `POST /rag/evidencias` y `POST /rag/validar`
  y revisar a mano las respuestas de aceptación.
