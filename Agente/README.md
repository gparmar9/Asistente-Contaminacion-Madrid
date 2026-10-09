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
   | `DATOS` | Solo `consultar_datos`, obligatoria: si el modelo no la pide, la lanza el código. Sin base de datos, caída o con la consulta sin resolver, frase fija |
   | `PREDICCION`, `FUERA_DE_ALCANCE` | Frase fija, sin más llamadas al modelo |
   | `CHARLA` | Sin herramientas, prompt corto |
   | `DESCONOCIDA` | Todas las herramientas, ninguna obligada |

1. Se piden las definiciones de las herramientas: al RAG (`GET /rag/herramienta`, cacheado) y,
   la de datos, comprobando que la base responde (último día, cacheado una hora). La que no
   responde no se ofrece en ese turno. La de datos añade al prompt la fecha de hoy y el rango
   con mediciones.
2. El modelo recibe la pregunta y las herramientas disponibles. Si pide `buscar_evidencias`,
   el código llama a `POST /rag/evidencias` y devuelve el resultado como mensaje `tool`.
   Si la búsqueda sale vacía (`sin_evidencia`) y no hay evidencias previas, el turno se cierra
   con una frase fija sin volver a llamar al modelo.
3. Máximo `MAX_VUELTAS` vueltas. Al terminar:
   - **sin evidencias ni datos**: el texto del modelo es la respuesta (o síntesis forzada sin herramientas);
   - **con resultados de `consultar_datos`** (ruta de datos): una llamada sin herramientas recibe
     la pregunta, las fechas y las filas y escribe la respuesta (prosa; tabla si hay más de 3 filas;
     periodo explícito; NO2 y PM10 si la pregunta no nombra contaminante). Sale como tokens.
     `fuentes` lleva una entrada `sql` por consulta, con su descripción (nunca el SQL);
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

## Datos de mediciones (herramienta SQL `consultar_datos`)

El agente lee PostgreSQL directamente (`DATABASE_URL`), solo a través de la vista
`mediciones_bloques` y con el rol de solo lectura `agente_lectura`, también en local. La única
puerta es `src/agente/datos/mediciones.py`: sesión de solo lectura, tiempo límite por sentencia
(`DB_TIMEOUT_S`) y tope de filas (`DB_MAX_FILAS`). Sin `DATABASE_URL`, las preguntas de datos
reciben una frase fija.

`consultar_datos(pregunta)` recibe la pregunta en lenguaje natural y dentro:

1. **Redactor** (`business/redactor_sql.py`): una llamada a temperatura 0 al modelo de
   `LLM_MODELO_SQL` (vacío = el del agente) con el esquema de la vista, las estaciones, las fechas
   y las reglas (media ponderada, NO2 y PM10 por defecto, `ILIKE`, definiciones de «esta semana»,
   «hora punta»...). Devuelve solo el SQL.
2. **Validador** (`business/validar_sql.py`, `sqlglot`): una sentencia `SELECT`, solo la vista, sin
   DML ni funciones de sistema, `LIMIT` impuesto.
3. **Ejecución** en el DAL. Si el validador o PostgreSQL rechazan el SQL, **un reintento** con el
   error; si la base está caída, sin reintento.
4. Al modelo vuelven `{descripcion, columnas, filas, truncado}` con las cifras a 1 decimal y un
   tope de tamaño. El SQL va a la traza (span `redactar_sql` por intento, evento `sql_invalido`,
   atributo `agente.sql`), nunca a la respuesta.

La vista tiene una fila por estación, contaminante, fecha y bloque (bloques de 7, 6, 7 y 4 horas)
y solo los 2 últimos años, contados desde el último día cargado. La media de un día, una estación
o un distrito es `SUM(media * n_horas) / SUM(n_horas)`, nunca `AVG(media)`.

Entorno local, sin tocar RDS (es la opción B del README principal, acotada al agente). Los pasos
con `psql` los ejecuta la persona, con el usuario propietario de las tablas:

```bash
# 1. PostgreSQL 18 en Docker (desde la raíz del repositorio)
docker compose up -d
# 2. Datos: el parquet desde S3 y la carga de 2 años (más rápida y con menos disco)
aws s3 cp s3://jupiter-calidad-aire-madrid/data/processed/resumen_datos_ml.parquet data/processed/
python src/etl/cargar_resumen_ml.py --desde 2024-01-01   # ajustar a ~2 años antes del último día
python src/etl/cargar_estaciones.py
# 3. Vista + índice, y después el rol (la contraseña, desde el entorno: nunca en el repositorio)
psql "$DATABASE_URL" -f deploy/sql/vista_mediciones_bloques.sql
psql "$DATABASE_URL" -v clave="$AGENTE_LECTURA_CLAVE" -f deploy/sql/rol_agente_lectura.sql
```

4. En `Agente/.env`: `DATABASE_URL=postgresql://agente_lectura:<clave>@localhost:5432/postgres`
   (con el `/postgres` final: sin él, PostgreSQL busca una base con el nombre del usuario)
   y, para evaluar, `FECHA_REFERENCIA` = el día siguiente al último del parquet (así el lote da
   lo mismo en cualquier máquina).

Recargar el parquet no rompe la vista: el cargador vacía la tabla (`TRUNCATE`) en vez de borrarla.
En RDS, la vista, el índice y el rol los ejecuta la persona antes de conectar el agente.

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

`tests/test_integracion_pg.py` queda fuera: comprueba el dialecto, la vista y los permisos del rol
contra el Postgres local cargado (no escribe nada). Solo con
`RUN_PG_TESTS=1 DATABASE_URL=postgresql://agente_lectura:...@localhost:5432/postgres`.

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

Datos: `evaluacion/casos_datos.json` tiene 16 casos (11 de datos, 2 mixtos y 3 de límites) con
lo esperado (`datos`: si se consulta la base) y un `gold` estructurado (entidad, contaminante,
periodo, valor u orden) para revisar a mano. Necesita `DATABASE_URL` y `FECHA_REFERENCIA=2026-05-01`
(los periodos del gold cuentan desde esa fecha). Las mixtas se evalúan a partir de la fase de varias
intenciones.

La carpeta `resultados/` no se versiona; los resultados revisados se añaden con `git add -f`.
