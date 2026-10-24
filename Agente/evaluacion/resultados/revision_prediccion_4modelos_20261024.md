# Revisión: golden set + preguntas nuevas con los 4 modelos candidatos (2026-10-24)

Los cuatro modelos de la comparativa del equipo (`mistral.ministral-3-14b-instruct`,
`nvidia.nemotron-super-3-120b`, `openai.gpt-oss-120b-1:0`, `qwen.qwen3-next-80b-a3b`), en Bedrock
`eu-west-1`, por el clasificador (31 preguntas: 29 originales + 2 mixtas nuevas) y por la batería de
predicción (`casos_prediccion.json`, 9 casos / 10 turnos contra su gold). Una pasada por lote, en
serie, `LLM_MAX_TOKENS` 2048.

**Condiciones (distintas de las revisiones de octubre-16/18, no mezclar cifras):**

- Datos: PostgreSQL local con la vista `mediciones_bloques` sobre `resumen_datos_ml` (51.793 filas,
  2026-05-07 → 2026-10-07), no la réplica de Carlos (que llegaba al 2026-04-30).
- `FECHA_REFERENCIA=2026-10-08` (víspera + 1 del último día completo del servicio de predicción;
  `predecible_hasta` 2026-10-09).
- Servicio de predicción real (`prediccion.api` en contenedor, bundles h1 y h2).
- **Sin RAG**: un stub HTTP publica la definición de `buscar_evidencias` y devuelve cero evidencias
  (ningún caso del lote busca documentos; la batería documental queda fuera de esta revisión).
- Predicción en dos pasadas: **sin SQL** (`DATABASE_URL` vacía) y **con SQL** (rol `agente_lectura`,
  `HERRAMIENTA_DATOS=libre`).

Veredictos **automáticos y estructurales** (lo declarado en `esperado`: intención, ruta, uso de
herramientas). La columna «Respuesta correcta» de los informes crudos está en blanco, pendiente de
revisión humana.

## Clasificador (31 preguntas)

| | Ministral 14B | Nemotron 3 Super | gpt-oss-120b | Qwen3 Next 80B |
|-|-|-|-|-|
| Intención | 28/31 | 29/31 | **31/31** | 29/31 |
| Mixtas nuevas | 1/2 | **2/2** | **2/2** | 1/2 |
| Fase 5 (seguimientos y alcance) | 9/9 | 9/9 | 9/9 | 9/9 |
| Tema (etiquetados) | 12/12 | 12/12 | 12/12 | 12/12 |
| Latencia típica | ~240 ms | ~360 ms | 380–1600 ms | ~370 ms |

Fallos: Ministral, `DATOS, DOCUMENTAL → DATOS` ×2 y `DOCUMENTAL → DATOS` ×1; Nemotron,
`PREDICCION → DESCONOCIDA` ×2; Qwen, `DATOS, DOCUMENTAL → DATOS` y `DATOS, DOCUMENTAL →
DATOS, PREDICCION`; gpt-oss, pleno.

## Predicción (9 casos / 10 turnos)

| | Ministral 14B | Nemotron 3 Super | gpt-oss-120b | Qwen3 Next 80B |
|-|-|-|-|-|
| Sin SQL | 6/10 | **7/10** | **7/10** | 6/10 |
| Con SQL | 7/10 | 7/10 | **8/10** | 7/10 |
| Con SQL, descontando entorno¹ | 7/8 | 7/8 | **8/8** | 7/8 |
| Latencia mediana (con SQL) | **2,9 s** | 3,9 s | 6,2 s | 5,2 s |
| p95 (con SQL) | 10,2 s | 9,1 s | 11,9 s | 9,8 s |
| Coste del lote (con SQL) | **0,0091 $** | 0,0106 $ | 0,0140 $ | 0,0105 $ |

¹ Dos fallos idénticos en los cuatro modelos que no discriminan y vienen del entorno local:

- **pre-05** («¿Qué calidad del aire hizo el 2026-04-20…?»): el guion espera `PREDICCION` + frase
  fija («ya medido») porque con el marco original esa fecha caía a días del presente; con
  `FECHA_REFERENCIA=2026-10-08` queda a 6 meses y los cuatro clasifican `DATOS`. Con SQL activa los
  cuatro la resuelven consultando los datos reales: razonable, pero no lo que pide el gold.
- **pre-08** («¿Cómo estará el aire en Chamberí?»): el distrito no tiene ni una fila en la vista
  local; el servicio devuelve `sin_datos` y el agente la frase fija en los cuatro.

Además, `conv-pred-01.1` («¿Cómo está el aire ahora en Vallecas?») falla solo en la pasada sin SQL
(la herramienta de datos estaba apagada); con SQL la resuelven bien los cuatro por la ruta de datos.

**El caso que de verdad separa a los modelos es pre-01** («¿Habrá buena calidad del aire esta tarde
en el Retiro?»): gpt-oss llama a la herramienta de predicción en las dos pasadas; Nemotron en una de
dos; Ministral y Qwen en ninguna (se quedan en la frase fija). En la pasada en que Nemotron acertó,
la comprobación posterior de cifras dejó un hallazgo (sin bloquear).

## Lectura

- **gpt-oss-120b**: el más fiable — pleno en el clasificador y único 8/8 limpio en predicción —,
  a cambio de la peor latencia (mediana 6,2 s; p95 11,9 s) y el mayor coste por turno.
- **Nemotron 3 Super**: a un caso del pleno en ambos lotes, con mejor latencia que gpt-oss; su
  fallo propio del clasificador (2× `PREDICCION → DESCONOCIDA`) es el intent que más nos importa.
- **Ministral 14B**: el más rápido y barato, pero el más flojo dirigiendo la herramienta de
  predicción (nunca resuelve pre-01).
- **Qwen3 Next 80B**: rinde como Ministral en predicción pese al tamaño; en la revisión del 16-10
  ya mostró debilidad en mixtas y dos fugas de prompt en el lote adversario.

Coherente con las revisiones del equipo (datos de abril): gpt-oss y Nemotron arriba. Para el intent
PREDICCION en concreto, gpt-oss es el más fiable y Nemotron el mejor equilibrio acierto/latencia/coste.

## Informes crudos

`turnos_prediccion-{ministral,nemotron,gptoss,qwen}-20261024_*.md` (sin SQL) y
`turnos_prediccion-db-{…}-20261024_*.md` (con SQL), en esta misma carpeta, con cada respuesta y la
columna de revisión manual en blanco. Trazas JSONL en `Agente/trazas/`.
