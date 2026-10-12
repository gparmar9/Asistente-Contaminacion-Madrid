# Informe de trazas del agente

2 turnos en 1 lote(s). Medianas con su n; el p95 es solo descriptivo. Precios: 2026-10-04, Bedrock eu-west-1.

## Resumen por lote

| Lote | Modelo | Turnos | Latencia mediana (ms) | p95 (ms) | Tokens mediana (entrada / salida) | Coste total (USD) | Sin coste (tokens o precio) | Síntesis válida a la primera |
|-|-|-|-|-|-|-|-|-|
| agente | mistral.ministral-3-14b-instruct | 2 | 24292 | 43117 | 2236 / 422 | 0,0013 | 0 | 1/1 |

## Lote `agente`

### Turnos por ruta e intención

| Ruta | Intención | n | Latencia mediana (ms) | Tokens mediana (entrada / salida) | Coste mediano (USD) |
|-|-|-|-|-|-|
| documental | DOCUMENTAL | 1 | 5467 | 3440 / 687 | 0,00099 |
| fija | DOCUMENTAL | 1 | 43117 | 1031 / 158 | 0,00029 |

### Latencia por fase

Las llamadas al LLM, por la fase que las contiene; `llm (bucle)` es una por vuelta.

| Span | n | Mediana (ms) | p95 (ms) |
|-|-|-|-|
| turno | 2 | 24292 | 43117 |
| clasificar | 2 | 411 | 543 |
| bucle | 2 | 12248 | 22018 |
| busqueda_forzada | 1 | 20044 | 20044 |
| sintesis_documental | 1 | 2709 | 2709 |
| validar | 1 | 90 | 90 |
| buscar_evidencias | 3 | 20044 | 20044 |
| llm (bucle) | 4 | 987 | 1921 |
| llm (clasificar) | 2 | 411 | 543 |
| llm (sintesis_documental) | 1 | 2619 | 2619 |

### Calidad del turno

| Indicador | Valor |
|-|-|
| Síntesis documentales | 1 |
| Válida a la primera (/rag/validar) | 1 |
| Válido tras reparación | 0 |
| Inválido tras reparar (insuficiencia) | 0 |
| Búsquedas forzadas por el código | 1 |
| Síntesis forzadas (límite de vueltas) | 0 |
| Llamadas a herramientas | 3 |
| Herramientas vetadas pedidas por el modelo | 0 |
| Herramientas con error | 2 |
| Turnos con error (excepción) | 0 |
| Turnos con tokens incompletos | 0 |

### Decisiones del código

| Decisión del código | n |
|-|-|
| documentacion_no_disponible | 1 |
