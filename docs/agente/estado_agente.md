# Estado del trabajo en el agente (`Agente/`) — para continuar en otra sesión

Última actualización: 2026-10-04. Acompaña al plan (`docs/agente/plan_agente_llm.md`) y al registro
de decisiones (`docs/agente/decisiones_agente.md`). Se sobrescribe al final de cada sesión: dice
dónde está el trabajo, no su historia.

Para retomar: leer este documento, después el plan (§4 Fases) y `decisiones_agente.md` (§2).
Preguntar antes de decidir nada que el plan no cierre.

---

## 1. Dónde está el trabajo

| Dato | Valor |
|-|-|
| Rama | `feature/Agente` (sale de `development` tras el PR #64) |
| Commit | **Ninguno**: fases 1 a 3 están solo en el árbol de trabajo (el usuario decidió no hacer commit todavía) |
| Fases | **1 a 3 de 7 hechas**. La 4 no se ha empezado |
| Tests | 20 en verde: `Agente/.venv/bin/python -m pytest Agente/tests -q` |
| Entorno local | `Agente/.venv` (uv, Python 3.12). No está en git |
| Proveedor de LLM | Ninguno probado. Sin `.env`, `/responder` devuelve 503 |

Ficheros tocados fuera de `Agente/`: `.github/workflows/tests.yml` (job `agente`), `.gitignore`
(`/Agente/.venv/`), `AGENTS.md` y `README.md` (comando de tests), `docs/notas_memoria.md` (§2, §4,
§7.3.1, §12, §13), `docs/agente/decisiones_agente.md` y este documento. El plan sigue sin versionar:
la regla `docs/**/plan_*.md` del `.gitignore` solo existe en `feature/rag-metricas`.

---

## 2. Cómo funciona un turno hoy

1. **Clasificar.** Una llamada corta (temperatura 0, máximo 10 s) devuelve intención y tema.
   Si falla, tarda o el formato no es exacto: `DESCONOCIDA`.
2. **Decidir (código).**
   - `DATOS`, `PREDICCION`, `FUERA_DE_ALCANCE` → frase fija. Fin.
   - `CHARLA` → el modelo responde sin herramientas, con un prompt corto.
   - `DOCUMENTAL` → solo se ofrece `buscar_evidencias`. Si el RAG está caído → frase fija. Fin.
   - `DESCONOCIDA` → todas las herramientas, ninguna obligada (lo que hacían las fases 1 y 2).
3. **Bucle.** El modelo pide herramientas; el código las ejecuta. Máximo 3 vueltas. Si pide una
   herramienta no permitida, recibe un error y no se ejecuta.
4. Si una búsqueda sale vacía (`sin_evidencia`) y no hay evidencias: frase fija, fin.
5. Si es `DOCUMENTAL` y el modelo no buscó: **busca el código** con la pregunta y el tema.
6. **Cerrar.**
   - Sin evidencias → el texto del modelo es la respuesta (ruta libre).
   - Con evidencias → ruta documental: JSON aparte, `/rag/validar`, una reparación, y se entrega
     tal cual el texto del RAG.

Llamadas al LLM (incluido el clasificador): frase fija 1; charla 2; documental 4 (5 con
reparación; 3 si busca el código).

---

## 3. Qué hay en `Agente/`

```
src/agente/
  main.py               FastAPI: GET /salud, POST /responder (bucle en app.state; get_bucle() se sustituye en tests)
  config/settings.py    variables de entorno (ver .env.example)
  entities/chat.py      Pregunta, Fuente, Respuesta
  entities/intencion.py Intencion, Tema, Clasificacion
  business/intencion.py clasificar() + parser estricto; tabla de decisión (decidir())
  business/bucle.py     el turno (§2). ResultadoTurno trae intencion, busqueda_forzada y
                        ruta: libre | documental | sin_evidencia | fija
  business/sintesis.py  ruta documental: EvidenciasTurno (renumera D1..Dn) y sintesis_documental()
  business/frases.py    prompts y frases fijas (clasificador, charla, DATOS, PREDICCION, FUERA_DE_ALCANCE...)
  tools/base.py         contrato de herramienta; ResultadoHerramienta.internos = datos para el código, no para el modelo
  tools/rag.py          HerramientaRag: definicion() (cachea también esquema_salida), ejecutar(), validar()
  llm/cliente.py        crear_llm(): OpenAILike | BedrockConverse
  llm/falso.py          LLMFalso con guion para tests
tests/
  conftest.py           RagFingido: búsquedas en orden (respuestas=[...]) y /rag/validar simplificado
  test_bucle.py         0 llamadas, JSON válido, dos búsquedas (renumeración), reparación, doble fallo,
                        sin_evidencia, límite de vueltas, RAG caído
  test_api.py           contrato de /responder; LLM caído -> 503
  test_intencion.py     frase fija por intención, búsqueda que lanza el código, charla,
                        clasificador caído, herramienta vetada, documental con RAG caído
  test_cliente_llm.py   la fábrica activa tool use en OpenAILike
evaluacion/
  preguntas_clasificador.json  20 preguntas etiquetadas (15 de Preguntas.txt + 5 nuevas)
  evaluar_clasificador.py      mide el clasificador con un proveedor real (fuera de CI)
```

`Bucle` sin `llm_clasificador` trata todo como `DESCONOCIDA`: así los tests de las fases 1 y 2
siguen igual. `main.py` siempre crea el clasificador.

---

## 4. Cómo trabajar en local

```bash
cd Agente
uv venv .venv --python 3.12                                   # solo si falta
uv pip install --python .venv/bin/python -r requirements-dev.txt
.venv/bin/python -m pytest tests -q
cp .env.example .env    # rellenar LLM_* y RAG_URL=http://localhost:8010
.venv/bin/uvicorn agente.main:app --app-dir src --port 8200 --env-file .env
```

El RAG real se levanta desde la raíz con
`RAG_UMBRAL_DISTANCIA=0.1754 env-pontia-ml/bin/python -m rag.api` (puerto 8010, tarda ~1 min en
cargar el modelo). El entorno `env-pontia-ml` no tiene LlamaIndex: el agente usa siempre `Agente/.venv`.

---

## 5. Cosas que conviene recordar

- La síntesis documental usa **mensajes nuevos** (pregunta + evidencias), no el historial con
  herramientas: Bedrock Converse rechaza bloques de herramienta sin `toolConfig`.
  **Ojo**: la síntesis forzada de la ruta libre (límite de vueltas sin evidencias) sí envía ese
  historial sin herramientas. Probarlo con Bedrock en la fase 7.
- Si el modelo no devuelve JSON, el texto se manda igual a `/rag/validar`; el RAG lo rechaza y da
  el mensaje de reparación. El agente no valida el contrato por su cuenta.
- El texto del RAG lleva dentro el aviso, la lista de evidencias citadas (con `chunk_id`) y la
  bibliografía. `fuentes` y `advertencia` repiten esa información (decisión del usuario). La
  comprobación de internos de la fase 6 tendrá que ignorar ese bloque.
- `AVISO_SANITARIO` está copiado del RAG en `frases.py`: si el RAG cambia su texto, cambiarlo aquí.
- Si una respuesta del modelo pide a la vez una búsqueda vacía y otra herramienta que no es el RAG,
  el turno se cierra igual con `sin_evidencia`. Hoy no pasa (solo hay una herramienta); revisarlo
  cuando lleguen la herramienta SQL o la de ML.
- El tema del clasificador solo se usa en la búsqueda que lanza el código. Las búsquedas que pide
  el modelo llevan su propio tema.
- `REPETIR` no existe todavía: se añade en la fase 5 (decisión del usuario).
- Detalles de LlamaIndex de la fase 1 (copias del esquema, `is_function_calling_model`,
  `tool_call_id`): ver los comentarios en `tools/base.py`, `llm/cliente.py` y `bucle.py`.

---

## 6. Qué se verificó en esta sesión

- Fase 3: 20 tests en verde (11 de antes + 9 nuevos).
- El guion de evaluación compila y lee las 20 preguntas. **No se ha ejecutado**: falta proveedor.
- Fases 1 y 2 (sesión anterior): probadas con el `rag.api` real y el LLM falso.

---

## 7. Pendiente

1. **Commit** de las fases 1 a 3 cuando el usuario lo diga. El plan no debe entrar.
2. Confirmar la atribución «Carlos» en la bitácora (§12 de `notas_memoria.md`).
3. Elegir un proveedor de desarrollo (Mistral API, Groq u Ollama) y hacer la prueba de punta a
   punta con un modelo real (§6 del plan). Interesa ver si el modelo devuelve JSON válido a la
   primera y ejecutar `Agente/evaluacion/evaluar_clasificador.py`.
5. Revisar las etiquetas de `preguntas_clasificador.json` (las puse yo; alguna es discutible, p. ej.
   «¿Qué zona me recomendarías para vivir si tengo alergias?» como `DATOS`).
4. **Bedrock: comparar Ministral 14B 3.0 (`mistral.ministral-3-14b-instruct`) y gpt-oss-120b
   (`openai.gpt-oss-120b-1:0`)** en `eu-west-1` (decisión del 2026-10-04, ver `decisiones_agente.md`
   §2 y §7). Antes:
   - Credenciales: un perfil en `~/.aws/credentials` y `AWS_PROFILE` en `Agente/.env`. Faltan.
   - Subclase de `BedrockConverse` en `llm/cliente.py` que sobrescriba `metadata`: sin ella,
     Ministral 14B da `ValueError: Unknown model`. Pendiente de que el usuario la pida.
   - En WSL no hay AWS CLI; las consultas de solo lectura se hacen con `boto3` de `Agente/.venv`.

## 8. Siguiente paso: fase 4 (streaming y sesión)

Según el plan (§3 y §4, fase 4): `POST /responder/stream` (SSE) con eventos `status`, `token`,
`passthrough`, `error` y `done`; `/responder` reutiliza el mismo generador. `session_id` generado si
no llega y un turno a la vez por sesión. En `ApiUsuario`: `session_id` en el contrato,
`POST /chat/stream` como proxy SSE y `AGENTE_URL`.
