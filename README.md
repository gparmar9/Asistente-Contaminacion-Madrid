# TFM – Sistema Inteligente de Monitorización de Calidad del Aire (Madrid)

Sistema basado en Machine Learning que analiza los datos de calidad del aire de Madrid, **detecta
anomalías automáticamente** y permite consultarlos en lenguaje natural mediante un asistente
conversacional con LLM y *tool use*. Los informes y el dashboard quedan pendientes.

> **Diseño completo y decisiones**: [`docs/plan_arquitectura_v3.html`](docs/plan_arquitectura_v3.html)
> (ábrelo en el navegador). Es la referencia viva de la arquitectura.

---

## MVP — Producto mínimo viable

El corte más pequeño que entrega valor real a un usuario: **ver el estado de la calidad del aire de
Madrid, saber si hay algo anómalo y poder preguntarlo en lenguaje natural**, combinando las
mediciones con conocimiento de salud y normativa.

**Qué entra en el MVP**

| Pieza | Qué aporta |
|---|---|
| Ingesta del histórico y del tiempo real a PostgreSQL | La base de datos sobre la que se responde |
| Detección de anomalías: Isolation Forest + baseline z-score | Distingue lo inusual de lo normal, tanto episodios ambientales como fallos de sensor |
| Vector DB con documentos de salud y normativa | Aporta el conocimiento que las mediciones por sí solas no contienen |
| Chatbot con *tool use*: `query_sql` y `buscar_evidencias` (citas verificables) | Traduce una pregunta en lenguaje natural a consultas sobre ambas fuentes |
| Interfaz mínima para chatear, con aviso médico visible | Hace el sistema usable por alguien que no escribe SQL |

**Qué queda fuera**: informes automáticos rotativos, dashboard completo con visualizaciones y
detectores de anomalías adicionales (LSTM Autoencoder). Son mejoras posteriores: el sistema tiene
sentido sin ellas.

El criterio para aceptar o descartar una pieza es sencillo: si el usuario puede llegar a una
respuesta útil sin ella, no es parte del MVP.

---

## Arquitectura (resumen)

```
                    EventBridge Scheduler — cada noche a las 23:45 (Europe/Madrid)
                                    │ invoca
API Madrid (tiempo real) ──▶ AWS Lambda: pipeline_tiempo_real ──▶ RDS PostgreSQL
                                    │                             ├── calidad_aire_horas_live
                       modelo .joblib descargado de S3            └── resumen_datos_ml (+ anomalías)
                                    │
                          CloudWatch: logs y alarmas por correo

CSV histórico 2018-2026 ──▶ Notebooks 01/02/03 (en local) ──▶ modelo .joblib ──▶ S3

data/rag/*.md ──▶ rag.indexar (embeddings e5) ──▶ ChromaDB: corpus_rag ──▶ rag.api: evidencias D1..Dn   (Fase 2)

Usuario ──▶ ApiUsuario (FastAPI) ──▶ RDS: /estaciones, /series          (Fase 4, en curso)
                 └── /chat ──▶ LLMOrchestrator ──▶ LLM hospedado (OpenAI-compatible)
                                     ├── query_sql ─────────▶ RDS PostgreSQL   (Fase 3)
                                     └── buscar_evidencias ─▶ servicio RAG (rag.api, HTTP)

Dashboard e informes ──▶ pendientes (consumirán solo la API)
```

Los **datos de contaminación viven estructurados en PostgreSQL**. La Vector DB solo contiene
documentos externos (salud, normativa), no datos de estaciones.

---

## Puesta en marcha

Hay **dos formas de trabajar**, y casi siempre querrás la primera:

| | Cuándo usarla | Qué necesitas |
|---|---|---|
| **A · Conectarse a la nube** | Trabajo normal: consultar datos, desarrollar el chatbot | Acceso a la cuenta de AWS |
| **B · Entorno local completo** | Reentrenar el modelo o experimentar sin tocar la base compartida | Docker, además de AWS para descargar los datos |

### Requisitos

- **Python 3.12** y **git**
- **Acceso a la cuenta de AWS del máster** (por el portal del curso)
- **AWS CLI v2** instalada (`winget install -e --id Amazon.AWSCLI` en Windows)
- **Docker Desktop**, solo para la opción B

La primera vez, fija la región una sola vez (no hace falta repetirlo en cada sesión):

```powershell
aws configure set region eu-west-1
```

Las credenciales en sí son distintas: son temporales y hay que pegarlas de nuevo en la terminal
cada vez que empieces a trabajar (ver Opción A, paso 1).

### Preparar el entorno (común a las dos opciones)

```bash
git clone git@github.com:gparmar9/Asistente-Contaminacion-Madrid.git
cd Asistente-Contaminacion-Madrid

python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

pip install -r requirements.txt
```

---

### Opción A · Conectarse a la base de datos en la nube

La base de datos vive en **Amazon RDS**, ya tiene el histórico cargado (1,27 M de filas) y **se
actualiza sola cada noche a las 23:45**. No hay que cargar nada ni levantar Docker.

**1. Credenciales de AWS.** Entra en el portal del máster, copia el bloque de claves y pégalo en tu
terminal. Son temporales: cuando un comando responda `ExpiredToken`, copia unas nuevas.

**2. Autoriza tu IP** — solo la primera vez, y cada vez que cambies de red:

```powershell
$sg = aws rds describe-db-instances --db-instance-identifier jupiter-postgres --query "DBInstances[0].VpcSecurityGroups[0].VpcSecurityGroupId" --output text
$mi_ip = (Invoke-RestMethod https://checkip.amazonaws.com).Trim()
aws ec2 authorize-security-group-ingress --group-id $sg --protocol tcp --port 5432 --cidr "$mi_ip/32"
```

**3. Coge la cadena de conexión.** Está cifrada en AWS, así que no hay que pasarse contraseñas por
chat ni guardarlas en ficheros:

```powershell
$Env:DATABASE_URL = aws ssm get-parameter --name "/jupiter/database_url" --with-decryption --query "Parameter.Value" --output text
```

**4. Comprueba que conectas:**

```powershell
python -c "import os; from sqlalchemy import create_engine, text; print(create_engine(os.environ['DATABASE_URL']).connect().execute(text('select count(*) from resumen_datos_ml')).scalar())"
```

Debería responder **1274644**.

> Es la **base de datos compartida del equipo**. Consulta con toda libertad; antes de borrar o
> recargar tablas, avisa por el grupo.

---

### Opción B · Entorno local completo

**1. Variables de entorno**

```bash
# Windows:
copy .env.example .env
# Linux/Mac:
cp .env.example .env
```

Edita `.env` y pon una contraseña. Las credenciales de `POSTGRES_*` deben **coincidir** con las de
`DATABASE_URL`. El fichero `.env` está en `.gitignore` (no se sube nunca).

**2. Levantar PostgreSQL en Docker**

```bash
docker compose up -d
```

Arranca un contenedor `jupiter_postgres` (PostgreSQL 18) en `localhost:5432`, con los datos en un
**volumen nombrado** (persisten aunque pares el contenedor).

> Si ya tienes un PostgreSQL nativo ocupando el puerto 5432: para el servicio, o cambia
> `POSTGRES_PORT` en `.env` (p. ej. a `5433`) y actualiza el puerto en `DATABASE_URL`.

Parar / arrancar: `docker compose down` / `docker compose up -d`. `docker compose down -v` **borra**
los datos.

**3. Descargar el histórico y el modelo desde S3**

Ya no hace falta bajar nada del Drive: los ficheros grandes (que no están en git) viven en S3.

```bash
aws s3 cp s3://jupiter-calidad-aire-madrid/data/raw/datos_completos_2018_2026.csv data/raw/
aws s3 cp s3://jupiter-calidad-aire-madrid/models/isolation_forest.joblib models/
aws s3 cp s3://jupiter-calidad-aire-madrid/data/processed/ data/processed/ --recursive --exclude "*" --include "*.parquet"
```

> Si prefieres **regenerar** los artefactos en vez de descargarlos, ejecuta en orden los notebooks
> [`02_preprocesado_y_features.ipynb`](notebooks/02_preprocesado_y_features.ipynb) y
> [`03_entrenamiento_anomalias.ipynb`](notebooks/03_entrenamiento_anomalias.ipynb).

**4. Cargar las tablas**

```bash
python src/etl/cargar_resumen_ml.py     # ~1,27 M filas vía COPY
python src/etl/cargar_estaciones.py     # catálogo de las 24 estaciones
```

**5. Ejecutar el pipeline a mano**

```bash
python src/etl/pipeline_tiempo_real.py
```

Baja datos de la API, los guarda en `calidad_aire_horas_live`, corre la inferencia y hace *upsert*
en `resumen_datos_ml`. Es **idempotente**: reejecutarlo no duplica nada, y corrige las horas que
todavía no estaban publicadas cuando se ejecutó antes.

> En la nube esto lo hace la Lambda cada noche; en local solo tiene sentido para probar.

---

### Indexar el corpus documental (RAG, Fase 2)

No depende de Postgres ni del histórico: funciona igual elijas la opción A o la B.

```bash
pip install torch --index-url https://download.pytorch.org/whl/cpu   # opcional: rueda de CPU, más ligera
pip install -r requirements-rag.txt   # versiones fijadas; instala también el paquete `rag` (-e .)
python -m rag.indexar                 # trocea data/rag/*.md e indexa en data/chroma/
```

La primera ejecución también descarga el modelo de embeddings `intfloat/multilingual-e5-base`
(~1,1 GB, se cachea en `~/.cache/huggingface`). Comprueba que la recuperación responde y arranca
el servicio:

```bash
python -m rag.evidencias "¿puedo correr si soy asmático?"   # evidencias D1..Dn bajo el umbral
python -m rag.api                                           # servicio HTTP en :8010 (docs en /docs)
```

El índice se reconstruye entero en cada ejecución, así que reindexar tras tocar el corpus es la
forma normal de actualizarlo. Detalles, variables de entorno y contrato HTTP en
[`src/rag/README.md`](src/rag/README.md).

### Comprobar los tests

```bash
# Tests unitarios del ETL/ML (rápidos, sin base de datos):
pytest tests/ --ignore=tests/test_integracion_db.py -v

# Tests de ApiUsuario (SQLite en memoria, sin BBDD externa):
pytest ApiUsuario/tests/ -v

# Tests del orquestador LLM (LLM falso con guion, sin red ni torch):
pytest LLMOrchestrator/tests/ -v
```

Cada servicio instala sus dependencias de test aparte
(`ApiUsuario/requirements-dev.txt`, `LLMOrchestrator/requirements-dev.txt`).
En CI los tres corren como jobs independientes.

### La API en contenedores (como irá en la EC2)

Cada servicio tiene su propia imagen, para poder desplegarlos por separado:

| Servicio | Imagen | Puerto | Habla con |
|---|---|---|---|
| `api-usuario` | [`ApiUsuario/Dockerfile`](ApiUsuario/Dockerfile) | 8000 | base de datos, `orquestador` |
| `orquestador` | [`LLMOrchestrator/Dockerfile`](LLMOrchestrator/Dockerfile) | 8100 | base de datos, `rag`, proveedor del LLM |
| `rag` | [`deploy/Dockerfile.rag`](deploy/Dockerfile.rag) | 8010 | nadie: lleva el modelo y el índice dentro |

Levantarlo todo en local (necesita Docker Desktop arrancado):

```bash
# 1. Claves del LLM: copiar LLMOrchestrator/.env.example a LLMOrchestrator/.env
#    y rellenar LLM_BASE_URL, LLM_API_KEY y LLM_MODEL.
# 2. Construir y arrancar (la primera vez tarda: la imagen del RAG ocupa ~2,5-3 GB):
docker compose --profile api up -d --build

# 3. Probar:
curl http://localhost:8000/health
curl -X POST http://localhost:8000/chat -H "Content-Type: application/json" \
     -d '{"pregunta": "¿Qué efectos tiene el NO2 en la salud?"}'

# Parar:
docker compose --profile api down
```

- Sin `--profile api`, `docker compose up -d` sigue levantando **solo la base de datos**.
- Por defecto la API usa la base de datos del contenedor `db`. Para usar la de RDS (con los datos
  reales), define `DATABASE_URL_API` en el `.env` de la raíz y enciende la RDS antes.
- Los puertos solo se abren en `127.0.0.1`. Dentro de Docker, los servicios se encuentran por
  su nombre (`http://rag:8010`), que es justo lo que se repetirá en la EC2.
- La primera pregunta que use documentos tarda unos segundos más: el RAG carga el modelo en
  memoria en la primera búsqueda.

---

## Infraestructura en AWS

La Fase 1 ya no depende de ningún ordenador: **se ejecuta sola en AWS todas las noches a las 23:45**.

| Servicio | Para qué |
|---|---|
| **S3** | Modelo entrenado, histórico y parquet |
| **RDS PostgreSQL** | La base de datos del proyecto |
| **Lambda + EventBridge Scheduler** | Ejecuta el pipeline cada noche |
| **SSM Parameter Store** | La cadena de conexión, cifrada |
| **CloudWatch + SNS** | Logs y aviso por correo si algo falla |

Comprobar que se ejecutó anoche:

```powershell
aws logs tail /aws/lambda/jupiter-pipeline --since 1d --format short
```

> **Detalle completo** (imagen, roles IAM, cómo desplegar un cambio y qué se rompe si te lo
> saltas): [`deploy/README.md`](deploy/README.md).

---

## Estructura del repositorio

```
├── data/
│   ├── raw/
│   │   ├── datos_completos_2018_2026.csv   # histórico (NO en git — se descarga de S3)
│   │   └── estaciones-de-control.csv       # catálogo de estaciones
│   ├── processed/
│   │   ├── calidad_aire_live.csv           # backup del workflow ya desactivado (ver CI/CD)
│   │   └── *.parquet                        # artefactos de notebooks (NO en git)
│   ├── rag/                                 # corpus fuente del RAG (Fase 2) — .md versionados
│   └── chroma/                              # índice vectorial generado (NO en git)
├── docs/
│   ├── notas_memoria.md                     # decisiones, resultados y bitácora para la memoria
│   └── plan_arquitectura_v3.html            # diseño y decisiones (referencia viva)
├── models/
│   └── isolation_forest.joblib             # modelos entrenados (6, uno por contaminante)
├── notebooks/
│   ├── 01_eda_calidad_aire.ipynb           # análisis exploratorio
│   ├── 02_preprocesado_y_features.ipynb    # bloques del día + features → ResumenDatosML
│   └── 03_entrenamiento_anomalias.ipynb    # Isolation Forest + baseline
├── src/
│   ├── etl/
│   │   ├── limpiar_datos.py                    # parser único ancho→largo (wide_a_largo)
│   │   ├── features_bloques.py             # agregación a bloques + features (z-score, cobertura, CV)
│   │   ├── inferencia.py                   # aplica el Isolation Forest
│   │   ├── pipeline_tiempo_real.py         # orquestador API → Postgres → anomalías
│   │   ├── cargar_resumen_ml.py            # carga inicial masiva (COPY) de ResumenDatosML
│   │   ├── cargar_estaciones.py            # carga la tabla de estaciones (con distrito)
│   │   └── ingesta_datos_live_csv.py       # backup diario a CSV (GitHub Action)
│   └── rag/                                # paquete `rag` (detalle en src/rag/README.md)
│       ├── corpus.py                       # .md → fragmentos con chunk_id estable (lógica pura)
│       ├── embeddings.py                   # modelo e5 + recuento de tokens + colección ChromaDB
│       ├── indexar.py                      # reconstruye el índice vectorial desde el corpus
│       ├── buscar.py                       # búsqueda semántica con filtro por tema
│       ├── evidencias.py                   # evidencias D1..Dn, validación de citas y bibliografía
│       ├── api.py                          # servicio FastAPI (tool `buscar_evidencias` para el LLM)
│       └── errores.py                      # excepciones tipadas
├── ApiUsuario/                              # servicio FastAPI de usuario (Fase 4)
│   ├── Dockerfile                           # imagen del servicio
│   ├── src/api_usuario/                     # routers / entities / data_access / shared
│   └── tests/                               # tests con SQLite en memoria (sin BBDD externa)
├── LLMOrchestrator/                         # servicio del agente LLM (Fase 3)
│   ├── Dockerfile                           # imagen del servicio (sin torch: el RAG va aparte)
│   ├── src/llm_orchestrator/
│   │   ├── business/agente.py               # bucle del agente (LLM ↔ tools)
│   │   ├── tools/sql_tool.py                # query_sql: 5 consultas predefinidas
│   │   ├── tools/rag_tool.py                # buscar_evidencias: cliente HTTP del servicio RAG
│   │   └── integrations/llm_client.py       # cliente OpenAI-compatible (proveedor por entorno)
│   └── tests/                               # LLM falso con guion, sin red ni torch
├── deploy/                                  # infraestructura AWS (imágenes Lambda y RAG + roles IAM)
├── tests/                                   # tests unitarios y de integración
├── docker-compose.yml                       # PostgreSQL 18; con --profile api, la API completa
├── .env.example                             # plantilla de variables de entorno
├── requirements.txt
├── requirements-rag.txt                     # dependencias extra de la Fase 2 (RAG), versiones fijadas
├── pyproject.toml                           # empaquetado del paquete `rag` y config de pytest
└── AGENTS.md / CLAUDE.md                    # instrucciones para asistentes de IA (Claude, Codex)
```

---

## Modelo de datos (PostgreSQL)

| Tabla | Qué contiene |
|---|---|
| `calidad_aire_horas_live` | Horario crudo en formato largo (una fila por medición horaria). Clave única `(estacion, magnitud, fecha)`. |
| `resumen_datos_ml` | **Tabla principal**: una fila por (estación, magnitud, día, bloque) con estadísticos, baseline y salida del modelo (`anomaly_score`, `is_anomaly`, `expected_value`). ~1,27M filas. |
| `baseline_historico` | Valor esperado (`media_esperada`, `std_esperada`) por (estación, magnitud, bloque, mes). Lo usa la inferencia. |
| `estaciones` | Dimensión de las 24 estaciones: `nombre`, `distrito`, `tipo` (tráfico/fondo/suburbana), coordenadas y qué contaminantes mide. Se une a las tablas de mediciones por `estacion = codigo_corto`; da el contexto geográfico al chatbot. |

---

## Corpus documental para el RAG (Fase 2)

Los datos de contaminación viven **estructurados en PostgreSQL**; el conocimiento externo
(salud, normativa, protocolos) vive como **documentos** en [`data/rag/`](data/rag/). Es el
corpus fuente que se trocea, se convierte en *embeddings* y se indexa en la Vector DB (ChromaDB)
— ver [Pipeline RAG / Vector DB](#pipeline-rag--vector-db-fase-2).

- **Formato**: Markdown (`.md`), un documento por tema. Se trocea por encabezados `##`.
- **Se versionan en git**: son fuente escrita a mano y pequeños. Lo que **no** se versiona es el
  índice generado (embeddings/ChromaDB), igual que los `.parquet`.
- **Metadatos**: cada documento empieza con *frontmatter* YAML para poder filtrar en la búsqueda
  y construir la bibliografía de la respuesta (las fuentes salen de aquí, nunca del modelo):

  ```yaml
  ---
  titulo: Ozono troposférico (O3) y salud
  tema: salud                  # salud | normativa | proyecto
  contaminantes: [O3]          # códigos afectados; [] si no aplica
  revisado: true               # false = se lee y valida, pero no se indexa
  fecha_revision: 2026-09-23   # obligatoria si revisado
  fuentes:                     # obligatoria y no vacía si revisado
    - titulo: WHO global air quality guidelines 2021
      organismo: OMS
      url: https://www.who.int/publications/i/item/9789240034228
  ---
  ```

  > Nota YAML: los códigos que YAML interpreta como booleanos (`NO`, `YES`, `ON`, `OFF`) deben ir
  > entrecomillados en la lista `contaminantes` (p. ej. `["NO", NO2, NOx]`).

- **Aviso sanitario**: el texto del aviso vive **en código** ([`src/rag/evidencias.py`](src/rag/evidencias.py)),
  no en el corpus, y se añade siempre que la pregunta es de salud o se cita un documento con
  `tema: salud`. Así no depende de que la búsqueda recupere justo el documento
  [`aviso_medico.md`](data/rag/aviso_medico.md), que sigue en el corpus como documento normal.

## Pipeline RAG / Vector DB (Fase 2)

Convierte el corpus de [`data/rag/`](data/rag/) en un índice vectorial y lo expone como una
**herramienta (*function tool*) para el LLM de la API de chat**. El RAG **no llama a ningún modelo
de lenguaje**: devuelve fragmentos numerados `D1..Dn`, el modelo redacta citándolos y después el
RAG **valida las citas y construye la bibliografía desde el corpus**. Vive en
[`src/rag/`](src/rag/) y **no depende de PostgreSQL**: se puede usar sin levantar Docker.

```
data/rag/*.md ──▶ rag.corpus ──▶ rag.embeddings ──▶ ChromaDB (data/chroma/, colección `corpus_rag`)
 (revisado: true)  (fragmentos)   (vectores e5)               │
                                                              ▼
                          rag.buscar · rag.evidencias (umbral → D1..Dn, validación de citas)
                                                              │
                                                              ▼
            rag.api (FastAPI, :8010) · POST /rag/evidencias · POST /rag/validar  ◀── LLM de la API de chat
```

| Módulo | Qué hace |
|---|---|
| [`corpus.py`](src/rag/corpus.py) | Valida el frontmatter, trocea por `##` con guardia de tokens y genera `chunk_id` estables (`ozono_salud:efectos-en-la-salud:0`). **Lógica pura**, sin torch ni ChromaDB: se testea en CI. |
| [`embeddings.py`](src/rag/embeddings.py) | Modelo `intfloat/multilingual-e5-base` (512 tokens), recuento con el tokenizer real y colección Chroma con métrica **coseno**. |
| [`indexar.py`](src/rag/indexar.py) | `python -m rag.indexar`: reconstruye el índice entero (**idempotente**). Calcula los embeddings antes de borrar el índice anterior y guarda modelo, commit del corpus y fecha. |
| [`buscar.py`](src/rag/buscar.py) | `buscar(consulta, k, tema)`: los *k* fragmentos más cercanos. Rechaza un índice construido con otro modelo. |
| [`evidencias.py`](src/rag/evidencias.py) | Filtra por umbral de distancia (0,1754, calibrado con `rag.evaluar`), numera `D1..Dn`, valida que cada afirmación del modelo cite IDs existentes y añade avisos y bibliografía. |
| [`api.py`](src/rag/api.py) | Servicio FastAPI: `/rag/evidencias`, `/rag/validar`, `/rag/herramienta` y `/salud`. Es lo que consume `LLMOrchestrator` por HTTP (tool `buscar_evidencias` de la Fase 3). |

El contrato completo para la API de chat (flujo en tres pasos, esquema de salida del modelo,
reparación) está en [`src/rag/README.md`](src/rag/README.md).

**Dependencias**: van en [`requirements-rag.txt`](requirements-rag.txt) aparte, porque
`sentence-transformers` arrastra PyTorch (descarga grande) y no hace falta para la Fase 1.

> El índice generado (`data/chroma/`) **no se versiona**, igual que los `.parquet`: se regenera con
> `python -m rag.indexar`.

---

### Bloques del día

| Bloque | Horas | Relevancia |
|---|---|---|
| Madrugada | 00–06 | Nivel de fondo |
| Mañana | 07–12 | Hora punta → picos de NO₂ |
| Tarde | 13–19 | Picos de O₃ (sol + calor) |
| Noche | 20–23 | Tráfico vespertino |

---

## Detección de anomalías

Dos familias de anomalías, cada una con su señal en las features:

| Familia | Señal (feature) |
|---|---|
| **Ambiental** (nivel inusual) | `z_score` — desviación respecto al valor esperado |
| **Operativa** — sensor caído | `cobertura` baja — faltan horas del bloque |
| **Operativa** — sensor congelado | `cv ≈ 0` — variabilidad relativa nula |

**Modelo actual**: un **Isolation Forest por contaminante** (scikit-learn) sobre esas 3 features
comparables entre estaciones, más el **baseline z-score** como referencia. Un detector temporal
(autoencoder) queda como mejora opcional — ver la sección **Extras / futuras mejoras** más abajo.

---

## Datos

- **Fuentes**: histórico 2018–2026 + API de Ciudades Abiertas de Madrid (~cada 20 min) + catálogo de estaciones.
- **6 magnitudes objetivo**: NO (7), NO2 (8), PM2.5 (9), PM10 (10), NOx (12), O3 (14).
- **Formato**: la fuente viene en formato *ancho* (`H01..H24` / `V01..V24`). `wide_a_largo` lo pasa a
  *largo* (una fila por hora). Las horas con flag `N` son placeholders y se descartan para el modelado.

---

## CI/CD y ramas

- **`tests.yml`**: dos jobs — *unit* (lógica pura, sin DB) e *integración* (Postgres de servicio efímero).
  Los tests de integración solo corren con `RUN_DB_TESTS=1` (lo pone el CI), nunca contra tu DB local por accidente.
- **`ingesta_diaria.yml`**: **desactivado**. Guardaba un CSV diario en el repositorio como backup
  gratuito; con los datos en RDS (y sus backups automáticos) dejó de tener sentido, inflaba el
  historial de git y provocaba conflictos de merge en todas las ramas. Se conserva el disparo manual.
- **`proteger_main.yml`**: los PR a `main` solo pueden venir de `development`.

**Flujo de ramas**: trabaja en ramas `feature/...` → PR a `development` → PR de `development` a `main`.

---

## Extras / futuras mejoras

Fuera del alcance actual; opcionales o para una segunda versión. El modelo de anomalías es una pieza
enchufable, así que añadir un detector nuevo no toca el resto del sistema.

| Mejora | Qué aporta | Coste |
|---|---|---|
| **Autoencoder (PCA)** | Anomalías de *forma* del perfil horario, solo con scikit-learn (un autoencoder lineal equivale a PCA). | Bajo |
| **LSTM Autoencoder** | Lo mismo con no-linealidades temporales; componente de *deep learning* para la memoria. | Alto (PyTorch) |
| Baseline con recencia | Ponderar más los años recientes (la contaminación baja con los años). | Bajo |
| **No puntuar el bloque del día aún abierto** | Evita falsos positivos de «sensor caído» mientras la franja está a medias: el mismo día da ~97 anomalías a media tarde y ~15 por la noche. Imprescindible antes de subir la frecuencia de ingesta. | Bajo |
| Distinguir el tipo de anomalía (ambiental / operativa) | El chatbot necesita separar «el ozono está alto» de «a esta estación le faltan horas». | Bajo |
| Despliegue automático a AWS desde GitHub Actions (OIDC) | Publicar la imagen sin claves estáticas ni pasos manuales. | Medio |

> **Sobre el LSTM**: se valoró como modelo principal en v2, pero el detector actual ya cubre las
> familias de anomalías relevantes (nivel, sensor caído, sensor congelado) y, sin datos etiquetados,
> su mejora no es medible. Queda como mejora opcional, no como bloqueo.

---

## Tecnologías

Python · pandas / NumPy · scikit-learn · PostgreSQL 18 (RDS en AWS, Docker en local) ·
SQLAlchemy + psycopg2 · pyarrow · pytest · GitHub Actions · ChromaDB + sentence-transformers + FastAPI (RAG) ·
FastAPI (`ApiUsuario`: estaciones, series y chat — ver [ApiUsuario/README.md](ApiUsuario/README.md);
`LLMOrchestrator`: agente LLM con tool use — ver [LLMOrchestrator/README.md](LLMOrchestrator/README.md)) ·
**AWS**: Lambda, ECR, S3, RDS, EventBridge Scheduler, SSM Parameter Store, CloudWatch y SNS
