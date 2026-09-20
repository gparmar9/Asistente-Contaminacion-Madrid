# TFM – Sistema Inteligente de Monitorización de Calidad del Aire (Madrid)

Sistema basado en Machine Learning que analiza los datos de calidad del aire de Madrid, **detecta
anomalías automáticamente** y (fase futura) permite consultarlos mediante informes y un asistente
conversacional con LLM.

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
| Chatbot con *tool use*: `query_sql` y `search_documents` | Traduce una pregunta en lenguaje natural a consultas sobre ambas fuentes |
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

data/rag/*.md ──▶ trocear_corpus.py ──▶ embeddings locales ──▶ ChromaDB: corpus_rag   (Fase 2)

RDS + ChromaDB ──▶ LLM con tool use ──▶ Dashboard   (Fases 3-4)
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
pip install -r requirements-rag.txt   # arrastra PyTorch, descarga grande
python src/rag/ingesta_vector.py      # trocea data/rag/*.md e indexa en data/chroma/
```

La primera ejecución también descarga el modelo de embeddings (~470 MB, se cachea en `~/.cache`).
Comprueba que la búsqueda responde:

```bash
python src/rag/buscar.py "¿puedo correr hoy si soy asmático?"
```

El índice se reconstruye entero en cada ejecución, así que reejecutar la ingesta tras tocar el
corpus es la forma normal de actualizarlo.

### Comprobar los tests

```bash
# Tests unitarios (rápidos, sin base de datos):
pytest tests/ --ignore=tests/test_integracion_db.py -v
```

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
│   └── rag/
│       ├── trocear_corpus.py               # .md → fragmentos con metadatos (lógica pura, testeada)
│       ├── embeddings.py                   # modelo de embeddings + colección ChromaDB (config común)
│       ├── ingesta_vector.py               # reconstruye el índice vectorial desde el corpus
│       └── buscar.py                       # búsqueda semántica (base de la tool `search_documents`)
├── deploy/                                  # infraestructura AWS (imagen Lambda + roles IAM)
├── tests/                                   # tests unitarios y de integración
├── docker-compose.yml                       # PostgreSQL 18 + volumen
├── .env.example                             # plantilla de variables de entorno
├── requirements.txt
├── requirements-rag.txt                     # dependencias extra de la Fase 2 (RAG)
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
  y citar la fuente en la respuesta:

  ```yaml
  ---
  titulo: Ozono troposférico (O3) y salud
  tema: salud            # salud | normativa | referencia | estaciones | protocolo | disclaimer
  contaminantes: [O3]    # códigos afectados; [] si no aplica
  fuente: "OMS 2021; EEA; US EPA"
  ---
  ```

  > Nota YAML: los códigos que YAML interpreta como booleanos (`NO`, `YES`, `ON`, `OFF`) deben ir
  > entrecomillados en la lista `contaminantes` (p. ej. `["NO", NO2, NOx]`).

- **Disclaimer médico**: [`data/rag/aviso_medico.md`](data/rag/aviso_medico.md) (`tema: disclaimer`)
  está en el corpus para trazabilidad, pero su recordatorio debe aplicarse **desde el system
  prompt** del chatbot, no dejarse a la recuperación semántica (podría no recuperarse justo en la
  respuesta que lo necesita).

## Pipeline RAG / Vector DB (Fase 2)

Convierte el corpus de [`data/rag/`](data/rag/) en un índice vectorial consultable en lenguaje
natural. Vive en [`src/rag/`](src/rag/) y **no depende de PostgreSQL**: se puede usar sin levantar
Docker.

```
data/rag/*.md ──▶ trocear_corpus.py ──▶ embeddings.py ──▶ ChromaDB (data/chroma/, colección `corpus_rag`)
                     (fragmentos)        (vectores)                    │
                                                                       ▼
                                                       buscar.py · buscar_documentos(consulta, k, tema)
```

| Módulo | Qué hace |
|---|---|
| [`trocear_corpus.py`](src/rag/trocear_corpus.py) | Separa el frontmatter, trocea por encabezados `##` y antepone `título — sección` a cada fragmento (da contexto al embedding y mejora el *recall*). Ignora la sección `Fuentes` (ya está en los metadatos) y aplana `contaminantes` a cadena, porque ChromaDB no admite listas. Es **lógica pura**, sin torch ni ChromaDB: por eso se puede testear en CI. |
| [`embeddings.py`](src/rag/embeddings.py) | Config compartida por ingesta y búsqueda para que no se desincronicen: modelo `paraphrase-multilingual-MiniLM-L12-v2` (local, multilingüe, se cachea en memoria) y colección persistente con métrica **coseno**. |
| [`ingesta_vector.py`](src/rag/ingesta_vector.py) | Reconstruye el índice de cero en cada ejecución, así que el índice siempre refleja el corpus actual (**idempotente**). |
| [`buscar.py`](src/rag/buscar.py) | `buscar_documentos(consulta, k, tema)` — devuelve los *k* fragmentos más cercanos, con filtro opcional por metadatos. Es la base de la futura tool `search_documents` de la Fase 3. |

**Dependencias**: van en [`requirements-rag.txt`](requirements-rag.txt) aparte, porque
`sentence-transformers` arrastra PyTorch (descarga de cientos de MB) y no hace falta para la Fase 1.

> El índice generado (`data/chroma/`) **no se versiona**, igual que los `.parquet`: se regenera con
> `python src/rag/ingesta_vector.py`.

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
SQLAlchemy + psycopg2 · pyarrow · pytest · GitHub Actions · ChromaDB + sentence-transformers (RAG) ·
**AWS**: Lambda, ECR, S3, RDS, EventBridge Scheduler, SSM Parameter Store, CloudWatch y SNS ·
(futuro: FastAPI, LLM)
