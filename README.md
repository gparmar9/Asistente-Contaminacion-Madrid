# Asistente de calidad del aire de Madrid

**Proyecto Júpiter**: Trabajo de Fin de Máster del Máster en Inteligencia Artificial, Cloud
Computing y DevOps (Pontia).

## Equipo

| Alumno | GitHub |
|---|---|
| Carlos Fernández Ruiz | [@cfernandezTC](https://github.com/cfernandezTC) |
| Guillermo Parés Marín | [@gparmar9](https://github.com/gparmar9) |
| Sheila Pena | [@spena7496](https://github.com/spena7496) |

## Qué hace

Una web donde cualquiera puede ver el estado de la calidad del aire de Madrid y preguntarlo en
lenguaje natural:

- **Datos**: una Lambda descarga la API en tiempo real del Ayuntamiento cuatro veces al día y los
  guarda en PostgreSQL (RDS), junto al histórico 2018-2026.
- **Anomalías**: un Isolation Forest por contaminante marca los valores inusuales y los fallos de
  sensor.
- **Asistente**: un agente con LLM (Amazon Bedrock) responde con las mediciones y con documentos
  de salud y normativa recuperados de una base de datos vectorial (ChromaDB), siempre citando la
  fuente.
- **Despliegue**: contenedores Docker en AWS, CI/CD con GitHub Actions y trazas de cada respuesta
  en Phoenix y CloudWatch.

## Probarlo

Todo está desplegado en AWS. La web se abre sin instalar nada:

**http://madrid-respira.duckdns.org/**

Entra por `http://`: la web no tiene HTTPS. Si el subdominio no resolviera, la misma web está
en http://108.132.195.84/.

## Puesta en marcha en local

Requisitos: **git** y **Docker Desktop**.

```bash
git clone https://github.com/gparmar9/Asistente-Contaminacion-Madrid.git
cd Asistente-Contaminacion-Madrid

cp .env.example .env          # contraseñas de PostgreSQL y credenciales de AWS para Bedrock

docker compose --profile api up -d --build    # la primera vez tarda (imagen del RAG ~3 GB)
```

Se levantan los mismos servicios y con la misma configuración que en AWS
([`deploy/ec2/compose.yml`](deploy/ec2/compose.yml)): RAG, predicción, el agente con los modelos
de producción (Nemotron 3 Super y gpt-oss-120b en Amazon Bedrock), la API, la web y Phoenix. Las
diferencias son tres: la base de datos es un PostgreSQL en Docker en lugar de RDS, las imágenes se
construyen en tu equipo y Bedrock usa tus credenciales de AWS (en el `.env` o exportadas en el
terminal) en lugar del rol de la máquina.

La web queda en **http://localhost:8080** y las trazas del agente, en
**http://localhost:8080/phoenix/**. Para pararlo todo: `docker compose --profile api down`.

La base de datos local arranca vacía. Para cargar el histórico de bloques (2018 a abril de 2026;
necesita acceso a la cuenta de AWS del proyecto):

```bash
pip install -r requirements.txt
aws s3 cp s3://jupiter-calidad-aire-madrid/data/processed/resumen_datos_ml.parquet data/processed/
python src/etl/cargar_resumen_ml.py
python src/etl/cargar_estaciones.py

docker compose --profile api up -d    # crea la vista y el rol de solo lectura del agente
```

En local no corre la ingesta en tiempo real (en AWS la hace la Lambda): los datos acaban donde
acaba el histórico, así que las preguntas sobre hoy, la semana pasada o mañana no tienen datos.
Para probar el sistema completo, con datos al día, usa la web desplegada.

### Imágenes Docker

| Servicio | Dockerfile | Qué es |
|---|---|---|
| `web` | [`Web/Dockerfile`](Web/Dockerfile) | Dashboard React servido por nginx (proxy de `/api/*`) |
| `api-usuario` | [`ApiUsuario/Dockerfile`](ApiUsuario/Dockerfile) | API FastAPI: estaciones, series y chat |
| `agente` | [`Agente/Dockerfile`](Agente/Dockerfile) | Agente LLM con herramientas |
| `rag` | [`deploy/Dockerfile.rag`](deploy/Dockerfile.rag) | Búsqueda en la base vectorial (modelo e índice dentro) |
| `prediccion` | [`deploy/Dockerfile.prediccion`](deploy/Dockerfile.prediccion) | Predicción a uno y dos días (modelos dentro) |
| `phoenix` | imagen pública `arizephoenix/phoenix` | Trazas del agente |
| `pipeline` | [`deploy/Dockerfile.lambda`](deploy/Dockerfile.lambda) | Ingesta y detección de anomalías (AWS Lambda) |

En AWS las imágenes se construyen y publican en ECR con el workflow
[`desplegar.yml`](.github/workflows/desplegar.yml), y la EC2 las levanta con
[`deploy/ec2/compose.yml`](deploy/ec2/compose.yml). Detalle en [`deploy/README.md`](deploy/README.md).

### Comandos de desarrollo

Para desarrollar hemos usado un [`Makefile`](Makefile) que arranca cada servicio sin construir
imágenes, todos en un solo terminal (Ctrl+C los para todos). En Windows hace falta GNU Make:
`winget install ezwinports.make`.

```bash
make help        # lista todos los comandos
make install     # la primera vez: entorno de Python y dependencias de la web
make dev         # todo: PostgreSQL, Phoenix, RAG, agente, API y web
                 # (antes: make install-rag, make install-agente y rellenar Agente/.env)
make test        # todos los tests
```

### Tests

```bash
pytest tests/ --ignore=tests/test_integracion_db.py -v
pytest ApiUsuario/tests/ -v
pytest Agente/tests/ -v
cd Web && npm test
```

## Más documentación

- [`deploy/README.md`](deploy/README.md): infraestructura en AWS y despliegue.
- [`Agente/README.md`](Agente/README.md), [`ApiUsuario/README.md`](ApiUsuario/README.md),
  [`src/rag/README.md`](src/rag/README.md) y [`Web/README.md`](Web/README.md): cada servicio.
