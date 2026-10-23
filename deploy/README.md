# Despliegue en AWS — Fase 1

Todo lo necesario para que el pipeline de tiempo real se ejecute **solo en la nube**, sin depender
de que nadie tenga el ordenador encendido. Esta carpeta contiene únicamente infraestructura; la
lógica del proyecto sigue en [`src/`](../src/).

```
deploy/
├── Dockerfile.lambda          # imagen que ejecuta Lambda
├── Dockerfile.lambda.dockerignore   # qué NO viaja al construirla (Docker lo asocia por el nombre)
├── Dockerfile.rag             # imagen del servicio RAG (modelo e índice dentro)
├── Dockerfile.rag.dockerignore
├── lambda_handler.py          # punto de entrada de la función
├── requirements-lambda.txt    # dependencias de esa imagen (solo las que usa el pipeline)
└── iam/                       # permisos, versionados para poder revisarlos en un PR
    ├── confianza-lambda.json
    ├── permisos-lambda.json
    ├── confianza-scheduler.json
    ├── permisos-scheduler.json
    ├── confianza-github-actions.json
    ├── permisos-github-actions.json
    ├── permisos-github-entorno.json
    ├── confianza-automation-rds.json
    └── permisos-automation-rds.json
```

El despliegue automático vive en [`desplegar.yml`](../.github/workflows/desplegar.yml) y
[`rollback.yml`](../.github/workflows/rollback.yml) (en `.github/workflows/`), y el encendido y
apagado a demanda en [`encender_entorno.yml`](../.github/workflows/encender_entorno.yml) y
[`apagar_entorno.yml`](../.github/workflows/apagar_entorno.yml).

## Qué hay desplegado

| Servicio | Nombre | Para qué |
|---|---|---|
| **S3** | `jupiter-calidad-aire-madrid` | Modelo entrenado, histórico y parquet |
| **RDS PostgreSQL** | `jupiter-postgres` | La base de datos del proyecto (`db.t4g.micro`). Siempre encendida |
| **ECR** | `jupiter-pipeline` | Registro de la imagen de la función |
| **Lambda** | `jupiter-pipeline` | Ejecuta el pipeline (imagen, x86_64, 1024 MB, timeout 300 s) |
| **EventBridge Scheduler** | `jupiter-pipeline-diario` | Lo dispara al cierre de cada franja: 07:45, 13:45, 20:45 y 23:45 (`Europe/Madrid`) |
| **EventBridge Scheduler** | `jupiter-rds-encender` / `jupiter-rds-apagar` | **Desactivadas** desde el 2026-10-23: encendían la RDS a las 22:00 y la apagaban a la 01:30 |
| **Systems Manager Automation** | `AWS-StartRdsInstance` / `AWS-StopRdsInstance` | Runbooks de AWS que encienden o apagan la RDS solo si hace falta |
| **SSM Parameter Store** | `/jupiter/database_url` | La cadena de conexión, cifrada |
| **CloudWatch + SNS** | `jupiter-alertas` | Logs, métricas de negocio del pipeline (`Jupiter/Pipeline`) y avisos por correo |
| **CloudFormation** | `jupiter-observabilidad` | Logs de la EC2, filtros de métricas, alarmas y el panel `jupiter` |
| **AWS Budgets** | `jupiter-bedrock-diario` | Aviso por correo si Bedrock pasa de 2 $ en un día (solo avisa) |

Flujo de una ejecución:

```
Scheduler (cierre de franja) ──▶ Lambda ──▶ API de Madrid
                        │  ├─ lee el secreto de SSM
                        │  └─ descarga el modelo de S3 (lo cachea en /tmp)
                        └──▶ RDS: calidad_aire_horas_live + resumen_datos_ml
```

## Por qué una imagen de contenedor y no un .zip

Lambda admite los dos formatos, pero un `.zip` está limitado a **250 MB descomprimidos** y
pandas + NumPy + SciPy + scikit-learn los superan. Con imagen el límite son 10 GB (la nuestra ocupa
1,26 GB, ~300 MB comprimida en ECR).

Ventaja añadida: la imagen base de AWS trae un emulador del entorno de Lambda, así que **lo mismo
que se despliega se puede probar en local** antes de subirlo.

## El handler

[`lambda_handler.py`](lambda_handler.py) **no duplica lógica**: importa `pipeline_tiempo_real` y solo
resuelve de dónde salen en la nube las tres cosas que en local vienen del disco y del `.env`:

| En local | En la nube |
|---|---|
| `models/isolation_forest.joblib` | S3 → `/tmp` (único directorio escribible) |
| `DATABASE_URL` del `.env` | SSM Parameter Store, descifrado con el rol |
| `data/raw/estaciones-de-control.csv` | Dentro de la propia imagen (son 5 KB) |

Todo eso está **fuera** de la función `handler`, así que se ejecuta una vez por contenedor y no en
cada invocación: Lambda reutiliza el contenedor mientras está caliente.

**Variables de entorno de la función:** `S3_BUCKET` (obligatoria), `MODEL_KEY` y `SSM_PARAM`
(opcionales, con valores por defecto). La contraseña no aparece en ninguna.

## Los roles IAM

Hay **cinco roles**, y cada uno se define con **dos ficheros**, que responden a preguntas distintas:

- **Política de confianza** (`confianza-*.json`): *¿quién puede ponerse este rol?*
- **Política de permisos** (`permisos-*.json`): *¿qué puede hacer quien lo lleva puesto?*

### `jupiter-lambda-pipeline` — rol de ejecución de la función

| Permiso | Alcance |
|---|---|
| `AWSLambdaBasicExecutionRole` (política gestionada por AWS) | Escribir sus logs en CloudWatch |
| `s3:GetObject` | **Solo** `models/*` del bucket del proyecto |
| `ssm:GetParameter` + `kms:Decrypt` | **Solo** los parámetros `/jupiter/*`, y solo a través de SSM |

Fíjate en lo estrecho que es: la función **no puede leer el histórico** del bucket, ni escribir en
S3, ni tocar otros parámetros. Si alguien colara código malicioso en la imagen, el daño posible
sería mínimo. Eso es el **mínimo privilegio**.

### `jupiter-scheduler-pipeline` — rol del disparador

Lo usan las tres programaciones. Puede:

- `lambda:InvokeFunction` sobre **esta** función, y ninguna otra.
- Lanzar **solo** los runbooks `AWS-StartRdsInstance` y `AWS-StopRdsInstance`.
- Entregarles el rol `jupiter-automation-rds` (`iam:PassRole`, solo hacia Systems Manager).

No puede tocar la RDS directamente: lo hace el runbook con su propio rol.

### `jupiter-automation-rds` — rol de los runbooks

Lo asume Systems Manager (solo desde esta cuenta) mientras ejecuta el runbook. Permisos:
consultar, encender y apagar **solo** `jupiter-postgres`. No puede borrarla ni tocar otras bases
de datos.

### `jupiter-github-actions-deploy` — rol del despliegue automático

Lo asumen los workflows [`desplegar.yml`](../.github/workflows/desplegar.yml) y
[`rollback.yml`](../.github/workflows/rollback.yml) mediante
**OIDC**: GitHub entrega al workflow un token firmado que dice de qué repositorio y rama viene, y
AWS lo cambia por credenciales temporales de este rol. No hay ninguna clave de AWS guardada en
GitHub.

| Permiso | Alcance |
|---|---|
| `ecr:GetAuthorizationToken` | Login en ECR (esta acción no admite restringirse a un repositorio) |
| Subida de imágenes (`ecr:PutImage`, capas…) | **Solo** el repositorio `jupiter-pipeline` |
| `lambda:UpdateFunctionCode`, `PublishVersion`, `ListVersionsByFunction` + lecturas | **Solo** la función `jupiter-pipeline` (no puede borrarla ni cambiar su configuración) |

**Quién puede asumirlo** ([`confianza-github-actions.json`](iam/confianza-github-actions.json)):
tokens con audiencia `sts.amazonaws.com` y `sub` de **este** repositorio, desde cualquier rama.
IAM solo sabe evaluar las claves `aud` y `sub` del token, así que no se puede restringir a un
fichero de workflow concreto. Consecuencia práctica: cualquier workflow del repositorio con
`id-token: write` podría asumir el rol, así que **los cambios en `.github/workflows/` se revisan
en el PR** como cualquier otro código con acceso a producción. El `<ID_CUENTA>` del fichero se
sustituye al aplicarlo; no se versiona porque el repositorio es público.

### `jupiter-github-actions-entorno` — rol del encendido y apagado a demanda

Lo asumen los workflows [`encender_entorno.yml`](../.github/workflows/encender_entorno.yml) y
[`apagar_entorno.yml`](../.github/workflows/apagar_entorno.yml) con la **misma** política de
confianza OIDC que el anterior. Permisos
([`permisos-github-entorno.json`](iam/permisos-github-entorno.json)): consultar, encender y apagar
**solo** `jupiter-postgres`. No puede borrarla ni modificarla. Se separa del rol de despliegue para
que cada workflow tenga solo lo que necesita.

## Desplegar un cambio

### Opción recomendada: desde GitHub Actions

Pestaña **Actions → Desplegar → Run workflow**, eligiendo:

- **La rama** en *Use workflow from*: se despliega el código de esa rama tal como está (lo normal
  es `main`; con otra rama el workflow avisa). No hay que indicar ninguna imagen: el workflow la
  construye. Elegir una versión anterior es cosa del rollback.
- **El componente** en el desplegable: `lambda` o uno de los servicios de la EC2 (`web`,
  `api-usuario`, `agente`, `rag`, `phoenix`; ver [La aplicación en la EC2](#la-aplicación-en-la-ec2)).

Dos despliegues del **mismo** componente nunca coinciden (el segundo espera); de componentes
distintos, sí. Para la Lambda, el workflow:

1. Ejecuta los tests unitarios y de integración ([`tests.yml`](../.github/workflows/tests.yml)).
   Si fallan, no despliega.
2. Construye la imagen y la sube a ECR con dos tags: el **SHA corto del commit** (para saber qué
   código corre y poder volver atrás) y `latest`.
3. Actualiza la función, espera a que termine y publica una **versión numerada** con la
   descripción `deploy <commit> desde <rama> (@quien)`.
4. **Verifica** que el `CodeSha256` de la Lambda coincide con el digest recién subido; si no,
   falla.
5. Deja un resumen en la ejecución: rama, commit, tag, digest y versión publicada.

Requisitos (una sola vez):

- El workflow tiene que estar en `main`: GitHub solo muestra el botón *Run workflow* de los
  workflows que existen en la rama por defecto.
- Un secreto del repositorio `AWS_ACCOUNT_ID` (*Settings → Secrets and variables → Actions*) con
  el ID de la cuenta. Es un secreto para que GitHub lo oculte en los logs, que son públicos.

### Opción manual (sin GitHub Actions)

Desde la **raíz del repositorio** (el contexto de construcción incluye `src/` y el CSV de
estaciones):

```powershell
docker build --provenance=false --sbom=false -f deploy/Dockerfile.lambda -t jupiter-pipeline .

$cuenta = aws sts get-caller-identity --query Account --output text
$registro = "$cuenta.dkr.ecr.eu-west-1.amazonaws.com"

aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin $registro
docker tag jupiter-pipeline:latest "$registro/jupiter-pipeline:latest"
docker push "$registro/jupiter-pipeline:latest"

aws lambda update-function-code --function-name jupiter-pipeline --image-uri "$registro/jupiter-pipeline:latest" --publish
aws lambda wait function-updated --function-name jupiter-pipeline
```

### Tres cosas que se olvidan y cuestan una tarde

1. **`update-function-code` no es opcional.** Subir la imagen a ECR con el mismo tag **no**
   actualiza la función: se queda con la copia que tenía. Y como el tag es `latest`, nada te avisa.
2. **`--provenance=false`.** Sin esa opción, Docker sube además un manifiesto de atestación y la
   imagen queda publicada como *lista de manifiestos*, un formato que Lambda **no sabe leer**. El
   error que da no apunta a la causa.
3. **La arquitectura tiene que coincidir.** La imagen se construye `x86_64` en un PC normal y la
   función está creada como `x86_64`. Si no cuadran, falla al arrancar sin explicar por qué.

Para verificar que la función tiene la imagen nueva, compara el digest del `push` con:

```powershell
aws lambda get-function-configuration --function-name jupiter-pipeline --query "{estado:State,sha:CodeSha256}" --output table
```

## Volver a una versión anterior (rollback)

Si un despliegue rompe algo: **Actions → Rollback → Run workflow**, eligiendo el componente en el
desplegable (hoy solo `lambda`), en dos pasos.

1. **Consultar** (campos vacíos). No cambia nada: muestra en el resumen de la ejecución una tabla
   con las versiones publicadas (número, fecha, imagen, descripción) y cuál está en producción, y
   sugiere la anterior.
2. **Volver atrás**: relanzar con la **versión** elegida y el **motivo**. El workflow apunta la
   función a la imagen de esa versión por su digest (no reconstruye nada: segundos), publica una
   versión nueva con la descripción `rollback a vN: <motivo> (@quien)` y verifica que la Lambda
   ejecuta exactamente esa imagen.

No hay un "volver a la anterior" automático a propósito: después de un rollback, la versión
anterior es justo la que estaba mal. Si la versión activa ya es un rollback, la tabla lo avisa en
vez de sugerir.

El rollback funciona porque **ECR conserva las imágenes antiguas** aunque pierdan su tag cuando se
sube una nueva. Si algún día se añade una regla de ciclo de vida que borre imágenes sin tag, las
versiones que apunten a ellas dejarán de poder recuperarse.

Deshacer un rollback es otro rollback (a la versión que se retiró) o un despliegue nuevo.

## Horario de las cargas

La Lambda carga **al cierre de cada franja**, cuando la API ya ha publicado su última hora (la API
publica cada hora con menos de 45 min de retraso). Una sola programación,
`cron(45 7,13,20,23 * * ? *)` en `Europe/Madrid`:

| Hora (Madrid) | Franja que se cierra | Última hora publicada |
|---|---|---|
| 07:45 | madrugada (0–6 h) | 6 |
| 13:45 | mañana (7–12 h) | 12 |
| 20:45 | tarde (13–19 h) | 19 |
| 23:45 | noche (20–23 h) | 22: la hora 23 no llega nunca |

Cada ejecución recalcula **todos los bloques cerrados del día** con lo que la API haya publicado y
**no escribe los que siguen abiertos**: un bloque a medias tiene poca cobertura y el detector lo
marcaría como anomalía. Si una hora llega tarde, la carga siguiente la incorpora. El bloque de
noche se mide sobre las 3 horas que la API publica (20–22): con la cobertura sobre 4, el detector
lo marcaba unas 20 veces más que en el histórico (ver la §6.5 bis de las notas de la memoria).

Para rehacer los bloques ya guardados (p. ej., tras cambiar cómo se calculan), sin llamar a la API:

```powershell
python src/etl/pipeline_tiempo_real.py --reprocesar 2026-09-16   # usa DATABASE_URL del .env
```

## Base de datos siempre encendida

Con cargas también de día, **la RDS está siempre encendida** (desde el 2026-10-23). Las
programaciones `jupiter-rds-encender` (22:00) y `jupiter-rds-apagar` (01:30) quedan
**desactivadas**, no borradas: se pueden reactivar si se vuelve a una sola carga nocturna.
«Apagar entorno» apaga solo la EC2; «Encender entorno» enciende la RDS si la encuentra parada.
Para la entrega, también la EC2 se queda encendida: `jupiter-ec2-apagar` está desactivada.

Ventanas de RDS (solo admite UTC): copias **21:10–21:40 UTC** y mantenimiento **domingo
22:00–22:30 UTC**; ninguna coincide con una carga, ni en verano ni en invierno.

```powershell
# Estado de las programaciones de la RDS (deben estar DISABLED)
aws scheduler get-schedule --name jupiter-rds-apagar --query State --output text
aws scheduler get-schedule --name jupiter-rds-encender --query State --output text

# Estado actual de la base de datos
aws rds describe-db-instances --db-instance-identifier jupiter-postgres --query "DBInstances[0].DBInstanceStatus" --output text
```

## La aplicación en la EC2

> **Estado:** en marcha desde el 2026-10-13 (EC2 `jupiter-api`). Web, API y chat verificados de
> punta a punta.

Una EC2 `t3.medium` (x86, 30 GB) con Amazon Linux 2023 ejecuta con Docker Compose los cuatro
servicios de la aplicación, cada uno con su imagen de ECR, y Phoenix para las trazas del agente
(imagen pública, ver [Trazas del agente en `/phoenix/`](#trazas-del-agente-en-phoenix)):

```
navegador ──:80──▶ web (nginx: SPA + proxy /api/*) ──▶ api-usuario ──▶ agente ──▶ rag
                    │                                     │              │  └──▶ Bedrock (rol de la EC2)
                    │                                     └──── RDS ─────┘  (agente: rol agente_lectura)
                    └── proxy /phoenix/* ──▶ phoenix ◀── trazas OTLP ── agente
```

Solo `web` publica puerto (80, abierto a todo internet y sin contraseña, con IP fija: ver
[Acceso a la web](#acceso-a-la-web-pública-con-ip-fija)); `api-usuario`, `agente`, `rag` y
`phoenix` no tienen autenticación y quedan en la red interna de Docker. El `LLMOrchestrator` no se despliega:
el equipo lo sustituyó por el `Agente` (ver `docs/notas_memoria.md` §2).

| Fichero ([`ec2/`](ec2/)) | Qué hace |
|---|---|
| [`compose.yml`](ec2/compose.yml) | Los servicios con imágenes de ECR (`jupiter-web`, `jupiter-api-usuario`, `jupiter-agente`, `jupiter-rag`) y `phoenix` (Docker Hub, versión fijada aquí). Sin base de datos: usa RDS |
| [`arrancar.sh`](ec2/arrancar.sh) | Lee de SSM qué versión de cada servicio está activa y la configuración, inicia sesión en ECR y levanta los contenedores. Con un argumento (`arrancar.sh rag`) actualiza solo ese servicio. nginx resuelve los nombres de los contenedores en cada petición, así que recrear `api-usuario` o `phoenix` no obliga a reiniciar `web` |
| [`jupiter.service`](ec2/jupiter.service) | Servicio de systemd: en cada arranque de la máquina trae de S3 el `arrancar.sh` actual y lo ejecuta. `arrancar.sh` reinstala a su vez este fichero si cambió en S3, así que un despliegue con la EC2 apagada se aplica completo al encenderla |
| [`cloudwatch-agent.json`](ec2/cloudwatch-agent.json) | Configuración del agente de CloudWatch: memoria y disco de la máquina (ver [Logs y métricas en CloudWatch](#logs-y-métricas-en-cloudwatch)) |
| [`user-data.sh`](ec2/user-data.sh) | Primer arranque: instala Docker, Compose y el agente de CloudWatch y deja instalado lo anterior |

De dónde sale cada cosa (todo con el rol de la máquina, `jupiter-ec2-api`, sin claves en disco):

| Origen | Contenido |
|---|---|
| `s3://jupiter-calidad-aire-madrid/ec2/` | Los ficheros de esta carpeta, copiados desde el repositorio |
| SSM `/jupiter/versiones/<servicio>` | Qué imagen está activa (tag = commit). La escribe el despliegue |
| SSM `/jupiter/database_url` | La misma cadena de conexión que usa la Lambda (la usa `api-usuario`) |
| SSM `/jupiter/agente/<variable>` | Configuración del agente que es secreta o se cambia sin tocar código: cada parámetro se convierte en una variable de entorno (`/jupiter/agente/llm_modelo` → `LLM_MODELO`). Una variable nueva del agente se da de alta creando el parámetro, sin tocar ningún script. Lo que es fijo de la máquina (proveedor, región, URL del RAG y de Phoenix) va en `compose.yml` y manda sobre SSM |
| SSM `/jupiter/web/contrasena` (SecureString, opcional) | Contraseña de la web y de `/phoenix/`, usuario `demo`. **Hoy no existe**: la web es pública (ver [Acceso a la web](#acceso-a-la-web-pública-con-ip-fija)) |
| Docker Hub `arizephoenix/phoenix` | Imagen pública de Phoenix; la versión está en `compose.yml`, no en SSM |

Parámetros del agente que hacen falta (los demás, como en [`Agente/.env.example`](../Agente/.env.example), son opcionales):

| Parámetro | Ejemplo | Sin él |
|---|---|---|
| `/jupiter/agente/llm_modelo` | `mistral.ministral-3-14b-instruct` | `/chat` responde 503 |
| `/jupiter/agente/llm_modelo_clasificador` | `openai.gpt-oss-120b-1:0` | El clasificador usa `llm_modelo` |
| `/jupiter/agente/database_url` (SecureString) | `postgresql://agente_lectura:…@<RDS>:5432/postgres` | Las preguntas de datos reciben la frase de datos no disponibles |

El proveedor (`LLM_PROVEEDOR=bedrock`), la región y las URL internas del RAG y de Phoenix están
fijos en el compose: Bedrock se usa con el rol de la máquina, sin claves. La `database_url` del agente usa el rol de solo lectura
`agente_lectura` ([`sql/rol_agente_lectura.sql`](sql/rol_agente_lectura.sql)) sobre la vista
`mediciones_bloques` ([`sql/vista_mediciones_bloques.sql`](sql/vista_mediciones_bloques.sql)),
nunca el usuario administrador.

Permisos del rol ([`permisos-ec2-api.json`](iam/permisos-ec2-api.json)): descargar **solo** esas
cuatro imágenes, leer **solo** esos parámetros y la carpeta `ec2/` del bucket, invocar modelos de
Bedrock en `eu-west-1`, escribir logs **solo** en el grupo `/jupiter/ec2` y publicar métricas **solo**
en el namespace `CWAgent`, más la política
gestionada `AmazonSSMManagedInstanceCore` para entrar con *Session Manager* (sin SSH ni puerto 22).

Los ficheros de `ec2/` llevan finales de línea LF aunque se trabaje en Windows
([`.gitattributes`](../.gitattributes)): un script con CRLF falla en Linux.

### Acceso a la web: pública, con IP fija

Para la corrección del TFM, la web está **abierta a todo internet sin contraseña** en una
**IP elástica** (fija): el tribunal entra con un enlace que no cambia aunque se apague la EC2.
`/phoenix/` responde **403** a todo el mundo, porque guarda las preguntas de los usuarios. Lo deja
así [`aws/3_web_publica.ps1`](aws/3_web_publica.ps1), seguido de **Desplegar → `web`**:

1. Crea la IP elástica con la etiqueta `Name=jupiter-api` y la asocia a la EC2. Sustituye a la IP
   automática, que cambiaba en cada encendido; cuesta lo mismo (0,005 $/h), también con la EC2
   apagada. Si se recrea la EC2, volver a ejecutar el script la asocia a la nueva.
2. Borra `/jupiter/web/contrasena`. Al desplegar `web`, nginx deja de pedir contraseña.
3. Crea el presupuesto **`jupiter-bedrock-diario`** (AWS Budgets): 2 $ al día solo de Bedrock, con
   correo al pasar el 80 % y el 100 %. **Solo avisa, no corta nada**, y AWS actualiza los costes
   unas tres veces al día: llega horas tarde. El aviso rápido es la alarma de CloudWatch
   `jupiter-bedrock-tokens-dia` ([Alarmas y panel](#alarmas-y-panel)).

```powershell
pwsh deploy/aws/3_web_publica.ps1 -Correo <correo-del-equipo>   # -LimiteDia 2 por defecto
```

Riesgo asumido: la API y el chat no tienen autenticación, así que cualquiera puede hacer preguntas
al asistente y cada una se paga en Bedrock (del orden de 0,001-0,003 $ por pregunta
`[estimación]`). Para cortar el acceso: volver a poner la contraseña (abajo) y desplegar `web`.

#### Usuario y contraseña (opcional)

El mecanismo sigue en `arrancar.sh` para volver a cerrar la web sin tocar código:

- `arrancar.sh` lee la contraseña de `/jupiter/web/contrasena`, calcula su hash con `openssl` y
  escribe `web-acceso.conf`, `web.htpasswd` y `phoenix-acceso.conf`, que `compose.yml` monta en el
  contenedor `web`. En el disco solo queda el hash. nginx pide usuario (`demo`) y contraseña en toda
  la web, también en `/api/*`, salvo a la propia máquina (el healthcheck entra por 127.0.0.1). En
  `/phoenix/` la pide siempre, también desde la propia máquina. No hace falta reconstruir la imagen.
- Sin el parámetro, los ficheros de la web quedan vacíos y la web funciona como antes; `/phoenix/`
  responde **403**. Si el parámetro existe pero no se puede leer (permisos), el arranque **se
  detiene**: mejor sin web que abierta sin contraseña.
- **La web va por HTTP**: la contraseña viaja sin cifrar. Usa una contraseña exclusiva para esto,
  nunca una que se use en otro sitio. **Nunca en el repositorio**: es público.

Poner o cambiar la contraseña (cifrada en SSM):

```powershell
aws ssm put-parameter --name /jupiter/web/contrasena --type SecureString --overwrite --value 'la-contraseña'
```

Se aplica al encenderse la EC2 o con **Desplegar → `web`** si ya está encendida. Para quitarla:
`aws ssm delete-parameter --name /jupiter/web/contrasena` y volver a desplegar `web` (es lo que hace
`3_web_publica.ps1`).

**El 443 está cerrado**: no hay HTTPS y nginx solo escucha en el 80. Un navegador que intenta
primero `https://` espera hasta agotar el tiempo antes de pasar a `http://` (con el puerto abierto
recibía un rechazo inmediato). Si la web tarda en abrir desde un móvil, se escribe `http://`
delante de la IP.

### Trazas del agente en `/phoenix/`

[Phoenix](https://docs.arize.com/phoenix) recibe las trazas OpenTelemetry del agente (cascada de
cada turno: clasificación, llamadas al LLM, RAG y SQL, con tiempos, tokens y errores) y las enseña
en `http://<ip>/phoenix/`.

- **Acceso**: usuario `demo` y la contraseña de la web, **siempre**. Sin `/jupiter/web/contrasena`
  responde 403, aunque la web esté abierta: guarda las preguntas y respuestas de los usuarios y no
  tiene autenticación propia. **Con la web pública no hay contraseña, así que hoy responde 403**:
  para consultarlo, poner la contraseña un rato y desplegar `web` (la web vuelve a pedirla).
- **Envío**: el agente manda las trazas a `http://phoenix:6006/v1/traces` por la red interna, sin
  pasar por nginx ni por el prefijo. La dirección está fija en `compose.yml` (`PHOENIX_ENDPOINT`),
  sin parámetro de SSM. El agente no depende de Phoenix para arrancar: si no está, pierde las trazas
  y sigue respondiendo. Van al proyecto `agente`.
- **KPIs en la web**: `http://<ip>/admin/uso` (enlace «Administración» en el pie) muestra los KPIs
  que el agente calcula leyendo la API REST de Phoenix por la misma red interna (raíz deducida de
  `PHOENIX_ENDPOINT`, sin variable nueva): web → `/api/kpis` → api-usuario → `agente:8200/kpis`.
  Solo agregados, sin texto de usuarios; caché de 60 s en el agente. Con Phoenix parado la página
  avisa del error (503) y el resto de la web sigue igual. Lleva el rótulo «Solo administradores»
  pero no tiene más protección que la contraseña de la web (hoy, ninguna: la ve cualquiera, solo con
  agregados): el proyecto no tiene gestión de usuarios.
- **Datos**: SQLite en el volumen Docker `phoenix_data` (disco de la EC2): sobrevive a apagar y
  encender, se pierde si se recrea la máquina. Retención de 30 días, pero la purga corre los
  domingos a las 00:00 UTC y con la EC2 apagada a esa hora no se ejecuta: las trazas crecen.
- **Versión**: fijada en `compose.yml` (`arizephoenix/phoenix:version-20.19.0`, la misma que en
  local). Cambiarla = commit + **Desplegar → `phoenix`**; volver atrás = revertir el commit y
  desplegar otra vez. No pasa por ECR ni por el rollback.

### Logs y métricas en CloudWatch

Lo del asistente se mira en Phoenix; CloudWatch recoge el resto: el borde HTTP y la máquina.

- **Logs**: `web`, `api-usuario` y `rag` escriben en el grupo `/jupiter/ec2` (driver `awslogs` de
  Docker, un *stream* por contenedor: `jupiter-web-1`, `jupiter-api-usuario-1`, `jupiter-rag-1`;
  14 días de retención). El de `web` es el log de acceso de nginx en JSON (`status`, `uri`,
  `request_time`, `upstream_response_time`), listo para filtros de métricas. `agente` y `phoenix`
  se quedan en la máquina: lo del agente ya está en sus trazas. Docker guarda además una copia
  local, así que `docker compose logs` funciona igual para todos.
- **Si CloudWatch falla**: los logs van en modo `non-blocking`, así que un contenedor no se para
  si CloudWatch no responde (se pierden líneas). Sin permisos de escritura, en cambio, Docker no
  llega a crear el contenedor: el rol se amplía **antes** de desplegar este `compose.yml`.
- **Métricas de la máquina**: `AWS/EC2` no mide memoria ni disco. El agente de CloudWatch publica
  `mem_used_percent` y `disk_used_percent` (de `/`) cada 60 s en `CWAgent`, con la dimensión
  `InstanceId`: exactamente 2 métricas, dentro de las 10 gratuitas. `arrancar.sh` lo instala si
  falta y le aplica [`cloudwatch-agent.json`](ec2/cloudwatch-agent.json) en cada ejecución; si
  algo falla, avisa en el log y la aplicación arranca igual.
- **El grupo de logs** lo crea la pila de CloudFormation `jupiter-observabilidad`
  ([`cloudwatch/observabilidad.yaml`](cloudwatch/observabilidad.yaml)), no Docker ni un comando
  suelto: si existiera antes, la pila no podría crearlo. La misma pila crea los filtros, las
  alarmas y el panel ([Alarmas y panel](#alarmas-y-panel)).

Activarlo (una vez, en este orden):

```powershell
# 0. Comprobar que la sesión apunta a la cuenta del máster (con varios perfiles, AWS_PROFILE)
aws sts get-caller-identity --query Account --output text
# 1. Logs, filtros, alarmas y panel (crea o actualiza la pila). Necesita el ID de la EC2:
#    al recrear la máquina con 2_crear_ec2.ps1 hay que volver a ejecutarlo.
$Ec2 = aws ec2 describe-instances --filters "Name=tag:Name,Values=jupiter-api" "Name=instance-state-name,Values=pending,running,stopping,stopped" --query "Reservations[0].Instances[0].InstanceId" --output text
aws cloudformation deploy --template-file deploy/cloudwatch/observabilidad.yaml --stack-name jupiter-observabilidad --parameter-overrides InstanceId=$Ec2
# 2. Los permisos nuevos del rol de la EC2 (sustituye la política en línea por la del repositorio)
aws iam put-role-policy --role-name jupiter-ec2-api --policy-name ec2-api --policy-document file://deploy/iam/permisos-ec2-api.json
```

Después, **Desplegar → `phoenix`** publica `ec2/` en S3 e instala el agente de CloudWatch sin
recrear los demás contenedores; **Apagar entorno** y **Encender entorno** aplican los logs a todos.

Consultar un log (Logs Insights sobre `/jupiter/ec2`):

```
fields @timestamp, status, request_time, uri
| filter @logStream like /web/ and uri like /^\/api\/chat/
| sort @timestamp desc
| limit 20
```

### Operarla desde GitHub Actions

Todo se hace con los mismos workflows que la Lambda; nadie entra en la máquina.

| Workflow | Qué hace con la EC2 |
|---|---|
| **Desplegar** → `web`, `api-usuario`, `agente` o `rag` | Construye la imagen del código de la rama elegida (normalmente `main`), la sube a ECR con el commit como tag, publica `ec2/` en S3 y guarda la versión en SSM con quién y desde dónde. Si la EC2 está encendida, actualiza **solo ese contenedor** y espera a que esté sano; si está apagada, la usará al encenderse |
| **Desplegar** → `phoenix` | Sin tests ni build: publica `ec2/` en S3 y, si la EC2 está encendida, recrea solo `phoenix` con la versión de `compose.yml` |
| **Rollback** → mismo componente | Sin versión: tabla de imágenes en ECR, la activa y el historial de cambios. Con versión (commit corto) y motivo: la vuelve a activar sin reconstruir nada. No aplica a `phoenix` |
| **Encender entorno** | Enciende la RDS y la EC2 a la vez, espera a que los servicios estén sanos y muestra la dirección de la web (la IP elástica: no cambia). Si el arranque falla, muestra el log y lo reintenta una vez: con la EC2 ya encendida, relanzarlo tras corregir la causa vuelve a levantar la aplicación |
| **Apagar entorno** | Apaga solo la EC2: la RDS se queda siempre encendida para las cargas de cada franja |

Los workflows localizan la máquina por su etiqueta `Name=jupiter-api` y le ejecutan comandos
con *SSM Run Command* ([`.github/scripts/ec2.sh`](../.github/scripts/ec2.sh)). Los roles solo pueden
encender, apagar o ejecutar comandos en instancias con esa etiqueta.

Antes de encender la EC2 por primera vez hay que desplegar los cuatro servicios: sin versión de
alguno, `arrancar.sh` se detiene con un mensaje claro.

### Crear los recursos en AWS (una sola vez)

Con credenciales de AWS en la sesión y desde la raíz del repositorio:

1. **`pwsh deploy/aws/1_recursos_ec2.ps1`**: los 4 repositorios de ECR (con limpieza: 5 imágenes
   por servicio), el rol de la máquina, los permisos nuevos de los roles de GitHub, el grupo de
   seguridad (80 solo para tu IP, que detecta solo o se pasa con `-MiIp`), el modelo del agente y
   del clasificador en SSM y los ficheros de `ec2/` en S3.
2. **Mergear a `main`** y lanzar **Desplegar** con `web`, `api-usuario`, `agente` y `rag`. Sin EC2,
   cada despliegue solo sube la imagen y registra su versión.
3. **Usuario de solo lectura del agente:** ejecutar en la RDS [`sql/vista_mediciones_bloques.sql`](sql/vista_mediciones_bloques.sql)
   y [`sql/rol_agente_lectura.sql`](sql/rol_agente_lectura.sql), y guardar su conexión cifrada:
   `aws ssm put-parameter --name /jupiter/agente/database_url --type SecureString --value "postgresql://agente_lectura:<clave>@<endpoint>:5432/postgres"`.
   Se puede hacer también después: sin él, el chat funciona salvo las preguntas de datos.
4. **`pwsh deploy/aws/2_crear_ec2.ps1`**: crea la EC2 (comprueba antes que estén las 4 versiones) y
   su apagado a la 01:30 (`jupiter-ec2-apagar`, con el rol `jupiter-scheduler-ec2`; **desactivado
   desde el 2026-10-23** para la entrega, y volver a ejecutar el script lo reactiva). Al crearse
   instala Docker y levanta la aplicación; el script muestra su dirección.
5. **`pwsh deploy/aws/3_web_publica.ps1 -Correo <correo>`** (opcional, para abrir la web al
   público): IP fija, web sin contraseña y presupuesto diario de Bedrock (ver
   [Acceso a la web](#acceso-a-la-web-pública-con-ip-fija)). Después, **Desplegar → `web`**.

Para cambiar de modelo: `aws ssm put-parameter --name /jupiter/agente/llm_modelo --overwrite --value <id>`
y reiniciar el agente (Desplegar → `agente`, o apagar y encender).

## Probar en local antes de desplegar

```powershell
docker run --rm -p 9000:8080 `
  -e S3_BUCKET=jupiter-calidad-aire-madrid `
  -e AWS_DEFAULT_REGION=eu-west-1 `
  -e AWS_ACCESS_KEY_ID=$Env:AWS_ACCESS_KEY_ID `
  -e AWS_SECRET_ACCESS_KEY=$Env:AWS_SECRET_ACCESS_KEY `
  -e AWS_SESSION_TOKEN=$Env:AWS_SESSION_TOKEN `
  jupiter-pipeline
```

Y desde otra terminal:

```powershell
curl.exe -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" -d '{}'
```

En local se le pasan tus credenciales porque no hay rol; en AWS eso desaparece.

## Operación del día a día

```powershell
# ¿Se ha ejecutado?
aws logs tail /aws/lambda/jupiter-pipeline --since 1d --format short

# Invocarla a mano
aws lambda invoke --function-name jupiter-pipeline --cli-binary-format raw-in-base64-out --payload '{}' "$env:TEMP\respuesta.json"
Get-Content "$env:TEMP\respuesta.json"

# Estado de la programación
aws scheduler get-schedule --name jupiter-pipeline-diario --query "{estado:State,expresion:ScheduleExpression,zona:ScheduleExpressionTimezone}"

# Arrancar la base de datos a mano si estuviera parada (o el workflow "Encender entorno")
aws rds start-db-instance --db-instance-identifier jupiter-postgres
```

> Si la base de datos está parada, **las cargas fallan** y las franjas de ese rato se pierden (la
> API solo sirve el día en curso). Nada la apaga ya de forma automática: si alguien la para a mano,
> hay que volver a encenderla.

## Alarmas y panel

Todas avisan por correo a través del tema SNS `jupiter-alertas`. Las dos de la Lambda se crearon a
mano; las otras nueve las crea la pila `jupiter-observabilidad`
([`cloudwatch/observabilidad.yaml`](cloudwatch/observabilidad.yaml)).

| Alarma | Salta cuando | Por qué importa |
|---|---|---|
| `jupiter-pipeline-errores` | La función lanza una excepción | El fallo evidente |
| `jupiter-pipeline-sin-ejecuciones` | No hay ninguna invocación en 24 h | **El fallo silencioso**: si el disparador deja de funcionar no hay ningún error que contar, y el hueco en los datos se descubriría semanas después |
| `jupiter-pipeline-sin-datos` | `HorasNuevas` suma menos de 1 en 24 h, o no hay dato | El otro fallo silencioso: la Lambda se ejecuta, la API de Madrid no devuelve registros y la función termina sin excepción. También salta si la Lambda falla (p. ej. con la RDS apagada) |
| `jupiter-web-5xx` | 5 o más respuestas 5xx en 5 min | API, agente o proxy rotos. Si el agente cae, `api-usuario` responde 503: cubre también el chat caído |
| `jupiter-bedrock-errores` | 3 o más errores de cliente o llamadas limitadas en 15 min | Modelo retirado, cuota agotada o permisos |
| `jupiter-bedrock-tokens-dia` | Más de `UmbralTokensDia` tokens de entrada en 24 h | Freno de coste: un bucle o la web abierta al mundo |
| `jupiter-ec2-status` | La EC2 falla sus comprobaciones de estado 10 min | Máquina rota |
| `jupiter-ec2-creditos-cpu` | Menos de 20 créditos de CPU | La EC2 está en modo `unlimited`: no se frena, pero empieza a pagar CPU extra |
| `jupiter-ec2-memoria` | Memoria > 90 % durante 10 min | Cinco contenedores en 4 GB |
| `jupiter-ec2-disco` | Disco `/` > 85 % | Imágenes, logs locales y trazas de Phoenix, que no se purgan con la EC2 apagada |
| `jupiter-rds-espacio` | Menos de 2 GB libres en la RDS | La RDS tiene autoescalado de disco: antes de llenarse crece, y cuesta más |

- Las de la pila usan `notBreaching`: con la EC2 o la RDS apagadas no hay datos y eso no es
  un fallo. Solo `jupiter-pipeline-sin-ejecuciones` y `jupiter-pipeline-sin-datos` tratan la falta
  de datos como fallo, a propósito.
- **Bedrock se mide para toda la cuenta**, no por aplicación: las evaluaciones de modelos del equipo
  también cuentan para los errores y los tokens.
- **Umbral de tokens**: parámetro `UmbralTokensDia` de la plantilla, provisional en 2 M (el día con
  más consumo medido fueron 1,23 M, en una campaña de evaluación). Cambiarlo no toca nada más: los
  parámetros que no se pasan conservan su valor.

  ```powershell
  aws cloudformation deploy --template-file deploy/cloudwatch/observabilidad.yaml --stack-name jupiter-observabilidad --parameter-overrides UmbralTokensDia=<tokens>
  ```

**Filtros de métricas** sobre el log de acceso de nginx (namespace `Jupiter`, sin dimensiones):

| Métrica | Filtro |
|---|---|
| `Peticiones5xx` | `{ $.status >= 500 }` |
| `Peticiones4xx` | `{ $.status >= 400 && $.status < 500 && $.status != 401 }`: sin 401, que con la contraseña piden los robots y el primer acceso de cada navegador |
| `TiempoRespuestaChat` | `{ $.uri = "/api/chat*" && $.status = 200 }`, valor `request_time`. En el streaming es el tiempo hasta el último evento |

**Panel [`jupiter`](https://eu-west-1.console.aws.amazon.com/cloudwatch/home?region=eu-west-1#dashboards/dashboard/jupiter)** (la URL sale también en las salidas de la pila): Bedrock por modelo (invocaciones,
latencia, tokens y errores), borde HTTP (4xx/5xx, tiempo de `/api/chat` y el estado de las once
alarmas), máquina (CPU, créditos, memoria y disco), datos (ejecuciones de la Lambda, CPU,
conexiones y espacio de la RDS) y negocio (horas cargadas, estaciones con datos, tasa de anomalías
y preguntas del día). El detalle de cada pregunta al asistente está en `/phoenix/`.

### Métricas de negocio del pipeline

Cada ejecución de la Lambda termina con una línea JSON en formato **EMF** (*Embedded Metric
Format*, [`metricas_emf.py`](metricas_emf.py)). CloudWatch extrae de ella las métricas al recibir
el log: sin `PutMetricData`, sin boto3 y sin permisos nuevos en el rol. Namespace
`Jupiter/Pipeline`, sin dimensiones (cada dimensión sería otra métrica de pago):

| Métrica | Qué es |
|---|---|
| `HorasNuevas` | Filas horarias insertadas o corregidas en `calidad_aire_horas_live` |
| `EstacionesConDatos` | Estaciones con al menos una medición válida (de 24) |
| `BloquesAnomalos` | Bloques marcados como anómalos por el Isolation Forest |
| `TasaAnomalias` | `BloquesAnomalos` / bloques procesados, en %. El modelo espera ~1 % (`contamination`) |

Si la API no devuelve registros la línea sale igual, con ceros: así la alarma
`jupiter-pipeline-sin-datos` se evalúa con dato y el hueco se ve en el panel.

La misma línea lleva tres campos que **no son métricas** (no cuestan): `pct_validas` (% de filas
con validación `V`), `estaciones_sin_datos` (códigos de las que faltan) y
`anomalias_por_magnitud` (bloques anómalos por código de magnitud). Se consultan con Logs
Insights sobre `/aws/lambda/jupiter-pipeline`:

```
fields @timestamp, HorasNuevas, EstacionesConDatos, TasaAnomalias, pct_validas, @message
| filter ispresent(HorasNuevas)
| sort @timestamp desc
| limit 30
```

«Preguntas respondidas hoy» no es una métrica nueva: es la estadística `SampleCount` de
`TiempoRespuestaChat`, que cuenta una muestra por cada 200 en `/api/chat*`. El día es el de UTC.

**Orden al desplegar el cambio:** primero la imagen nueva de la Lambda y una invocación a mano (con
la RDS encendida) para que exista la métrica `HorasNuevas`; después la pila. Al revés, la alarma
nueva nace en `ALARM` (sin datos = fallo) y manda un correo hasta la primera ejecución.

**Probar una alarma.** Lo más rápido comprueba que llega el correo (la alarma vuelve sola a su
estado en el siguiente periodo):

```powershell
aws cloudwatch set-alarm-state --alarm-name jupiter-web-5xx --state-value ALARM --state-reason "Prueba"
```

La prueba completa fuerza la condición real: con la EC2 encendida, parar el agente por Session
Manager (`cd /opt/jupiter && docker compose --env-file versiones.env -f compose.yml stop agente`),
hacer cinco preguntas en el chat (503) y esperar el correo de `jupiter-web-5xx` en unos 5-10 min.
Después, `... start agente` o apagar y encender.

## Coste

Prácticamente todo el coste es la instancia de RDS, que ahora está siempre encendida (la capa
gratuita no aplica a la cuenta): ~12,4 $/mes de instancia, ~2,3 $ de disco y ~1 $ de copias
`[estimación]`. Con el encendido programado de antes (3,5 h por noche) costaba 4,67 $ del 1 al 22
de octubre. Las IPv4 públicas son el segundo gasto. Lambda, S3, ECR,
EventBridge, SSM, CloudWatch y SNS caen dentro de capas gratuitas a este volumen. Detalle en [`docs/notas_memoria.md`](../docs/notas_memoria.md).

**El coste a evitar** es un NAT Gateway (~32 $/mes): por eso la Lambda vive **fuera de la VPC** y se
conecta a RDS por su endpoint público, con TLS obligatorio (`rds.force_ssl = 1`) y la contraseña
cifrada en SSM. Es una decisión consciente, explicada en las notas.
